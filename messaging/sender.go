package messaging

import (
	"bytes"
	"context"
	"slices"
	"time"

	"github.com/pkg/errors"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	chatpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/chat/v1"
	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"
	eventpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/event/v1"
	messagingpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/messaging/v1"

	"github.com/code-payments/flipcash2-server/badge"
	"github.com/code-payments/flipcash2-server/blocklist"
	"github.com/code-payments/flipcash2-server/chat"
	"github.com/code-payments/flipcash2-server/event"
	"github.com/code-payments/flipcash2-server/model"
	"github.com/code-payments/flipcash2-server/profile"
	"github.com/code-payments/flipcash2-server/push"
	ocp_data "github.com/code-payments/ocp-server/ocp/data"
)

// sideEffectTimeout bounds the post-persistence side effects of a send
// (pointer advance, last-message bump, broadcast, pushes) once they have been
// detached from the caller's cancellation.
const sideEffectTimeout = 5 * time.Second

// Sender is the engine behind a message send: it persists the message and
// performs every side effect — advancing the sender's read pointer, bumping the
// chat's last message, and broadcasting (with pushes) to members. It carries no
// authentication or transport concerns, so internal callers (e.g. injecting a
// cash message after a payment) can construct just a Sender rather than the full
// gRPC Server. The Server holds one and delegates SendMessage to it.
type Sender struct {
	log *zap.Logger

	badges     badge.Store
	chats      chat.Store
	messages   Store
	profiles   profile.Store
	blocklists blocklist.Store

	// media resolves blob metadata so a broadcast new-message event carries the
	// same resolved media a read would. Hydration is best-effort and a no-op for
	// media-free sends (e.g. server-authored cash messages).
	media Media

	ocpData ocp_data.Provider

	pusher push.Pusher

	userEventBus *event.Bus[*commonpb.UserId, *eventpb.Event]
	chatEventBus *event.Bus[*commonpb.ChatId, *eventpb.ChatEvent]

	// pushPageSize, pushMutedWholeSetCap and pushPageSlots shape the push
	// fan-out (see push.go); the defaults are production's, and the options
	// exist so tests can drive a multi-page walk over a handful of members.
	pushPageSize         int
	pushMutedWholeSetCap uint64

	// pushPageSlots bounds how many group pages this Sender has in send at
	// once across every fan-out it runs, one token per page in flight (see
	// defaultPushPageConcurrency). It is what makes a walk's pages send in
	// parallel, and what stops a burst of sends into large groups from
	// running unboundedly many page sends at once.
	pushPageSlots chan struct{}

	// teamUserID is the Flipcash team account (see WithTeamAccount), nil when
	// there is none.
	teamUserID *commonpb.UserId
}

// SenderOption configures a Sender beyond its dependencies.
type SenderOption func(*Sender)

// WithPushPageSize sets how many members a push fan-out walks per page (see
// defaultPushPageSize). Values below one are ignored.
func WithPushPageSize(n int) SenderOption {
	return func(s *Sender) {
		if n > 0 {
			s.pushPageSize = n
		}
	}
}

// WithPushMutedWholeSetCap sets the recorded-mute count at or below which a
// fan-out reads a chat's muted set whole rather than per page (see
// defaultPushMutedWholeSetCap). Zero forces the per-page read for every
// group.
func WithPushMutedWholeSetCap(n uint64) SenderOption {
	return func(s *Sender) { s.pushMutedWholeSetCap = n }
}

// WithPushPageConcurrency sets how many group pages the Sender sends at once
// across every fan-out it runs (see defaultPushPageConcurrency). One
// serializes pages, as a walk did before pages were sent concurrently.
// Values below one are ignored.
func WithPushPageConcurrency(n int) SenderOption {
	return func(s *Sender) {
		if n > 0 {
			s.pushPageSlots = make(chan struct{}, n)
		}
	}
}

// WithTeamAccount names the Flipcash team account (see flipcashteam), an
// account the server writes as and nobody reads as, in a DM with every user.
// A send by it does not advance its read pointer, which would only claim, to
// the user it talks to, that it had read what they sent. And nothing in its
// DMs is delivered to it, on the event stream or by push (see
// publishChatUpdate): a stream opened as it would carry every event in every
// one of its DMs, and a push to it is a profile, mute, blocklist and token
// read spent on an account with no devices. The user it talks to hears about
// everything as usual. Nil, the default, names no one.
//
// Its DMs are created with it excluded from the feed, which the parent
// configures the chat store to do for every DM with it (see
// chat.WithExcludedFromFeed), so no send in them moves its copy of the chat's
// activity either: its copies of every DM share one partition, which would take a
// write from every message in all of them, and the feed they order is one
// nobody reads.
func WithTeamAccount(teamUserID *commonpb.UserId) SenderOption {
	return func(s *Sender) { s.teamUserID = teamUserID }
}

func NewSender(
	log *zap.Logger,
	badges badge.Store,
	chats chat.Store,
	messages Store,
	profiles profile.Store,
	blocklists blocklist.Store,
	media Media,
	ocpData ocp_data.Provider,
	pusher push.Pusher,
	userEventBus *event.Bus[*commonpb.UserId, *eventpb.Event],
	chatEventBus *event.Bus[*commonpb.ChatId, *eventpb.ChatEvent],
	opts ...SenderOption,
) *Sender {
	s := &Sender{
		log:          log,
		badges:       badges,
		chats:        chats,
		messages:     messages,
		profiles:     profiles,
		blocklists:   blocklists,
		media:        media,
		ocpData:      ocpData,
		pusher:       pusher,
		userEventBus: userEventBus,
		chatEventBus: chatEventBus,

		pushPageSize:         defaultPushPageSize,
		pushMutedWholeSetCap: defaultPushMutedWholeSetCap,
		pushPageSlots:        make(chan struct{}, defaultPushPageConcurrency),
	}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

// OutgoingMessage is one message of a SendBatch: what Send takes per message,
// minus the chat, which a batch shares.
type OutgoingMessage struct {
	SenderID           *commonpb.UserId // nil for a system message
	Content            []*messagingpb.Content
	ClientMessageID    *messagingpb.ClientMessageId
	CountsTowardUnread bool
}

// Send persists content as a message in the chat and performs every side effect
// of a send. It is SendBatch with a batch of one, and the shared core behind the
// SendMessage RPC and internal, server-authored sends — the latter bypass the
// RPC's client-side content and membership checks (e.g. injecting a cash
// message after a payment settles).
func (s *Sender) Send(
	ctx context.Context,
	chatID *commonpb.ChatId,
	senderID *commonpb.UserId,
	content []*messagingpb.Content,
	clientMessageID *messagingpb.ClientMessageId,
	countsTowardUnread bool,
) (*messagingpb.Message, error) {
	sent, err := s.SendBatch(ctx, chatID, []OutgoingMessage{{
		SenderID:           senderID,
		Content:            content,
		ClientMessageID:    clientMessageID,
		CountsTowardUnread: countsTowardUnread,
	}})
	if err != nil {
		return nil, err
	}
	return sent[0], nil
}

// SendBatch persists a batch of messages in one chat atomically (see
// Store.PutMessages) and performs every side effect of a send once for the
// whole batch: it advances each sender's own read pointer past the last message
// they sent in it, records the batch's last message as the chat's most recent,
// and broadcasts one update carrying every message to all members. The result
// is in batch order.
//
// A SenderID may be nil to denote a system message, in which case no read
// pointer is advanced for it. CountsTowardUnread controls whether a message
// advances the chat's unread sequence: true for user-authored messages (the
// sender doesn't see their own message as unread because their read pointer is
// advanced past it), false for messages that shouldn't bump anyone's unread
// count. Sends are idempotent on the batch's client message IDs: a retry returns
// the originally persisted messages and skips the side effects, which already
// ran on the first send — re-running them would duplicate pushes to members.
// Each message earns its own push, as it would sent alone.
//
// It is for server-authored sends whose messages must be seen together or not
// at all (e.g. a chat's opening messages); a client sends one at a time.
func (s *Sender) SendBatch(
	ctx context.Context,
	chatID *commonpb.ChatId,
	outgoing []OutgoingMessage,
) ([]*messagingpb.Message, error) {
	log := s.log
	if senderID := commonSender(outgoing); senderID != nil {
		log = log.With(zap.String("user_id", model.UserIDString(senderID)))
	}

	if err := chatID.Validate(); err != nil {
		return nil, errors.Wrap(err, "chat id failed validation")
	}
	if len(outgoing) == 0 {
		return nil, errors.New("expected at least one message")
	}
	now := time.Now().UTC()
	inputs := make([]MessageInput, len(outgoing))
	for i, out := range outgoing {
		if err := out.SenderID.Validate(); err != nil {
			return nil, errors.Wrap(err, "sender id failed validation")
		}
		if len(out.Content) != 1 {
			return nil, errors.New("expected one piece of content")
		}
		if err := out.Content[0].Validate(); err != nil {
			return nil, errors.Wrap(err, "content failed validation")
		}
		if err := out.ClientMessageID.Validate(); err != nil {
			return nil, errors.Wrap(err, "client message id failed validation")
		}
		inputs[i] = MessageInput{
			SenderID:           out.SenderID,
			Content:            out.Content,
			Timestamp:          now,
			ClientMessageID:    out.ClientMessageID,
			CountsTowardUnread: out.CountsTowardUnread,
		}
	}

	msgs, created, err := s.messages.PutMessages(ctx, chatID, inputs)
	if err != nil {
		log.With(zap.Error(err)).Warn("Failure persisting messages")
		return nil, status.Error(codes.Internal, "")
	}

	// Build the message protos once and resolve their media metadata, so the
	// protos returned to the caller (the SendMessage response) and the ones
	// broadcast to members carry the same hydrated messages. Best-effort and a
	// no-op for media-free sends; the messages are already persisted, so a
	// resolution failure just leaves Blob unset for clients to re-fetch.
	msgProtos := make([]*messagingpb.Message, len(msgs))
	for i, msg := range msgs {
		msgProtos[i] = msg.ToProto()
	}
	if s.media != nil {
		if err := hydrateMedia(ctx, s.media, msgProtos); err != nil {
			log.With(zap.Error(err)).Warn("Failure resolving media metadata")
		}
	}

	// A retried send (same client message IDs) already ran every side effect
	// when the messages were first persisted — most importantly the push to
	// members. Re-running them would duplicate notifications, so return the
	// original messages and stop here.
	if !created {
		return msgProtos, nil
	}

	// The messages are now durable, and a retry skips the side effects below —
	// so one lost here (most importantly the push) is never re-run. Detach from
	// the caller's cancellation so a client disconnect or RPC deadline can't
	// abort them, keeping context values (auth/trace metadata) intact. The
	// timeout bounds the work, since the side effects run synchronously in the
	// handler and a never-canceled context would let a wedged call hold it
	// forever.
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), sideEffectTimeout)
	defer cancel()

	// Each sender has implicitly read their own messages, so advance their READ
	// pointer past the last one they sent. The targets are messages we just
	// persisted, so their existence is guaranteed — advance directly without a
	// separate existence read. Best-effort: it's reconstructable and self-heals.
	// A system message (no sender) has no pointer to advance, and neither does
	// the team account, which reads nothing (see WithTeamAccount).
	var advancedPointers []*messagingpb.Pointer
	for _, last := range lastMessagePerSender(msgs) {
		if s.isTeamAccount(last.SenderID) {
			continue
		}
		pointer, advanced, err := s.messages.AdvancePointer(ctx, chatID, last.SenderID, messagingpb.Pointer_READ, last.ID)
		if err != nil {
			log.With(zap.Error(err)).Warn("Failure advancing sender read pointer")
			continue
		}
		if advanced {
			advancedPointers = append(advancedPointers, pointer)
		}
	}

	// Record the batch's last message as the chat's most recent: bumps
	// last_activity so the chat sorts to the top of members' inboxes and
	// denormalizes last_message_id for the feed. Decoupled from persistence: a
	// lagging bump self-heals on the next message. It also hands back the chat
	// members, which the broadcast below reuses to avoid a second membership
	// read — only for DMs, whose members ride on the canonical record the bump
	// touches anyway. A group chat's membership lives in its own records, so
	// members comes back empty and the broadcast loads it itself.
	last := msgs[len(msgs)-1]
	lastMessageAdvanced, members, err := s.chats.AdvanceLastMessage(ctx, chatID, last.ID, last.Timestamp)
	if err != nil && !errors.Is(err, chat.ErrChatNotFound) {
		log.With(zap.Error(err)).Warn("Failure advancing chat last message")
	}

	// Notify all members (including the senders' other devices) of the new
	// messages. Each send rides the gap-detected event log as a message_sent
	// event, and only there: the deprecated new_messages field is no longer
	// populated, so a message crosses the wire once per recipient. The senders'
	// read pointers and the new last activity are only included when they
	// actually advanced — a no-op must not broadcast a stale pointer or
	// timestamp. A group never carries real-time pointers (see AdvancePointer):
	// the senders' auto-advances are stored, but riding them along here would
	// give group clients a partial pointer stream they can't rely on. msgProtos
	// were already built and media-hydrated above.
	events := make([]*messagingpb.Event, len(msgProtos))
	for i, msgProto := range msgProtos {
		events[i] = NewMessageSentEvent(msgProto)
	}
	update := &eventpb.ChatUpdate{
		Events: &messagingpb.EventBatch{Events: events},
	}
	if len(advancedPointers) > 0 && !chat.IsGroupChatID(chatID) {
		update.PointerUpdates = &messagingpb.PointerBatch{Pointers: advancedPointers}
	}
	if lastMessageAdvanced {
		update.MetadataUpdates = []*chatpb.MetadataUpdate{{
			Kind: &chatpb.MetadataUpdate_LastActivityChanged_{
				LastActivityChanged: &chatpb.MetadataUpdate_LastActivityChanged{
					NewLastActivity: timestamppb.New(last.Timestamp),
				},
			},
		}}
	}
	// Reuse the members AdvanceLastMessage already loaded (empty for a group
	// chat or if it failed, in which case publishChatUpdate loads them itself).
	s.publishChatUpdate(ctx, log, chatID, update, nil, members)

	return msgProtos, nil
}

// TeamAccount returns the team account the Sender was built with (see
// WithTeamAccount), or nil when there is none. It is for sending as the team
// (see flipcashteam.SendMessages), so that the account sent as is the one the
// Sender treats as the team, from one configuration.
func (s *Sender) TeamAccount() *commonpb.UserId {
	if s.teamUserID == nil {
		return nil
	}
	return &commonpb.UserId{Value: bytes.Clone(s.teamUserID.Value)}
}

// isTeamAccount reports whether userID is the team account (see
// WithTeamAccount).
func (s *Sender) isTeamAccount(userID *commonpb.UserId) bool {
	return s.teamUserID != nil && bytes.Equal(userID.GetValue(), s.teamUserID.Value)
}

// withoutTeamAccount returns userIDs less the team account (see
// WithTeamAccount), or userIDs itself when it holds no team account.
func (s *Sender) withoutTeamAccount(userIDs []*commonpb.UserId) []*commonpb.UserId {
	if s.teamUserID == nil || !slices.ContainsFunc(userIDs, s.isTeamAccount) {
		return userIDs
	}
	out := make([]*commonpb.UserId, 0, len(userIDs))
	for _, userID := range userIDs {
		if !s.isTeamAccount(userID) {
			out = append(out, userID)
		}
	}
	return out
}

// commonSender returns the sender every message of a batch shares, or nil when
// they differ or the batch's messages are system messages.
func commonSender(outgoing []OutgoingMessage) *commonpb.UserId {
	if len(outgoing) == 0 || outgoing[0].SenderID == nil {
		return nil
	}
	for _, out := range outgoing[1:] {
		if !bytes.Equal(out.SenderID.GetValue(), outgoing[0].SenderID.Value) {
			return nil
		}
	}
	return outgoing[0].SenderID
}

// lastMessagePerSender returns, for each sender of a batch in order of first
// appearance, the last message they sent in it. System messages (no sender)
// are left out, since they have no pointer to advance.
func lastMessagePerSender(msgs []*Message) []*Message {
	var order []string
	last := make(map[string]*Message)
	for _, msg := range msgs {
		if msg.SenderID == nil {
			continue
		}
		key := string(msg.SenderID.Value)
		if _, ok := last[key]; !ok {
			order = append(order, key)
		}
		last[key] = msg
	}
	out := make([]*Message, len(order))
	for i, key := range order {
		out[i] = last[key]
	}
	return out
}
