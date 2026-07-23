package messaging

import (
	"context"
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

	eventBus *event.Bus[*commonpb.UserId, *eventpb.Event]
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
	eventBus *event.Bus[*commonpb.UserId, *eventpb.Event],
) *Sender {
	return &Sender{
		log:        log,
		badges:     badges,
		chats:      chats,
		messages:   messages,
		profiles:   profiles,
		blocklists: blocklists,
		media:      media,
		ocpData:    ocpData,
		pusher:     pusher,
		eventBus:   eventBus,
	}
}

// Send persists content as a message in the chat and performs every side effect
// of a send: it advances the sender's own read pointer past the message, records
// the message as the chat's most recent, and broadcasts the resulting update to
// all members. It is the shared core behind the SendMessage RPC and internal,
// server-authored sends — the latter bypass the RPC's client-side content and
// membership checks (e.g. injecting a cash message after a payment settles).
//
// senderID may be nil to denote a system message, in which case no read pointer
// is advanced. countsTowardUnread controls whether the message advances the
// chat's unread sequence: true for user-authored messages (the sender doesn't
// see their own message as unread because their read pointer is advanced past
// it), false for messages that shouldn't bump anyone's unread count. Sends are
// idempotent on (chatID, clientMessageID): a retry returns the originally
// persisted message and skips the side effects, which already ran on the first
// send — re-running them would duplicate pushes to members.
func (s *Sender) Send(
	ctx context.Context,
	chatID *commonpb.ChatId,
	senderID *commonpb.UserId,
	content []*messagingpb.Content,
	clientMessageID *messagingpb.ClientMessageId,
	countsTowardUnread bool,
) (*messagingpb.Message, error) {
	log := s.log
	if senderID != nil {
		log = log.With(zap.String("user_id", model.UserIDString(senderID)))
	}

	if err := chatID.Validate(); err != nil {
		return nil, errors.Wrap(err, "chat id failed validation")
	}
	if err := senderID.Validate(); err != nil {
		return nil, errors.Wrap(err, "sender id failed validation")
	}
	if len(content) != 1 {
		return nil, errors.New("expected one piece of content")
	}
	if err := content[0].Validate(); err != nil {
		return nil, errors.Wrap(err, "content failed validation")
	}
	if err := clientMessageID.Validate(); err != nil {
		return nil, errors.Wrap(err, "client message id failed validation")
	}

	msg, created, err := s.messages.PutMessage(ctx, chatID, senderID, content, time.Now().UTC(), clientMessageID, countsTowardUnread)
	if err != nil {
		log.With(zap.Error(err)).Warn("Failure persisting message")
		return nil, status.Error(codes.Internal, "")
	}

	// Build the message proto once and resolve its media metadata, so the proto
	// returned to the caller (the SendMessage response) and the one broadcast to
	// members carry the same hydrated message. Best-effort and a no-op for
	// media-free sends; the message is already persisted, so a resolution failure
	// just leaves Blob unset for clients to re-fetch.
	msgProto := msg.ToProto()
	if s.media != nil {
		if err := hydrateMedia(ctx, s.media, []*messagingpb.Message{msgProto}); err != nil {
			log.With(zap.Error(err)).Warn("Failure resolving media metadata")
		}
	}

	// A retried send (same client message ID) already ran every side effect when
	// the message was first persisted — most importantly the push to members.
	// Re-running them would duplicate notifications, so return the original
	// message and stop here.
	if !created {
		return msgProto, nil
	}

	// The message is now durable, and a retry skips the side effects below — so
	// one lost here (most importantly the push) is never re-run. Detach from the
	// caller's cancellation so a client disconnect or RPC deadline can't abort
	// them, keeping context values (auth/trace metadata) intact. The timeout
	// bounds the work, since the side effects run synchronously in the handler
	// and a never-canceled context would let a wedged call hold it forever.
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), sideEffectTimeout)
	defer cancel()

	// The sender has implicitly read their own message, so advance their READ
	// pointer past it. The target is the message we just persisted, so its
	// existence is guaranteed — advance directly without a separate existence read.
	// Best-effort: it's reconstructable and self-heals. A system message (no
	// sender) has no pointer to advance.
	var senderPointer *messagingpb.Pointer
	var pointerAdvanced bool
	if senderID != nil {
		senderPointer, pointerAdvanced, err = s.messages.AdvancePointer(ctx, chatID, senderID, messagingpb.Pointer_READ, msg.ID)
		if err != nil {
			log.With(zap.Error(err)).Warn("Failure advancing sender read pointer")
		}
	}

	// Record this message as the chat's most recent: bumps last_activity so the
	// chat sorts to the top of members' inboxes and denormalizes last_message_id
	// for the feed. Decoupled from persistence: a lagging bump self-heals on the
	// next message. It also hands back the chat members, which the broadcast below
	// reuses to avoid a second membership read.
	lastMessageAdvanced, members, err := s.chats.AdvanceLastMessage(ctx, chatID, msg.ID, msg.Timestamp)
	if err != nil && !errors.Is(err, chat.ErrChatNotFound) {
		log.With(zap.Error(err)).Warn("Failure advancing chat last message")
	}

	// Notify all members (including the sender's other devices) of the new
	// message. The send rides the gap-detected event log as a message_sent event;
	// new_messages carries the same message for clients that predate the event
	// log (it is deprecated but still populated during the transition). The
	// sender's read pointer and the new last activity are only included when they
	// actually advanced — a no-op must not broadcast a stale pointer or timestamp.
	// msgProto was already built and media-hydrated above.
	update := &eventpb.ChatUpdate{
		NewMessages: &messagingpb.MessageBatch{Messages: []*messagingpb.Message{msgProto}},
		Events:      &messagingpb.EventBatch{Events: []*messagingpb.Event{NewMessageSentEvent(msgProto)}},
	}
	if pointerAdvanced {
		update.PointerUpdates = &messagingpb.PointerBatch{Pointers: []*messagingpb.Pointer{senderPointer}}
	}
	if lastMessageAdvanced {
		update.MetadataUpdates = []*chatpb.MetadataUpdate{{
			Kind: &chatpb.MetadataUpdate_LastActivityChanged_{
				LastActivityChanged: &chatpb.MetadataUpdate_LastActivityChanged{
					NewLastActivity: timestamppb.New(msg.Timestamp),
				},
			},
		}}
	}
	// Reuse the members AdvanceLastMessage already loaded (nil if it failed, in
	// which case publishChatUpdate loads them itself).
	publishChatUpdate(ctx, log, s.badges, s.chats, s.profiles, s.blocklists, s.ocpData, s.pusher, s.eventBus, chatID, update, nil, members)

	return msgProto, nil
}
