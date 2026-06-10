package messaging

import (
	"context"
	"errors"
	"time"

	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	chatpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/chat/v1"
	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"
	eventpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/event/v1"
	messagingpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/messaging/v1"

	"github.com/code-payments/flipcash2-server/chat"
	"github.com/code-payments/flipcash2-server/event"
	"github.com/code-payments/flipcash2-server/model"
	"github.com/code-payments/flipcash2-server/profile"
	"github.com/code-payments/flipcash2-server/push"
	ocp_data "github.com/code-payments/ocp-server/ocp/data"
)

// Sender is the engine behind a message send: it persists the message and
// performs every side effect — advancing the sender's read pointer, bumping the
// chat's last message, and broadcasting (with pushes) to members. It carries no
// authentication or transport concerns, so internal callers (e.g. injecting a
// cash message after a payment) can construct just a Sender rather than the full
// gRPC Server. The Server holds one and delegates SendMessage to it.
type Sender struct {
	log *zap.Logger

	chats    chat.Store
	messages Store
	profiles profile.Store

	ocpData ocp_data.Provider

	pusher push.Pusher

	eventBus *event.Bus[*commonpb.UserId, *eventpb.Event]
}

func NewSender(
	log *zap.Logger,
	chats chat.Store,
	messages Store,
	profiles profile.Store,
	ocpData ocp_data.Provider,
	pusher push.Pusher,
	eventBus *event.Bus[*commonpb.UserId, *eventpb.Event],
) *Sender {
	return &Sender{
		log:      log,
		chats:    chats,
		messages: messages,
		profiles: profiles,
		ocpData:  ocpData,
		pusher:   pusher,
		eventBus: eventBus,
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
// idempotent on (chatID, clientMessageID).
func (s *Sender) Send(
	ctx context.Context,
	chatID *commonpb.ChatId,
	senderID *commonpb.UserId,
	content []*messagingpb.Content,
	clientMessageID *messagingpb.ClientMessageId,
	countsTowardUnread bool,
) (*Message, error) {
	log := s.log
	if senderID != nil {
		log = log.With(zap.String("user_id", model.UserIDString(senderID)))
	}

	msg, err := s.messages.PutMessage(ctx, chatID, senderID, content, time.Now().UTC(), clientMessageID, countsTowardUnread)
	if err != nil {
		log.With(zap.Error(err)).Warn("Failure persisting message")
		return nil, status.Error(codes.Internal, "")
	}

	// The sender has implicitly read their own message, so advance their READ
	// pointer past it. The target is the message we just persisted, so its
	// existence is guaranteed — use the unchecked path to skip the existence read.
	// Best-effort: it's reconstructable and self-heals. A system message (no
	// sender) has no pointer to advance.
	var pointerAdvanced bool
	if senderID != nil {
		pointerAdvanced, err = s.messages.AdvancePointerUnchecked(ctx, chatID, senderID, messagingpb.Pointer_READ, msg.ID)
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
	// message. The sender's read pointer and the new last activity are only
	// included when they actually advanced — a no-op must not broadcast a stale
	// pointer or timestamp.
	update := &eventpb.ChatUpdate{
		NewMessages: &messagingpb.MessageBatch{Messages: []*messagingpb.Message{msg.ToProto()}},
	}
	if pointerAdvanced {
		update.PointerUpdates = &messagingpb.PointerBatch{Pointers: []*messagingpb.Pointer{{
			Type:   messagingpb.Pointer_READ,
			UserId: senderID,
			Value:  msg.ID,
		}}}
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
	publishChatUpdate(ctx, log, s.chats, s.profiles, s.ocpData, s.pusher, s.eventBus, chatID, update, nil, members)

	return msg, nil
}
