package messaging

import (
	"bytes"
	"context"
	"time"

	"go.uber.org/zap"
	"google.golang.org/protobuf/types/known/timestamppb"

	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"
	eventpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/event/v1"
	messagingpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/messaging/v1"

	"github.com/code-payments/flipcash2-server/chat"
	"github.com/code-payments/flipcash2-server/model"
)

// broadcastTimeout bounds the synchronous part of a chat update: loading a
// DM's members when the caller didn't supply them, and publishing to the event
// buses. Both are quick, so this is a guard against a wedged store rather than
// a budget the work is expected to approach.
const broadcastTimeout = 5 * time.Second

// publishChatUpdate broadcasts a ChatUpdate to the chat's members, optionally
// excluding one user (e.g. the originator of a typing notification). It is
// best-effort: a failure to load members is logged, not surfaced, so it never
// fails the originating RPC.
//
// A DM's update is fanned out per member over the user-keyed event bus. A
// group's update is published once on its chat topic — the delivery layer
// resolves the servers hosting subscribed streams from the chat-keyed
// subscription registry — so publishing costs the same no matter how large
// the group, and no member read is needed at all. The pushes an update earns
// walk the group's roster in pages of their own (see pushSentMessages).
//
// members may be supplied by a caller that already has a DM's pair in hand
// (e.g. from AdvanceLastMessage), avoiding a redundant read; when nil, they
// are loaded here. A group's members are never loaded whole: the argument is
// ignored for a group.
func (s *Sender) publishChatUpdate(
	ctx context.Context,
	log *zap.Logger,
	chatID *commonpb.ChatId,
	update *eventpb.ChatUpdate,
	exclude *commonpb.UserId,
	members []*commonpb.UserId,
) {
	isGroup := chat.IsGroupChatID(chatID)

	// The broadcast is a side effect of a mutation the caller has already
	// committed (a send, pointer advance, reaction, edit, or delete), so the
	// caller going away must not abort it: a client that disconnects right after
	// its write lands would otherwise leave every other member unnotified until
	// their next sync. Detach from the caller's cancellation, keeping context
	// values (auth/trace metadata), and bound the synchronous work — the DM
	// member read and the bus publishes — with a budget of its own. The push
	// work detaches again with its own, longer budget.
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), broadcastTimeout)
	defer cancel()

	if !isGroup && len(members) == 0 {
		var err error
		members, err = s.chats.GetMembers(ctx, chatID)
		if err != nil {
			log.With(zap.Error(err)).Warn("Failure loading members for chat update broadcast")
			return
		}
	}

	update.Chat = chatID
	e := &eventpb.Event{
		Id:   model.MustGenerateEventID(),
		Ts:   timestamppb.Now(),
		Type: &eventpb.Event_ChatUpdate{ChatUpdate: update},
	}
	if isGroup {
		var excludes []*commonpb.UserId
		if exclude != nil {
			excludes = []*commonpb.UserId{exclude}
		}
		s.chatEventBus.OnEvent(chatID, &eventpb.ChatEvent{
			ChatId:         chatID,
			Event:          e,
			ExcludeUserIds: excludes,
		})
	} else {
		for _, m := range members {
			if exclude != nil && bytes.Equal(m.Value, exclude.Value) {
				continue
			}
			s.userEventBus.OnEvent(m, e)
		}
	}

	// todo: Tie in push to the event bus?
	//
	// Only a freshly sent message earns a push. Edits, deletes, reactions,
	// pointers and typing all ride the same broadcast and must not.
	sent := sentMessages(update)
	if len(sent) == 0 {
		return
	}

	// Everything the push needs beyond this point — a group's metadata and
	// roster, the blocklist and mutes, the sender profile, and the sends
	// themselves — runs detached from the originating RPC. Live delivery is
	// already out on the event bus above, so the caller has nothing left to
	// wait for, and a group send's latency no longer scales with its
	// membership.
	go s.pushSentMessages(ctx, log, chatID, members, sent)
}

// sentMessages returns the messages an update introduces: the message_sent
// mutations of its event-log events, in order. This is the broadcast's only
// carrier of new messages now that the deprecated new_messages field is no
// longer populated.
func sentMessages(update *eventpb.ChatUpdate) []*messagingpb.Message {
	var sent []*messagingpb.Message
	for _, e := range update.GetEvents().GetEvents() {
		for _, m := range e.GetMutations() {
			if msg := m.GetMessageSent(); msg != nil {
				sent = append(sent, msg)
			}
		}
	}
	return sent
}
