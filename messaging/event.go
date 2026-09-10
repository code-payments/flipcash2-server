package messaging

import (
	"bytes"
	"context"
	"time"

	"go.uber.org/zap"
	"google.golang.org/protobuf/types/known/timestamppb"

	chatpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/chat/v1"
	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"
	eventpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/event/v1"
	messagingpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/messaging/v1"

	"github.com/code-payments/flipcash2-server/badge"
	"github.com/code-payments/flipcash2-server/blocklist"
	"github.com/code-payments/flipcash2-server/chat"
	"github.com/code-payments/flipcash2-server/event"
	"github.com/code-payments/flipcash2-server/profile"
	"github.com/code-payments/flipcash2-server/push"
	ocp_data "github.com/code-payments/ocp-server/ocp/data"
)

// pushTimeout bounds a chat update's detached push work end to end: loading a
// group's membership and metadata, filtering recipients, and the batched FCM
// send. It is sized for a large group, whose membership read alone can span
// several pages and whose send fans out over multiple FCM batches.
const pushTimeout = 15 * time.Second

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
// the group, and no member read is needed at all unless the update also
// pushes.
//
// members may be supplied by a caller that already has the set in hand (e.g.
// from AdvanceLastMessage), avoiding a redundant read; when nil, the members
// are loaded where they are consumed: DM fan-out here, pushes below.
func publishChatUpdate(
	ctx context.Context,

	log *zap.Logger,

	badges badge.Store,
	chats chat.Store,
	profiles profile.Store,
	blocklists blocklist.Store,
	ocpData ocp_data.Provider,

	pusher push.Pusher,
	userEventBus *event.Bus[*commonpb.UserId, *eventpb.Event],
	chatEventBus *event.Bus[*commonpb.ChatId, *eventpb.ChatEvent],

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
	// work below detaches again with its own, longer budget.
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), broadcastTimeout)
	defer cancel()

	if !isGroup && len(members) == 0 {
		var err error
		members, err = chats.GetMembers(ctx, chatID)
		if err != nil {
			log.With(zap.Error(err)).Warn("Failure loading members for chat update broadcast")
			return
		}
	}

	update.Chat = chatID
	e := &eventpb.Event{
		Id:   event.MustGenerateEventID(),
		Ts:   timestamppb.Now(),
		Type: &eventpb.Event_ChatUpdate{ChatUpdate: update},
	}
	if isGroup {
		var excludes []*commonpb.UserId
		if exclude != nil {
			excludes = []*commonpb.UserId{exclude}
		}
		chatEventBus.OnEvent(chatID, &eventpb.ChatEvent{
			ChatId:         chatID,
			Event:          e,
			ExcludeUserIds: excludes,
		})
	} else {
		for _, m := range members {
			if exclude != nil && bytes.Equal(m.Value, exclude.Value) {
				continue
			}
			userEventBus.OnEvent(m, e)
		}
	}

	// todo: Tie in push to the event bus?
	if update.NewMessages == nil {
		return
	}

	// Everything the push needs beyond this point — a group's membership, its
	// metadata, the blocklist and sender profile, and the send itself — runs
	// detached from the originating RPC. Live delivery is already out on the
	// event bus above, so the caller has nothing left to wait for, and a group
	// send's latency no longer scales with its membership.
	go func() {
		ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), pushTimeout)
		defer cancel()

		// Pushes address members individually regardless of how the event was
		// published, so a group's members are read here — the one place the group
		// path still needs them.
		if isGroup && len(members) == 0 {
			var err error
			members, err = chats.GetMembers(ctx, chatID)
			if err != nil {
				log.With(zap.Error(err)).Warn("Failure loading members for message pushes")
				return
			}
		}

		// Pushes identify the sender differently per chat type — a contact DM push
		// carries the sender's phone number, which is private in every other chat
		// type. A DM's type is recovered from the members already in hand, since a
		// DM's ID commits to its type via the derivation domain — no store read. A
		// group's type cannot be member-derived, so its stored metadata is read
		// instead, which the push needs anyway for the group's title. That read is
		// the canonical record alone — it does not re-enumerate the membership this
		// function already has in hand.
		var chatType chatpb.ChatType
		var chatTitle string
		if isGroup {
			chatType = chatpb.ChatType_GROUP
			md, err := chats.GetChatByID(ctx, chatID)
			if err != nil {
				log.With(zap.Error(err)).Warn("Failure loading chat metadata for message pushes")
				return
			}
			chatTitle = md.Title
		} else {
			chatType = chat.DeriveDmChatType(chatID, members)
		}

		for _, message := range update.NewMessages.Messages {
			if message.SenderId == nil {
				continue
			}
			sendMessagePush(ctx, log, badges, profiles, blocklists, ocpData, pusher, chatID, chatType, chatTitle, members, message)
		}
	}()
}

// sendMessagePush pushes one new message to every member who should hear
// about it. Failures are logged, never surfaced: the message itself is already
// delivered on the event stream, so a failed push costs only the notification.
func sendMessagePush(
	ctx context.Context,

	log *zap.Logger,

	badges badge.Store,
	profiles profile.Store,
	blocklists blocklist.Store,
	ocpData ocp_data.Provider,

	pusher push.Pusher,

	chatID *commonpb.ChatId,
	chatType chatpb.ChatType,
	chatTitle string,
	members []*commonpb.UserId,
	message *messagingpb.Message,
) {
	// Push recipients are every member but the sender, minus anyone who has
	// blocked the sender — a user who blocks another must stop receiving
	// pushes for that user's messages. The check is scoped to each
	// recipient's own blocklist (recipient is the owner, sender the blocked
	// candidate), and every recipient is resolved in one batched read, so a
	// group send costs a single lookup rather than one per member.
	candidates := make([]*commonpb.UserId, 0, len(members))
	for _, member := range members {
		if bytes.Equal(member.Value, message.SenderId.Value) {
			continue
		}
		candidates = append(candidates, member)
	}
	// The sender was the only member: nothing to push, and nothing to ask the
	// blocklist about.
	if len(candidates) == 0 {
		return
	}

	// Fails closed: on a lookup failure we suppress the push rather than risk
	// notifying a recipient who has blocked the sender. One batched read has
	// no partial outcome to salvage, so that suppresses the push for every
	// recipient rather than just the one that failed — the message itself is
	// still delivered on the event stream, so a transient blocklist error
	// costs only the notification, never the message.
	blockingSender, err := blocklists.GetBlockers(ctx, message.SenderId, candidates)
	if err != nil {
		log.With(zap.Error(err)).Warn("Failure checking blocklists for message push; suppressing push")
		return
	}
	membersForPush := make([]*commonpb.UserId, 0, len(candidates))
	for _, candidate := range candidates {
		if blockingSender[string(candidate.Value)] {
			continue
		}
		membersForPush = append(membersForPush, candidate)
	}
	// Every recipient has blocked the sender: nothing to push. Return before
	// the sender-profile read and body render, which would otherwise be spent
	// on a push addressed to no one.
	if len(membersForPush) == 0 {
		return
	}

	senderProfile, err := profiles.GetProfile(ctx, message.SenderId, true)
	if err == profile.ErrNotFound {
		return
	} else if err != nil {
		log.With(zap.Error(err)).Warn("Failure getting sender profile for push")
		return
	}

	switch chatType {
	case chatpb.ChatType_CONTACT_DM:
		if senderProfile.PhoneNumber == nil {
			return
		}
		err = push.SendContactDmPush(ctx, pusher, badges, ocpData, chatID, message, message.SenderId, senderProfile.PhoneNumber, membersForPush...)
	case chatpb.ChatType_TIP_DM:
		if senderProfile.DisplayName == "" {
			return
		}
		err = push.SendTipDmPush(ctx, pusher, badges, ocpData, chatID, message, message.SenderId, senderProfile.DisplayName, membersForPush...)
	case chatpb.ChatType_GROUP:
		if senderProfile.DisplayName == "" {
			return
		}
		err = push.SendGroupChatPush(ctx, pusher, badges, ocpData, chatID, message, message.SenderId, senderProfile.DisplayName, chatTitle, membersForPush...)
	default:
		return
	}
	if err != nil {
		log.With(zap.Error(err)).Warn("Failure sending message push")
		return
	}
}
