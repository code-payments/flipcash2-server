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

const pushTimeout = 3 * time.Second

// publishChatUpdate fans a ChatUpdate out to each member of the chat over the
// event bus, optionally excluding one user (e.g. the originator of a typing
// notification). It is best-effort: a failure to load members is logged, not
// surfaced, so it never fails the originating RPC.
//
// members may be supplied by a caller that already has the set in hand (e.g.
// from AdvanceLastMessage), avoiding a redundant read; when nil, the members are
// loaded here.
func publishChatUpdate(
	ctx context.Context,

	log *zap.Logger,

	badges badge.Store,
	chats chat.Store,
	profiles profile.Store,
	blocklists blocklist.Store,
	ocpData ocp_data.Provider,

	pusher push.Pusher,
	eventBus *event.Bus[*commonpb.UserId, *eventpb.Event],

	chatID *commonpb.ChatId,
	update *eventpb.ChatUpdate,
	exclude *commonpb.UserId,
	members []*commonpb.UserId,
) {
	if len(members) == 0 {
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
	for _, m := range members {
		if exclude != nil && bytes.Equal(m.Value, exclude.Value) {
			continue
		}
		eventBus.OnEvent(m, e)
	}

	// todo: Tie in push to the event bus?
	if update.NewMessages == nil {
		return
	}

	// Pushes identify the sender differently per chat type — a contact DM push
	// carries the sender's phone number, which is private in every other chat
	// type. The type is recovered from the members already in hand, since a
	// DM's ID commits to its type via the derivation domain — no store read.
	// Skipping pushes on UNKNOWN is the safe default: a push must never fall
	// back to a rendering that could leak the sender's phone number.
	chatType := chat.DeriveDmChatType(chatID, members)

	for _, message := range update.NewMessages.Messages {
		if message.SenderId == nil {
			continue
		}

		go func(message *messagingpb.Message) {
			ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), pushTimeout)
			defer cancel()

			// Push recipients are every member but the sender, minus anyone who has
			// blocked the sender — a user who blocks another must stop receiving
			// pushes for that user's messages. The block check is scoped to the
			// recipient's own blocklist (recipient is the owner, sender the blocked
			// candidate). It fails closed: on a lookup failure we suppress the push
			// rather than risk notifying a recipient who has blocked the sender. The
			// message itself is still delivered on the event stream, so a transient
			// blocklist error costs only the notification, never the message.
			var membersForPush []*commonpb.UserId
			for _, member := range members {
				if bytes.Equal(member.Value, message.SenderId.Value) {
					continue
				}
				blocked, err := blocklists.IsBlocked(ctx, member, message.SenderId)
				if err != nil {
					log.With(zap.Error(err)).Warn("Failure checking blocklist for message push; suppressing push")
					continue
				}
				if blocked {
					continue
				}
				membersForPush = append(membersForPush, member)
			}
			// Every recipient was the sender or has blocked the sender: nothing to
			// push. Return before the sender-profile read and body render, which
			// would otherwise be spent on a push addressed to no one.
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
				err = push.SendContactDmPush(ctx, pusher, badges, ocpData, update.Chat, message, message.SenderId, senderProfile.PhoneNumber, membersForPush...)
			case chatpb.ChatType_TIP_DM:
				if senderProfile.DisplayName == "" {
					return
				}
				err = push.SendTipDmPush(ctx, pusher, badges, ocpData, update.Chat, message, message.SenderId, senderProfile.DisplayName, membersForPush...)
			default:
				return
			}
			if err != nil {
				log.With(zap.Error(err)).Warn("Failure sending message push")
				return
			}
		}(message)
	}
}
