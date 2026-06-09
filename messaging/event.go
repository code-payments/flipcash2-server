package messaging

import (
	"bytes"
	"context"

	"go.uber.org/zap"
	"google.golang.org/protobuf/types/known/timestamppb"

	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"
	eventpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/event/v1"

	"github.com/code-payments/flipcash2-server/chat"
	"github.com/code-payments/flipcash2-server/event"
	"github.com/code-payments/flipcash2-server/profile"
	"github.com/code-payments/flipcash2-server/push"
	ocp_data "github.com/code-payments/ocp-server/ocp/data"
)

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

	chats chat.Store,
	profiles profile.Store,
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
	// todo: Assumes a contact-based DM chat
	if update.NewMessages == nil {
		return
	}
	for _, message := range update.NewMessages.Messages {
		if message.SenderId == nil {
			continue
		}

		senderProfile, err := profiles.GetProfile(ctx, message.SenderId, true)
		if err == profile.ErrNotFound {
			continue
		} else if err != nil {
			log.With(zap.Error(err)).Warn("Failure getting sender profile for push")
			continue
		}
		if senderProfile.PhoneNumber == nil {
			continue
		}

		var membersForPush []*commonpb.UserId
		for _, member := range members {
			if !bytes.Equal(member.Value, message.SenderId.Value) {
				membersForPush = append(membersForPush, member)
			}
		}

		err = push.SendContactDmPush(ctx, pusher, ocpData, update.Chat, message, senderProfile.PhoneNumber, membersForPush...)
		if err != nil {
			log.With(zap.Error(err)).Warn("Failure sending message push")
			continue
		}
	}
}
