package messaging

import (
	"bytes"
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

	"github.com/code-payments/flipcash2-server/auth"
	"github.com/code-payments/flipcash2-server/chat"
	"github.com/code-payments/flipcash2-server/database"
	"github.com/code-payments/flipcash2-server/event"
	"github.com/code-payments/flipcash2-server/model"
	"github.com/code-payments/flipcash2-server/profile"
	"github.com/code-payments/flipcash2-server/push"
)

type Server struct {
	log *zap.Logger

	authz auth.Authorizer

	chats    chat.Store
	messages Store
	profiles profile.Store

	pusher push.Pusher

	eventBus *event.Bus[*commonpb.UserId, *eventpb.Event]

	messagingpb.UnimplementedMessagingServer
}

func NewServer(
	log *zap.Logger,
	authz auth.Authorizer,
	chats chat.Store,
	messages Store,
	profiles profile.Store,
	pusher push.Pusher,
	eventBus *event.Bus[*commonpb.UserId, *eventpb.Event],
) *Server {
	return &Server{
		log:      log,
		authz:    authz,
		chats:    chats,
		messages: messages,
		profiles: profiles,
		pusher:   pusher,
		eventBus: eventBus,
	}
}

func (s *Server) GetMessage(ctx context.Context, req *messagingpb.GetMessageRequest) (*messagingpb.GetMessageResponse, error) {
	userID, err := s.authz.Authorize(ctx, req, &req.Auth)
	if err != nil {
		return nil, err
	}

	log := s.log.With(zap.String("user_id", model.UserIDString(userID)))

	if member, err := s.isMember(ctx, log, req.ChatId, userID); err != nil {
		return nil, err
	} else if !member {
		return &messagingpb.GetMessageResponse{Result: messagingpb.GetMessageResponse_DENIED}, nil
	}

	msg, err := s.messages.GetMessage(ctx, req.ChatId, req.MessageId)
	switch {
	case errors.Is(err, ErrMessageNotFound):
		return &messagingpb.GetMessageResponse{Result: messagingpb.GetMessageResponse_NOT_FOUND}, nil
	case err != nil:
		log.With(zap.Error(err)).Warn("Failure getting message")
		return nil, status.Error(codes.Internal, "")
	}

	return &messagingpb.GetMessageResponse{
		Result:  messagingpb.GetMessageResponse_OK,
		Message: msg.ToProto(),
	}, nil
}

func (s *Server) GetMessages(ctx context.Context, req *messagingpb.GetMessagesRequest) (*messagingpb.GetMessagesResponse, error) {
	userID, err := s.authz.Authorize(ctx, req, &req.Auth)
	if err != nil {
		return nil, err
	}

	log := s.log.With(zap.String("user_id", model.UserIDString(userID)))

	if member, err := s.isMember(ctx, log, req.ChatId, userID); err != nil {
		return nil, err
	} else if !member {
		return &messagingpb.GetMessagesResponse{Result: messagingpb.GetMessagesResponse_DENIED}, nil
	}

	var msgs []*Message
	if batch := req.GetMessageIds(); batch != nil {
		refs := make([]MessageRef, len(batch.MessageIds))
		for i, id := range batch.MessageIds {
			refs[i] = MessageRef{ChatID: req.ChatId, MessageID: id}
		}
		msgs, err = s.messages.GetMessagesByRefs(ctx, refs)
	} else {
		opts := database.FromProtoQueryOptions(req.GetOptions())
		msgs, err = s.messages.GetMessages(ctx, req.ChatId, opts...)
	}
	if err != nil {
		log.With(zap.Error(err)).Warn("Failure getting messages")
		return nil, status.Error(codes.Internal, "")
	}

	if len(msgs) == 0 {
		return &messagingpb.GetMessagesResponse{Result: messagingpb.GetMessagesResponse_NOT_FOUND}, nil
	}
	protos := make([]*messagingpb.Message, len(msgs))
	for i, m := range msgs {
		protos[i] = m.ToProto()
	}
	return &messagingpb.GetMessagesResponse{
		Result:   messagingpb.GetMessagesResponse_OK,
		Messages: &messagingpb.MessageBatch{Messages: protos},
	}, nil
}

func (s *Server) SendMessage(ctx context.Context, req *messagingpb.SendMessageRequest) (*messagingpb.SendMessageResponse, error) {
	userID, err := s.authz.Authorize(ctx, req, &req.Auth)
	if err != nil {
		return nil, err
	}

	log := s.log.With(zap.String("user_id", model.UserIDString(userID)))

	switch req.Content[0].Type.(type) {
	case *messagingpb.Content_Text:
	default:
		return &messagingpb.SendMessageResponse{Result: messagingpb.SendMessageResponse_DENIED}, nil
	}

	if member, err := s.isMember(ctx, log, req.ChatId, userID); err != nil {
		return nil, err
	} else if !member {
		return &messagingpb.SendMessageResponse{Result: messagingpb.SendMessageResponse_DENIED}, nil
	}

	msg, err := s.messages.PutMessage(ctx, req.ChatId, userID, req.Content, time.Now().UTC(), req.ClientMessageId, true)
	if err != nil {
		log.With(zap.Error(err)).Warn("Failure persisting message")
		return nil, status.Error(codes.Internal, "")
	}

	// The sender has implicitly read their own message, so advance their READ
	// pointer past it. The target is the message we just persisted, so its
	// existence is guaranteed — use the unchecked path to skip the existence read.
	// Best-effort: it's reconstructable and self-heals.
	pointerAdvanced, err := s.messages.AdvancePointerUnchecked(ctx, req.ChatId, userID, messagingpb.Pointer_READ, msg.ID)
	if err != nil {
		log.With(zap.Error(err)).Warn("Failure advancing sender read pointer")
	}

	// Record this message as the chat's most recent: bumps last_activity so the
	// chat sorts to the top of members' inboxes and denormalizes last_message_id
	// for the feed. Decoupled from persistence: a lagging bump self-heals on the
	// next message. It also hands back the chat members, which the broadcast below
	// reuses to avoid a second membership read.
	lastMessageAdvanced, members, err := s.chats.AdvanceLastMessage(ctx, req.ChatId, msg.ID, msg.Timestamp)
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
			UserId: userID,
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
	s.publishChatUpdate(ctx, log, req.ChatId, update, nil, members)

	return &messagingpb.SendMessageResponse{
		Result:  messagingpb.SendMessageResponse_OK,
		Message: msg.ToProto(),
	}, nil
}

func (s *Server) AdvancePointer(ctx context.Context, req *messagingpb.AdvancePointerRequest) (*messagingpb.AdvancePointerResponse, error) {
	userID, err := s.authz.Authorize(ctx, req, &req.Auth)
	if err != nil {
		return nil, err
	}

	log := s.log.With(zap.String("user_id", model.UserIDString(userID)))

	if member, err := s.isMember(ctx, log, req.ChatId, userID); err != nil {
		return nil, err
	} else if !member {
		return &messagingpb.AdvancePointerResponse{Result: messagingpb.AdvancePointerResponse_DENIED}, nil
	}

	advanced, err := s.messages.AdvancePointer(ctx, req.ChatId, userID, req.PointerType, req.NewValue)
	switch {
	case errors.Is(err, ErrMessageNotFound):
		return &messagingpb.AdvancePointerResponse{Result: messagingpb.AdvancePointerResponse_MESSAGE_NOT_FOUND}, nil
	case err != nil:
		log.With(zap.Error(err)).Warn("Failure advancing pointer")
		return nil, status.Error(codes.Internal, "")
	}

	if advanced {
		s.publishChatUpdate(ctx, log, req.ChatId, &eventpb.ChatUpdate{
			PointerUpdates: &messagingpb.PointerBatch{Pointers: []*messagingpb.Pointer{{
				Type:   req.PointerType,
				UserId: userID,
				Value:  req.NewValue,
			}}},
		}, nil, nil)
	}

	return &messagingpb.AdvancePointerResponse{Result: messagingpb.AdvancePointerResponse_OK}, nil
}

func (s *Server) NotifyIsTyping(ctx context.Context, req *messagingpb.NotifyIsTypingRequest) (*messagingpb.NotifyIsTypingResponse, error) {
	userID, err := s.authz.Authorize(ctx, req, &req.Auth)
	if err != nil {
		return nil, err
	}

	log := s.log.With(zap.String("user_id", model.UserIDString(userID)))

	if member, err := s.isMember(ctx, log, req.ChatId, userID); err != nil {
		return nil, err
	} else if !member {
		return &messagingpb.NotifyIsTypingResponse{Result: messagingpb.NotifyIsTypingResponse_DENIED}, nil
	}

	// Typing notifications are transient and only meaningful to other members.
	s.publishChatUpdate(ctx, log, req.ChatId, &eventpb.ChatUpdate{
		IsTypingNotifications: &messagingpb.IsTypingNotificationBatch{
			IsTypingNotifications: []*messagingpb.IsTypingNotification{{
				UserId: userID,
				State:  req.State,
			}},
		},
	}, userID, nil)

	return &messagingpb.NotifyIsTypingResponse{Result: messagingpb.NotifyIsTypingResponse_OK}, nil
}

func (s *Server) isMember(ctx context.Context, log *zap.Logger, chatID *commonpb.ChatId, userID *commonpb.UserId) (bool, error) {
	isMember, err := s.chats.IsMember(ctx, chatID, userID)
	if err != nil {
		log.With(zap.Error(err)).Warn("Failure checking chat membership")
		return false, status.Error(codes.Internal, "")
	}
	return isMember, nil
}

// publishChatUpdate fans a ChatUpdate out to each member of the chat over the
// event bus, optionally excluding one user (e.g. the originator of a typing
// notification). It is best-effort: a failure to load members is logged, not
// surfaced, so it never fails the originating RPC.
//
// members may be supplied by a caller that already has the set in hand (e.g.
// from AdvanceLastMessage), avoiding a redundant read; when nil, the members are
// loaded here.
func (s *Server) publishChatUpdate(ctx context.Context, log *zap.Logger, chatID *commonpb.ChatId, update *eventpb.ChatUpdate, exclude *commonpb.UserId, members []*commonpb.UserId) {
	if len(members) == 0 {
		var err error
		members, err = s.chats.GetMembers(ctx, chatID)
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
		s.eventBus.OnEvent(m, e)
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

		senderProfile, err := s.profiles.GetProfile(ctx, message.SenderId, true)
		if err == profile.ErrNotFound {
			continue
		} else if err != nil {
			log.With(zap.Error(err)).Warn("Failure getting sender profile for push")
			continue
		}
		if senderProfile.PhoneNumber != nil {
			continue
		}

		var membersForPush []*commonpb.UserId
		for _, member := range members {
			if !bytes.Equal(member.Value, message.SenderId.Value) {
				membersForPush = append(membersForPush, member)
			}
		}

		err = push.SendContactDmPush(ctx, s.pusher, update.Chat, message, senderProfile.PhoneNumber, membersForPush...)
		if err != nil {
			log.With(zap.Error(err)).Warn("Failure sending message push")
			continue
		}
	}
}
