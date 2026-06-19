package messaging

import (
	"context"
	"errors"

	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"
	eventpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/event/v1"
	messagingpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/messaging/v1"

	"github.com/code-payments/flipcash2-server/auth"
	"github.com/code-payments/flipcash2-server/chat"
	"github.com/code-payments/flipcash2-server/database"
	"github.com/code-payments/flipcash2-server/model"
)

type Server struct {
	log *zap.Logger

	authz auth.Authorizer

	chats    chat.Store
	messages Store

	sender *Sender

	messagingpb.UnimplementedMessagingServer
}

func NewServer(
	log *zap.Logger,
	authz auth.Authorizer,
	chats chat.Store,
	messages Store,
	sender *Sender,
) *Server {
	return &Server{
		log:      log,
		authz:    authz,
		chats:    chats,
		messages: messages,
		sender:   sender,
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

	var repliedMessageID *messagingpb.MessageId
	switch content := req.Content[0].Type.(type) {
	case *messagingpb.Content_Text:
	case *messagingpb.Content_Reply:
		switch content.Reply.Content[0].Type.(type) {
		case *messagingpb.Content_Text:
		default:
			return &messagingpb.SendMessageResponse{Result: messagingpb.SendMessageResponse_DENIED}, nil
		}
		repliedMessageID = content.Reply.RepliedMessageId
	default:
		return &messagingpb.SendMessageResponse{Result: messagingpb.SendMessageResponse_DENIED}, nil
	}

	if member, err := s.isMember(ctx, log, req.ChatId, userID); err != nil {
		return nil, err
	} else if !member {
		return &messagingpb.SendMessageResponse{Result: messagingpb.SendMessageResponse_DENIED}, nil
	}

	// The replied-to message must exist in this chat and be repliable. Checked
	// after membership so non-members can't probe which message IDs exist.
	if repliedMessageID != nil {
		repliedMessage, err := s.messages.GetMessage(ctx, req.ChatId, repliedMessageID)
		switch {
		case errors.Is(err, ErrMessageNotFound):
			return &messagingpb.SendMessageResponse{Result: messagingpb.SendMessageResponse_DENIED}, nil
		case err != nil:
			log.With(zap.Error(err)).Warn("Failure getting replied-to message")
			return nil, status.Error(codes.Internal, "")
		}
		if !repliedMessage.IsReplyable() {
			return &messagingpb.SendMessageResponse{Result: messagingpb.SendMessageResponse_DENIED}, nil
		}
	}

	msg, err := s.sender.Send(ctx, req.ChatId, userID, req.Content, req.ClientMessageId, true)
	if err != nil {
		return nil, err
	}

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

	pointer, advanced, err := s.messages.AdvancePointer(ctx, req.ChatId, userID, req.PointerType, req.NewValue)
	switch {
	case errors.Is(err, ErrMessageNotFound):
		return &messagingpb.AdvancePointerResponse{Result: messagingpb.AdvancePointerResponse_MESSAGE_NOT_FOUND}, nil
	case err != nil:
		log.With(zap.Error(err)).Warn("Failure advancing pointer")
		return nil, status.Error(codes.Internal, "")
	}

	if advanced {
		publishChatUpdate(ctx, log, s.sender.badges, s.sender.chats, s.sender.profiles, s.sender.ocpData, s.sender.pusher, s.sender.eventBus, req.ChatId, &eventpb.ChatUpdate{
			PointerUpdates: &messagingpb.PointerBatch{Pointers: []*messagingpb.Pointer{pointer}},
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
	publishChatUpdate(ctx, log, s.sender.badges, s.sender.chats, s.sender.profiles, s.sender.ocpData, s.sender.pusher, s.sender.eventBus, req.ChatId, &eventpb.ChatUpdate{
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
