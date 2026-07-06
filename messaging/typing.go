package messaging

import (
	"context"

	"go.uber.org/zap"

	eventpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/event/v1"
	messagingpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/messaging/v1"

	"github.com/code-payments/flipcash2-server/model"
)

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
