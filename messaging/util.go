package messaging

import (
	"context"

	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"
	messagingpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/messaging/v1"
)

func (s *Server) isMember(ctx context.Context, log *zap.Logger, chatID *commonpb.ChatId, userID *commonpb.UserId) (bool, error) {
	isMember, err := s.chats.IsMember(ctx, chatID, userID)
	if err != nil {
		log.With(zap.Error(err)).Warn("Failure checking chat membership")
		return false, status.Error(codes.Internal, "")
	}
	return isMember, nil
}

func (s *Server) messageExists(ctx context.Context, log *zap.Logger, chatID *commonpb.ChatId, messageID *messagingpb.MessageId) (bool, error) {
	exists, err := s.messages.MessageExists(ctx, chatID, messageID)
	if err != nil {
		log.With(zap.Error(err)).Warn("Failure checking message existence")
		return false, status.Error(codes.Internal, "")
	}
	return exists, nil
}
