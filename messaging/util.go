package messaging

import (
	"context"

	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"
	messagingpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/messaging/v1"
)

// canListen reports whether userID may read chatID: they are a member, and
// they satisfy the chat's listener rules. It is the gate on every read path.
//
// Membership is checked first — it is the cheaper check (cached for a DM), and
// it keeps a chat's rules from being evaluated on behalf of a non-member. A
// group's rules are served by the chat store, which caches them, so in steady
// state the gate costs no store read on top of the membership check.
func (s *Server) canListen(ctx context.Context, log *zap.Logger, chatID *commonpb.ChatId, userID *commonpb.UserId) (bool, error) {
	return s.canParticipate(ctx, log, chatID, userID, s.rules.CanListen)
}

// canSpeak reports whether userID may send in chatID: they are a member, and
// they satisfy the chat's listener and speaker rules. It is the gate on every
// path that produces something other members see — a message, an edit, a
// deletion, a reaction, a typing notification.
func (s *Server) canSpeak(ctx context.Context, log *zap.Logger, chatID *commonpb.ChatId, userID *commonpb.UserId) (bool, error) {
	return s.canParticipate(ctx, log, chatID, userID, s.rules.CanSpeak)
}

func (s *Server) canParticipate(
	ctx context.Context,
	log *zap.Logger,
	chatID *commonpb.ChatId,
	userID *commonpb.UserId,
	satisfiesRules func(context.Context, *commonpb.ChatId, *commonpb.UserId) (bool, error),
) (bool, error) {
	isMember, err := s.chats.IsMember(ctx, chatID, userID)
	if err != nil {
		log.With(zap.Error(err)).Warn("Failure checking chat membership")
		return false, status.Error(codes.Internal, "")
	}
	if !isMember {
		return false, nil
	}

	// Membership just confirmed the chat exists, so a not-found from the
	// evaluator is not a case this path distinguishes from any other failure.
	ok, err := satisfiesRules(ctx, chatID, userID)
	if err != nil {
		log.With(zap.Error(err)).Warn("Failure evaluating chat rules")
		return false, status.Error(codes.Internal, "")
	}
	return ok, nil
}

func (s *Server) messageExists(ctx context.Context, log *zap.Logger, chatID *commonpb.ChatId, messageID *messagingpb.MessageId) (bool, error) {
	exists, err := s.messages.MessageExists(ctx, chatID, messageID)
	if err != nil {
		log.With(zap.Error(err)).Warn("Failure checking message existence")
		return false, status.Error(codes.Internal, "")
	}
	return exists, nil
}
