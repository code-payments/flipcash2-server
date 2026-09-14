package messaging

import (
	"context"

	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"
	messagingpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/messaging/v1"
)

// canListen reports whether userID may read chatID: they are a member. It is
// the gate on every read path.
//
// Membership alone is checked, not the chat's listener rules, even though a
// listener rule is what admits a member in the first place. Reads are the hot
// path, and a listener rule can be costly to evaluate (a minimum balance is a
// valuation by the OCP server), so evaluating rules on every read would put
// that cost on every message fetched. Instead the membership record stands for
// the rules: a member is someone the listener rules admitted, and remains
// admitted until they are removed.
//
// TODO: The intended design is for membership to track the rules — when a
// member stops satisfying a listener rule (their balance drops below the
// requirement, their staff flag is revoked), whatever detects the transition
// removes them from the group, so the membership record is always current and
// the read path stays a single cheap check. Until that exists, a member who no
// longer satisfies a listener rule keeps reading; they are only denied on the
// send paths, where the rules are evaluated (see canSpeak).
func (s *Server) canListen(ctx context.Context, log *zap.Logger, chatID *commonpb.ChatId, userID *commonpb.UserId) (bool, error) {
	return s.isMember(ctx, log, chatID, userID)
}

// canSpeak reports whether userID may send in chatID: they are a member, and
// they satisfy the chat's listener and speaker rules. It is the gate on every
// path that produces something other members see — a message, an edit, a
// deletion, a typing notification.
//
// Membership is checked first — it is the cheaper check (cached for a DM), and
// it keeps a chat's rules from being evaluated on behalf of a non-member. A
// group's rules are served by the chat store, which caches them, so in steady
// state the rules themselves cost no store read on top of the membership check;
// what a rule costs is its evaluation.
func (s *Server) canSpeak(ctx context.Context, log *zap.Logger, chatID *commonpb.ChatId, userID *commonpb.UserId) (bool, error) {
	if isMember, err := s.isMember(ctx, log, chatID, userID); err != nil || !isMember {
		return false, err
	}

	// Membership just confirmed the chat exists, so a not-found from the
	// evaluator is not a case this path distinguishes from any other failure.
	ok, err := s.rules.CanSpeak(ctx, chatID, userID)
	if err != nil {
		log.With(zap.Error(err)).Warn("Failure evaluating chat rules")
		return false, status.Error(codes.Internal, "")
	}
	return ok, nil
}

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
