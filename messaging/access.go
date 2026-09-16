package messaging

import (
	"context"

	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"
)

// The three gates every Messaging RPC passes through before touching a chat,
// each a thin projection of the chat domain's Access (see chat.Access for the
// definitions and the reasoning) onto this server's logging and error
// conventions. Which RPC sits behind which gate:
//
//   - canListen: every read — GetMessage, GetMessages, GetDelta, the reaction
//     summaries and reactor lists. A member always passes; a non-member passes
//     for a group whose listener rules they satisfy, so a qualifying user can
//     preview a group before joining.
//   - isMember: every write that is not a send but is still a member's alone.
//     A pointer advance leaves a per-user item in the chat's partition, and a
//     reaction is something other members see, so neither is open to a
//     non-member however well they satisfy the rules.
//   - canSpeak: every send — a message, an edit, a deletion, a typing
//     notification. Members who satisfy the listener and speaker rules.
//
// A member's read is answered on membership alone, without evaluating a rule,
// even though a listener rule is what admitted them: reads are the hot path,
// and a listener rule can be costly to evaluate (a minimum balance is a
// valuation by the OCP server). The membership record stands for the rules on
// a member — someone the rules admitted, who remains admitted until removed.
// The intended design is for membership to track the rules: when a member
// stops satisfying a listener rule (their balance drops, their staff flag is
// revoked), whatever detects the transition removes them from the group, so
// the record is always current. Until that exists, a member who no longer
// satisfies a listener rule keeps reading, and is denied only on the send
// paths, where the rules are evaluated. A non-member's read does evaluate the
// rules, since nothing else can admit them; Access bounds what that costs.

func (s *Server) canListen(ctx context.Context, log *zap.Logger, chatID *commonpb.ChatId, userID *commonpb.UserId) (bool, error) {
	ok, err := s.access.CanListen(ctx, chatID, userID)
	if err != nil {
		log.With(zap.Error(err)).Warn("Failure checking chat listen access")
		return false, status.Error(codes.Internal, "")
	}
	return ok, nil
}

func (s *Server) isMember(ctx context.Context, log *zap.Logger, chatID *commonpb.ChatId, userID *commonpb.UserId) (bool, error) {
	ok, err := s.access.IsMember(ctx, chatID, userID)
	if err != nil {
		log.With(zap.Error(err)).Warn("Failure checking chat membership")
		return false, status.Error(codes.Internal, "")
	}
	return ok, nil
}

func (s *Server) canSpeak(ctx context.Context, log *zap.Logger, chatID *commonpb.ChatId, userID *commonpb.UserId) (bool, error) {
	ok, err := s.access.CanSpeak(ctx, chatID, userID)
	if err != nil {
		log.With(zap.Error(err)).Warn("Failure checking chat speak access")
		return false, status.Error(codes.Internal, "")
	}
	return ok, nil
}
