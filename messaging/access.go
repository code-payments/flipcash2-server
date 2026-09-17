package messaging

import (
	"context"

	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"
	messagingpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/messaging/v1"

	"github.com/code-payments/flipcash2-server/chat"
)

// The gates every Messaging RPC passes through before touching a chat, each a
// thin projection of the chat domain's Access (see chat.Access for the
// definitions and the reasoning) onto this server's logging and error
// conventions. Which RPC sits behind which gate:
//
//   - reading: every read that returns messages — GetMessage, GetMessages,
//     GetDelta. It answers not just whether the caller may read but how the
//     read is answered, full or redacted (see chat.Standing.Reading), from the
//     caller's standing and the ViewMode they asked for. A member always
//     reads in full; a non-member reads a group in full if they satisfy its
//     listener rules, so a qualifying user can preview a group before joining,
//     and redacted otherwise if the group carries a listener rule at all, so
//     any registered user can see a gated group's shape (see present).
//   - canListen: every other read — the reaction summaries and reactor lists,
//     which carry no ViewMode and are a full reader's alone (see
//     redact.Message on reactions).
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

func (s *Server) reading(ctx context.Context, log *zap.Logger, chatID *commonpb.ChatId, userID *commonpb.UserId, mode messagingpb.ViewMode) (chat.Reading, error) {
	standing, err := s.access.Standing(ctx, chatID, userID, mode)
	if err != nil {
		log.With(zap.Error(err)).Warn("Failure determining chat standing")
		return chat.ReadingDenied, status.Error(codes.Internal, "")
	}
	return standing.Reading(mode), nil
}

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
