package chat

import (
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

	"github.com/code-payments/flipcash2-server/model"
)

// MuteChat records the caller's mute on a chat, until a time or indefinitely.
//
// Only a member may mute: the gate is membership alone, never the rules, so a
// member a rule has since excluded can still quiet the chat they are in. A
// DM member is always a member, so DMs are muted like groups. A timed mute
// must end in the future as it will be recorded (see Mute.Normalize), which
// is checked here rather than by request validation because MuteState also
// appears in responses, where a mute that lapsed a moment before the read
// must not fail a client's validation.
//
// The write is the store's own no-op-aware update: the mute already recorded
// is answered with the current state and publishes nothing, and any real
// change — a first mute, a different end, indefinite where timed was —
// reaches the caller's other devices as a MetadataUpdate.ViewerStateChanged
// on their user topic (see publishViewerStateChanged). The response carries
// the same state, so the calling device applies it at its version without
// waiting for the stream. A mute lasts as long as the membership it was set
// under: leaving the chat clears it, best effort (see clearMuteOnLeave), and
// a departed member cannot set or clear one until they rejoin.
func (s *Server) MuteChat(ctx context.Context, req *chatpb.MuteChatRequest) (*chatpb.MuteChatResponse, error) {
	userID, err := s.authz.Authorize(ctx, req, &req.Auth)
	if err != nil {
		return nil, err
	}

	log := s.log.With(
		zap.String("user_id", model.UserIDString(userID)),
		zap.String("chat_id", model.ChatIDString(req.ChatId)),
	)

	// The end is judged as it will be recorded — truncated to the second —
	// so a mute that passes here is recorded still in force, never already
	// lapsed by the truncation.
	mute, err := MuteFromProto(req.Mute).Normalize()
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	if !mute.Forever && !mute.Until.After(time.Now()) {
		return nil, status.Error(codes.InvalidArgument, "mute must end in the future")
	}

	allowed, err := s.mutingAllowed(ctx, log, req.ChatId, userID)
	if err != nil {
		return nil, err
	}
	switch allowed {
	case chatpb.MuteChatResponse_NOT_FOUND:
		return &chatpb.MuteChatResponse{Result: chatpb.MuteChatResponse_NOT_FOUND}, nil
	case chatpb.MuteChatResponse_DENIED:
		return &chatpb.MuteChatResponse{Result: chatpb.MuteChatResponse_DENIED}, nil
	}

	state, changed, err := s.chats.SetMute(ctx, req.ChatId, userID, mute)
	if err != nil {
		if errors.Is(err, ErrMuteUntilOutOfRange) {
			return nil, status.Error(codes.InvalidArgument, err.Error())
		}
		log.With(zap.Error(err)).Warn("Failure recording mute")
		return nil, status.Error(codes.Internal, "")
	}

	if changed {
		s.publishViewerStateChanged(userID, req.ChatId, state)
	}
	return &chatpb.MuteChatResponse{
		Result:      chatpb.MuteChatResponse_OK,
		ViewerState: state.ToProto(),
	}, nil
}

// UnmuteChat clears the caller's mute on a chat, lapsed or not. It is gated
// and published exactly as MuteChat is; clearing a chat that is not muted is
// the no-op that returns the current state and publishes nothing.
func (s *Server) UnmuteChat(ctx context.Context, req *chatpb.UnmuteChatRequest) (*chatpb.UnmuteChatResponse, error) {
	userID, err := s.authz.Authorize(ctx, req, &req.Auth)
	if err != nil {
		return nil, err
	}

	log := s.log.With(
		zap.String("user_id", model.UserIDString(userID)),
		zap.String("chat_id", model.ChatIDString(req.ChatId)),
	)

	allowed, err := s.mutingAllowed(ctx, log, req.ChatId, userID)
	if err != nil {
		return nil, err
	}
	switch allowed {
	case chatpb.MuteChatResponse_NOT_FOUND:
		return &chatpb.UnmuteChatResponse{Result: chatpb.UnmuteChatResponse_NOT_FOUND}, nil
	case chatpb.MuteChatResponse_DENIED:
		return &chatpb.UnmuteChatResponse{Result: chatpb.UnmuteChatResponse_DENIED}, nil
	}

	state, changed, err := s.chats.ClearMute(ctx, req.ChatId, userID)
	if err != nil {
		log.With(zap.Error(err)).Warn("Failure clearing mute")
		return nil, status.Error(codes.Internal, "")
	}

	if changed {
		s.publishViewerStateChanged(userID, req.ChatId, state)
	}
	return &chatpb.UnmuteChatResponse{
		Result:      chatpb.UnmuteChatResponse_OK,
		ViewerState: state.ToProto(),
	}, nil
}

// mutingAllowed is the gate MuteChat and UnmuteChat share, reported in
// MuteChat's result vocabulary (Unmute's is identical): NOT_FOUND for a chat
// that does not exist, DENIED for a non-member, OK otherwise. The canonical
// record is read first because a membership check alone cannot tell the two
// failures apart, and then decides what it can: a DM's membership is on it,
// so a DM is gated on that one read; a group's is read from the store,
// strongly consistent, as every gate reads it (see Access.IsMemberWithChat)
// — the first thing a user does after joining may be to mute. The rules are
// never evaluated: membership alone is the gate. A gRPC error is returned
// only for a failed read.
func (s *Server) mutingAllowed(ctx context.Context, log *zap.Logger, chatID *commonpb.ChatId, userID *commonpb.UserId) (chatpb.MuteChatResponse_Result, error) {
	c, err := s.chats.GetChatByID(ctx, chatID)
	if err != nil {
		if errors.Is(err, ErrChatNotFound) {
			return chatpb.MuteChatResponse_NOT_FOUND, nil
		}
		log.With(zap.Error(err)).Warn("Failure getting chat")
		return 0, status.Error(codes.Internal, "")
	}

	isMember, err := s.access.IsMemberWithChat(ctx, c, userID)
	if err != nil {
		log.With(zap.Error(err)).Warn("Failure checking chat membership")
		return 0, status.Error(codes.Internal, "")
	}
	if !isMember {
		return chatpb.MuteChatResponse_DENIED, nil
	}
	return chatpb.MuteChatResponse_OK, nil
}

// publishViewerStateChanged sends the user's new state on chatID to their own
// devices as a MetadataUpdate.ViewerStateChanged on their user topic. Only the
// user topic: the state is theirs alone and never reaches other members, so
// the chat topic stays silent. It is best-effort and non-blocking, like every
// publish here, and never fails the RPC whose change it announces; a device
// that misses it converges from the next Metadata it reads, which carries the
// same state at its version — exactly the same, lapsed mute included, since
// the projection never depends on when it was made (see ViewerState.ToProto).
func (s *Server) publishViewerStateChanged(userID *commonpb.UserId, chatID *commonpb.ChatId, state ViewerState) {
	s.userEventBus.OnEvent(userID, &eventpb.Event{
		Id: model.MustGenerateEventID(),
		Ts: timestamppb.Now(),
		Type: &eventpb.Event_ChatUpdate{ChatUpdate: &eventpb.ChatUpdate{
			Chat: chatID,
			MetadataUpdates: []*chatpb.MetadataUpdate{{
				Kind: &chatpb.MetadataUpdate_ViewerStateChanged_{
					ViewerStateChanged: &chatpb.MetadataUpdate_ViewerStateChanged{
						ViewerState: state.ToProto(),
					},
				},
			}},
		}},
	})
}
