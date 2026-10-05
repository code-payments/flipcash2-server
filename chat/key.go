package chat

import (
	"context"
	"errors"

	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	chatpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/chat/v1"
	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"

	"github.com/code-payments/flipcash2-server/model"
)

// Key envelopes: a member's own copy of a private group's chat key (see
// KeyEnvelope), stored and fetched by that member.
//
// Both RPCs are a member's alone, and only of a private group. They are gated
// like the mute RPCs (see mutingAllowed): the canonical record first, since
// its absence is the only thing that tells NOT_FOUND from DENIED, then that
// the group is private, then membership, read strongly consistent. A DM is
// DENIED before anything is read.
//
// SetKeyEnvelope stores the envelope as one the caller wrapped for themself,
// whatever they hold already. It is how a group gets its key: the creator
// stores theirs as the second step of creating the group, and a private group
// has a key exactly when they have (see KeyEnvelope). It is also how an
// admitted member replaces the envelope the creator wrapped for them with one
// that depends on no one else's key. In both cases the first envelope a
// caller stores for themself stands (see Store.SetKeyEnvelope): a later call
// with a different one changes nothing and is ALREADY_SET, so two of a
// creator's devices setting up the same new group cannot end up holding
// different keys, and a repeat of the stored envelope is OK, so a retry after
// a lost response is.
//
// The server cannot tell a good envelope from a bad one. A caller who stores
// one that does not hold the group's key has locked only themself out, since
// an envelope is read back by its own user alone; for the creator of a group
// with no key yet, whatever they store is the group's key.
//
// Nothing is published by either: a user's other devices fetch the envelope
// when they need it.
//
// Storing the creator's envelope is what opens the group to its members'
// messages (see Access.SpeakerStanding); nothing is published for that
// either, since the creator's client is the one that stored it. The lobby its
// members are admitted from is not built (see Chat.IsPrivate), so today the
// only envelope a group holds is its creator's.

func (s *Server) SetKeyEnvelope(ctx context.Context, req *chatpb.SetKeyEnvelopeRequest) (*chatpb.SetKeyEnvelopeResponse, error) {
	userID, err := s.authz.Authorize(ctx, req, &req.Auth)
	if err != nil {
		return nil, err
	}

	log := s.log.With(
		zap.String("user_id", model.UserIDString(userID)),
		zap.String("chat_id", model.ChatIDString(req.ChatId)),
	)

	allowed, err := s.keyEnvelopeAllowed(ctx, log, req.ChatId, userID)
	if err != nil {
		return nil, err
	}
	switch allowed {
	case chatpb.GetKeyEnvelopeResponse_NOT_FOUND:
		return &chatpb.SetKeyEnvelopeResponse{Result: chatpb.SetKeyEnvelopeResponse_NOT_FOUND}, nil
	case chatpb.GetKeyEnvelopeResponse_DENIED:
		return &chatpb.SetKeyEnvelopeResponse{Result: chatpb.SetKeyEnvelopeResponse_DENIED}, nil
	}

	envelope := KeyEnvelopeFromProto(req.KeyEnvelope, userID)
	stored, err := s.chats.SetKeyEnvelope(ctx, req.ChatId, userID, envelope)
	if err != nil {
		log.With(zap.Error(err)).Warn("Failure storing key envelope")
		return nil, status.Error(codes.Internal, "")
	}
	if !stored.Equal(envelope) {
		return &chatpb.SetKeyEnvelopeResponse{Result: chatpb.SetKeyEnvelopeResponse_ALREADY_SET}, nil
	}
	return &chatpb.SetKeyEnvelopeResponse{Result: chatpb.SetKeyEnvelopeResponse_OK}, nil
}

func (s *Server) GetKeyEnvelope(ctx context.Context, req *chatpb.GetKeyEnvelopeRequest) (*chatpb.GetKeyEnvelopeResponse, error) {
	userID, err := s.authz.Authorize(ctx, req, &req.Auth)
	if err != nil {
		return nil, err
	}

	log := s.log.With(
		zap.String("user_id", model.UserIDString(userID)),
		zap.String("chat_id", model.ChatIDString(req.ChatId)),
	)

	allowed, err := s.keyEnvelopeAllowed(ctx, log, req.ChatId, userID)
	if err != nil {
		return nil, err
	}
	if allowed != chatpb.GetKeyEnvelopeResponse_OK {
		return &chatpb.GetKeyEnvelopeResponse{Result: allowed}, nil
	}

	envelope, err := s.chats.GetKeyEnvelope(ctx, req.ChatId, userID)
	switch {
	case errors.Is(err, ErrKeyEnvelopeNotFound):
		return &chatpb.GetKeyEnvelopeResponse{Result: chatpb.GetKeyEnvelopeResponse_NO_ENVELOPE}, nil
	case err != nil:
		log.With(zap.Error(err)).Warn("Failure getting key envelope")
		return nil, status.Error(codes.Internal, "")
	}
	return &chatpb.GetKeyEnvelopeResponse{
		Result:      chatpb.GetKeyEnvelopeResponse_OK,
		KeyEnvelope: envelope.ToProto(),
		WrappedBy:   envelope.WrappedBy,
	}, nil
}

// keyEnvelopeAllowed is the gate SetKeyEnvelope and GetKeyEnvelope share,
// reported in GetKeyEnvelope's result vocabulary (Set's is identical for
// these): NOT_FOUND for a chat that does not exist, DENIED for a chat that is
// not a private group or a caller who is not a member of it, OK otherwise. A
// gRPC error is returned only for a failed read.
func (s *Server) keyEnvelopeAllowed(ctx context.Context, log *zap.Logger, chatID *commonpb.ChatId, userID *commonpb.UserId) (chatpb.GetKeyEnvelopeResponse_Result, error) {
	if !IsGroupChatID(chatID) {
		return chatpb.GetKeyEnvelopeResponse_DENIED, nil
	}

	c, err := s.chats.GetChatByID(ctx, chatID)
	switch {
	case errors.Is(err, ErrChatNotFound):
		return chatpb.GetKeyEnvelopeResponse_NOT_FOUND, nil
	case err != nil:
		log.With(zap.Error(err)).Warn("Failure getting chat")
		return 0, status.Error(codes.Internal, "")
	}
	if !c.IsPrivate {
		return chatpb.GetKeyEnvelopeResponse_DENIED, nil
	}

	isMember, err := s.access.IsMemberWithChat(ctx, c, userID)
	if err != nil {
		log.With(zap.Error(err)).Warn("Failure checking chat membership")
		return 0, status.Error(codes.Internal, "")
	}
	if !isMember {
		return chatpb.GetKeyEnvelopeResponse_DENIED, nil
	}
	return chatpb.GetKeyEnvelopeResponse_OK, nil
}
