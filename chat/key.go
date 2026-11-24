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
// like the mute RPCs (see memberSettingGate): the canonical record first, since
// its absence is the only thing that tells NOT_FOUND from DENIED, then that
// the group is private, then membership, read strongly consistent. A DM is
// DENIED before anything is read.
//
// SetKeyEnvelope stores the envelope as one the caller wrapped for themself,
// whatever they hold already. It is how a group gets its key: the creator
// stores theirs as the second step of creating the group, and a group created
// that way has a key exactly when they have (see KeyEnvelope). It is also how an
// admitted member replaces the envelope the creator wrapped for them with one
// that depends on no one else's key, and how a member who joined with an
// invite replaces the invite's copy of the chat key they hold (see
// InviteKeyCopy): the copy and the secret envelope stored with it go in the
// same write, since together they are one stored envelope the caller did not
// wrap, replaced as one the creator wrapped for them is. In every case the
// first envelope a caller stores for themself stands (see
// Store.SetKeyEnvelope): a later call with a different one changes nothing
// and is ALREADY_SET, so two of a creator's devices setting up the same new
// group cannot end up holding different keys, and a repeat of the stored
// envelope is OK, so a retry after a lost response is.
//
// The server cannot tell a good envelope from a bad one. A caller who stores
// one that does not hold the group's key has locked only themself out, since
// an envelope is read back by its own user alone; for the creator of a group
// with no key yet, whatever they store is the group's key.
//
// GetKeyEnvelope returns what is stored for the caller: an envelope someone
// wrapped as key_envelope, and an invite's copy as invite_key_bundle, the
// copy with the caller's secret envelope (see InviteKeyCopy.ToProto); exactly
// one of the two. Either way wrapped_by is who stored it (for a copy, the
// invite's creator, who signed it) and wrapped_by_public_key that user's
// account key, which a copy's signature is checked against.
//
// Nothing is published by either: a user's other devices fetch the envelope
// when they need it.
//
// For a group created through StartChat, storing the creator's envelope is
// what opens the group to its members' messages and its lobby to those who
// would join (see Access.SpeakerStanding and Server.EnterLobby); nothing is
// published for that either, since the creator's client is the one that
// stored it. A group the team account created for a user is open to its
// members' messages from its creation instead, having been written with their
// envelopes (see Store.PutPrivateGroup), and never has a lobby; the team has
// no client and stores no envelope of its own (see Access.hasKey). Every
// other envelope a group holds was stored by the creator admitting that
// member (see Server.AdmitLobbyMember), written with the group at its
// creation, or is an invite's copy stored by the member's join with it (see
// Server.JoinChat and Store.JoinWithInvite), until the member replaces it
// with their own.

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

	// The wrapper's public key is the account's, as for a lobby member (see
	// hydrateLobbyMembers): it is what the client opens the envelope against,
	// or checks an invite copy's signature against, when it has learned none
	// for the wrapper outside this service.
	keys, err := s.accounts.GetPubKeys(ctx, envelope.WrappedBy)
	if err != nil {
		log.With(zap.Error(err)).Warn("Failure getting key envelope wrapper's public key")
		return nil, status.Error(codes.Internal, "")
	}
	if len(keys) == 0 {
		log.With(zap.String("wrapped_by", model.UserIDString(envelope.WrappedBy))).Warn("Key envelope wrapper has no public key")
		return nil, status.Error(codes.Internal, "")
	}

	resp := &chatpb.GetKeyEnvelopeResponse{
		Result:             chatpb.GetKeyEnvelopeResponse_OK,
		WrappedBy:          envelope.WrappedBy,
		WrappedByPublicKey: keys[0],
	}
	if envelope.IsInviteCopy() {
		resp.Envelope = &chatpb.GetKeyEnvelopeResponse_InviteKeyBundle_{InviteKeyBundle: envelope.InviteCopy.ToProto()}
	} else {
		resp.Envelope = &chatpb.GetKeyEnvelopeResponse_KeyEnvelope{KeyEnvelope: envelope.ToProto()}
	}
	return resp, nil
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

// MemberKeyEnvelope is a key envelope written with the private group it is
// for, at the group's creation (see Store.PutPrivateGroup): Envelope is
// UserID's envelope, wrapped by the group's creator.
type MemberKeyEnvelope struct {
	UserID   *commonpb.UserId
	Envelope KeyEnvelope
}

// ValidatePrivateGroupCreation checks a private group and the envelopes it is
// to be created with, as Store.PutPrivateGroup takes them, for a store whose
// feed exclusions are exclusions (see FeedExclusions), so that every backend
// refuses the same creations: c must be a group (a GroupChatIDSize ID, type GROUP) that
// is private and names its creator, with at least one member (ErrNoMembers
// otherwise); each envelope must be for one of c's members, at most one per
// member, and be one the creator wrapped (WrappedBy is c.CreatorID) rather
// than an invite's copy, since at creation no one else has wrapped anything
// and no invite exists; and c's distinct members and the envelopes together
// must number at most MaxGroupChatCreationMembers (ErrTooManyMembers
// otherwise), since every one is an item of the same atomic write.
//
// The creator's own envelope is required, unless exclusions name the
// creator, and then refused. A group is keyed by its creator's envelope (see
// Access.hasKey), so one created without it would be keyless, and nothing
// could be said in it until the creator stored one: a creation through the
// store is the server making a group ready to use, which StartChat's
// create-then-SetKeyEnvelope is not. The creator must therefore be a member,
// since an envelope is only ever for a member. A creator the store excludes,
// in production exactly the team account, is the exception: its groups are
// keyed by construction (see Access.teamCreated), it has no client to open
// an envelope with, and envelopes are keyed by user, so storing one for each
// group it creates would put every one under the same hot partition, which
// is what excluding it is for.
//
// Every failure but ErrNoMembers and ErrTooManyMembers is a plain error: each
// is a caller's bug, not a state a client can reach.
func ValidatePrivateGroupCreation(c *Chat, envelopes []MemberKeyEnvelope, exclusions FeedExclusions) error {
	if !IsGroupChatID(c.ID) || c.Type != chatpb.ChatType_GROUP {
		return errors.New("not a group chat")
	}
	if !c.IsPrivate {
		return errors.New("not a private group")
	}
	if c.CreatorID == nil {
		return errors.New("private group names no creator")
	}

	var members []*commonpb.UserId
	for _, member := range c.Members {
		if !containsUser(members, member) {
			members = append(members, member)
		}
	}
	if len(members) == 0 {
		return ErrNoMembers
	}

	var enveloped []*commonpb.UserId
	for _, e := range envelopes {
		switch {
		case e.UserID == nil:
			return errors.New("key envelope names no user")
		case !containsUser(members, e.UserID):
			return errors.New("key envelope for a user who is not a member")
		case containsUser(enveloped, e.UserID):
			return errors.New("more than one key envelope for a member")
		case e.Envelope.IsInviteCopy():
			return errors.New("key envelope is an invite's copy")
		case !e.Envelope.IsWrappedBy(c.CreatorID):
			return errors.New("key envelope not wrapped by the creator")
		}
		enveloped = append(enveloped, e.UserID)
	}

	creatorEnveloped := containsUser(enveloped, c.CreatorID)
	switch excluded := exclusions.Excludes(c.CreatorID); {
	case excluded && creatorEnveloped:
		return errors.New("key envelope for a creator the store excludes")
	case !excluded && !creatorEnveloped:
		return errors.New("no key envelope of the creator's own")
	}

	if len(members)+len(envelopes) > MaxGroupChatCreationMembers {
		return ErrTooManyMembers
	}
	return nil
}
