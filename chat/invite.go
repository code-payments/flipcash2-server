package chat

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"time"

	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	chatpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/chat/v1"
	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"
	messagingpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/messaging/v1"

	"github.com/code-payments/flipcash2-server/model"
)

// Invites: links that let users join a private group without its creator's
// approval (see Invite and chatpb.Invite).
//
// The link carries one secret, which the server never learns. Clients derive
// three values from it (see chatpb.Invite): the token, which they send to
// prove they hold the link; the wrap key, which opens the invite's copy of the
// chat key and is never sent; and the invite's ID, which names it. The server
// sees only the token, and computes the ID from it whenever one is sent
// (MustDeriveInviteID), so it never stores a token, and nothing it stores or
// returns lets anyone join. The token is never logged either: anyone who reads
// one can join with the invite and use up its limit.
//
// The server cannot open the invite's copy of the chat key, but it checks the
// copy's signature when the invite is created (VerifyInviteSignature), since
// no joiner could accept a copy whose signature does not verify against its
// creator's key.
//
// The RPCs. Who may invite is Chat.PermissionsFor's CanInvite, the same flag
// every ViewerState carries as can_invite: today a private group's recorded
// creator, while a member. GetChatInvites and RevokeInvite, and CreateInvite
// once its retry lookup has passed, share one gate (inviterChat): a DM ID is
// DENIED before any read, the canonical record is read for NOT_FOUND, then
// CanInvite is asked off the record as if the caller were a member, so a
// caller who could never invite (a public group, anyone but the creator)
// costs no membership read, and only then membership, strongly consistent as
// every gate reads it (a departed creator is DENIED until they rejoin). An
// invite the creator made keeps admitting users while they are away; they
// just cannot list or revoke it until they are back (see chatpb.Invite).
//
//   - CreateInvite derives the invite's ID from the request's token and looks
//     it up before anything else: an invite with that ID by the caller to the
//     chat is the retry the proto promises, OK with that invite whatever its
//     status and whatever the rest of the request says, with no gate asked;
//     an invite with that ID by anyone else or to any other chat is DENIED.
//     Otherwise, while invites are being rolled out, only a staff user may
//     create one (DENIED otherwise, see canCreateInvite; a retry is answered
//     whatever the caller's staff flag says now); then the shared gate; then
//     the group must have its key
//     (Access.hasKey, DENIED otherwise: an invite to a keyless group would
//     carry a copy of a key that does not exist); then expires_at, if set,
//     must be after now and at most MaxInviteDuration away
//     (INVALID_EXPIRES_AT); then the copy's signature must verify against
//     the caller's account key for this chat and ID (INVALID_SIGNATURE);
//     then the store's write, which refuses a chat at its cap of ACTIVE
//     invites (TOO_MANY_INVITES; maxActiveInvites, MaxActiveInvites in
//     production) and, when a concurrent create of the same ID won the race,
//     returns that invite, judged by the same retry rule.
//   - GetInvite is any registered user's: the invite for the token
//     (NOT_FOUND), its chat's record (NOT_FOUND, which a stored invite never
//     outlives), BANNED for a user banned from the chat, and otherwise the
//     chat exactly as GetChat shows it to the caller under FULL (see
//     chatAsSeenBy): a member's metadata in full, a non-member the record
//     with in_lobby. With it goes the invite's status as of now, and nothing
//     else of the invite: no use count, no key, nothing written.
//   - GetChatInvites pages the chat's invites, every creator's and in every
//     status, most recently created first (see InvitePosition), up to
//     maxGetChatInvitesPageSize a page, with an exact has_more and a token
//     bound to the chat (see encodeInviteToken). It reads an index that may
//     trail writes by a moment (see Store.GetChatInvitesPage), so an invite
//     just created may be missing from it briefly, and one just revoked or
//     used may show its earlier status and count; what CreateInvite and
//     RevokeInvite return is current.
//   - RevokeInvite revokes the chat's invite with the given ID (NOT_FOUND
//     when no invite to this chat has it, another chat's included), deleting
//     the invite's copy of the chat key in the same write; a repeat is OK.
//
// Nothing here is published: an invite is the inviter's, and its effects
// (joins) are announced where they happen.

// inviteIDDomain separates an invite ID's digest from every other SHA-256 in
// the protocol, as chatpb.Invite specifies.
const inviteIDDomain = "flipcash-group-invite-id-v1"

// inviteSignatureDomain separates an invite copy's signed message from every
// other Ed25519 signature made with an account key, as
// chatpb.InviteKeyEnvelope specifies.
const inviteSignatureDomain = "flipcash-group-invite-sig-v1"

// MustDeriveInviteID is the ID of the invite whose link the token was derived
// from: the leading InviteIDSize bytes of SHA-256 over inviteIDDomain and the
// token, as chatpb.Invite specifies, so client and server compute the same
// one. It panics if the token is not InviteTokenSize bytes, which would be a
// programming error: a request's token is checked to width before it reaches
// here.
func MustDeriveInviteID(token *chatpb.InviteToken) *chatpb.InviteId {
	if len(token.GetValue()) != InviteTokenSize {
		panic(fmt.Sprintf("invite token must be %d bytes, got %d", InviteTokenSize, len(token.GetValue())))
	}

	h := sha256.New()
	h.Write([]byte(inviteIDDomain))
	h.Write(token.Value)
	return &chatpb.InviteId{Value: h.Sum(nil)[:InviteIDSize]}
}

// VerifyInviteSignature reports whether envelope, an invite's copy of chatID's
// chat key, carries a valid signature by publicKey for the invite inviteID:
// an Ed25519 signature over inviteSignatureDomain, the chat ID, the invite ID,
// the nonce and the ciphertext, as chatpb.InviteKeyEnvelope specifies. Every
// part is of fixed width (the proto checks the nonce's and ciphertext's, and
// the IDs are a group's and an invite's), so the concatenation is
// unambiguous. A public key that is not an Ed25519 key never verifies.
func VerifyInviteSignature(publicKey *commonpb.PublicKey, chatID *commonpb.ChatId, inviteID *chatpb.InviteId, envelope *chatpb.InviteKeyEnvelope) bool {
	if len(publicKey.GetValue()) != ed25519.PublicKeySize {
		return false
	}
	return ed25519.Verify(publicKey.Value, inviteSignatureMessage(chatID, inviteID, envelope), envelope.GetSignature())
}

// inviteSignatureMessage is the message an invite copy's signature is over
// (see VerifyInviteSignature).
func inviteSignatureMessage(chatID *commonpb.ChatId, inviteID *chatpb.InviteId, envelope *chatpb.InviteKeyEnvelope) []byte {
	var m []byte
	m = append(m, inviteSignatureDomain...)
	m = append(m, chatID.GetValue()...)
	m = append(m, inviteID.GetValue()...)
	m = append(m, envelope.GetNonce()...)
	m = append(m, envelope.GetCiphertext()...)
	return m
}

// ValidateNewInvite reports whether invite and keyCopy have the shape
// Store.CreateInvite stores: an invite to a group chat, named by an ID of
// InviteIDSize bytes, made by a user, carrying its secret envelope and its
// copy of the chat key, not yet used or revoked, and created and expiring at
// or after the Unix epoch, which every stored instant is encoded from. It
// judges the shape alone: who may invite, the copy's signature and how far
// away the expiry may be are the caller's.
func ValidateNewInvite(invite Invite, keyCopy *chatpb.InviteKeyEnvelope) error {
	switch {
	case !IsGroupChatID(invite.ChatID):
		return fmt.Errorf("invite: not a group chat id")
	case len(invite.ID.GetValue()) != InviteIDSize:
		return fmt.Errorf("invite: id must be %d bytes", InviteIDSize)
	case len(invite.CreatedBy.GetValue()) != model.UserIDSize:
		return fmt.Errorf("invite: creator must be a user id")
	case invite.SecretEnvelope == nil:
		return fmt.Errorf("invite: secret envelope is required")
	case keyCopy == nil:
		return fmt.Errorf("invite: key copy is required")
	case invite.UseCount != 0:
		return fmt.Errorf("invite: a new invite has no uses")
	case invite.RevokedAt != nil:
		return fmt.Errorf("invite: a new invite is not revoked")
	case invite.CreatedAt.Before(time.Unix(0, 0)):
		return fmt.Errorf("invite: created before the unix epoch")
	case invite.ExpiresAt != nil && invite.ExpiresAt.Before(time.Unix(0, 0)):
		return fmt.Errorf("invite: expires before the unix epoch")
	}
	return nil
}

// ValidateInviteJoin reports whether the arguments of a join with an invite
// (see Store.JoinWithInvite) have the shape a store writes: a group chat, a
// user, an invite ID of InviteIDSize bytes, the joiner's secret envelope, and
// a now at or after the Unix epoch, which the invite's instants are compared
// with as Unix nanoseconds.
func ValidateInviteJoin(chatID *commonpb.ChatId, userID *commonpb.UserId, inviteID *chatpb.InviteId, secretEnvelope *chatpb.InviteSecretEnvelope, now time.Time) error {
	switch {
	case !IsGroupChatID(chatID):
		return fmt.Errorf("invite join: not a group chat id")
	case len(userID.GetValue()) != model.UserIDSize:
		return fmt.Errorf("invite join: not a user id")
	case len(inviteID.GetValue()) != InviteIDSize:
		return fmt.Errorf("invite join: invite id must be %d bytes", InviteIDSize)
	case secretEnvelope == nil:
		return fmt.Errorf("invite join: secret envelope is required")
	case now.Before(time.Unix(0, 0)):
		return fmt.Errorf("invite join: now is before the unix epoch")
	}
	return nil
}

// ValidateInvitePosition reports whether pos is a place a store can resume a
// listing of invites from (see Store.GetChatInvitesPage): an invite ID of
// InviteIDSize bytes, created at or after the Unix epoch.
func ValidateInvitePosition(pos InvitePosition) error {
	switch {
	case len(pos.InviteID.GetValue()) != InviteIDSize:
		return fmt.Errorf("invite position: id must be %d bytes", InviteIDSize)
	case pos.CreatedAt.Before(time.Unix(0, 0)):
		return fmt.Errorf("invite position: created before the unix epoch")
	}
	return nil
}

// InviteTime is t as a store records an invite's instants: at nanosecond
// precision, in UTC, with no monotonic reading, which is what a read gives
// back.
func InviteTime(t time.Time) time.Time {
	return time.Unix(0, t.UnixNano()).UTC()
}

const (
	// maxGetChatInvitesPageSize bounds a single GetChatInvites page and is
	// its default.
	maxGetChatInvitesPageSize = 100

	inviteTokenVersion = 1

	// inviteTokenLen is the fixed size of a GetChatInvites paging token: the
	// version byte, the chat ID, the creation time and the invite ID (see
	// encodeInviteToken).
	inviteTokenLen = 1 + GroupChatIDSize + 8 + InviteIDSize
)

func (s *Server) CreateInvite(ctx context.Context, req *chatpb.CreateInviteRequest) (*chatpb.CreateInviteResponse, error) {
	userID, err := s.authz.Authorize(ctx, req, &req.Auth)
	if err != nil {
		return nil, err
	}

	inviteID := MustDeriveInviteID(req.InviteToken)
	log := s.inviteLog(userID, req.ChatId, inviteID)
	now := time.Now()

	// An existing invite with this ID is looked up before anything else is
	// checked (see above).
	existing, err := s.chats.GetInvite(ctx, inviteID)
	switch {
	case err == nil:
		return createInviteRetry(existing, req.ChatId, userID, now), nil
	case !errors.Is(err, ErrInviteNotFound):
		log.With(zap.Error(err)).Warn("Failure getting invite")
		return nil, status.Error(codes.Internal, "")
	}

	allowed, err := s.canCreateInvite(ctx, userID)
	if err != nil {
		log.With(zap.Error(err)).Warn("Failure checking invite creation")
		return nil, status.Error(codes.Internal, "")
	}
	if !allowed {
		return &chatpb.CreateInviteResponse{Result: chatpb.CreateInviteResponse_DENIED}, nil
	}

	c, result, err := s.inviterChat(ctx, log, req.ChatId, userID)
	if err != nil {
		return nil, err
	}
	switch result {
	case inviterChatNotFound:
		return &chatpb.CreateInviteResponse{Result: chatpb.CreateInviteResponse_NOT_FOUND}, nil
	case inviterChatDenied:
		return &chatpb.CreateInviteResponse{Result: chatpb.CreateInviteResponse_DENIED}, nil
	}

	keyed, err := s.access.hasKey(ctx, c.ID, c.ChatRules())
	if err != nil {
		log.With(zap.Error(err)).Warn("Failure checking chat key")
		return nil, status.Error(codes.Internal, "")
	}
	if !keyed {
		return &chatpb.CreateInviteResponse{Result: chatpb.CreateInviteResponse_DENIED}, nil
	}

	var expiresAt *time.Time
	if req.ExpiresAt != nil {
		if req.ExpiresAt.CheckValid() != nil {
			return &chatpb.CreateInviteResponse{Result: chatpb.CreateInviteResponse_INVALID_EXPIRES_AT}, nil
		}
		t := req.ExpiresAt.AsTime()
		if !t.After(now) || t.Sub(now) > MaxInviteDuration {
			return &chatpb.CreateInviteResponse{Result: chatpb.CreateInviteResponse_INVALID_EXPIRES_AT}, nil
		}
		expiresAt = &t
	}

	// The copy is checked against the caller's account key, as a joiner will
	// check it (see chatpb.InviteKeyEnvelope). Every registered user has one,
	// so none is a data integrity problem, as for a lobby member.
	keys, err := s.accounts.GetPubKeys(ctx, userID)
	if err != nil {
		log.With(zap.Error(err)).Warn("Failure getting inviter's public key")
		return nil, status.Error(codes.Internal, "")
	}
	if len(keys) == 0 {
		log.Warn("Inviter has no public key")
		return nil, status.Error(codes.Internal, "")
	}
	if !VerifyInviteSignature(keys[0], c.ID, inviteID, req.KeyEnvelope) {
		return &chatpb.CreateInviteResponse{Result: chatpb.CreateInviteResponse_INVALID_SIGNATURE}, nil
	}

	invite := Invite{
		ID:             inviteID,
		ChatID:         c.ID,
		CreatedBy:      userID,
		MaxUses:        req.MaxUses,
		CreatedAt:      now,
		ExpiresAt:      expiresAt,
		SecretEnvelope: req.SecretEnvelope,
	}
	stored, created, err := s.chats.CreateInvite(ctx, invite, req.KeyEnvelope, s.maxActiveInvites, now)
	switch {
	case errors.Is(err, ErrTooManyInvites):
		return &chatpb.CreateInviteResponse{Result: chatpb.CreateInviteResponse_TOO_MANY_INVITES}, nil
	case err != nil:
		log.With(zap.Error(err)).Warn("Failure creating invite")
		return nil, status.Error(codes.Internal, "")
	}
	if !created {
		// A concurrent create of the same ID landed first: a retry of this
		// one, or a clash, judged as the lookup above judges one.
		return createInviteRetry(stored, c.ID, userID, now), nil
	}

	return &chatpb.CreateInviteResponse{
		Result: chatpb.CreateInviteResponse_OK,
		Invite: stored.ToProto(now),
	}, nil
}

// canCreateInvite reports whether userID may create an invite: today, only a
// staff user. It is a transitional gate, like the one private groups were
// built behind: clients have not shipped invites. Opening creation to every
// user who may invite (Permissions.CanInvite) is removing this check.
func (s *Server) canCreateInvite(ctx context.Context, userID *commonpb.UserId) (bool, error) {
	return s.accounts.IsStaff(ctx, userID)
}

// createInviteRetry answers a CreateInvite whose ID is already held by
// existing: OK with it when the caller made it for chatID, whatever its
// status, and DENIED when anyone else did or it is an invite to another
// chat.
func createInviteRetry(existing Invite, chatID *commonpb.ChatId, userID *commonpb.UserId, now time.Time) *chatpb.CreateInviteResponse {
	if !bytes.Equal(existing.ChatID.GetValue(), chatID.GetValue()) || !bytes.Equal(existing.CreatedBy.GetValue(), userID.GetValue()) {
		return &chatpb.CreateInviteResponse{Result: chatpb.CreateInviteResponse_DENIED}
	}
	return &chatpb.CreateInviteResponse{
		Result: chatpb.CreateInviteResponse_OK,
		Invite: existing.ToProto(now),
	}
}

func (s *Server) GetInvite(ctx context.Context, req *chatpb.GetInviteRequest) (*chatpb.GetInviteResponse, error) {
	userID, err := s.authz.Authorize(ctx, req, &req.Auth)
	if err != nil {
		return nil, err
	}

	inviteID := MustDeriveInviteID(req.InviteToken)
	log := s.log.With(
		zap.String("user_id", model.UserIDString(userID)),
		zap.String("invite_id", hex.EncodeToString(inviteID.Value)),
	)

	invite, err := s.chats.GetInvite(ctx, inviteID)
	switch {
	case errors.Is(err, ErrInviteNotFound):
		return &chatpb.GetInviteResponse{Result: chatpb.GetInviteResponse_NOT_FOUND}, nil
	case err != nil:
		log.With(zap.Error(err)).Warn("Failure getting invite")
		return nil, status.Error(codes.Internal, "")
	}
	log = log.With(zap.String("chat_id", model.ChatIDString(invite.ChatID)))

	c, err := s.chats.GetChatByID(ctx, invite.ChatID)
	switch {
	case errors.Is(err, ErrChatNotFound):
		return &chatpb.GetInviteResponse{Result: chatpb.GetInviteResponse_NOT_FOUND}, nil
	case err != nil:
		log.With(zap.Error(err)).Warn("Failure getting chat")
		return nil, status.Error(codes.Internal, "")
	}

	// A banned user is told that and nothing else, not even which chat. Read
	// strongly consistent, as every gate reads a restriction.
	state, err := s.access.viewerState(ctx, c.ID, userID)
	if err != nil {
		log.With(zap.Error(err)).Warn("Failure getting viewer state")
		return nil, status.Error(codes.Internal, "")
	}
	if state.IsBanned() {
		return &chatpb.GetInviteResponse{Result: chatpb.GetInviteResponse_BANNED}, nil
	}

	// The request carries no view mode, so the chat is shown as GetChat
	// shows it under its default, FULL.
	md, ok, err := s.chatAsSeenBy(ctx, log, c, userID, messagingpb.ViewMode_FULL)
	if err != nil {
		return nil, err
	}
	if !ok {
		// Only a DM is refused, and an invite is only ever to a group (see
		// ValidateNewInvite).
		log.Warn("Invite is to a chat its caller may not see")
		return nil, status.Error(codes.Internal, "")
	}

	return &chatpb.GetInviteResponse{
		Result: chatpb.GetInviteResponse_OK,
		Chat:   md,
		Status: invite.StatusAt(time.Now()),
	}, nil
}

func (s *Server) GetChatInvites(ctx context.Context, req *chatpb.GetChatInvitesRequest) (*chatpb.GetChatInvitesResponse, error) {
	userID, err := s.authz.Authorize(ctx, req, &req.Auth)
	if err != nil {
		return nil, err
	}

	log := s.log.With(
		zap.String("user_id", model.UserIDString(userID)),
		zap.String("chat_id", model.ChatIDString(req.ChatId)),
	)

	limit := maxGetChatInvitesPageSize
	if pageSize := req.GetQueryOptions().GetPageSize(); pageSize > 0 && int(pageSize) < limit {
		limit = int(pageSize)
	}

	// A token names the chat it was minted for, and is refused elsewhere (see
	// GetRoster for the same rule).
	var after *InvitePosition
	if token := req.GetQueryOptions().GetPagingToken(); token != nil {
		pos, ok := decodeInviteToken(token, req.ChatId)
		if !ok {
			return nil, status.Error(codes.InvalidArgument, "invalid paging token")
		}
		after = &pos
	}

	c, result, err := s.inviterChat(ctx, log, req.ChatId, userID)
	if err != nil {
		return nil, err
	}
	switch result {
	case inviterChatNotFound:
		return &chatpb.GetChatInvitesResponse{Result: chatpb.GetChatInvitesResponse_NOT_FOUND}, nil
	case inviterChatDenied:
		return &chatpb.GetChatInvitesResponse{Result: chatpb.GetChatInvitesResponse_DENIED}, nil
	}

	// Every creator's invites, though today the caller made them all. One
	// more than the page, for an exact has_more.
	invites, err := s.chats.GetChatInvitesPage(ctx, c.ID, nil, after, limit+1)
	if err != nil {
		log.With(zap.Error(err)).Warn("Failure reading invites page")
		return nil, status.Error(codes.Internal, "")
	}
	hasMore := len(invites) > limit
	if hasMore {
		invites = invites[:limit]
	}

	now := time.Now()
	out := make([]*chatpb.Invite, len(invites))
	for i, invite := range invites {
		out[i] = invite.ToProto(now)
	}

	resp := &chatpb.GetChatInvitesResponse{
		Result:  chatpb.GetChatInvitesResponse_OK,
		Invites: out,
		HasMore: hasMore,
	}
	if hasMore {
		resp.PagingToken = encodeInviteToken(c.ID, invites[len(invites)-1].Position())
	}
	return resp, nil
}

func (s *Server) RevokeInvite(ctx context.Context, req *chatpb.RevokeInviteRequest) (*chatpb.RevokeInviteResponse, error) {
	userID, err := s.authz.Authorize(ctx, req, &req.Auth)
	if err != nil {
		return nil, err
	}

	log := s.inviteLog(userID, req.ChatId, req.InviteId)

	c, result, err := s.inviterChat(ctx, log, req.ChatId, userID)
	if err != nil {
		return nil, err
	}
	switch result {
	case inviterChatNotFound:
		return &chatpb.RevokeInviteResponse{Result: chatpb.RevokeInviteResponse_NOT_FOUND}, nil
	case inviterChatDenied:
		return &chatpb.RevokeInviteResponse{Result: chatpb.RevokeInviteResponse_DENIED}, nil
	}

	_, _, err = s.chats.RevokeInvite(ctx, c.ID, req.InviteId, time.Now())
	switch {
	case errors.Is(err, ErrInviteNotFound):
		return &chatpb.RevokeInviteResponse{Result: chatpb.RevokeInviteResponse_NOT_FOUND}, nil
	case err != nil:
		log.With(zap.Error(err)).Warn("Failure revoking invite")
		return nil, status.Error(codes.Internal, "")
	}
	return &chatpb.RevokeInviteResponse{Result: chatpb.RevokeInviteResponse_OK}, nil
}

// inviterChatResult is the shared gate's verdict on an invite RPC's chat.
type inviterChatResult uint8

const (
	inviterChatOK inviterChatResult = iota
	inviterChatNotFound
	inviterChatDenied
)

// inviterChat is the gate the invite RPCs share (see above): the chat is a
// group (DENIED before any read for a DM ID), its record exists (NOT_FOUND),
// and the caller may invite to it (DENIED otherwise) — asked first off the
// record as if they were a member, so a caller who could never invite costs
// no membership read, and then with their membership, strongly consistent as
// every gate reads it. On OK the record is returned for the caller's further
// gates. A gRPC error is returned only for a failed read.
func (s *Server) inviterChat(ctx context.Context, log *zap.Logger, chatID *commonpb.ChatId, userID *commonpb.UserId) (*Chat, inviterChatResult, error) {
	if !IsGroupChatID(chatID) {
		return nil, inviterChatDenied, nil
	}
	c, err := s.chats.GetChatByID(ctx, chatID)
	switch {
	case errors.Is(err, ErrChatNotFound):
		return nil, inviterChatNotFound, nil
	case err != nil:
		log.With(zap.Error(err)).Warn("Failure getting chat")
		return nil, 0, status.Error(codes.Internal, "")
	}
	if !c.PermissionsFor(userID, true).CanInvite {
		return nil, inviterChatDenied, nil
	}
	isMember, err := s.access.IsMemberWithChat(ctx, c, userID)
	if err != nil {
		log.With(zap.Error(err)).Warn("Failure checking chat membership")
		return nil, 0, status.Error(codes.Internal, "")
	}
	if !c.PermissionsFor(userID, isMember).CanInvite {
		return nil, inviterChatDenied, nil
	}
	return c, inviterChatOK, nil
}

// inviteLog is the logger an invite RPC logs with: the caller, the chat and
// the invite, by its ID. Never the token (see above).
func (s *Server) inviteLog(userID *commonpb.UserId, chatID *commonpb.ChatId, inviteID *chatpb.InviteId) *zap.Logger {
	return s.log.With(
		zap.String("user_id", model.UserIDString(userID)),
		zap.String("chat_id", model.ChatIDString(chatID)),
		zap.String("invite_id", hex.EncodeToString(inviteID.GetValue())),
	)
}

// encodeInviteToken serializes a GetChatInvites page's resume position,
// bound to the chat it pages: the version byte, the chat ID, the creation
// time as big-endian nanoseconds, and the invite ID (see encodeLobbyToken for
// the same shape).
func encodeInviteToken(chatID *commonpb.ChatId, pos InvitePosition) *commonpb.PagingToken {
	buf := make([]byte, inviteTokenLen)
	buf[0] = inviteTokenVersion
	copy(buf[1:1+GroupChatIDSize], chatID.Value)
	binary.BigEndian.PutUint64(buf[1+GroupChatIDSize:], uint64(pos.CreatedAt.UnixNano()))
	copy(buf[1+GroupChatIDSize+8:], pos.InviteID.Value)
	return &commonpb.PagingToken{Value: buf}
}

// decodeInviteToken parses a GetChatInvites token minted for chatID; a
// malformed token, or one bound to another chat, is refused. So is one whose
// time is before the Unix epoch, which no invite was created at and a store
// refuses to resume from (see ValidateInvitePosition).
func decodeInviteToken(token *commonpb.PagingToken, chatID *commonpb.ChatId) (InvitePosition, bool) {
	b := token.GetValue()
	if len(b) != inviteTokenLen || b[0] != inviteTokenVersion {
		return InvitePosition{}, false
	}
	if !bytes.Equal(b[1:1+GroupChatIDSize], chatID.GetValue()) {
		return InvitePosition{}, false
	}
	nanos := int64(binary.BigEndian.Uint64(b[1+GroupChatIDSize:]))
	if nanos < 0 {
		return InvitePosition{}, false
	}
	return InvitePosition{
		CreatedAt: time.Unix(0, nanos).UTC(),
		InviteID:  &chatpb.InviteId{Value: append([]byte(nil), b[1+GroupChatIDSize+8:]...)},
	}, true
}
