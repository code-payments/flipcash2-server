package chat

import (
	"crypto/ed25519"
	"crypto/sha256"
	"fmt"
	"time"

	chatpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/chat/v1"
	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"

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
