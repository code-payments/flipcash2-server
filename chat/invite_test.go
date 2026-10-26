package chat

import (
	"bytes"
	"crypto/ed25519"
	"encoding/hex"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	chatpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/chat/v1"
	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"

	"github.com/code-payments/flipcash2-server/model"
)

func TestMustDeriveInviteID(t *testing.T) {
	// A known answer, computed outside this package: SHA-256 over the domain
	// and the bytes 0x00 to 0x1f, cut to 16 bytes. Clients derive the same.
	token := &chatpb.InviteToken{Value: make([]byte, InviteTokenSize)}
	for i := range token.Value {
		token.Value[i] = byte(i)
	}
	id := MustDeriveInviteID(token)
	require.Equal(t, "919aa2e2ac29e31ff010a73364cf4b98", hex.EncodeToString(id.Value))
	require.Len(t, id.Value, InviteIDSize)

	// Deterministic, and distinct tokens derive distinct IDs.
	require.Equal(t, id.Value, MustDeriveInviteID(token).Value)
	other := &chatpb.InviteToken{Value: bytes.Repeat([]byte{1}, InviteTokenSize)}
	require.NotEqual(t, id.Value, MustDeriveInviteID(other).Value)

	// A token of the wrong width is a programming error.
	require.Panics(t, func() { MustDeriveInviteID(&chatpb.InviteToken{Value: make([]byte, InviteTokenSize-1)}) })
	require.Panics(t, func() { MustDeriveInviteID(nil) })
}

func TestVerifyInviteSignature(t *testing.T) {
	keys := model.MustGenerateKeyPair()
	chatID := MustGenerateGroupChatID()
	inviteID := &chatpb.InviteId{Value: bytes.Repeat([]byte{7}, InviteIDSize)}

	sign := func(chatID *commonpb.ChatId, inviteID *chatpb.InviteId, envelope *chatpb.InviteKeyEnvelope) *chatpb.InviteKeyEnvelope {
		var m []byte
		m = append(m, "flipcash-group-invite-sig-v1"...)
		m = append(m, chatID.Value...)
		m = append(m, inviteID.Value...)
		m = append(m, envelope.Nonce...)
		m = append(m, envelope.Ciphertext...)
		envelope.Signature = ed25519.Sign(keys.Private(), m)
		return envelope
	}
	newEnvelope := func() *chatpb.InviteKeyEnvelope {
		return sign(chatID, inviteID, &chatpb.InviteKeyEnvelope{
			Scheme:     chatpb.InviteKeyEnvelope_XCHACHA20POLY1305,
			Nonce:      bytes.Repeat([]byte{1}, 24),
			Ciphertext: bytes.Repeat([]byte{2}, 48),
		})
	}

	require.True(t, VerifyInviteSignature(keys.Proto(), chatID, inviteID, newEnvelope()))

	// Another user's key.
	require.False(t, VerifyInviteSignature(model.MustGenerateKeyPair().Proto(), chatID, inviteID, newEnvelope()))

	// Another chat or invite than the one signed for.
	require.False(t, VerifyInviteSignature(keys.Proto(), MustGenerateGroupChatID(), inviteID, newEnvelope()))
	require.False(t, VerifyInviteSignature(keys.Proto(), chatID, &chatpb.InviteId{Value: bytes.Repeat([]byte{8}, InviteIDSize)}, newEnvelope()))

	// Any byte of the nonce, ciphertext or signature changed.
	for _, tamper := range []func(e *chatpb.InviteKeyEnvelope){
		func(e *chatpb.InviteKeyEnvelope) { e.Nonce[0] ^= 1 },
		func(e *chatpb.InviteKeyEnvelope) { e.Nonce[23] ^= 1 },
		func(e *chatpb.InviteKeyEnvelope) { e.Ciphertext[0] ^= 1 },
		func(e *chatpb.InviteKeyEnvelope) { e.Ciphertext[47] ^= 1 },
		func(e *chatpb.InviteKeyEnvelope) { e.Signature[0] ^= 1 },
		func(e *chatpb.InviteKeyEnvelope) { e.Signature[63] ^= 1 },
	} {
		envelope := newEnvelope()
		tamper(envelope)
		require.False(t, VerifyInviteSignature(keys.Proto(), chatID, inviteID, envelope))
	}

	// A key or signature of the wrong shape never verifies, and never panics.
	require.False(t, VerifyInviteSignature(&commonpb.PublicKey{Value: make([]byte, 31)}, chatID, inviteID, newEnvelope()))
	require.False(t, VerifyInviteSignature(nil, chatID, inviteID, newEnvelope()))
	short := newEnvelope()
	short.Signature = short.Signature[:63]
	require.False(t, VerifyInviteSignature(keys.Proto(), chatID, inviteID, short))
}

func TestInvite_StatusAt(t *testing.T) {
	now := time.Date(2026, 10, 26, 12, 0, 0, 0, time.UTC)
	past := now.Add(-time.Second)
	at := now
	future := now.Add(time.Second)

	for _, tc := range []struct {
		name   string
		invite Invite
		want   chatpb.Invite_Status
		err    error
	}{
		{"unlimited, never expiring", Invite{UseCount: 1000}, chatpb.Invite_ACTIVE, nil},
		{"uses left", Invite{MaxUses: 3, UseCount: 2}, chatpb.Invite_ACTIVE, nil},
		{"expiring later", Invite{ExpiresAt: &future}, chatpb.Invite_ACTIVE, nil},
		{"used up", Invite{MaxUses: 3, UseCount: 3}, chatpb.Invite_EXHAUSTED, ErrInviteExhausted},
		{"expired", Invite{ExpiresAt: &past}, chatpb.Invite_EXPIRED, ErrInviteExpired},
		{"expiring at this instant", Invite{ExpiresAt: &at}, chatpb.Invite_EXPIRED, ErrInviteExpired},
		{"revoked", Invite{RevokedAt: &past}, chatpb.Invite_REVOKED, ErrInviteRevoked},

		// When more than one reason applies: REVOKED, then EXPIRED, then
		// EXHAUSTED.
		{"expired and used up", Invite{MaxUses: 1, UseCount: 1, ExpiresAt: &past}, chatpb.Invite_EXPIRED, ErrInviteExpired},
		{"revoked and used up", Invite{MaxUses: 1, UseCount: 1, RevokedAt: &past}, chatpb.Invite_REVOKED, ErrInviteRevoked},
		{"revoked and expired", Invite{ExpiresAt: &past, RevokedAt: &past}, chatpb.Invite_REVOKED, ErrInviteRevoked},
		{"revoked, expired and used up", Invite{MaxUses: 1, UseCount: 1, ExpiresAt: &past, RevokedAt: &past}, chatpb.Invite_REVOKED, ErrInviteRevoked},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, tc.invite.StatusAt(now))
			require.ErrorIs(t, tc.invite.AdmitsAt(now), tc.err)
			if tc.err == nil {
				require.NoError(t, tc.invite.AdmitsAt(now))
			}
		})
	}
}

func TestInvite_IsLastUse(t *testing.T) {
	require.False(t, Invite{}.IsLastUse())
	require.False(t, Invite{UseCount: 1000}.IsLastUse())
	require.True(t, Invite{MaxUses: 1}.IsLastUse())
	require.False(t, Invite{MaxUses: 3, UseCount: 1}.IsLastUse())
	require.True(t, Invite{MaxUses: 3, UseCount: 2}.IsLastUse())
}

func TestInvite_ToProtoAndClone(t *testing.T) {
	now := time.Date(2026, 10, 26, 12, 0, 0, 0, time.UTC)
	expiresAt := now.Add(time.Hour)
	invite := Invite{
		ID:        &chatpb.InviteId{Value: bytes.Repeat([]byte{7}, InviteIDSize)},
		ChatID:    MustGenerateGroupChatID(),
		CreatedBy: model.MustGenerateUserID(),
		MaxUses:   5,
		UseCount:  2,
		CreatedAt: now.Add(-time.Hour),
		ExpiresAt: &expiresAt,
		SecretEnvelope: &chatpb.InviteSecretEnvelope{
			Scheme:     chatpb.InviteSecretEnvelope_X25519_XCHACHA20POLY1305,
			Nonce:      bytes.Repeat([]byte{1}, 24),
			Ciphertext: bytes.Repeat([]byte{2}, 48),
		},
	}

	pb := invite.ToProto(now)
	require.NoError(t, pb.Validate())
	require.Equal(t, invite.ID.Value, pb.InviteId.Value)
	require.Equal(t, invite.ChatID.Value, pb.ChatId.Value)
	require.Equal(t, invite.CreatedBy.Value, pb.CreatedBy.Value)
	require.EqualValues(t, 5, pb.MaxUses)
	require.EqualValues(t, 2, pb.UseCount)
	require.True(t, invite.CreatedAt.Equal(pb.CreatedAt.AsTime()))
	require.True(t, expiresAt.Equal(pb.ExpiresAt.AsTime()))
	require.Equal(t, chatpb.Invite_ACTIVE, pb.Status)
	require.Equal(t, chatpb.Invite_EXPIRED, invite.ToProto(expiresAt).Status)

	// The projection shares nothing with the invite.
	pb.InviteId.Value[0] ^= 1
	pb.SecretEnvelope.Nonce[0] ^= 1
	require.Equal(t, byte(7), invite.ID.Value[0])
	require.Equal(t, byte(1), invite.SecretEnvelope.Nonce[0])

	// Nor does a clone, and a revoke shows in the status alone.
	clone := invite.Clone()
	revokedAt := now
	clone.RevokedAt = &revokedAt
	*clone.ExpiresAt = now
	clone.ChatID.Value[0] ^= 1
	require.Nil(t, invite.RevokedAt)
	require.True(t, expiresAt.Equal(*invite.ExpiresAt))
	require.NotEqual(t, invite.ChatID.Value, clone.ChatID.Value)
	require.Equal(t, chatpb.Invite_REVOKED, clone.ToProto(now).Status)

	// An invite that never expires carries no expiry.
	invite.ExpiresAt = nil
	require.Nil(t, invite.ToProto(now).ExpiresAt)
}

func TestNewInviteKeyEnvelope(t *testing.T) {
	creatorID := model.MustGenerateUserID()
	invite := Invite{
		ID:        &chatpb.InviteId{Value: bytes.Repeat([]byte{7}, InviteIDSize)},
		CreatedBy: creatorID,
	}
	copyEnvelope := &chatpb.InviteKeyEnvelope{
		Scheme:     chatpb.InviteKeyEnvelope_XCHACHA20POLY1305,
		Nonce:      bytes.Repeat([]byte{1}, 24),
		Ciphertext: bytes.Repeat([]byte{2}, 48),
		Signature:  bytes.Repeat([]byte{3}, 64),
	}
	secretEnvelope := &chatpb.InviteSecretEnvelope{
		Scheme:     chatpb.InviteSecretEnvelope_X25519_XCHACHA20POLY1305,
		Nonce:      bytes.Repeat([]byte{4}, 24),
		Ciphertext: bytes.Repeat([]byte{5}, 48),
	}

	// Recorded as wrapped by the invite's creator, who signed the copy, and
	// holding nothing in the wrapped envelope's fields.
	envelope := NewInviteKeyEnvelope(invite, copyEnvelope, secretEnvelope)
	require.True(t, envelope.IsInviteCopy())
	require.True(t, envelope.IsWrappedBy(creatorID))
	require.Empty(t, envelope.Nonce)
	require.Empty(t, envelope.Ciphertext)
	require.Equal(t, chatpb.KeyEnvelope_UNKNOWN, envelope.Scheme)

	bundle := envelope.InviteCopy.ToProto()
	require.NoError(t, bundle.Validate())
	require.Equal(t, invite.ID.Value, bundle.InviteId.Value)
	require.Equal(t, copyEnvelope.Signature, bundle.KeyEnvelope.Signature)
	require.Equal(t, secretEnvelope.Ciphertext, bundle.SecretEnvelope.Ciphertext)

	// It shares nothing with its inputs, its clone or its projection.
	copyEnvelope.Nonce[0] ^= 1
	bundle.SecretEnvelope.Nonce[0] ^= 1
	clone := envelope.Clone()
	clone.InviteCopy.InviteID.Value[0] ^= 1
	require.Equal(t, byte(1), envelope.InviteCopy.Envelope.Nonce[0])
	require.Equal(t, byte(4), envelope.InviteCopy.SecretEnvelope.Nonce[0])
	require.Equal(t, byte(7), envelope.InviteCopy.InviteID.Value[0])

	// Equal tells copies apart by every part, and a copy from a wrapped
	// envelope.
	require.True(t, envelope.Equal(envelope.Clone()))
	require.False(t, envelope.Equal(clone))
	otherSecret := envelope.Clone()
	otherSecret.InviteCopy.SecretEnvelope.Ciphertext[0] ^= 1
	require.False(t, envelope.Equal(otherSecret))
	otherWrapper := envelope.Clone()
	otherWrapper.WrappedBy = model.MustGenerateUserID()
	require.False(t, envelope.Equal(otherWrapper))
	wrapped := KeyEnvelope{WrappedBy: creatorID}
	require.False(t, envelope.Equal(wrapped))
	require.False(t, wrapped.Equal(envelope))
	require.False(t, wrapped.IsInviteCopy())
	require.True(t, wrapped.Equal(wrapped.Clone()))
}
