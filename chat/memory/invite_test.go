package memory

import (
	"bytes"
	"context"
	"crypto/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	chatpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/chat/v1"
	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"

	"github.com/code-payments/flipcash2-server/chat"
	"github.com/code-payments/flipcash2-server/model"
)

// TestChat_MemoryInviteCopyAndUsedUp covers what no Store method observes:
// the copy is held from creation until the revoke, and a used-up invite is
// not counted toward the cap. A use is written here directly, as a join
// writes it (see TestChat_MemoryInviteJoinCopy for the join itself).
func TestChat_MemoryInviteCopyAndUsedUp(t *testing.T) {
	ctx := context.Background()
	m := NewInMemory(nil).(*memory)
	now := time.Now()
	groupID := chat.MustGenerateGroupChatID()

	newInvite := func(maxUses uint32) chat.Invite {
		token := make([]byte, chat.InviteTokenSize)
		_, err := rand.Read(token)
		require.NoError(t, err)
		return chat.Invite{
			ID:        chat.MustDeriveInviteID(&chatpb.InviteToken{Value: token}),
			ChatID:    groupID,
			CreatedBy: model.MustGenerateUserID(),
			MaxUses:   maxUses,
			CreatedAt: now,
			SecretEnvelope: &chatpb.InviteSecretEnvelope{
				Scheme:     chatpb.InviteSecretEnvelope_X25519_XCHACHA20POLY1305,
				Nonce:      bytes.Repeat([]byte{1}, 24),
				Ciphertext: bytes.Repeat([]byte{1}, 48),
			},
		}
	}
	keyCopy := &chatpb.InviteKeyEnvelope{
		Scheme:     chatpb.InviteKeyEnvelope_XCHACHA20POLY1305,
		Nonce:      bytes.Repeat([]byte{2}, 24),
		Ciphertext: bytes.Repeat([]byte{2}, 48),
		Signature:  bytes.Repeat([]byte{2}, 64),
	}

	used := newInvite(1)
	_, created, err := m.CreateInvite(ctx, used, keyCopy, 1, now)
	require.NoError(t, err)
	require.True(t, created)
	require.Contains(t, m.inviteCopies, string(used.ID.Value))
	keyCopy.Nonce[0] = 9
	require.Equal(t, byte(2), m.inviteCopies[string(used.ID.Value)].Nonce[0])

	_, _, err = m.CreateInvite(ctx, newInvite(0), keyCopy, 1, now)
	require.ErrorIs(t, err, chat.ErrTooManyInvites)

	// Its last use taken, it no longer counts.
	m.Lock()
	stored := m.invites[string(used.ID.Value)]
	stored.UseCount = 1
	m.invites[string(used.ID.Value)] = stored
	m.Unlock()
	next := newInvite(0)
	_, created, err = m.CreateInvite(ctx, next, keyCopy, 1, now)
	require.NoError(t, err)
	require.True(t, created)

	// The revoke deletes the copy; a repeat changes nothing.
	_, changed, err := m.RevokeInvite(ctx, groupID, next.ID, now)
	require.NoError(t, err)
	require.True(t, changed)
	require.NotContains(t, m.inviteCopies, string(next.ID.Value))
	_, changed, err = m.RevokeInvite(ctx, groupID, next.ID, now)
	require.NoError(t, err)
	require.False(t, changed)
}

// TestChat_MemoryInviteJoinCopy covers what no Store method observes of a
// join: the invite's copy is held until the join that takes the last use,
// and deleted by it, while a join before it leaves the copy in place.
func TestChat_MemoryInviteJoinCopy(t *testing.T) {
	ctx := context.Background()
	m := NewInMemory(nil).(*memory)
	now := time.Now()
	creator := model.MustGenerateUserID()
	groupID := chat.MustGenerateGroupChatID()
	require.NoError(t, m.PutChat(ctx, &chat.Chat{
		ID:           groupID,
		Type:         chatpb.ChatType_GROUP,
		Members:      []*commonpb.UserId{creator},
		Title:        "Private",
		IsPrivate:    true,
		CreatorID:    creator,
		LastActivity: now,
	}))

	token := make([]byte, chat.InviteTokenSize)
	_, err := rand.Read(token)
	require.NoError(t, err)
	secret := &chatpb.InviteSecretEnvelope{
		Scheme:     chatpb.InviteSecretEnvelope_X25519_XCHACHA20POLY1305,
		Nonce:      bytes.Repeat([]byte{1}, 24),
		Ciphertext: bytes.Repeat([]byte{1}, 48),
	}
	invite := chat.Invite{
		ID:             chat.MustDeriveInviteID(&chatpb.InviteToken{Value: token}),
		ChatID:         groupID,
		CreatedBy:      creator,
		MaxUses:        2,
		CreatedAt:      now,
		SecretEnvelope: secret,
	}
	keyCopy := &chatpb.InviteKeyEnvelope{
		Scheme:     chatpb.InviteKeyEnvelope_XCHACHA20POLY1305,
		Nonce:      bytes.Repeat([]byte{2}, 24),
		Ciphertext: bytes.Repeat([]byte{2}, 48),
		Signature:  bytes.Repeat([]byte{2}, 64),
	}
	_, created, err := m.CreateInvite(ctx, invite, keyCopy, chat.MaxActiveInvites, now)
	require.NoError(t, err)
	require.True(t, created)

	result, err := m.JoinWithInvite(ctx, groupID, model.MustGenerateUserID(), invite.ID, secret, now)
	require.NoError(t, err)
	require.True(t, result.Changed)
	require.Contains(t, m.inviteCopies, string(invite.ID.Value))

	result, err = m.JoinWithInvite(ctx, groupID, model.MustGenerateUserID(), invite.ID, secret, now)
	require.NoError(t, err)
	require.True(t, result.Changed)
	require.NotContains(t, m.inviteCopies, string(invite.ID.Value))
}
