package tests

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"

	"github.com/code-payments/flipcash2-server/blob"
	"github.com/code-payments/flipcash2-server/model"
)

// RunAccessStoreTests runs the shared blob.AccessStore test suite.
func RunAccessStoreTests(t *testing.T, store blob.AccessStore, teardown func()) {
	for _, tf := range []func(t *testing.T, store blob.AccessStore){
		testAccessGrantHasRevoke,
		testAccessNoCollision,
		testAccessValidation,
	} {
		tf(t, store)
		teardown()
	}
}

func newChatID(t *testing.T) *commonpb.ChatId {
	// A chat id is 32 bytes; two random UUIDs concatenated stand in for one.
	a, err := uuid.NewRandom()
	require.NoError(t, err)
	b, err := uuid.NewRandom()
	require.NoError(t, err)
	value := append(append([]byte{}, a[:]...), b[:]...)
	return &commonpb.ChatId{Value: value}
}

func testAccessGrantHasRevoke(t *testing.T, store blob.AccessStore) {
	ctx := context.Background()

	blobID := blob.MustGenerateID()
	chat := blob.PrincipalForChat(newChatID(t))

	// Absent before granting.
	has, err := store.HasGrant(ctx, blobID, chat, blob.PermissionRead)
	require.NoError(t, err)
	require.False(t, has)

	require.NoError(t, store.Grant(ctx, &blob.Grant{BlobID: blobID, Principal: chat, Permission: blob.PermissionRead}))

	has, err = store.HasGrant(ctx, blobID, chat, blob.PermissionRead)
	require.NoError(t, err)
	require.True(t, has)

	// Granting again is an idempotent no-op, not an error.
	require.NoError(t, store.Grant(ctx, &blob.Grant{BlobID: blobID, Principal: chat, Permission: blob.PermissionRead}))
	has, err = store.HasGrant(ctx, blobID, chat, blob.PermissionRead)
	require.NoError(t, err)
	require.True(t, has)

	// Revoke removes it; revoking again is also an idempotent no-op.
	require.NoError(t, store.Revoke(ctx, blobID, chat, blob.PermissionRead))
	has, err = store.HasGrant(ctx, blobID, chat, blob.PermissionRead)
	require.NoError(t, err)
	require.False(t, has)
	require.NoError(t, store.Revoke(ctx, blobID, chat, blob.PermissionRead))
}

func testAccessNoCollision(t *testing.T, store blob.AccessStore) {
	ctx := context.Background()

	blobA := blob.MustGenerateID()
	blobB := blob.MustGenerateID()
	chat := blob.PrincipalForChat(newChatID(t))
	user := blob.PrincipalForUser(model.MustGenerateUserID())

	require.NoError(t, store.Grant(ctx, &blob.Grant{BlobID: blobA, Principal: chat, Permission: blob.PermissionRead}))

	// The same grant on a different blob is absent.
	has, err := store.HasGrant(ctx, blobB, chat, blob.PermissionRead)
	require.NoError(t, err)
	require.False(t, has)

	// The same blob and permission but a different principal is absent.
	has, err = store.HasGrant(ctx, blobA, user, blob.PermissionRead)
	require.NoError(t, err)
	require.False(t, has)

	// The granted triple is present.
	has, err = store.HasGrant(ctx, blobA, chat, blob.PermissionRead)
	require.NoError(t, err)
	require.True(t, has)

	// The principal type is part of the key: a user and a chat with identical id
	// bytes are distinct grants and must not collide.
	raw := newChatID(t).Value
	asUser := blob.Principal{Type: blob.PrincipalTypeUser, ID: raw}
	asChat := blob.Principal{Type: blob.PrincipalTypeChat, ID: raw}
	require.NoError(t, store.Grant(ctx, &blob.Grant{BlobID: blobA, Principal: asUser, Permission: blob.PermissionRead}))
	has, err = store.HasGrant(ctx, blobA, asChat, blob.PermissionRead)
	require.NoError(t, err)
	require.False(t, has)
	has, err = store.HasGrant(ctx, blobA, asUser, blob.PermissionRead)
	require.NoError(t, err)
	require.True(t, has)
}

func testAccessValidation(t *testing.T, store blob.AccessStore) {
	ctx := context.Background()

	blobID := blob.MustGenerateID()
	chat := blob.PrincipalForChat(newChatID(t))

	// An unknown permission is rejected, not silently stored.
	err := store.Grant(ctx, &blob.Grant{BlobID: blobID, Principal: chat, Permission: blob.PermissionUnknown})
	require.ErrorIs(t, err, blob.ErrInvalidGrant)

	// An unknown principal type is rejected.
	err = store.Grant(ctx, &blob.Grant{BlobID: blobID, Principal: blob.Principal{Type: blob.PrincipalTypeUnknown, ID: []byte{1}}, Permission: blob.PermissionRead})
	require.ErrorIs(t, err, blob.ErrInvalidGrant)

	// An empty principal id is rejected.
	err = store.Grant(ctx, &blob.Grant{BlobID: blobID, Principal: blob.Principal{Type: blob.PrincipalTypeChat}, Permission: blob.PermissionRead})
	require.ErrorIs(t, err, blob.ErrInvalidGrant)

	// A missing blob id is rejected.
	err = store.Grant(ctx, &blob.Grant{BlobID: nil, Principal: chat, Permission: blob.PermissionRead})
	require.ErrorIs(t, err, blob.ErrInvalidGrant)

	// HasGrant validates its lookup key too.
	_, err = store.HasGrant(ctx, blobID, blob.Principal{Type: blob.PrincipalTypeUnknown, ID: []byte{1}}, blob.PermissionRead)
	require.ErrorIs(t, err, blob.ErrInvalidGrant)
}
