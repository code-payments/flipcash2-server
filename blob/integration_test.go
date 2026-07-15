package blob_test

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	blobpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/blob/v1"
	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"

	"github.com/code-payments/flipcash2-server/blob"
	"github.com/code-payments/flipcash2-server/blob/memory"
	"github.com/code-payments/flipcash2-server/model"
)

func newBlobID(t *testing.T) *blobpb.BlobId {
	id, err := uuid.NewRandom()
	require.NoError(t, err)
	v := id
	return &blobpb.BlobId{Value: v[:]}
}

func newChatID() *commonpb.ChatId {
	a := model.MustGenerateUserID().Value
	b := model.MustGenerateUserID().Value
	return &commonpb.ChatId{Value: append(append([]byte{}, a...), b...)}
}

func putReadyOriginal(t *testing.T, store blob.Store, owner *commonpb.UserId) *blobpb.BlobId {
	ctx := context.Background()
	id := newBlobID(t)
	require.NoError(t, store.CreatePending(ctx, &blob.Blob{
		ID:         id,
		Rendition:  blob.RenditionOriginal,
		Owner:      owner,
		State:      blob.StatePending,
		StorageKey: "images/x/original.png",
		MimeType:   "image/png",
		SizeBytes:  1,
	}))
	_, err := store.Advance(ctx, id, blob.StateReady, nil)
	require.NoError(t, err)
	return id
}

func TestIntegration_ShareIntoChat(t *testing.T) {
	ctx := context.Background()
	store := memory.NewInMemory()
	access := memory.NewInMemoryAccessStore()
	integration := blob.NewIntegration(store, memory.NewInMemoryStorage(), access)

	owner := model.MustGenerateUserID()
	chatID := newChatID()
	chatPrincipal := blob.PrincipalForChat(chatID)

	t.Run("a ready original owned by the sharer is shared and granted", func(t *testing.T) {
		id := putReadyOriginal(t, store, owner)
		require.NoError(t, integration.ShareIntoChat(ctx, owner, chatID, []*blobpb.BlobId{id}))

		has, err := access.HasGrant(ctx, id, chatPrincipal, blob.PermissionRead)
		require.NoError(t, err)
		require.True(t, has)

		// Idempotent: sharing again leaves the grant in place, not an error.
		require.NoError(t, integration.ShareIntoChat(ctx, owner, chatID, []*blobpb.BlobId{id}))
	})

	t.Run("empty input is a no-op", func(t *testing.T) {
		require.NoError(t, integration.ShareIntoChat(ctx, owner, chatID, nil))
	})

	t.Run("a blob owned by someone else is rejected and grants nothing", func(t *testing.T) {
		mine := putReadyOriginal(t, store, owner)
		theirs := putReadyOriginal(t, store, model.MustGenerateUserID())

		err := integration.ShareIntoChat(ctx, owner, chatID, []*blobpb.BlobId{mine, theirs})
		require.ErrorIs(t, err, blob.ErrBlobNotShareable)

		// All-or-nothing: the valid blob in the batch was not granted either.
		has, err := access.HasGrant(ctx, mine, chatPrincipal, blob.PermissionRead)
		require.NoError(t, err)
		require.False(t, has)
	})

	t.Run("a non-ready blob is rejected", func(t *testing.T) {
		id := newBlobID(t)
		require.NoError(t, store.CreatePending(ctx, &blob.Blob{
			ID: id, Rendition: blob.RenditionOriginal, Owner: owner, State: blob.StatePending,
			StorageKey: "k", MimeType: "image/png", SizeBytes: 1,
		}))
		require.ErrorIs(t, integration.ShareIntoChat(ctx, owner, chatID, []*blobpb.BlobId{id}), blob.ErrBlobNotShareable)
	})

	t.Run("a rendition is rejected (only originals are shareable)", func(t *testing.T) {
		original := putReadyOriginal(t, store, owner)
		rendition := newBlobID(t)
		require.NoError(t, store.CreatePending(ctx, &blob.Blob{
			ID: rendition, Rendition: blob.RenditionDisplay, ParentID: original, Owner: owner, State: blob.StatePending,
			StorageKey: "k", MimeType: "image/png", SizeBytes: 1,
		}))
		_, err := store.Advance(ctx, rendition, blob.StateReady, nil)
		require.NoError(t, err)
		require.ErrorIs(t, integration.ShareIntoChat(ctx, owner, chatID, []*blobpb.BlobId{rendition}), blob.ErrBlobNotShareable)
	})

	t.Run("a non-image blob is rejected", func(t *testing.T) {
		id := newBlobID(t)
		require.NoError(t, store.CreatePending(ctx, &blob.Blob{
			ID: id, Rendition: blob.RenditionOriginal, Owner: owner, State: blob.StatePending,
			StorageKey: "k", MimeType: "video/mp4", SizeBytes: 1,
		}))
		_, err := store.Advance(ctx, id, blob.StateReady, nil)
		require.NoError(t, err)
		require.ErrorIs(t, integration.ShareIntoChat(ctx, owner, chatID, []*blobpb.BlobId{id}), blob.ErrBlobNotShareable)
	})

	t.Run("an unknown blob is rejected", func(t *testing.T) {
		require.ErrorIs(t, integration.ShareIntoChat(ctx, owner, chatID, []*blobpb.BlobId{newBlobID(t)}), blob.ErrBlobNotShareable)
	})
}

func TestIntegration_ResolveRenditions(t *testing.T) {
	ctx := context.Background()
	store := memory.NewInMemory()
	integration := blob.NewIntegration(store, memory.NewInMemoryStorage(), memory.NewInMemoryAccessStore())

	owner := model.MustGenerateUserID()

	// A READY original resolves to its rendition set: the ORIGINAL plus its manifest.
	ready := putReadyOriginal(t, store, owner)
	displayID := newBlobID(t)
	require.NoError(t, store.AttachRenditions(ctx, ready, []blob.RenditionRef{{
		ID:         displayID,
		Rendition:  blob.RenditionDisplay,
		MimeType:   "image/jpeg",
		SizeBytes:  42,
		StorageKey: "images/x/display_800x600.jpg",
		Image:      &blob.ImageMetadata{Width: 800, Height: 600, Blurhash: "LKO2"},
	}}))
	// A pending (non-READY) original is omitted.
	pending := newBlobID(t)
	require.NoError(t, store.CreatePending(ctx, &blob.Blob{
		ID: pending, Rendition: blob.RenditionOriginal, Owner: owner, State: blob.StatePending,
		StorageKey: "k", MimeType: "image/png", SizeBytes: 1,
	}))
	// An unknown id is omitted.
	unknown := newBlobID(t)

	resolved, err := integration.ResolveRenditions(ctx, []*blobpb.BlobId{ready, pending, unknown})
	require.NoError(t, err)
	require.Len(t, resolved, 1)

	renditions := resolved[string(ready.Value)]
	require.Len(t, renditions, 2)

	// ORIGINAL first, with its own metadata and a fresh download URL.
	require.Equal(t, blobpb.Rendition_ORIGINAL, renditions[0].Role)
	require.Equal(t, ready.Value, renditions[0].BlobId.Value)
	require.Equal(t, "image/png", renditions[0].Blob.MimeType)
	require.NotEmpty(t, renditions[0].Blob.GetDownloadUrl().GetUrl())

	// Then the derived rendition from the manifest.
	require.Equal(t, blobpb.Rendition_DISPLAY, renditions[1].Role)
	require.Equal(t, displayID.Value, renditions[1].BlobId.Value)
	require.Equal(t, "image/jpeg", renditions[1].Blob.MimeType)
	require.EqualValues(t, 42, renditions[1].Blob.SizeBytes)
	require.EqualValues(t, 800, renditions[1].Blob.GetImage().GetWidth())
	require.NotEmpty(t, renditions[1].Blob.GetDownloadUrl().GetUrl())

	// Empty input yields a nil map.
	empty, err := integration.ResolveRenditions(ctx, nil)
	require.NoError(t, err)
	require.Nil(t, empty)
}
