package tests

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	blobpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/blob/v1"

	"github.com/code-payments/flipcash2-server/blob"
	"github.com/code-payments/flipcash2-server/model"
)

// RunStoreTests runs the shared blob.Store test suite.
func RunStoreTests(t *testing.T, store blob.Store, teardown func()) {
	for _, tf := range []func(t *testing.T, store blob.Store){
		testStoreCreateAndGet,
		testStoreAdvance,
		testStoreRenditions,
	} {
		tf(t, store)
		teardown()
	}
}

func newBlobID(t *testing.T) *blobpb.BlobId {
	id, err := uuid.NewRandom()
	require.NoError(t, err)
	value := id
	return &blobpb.BlobId{Value: value[:]}
}

func pendingOriginal(t *testing.T) *blob.Blob {
	id := newBlobID(t)
	key, err := blob.StorageKey(id, "image/png")
	require.NoError(t, err)
	return &blob.Blob{
		ID:         id,
		Rendition:  blob.RenditionOriginal,
		Owner:      model.MustGenerateUserID(),
		State:      blob.StatePending,
		StorageKey: key,
		MimeType:   "image/png",
		SizeBytes:  1234,
	}
}

func testStoreCreateAndGet(t *testing.T, store blob.Store) {
	ctx := context.Background()

	_, err := store.GetByID(ctx, newBlobID(t))
	require.ErrorIs(t, err, blob.ErrNotFound)

	original := pendingOriginal(t)
	require.NoError(t, store.CreatePending(ctx, original))

	// Re-inserting the same id is rejected.
	require.ErrorIs(t, store.CreatePending(ctx, original), blob.ErrExists)

	got, err := store.GetByID(ctx, original.ID)
	require.NoError(t, err)
	require.Equal(t, blob.StatePending, got.State)
	require.Equal(t, blob.RenditionOriginal, got.Rendition)
	require.Equal(t, original.MimeType, got.MimeType)
	require.Equal(t, original.SizeBytes, got.SizeBytes)
	require.Equal(t, original.StorageKey, got.StorageKey)
	require.Equal(t, original.Owner.Value, got.Owner.Value)
	require.Nil(t, got.ParentID)

	// GetByIDs returns only the ids that exist.
	second := pendingOriginal(t)
	require.NoError(t, store.CreatePending(ctx, second))

	found, err := store.GetByIDs(ctx, []*blobpb.BlobId{original.ID, newBlobID(t), second.ID})
	require.NoError(t, err)
	require.Len(t, found, 2)
}

func testStoreAdvance(t *testing.T, store blob.Store) {
	ctx := context.Background()

	advanced, err := store.Advance(ctx, newBlobID(t), blob.StateUploaded, nil)
	require.ErrorIs(t, err, blob.ErrNotFound)
	require.False(t, advanced)

	original := pendingOriginal(t)
	require.NoError(t, store.CreatePending(ctx, original))

	// Advance forward through the pipeline checkpoints; each real transition reports advanced.
	advanced, err = store.Advance(ctx, original.ID, blob.StateUploaded, nil)
	require.NoError(t, err)
	require.True(t, advanced)
	got, err := store.GetByID(ctx, original.ID)
	require.NoError(t, err)
	require.Equal(t, blob.StateUploaded, got.State)

	// The metadata is persisted at the StateInspected checkpoint.
	image := &blob.ImageMetadata{Width: 100, Height: 200, Blurhash: "LEHV6nWB"}
	advanced, err = store.Advance(ctx, original.ID, blob.StateInspected, image)
	require.NoError(t, err)
	require.True(t, advanced)
	got, err = store.GetByID(ctx, original.ID)
	require.NoError(t, err)
	require.Equal(t, blob.StateInspected, got.State)
	require.NotNil(t, got.Image)
	require.EqualValues(t, 100, got.Image.Width)
	require.EqualValues(t, 200, got.Image.Height)
	require.Equal(t, "LEHV6nWB", got.Image.Blurhash)
	// The declared type and size are never altered.
	require.Equal(t, original.MimeType, got.MimeType)
	require.Equal(t, original.SizeBytes, got.SizeBytes)

	advanced, err = store.Advance(ctx, original.ID, blob.StatePromoted, nil)
	require.NoError(t, err)
	require.True(t, advanced)
	advanced, err = store.Advance(ctx, original.ID, blob.StateReady, nil)
	require.NoError(t, err)
	require.True(t, advanced)
	got, err = store.GetByID(ctx, original.ID)
	require.NoError(t, err)
	require.Equal(t, blob.StateReady, got.State)
	require.NotNil(t, got.Image) // retained across later advances

	// Advancing is forward-only and idempotent: a backward target, or a terminal
	// blob, does not transition and reports advanced == false.
	advanced, err = store.Advance(ctx, original.ID, blob.StateUploaded, nil)
	require.NoError(t, err)
	require.False(t, advanced)
	advanced, err = store.Advance(ctx, original.ID, blob.StateRejected, nil)
	require.NoError(t, err)
	require.False(t, advanced)
	got, err = store.GetByID(ctx, original.ID)
	require.NoError(t, err)
	require.Equal(t, blob.StateReady, got.State)

	// A blob can be rejected outright with no derived metadata.
	rejected := pendingOriginal(t)
	require.NoError(t, store.CreatePending(ctx, rejected))
	advanced, err = store.Advance(ctx, rejected.ID, blob.StateRejected, nil)
	require.NoError(t, err)
	require.True(t, advanced)
	got, err = store.GetByID(ctx, rejected.ID)
	require.NoError(t, err)
	require.Equal(t, blob.StateRejected, got.State)
	require.Nil(t, got.Image)
}

func testStoreRenditions(t *testing.T, store blob.Store) {
	ctx := context.Background()

	original := pendingOriginal(t)
	require.NoError(t, store.CreatePending(ctx, original))

	// An original with no renditions yields an empty set.
	renditions, err := store.GetRenditions(ctx, original.ID)
	require.NoError(t, err)
	require.Empty(t, renditions)

	display := pendingOriginal(t)
	display.Rendition = blob.RenditionDisplay
	display.ParentID = original.ID

	thumbnail := pendingOriginal(t)
	thumbnail.Rendition = blob.RenditionThumbnail
	thumbnail.ParentID = original.ID

	require.NoError(t, store.CreatePending(ctx, display))
	require.NoError(t, store.CreatePending(ctx, thumbnail))

	renditions, err = store.GetRenditions(ctx, original.ID)
	require.NoError(t, err)
	require.Len(t, renditions, 2)

	kinds := map[blob.RenditionType]bool{}
	for _, r := range renditions {
		require.Equal(t, original.ID.Value, r.ParentID.Value)
		kinds[r.Rendition] = true
	}
	require.True(t, kinds[blob.RenditionDisplay])
	require.True(t, kinds[blob.RenditionThumbnail])

	// The original itself is not one of its own renditions.
	for _, r := range renditions {
		require.NotEqual(t, original.ID.Value, r.ID.Value)
	}
}
