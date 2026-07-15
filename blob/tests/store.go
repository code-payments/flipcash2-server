package tests

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	blobpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/blob/v1"
	moderationpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/moderation/v1"

	"github.com/code-payments/flipcash2-server/blob"
	"github.com/code-payments/flipcash2-server/model"
)

// RunStoreTests runs the shared blob.Store test suite.
func RunStoreTests(t *testing.T, store blob.Store, teardown func()) {
	for _, tf := range []func(t *testing.T, store blob.Store){
		testStoreCreateAndGet,
		testStoreAdvance,
		testStoreReject,
		testStoreRenditions,
	} {
		tf(t, store)
		teardown()
	}
}

func pendingOriginal(t *testing.T) *blob.Blob {
	id := blob.MustGenerateID()
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

	_, err := store.GetByID(ctx, blob.MustGenerateID())
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

	found, err := store.GetByIDs(ctx, []*blobpb.BlobId{original.ID, blob.MustGenerateID(), second.ID})
	require.NoError(t, err)
	require.Len(t, found, 2)
}

func testStoreAdvance(t *testing.T, store blob.Store) {
	ctx := context.Background()

	advanced, err := store.Advance(ctx, blob.MustGenerateID(), blob.StateUploaded, nil)
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
	got, err = store.GetByID(ctx, original.ID)
	require.NoError(t, err)
	require.Equal(t, blob.StateReady, got.State)

	// Advance cannot be used to reject a blob; StateRejected is reached only
	// through Reject, so passing it as a target is an error and changes nothing.
	advanced, err = store.Advance(ctx, original.ID, blob.StateRejected, nil)
	require.ErrorIs(t, err, blob.ErrCannotAdvanceToRejected)
	require.False(t, advanced)
}

func testStoreReject(t *testing.T, store blob.Store) {
	ctx := context.Background()

	// Rejecting an unknown blob reports not-found.
	advanced, err := store.Reject(ctx, blob.MustGenerateID(), &blob.RejectionMetadata{Reason: blob.RejectionReasonCorrupt})
	require.ErrorIs(t, err, blob.ErrNotFound)
	require.False(t, advanced)

	// A pending blob is rejected, and the reason round-trips (no flagged category
	// for a non-moderation reason).
	corrupt := pendingOriginal(t)
	require.NoError(t, store.CreatePending(ctx, corrupt))
	advanced, err = store.Reject(ctx, corrupt.ID, &blob.RejectionMetadata{Reason: blob.RejectionReasonCorrupt})
	require.NoError(t, err)
	require.True(t, advanced)
	got, err := store.GetByID(ctx, corrupt.ID)
	require.NoError(t, err)
	require.Equal(t, blob.StateRejected, got.State)
	require.NotNil(t, got.Rejection)
	require.Equal(t, blob.RejectionReasonCorrupt, got.Rejection.Reason)
	require.Equal(t, moderationpb.FlaggedCategory_NONE, got.Rejection.FlaggedCategory)

	// A moderation rejection also round-trips its flagged category.
	flagged := pendingOriginal(t)
	require.NoError(t, store.CreatePending(ctx, flagged))
	advanced, err = store.Reject(ctx, flagged.ID, &blob.RejectionMetadata{
		Reason:          blob.RejectionReasonModeration,
		FlaggedCategory: moderationpb.FlaggedCategory_NSFW,
	})
	require.NoError(t, err)
	require.True(t, advanced)
	got, err = store.GetByID(ctx, flagged.ID)
	require.NoError(t, err)
	require.Equal(t, blob.RejectionReasonModeration, got.Rejection.Reason)
	require.Equal(t, moderationpb.FlaggedCategory_NSFW, got.Rejection.FlaggedCategory)

	// Rejection is terminal and idempotent: a second reject neither transitions nor
	// overwrites the recorded reason.
	advanced, err = store.Reject(ctx, flagged.ID, &blob.RejectionMetadata{Reason: blob.RejectionReasonCorrupt})
	require.NoError(t, err)
	require.False(t, advanced)
	got, err = store.GetByID(ctx, flagged.ID)
	require.NoError(t, err)
	require.Equal(t, blob.RejectionReasonModeration, got.Rejection.Reason)

	// A READY blob cannot be rejected.
	ready := pendingOriginal(t)
	require.NoError(t, store.CreatePending(ctx, ready))
	for _, to := range []blob.State{blob.StateUploaded, blob.StateInspected, blob.StatePromoted, blob.StateReady} {
		_, err = store.Advance(ctx, ready.ID, to, nil)
		require.NoError(t, err)
	}
	advanced, err = store.Reject(ctx, ready.ID, &blob.RejectionMetadata{Reason: blob.RejectionReasonCorrupt})
	require.NoError(t, err)
	require.False(t, advanced)
	got, err = store.GetByID(ctx, ready.ID)
	require.NoError(t, err)
	require.Equal(t, blob.StateReady, got.State)
	require.Nil(t, got.Rejection)
}

func testStoreRenditions(t *testing.T, store blob.Store) {
	ctx := context.Background()

	original := pendingOriginal(t)
	require.NoError(t, store.CreatePending(ctx, original))

	// A freshly created original carries no rendition manifest.
	got, err := store.GetByID(ctx, original.ID)
	require.NoError(t, err)
	require.Empty(t, got.Renditions)

	refs := []blob.RenditionRef{
		{
			ID:         blob.MustGenerateID(),
			Rendition:  blob.RenditionThumbnail,
			MimeType:   "image/jpeg",
			SizeBytes:  111,
			StorageKey: "images/x/thumbnail_160x90.jpg",
			Image:      &blob.ImageMetadata{Width: 160, Height: 90, Blurhash: "LKO2", HasAlpha: false},
		},
		{
			ID:         blob.MustGenerateID(),
			Rendition:  blob.RenditionDisplay,
			MimeType:   "image/jpeg",
			SizeBytes:  222,
			StorageKey: "images/x/display_800x450.jpg",
			Image:      &blob.ImageMetadata{Width: 800, Height: 450, Blurhash: "LKO2", HasAlpha: false},
		},
	}
	require.NoError(t, store.AttachRenditions(ctx, original.ID, refs))

	// The manifest round-trips on the original's own record, in order, every field
	// intact.
	got, err = store.GetByID(ctx, original.ID)
	require.NoError(t, err)
	require.Len(t, got.Renditions, 2)

	require.Equal(t, refs[0].ID.Value, got.Renditions[0].ID.Value)
	require.Equal(t, blob.RenditionThumbnail, got.Renditions[0].Rendition)
	require.Equal(t, "image/jpeg", got.Renditions[0].MimeType)
	require.EqualValues(t, 111, got.Renditions[0].SizeBytes)
	require.Equal(t, "images/x/thumbnail_160x90.jpg", got.Renditions[0].StorageKey)
	require.NotNil(t, got.Renditions[0].Image)
	require.EqualValues(t, 160, got.Renditions[0].Image.Width)
	require.EqualValues(t, 90, got.Renditions[0].Image.Height)
	require.Equal(t, "LKO2", got.Renditions[0].Image.Blurhash)

	require.Equal(t, refs[1].ID.Value, got.Renditions[1].ID.Value)
	require.Equal(t, blob.RenditionDisplay, got.Renditions[1].Rendition)

	// Re-attaching overwrites rather than appends, so a replayed generation is
	// idempotent.
	require.NoError(t, store.AttachRenditions(ctx, original.ID, refs))
	got, err = store.GetByID(ctx, original.ID)
	require.NoError(t, err)
	require.Len(t, got.Renditions, 2)

	// Attaching to an original that does not exist is ErrNotFound.
	require.ErrorIs(t, store.AttachRenditions(ctx, blob.MustGenerateID(), refs), blob.ErrNotFound)
}
