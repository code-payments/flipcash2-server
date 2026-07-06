package cache_test

import (
	"context"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	blobpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/blob/v1"

	"github.com/code-payments/flipcash2-server/blob"
	"github.com/code-payments/flipcash2-server/blob/cache"
	"github.com/code-payments/flipcash2-server/blob/memory"
	"github.com/code-payments/flipcash2-server/blob/tests"
	"github.com/code-payments/flipcash2-server/model"
)

func TestBlob_CachingStore(t *testing.T) {
	tests.RunStoreTests(t, cache.NewInCache(memory.NewInMemory()), func() {})
}

// countingStore is a blob.Store that counts read calls (and how many ids each
// GetByIDs actually fetched) while delegating to a real backing store, so the
// tests can prove which reads were served from cache versus the backing.
type countingStore struct {
	blob.Store

	mu         sync.Mutex
	getByID    int
	getByIDs   int
	idsFetched int
}

func (s *countingStore) GetByID(ctx context.Context, id *blobpb.BlobId) (*blob.Blob, error) {
	s.mu.Lock()
	s.getByID++
	s.mu.Unlock()
	return s.Store.GetByID(ctx, id)
}

func (s *countingStore) GetByIDs(ctx context.Context, ids []*blobpb.BlobId) ([]*blob.Blob, error) {
	s.mu.Lock()
	s.getByIDs++
	s.idsFetched += len(ids)
	s.mu.Unlock()
	return s.Store.GetByIDs(ctx, ids)
}

func (s *countingStore) counts() (getByID, getByIDs, idsFetched int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.getByID, s.getByIDs, s.idsFetched
}

func newCache() (*countingStore, blob.Store) {
	backing := &countingStore{Store: memory.NewInMemory()}
	return backing, cache.NewInCache(backing)
}

func newPending(t *testing.T) *blob.Blob {
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

// seedReady creates a blob and advances it to the terminal READY state through
// the given store.
func seedReady(t *testing.T, ctx context.Context, store blob.Store) *blob.Blob {
	b := newPending(t)
	require.NoError(t, store.CreatePending(ctx, b))
	for _, to := range []blob.State{blob.StateUploaded, blob.StateInspected, blob.StatePromoted, blob.StateReady} {
		_, err := store.Advance(ctx, b.ID, to, nil)
		require.NoError(t, err)
	}
	return b
}

func TestCache_GetByID_CachesTerminal(t *testing.T) {
	ctx := context.Background()
	backing, c := newCache()

	b := seedReady(t, ctx, c)

	for range 3 {
		got, err := c.GetByID(ctx, b.ID)
		require.NoError(t, err)
		require.Equal(t, blob.StateReady, got.State)
	}

	// A terminal blob is immutable, so it is fetched once and served from cache
	// thereafter.
	getByID, _, _ := backing.counts()
	require.Equal(t, 1, getByID)
}

func TestCache_GetByID_DoesNotCacheNonTerminalAndReflectsAdvance(t *testing.T) {
	ctx := context.Background()
	backing, c := newCache()

	b := newPending(t)
	require.NoError(t, c.CreatePending(ctx, b))

	// A non-terminal (pending) blob is re-queried every time, never cached.
	for range 2 {
		got, err := c.GetByID(ctx, b.ID)
		require.NoError(t, err)
		require.Equal(t, blob.StatePending, got.State)
	}
	getByID, _, _ := backing.counts()
	require.Equal(t, 2, getByID)

	// Once it advances to a terminal state, the read reflects the new state (no
	// stale pending is served) and is cached from then on.
	for _, to := range []blob.State{blob.StateUploaded, blob.StateInspected, blob.StatePromoted, blob.StateReady} {
		_, err := c.Advance(ctx, b.ID, to, nil)
		require.NoError(t, err)
	}

	got, err := c.GetByID(ctx, b.ID)
	require.NoError(t, err)
	require.Equal(t, blob.StateReady, got.State) // fetched fresh, now cached

	got, err = c.GetByID(ctx, b.ID)
	require.NoError(t, err)
	require.Equal(t, blob.StateReady, got.State) // served from cache

	getByID, _, _ = backing.counts()
	require.Equal(t, 3, getByID)
}

func TestCache_GetByIDs_ServesHitsAndFetchesOnlyMisses(t *testing.T) {
	ctx := context.Background()
	backing, c := newCache()

	ready1 := seedReady(t, ctx, c)
	ready2 := seedReady(t, ctx, c)
	pending := newPending(t)
	require.NoError(t, c.CreatePending(ctx, pending))
	missing := blob.MustGenerateID()

	// Warm the cache for ready1.
	_, err := c.GetByID(ctx, ready1.ID)
	require.NoError(t, err)

	// A mixed batch: a cache hit (ready1), an uncached terminal (ready2), an
	// uncached non-terminal (pending), and a missing id.
	got, err := c.GetByIDs(ctx, []*blobpb.BlobId{ready1.ID, ready2.ID, pending.ID, missing})
	require.NoError(t, err)
	require.Len(t, got, 3) // the missing id is omitted

	// ready1 was served from cache, so only the three misses reach the backing.
	_, getByIDs, idsFetched := backing.counts()
	require.Equal(t, 1, getByIDs)
	require.Equal(t, 3, idsFetched)

	// ready2 is now cached (terminal); pending and missing are not. A second batch
	// of the two terminals is served entirely from cache.
	got, err = c.GetByIDs(ctx, []*blobpb.BlobId{ready1.ID, ready2.ID})
	require.NoError(t, err)
	require.Len(t, got, 2)
	_, getByIDs, _ = backing.counts()
	require.Equal(t, 1, getByIDs) // unchanged: no backing call

	// The still-pending blob keeps hitting the backing.
	_, err = c.GetByIDs(ctx, []*blobpb.BlobId{pending.ID})
	require.NoError(t, err)
	_, getByIDs, _ = backing.counts()
	require.Equal(t, 2, getByIDs)
}

func TestCache_GetByIDs_CollapsesDuplicates(t *testing.T) {
	ctx := context.Background()
	_, c := newCache()

	ready := seedReady(t, ctx, c)

	// A batch repeating the same id resolves to a single record, whether served
	// from the backing (first call) or the cache (second call).
	got, err := c.GetByIDs(ctx, []*blobpb.BlobId{ready.ID, ready.ID})
	require.NoError(t, err)
	require.Len(t, got, 1)

	got, err = c.GetByIDs(ctx, []*blobpb.BlobId{ready.ID, ready.ID})
	require.NoError(t, err)
	require.Len(t, got, 1)
}

func TestCache_GetByID_IsolatesCachedValueFromMutation(t *testing.T) {
	ctx := context.Background()
	backing, c := newCache()

	b := seedReady(t, ctx, c)

	// First read populates the cache; mutate the returned copy in place.
	got, err := c.GetByID(ctx, b.ID)
	require.NoError(t, err)
	got.MimeType = "mutated"
	got.Owner.Value[0] ^= 0xff

	// A subsequent read returns the pristine cached record, unaffected by the
	// caller's mutation.
	again, err := c.GetByID(ctx, b.ID)
	require.NoError(t, err)
	require.Equal(t, "image/png", again.MimeType)
	require.Equal(t, b.Owner.Value, again.Owner.Value)

	// And it was served from cache (a single backing fetch).
	getByID, _, _ := backing.counts()
	require.Equal(t, 1, getByID)
}
