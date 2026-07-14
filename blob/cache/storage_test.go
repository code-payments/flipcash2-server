package cache_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/timestamppb"

	blobpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/blob/v1"

	"github.com/code-payments/flipcash2-server/blob"
	"github.com/code-payments/flipcash2-server/blob/cache"
	"github.com/code-payments/flipcash2-server/blob/memory"
)

// countingStorage is a blob.ObjectStorage that counts how many URLs it actually
// signed, so a test can prove which reads were served from cache. ttl controls the
// lifetime of the URLs it mints.
type countingStorage struct {
	blob.ObjectStorage

	mu    sync.Mutex
	signs int
	ttl   time.Duration
}

func newCountingStorage(ttl time.Duration) *countingStorage {
	return &countingStorage{ObjectStorage: memory.NewInMemoryStorage(), ttl: ttl}
}

func (s *countingStorage) SignDownloadURL(_ context.Context, key string) (*blobpb.DownloadUrl, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.signs++
	return &blobpb.DownloadUrl{
		// The signature is what makes each minting distinct; standing in for it with
		// the sign count lets a test tell a re-mint from a cache hit.
		Url:       "https://cdn.blobs.test/" + key + "?sig=" + time.Duration(s.signs).String(),
		ExpiresAt: timestamppb.New(time.Now().Add(s.ttl)),
	}, nil
}

func (s *countingStorage) signCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.signs
}

func TestBlob_StorageCache_SignsOncePerKey(t *testing.T) {
	ctx := context.Background()
	backing := newCountingStorage(time.Hour)
	storage := cache.NewStorageCache(backing, time.Minute)

	first, err := storage.SignDownloadURL(ctx, "images/a/original.jpg")
	require.NoError(t, err)

	// Repeated resolves of the same blob — the same avatar recurring across a chat
	// feed — are served from cache rather than re-signed.
	for range 10 {
		again, err := storage.SignDownloadURL(ctx, "images/a/original.jpg")
		require.NoError(t, err)
		require.Equal(t, first.Url, again.Url)
		require.Equal(t, first.ExpiresAt.AsTime(), again.ExpiresAt.AsTime())
	}
	require.Equal(t, 1, backing.signCount())

	// A different blob is a different key, so it is signed on its own.
	other, err := storage.SignDownloadURL(ctx, "images/b/original.jpg")
	require.NoError(t, err)
	require.NotEqual(t, first.Url, other.Url)
	require.Equal(t, 2, backing.signCount())
}

func TestBlob_StorageCache_ServedUrlKeepsMinimumLifetime(t *testing.T) {
	ctx := context.Background()

	// A URL whose life is barely longer than the margin: it may be cached, but only
	// for the sliver of time it still has minRemaining to spare.
	backing := newCountingStorage(150 * time.Millisecond)
	storage := cache.NewStorageCache(backing, 100*time.Millisecond)

	first, err := storage.SignDownloadURL(ctx, "images/a/original.jpg")
	require.NoError(t, err)
	require.Equal(t, 1, backing.signCount())

	// Past the point where the entry could still be handed out with the margin
	// intact, it is re-minted rather than served with too little life left.
	time.Sleep(80 * time.Millisecond)

	refreshed, err := storage.SignDownloadURL(ctx, "images/a/original.jpg")
	require.NoError(t, err)
	require.Equal(t, 2, backing.signCount())
	require.NotEqual(t, first.Url, refreshed.Url)

	// Whatever a caller gets back always has at least the margin left on it, so it
	// cannot expire mid-download.
	require.Greater(t, time.Until(refreshed.ExpiresAt.AsTime()), 100*time.Millisecond)
}

func TestBlob_StorageCache_ShortLivedUrlIsNotCached(t *testing.T) {
	ctx := context.Background()

	// The signer's TTL is below the margin, so no entry could ever satisfy it.
	// Caching is effectively disabled rather than serving a URL about to expire.
	backing := newCountingStorage(time.Second)
	storage := cache.NewStorageCache(backing, time.Minute)

	for i := 1; i <= 3; i++ {
		_, err := storage.SignDownloadURL(ctx, "images/a/original.jpg")
		require.NoError(t, err)
		require.Equal(t, i, backing.signCount())
	}
}

func TestBlob_StorageCache_DoesNotCacheUploadTargets(t *testing.T) {
	ctx := context.Background()
	backing := newCountingStorage(time.Hour)
	storage := cache.NewStorageCache(backing, time.Minute)

	// An upload target is a bearer credential pinned to one reservation, so it must
	// pass through to the backing every time rather than being reused.
	a, err := storage.PresignUpload(ctx, "images/a/original.jpg", "image/jpeg", 10)
	require.NoError(t, err)
	b, err := storage.PresignUpload(ctx, "images/b/original.jpg", "image/jpeg", 20)
	require.NoError(t, err)

	require.Equal(t, "images/a/original.jpg", a.FormFields["key"])
	require.Equal(t, "images/b/original.jpg", b.FormFields["key"])
	require.Equal(t, 0, backing.signCount())
}

func TestBlob_StorageCache_ReturnsCopies(t *testing.T) {
	ctx := context.Background()
	backing := newCountingStorage(time.Hour)
	storage := cache.NewStorageCache(backing, time.Minute)

	first, err := storage.SignDownloadURL(ctx, "images/a/original.jpg")
	require.NoError(t, err)

	// A caller mutating what it got back must not corrupt the entry everyone else
	// will be served.
	first.Url = "https://evil.test/"

	again, err := storage.SignDownloadURL(ctx, "images/a/original.jpg")
	require.NoError(t, err)
	require.Equal(t, "https://evil.test/", first.Url)
	require.NotEqual(t, first.Url, again.Url)
	require.Equal(t, 1, backing.signCount())
}
