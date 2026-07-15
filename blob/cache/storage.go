package cache

import (
	"context"
	"time"

	"github.com/ReneKroon/ttlcache"
	"google.golang.org/protobuf/proto"

	blobpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/blob/v1"

	"github.com/code-payments/flipcash2-server/blob"
)

// StorageCache wraps a blob.ObjectStorage, caching the signed download URLs it
// mints. Every other operation passes straight through.
//
// Signing is the expensive part of resolving a blob: a CloudFront signed URL costs
// an RSA signature (~0.7ms), and a single chat feed page resolves one per member's
// avatar. Caching it matters because a download URL is NOT per-viewer — it signs the
// object's path and an expiry, nothing about who asked — so the URL minted for one
// reader is byte-identical to the one every other reader would get. The bytes behind
// a key are immutable too, so a cached URL can never point at stale content.
//
// Upload targets are deliberately NOT cached: a presigned upload is a bearer
// credential minted per reservation, pinned to that upload's key, MIME type and exact
// size, and reusing one across uploads would be wrong.
type StorageCache struct {
	storage blob.ObjectStorage
	urls    *ttlcache.Cache

	minRemaining time.Duration
}

// NewStorageCache returns a blob.ObjectStorage that caches signed download URLs in
// front of storage.
//
// minRemaining is the validity a cached URL is guaranteed to have left when it is
// handed out: an entry is only cached for its lifetime MINUS this margin, so it is
// re-minted before it can be served close to expiry. Set it comfortably above the
// time a client might sit on the URL before fetching (it must not receive one that
// expires mid-download); the cost of a larger margin is only a lower hit rate. A
// minRemaining at or above the signer's TTL disables caching entirely, since no
// entry could satisfy it.
func NewStorageCache(storage blob.ObjectStorage, minRemaining time.Duration) blob.ObjectStorage {
	return &StorageCache{
		storage:      storage,
		urls:         ttlcache.NewCache(),
		minRemaining: minRemaining,
	}
}

// SignDownloadURL returns a cached signed URL for key when one is held with enough
// life left in it, and otherwise mints and caches a fresh one. The returned proto is
// a copy, so a caller mutating it cannot corrupt the cached entry.
func (c *StorageCache) SignDownloadURL(ctx context.Context, key string) (*blobpb.DownloadUrl, error) {
	if cached, ok := c.urls.Get(key); ok {
		return proto.Clone(cached.(*blobpb.DownloadUrl)).(*blobpb.DownloadUrl), nil
	}

	signed, err := c.storage.SignDownloadURL(ctx, key)
	if err != nil {
		return nil, err
	}

	// Hold the entry only for as long as it can still be served with minRemaining to
	// spare; the cache expiring it is what forces the re-mint. A URL that is already
	// shorter-lived than the margin is returned but not cached.
	if ttl := time.Until(signed.ExpiresAt.AsTime()) - c.minRemaining; ttl > 0 {
		c.urls.SetWithTTL(key, proto.Clone(signed).(*blobpb.DownloadUrl), ttl)
	}
	return signed, nil
}

func (c *StorageCache) PresignUpload(ctx context.Context, key, mimeType string, sizeBytes uint64) (*blobpb.UploadTarget, error) {
	return c.storage.PresignUpload(ctx, key, mimeType, sizeBytes)
}

func (c *StorageCache) GetUploaded(ctx context.Context, key string) ([]byte, error) {
	return c.storage.GetUploaded(ctx, key)
}

func (c *StorageCache) CopyToOrigin(ctx context.Context, key string) error {
	return c.storage.CopyToOrigin(ctx, key)
}

func (c *StorageCache) PutOrigin(ctx context.Context, key, mimeType string, data []byte) error {
	return c.storage.PutOrigin(ctx, key, mimeType, data)
}

func (c *StorageCache) DeleteUpload(ctx context.Context, key string) error {
	return c.storage.DeleteUpload(ctx, key)
}

var _ blob.ObjectStorage = (*StorageCache)(nil)
