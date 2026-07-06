package cache

import (
	"context"

	"github.com/ReneKroon/ttlcache"

	blobpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/blob/v1"

	"github.com/code-payments/flipcash2-server/blob"
)

// Cache wraps a blob.Store, caching only blobs that have reached a terminal
// state (Ready or Rejected).
type Cache struct {
	db    blob.Store
	blobs *ttlcache.Cache
}

// NewInCache returns a blob.Store that caches terminal blob records in front of
// db. It composes over any backing implementation.
func NewInCache(db blob.Store) blob.Store {
	return &Cache{
		db:    db,
		blobs: ttlcache.NewCache(),
	}
}

func (c *Cache) CreatePending(ctx context.Context, b *blob.Blob) error {
	return c.db.CreatePending(ctx, b)
}

func (c *Cache) GetByID(ctx context.Context, id *blobpb.BlobId) (*blob.Blob, error) {
	key := string(id.Value)
	if cached, ok := c.blobs.Get(key); ok {
		return cached.(*blob.Blob).Clone(), nil
	}

	b, err := c.db.GetByID(ctx, id)
	if err == nil && b.State.Terminal() {
		c.blobs.Set(key, b.Clone())
	}
	return b, err
}

func (c *Cache) GetByIDs(ctx context.Context, ids []*blobpb.BlobId) ([]*blob.Blob, error) {
	out := make([]*blob.Blob, 0, len(ids))
	seen := make(map[string]struct{}, len(ids))
	misses := make([]*blobpb.BlobId, 0, len(ids))
	for _, id := range ids {
		key := string(id.Value)
		// A BlobId resolves to at most one record, so collapse duplicate ids to a
		// single result, matching the underlying store's contract.
		if _, dup := seen[key]; dup {
			continue
		}
		seen[key] = struct{}{}

		if cached, ok := c.blobs.Get(key); ok {
			out = append(out, cached.(*blob.Blob).Clone())
			continue
		}
		misses = append(misses, id)
	}

	if len(misses) > 0 {
		fetched, err := c.db.GetByIDs(ctx, misses)
		if err != nil {
			return nil, err
		}
		for _, b := range fetched {
			if b.State.Terminal() {
				c.blobs.Set(string(b.ID.Value), b.Clone())
			}
			out = append(out, b)
		}
	}
	return out, nil
}

func (c *Cache) GetRenditions(ctx context.Context, parentID *blobpb.BlobId) ([]*blob.Blob, error) {
	return c.db.GetRenditions(ctx, parentID)
}

func (c *Cache) Advance(ctx context.Context, id *blobpb.BlobId, to blob.State, image *blob.ImageMetadata) (bool, error) {
	return c.db.Advance(ctx, id, to, image)
}

func (c *Cache) Reject(ctx context.Context, id *blobpb.BlobId, rejection *blob.RejectionMetadata) (bool, error) {
	return c.db.Reject(ctx, id, rejection)
}
