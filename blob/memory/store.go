package memory

import (
	"bytes"
	"context"
	"sync"

	blobpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/blob/v1"

	"github.com/code-payments/flipcash2-server/blob"
)

type memory struct {
	sync.Mutex

	// blobs maps string(id.Value) to the stored record.
	blobs map[string]*blob.Blob
}

// NewInMemory returns an in-memory blob.Store for tests.
func NewInMemory() blob.Store {
	return &memory{
		blobs: make(map[string]*blob.Blob),
	}
}

func (m *memory) CreatePending(_ context.Context, b *blob.Blob) error {
	m.Lock()
	defer m.Unlock()

	key := string(b.ID.Value)
	if _, ok := m.blobs[key]; ok {
		return blob.ErrExists
	}

	m.blobs[key] = b.Clone()
	return nil
}

func (m *memory) GetByID(_ context.Context, id *blobpb.BlobId) (*blob.Blob, error) {
	m.Lock()
	defer m.Unlock()

	b, ok := m.blobs[string(id.Value)]
	if !ok {
		return nil, blob.ErrNotFound
	}
	return b.Clone(), nil
}

func (m *memory) GetByIDs(_ context.Context, ids []*blobpb.BlobId) ([]*blob.Blob, error) {
	m.Lock()
	defer m.Unlock()

	res := make([]*blob.Blob, 0, len(ids))
	seen := make(map[string]struct{}, len(ids))
	for _, id := range ids {
		key := string(id.Value)
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		if b, ok := m.blobs[key]; ok {
			res = append(res, b.Clone())
		}
	}
	return res, nil
}

func (m *memory) GetRenditions(_ context.Context, parentID *blobpb.BlobId) ([]*blob.Blob, error) {
	m.Lock()
	defer m.Unlock()

	var res []*blob.Blob
	for _, b := range m.blobs {
		if b.ParentID != nil && bytes.Equal(b.ParentID.Value, parentID.Value) {
			res = append(res, b.Clone())
		}
	}
	return res, nil
}

func (m *memory) Advance(_ context.Context, id *blobpb.BlobId, to blob.State, image *blob.ImageMetadata) (bool, error) {
	if to == blob.StateRejected {
		return false, blob.ErrCannotAdvanceToRejected
	}

	m.Lock()
	defer m.Unlock()

	b, ok := m.blobs[string(id.Value)]
	if !ok {
		return false, blob.ErrNotFound
	}

	// Advance strictly forward and never out of a terminal state; advancing to a
	// state the blob is already at or past is an idempotent no-op.
	if b.State.Terminal() || b.State >= to {
		return false, nil
	}

	b.State = to
	if image != nil {
		imageCopy := *image
		b.Image = &imageCopy
	}
	return true, nil
}

func (m *memory) Reject(_ context.Context, id *blobpb.BlobId, rejection *blob.RejectionMetadata) (bool, error) {
	m.Lock()
	defer m.Unlock()

	b, ok := m.blobs[string(id.Value)]
	if !ok {
		return false, blob.ErrNotFound
	}

	// Never overwrite a terminal blob; a concurrent or replayed reject is an
	// idempotent no-op that defers to the committed state.
	if b.State.Terminal() {
		return false, nil
	}

	b.State = blob.StateRejected
	if rejection != nil {
		rejectionCopy := *rejection
		b.Rejection = &rejectionCopy
	}
	return true, nil
}

func (m *memory) reset() {
	m.Lock()
	defer m.Unlock()

	m.blobs = make(map[string]*blob.Blob)
}
