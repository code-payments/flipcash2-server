package memory

import (
	"context"
	"sort"
	"sync"
	"time"

	blobpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/blob/v1"

	"github.com/code-payments/flipcash2-server/blob"
)

// queueEntry is a blob's finalization-queue bookkeeping: which content kind's
// queue it waits in, when it is next due, how many attempts have failed, and
// when it was first enqueued (never reset, so the queue's max age is
// observable). An entry exists exactly while the blob is queued; reaching a
// terminal state removes it.
type queueEntry struct {
	kind       blob.ContentKind
	attempts   uint32
	dueAt      time.Time
	enqueuedAt time.Time
}

type memory struct {
	sync.Mutex

	// blobs maps string(id.Value) to the stored record.
	blobs map[string]*blob.Blob

	// queue maps string(id.Value) to the blob's finalization-queue entry.
	queue map[string]*queueEntry
}

// NewInMemory returns an in-memory blob.Store for tests.
func NewInMemory() blob.Store {
	return &memory{
		blobs: make(map[string]*blob.Blob),
		queue: make(map[string]*queueEntry),
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

func (m *memory) AttachRenditions(_ context.Context, id *blobpb.BlobId, refs []blob.RenditionRef) error {
	m.Lock()
	defer m.Unlock()

	b, ok := m.blobs[string(id.Value)]
	if !ok {
		return blob.ErrNotFound
	}
	// Store on a cloned copy so the manifest deep-copies with the record; overwrite
	// any existing manifest so a replayed generation is idempotent.
	b.Renditions = refs
	m.blobs[string(id.Value)] = b.Clone()
	return nil
}

func (m *memory) Advance(_ context.Context, id *blobpb.BlobId, to blob.State, image *blob.ImageMetadata) (bool, error) {
	if to == blob.StateRejected {
		return false, blob.ErrCannotAdvanceToRejected
	}

	m.Lock()
	defer m.Unlock()

	key := string(id.Value)
	b, ok := m.blobs[key]
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
	// Reaching the terminal READY state dequeues the blob: the finalization work
	// is done.
	if to == blob.StateReady {
		delete(m.queue, key)
	}
	return true, nil
}

func (m *memory) Reject(_ context.Context, id *blobpb.BlobId, rejection *blob.RejectionMetadata) (bool, error) {
	m.Lock()
	defer m.Unlock()

	key := string(id.Value)
	b, ok := m.blobs[key]
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
	// Rejection is terminal: dequeue the blob along with the transition.
	delete(m.queue, key)
	return true, nil
}

func (m *memory) MarkForFinalization(_ context.Context, id *blobpb.BlobId, kind blob.ContentKind, nextAttemptAt time.Time) error {
	m.Lock()
	defer m.Unlock()

	key := string(id.Value)
	b, ok := m.blobs[key]
	if !ok {
		return blob.ErrNotFound
	}
	// The work behind a terminal blob is already done; there is nothing to queue.
	if b.State.Terminal() {
		return nil
	}

	// Re-marking resets the due time (and the queue, should the kind differ) but
	// preserves the attempt count and the original enqueue time, so a client
	// re-completing cannot wipe the backoff bookkeeping or hide the entry's age.
	if entry, ok := m.queue[key]; ok {
		entry.kind = kind
		entry.dueAt = nextAttemptAt
		return nil
	}
	m.queue[key] = &queueEntry{kind: kind, dueAt: nextAttemptAt, enqueuedAt: time.Now()}
	return nil
}

func (m *memory) GetDueForFinalization(_ context.Context, kind blob.ContentKind, asOf time.Time, limit int) ([]*blob.FinalizationTask, error) {
	m.Lock()
	defer m.Unlock()

	due := make([]*blob.FinalizationTask, 0)
	for key, entry := range m.queue {
		if entry.kind != kind || entry.dueAt.After(asOf) {
			continue
		}
		due = append(due, &blob.FinalizationTask{
			ID:            &blobpb.BlobId{Value: append([]byte(nil), key...)},
			Attempts:      entry.attempts,
			NextAttemptAt: entry.dueAt,
		})
	}
	sort.Slice(due, func(i, j int) bool { return due[i].NextAttemptAt.Before(due[j].NextAttemptAt) })
	if len(due) > limit {
		due = due[:limit]
	}
	return due, nil
}

func (m *memory) GetFinalizationQueueStats(_ context.Context, kind blob.ContentKind) (*blob.FinalizationQueueStats, error) {
	m.Lock()
	defer m.Unlock()

	stats := &blob.FinalizationQueueStats{}
	for _, entry := range m.queue {
		if entry.kind != kind {
			continue
		}
		stats.Depth++
		if stats.OldestEnqueuedAt.IsZero() || entry.enqueuedAt.Before(stats.OldestEnqueuedAt) {
			stats.OldestEnqueuedAt = entry.enqueuedAt
		}
	}
	return stats, nil
}

func (m *memory) ClaimForFinalization(_ context.Context, id *blobpb.BlobId, asOf, until time.Time) (bool, error) {
	m.Lock()
	defer m.Unlock()

	entry, ok := m.queue[string(id.Value)]
	if !ok || entry.dueAt.After(asOf) {
		return false, nil
	}
	entry.dueAt = until
	return true, nil
}

func (m *memory) DelayFinalization(_ context.Context, id *blobpb.BlobId, nextAttemptAt time.Time) error {
	m.Lock()
	defer m.Unlock()

	// A blob that left the queue (a concurrent finalize drove it terminal) has
	// nothing to reschedule.
	entry, ok := m.queue[string(id.Value)]
	if !ok {
		return nil
	}
	entry.attempts++
	entry.dueAt = nextAttemptAt
	return nil
}

func (m *memory) reset() {
	m.Lock()
	defer m.Unlock()

	m.blobs = make(map[string]*blob.Blob)
	m.queue = make(map[string]*queueEntry)
}
