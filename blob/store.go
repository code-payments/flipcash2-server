package blob

import (
	"context"
	"time"

	blobpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/blob/v1"
)

// Store persists server-authoritative blob metadata. The bytes themselves live
// in an ObjectStorage; this store only tracks the lifecycle and derived
// metadata keyed by BlobId.
//
// It also carries the finalization queues: the durable record of which blobs
// have uploaded bytes awaiting processing, which the background workers drain
// (Mark/GetDue/Claim/Delay below). There is one queue per ContentKind — each
// kind's pipeline is drained by its own worker, tuned to that kind's cost — and
// the queue is bookkeeping ON the blob record, so reaching a terminal state
// removes a blob from its queue atomically. The store does not interpret the
// kind; callers queue a blob under the kind they derived from it.
type Store interface {
	// CreatePending inserts a freshly reserved blob in the PENDING state. The
	// blob may be an ORIGINAL (nil ParentID) or a server-created rendition of an
	// existing original (ParentID set).
	//
	// ErrExists is returned if a blob with the same id already exists.
	CreatePending(ctx context.Context, blob *Blob) error

	// GetByID returns the blob with the given id, or ErrNotFound. Ownership is
	// not enforced here; callers that act on behalf of the uploader (e.g.
	// finalization) compare Owner themselves.
	GetByID(ctx context.Context, id *blobpb.BlobId) (*Blob, error)

	// GetByIDs returns the blobs among the given ids that exist, in unspecified
	// order. A BlobId is an opaque capability, so resolution is not scoped to an
	// owner; ids that do not exist are simply omitted. A BlobId resolves to at
	// most one record, so duplicate ids collapse to a single result. An original's
	// rendition manifest (Blob.Renditions) is included.
	GetByIDs(ctx context.Context, ids []*blobpb.BlobId) ([]*Blob, error)

	// AttachRenditions records an original's rendition manifest onto its own
	// record, so the whole set resolves in the single read that fetches the
	// original. It overwrites any manifest already present, so a replayed
	// generation that recomputes the same set is idempotent. It is only ever
	// called on an ORIGINAL, and refs may be empty (an original whose ladder
	// produced nothing).
	//
	// ErrNotFound is returned if no blob exists for the given id.
	AttachRenditions(ctx context.Context, id *blobpb.BlobId, refs []RenditionRef) error

	// Advance moves a blob forward along the success path to a later lifecycle
	// state, persisting derived metadata when provided (image is set only on the
	// transition into StateInspected). It advances strictly forward and never out
	// of a terminal state, so a replayed or concurrent finalize is idempotent:
	// advancing to a state the blob is already at or past is a no-op. The declared
	// MimeType and SizeBytes are never changed. Reaching StateReady also removes
	// the blob from the finalization queue, atomically with the transition.
	//
	// StateRejected is not a valid target — rejection is terminal and carries
	// metadata, so it is reached only through Reject. Passing it returns
	// ErrCannotAdvanceToRejected.
	//
	// It reports whether this call actually performed the transition. A false
	// return with a nil error means the blob was already at or past the target
	// (or terminal) — a concurrent or replayed finalize lost the race — so the
	// caller can stop instead of applying further side effects on a stale view.
	//
	// ErrNotFound is returned if no blob exists for the given id.
	Advance(ctx context.Context, id *blobpb.BlobId, to State, image *ImageMetadata) (bool, error)

	// Reject moves a non-terminal blob to the terminal StateRejected, recording
	// why. Like Advance it transitions only out of a non-terminal state and is
	// idempotent: it reports whether it performed the transition, and a false with
	// a nil error means the blob was already terminal — a concurrent or replayed
	// finalize won the race, so the committed rejection (or readiness) stands and
	// must not be overwritten. Rejection also removes the blob from the
	// finalization queue, atomically with the transition.
	//
	// ErrNotFound is returned if no blob exists for the given id.
	Reject(ctx context.Context, id *blobpb.BlobId, rejection *RejectionMetadata) (bool, error)

	// MarkForFinalization queues a blob on its content kind's finalization
	// queue, due at nextAttemptAt. It is idempotent: re-marking an already-queued
	// blob resets its due time (and moves it if the kind changed, though a blob's
	// kind never legitimately changes) but preserves its failed-attempt count,
	// and marking a blob that already reached a terminal state is a no-op (the
	// work is done, so nothing is queued).
	//
	// ErrNotFound is returned if no blob exists for the given id.
	MarkForFinalization(ctx context.Context, id *blobpb.BlobId, kind ContentKind, nextAttemptAt time.Time) error

	// GetDueForFinalization returns up to limit blobs queued under kind whose due
	// time is at or before asOf, soonest first.
	GetDueForFinalization(ctx context.Context, kind ContentKind, asOf time.Time, limit int) ([]*FinalizationTask, error)

	// GetFinalizationQueueStats reports kind's queue depth and the enqueue time
	// of its longest-queued blob, in one walk of the queue. It backs the queue
	// gauges: depth trending up faster than workers drain it means the queue is
	// growing, and a max age climbing while depth stays flat means something is
	// stuck retrying rather than merely busy.
	GetFinalizationQueueStats(ctx context.Context, kind ContentKind) (*FinalizationQueueStats, error)

	// ClaimForFinalization pushes a queued blob's due time out to until, provided
	// it is still queued and due as of asOf. It reports whether the claim was
	// performed: false means the blob left the queue (it reached a terminal
	// state) or is no longer due (another worker claimed or delayed it first).
	//
	// A claim is an efficiency guard, not a lock — finalization is idempotent and
	// safe to run concurrently; claiming only keeps workers from duplicating
	// expensive work (e.g. moderation calls). A crashed claimant needs no
	// recovery: the claim expires when its until passes and the task simply
	// becomes due again.
	ClaimForFinalization(ctx context.Context, id *blobpb.BlobId, asOf, until time.Time) (bool, error)

	// DelayFinalization reschedules a queued blob after a failed attempt: its due
	// time moves to nextAttemptAt and its failed-attempt count increments. It is
	// a no-op on a blob that is no longer queued (a concurrent finalize drove it
	// terminal, dequeuing it).
	DelayFinalization(ctx context.Context, id *blobpb.BlobId, nextAttemptAt time.Time) error
}
