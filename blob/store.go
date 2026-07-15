package blob

import (
	"context"

	blobpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/blob/v1"
)

// Store persists server-authoritative blob metadata. The bytes themselves live
// in an ObjectStorage; this store only tracks the lifecycle and derived
// metadata keyed by BlobId.
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
	// MimeType and SizeBytes are never changed.
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
	// must not be overwritten.
	//
	// ErrNotFound is returned if no blob exists for the given id.
	Reject(ctx context.Context, id *blobpb.BlobId, rejection *RejectionMetadata) (bool, error)
}
