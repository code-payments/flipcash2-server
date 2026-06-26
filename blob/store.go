package blob

import (
	"context"

	blobpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/blob/v1"
	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"
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
	// most one record, so duplicate ids collapse to a single result.
	GetByIDs(ctx context.Context, ids []*blobpb.BlobId) ([]*Blob, error)

	// GetRenditions returns every blob whose ParentID is parentID — i.e. all
	// server-derived renditions of the given original — in unspecified order. It
	// returns an empty slice (not ErrNotFound) when there are none.
	GetRenditions(ctx context.Context, parentID *blobpb.BlobId) ([]*Blob, error)

	// Advance moves a blob forward to a later lifecycle state, persisting derived
	// metadata when provided (image is set only on the transition into
	// StateInspected). It advances strictly forward and never out of a terminal
	// state, so a replayed or concurrent finalize is idempotent: advancing to a
	// state the blob is already at or past is a no-op. The declared MimeType and
	// SizeBytes are never changed.
	//
	// It reports whether this call actually performed the transition. A false
	// return with a nil error means the blob was already at or past the target
	// (or terminal) — a concurrent or replayed finalize lost the race — so the
	// caller can stop instead of applying further side effects on a stale view.
	//
	// ErrNotFound is returned if no blob exists for the given id.
	Advance(ctx context.Context, id *blobpb.BlobId, to State, image *ImageMetadata) (bool, error)
}

// Clone returns a deep copy of the blob, so stores can hand out values callers
// cannot mutate in place.
func (b *Blob) Clone() *Blob {
	if b == nil {
		return nil
	}

	cloned := &Blob{
		Rendition:  b.Rendition,
		State:      b.State,
		StorageKey: b.StorageKey,
		MimeType:   b.MimeType,
		SizeBytes:  b.SizeBytes,
	}
	if b.ID != nil {
		cloned.ID = &blobpb.BlobId{Value: append([]byte(nil), b.ID.Value...)}
	}
	if b.ParentID != nil {
		cloned.ParentID = &blobpb.BlobId{Value: append([]byte(nil), b.ParentID.Value...)}
	}
	if b.Owner != nil {
		cloned.Owner = &commonpb.UserId{Value: append([]byte(nil), b.Owner.Value...)}
	}
	if b.Image != nil {
		image := *b.Image
		cloned.Image = &image
	}
	return cloned
}
