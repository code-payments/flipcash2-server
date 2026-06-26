package blob

import (
	"errors"

	blobpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/blob/v1"
	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"
	"github.com/google/uuid"
)

var (
	// ErrNotFound is returned when no blob exists for the given id.
	ErrNotFound = errors.New("blob not found")

	// ErrExists is returned when a blob with the given id already exists.
	ErrExists = errors.New("blob already exists")
)

// RenditionType identifies which rendition of a piece of media a blob holds.
// The ORIGINAL is the exact bytes the client uploaded; every other type is a
// variant the server derives from that original. It is a server-internal
// concept: clients only ever upload and reference ORIGINALs, and the server
// derives and serves the rest.
type RenditionType int

const (
	RenditionUnknown RenditionType = iota

	// RenditionOriginal is the exact bytes the client uploaded.
	RenditionOriginal

	// RenditionDisplay is a server-derived variant sized and optimized for
	// inline display — e.g. rendering the image inline within a chat message or
	// feed — rather than serving the full-resolution original.
	RenditionDisplay

	// RenditionThumbnail is a small, server-derived preview image — e.g. a
	// grid/list thumbnail — smaller than the display rendition.
	RenditionThumbnail
)

// ImageMetadata holds the server-derived, intrinsic descriptors of a still
// image. Every field is derived once from the stored bytes and is immutable.
//
// This is the IMAGE variant of a blob's kind-specific metadata. It is populated
// only for blobs whose bytes are an image; other content kinds (video, audio,
// ...) will each carry their own distinct metadata type, mirroring the
// blobpb.BlobMetadata.kind oneof. Images are simply the only kind supported
// today.
type ImageMetadata struct {
	Width    uint32
	Height   uint32
	Blurhash string
}

// State is the blob's internal, fine-grained lifecycle state. It records how far
// processing has progressed so an interrupted finalize can resume from the last
// completed checkpoint instead of repeating expensive steps — re-reading the
// bytes, re-deriving metadata, re-moderating, re-copying. It is deliberately
// finer-grained than the public blobpb.BlobStatus, which it maps onto, so the
// proto enum stays a derived view and this state is the source of truth.
//
// The success path advances strictly forward — Pending → Uploaded → Inspected →
// Promoted → GeneratingRenditions → Ready — with Rejected an alternative
// terminal. The ordering of the constants is significant: a blob is only ever
// advanced to a higher-ranked state.
type State int

const (
	// StatePending is a freshly reserved blob awaiting the client's upload.
	StatePending State = iota

	// StateUploaded means the client's upload is complete and the bytes are
	// present in the upload store. This is the signal a processing worker keys off
	// of to begin deriving metadata, moderating, and promoting the blob.
	StateUploaded

	// StateInspected means the uploaded bytes were validated against the declared
	// type/size, the metadata was derived, and moderation passed. The derived
	// metadata is persisted at this checkpoint, so resuming skips re-moderation.
	StateInspected

	// StatePromoted means the original's bytes were copied into the origin (CDN)
	// store. The blob is NOT client-ready yet — its renditions have not been
	// generated. Resuming skips the copy.
	StatePromoted

	// StateGeneratingRenditions means the original is in the origin store and the
	// server is deriving its renditions (display, thumbnail) from it. The blob is
	// still not client-ready, so clients do not see READY until this completes.
	// Reserved for when rendition generation is implemented; nothing enters it yet.
	StateGeneratingRenditions

	// StateReady means processing is complete — the renditions are generated and
	// the upload-store bytes have been cleaned up — so the blob is client-ready.
	// Terminal.
	StateReady

	// StateRejected means the bytes failed validation or moderation. Terminal.
	StateRejected
)

// Terminal reports whether no further processing is possible from this state.
func (s State) Terminal() bool {
	return s == StateReady || s == StateRejected
}

// ToBlobStatus maps the internal state onto the public lifecycle status. A blob
// is reported READY only once it is fully processed — its renditions generated —
// so a client never references it (e.g. in a message) before the renditions it
// will use exist.
func (s State) ToBlobStatus() blobpb.BlobStatus {
	switch s {
	case StatePending:
		return blobpb.BlobStatus_BLOB_STATUS_PENDING
	case StateUploaded, StateInspected, StatePromoted, StateGeneratingRenditions:
		return blobpb.BlobStatus_BLOB_STATUS_PROCESSING
	case StateReady:
		return blobpb.BlobStatus_BLOB_STATUS_READY
	case StateRejected:
		return blobpb.BlobStatus_BLOB_STATUS_REJECTED
	default:
		return blobpb.BlobStatus_BLOB_STATUS_UNKNOWN
	}
}

// Blob is the server-authoritative record for a stored blob. It is the durable
// identity behind a BlobId and tracks the blob through its lifecycle.
//
// The MimeType and SizeBytes are declared by the client on reservation and
// pinned into the signed upload policy, so storage rejects any upload that does
// not match them. They are immutable for the life of the blob: finalization
// re-validates the stored bytes against them and REJECTs the blob on any
// mismatch rather than overwriting them. Only the derived kind-specific
// metadata is filled in at finalization.
type Blob struct {
	ID *blobpb.BlobId

	// Rendition is which rendition of its media this blob holds. An ORIGINAL has
	// a nil ParentID; any other rendition type is a server-derived variant with
	// ParentID pointing at the original.
	Rendition RenditionType

	// ParentID is set when this blob is a server-derived rendition of another
	// blob; it points at the ORIGINAL the client uploaded. It is nil for an
	// ORIGINAL. Renditions are never uploaded by clients — the server derives
	// them from the original's bytes.
	ParentID *blobpb.BlobId

	Owner *commonpb.UserId

	// State is the blob's internal lifecycle state — the source of truth for how
	// far processing has progressed. The public blobpb.BlobStatus is derived from
	// it via State.ToBlobStatus.
	State State

	// StorageKey is the object key the bytes live under in the backing store. It
	// is derived from the ID and never leaves the server.
	StorageKey string

	// MimeType is the declared MIME type, pinned at reservation and immutable.
	MimeType string

	// SizeBytes is the declared size, pinned at reservation and immutable.
	SizeBytes uint64

	// Image is the derived IMAGE metadata, set only when this blob is an image
	// and READY. It is the image variant of the blob's kind-specific metadata;
	// as additional content kinds are supported they will be carried by their
	// own sibling fields here (e.g. Video, Audio), one per blobpb.BlobMetadata
	// kind variant. Only images exist today.
	Image *ImageMetadata
}

func newBlobID() (*blobpb.BlobId, error) {
	id, err := uuid.NewRandom()
	if err != nil {
		return nil, err
	}
	value := id
	return &blobpb.BlobId{Value: value[:]}, nil
}

func blobIDString(id *blobpb.BlobId) string {
	if id == nil {
		return "<nil>"
	}
	parsed, err := uuid.FromBytes(id.Value)
	if err != nil {
		return "<invalid>"
	}
	return parsed.String()
}
