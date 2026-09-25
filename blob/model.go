package blob

import (
	"errors"
	"time"

	blobpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/blob/v1"
	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"
	moderationpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/moderation/v1"
	"github.com/google/uuid"
)

var (
	// ErrNotFound is returned when no blob exists for the given id.
	ErrNotFound = errors.New("blob not found")

	// ErrExists is returned when a blob with the given id already exists.
	ErrExists = errors.New("blob already exists")

	// ErrCannotAdvanceToRejected is returned by Store.Advance when StateRejected is
	// passed as the target. Rejection is terminal and carries metadata, so it is
	// reached only through Store.Reject, never Advance.
	ErrCannotAdvanceToRejected = errors.New("cannot advance to rejected state; use Reject")
)

// ContentKind identifies which processing family a blob's bytes belong to:
// which validation, moderation, and rendition pipeline they go through, and
// which finalization queue they wait in. It is derived from the blob's record —
// its pinned MIME type, or the fact that it is end-to-end encrypted — never
// stored on its own. Images and encrypted blobs are the only kinds supported
// today; video, audio, etc. each become their own kind — with their own queue
// and worker tuning — as they are added.
//
// The values are persisted (in finalization queue partition keys), so they must
// be stable forever.
type ContentKind int

const (
	ContentKindUnknown ContentKind = iota

	// ContentKindImage is a still image.
	ContentKindImage

	// ContentKindEncrypted is an end-to-end encrypted blob (see
	// Blob.EncryptedFor). The server cannot read its bytes, so its pipeline is
	// the shortest: confirm the size, promote, done — no inspection, moderation
	// or renditions.
	ContentKindEncrypted
)

// ContentKindForMimeType maps a declared MIME type to its processing family.
// An unsupported type maps to ContentKindUnknown, which nothing may be queued
// under. An encrypted blob's MIME type is the opaque application/octet-stream,
// which maps to unknown here: its kind comes from the record, not the type
// (see Blob.ContentKind).
func ContentKindForMimeType(mimeType string) ContentKind {
	if SupportedImageMimeTypes[mimeType] {
		return ContentKindImage
	}
	return ContentKindUnknown
}

// String names the kind for logs and metric dimensions.
func (k ContentKind) String() string {
	switch k {
	case ContentKindImage:
		return "image"
	case ContentKindEncrypted:
		return "encrypted"
	default:
		return "unknown"
	}
}

// ContentKind is the blob's processing family. An end-to-end encrypted blob is
// ContentKindEncrypted whatever its declared type says (the type is the opaque
// application/octet-stream); any other blob's kind is derived from its pinned
// MIME type.
func (b *Blob) ContentKind() ContentKind {
	if b.EncryptedFor != nil {
		return ContentKindEncrypted
	}
	return ContentKindForMimeType(b.MimeType)
}

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
	HasAlpha bool
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
// advanced to a higher-ranked state. A kind with less to do skips states rather
// than passing through them: an end-to-end encrypted blob has nothing to
// inspect and no renditions to generate, so it goes Uploaded → Promoted → Ready
// (see Finalizer.finalizeEncrypted).
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

// RejectionReason is the internal mirror of blobpb.RejectionReason: why a blob's
// uploaded bytes failed finalization. It is set only on a StateRejected blob, is
// immutable thereafter, and is its own type (persisted as its own value) so the
// stored representation does not depend on the wire enum's numbering.
type RejectionReason int

const (
	RejectionReasonUnknown RejectionReason = iota
	RejectionReasonModeration
	RejectionReasonUnsupportedType
	RejectionReasonMismatchedType
	RejectionReasonTooLarge
	RejectionReasonCorrupt
	RejectionReasonInternal
	RejectionReasonPrivacyMetadataPresent
)

// ToProto maps the internal reason onto the public blobpb.RejectionReason.
func (r RejectionReason) ToProto() blobpb.RejectionReason {
	switch r {
	case RejectionReasonModeration:
		return blobpb.RejectionReason_REJECTION_REASON_MODERATION
	case RejectionReasonUnsupportedType:
		return blobpb.RejectionReason_REJECTION_REASON_UNSUPPORTED_TYPE
	case RejectionReasonMismatchedType:
		return blobpb.RejectionReason_REJECTION_REASON_MISMATCHED_TYPE
	case RejectionReasonTooLarge:
		return blobpb.RejectionReason_REJECTION_REASON_TOO_LARGE
	case RejectionReasonCorrupt:
		return blobpb.RejectionReason_REJECTION_REASON_CORRUPT
	case RejectionReasonInternal:
		return blobpb.RejectionReason_REJECTION_REASON_INTERNAL
	case RejectionReasonPrivacyMetadataPresent:
		return blobpb.RejectionReason_REJECTION_REASON_PRIVACY_METADATA
	default:
		return blobpb.RejectionReason_REJECTION_REASON_UNKNOWN
	}
}

// RejectionMetadata records why a blob was rejected during finalization. It is
// set only on a StateRejected blob and is immutable thereafter.
type RejectionMetadata struct {
	Reason RejectionReason

	// FlaggedCategory is the moderation category that tripped, set only when
	// Reason is RejectionReasonModeration; it is NONE (the zero value) otherwise.
	FlaggedCategory moderationpb.FlaggedCategory
}

// ToProto renders the rejection metadata for the wire. A nil receiver renders to
// nil, so a non-rejected blob simply carries no rejection.
func (r *RejectionMetadata) ToProto() *blobpb.RejectionMetadata {
	if r == nil {
		return nil
	}
	return &blobpb.RejectionMetadata{
		Reason:          r.Reason.ToProto(),
		FlaggedCategory: r.FlaggedCategory,
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
//
// An end-to-end encrypted blob (EncryptedFor set) is the exception to "the
// server derives the metadata": its bytes are opaque, so the record carries
// only what the client declared (an application/octet-stream of SizeBytes)
// and the surface it was encrypted for, and finalization checks nothing but
// the size.
type Blob struct {
	ID *blobpb.BlobId

	// EncryptedFor is set when the blob's bytes are end-to-end encrypted for one
	// surface (blobpb.InitiateExternalUploadRequest.end_to_end_encrypted_for),
	// naming that surface as the principal its read grant will be made to — a
	// DM is PrincipalForChat(chat), the only surface today. It is pinned at
	// reservation and immutable. The server cannot read such a blob, so it
	// derives no metadata or renditions from it, never moderates it, grants the
	// principal read access in the step that makes it READY (see
	// Finalizer.finalizeEncrypted), and lets it be referenced only from
	// encrypted content on that surface: every other attach surface refuses it
	// (see validateAttachable). It is nil for an ordinary blob.
	//
	// It is a Principal rather than a chat id so that the pipeline, the stores
	// and the read paths stay surface-agnostic: a new oneof arm in the proto is
	// mapped to its principal, and given its own admission gate, in exactly one
	// place (Server.initiateEncryptedUpload) and finalizes through the same code.
	EncryptedFor *Principal

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

	// Renditions is the manifest of derived renditions, populated ONLY on an
	// ORIGINAL and only once its renditions have been generated. Each entry is a
	// compact, immutable copy of a child rendition blob's servable metadata,
	// denormalized onto the parent so a media's whole rendition set resolves in the
	// single read that fetches the original — no per-original index query. The child
	// rendition blobs remain the canonical, independently-addressable records (a
	// rendition id resolves through GetBlobs and inherits the parent's ACL); this is
	// purely a read-path manifest. It is nil on a rendition blob itself and on an
	// original whose ladder produced nothing.
	Renditions []RenditionRef

	// Rejection records why this blob was rejected, set only when State is
	// StateRejected; it is nil for any non-rejected blob.
	Rejection *RejectionMetadata
}

// RenditionRef is a single entry in an original's rendition manifest: the
// servable metadata of a derived rendition blob, enough to mint its wire
// Rendition (role, handle, image descriptors, and a freshly signed download URL)
// without reading the child blob record. Its fields mirror the same servable
// subset of a Blob — MimeType, SizeBytes, StorageKey, and the reused ImageMetadata
// — so a ref converts to the Blob that buildMetadata already understands (see
// asBlob). It never changes once written, because a rendition's bytes are
// immutable.
type RenditionRef struct {
	// ID is the child rendition blob's id — the same id GetBlobs resolves.
	ID *blobpb.BlobId

	// Rendition is which rendition role these bytes serve (display, thumbnail).
	Rendition RenditionType

	// MimeType is the rendition's own encoded type, which may differ from the
	// original's (e.g. an opaque PNG original yields JPEG renditions).
	MimeType string

	// SizeBytes is the size of the encoded rendition bytes.
	SizeBytes uint64

	// StorageKey is the object key the rendition's bytes live under, used to sign
	// its download URL. It never leaves the server.
	StorageKey string

	// Image is the rendition's derived image metadata — its own dimensions, and the
	// BlurHash copied from the original. Reuses the same type the original carries.
	Image *ImageMetadata
}

// asBlob adapts a manifest entry to the minimal Blob that buildMetadata reads, so
// a rendition's wire metadata is minted through exactly the same path as an
// original's rather than a parallel one.
func (r RenditionRef) asBlob(originalBlob *Blob) *Blob {
	return &Blob{
		ID:         r.ID,
		ParentID:   originalBlob.ID,
		Owner:      originalBlob.Owner,
		Rendition:  r.Rendition,
		StorageKey: r.StorageKey,
		MimeType:   r.MimeType,
		SizeBytes:  r.SizeBytes,
		Image:      r.Image,
		State:      StateReady,
	}
}

// renditionRef captures the servable subset of a (child rendition) blob as a
// manifest entry, so a parent's manifest is assembled directly from the child
// blobs generation just wrote.
func renditionRef(b *Blob) RenditionRef {
	return RenditionRef{
		ID:         b.ID,
		Rendition:  b.Rendition,
		MimeType:   b.MimeType,
		SizeBytes:  b.SizeBytes,
		StorageKey: b.StorageKey,
		Image:      b.Image,
	}
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
	if b.EncryptedFor != nil {
		cloned.EncryptedFor = &Principal{
			Type: b.EncryptedFor.Type,
			ID:   append([]byte(nil), b.EncryptedFor.ID...),
		}
	}
	if b.Image != nil {
		image := *b.Image
		cloned.Image = &image
	}
	if b.Renditions != nil {
		cloned.Renditions = make([]RenditionRef, len(b.Renditions))
		for i, ref := range b.Renditions {
			cloned.Renditions[i] = ref
			if ref.ID != nil {
				cloned.Renditions[i].ID = &blobpb.BlobId{Value: append([]byte(nil), ref.ID.Value...)}
			}
			if ref.Image != nil {
				image := *ref.Image
				cloned.Renditions[i].Image = &image
			}
		}
	}
	if b.Rejection != nil {
		rejection := *b.Rejection
		cloned.Rejection = &rejection
	}
	return cloned
}

// FinalizationQueueStats is a point-in-time gauge of one content kind's
// finalization queue.
type FinalizationQueueStats struct {
	// Depth is how many blobs are queued, due or not — a delayed retry is
	// still backlog.
	Depth uint64

	// OldestEnqueuedAt is when the longest-queued blob was FIRST enqueued; its
	// distance from now is the queue's max age. Re-marks and retry delays never
	// reset it, so it exposes a blob stuck cycling through backoff — which the
	// depth alone hides. It is the zero time when the queue is empty.
	OldestEnqueuedAt time.Time
}

// FinalizationTask is a queued unit of finalization work: a blob whose uploaded
// bytes are awaiting processing, with the retry bookkeeping the worker schedules
// off of.
type FinalizationTask struct {
	ID *blobpb.BlobId

	// Attempts is the number of failed finalization attempts recorded so far
	// (via DelayFinalization). The worker uses it to pace backoff and to stop
	// retrying an unfinalizable blob.
	Attempts uint32

	// NextAttemptAt is when the task next becomes due.
	NextAttemptAt time.Time
}

func MustGenerateID() *blobpb.BlobId {
	id, err := uuid.NewRandom()
	if err != nil {
		panic(err)
	}
	value := id
	return &blobpb.BlobId{Value: value[:]}
}

func IDString(id *blobpb.BlobId) string {
	if id == nil {
		return "<nil>"
	}
	parsed, err := uuid.FromBytes(id.Value)
	if err != nil {
		return "<invalid>"
	}
	return parsed.String()
}
