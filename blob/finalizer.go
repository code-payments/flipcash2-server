package blob

import (
	"context"
	"errors"
	"image"

	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"

	blobpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/blob/v1"

	"github.com/code-payments/flipcash2-server/moderation"
)

// errBytesNotUploaded is an internal sentinel: finalization was asked to run but
// the bytes are not present in storage yet.
var errBytesNotUploaded = errors.New("bytes not uploaded")

// Finalizer drives an uploaded blob through the processing pipeline to a
// terminal state: confirm the upload landed, validate + derive metadata +
// moderate, copy into the origin store, generate renditions, and clean up. It is
// the single owner of that pipeline — the background worker runs it off the
// finalization queue (see Store.MarkForFinalization), while the RPCs only queue
// the work.
type Finalizer struct {
	log     *zap.Logger
	blobs   Store
	storage ObjectStorage

	// moderator classifies uploaded image bytes during finalization. It is
	// optional; when nil, moderation is skipped.
	moderator moderation.Client
}

// NewFinalizer returns a Finalizer over the given blob metadata store, object
// storage, and (optional) moderation client.
func NewFinalizer(
	log *zap.Logger,
	blobs Store,
	storage ObjectStorage,
	moderator moderation.Client,
) *Finalizer {
	return &Finalizer{
		log:       log,
		blobs:     blobs,
		storage:   storage,
		moderator: moderator,
	}
}

// Finalize drives a blob through its processing pipeline, resuming from whatever
// state it is already in: confirm the upload landed, validate + derive metadata +
// moderate, copy into the origin store, and clean up. Each step checkpoints its
// completed state, so a replay (a worker retry, or a concurrent worker) skips the
// steps already done — notably re-moderation and the copy. It is idempotent and
// safe to run concurrently for the same blob (the store's forward-only
// transitions resolve races), and returns the blob's resulting public status. A
// returned error means the pipeline stopped before a terminal state and the
// attempt should be retried.
func (f *Finalizer) Finalize(ctx context.Context, record *Blob) (blobpb.BlobStatus, error) {
	state := record.State
	if state.Terminal() {
		return state.ToBlobStatus(), nil
	}

	if record.ContentKind() != ContentKindImage {
		return blobpb.BlobStatus_BLOB_STATUS_UNKNOWN, errors.New("unsupported content kind for finalization")
	}

	var data []byte // the uploaded bytes, fetched once and reused across steps

	// The decoded original and its derived metadata, captured when the inspection
	// step runs so rendition generation can reuse them without decoding twice. Both
	// stay nil on a finalize that resumes past inspection, in which case the
	// generation step re-derives them from the still-present upload bytes.
	var decoded image.Image
	var imageMeta *ImageMetadata

	// Confirm the client's upload landed, then checkpoint StateUploaded.
	if state < StateUploaded {
		fetched, err := f.fetchUploaded(ctx, record)
		if err != nil {
			return blobpb.BlobStatus_BLOB_STATUS_UNKNOWN, err
		}
		data = fetched
		advanced, err := f.blobs.Advance(ctx, record.ID, StateUploaded, nil)
		if err != nil {
			return blobpb.BlobStatus_BLOB_STATUS_UNKNOWN, err
		}
		if !advanced {
			// Another finalizer moved the blob on; defer to it and report the
			// committed state rather than acting on our stale view.
			return f.currentStatus(ctx, record.ID)
		}
		state = StateUploaded
	}

	// Validate against the pinned contract, derive metadata, moderate, then
	// checkpoint StateInspected (persisting the metadata).
	if state < StateInspected {
		if data == nil {
			fetched, err := f.fetchUploaded(ctx, record)
			if err != nil {
				return blobpb.BlobStatus_BLOB_STATUS_UNKNOWN, err
			}
			data = fetched
		}

		// The declared size and type are immutable: if the stored bytes disagree
		// with either, the blob is rejected rather than corrected.
		if uint64(len(data)) != record.SizeBytes {
			// The stored bytes don't match the size pinned at reservation, so the
			// upload broke its declared size contract.
			return f.reject(ctx, record, &RejectionMetadata{Reason: RejectionReasonTooLarge})
		}
		inspection, err := InspectImage(data)
		if err != nil {
			// Undecodable, unsupported, or oversize bytes: not a servable image.
			return f.reject(ctx, record, &RejectionMetadata{Reason: rejectionReasonForInspection(err)})
		}
		if inspection.MimeType != record.MimeType {
			return f.reject(ctx, record, &RejectionMetadata{Reason: RejectionReasonMismatchedType})
		}
		if f.moderator != nil {
			// Moderate a size-bounded rendering, not the full-resolution original:
			// provider sync endpoints are tuned for small images and cap payload
			// size, and full resolution adds nothing to classification.
			payload, err := moderationPayload(data, inspection.Decoded)
			if err != nil {
				return blobpb.BlobStatus_BLOB_STATUS_UNKNOWN, err
			}
			result, err := f.moderator.ClassifyImage(ctx, payload)
			if err != nil {
				// Could not establish safety; leave the blob un-advanced so the
				// attempt can be retried rather than wrongly marking it servable.
				return blobpb.BlobStatus_BLOB_STATUS_UNKNOWN, err
			}
			if result.Flagged {
				return f.reject(ctx, record, &RejectionMetadata{
					Reason:          RejectionReasonModeration,
					FlaggedCategory: moderation.HighestFlaggedCategory(result),
				})
			}
		}

		advanced, err := f.blobs.Advance(ctx, record.ID, StateInspected, inspection.Metadata)
		if err != nil {
			return blobpb.BlobStatus_BLOB_STATUS_UNKNOWN, err
		}
		if !advanced {
			return f.currentStatus(ctx, record.ID)
		}
		// Carry the decoded image and derived metadata forward so the generation
		// step can derive renditions without re-reading and re-decoding the bytes.
		decoded = inspection.Decoded
		imageMeta = inspection.Metadata
		state = StateInspected
	}

	// Copy the original's bytes into the origin store, then checkpoint
	// StatePromoted. This is the durable source renditions will be derived from.
	if state < StatePromoted {
		if err := f.storage.CopyToOrigin(ctx, record.StorageKey); err != nil {
			return blobpb.BlobStatus_BLOB_STATUS_UNKNOWN, err
		}
		advanced, err := f.blobs.Advance(ctx, record.ID, StatePromoted, nil)
		if err != nil {
			return blobpb.BlobStatus_BLOB_STATUS_UNKNOWN, err
		}
		if !advanced {
			return f.currentStatus(ctx, record.ID)
		}
		state = StatePromoted
	}

	// Derive the renditions from the promoted original, then checkpoint
	// StateGeneratingRenditions. A blob is not client-ready until its renditions
	// exist, so this runs before READY. Generation is idempotent (each rendition has
	// a deterministic id), so a resumed finalize regenerates the same set harmlessly;
	// a failure here leaves the blob at StatePromoted for a retry rather than
	// rejecting an original that already passed moderation.
	//
	// Renditions are derived per content kind. Only images exist today, so this
	// dispatches straight to the image strategy; when another kind is added it is
	// inspected and generated on its own arm here, with its own ladder, rather than
	// through the image path.
	if state < StateGeneratingRenditions {
		if decoded == nil {
			// Resumed past inspection: the decoded image is no longer in hand. The
			// upload bytes are still present (cleanup runs only at READY), so re-read
			// and re-derive from them.
			if data == nil {
				fetched, err := f.fetchUploaded(ctx, record)
				if err != nil {
					return blobpb.BlobStatus_BLOB_STATUS_UNKNOWN, err
				}
				data = fetched
			}
			inspection, err := InspectImage(data)
			if err != nil {
				return blobpb.BlobStatus_BLOB_STATUS_UNKNOWN, err
			}
			decoded = inspection.Decoded
			imageMeta = inspection.Metadata
		}

		if err := f.generateImageRenditions(ctx, record, decoded, imageMeta); err != nil {
			return blobpb.BlobStatus_BLOB_STATUS_UNKNOWN, err
		}
		advanced, err := f.blobs.Advance(ctx, record.ID, StateGeneratingRenditions, nil)
		if err != nil {
			return blobpb.BlobStatus_BLOB_STATUS_UNKNOWN, err
		}
		if !advanced {
			return f.currentStatus(ctx, record.ID)
		}
		state = StateGeneratingRenditions
	}

	// Clean up the now-redundant upload bytes, then checkpoint StateReady. The
	// original is durably in the origin store, so the cleanup is best-effort and
	// must not hold the blob back from READY: a failure only orphans bytes the
	// upload bucket's lifecycle reclaims.
	if state < StateReady {
		f.cleanupUpload(ctx, record)
		advanced, err := f.blobs.Advance(ctx, record.ID, StateReady, nil)
		if err != nil {
			return blobpb.BlobStatus_BLOB_STATUS_UNKNOWN, err
		}
		if !advanced {
			return f.currentStatus(ctx, record.ID)
		}
		state = StateReady
	}

	return state.ToBlobStatus(), nil
}

// Fail terminally rejects a blob whose finalization attempts are exhausted, so
// the client sees a definitive (internal) rejection instead of an eternal
// PROCESSING. It is idempotent: a blob that reached a terminal state first keeps
// that state, and one already reclaimed (TTL) is a no-op.
func (f *Finalizer) Fail(ctx context.Context, id *blobpb.BlobId) error {
	record, err := f.blobs.GetByID(ctx, id)
	if errors.Is(err, ErrNotFound) {
		return nil
	} else if err != nil {
		return err
	}
	_, err = f.reject(ctx, record, &RejectionMetadata{Reason: RejectionReasonInternal})
	return err
}

// generateImageRenditions derives every rung of the IMAGE rendition ladder from
// the decoded original and stores each as its own READY blob: it scales and
// encodes the bytes, writes them straight into the origin store, and records a
// child blob whose ParentID points back at the original. meta supplies the
// original's persisted dimensions (so the derivation matches what the read path
// predicts), its BlurHash (intrinsic to the content, copied onto each rendition),
// and its alpha (which picks the output format).
//
// It is the image kind's strategy; another content kind has its own generator over
// its own ladder. It is idempotent: a rendition's id is a pure function of the
// original and the rung's output spec, so a replayed generation recreates the same
// id — overwriting the same object and treating an already-present record as the
// prior attempt to finish advancing — instead of orphaning a duplicate.
//
// The original is never upscaled. Within a role, the first rung whose bound reaches
// the original's longest side still yields ONE rendition, encoded at the original's
// own size and typed as that rung's role: even when its dimensions match the
// original, re-encoding as WebP typically saves bytes over the (possibly large,
// un-optimized, non-WebP) ORIGINAL, so a client always has a cheaper encoded variant
// to reach for at that role. Every larger rung of that SAME role would bound the same
// size to the same bytes, so they are skipped — but the ladder keeps climbing, so
// every role in it lands in the manifest for an image of any size.
func (f *Finalizer) generateImageRenditions(ctx context.Context, parent *Blob, decoded image.Image, meta *ImageMetadata) error {
	plans := planImageRenditions(meta)

	// Each planned rung derives, stores, and records independently — its output
	// spec was fully resolved by the plan — so the expensive part (resample +
	// encode + writes) runs concurrently and the generation's wall-clock cost is
	// the largest rung, not the ladder's sum. The decoded original is only ever
	// read. The fan-out is bounded by the ladder itself (a handful of rungs);
	// cross-blob concurrency is the worker's knob.
	//
	// refs is indexed, not appended, so the manifest keeps the plan's ladder
	// order (small to large) regardless of which rung finishes first. A failed
	// rung fails the whole generation; the retried finalize regenerates
	// idempotently.
	refs := make([]RenditionRef, len(plans))
	eg, egCtx := errgroup.WithContext(ctx)
	for i, plan := range plans {
		eg.Go(func() error {
			child, err := f.generateImageRendition(egCtx, parent, decoded, meta, plan)
			if err != nil {
				return err
			}
			refs[i] = renditionRef(child)
			return nil
		})
	}
	if err := eg.Wait(); err != nil {
		return err
	}

	// Record the manifest on the original so its whole rendition set resolves in the
	// single read that fetches it. The child records above are written first, so the
	// manifest only ever references renditions that already exist. It overwrites, so
	// a replay re-attaches the same set harmlessly.
	return f.blobs.AttachRenditions(ctx, parent.ID, refs)
}

// imageRenditionPlan is one ladder rung's fully resolved output spec: the role,
// target dimensions, and encoding a rendition will be derived with. It is
// computed from the original's metadata alone (see planImageRenditions), which
// is what lets the derivations themselves run in parallel.
type imageRenditionPlan struct {
	rendition RenditionType
	width     uint32
	height    uint32
	encoding  imageEncoding
}

// planImageRenditions resolves which rungs of the IMAGE ladder an original
// yields, in ladder order. It is a pure function of the original's metadata.
func planImageRenditions(meta *ImageMetadata) []imageRenditionPlan {
	// Roles whose largest useful rendition has already been planned — see the
	// reachedOriginal write below.
	coveredRoles := make(map[RenditionType]bool)

	plans := make([]imageRenditionPlan, 0, len(imageRenditionSpecs))
	for _, spec := range imageRenditionSpecs {
		// A rung whose role is already covered at the original's own size can only
		// re-encode those same bytes, so it is skipped. Skipping the rung rather than
		// ending the ladder is what keeps the remaining ROLES reachable: the ladder
		// carries several rungs per role, so a small original that tops out on the first
		// role's first rung must still climb to the later roles' rungs — each of which
		// emits at the original's size — instead of leaving those roles absent from the
		// manifest entirely.
		if coveredRoles[spec.Rendition] {
			continue
		}

		width, height := scaledDimensions(meta.Width, meta.Height, spec.MaxLongestSide)
		plans = append(plans, imageRenditionPlan{
			rendition: spec.Rendition,
			width:     width,
			height:    height,
			encoding:  imageEncodingFor(spec.Rendition, meta.HasAlpha),
		})

		// The first rung of a role whose bound is at or above the original's longest side
		// is the "next" rung the original doesn't exceed. It was planned at the original's
		// own size (scaledDimensions never upscales past the bound), and that role needs
		// nothing larger: every later rung of it would only re-encode the same bytes.
		if spec.MaxLongestSide >= max(meta.Width, meta.Height) {
			coveredRoles[spec.Rendition] = true
		}
	}
	return plans
}

// generateImageRendition derives a single planned rendition: it scales and
// encodes the original's decoded bytes per the plan, writes them into the
// origin store, and records the READY child blob, which it returns.
func (f *Finalizer) generateImageRendition(ctx context.Context, parent *Blob, decoded image.Image, meta *ImageMetadata, plan imageRenditionPlan) (*Blob, error) {
	id := imageRenditionID(parent.ID, plan.rendition, plan.width, plan.height, plan.encoding)
	key, err := imageRenditionStorageKey(parent.ID, plan.rendition, plan.width, plan.height, plan.encoding.mimeType)
	if err != nil {
		return nil, err
	}

	encoded, err := plan.encoding.encode(resampleImage(decoded, int(plan.width), int(plan.height)))
	if err != nil {
		return nil, err
	}

	// Bytes before record: the origin object must exist before anything can
	// reference the rendition. PutOrigin overwrites, so this is replay-safe.
	if err := f.storage.PutOrigin(ctx, key, plan.encoding.mimeType, encoded); err != nil {
		return nil, err
	}

	child := &Blob{
		ID:         id,
		Rendition:  plan.rendition,
		ParentID:   parent.ID,
		Owner:      parent.Owner,
		State:      StatePending,
		StorageKey: key,
		MimeType:   plan.encoding.mimeType,
		SizeBytes:  uint64(len(encoded)),
		Image: &ImageMetadata{
			Width:    plan.width,
			Height:   plan.height,
			Blurhash: meta.Blurhash,
			HasAlpha: meta.HasAlpha,
		},
	}
	// A replayed generation finds the record already present; treat that as the
	// previous attempt and drive it to READY rather than failing.
	if err := f.blobs.CreatePending(ctx, child); err != nil && !errors.Is(err, ErrExists) {
		return nil, err
	}
	if _, err := f.blobs.Advance(ctx, id, StateReady, child.Image); err != nil {
		return nil, err
	}
	return child, nil
}

// fetchUploaded reads a blob's uploaded bytes, translating an absent object into
// the errBytesNotUploaded sentinel.
func (f *Finalizer) fetchUploaded(ctx context.Context, record *Blob) ([]byte, error) {
	data, err := f.storage.GetUploaded(ctx, record.StorageKey)
	if errors.Is(err, ErrObjectNotFound) {
		return nil, errBytesNotUploaded
	} else if err != nil {
		return nil, err
	}
	return data, nil
}

func (f *Finalizer) reject(ctx context.Context, record *Blob, rejection *RejectionMetadata) (blobpb.BlobStatus, error) {
	advanced, err := f.blobs.Reject(ctx, record.ID, rejection)
	if err != nil {
		return blobpb.BlobStatus_BLOB_STATUS_UNKNOWN, err
	}
	if !advanced {
		// Another finalizer already drove the blob to a terminal state; report what
		// was actually committed rather than asserting REJECTED over it.
		return f.currentStatus(ctx, record.ID)
	}
	// Drop the rejected bytes from the upload store; they are never promoted.
	f.cleanupUpload(ctx, record)
	return blobpb.BlobStatus_BLOB_STATUS_REJECTED, nil
}

// rejectionReasonForInspection classifies an InspectImage failure into the
// rejection reason it should be recorded under. The byte-level validation
// failures are wrapped with sentinels; anything else (e.g. a downstream
// processing fault) is reported as internal.
func rejectionReasonForInspection(err error) RejectionReason {
	switch {
	case errors.Is(err, ErrImageUnsupportedType):
		return RejectionReasonUnsupportedType
	case errors.Is(err, ErrImageTooLarge):
		return RejectionReasonTooLarge
	case errors.Is(err, ErrImageCorrupt):
		return RejectionReasonCorrupt
	case errors.Is(err, ErrImagePrivacyMetadata):
		return RejectionReasonPrivacyMetadataPresent
	default:
		return RejectionReasonInternal
	}
}

// currentStatus re-reads a blob and returns its authoritative public status. It
// backs the lost-race paths in Finalize: when Advance reports it did not perform
// the transition, the local view is stale, so the committed state is read back
// rather than guessed.
func (f *Finalizer) currentStatus(ctx context.Context, id *blobpb.BlobId) (blobpb.BlobStatus, error) {
	record, err := f.blobs.GetByID(ctx, id)
	if err != nil {
		return blobpb.BlobStatus_BLOB_STATUS_UNKNOWN, err
	}
	return record.State.ToBlobStatus(), nil
}

// cleanupUpload best-effort removes a blob's bytes from the upload store after
// it reaches a terminal state. A failure here only leaves an orphan the upload
// bucket's lifecycle policy reclaims, so it is logged, not surfaced.
func (f *Finalizer) cleanupUpload(ctx context.Context, record *Blob) {
	if err := f.storage.DeleteUpload(ctx, record.StorageKey); err != nil {
		f.log.Warn("Failed to delete upload object after finalization",
			zap.String("blob_id", IDString(record.ID)),
			zap.Error(err),
		)
	}
}
