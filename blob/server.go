package blob

import (
	"bytes"
	"context"
	"errors"
	"time"

	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	blobpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/blob/v1"
	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"

	"github.com/code-payments/flipcash2-server/account"
	"github.com/code-payments/flipcash2-server/auth"
	"github.com/code-payments/flipcash2-server/model"
	"github.com/code-payments/flipcash2-server/moderation"
)

// finalizeTimeout bounds the detached finalization that CompleteExternalUpload
// drives. Finalization is dominated by moderation (an external call), so it is
// generous, but bounded so a wedged dependency cannot leak work forever.
const finalizeTimeout = 60 * time.Second

// errBytesNotUploaded is an internal sentinel: finalization was asked to run but
// the bytes are not present in storage yet.
var errBytesNotUploaded = errors.New("bytes not uploaded")

type Server struct {
	log *zap.Logger

	authz auth.Authorizer

	accounts account.Store
	blobs    Store
	storage  ObjectStorage

	// access holds the blob ACL grants; resolver resolves a grant's principal to
	// concrete coverage (e.g. chat membership). Together they back the non-owner
	// read path in GetBlobs.
	access   AccessStore
	resolver PrincipalResolver

	// moderator classifies uploaded image bytes during finalization. It is
	// optional; when nil, moderation is skipped.
	moderator moderation.Client

	requireStaff bool

	blobpb.UnimplementedBlobStorageServer
}

func NewServer(
	log *zap.Logger,
	authz auth.Authorizer,
	accounts account.Store,
	blobs Store,
	storage ObjectStorage,
	access AccessStore,
	resolver PrincipalResolver,
	moderator moderation.Client,
	requireStaff bool,
) *Server {
	return &Server{
		log:          log,
		authz:        authz,
		accounts:     accounts,
		blobs:        blobs,
		storage:      storage,
		access:       access,
		resolver:     resolver,
		moderator:    moderator,
		requireStaff: requireStaff,
	}
}

// GetUploadPolicy returns the upload constraints in force for the caller. It is
// advisory and cacheable: a client uses it to validate and resize before
// reserving an upload, but InitiateExternalUpload remains authoritative. Access
// is gated identically to initiating an upload, so a caller who could not upload
// does not receive a policy.
func (s *Server) GetUploadPolicy(ctx context.Context, req *blobpb.GetUploadPolicyRequest) (*blobpb.GetUploadPolicyResponse, error) {
	owner, err := s.authz.Authorize(ctx, req, &req.Auth)
	if err != nil {
		return nil, err
	}

	log := s.log.With(zap.String("owner_id", model.UserIDString(owner)))

	allowed, err := s.uploadAllowed(ctx, owner, log)
	if err != nil {
		return nil, err
	}
	if !allowed {
		return &blobpb.GetUploadPolicyResponse{Result: blobpb.GetUploadPolicyResponse_DENIED}, nil
	}

	return &blobpb.GetUploadPolicyResponse{
		Result: blobpb.GetUploadPolicyResponse_OK,
		Policy: currentPolicy,
	}, nil
}

func (s *Server) InitiateExternalUpload(ctx context.Context, req *blobpb.InitiateExternalUploadRequest) (*blobpb.InitiateExternalUploadResponse, error) {
	owner, err := s.authz.Authorize(ctx, req, &req.Auth)
	if err != nil {
		return nil, err
	}

	log := s.log.With(
		zap.String("owner_id", model.UserIDString(owner)),
		zap.String("mime_type", req.MimeType),
		zap.Uint64("size_bytes", req.SizeBytes),
	)

	// Uploads are gated on registration (and, while the feature is staff-gated, on
	// staff), like other write paths.
	allowed, err := s.uploadAllowed(ctx, owner, log)
	if err != nil {
		return nil, err
	}
	if !allowed {
		return &blobpb.InitiateExternalUploadResponse{Result: blobpb.InitiateExternalUploadResponse_DENIED}, nil
	}

	// The declared type and size become the immutable, pinned contract for the
	// upload. Reject anything we would not accept up front rather than after the
	// bytes land, surfacing the specific reason so the client can react instead of
	// guessing at a generic denial. A policy-driven denial echoes the policy
	// version so a client running on a stale cached policy knows to re-fetch.
	if !SupportedImageMimeTypes[req.MimeType] {
		log.Debug("Rejecting upload of unsupported mime type")
		return &blobpb.InitiateExternalUploadResponse{
			Result:        blobpb.InitiateExternalUploadResponse_UNSUPPORTED_TYPE,
			PolicyVersion: currentPolicyVersion,
		}, nil
	}

	if req.SizeBytes > MaxOriginalImageSizeBytes {
		log.Debug("Rejecting oversize upload")
		return &blobpb.InitiateExternalUploadResponse{
			Result:        blobpb.InitiateExternalUploadResponse_TOO_LARGE,
			PolicyVersion: currentPolicyVersion,
		}, nil
	}

	id, err := newBlobID()
	if err != nil {
		log.Warn("Failed to generate blob id", zap.Error(err))
		return nil, status.Error(codes.Internal, "failed to initiate upload")
	}
	// The mime type was validated as a supported image above, so this resolves; an
	// error here would mean the two lists drifted out of sync.
	key, err := StorageKey(id, req.MimeType)
	if err != nil {
		log.Warn("Failed to derive storage key", zap.Error(err))
		return nil, status.Error(codes.Internal, "failed to initiate upload")
	}

	target, err := s.storage.PresignUpload(ctx, key, req.MimeType, req.SizeBytes)
	if err != nil {
		log.Warn("Failed to presign upload target", zap.Error(err))
		return nil, status.Error(codes.Internal, "failed to initiate upload")
	}

	record := &Blob{
		ID:         id,
		Rendition:  RenditionOriginal,
		Owner:      owner,
		State:      StatePending,
		StorageKey: key,
		MimeType:   req.MimeType,
		SizeBytes:  req.SizeBytes,
	}
	if err := s.blobs.CreatePending(ctx, record); err != nil {
		log.Warn("Failed to reserve blob", zap.Error(err))
		return nil, status.Error(codes.Internal, "failed to initiate upload")
	}

	return &blobpb.InitiateExternalUploadResponse{
		Result:       blobpb.InitiateExternalUploadResponse_OK,
		BlobId:       id,
		UploadTarget: target,
	}, nil
}

// uploadAllowed reports whether the caller may upload: they must be registered,
// and — while the feature is staff-gated — staff. A false with a nil error is a
// clean denial; a non-nil error is an internal failure already logged and shaped
// for return to the client.
func (s *Server) uploadAllowed(ctx context.Context, owner *commonpb.UserId, log *zap.Logger) (bool, error) {
	isRegistered, err := s.accounts.IsRegistered(ctx, owner)
	if err != nil {
		log.Warn("Failed to get registration flag", zap.Error(err))
		return false, status.Error(codes.Internal, "failed to get registration flag")
	}
	if !isRegistered {
		return false, nil
	}

	if s.requireStaff {
		isStaff, err := s.accounts.IsStaff(ctx, owner)
		if err != nil {
			log.Warn("Failed to get staff flag", zap.Error(err))
			return false, status.Error(codes.Internal, "failed to get staff flag")
		}
		if !isStaff {
			return false, nil
		}
	}

	return true, nil
}

func (s *Server) CompleteExternalUpload(ctx context.Context, req *blobpb.CompleteExternalUploadRequest) (*blobpb.CompleteExternalUploadResponse, error) {
	owner, err := s.authz.Authorize(ctx, req, &req.Auth)
	if err != nil {
		return nil, err
	}

	log := s.log.With(
		zap.String("owner_id", model.UserIDString(owner)),
		zap.String("blob_id", blobIDString(req.BlobId)),
	)

	record, err := s.blobs.GetByID(ctx, req.BlobId)
	if errors.Is(err, ErrNotFound) {
		return &blobpb.CompleteExternalUploadResponse{Result: blobpb.CompleteExternalUploadResponse_NOT_FOUND}, nil
	} else if err != nil {
		log.Warn("Failed to load blob", zap.Error(err))
		return nil, status.Error(codes.Internal, "failed to complete upload")
	}

	// Completion is scoped to the uploader: another user holding the id cannot
	// finalize someone else's pending upload.
	if record.Owner == nil || !bytes.Equal(record.Owner.Value, owner.Value) {
		return &blobpb.CompleteExternalUploadResponse{Result: blobpb.CompleteExternalUploadResponse_NOT_FOUND}, nil
	}

	// Finalization mutates storage and the DB; detach it from the request context
	// so a client that drops the (advisory) RPC mid-flight cannot abort it and
	// leave the blob half-promoted. It still runs under a bounded timeout.
	finalizeCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), finalizeTimeout)
	defer cancel()

	finalStatus, err := s.finalize(finalizeCtx, record)
	if errors.Is(err, errBytesNotUploaded) {
		return &blobpb.CompleteExternalUploadResponse{Result: blobpb.CompleteExternalUploadResponse_NOT_UPLOADED}, nil
	} else if err != nil {
		log.Warn("Failed to finalize blob", zap.Error(err))
		return nil, status.Error(codes.Internal, "failed to complete upload")
	}

	// Finalization is driven entirely by this RPC for now — there is no background
	// worker — so the only way a blob reaches a terminal state is a client
	// completing it. A non-terminal status here means a concurrent finalize won the
	// race and is still in flight, so the blob is not yet resolved: surface a
	// retryable error rather than reporting a misleading status. The retry resumes
	// from the last checkpoint. TODO: once a worker drives finalization, return the
	// in-progress status to the client instead of erroring.
	if finalStatus != blobpb.BlobStatus_BLOB_STATUS_READY && finalStatus != blobpb.BlobStatus_BLOB_STATUS_REJECTED {
		log.Warn("Blob not yet finalized on completion", zap.String("status", finalStatus.String()))
		return nil, status.Error(codes.Unavailable, "upload not yet finalized")
	}

	// finalize reports only the status; the rejection metadata it recorded lives on
	// the (now terminal) record, so read it back to surface why. The committed
	// record is authoritative even when a concurrent finalize won the race.
	var rejectionMetadata *blobpb.RejectionMetadata
	if finalStatus == blobpb.BlobStatus_BLOB_STATUS_REJECTED {
		rejected, err := s.blobs.GetByID(ctx, record.ID)
		if err != nil {
			log.Warn("Failed to load rejection metadata", zap.Error(err))
			return nil, status.Error(codes.Internal, "failed to complete upload")
		}
		rejectionMetadata = rejected.Rejection.ToProto()
	}

	return &blobpb.CompleteExternalUploadResponse{
		Result:            blobpb.CompleteExternalUploadResponse_OK,
		Status:            finalStatus,
		RejectionMetadata: rejectionMetadata,
	}, nil
}

func (s *Server) GetBlobs(ctx context.Context, req *blobpb.GetBlobsRequest) (*blobpb.GetBlobsResponse, error) {
	caller, err := s.authz.Authorize(ctx, req, &req.Auth)
	if err != nil {
		return nil, err
	}

	if req.BlobIds == nil || len(req.BlobIds.BlobIds) == 0 {
		return &blobpb.GetBlobsResponse{Result: blobpb.GetBlobsResponse_OK}, nil
	}

	records, err := s.blobs.GetByIDs(ctx, req.BlobIds.BlobIds)
	if err != nil {
		s.log.Warn("Failed to resolve blobs", zap.Error(err))
		return nil, status.Error(codes.Internal, "failed to get blobs")
	}

	resolved := make([]*blobpb.Blob, 0, len(records))
	for _, record := range records {
		allowed, err := s.canRead(ctx, caller, record, req.Context)
		if err != nil {
			s.log.Warn("Failed to authorize blob",
				zap.String("blob_id", blobIDString(record.ID)),
				zap.Error(err),
			)
			return nil, status.Error(codes.Internal, "failed to get blobs")
		}
		if !allowed {
			// A record the caller may not read is skipped, leaving it
			// indistinguishable from one that does not exist.
			continue
		}

		blobStatus := record.State.ToBlobStatus()
		protoBlob := &blobpb.Blob{
			Id:     record.ID,
			Status: blobStatus,
		}

		switch blobStatus {
		case blobpb.BlobStatus_BLOB_STATUS_READY:
			// download_url and the rest of the metadata are only meaningful, and only
			// minted, for a servable (READY) blob.
			metadata, err := buildMetadata(ctx, s.storage, record)
			if err != nil {
				s.log.Warn("Failed to mint blob metadata",
					zap.String("blob_id", blobIDString(record.ID)),
					zap.Error(err),
				)
				return nil, status.Error(codes.Internal, "failed to get blobs")
			}
			protoBlob.Metadata = metadata
		case blobpb.BlobStatus_BLOB_STATUS_REJECTED:
			protoBlob.Rejection = record.Rejection.ToProto()
		}

		resolved = append(resolved, protoBlob)
	}

	resp := &blobpb.GetBlobsResponse{Result: blobpb.GetBlobsResponse_OK}
	// The batch is left unset (not empty) when nothing resolves.
	if len(resolved) > 0 {
		resp.Blobs = &blobpb.BlobBatch{Blobs: resolved}
	}
	return resp, nil
}

// canRead reports whether caller may read record. The blob's owner always may,
// and needs no access context. Any other caller must present an access context
// that authorizes the blob: the blob must carry a read grant for the context's
// principal AND the caller must be covered by that principal (e.g. be a member
// of the chat). Both are required — coverage alone would let anyone in the scope
// read any blob id they can guess, and the grant alone would ignore who is
// asking. A caller who may not read is reported (false, nil) so GetBlobs skips
// the record, leaving it indistinguishable from one that does not exist.
func (s *Server) canRead(ctx context.Context, caller *commonpb.UserId, record *Blob, accessContext *blobpb.AccessContext) (bool, error) {
	if record.Owner != nil && bytes.Equal(record.Owner.Value, caller.Value) {
		return true, nil
	}
	if accessContext == nil {
		// A non-owner read requires a context; without one the blob is unauthorized.
		return false, nil
	}

	principal, ok := principalForAccessContext(accessContext)
	if !ok {
		// An unknown or empty access scope authorizes nothing.
		return false, nil
	}

	// Grants are made against the ORIGINAL; a server-derived rendition inherits
	// its original's grants.
	resourceID := record.ID
	if record.ParentID != nil {
		resourceID = record.ParentID
	}

	granted, err := s.access.HasGrant(ctx, resourceID, principal, PermissionRead)
	if err != nil {
		return false, err
	}
	if !granted {
		return false, nil
	}
	return s.resolver.Covers(ctx, principal, caller)
}

// principalForAccessContext maps a request's access context to the principal a
// grant for that surface is made to. It returns ok=false for an unknown or empty
// scope, which the caller treats as authorizing nothing.
func principalForAccessContext(accessContext *blobpb.AccessContext) (Principal, bool) {
	switch scope := accessContext.GetScope().(type) {
	case *blobpb.AccessContext_Chat:
		return PrincipalForChat(scope.Chat), true
	default:
		return Principal{}, false
	}
}

// finalize drives a blob through its processing pipeline, resuming from whatever
// state it is already in: confirm the upload landed, validate + derive metadata +
// moderate, copy into the origin store, and clean up. Each step checkpoints its
// completed state, so a replay (a retried RPC or the storage-completion event)
// skips the steps already done — notably re-moderation and the copy. It is
// idempotent and returns the blob's resulting public status.
func (s *Server) finalize(ctx context.Context, record *Blob) (blobpb.BlobStatus, error) {
	state := record.State
	if state.Terminal() {
		return state.ToBlobStatus(), nil
	}

	var data []byte // the uploaded bytes, fetched once and reused across steps

	// Confirm the client's upload landed, then checkpoint StateUploaded.
	if state < StateUploaded {
		fetched, err := s.fetchUploaded(ctx, record)
		if err != nil {
			return blobpb.BlobStatus_BLOB_STATUS_UNKNOWN, err
		}
		data = fetched
		advanced, err := s.blobs.Advance(ctx, record.ID, StateUploaded, nil)
		if err != nil {
			return blobpb.BlobStatus_BLOB_STATUS_UNKNOWN, err
		}
		if !advanced {
			// Another finalizer moved the blob on; defer to it and report the
			// committed state rather than acting on our stale view.
			return s.currentStatus(ctx, record.ID)
		}
		state = StateUploaded
	}

	// Validate against the pinned contract, derive metadata, moderate, then
	// checkpoint StateInspected (persisting the metadata).
	if state < StateInspected {
		if data == nil {
			fetched, err := s.fetchUploaded(ctx, record)
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
			return s.reject(ctx, record, &RejectionMetadata{Reason: RejectionReasonTooLarge})
		}
		inspection, err := InspectImage(data)
		if err != nil {
			// Undecodable, unsupported, or oversize bytes: not a servable image.
			return s.reject(ctx, record, &RejectionMetadata{Reason: rejectionReasonForInspection(err)})
		}
		if inspection.MimeType != record.MimeType {
			return s.reject(ctx, record, &RejectionMetadata{Reason: RejectionReasonMismatchedType})
		}
		if s.moderator != nil {
			// Moderate a size-bounded rendering, not the full-resolution original:
			// provider sync endpoints are tuned for small images and cap payload
			// size, and full resolution adds nothing to classification.
			payload, err := moderationPayload(data, inspection.Decoded)
			if err != nil {
				return blobpb.BlobStatus_BLOB_STATUS_UNKNOWN, err
			}
			result, err := s.moderator.ClassifyImage(ctx, payload)
			if err != nil {
				// Could not establish safety; leave the blob un-advanced so completion
				// can be retried rather than wrongly marking it servable.
				return blobpb.BlobStatus_BLOB_STATUS_UNKNOWN, err
			}
			if result.Flagged {
				return s.reject(ctx, record, &RejectionMetadata{
					Reason:          RejectionReasonModeration,
					FlaggedCategory: moderation.HighestFlaggedCategory(result),
				})
			}
		}

		advanced, err := s.blobs.Advance(ctx, record.ID, StateInspected, inspection.Metadata)
		if err != nil {
			return blobpb.BlobStatus_BLOB_STATUS_UNKNOWN, err
		}
		if !advanced {
			return s.currentStatus(ctx, record.ID)
		}
		state = StateInspected
	}

	// Copy the original's bytes into the origin store, then checkpoint
	// StatePromoted. This is the durable source renditions will be derived from.
	if state < StatePromoted {
		if err := s.storage.CopyToOrigin(ctx, record.StorageKey); err != nil {
			return blobpb.BlobStatus_BLOB_STATUS_UNKNOWN, err
		}
		advanced, err := s.blobs.Advance(ctx, record.ID, StatePromoted, nil)
		if err != nil {
			return blobpb.BlobStatus_BLOB_STATUS_UNKNOWN, err
		}
		if !advanced {
			return s.currentStatus(ctx, record.ID)
		}
		state = StatePromoted
	}

	// Rendition generation (StateGeneratingRenditions) will slot in here, between
	// promotion and readiness: a blob is not client-ready until its renditions
	// exist. It is not implemented yet, so finalize advances straight to Ready.

	// Clean up the now-redundant upload bytes, then checkpoint StateReady. The
	// original is durably in the origin store, so the cleanup is best-effort and
	// must not hold the blob back from READY: a failure only orphans bytes the
	// upload bucket's lifecycle reclaims.
	if state < StateReady {
		s.cleanupUpload(ctx, record)
		advanced, err := s.blobs.Advance(ctx, record.ID, StateReady, nil)
		if err != nil {
			return blobpb.BlobStatus_BLOB_STATUS_UNKNOWN, err
		}
		if !advanced {
			return s.currentStatus(ctx, record.ID)
		}
		state = StateReady
	}

	return state.ToBlobStatus(), nil
}

// fetchUploaded reads a blob's uploaded bytes, translating an absent object into
// the errBytesNotUploaded sentinel.
func (s *Server) fetchUploaded(ctx context.Context, record *Blob) ([]byte, error) {
	data, err := s.storage.GetUploaded(ctx, record.StorageKey)
	if errors.Is(err, ErrObjectNotFound) {
		return nil, errBytesNotUploaded
	} else if err != nil {
		return nil, err
	}
	return data, nil
}

func (s *Server) reject(ctx context.Context, record *Blob, rejection *RejectionMetadata) (blobpb.BlobStatus, error) {
	advanced, err := s.blobs.Reject(ctx, record.ID, rejection)
	if err != nil {
		return blobpb.BlobStatus_BLOB_STATUS_UNKNOWN, err
	}
	if !advanced {
		// Another finalizer already drove the blob to a terminal state; report what
		// was actually committed rather than asserting REJECTED over it.
		return s.currentStatus(ctx, record.ID)
	}
	// Drop the rejected bytes from the upload store; they are never promoted.
	s.cleanupUpload(ctx, record)
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
	default:
		return RejectionReasonInternal
	}
}

// currentStatus re-reads a blob and returns its authoritative public status. It
// backs the lost-race paths in finalize: when Advance reports it did not perform
// the transition, the local view is stale, so the committed state is read back
// rather than guessed.
func (s *Server) currentStatus(ctx context.Context, id *blobpb.BlobId) (blobpb.BlobStatus, error) {
	record, err := s.blobs.GetByID(ctx, id)
	if err != nil {
		return blobpb.BlobStatus_BLOB_STATUS_UNKNOWN, err
	}
	return record.State.ToBlobStatus(), nil
}

// cleanupUpload best-effort removes a blob's bytes from the upload store after
// it reaches a terminal state. A failure here only leaves an orphan the upload
// bucket's lifecycle policy reclaims, so it is logged, not surfaced.
func (s *Server) cleanupUpload(ctx context.Context, record *Blob) {
	if err := s.storage.DeleteUpload(ctx, record.StorageKey); err != nil {
		s.log.Warn("Failed to delete upload object after finalization",
			zap.String("blob_id", blobIDString(record.ID)),
			zap.Error(err),
		)
	}
}

// buildMetadata assembles the server-authoritative metadata for a READY blob,
// minting a fresh, short-lived download URL. It is a free function over an
// ObjectStorage so both the Server (GetBlobs) and Media (Resolve) can mint
// metadata from their own storage without duplicating the logic.
func buildMetadata(ctx context.Context, storage ObjectStorage, record *Blob) (*blobpb.BlobMetadata, error) {
	downloadURL, err := storage.SignDownloadURL(ctx, record.StorageKey)
	if err != nil {
		return nil, err
	}

	metadata := &blobpb.BlobMetadata{
		MimeType:    record.MimeType,
		SizeBytes:   record.SizeBytes,
		DownloadUrl: downloadURL,
	}
	if record.Image != nil {
		metadata.Kind = &blobpb.BlobMetadata_Image{
			Image: &blobpb.ImageMetadata{
				Width:    record.Image.Width,
				Height:   record.Image.Height,
				Blurhash: record.Image.Blurhash,
			},
		}
	}
	return metadata, nil
}
