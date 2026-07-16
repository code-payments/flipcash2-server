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
)

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

	id := MustGenerateID()
	log = log.With(zap.String("blob_id", IDString(id)))

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

// CompleteExternalUpload confirms the client's upload landed and queues the blob
// for finalization. The processing pipeline itself — validation, moderation,
// promotion, rendition generation — runs on the background worker, so the RPC
// returns PROCESSING and the client observes the terminal status by polling
// GetBlobs (or re-completing). Completion is idempotent: once the blob is
// terminal it reports that committed status, with the rejection metadata when
// the blob was rejected.
func (s *Server) CompleteExternalUpload(ctx context.Context, req *blobpb.CompleteExternalUploadRequest) (*blobpb.CompleteExternalUploadResponse, error) {
	owner, err := s.authz.Authorize(ctx, req, &req.Auth)
	if err != nil {
		return nil, err
	}

	log := s.log.With(
		zap.String("owner_id", model.UserIDString(owner)),
		zap.String("blob_id", IDString(req.BlobId)),
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

	// A blob that already finished processing reports its committed terminal
	// status; the rejection metadata (nil for READY) tells the client why when it
	// was rejected.
	if record.State.Terminal() {
		return &blobpb.CompleteExternalUploadResponse{
			Result:            blobpb.CompleteExternalUploadResponse_OK,
			Status:            record.State.ToBlobStatus(),
			RejectionMetadata: record.Rejection.ToProto(),
		}, nil
	}

	// Confirm the bytes actually landed before queueing work: a completion ahead
	// of the upload is the client's error to observe and retry, not a doomed task
	// for the worker to spin on.
	exists, err := s.storage.UploadExists(ctx, record.StorageKey)
	if err != nil {
		log.Warn("Failed to check for uploaded bytes", zap.Error(err))
		return nil, status.Error(codes.Internal, "failed to complete upload")
	}
	if !exists {
		return &blobpb.CompleteExternalUploadResponse{Result: blobpb.CompleteExternalUploadResponse_NOT_UPLOADED}, nil
	}

	// The blob is queued on its content kind's queue, drained by that kind's
	// worker. The kind resolves for any blob whose reservation was accepted; an
	// unknown kind would mean the supported-type list and the kind mapping
	// drifted, and queueing it would strand the blob in a queue no worker
	// drains, so it is refused loudly instead.
	kind := record.ContentKind()
	if kind == ContentKindUnknown {
		log.Warn("No content kind for blob mime type", zap.String("mime_type", record.MimeType))
		return nil, status.Error(codes.Internal, "failed to complete upload")
	}

	// The bytes are present: checkpoint StateUploaded so the public status flips
	// from PENDING (awaiting upload) to PROCESSING, then queue the blob for the
	// finalization worker. Losing the Advance race to a concurrent finalize is
	// fine, and marking a blob that meanwhile went terminal is a no-op — the
	// client's next poll sees the committed state either way.
	if _, err := s.blobs.Advance(ctx, record.ID, StateUploaded, nil); err != nil {
		log.Warn("Failed to advance blob to uploaded", zap.Error(err))
		return nil, status.Error(codes.Internal, "failed to complete upload")
	}
	if err := s.blobs.MarkForFinalization(ctx, record.ID, kind, time.Now()); err != nil {
		log.Warn("Failed to queue blob for finalization", zap.Error(err))
		return nil, status.Error(codes.Internal, "failed to complete upload")
	}

	return &blobpb.CompleteExternalUploadResponse{
		Result: blobpb.CompleteExternalUploadResponse_OK,
		Status: blobpb.BlobStatus_BLOB_STATUS_PROCESSING,
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
				zap.String("blob_id", IDString(record.ID)),
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
					zap.String("blob_id", IDString(record.ID)),
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
	case *blobpb.AccessContext_Profile:
		return PrincipalForProfile(scope.Profile), true
	default:
		return Principal{}, false
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
