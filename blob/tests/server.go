package tests

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"image"
	"image/color"
	"image/png"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	blobpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/blob/v1"
	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"
	moderationpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/moderation/v1"

	"github.com/code-payments/flipcash2-server/account"
	"github.com/code-payments/flipcash2-server/auth"
	"github.com/code-payments/flipcash2-server/blob"
	"github.com/code-payments/flipcash2-server/model"
	"github.com/code-payments/flipcash2-server/moderation"
)

// uploadFunc plays the role of a client PUT-ing bytes to a presigned target. It
// is supplied by the caller so the suite depends only on the blob.ObjectStorage
// interface, not on a concrete fake.
type uploadFunc func(target *blobpb.UploadTarget, data []byte)

// RunServerTests runs the shared blob.Server test suite against the given
// metadata store and object storage. The storage is always a fake injected by
// the caller (along with an upload hook), since the real S3/CloudFront backend
// is not exercised by unit tests.
func RunServerTests(
	t *testing.T,
	accounts account.Store,
	blobs blob.Store,
	storage blob.ObjectStorage,
	access blob.AccessStore,
	upload uploadFunc,
	teardown func(),
) {
	for _, tf := range []func(t *testing.T, accounts account.Store, blobs blob.Store, storage blob.ObjectStorage, access blob.AccessStore, resolver *fakeResolver, upload uploadFunc){
		testGetUploadPolicy,
		testInitiateExternalUpload,
		testUploadLifecycle,
		testFinalizationRejections,
		testModeration,
		testRenditionGeneration,
		testGetBlobs,
	} {
		// A fresh resolver per test func; the access store is reset by teardown.
		resolver := newFakeResolver()
		tf(t, accounts, blobs, storage, access, resolver, upload)
		teardown()
	}
}

// harness bundles the server with the worker that drives the finalization
// pipeline the RPCs only queue work for, so a test can complete an upload and
// then deterministically run the processing it kicked off.
type harness struct {
	server *blob.Server
	worker *blob.Worker
}

func newHarness(t *testing.T, accounts account.Store, blobs blob.Store, storage blob.ObjectStorage, access blob.AccessStore, resolver blob.PrincipalResolver, moderator moderation.Client) *harness {
	log := zaptest.NewLogger(t)
	authn := auth.NewKeyPairAuthenticator(log)
	authz := account.NewAuthorizer(log, accounts, authn)
	return &harness{
		server: blob.NewServer(log, authz, accounts, blobs, storage, access, resolver, false),
		worker: blob.NewWorker(log, blobs, blob.NewFinalizer(log, blobs, storage, moderator), blob.ContentKindImage),
	}
}

// drain runs worker ticks until the due queue is empty. Happy-path and
// rejection finalizations complete on their first attempt, so this terminates
// for every flow the suite exercises.
func (h *harness) drain(t *testing.T) {
	for {
		processed, err := h.worker.Process(context.Background())
		require.NoError(t, err)
		if processed == 0 {
			return
		}
	}
}

// registerUser binds a fresh key pair to a new, registered account.
func registerUser(t *testing.T, accounts account.Store) (*commonpb.UserId, model.KeyPair) {
	userID := model.MustGenerateUserID()
	signer := model.MustGenerateKeyPair()
	_, err := accounts.Bind(context.Background(), userID, signer.Proto())
	require.NoError(t, err)
	require.NoError(t, accounts.SetRegistrationFlag(context.Background(), userID, true))
	return userID, signer
}

func testGetUploadPolicy(t *testing.T, accounts account.Store, blobs blob.Store, storage blob.ObjectStorage, access blob.AccessStore, resolver *fakeResolver, _ uploadFunc) {
	h := newHarness(t, accounts, blobs, storage, access, resolver, nil)

	t.Run("unregistered is denied", func(t *testing.T) {
		signer := model.MustGenerateKeyPair()
		_, err := accounts.Bind(context.Background(), model.MustGenerateUserID(), signer.Proto())
		require.NoError(t, err)

		req := &blobpb.GetUploadPolicyRequest{}
		require.NoError(t, signer.Auth(req, &req.Auth))

		resp, err := h.server.GetUploadPolicy(context.Background(), req)
		require.NoError(t, err)
		require.Equal(t, blobpb.GetUploadPolicyResponse_DENIED, resp.Result)
		require.Nil(t, resp.Policy)
	})

	t.Run("registered receives a policy covering every supported type", func(t *testing.T) {
		_, signer := registerUser(t, accounts)
		req := &blobpb.GetUploadPolicyRequest{}
		require.NoError(t, signer.Auth(req, &req.Auth))

		resp, err := h.server.GetUploadPolicy(context.Background(), req)
		require.NoError(t, err)
		require.Equal(t, blobpb.GetUploadPolicyResponse_OK, resp.Result)
		require.NotNil(t, resp.Policy)

		policy := resp.Policy
		require.NotNil(t, policy.Version)
		require.NotEmpty(t, policy.Version.Value)
		require.NotNil(t, policy.Ttl)
		require.Positive(t, policy.Ttl.AsDuration())

		// Exactly one entry per supported image type, with no wildcard fallback.
		require.Len(t, policy.MimeTypeConstraints, len(blob.SupportedImageMimeTypes))
		seen := make(map[string]bool)
		for _, c := range policy.MimeTypeConstraints {
			require.True(t, blob.SupportedImageMimeTypes[c.MimeTypePattern], "unexpected pattern %q", c.MimeTypePattern)
			require.False(t, seen[c.MimeTypePattern], "duplicate pattern %q", c.MimeTypePattern)
			seen[c.MimeTypePattern] = true

			require.EqualValues(t, blob.MaxOriginalImageSizeBytes, c.MaxSizeBytes)
			img := c.GetImage()
			require.NotNil(t, img)
			require.Positive(t, img.MaxWidth)
			require.Positive(t, img.MaxHeight)
			require.Positive(t, img.MaxPixels)
		}
		require.Len(t, seen, len(blob.SupportedImageMimeTypes))
	})

	t.Run("version matches the one echoed on a policy-driven denial", func(t *testing.T) {
		_, signer := registerUser(t, accounts)

		policyReq := &blobpb.GetUploadPolicyRequest{}
		require.NoError(t, signer.Auth(policyReq, &policyReq.Auth))
		policyResp, err := h.server.GetUploadPolicy(context.Background(), policyReq)
		require.NoError(t, err)
		require.Equal(t, blobpb.GetUploadPolicyResponse_OK, policyResp.Result)

		denyReq := &blobpb.InitiateExternalUploadRequest{MimeType: "image/png", SizeBytes: blob.MaxOriginalImageSizeBytes + 1}
		require.NoError(t, signer.Auth(denyReq, &denyReq.Auth))
		denyResp, err := h.server.InitiateExternalUpload(context.Background(), denyReq)
		require.NoError(t, err)
		require.Equal(t, blobpb.InitiateExternalUploadResponse_TOO_LARGE, denyResp.Result)
		require.NotNil(t, denyResp.PolicyVersion)
		require.Equal(t, policyResp.Policy.Version.Value, denyResp.PolicyVersion.Value)
	})
}

func testInitiateExternalUpload(t *testing.T, accounts account.Store, blobs blob.Store, storage blob.ObjectStorage, access blob.AccessStore, resolver *fakeResolver, _ uploadFunc) {
	h := newHarness(t, accounts, blobs, storage, access, resolver, nil)
	imageBytes := makePNG(t, 8, 8)

	t.Run("unregistered is denied", func(t *testing.T) {
		signer := model.MustGenerateKeyPair()
		_, err := accounts.Bind(context.Background(), model.MustGenerateUserID(), signer.Proto())
		require.NoError(t, err)

		req := &blobpb.InitiateExternalUploadRequest{MimeType: "image/png", SizeBytes: uint64(len(imageBytes))}
		require.NoError(t, signer.Auth(req, &req.Auth))

		resp, err := h.server.InitiateExternalUpload(context.Background(), req)
		require.NoError(t, err)
		require.Equal(t, blobpb.InitiateExternalUploadResponse_DENIED, resp.Result)
		// An authorization denial carries no policy version — the policy is not why
		// it was rejected.
		require.Nil(t, resp.PolicyVersion)
	})

	t.Run("unsupported mime type", func(t *testing.T) {
		_, signer := registerUser(t, accounts)
		req := &blobpb.InitiateExternalUploadRequest{MimeType: "application/pdf", SizeBytes: 1024}
		require.NoError(t, signer.Auth(req, &req.Auth))

		resp, err := h.server.InitiateExternalUpload(context.Background(), req)
		require.NoError(t, err)
		require.Equal(t, blobpb.InitiateExternalUploadResponse_UNSUPPORTED_TYPE, resp.Result)
		require.NotNil(t, resp.PolicyVersion)
		require.NotEmpty(t, resp.PolicyVersion.Value)
	})

	t.Run("oversize is too large", func(t *testing.T) {
		_, signer := registerUser(t, accounts)
		req := &blobpb.InitiateExternalUploadRequest{MimeType: "image/png", SizeBytes: blob.MaxOriginalImageSizeBytes + 1}
		require.NoError(t, signer.Auth(req, &req.Auth))

		resp, err := h.server.InitiateExternalUpload(context.Background(), req)
		require.NoError(t, err)
		require.Equal(t, blobpb.InitiateExternalUploadResponse_TOO_LARGE, resp.Result)
		require.NotNil(t, resp.PolicyVersion)
		require.NotEmpty(t, resp.PolicyVersion.Value)
	})

	t.Run("success reserves a pending original", func(t *testing.T) {
		_, signer := registerUser(t, accounts)
		req := &blobpb.InitiateExternalUploadRequest{MimeType: "image/png", SizeBytes: uint64(len(imageBytes))}
		require.NoError(t, signer.Auth(req, &req.Auth))

		resp, err := h.server.InitiateExternalUpload(context.Background(), req)
		require.NoError(t, err)
		require.Equal(t, blobpb.InitiateExternalUploadResponse_OK, resp.Result)
		require.NotNil(t, resp.BlobId)
		require.Len(t, resp.BlobId.Value, 16)
		require.NotNil(t, resp.UploadTarget)
		require.Equal(t, blobpb.UploadTarget_POST, resp.UploadTarget.Method)
		require.NotEmpty(t, resp.UploadTarget.Url)
		require.NotEmpty(t, resp.UploadTarget.FormFields["key"])
		require.NotNil(t, resp.UploadTarget.ExpiresAt)

		// The reservation lands as a PENDING original.
		record, err := blobs.GetByID(context.Background(), resp.BlobId)
		require.NoError(t, err)
		require.Equal(t, blob.StatePending, record.State)
		require.Equal(t, blob.RenditionOriginal, record.Rendition)
		require.Nil(t, record.ParentID)
	})
}

func testUploadLifecycle(t *testing.T, accounts account.Store, blobs blob.Store, storage blob.ObjectStorage, access blob.AccessStore, resolver *fakeResolver, upload uploadFunc) {
	h := newHarness(t, accounts, blobs, storage, access, resolver, nil)
	_, signer := registerUser(t, accounts)
	imageBytes := makePNG(t, 12, 9)

	blobID, target := initiate(t, h, signer, "image/png", uint64(len(imageBytes)))

	t.Run("complete before upload reports not uploaded", func(t *testing.T) {
		req := &blobpb.CompleteExternalUploadRequest{BlobId: blobID}
		require.NoError(t, signer.Auth(req, &req.Auth))

		resp, err := h.server.CompleteExternalUpload(context.Background(), req)
		require.NoError(t, err)
		require.Equal(t, blobpb.CompleteExternalUploadResponse_NOT_UPLOADED, resp.Result)
	})

	t.Run("complete by a different user is not found", func(t *testing.T) {
		_, otherSigner := registerUser(t, accounts)
		req := &blobpb.CompleteExternalUploadRequest{BlobId: blobID}
		require.NoError(t, otherSigner.Auth(req, &req.Auth))

		resp, err := h.server.CompleteExternalUpload(context.Background(), req)
		require.NoError(t, err)
		require.Equal(t, blobpb.CompleteExternalUploadResponse_NOT_FOUND, resp.Result)
	})

	t.Run("complete after upload queues processing and the worker drives it ready", func(t *testing.T) {
		upload(target, imageBytes)

		// The RPC only confirms the bytes landed and queues the work, so it
		// reports PROCESSING rather than a terminal status.
		resp := completeResponse(t, h, signer, blobID)
		require.Equal(t, blobpb.BlobStatus_BLOB_STATUS_PROCESSING, resp.Status)
		require.Nil(t, resp.RejectionMetadata)

		// The blob is checkpointed as uploaded (its public status is PROCESSING)
		// until the worker runs the pipeline.
		record, err := blobs.GetByID(context.Background(), blobID)
		require.NoError(t, err)
		require.Equal(t, blob.StateUploaded, record.State)

		h.drain(t)

		// The worker drove the blob to READY and recorded the derived metadata.
		record, err = blobs.GetByID(context.Background(), blobID)
		require.NoError(t, err)
		require.Equal(t, blob.StateReady, record.State)
		require.NotNil(t, record.Image)
		require.EqualValues(t, 12, record.Image.Width)
		require.EqualValues(t, 9, record.Image.Height)
		require.NotEmpty(t, record.Image.Blurhash)

		// Completing again reports the committed terminal status.
		resp2 := completeResponse(t, h, signer, blobID)
		require.Equal(t, blobpb.BlobStatus_BLOB_STATUS_READY, resp2.Status)
	})

	t.Run("complete of an unknown id is not found", func(t *testing.T) {
		req := &blobpb.CompleteExternalUploadRequest{BlobId: &blobpb.BlobId{Value: model.MustGenerateUserID().Value}}
		require.NoError(t, signer.Auth(req, &req.Auth))

		resp, err := h.server.CompleteExternalUpload(context.Background(), req)
		require.NoError(t, err)
		require.Equal(t, blobpb.CompleteExternalUploadResponse_NOT_FOUND, resp.Result)
	})
}

func testFinalizationRejections(t *testing.T, accounts account.Store, blobs blob.Store, storage blob.ObjectStorage, access blob.AccessStore, resolver *fakeResolver, upload uploadFunc) {
	h := newHarness(t, accounts, blobs, storage, access, resolver, nil)
	_, signer := registerUser(t, accounts)

	t.Run("non-image bytes are rejected as corrupt", func(t *testing.T) {
		junk := []byte("this is definitely not an image")
		blobID, target := initiate(t, h, signer, "image/png", uint64(len(junk)))
		upload(target, junk)

		requireRejected(t, h, signer, blobID, blobpb.RejectionReason_REJECTION_REASON_CORRUPT)
	})

	t.Run("mime type mismatch is rejected", func(t *testing.T) {
		imageBytes := makePNG(t, 4, 4)
		// Declare webp, upload png of the matching size: the bytes decode, but as a
		// different type than was pinned.
		blobID, target := initiate(t, h, signer, "image/webp", uint64(len(imageBytes)))
		upload(target, imageBytes)

		requireRejected(t, h, signer, blobID, blobpb.RejectionReason_REJECTION_REASON_MISMATCHED_TYPE)
	})

	t.Run("size mismatch is rejected as too large", func(t *testing.T) {
		imageBytes := makePNG(t, 4, 4)
		blobID, target := initiate(t, h, signer, "image/png", uint64(len(imageBytes)+1))
		upload(target, imageBytes)

		requireRejected(t, h, signer, blobID, blobpb.RejectionReason_REJECTION_REASON_TOO_LARGE)
	})

	t.Run("image carrying privacy metadata is rejected", func(t *testing.T) {
		// Clients must strip EXIF before uploading; the bytes are served verbatim, so
		// one that did not would hand recipients the GPS coordinates it was taken at.
		imageBytes := makePNGWithExif(t, 4, 4)
		blobID, target := initiate(t, h, signer, "image/png", uint64(len(imageBytes)))
		upload(target, imageBytes)

		requireRejected(t, h, signer, blobID, blobpb.RejectionReason_REJECTION_REASON_PRIVACY_METADATA)
	})
}

func testModeration(t *testing.T, accounts account.Store, blobs blob.Store, storage blob.ObjectStorage, access blob.AccessStore, resolver *fakeResolver, upload uploadFunc) {
	_, signer := registerUser(t, accounts)
	imageBytes := makePNG(t, 6, 6)

	t.Run("flagged image is rejected with the moderation category", func(t *testing.T) {
		h := newHarness(t, accounts, blobs, storage, access, resolver, &fakeModerator{flagged: true, categories: []string{"general_nsfw"}})
		blobID, target := initiate(t, h, signer, "image/png", uint64(len(imageBytes)))
		upload(target, imageBytes)

		resp := completeAndProcess(t, h, signer, blobID)
		require.Equal(t, blobpb.BlobStatus_BLOB_STATUS_REJECTED, resp.Status)
		require.NotNil(t, resp.RejectionMetadata)
		require.Equal(t, blobpb.RejectionReason_REJECTION_REASON_MODERATION, resp.RejectionMetadata.Reason)
		require.Equal(t, moderationpb.FlaggedCategory_NSFW, resp.RejectionMetadata.FlaggedCategory)
	})

	t.Run("clean image is ready", func(t *testing.T) {
		h := newHarness(t, accounts, blobs, storage, access, resolver, &fakeModerator{flagged: false})
		blobID, target := initiate(t, h, signer, "image/png", uint64(len(imageBytes)))
		upload(target, imageBytes)

		require.Equal(t, blobpb.BlobStatus_BLOB_STATUS_READY, complete(t, h, signer, blobID))
	})
}

func testGetBlobs(t *testing.T, accounts account.Store, blobs blob.Store, storage blob.ObjectStorage, access blob.AccessStore, resolver *fakeResolver, upload uploadFunc) {
	h := newHarness(t, accounts, blobs, storage, access, resolver, nil)
	ownerID, signer := registerUser(t, accounts)
	imageBytes := makePNG(t, 10, 5)

	// A READY blob owned by the uploader.
	readyID, target := initiate(t, h, signer, "image/png", uint64(len(imageBytes)))
	upload(target, imageBytes)
	require.Equal(t, blobpb.BlobStatus_BLOB_STATUS_READY, complete(t, h, signer, readyID))

	// A PENDING blob (reserved, never uploaded).
	pendingID, _ := initiate(t, h, signer, "image/png", uint64(len(imageBytes)))

	// A REJECTED blob (uploaded bytes that fail validation).
	junk := []byte("not an image at all")
	rejectedID, rejectedTarget := initiate(t, h, signer, "image/png", uint64(len(junk)))
	upload(rejectedTarget, junk)
	require.Equal(t, blobpb.BlobStatus_BLOB_STATUS_REJECTED, complete(t, h, signer, rejectedID))

	t.Run("owner resolves a ready blob with a fresh download url and metadata", func(t *testing.T) {
		req := &blobpb.GetBlobsRequest{BlobIds: &blobpb.BlobIdBatch{BlobIds: []*blobpb.BlobId{readyID}}}
		require.NoError(t, signer.Auth(req, &req.Auth))

		resp, err := h.server.GetBlobs(context.Background(), req)
		require.NoError(t, err)
		require.Equal(t, blobpb.GetBlobsResponse_OK, resp.Result)
		require.NotNil(t, resp.Blobs)
		require.Len(t, resp.Blobs.Blobs, 1)

		got := resp.Blobs.Blobs[0]
		require.Equal(t, blobpb.BlobStatus_BLOB_STATUS_READY, got.Status)
		require.NotNil(t, got.Metadata)
		require.Equal(t, "image/png", got.Metadata.MimeType)
		require.EqualValues(t, len(imageBytes), got.Metadata.SizeBytes)
		require.NotNil(t, got.Metadata.DownloadUrl)
		require.NotEmpty(t, got.Metadata.DownloadUrl.Url)
		require.NotNil(t, got.Metadata.DownloadUrl.ExpiresAt)
		require.True(t, got.Metadata.DownloadUrl.ExpiresAt.AsTime().After(time.Now()))
		image := got.Metadata.GetImage()
		require.NotNil(t, image)
		require.EqualValues(t, 10, image.Width)
		require.EqualValues(t, 5, image.Height)
	})

	t.Run("a non-owner with no context cannot resolve someone else's blob", func(t *testing.T) {
		// Without an access context a non-owner holding the id sees it as if it did
		// not exist, so the batch comes back unset rather than leaking the blob's
		// existence.
		_, other := registerUser(t, accounts)
		req := &blobpb.GetBlobsRequest{BlobIds: &blobpb.BlobIdBatch{BlobIds: []*blobpb.BlobId{readyID}}}
		require.NoError(t, other.Auth(req, &req.Auth))

		resp, err := h.server.GetBlobs(context.Background(), req)
		require.NoError(t, err)
		require.Equal(t, blobpb.GetBlobsResponse_OK, resp.Result)
		require.Nil(t, resp.Blobs)
	})

	t.Run("a non-owner resolves a blob shared into a chat they belong to", func(t *testing.T) {
		otherID, other := registerUser(t, accounts)
		chatID := newChatID(t)
		// The blob is shared into the chat, and the caller is a member of it.
		require.NoError(t, access.Grant(context.Background(), &blob.Grant{
			BlobID: readyID, Principal: blob.PrincipalForChat(chatID), Permission: blob.PermissionRead,
		}))
		resolver.allow(blob.PrincipalForChat(chatID), otherID)

		req := &blobpb.GetBlobsRequest{
			BlobIds: &blobpb.BlobIdBatch{BlobIds: []*blobpb.BlobId{readyID}},
			Context: &blobpb.AccessContext{Scope: &blobpb.AccessContext_Chat{Chat: chatID}},
		}
		require.NoError(t, other.Auth(req, &req.Auth))

		resp, err := h.server.GetBlobs(context.Background(), req)
		require.NoError(t, err)
		require.NotNil(t, resp.Blobs)
		require.Len(t, resp.Blobs.Blobs, 1)
		require.Equal(t, blobpb.BlobStatus_BLOB_STATUS_READY, resp.Blobs.Blobs[0].Status)
		require.NotNil(t, resp.Blobs.Blobs[0].Metadata)
	})

	t.Run("a member of the chat cannot resolve a blob never shared into it", func(t *testing.T) {
		// The caller belongs to the chat, but the blob carries no grant for it, so
		// membership alone does not authorize the read.
		otherID, other := registerUser(t, accounts)
		chatID := newChatID(t)
		resolver.allow(blob.PrincipalForChat(chatID), otherID)

		req := &blobpb.GetBlobsRequest{
			BlobIds: &blobpb.BlobIdBatch{BlobIds: []*blobpb.BlobId{readyID}},
			Context: &blobpb.AccessContext{Scope: &blobpb.AccessContext_Chat{Chat: chatID}},
		}
		require.NoError(t, other.Auth(req, &req.Auth))

		resp, err := h.server.GetBlobs(context.Background(), req)
		require.NoError(t, err)
		require.Nil(t, resp.Blobs)
	})

	t.Run("a grant does not help a caller who is not in the chat", func(t *testing.T) {
		// The blob is shared into the chat, but the caller is not a member (the
		// resolver was never told to cover them), so the grant alone does not
		// authorize the read.
		_, other := registerUser(t, accounts)
		chatID := newChatID(t)
		require.NoError(t, access.Grant(context.Background(), &blob.Grant{
			BlobID: readyID, Principal: blob.PrincipalForChat(chatID), Permission: blob.PermissionRead,
		}))

		req := &blobpb.GetBlobsRequest{
			BlobIds: &blobpb.BlobIdBatch{BlobIds: []*blobpb.BlobId{readyID}},
			Context: &blobpb.AccessContext{Scope: &blobpb.AccessContext_Chat{Chat: chatID}},
		}
		require.NoError(t, other.Auth(req, &req.Auth))

		resp, err := h.server.GetBlobs(context.Background(), req)
		require.NoError(t, err)
		require.Nil(t, resp.Blobs)
	})

	t.Run("anyone resolves a blob granted to a user's profile", func(t *testing.T) {
		// A profile picture is public: the grant to the profile is the whole
		// authorization, since every caller is covered by a profile principal.
		_, other := registerUser(t, accounts)
		require.NoError(t, access.Grant(context.Background(), &blob.Grant{
			BlobID: readyID, Principal: blob.PrincipalForProfile(ownerID), Permission: blob.PermissionRead,
		}))

		req := &blobpb.GetBlobsRequest{
			BlobIds: &blobpb.BlobIdBatch{BlobIds: []*blobpb.BlobId{readyID}},
			Context: &blobpb.AccessContext{Scope: &blobpb.AccessContext_Profile{Profile: ownerID}},
		}
		require.NoError(t, other.Auth(req, &req.Auth))

		resp, err := h.server.GetBlobs(context.Background(), req)
		require.NoError(t, err)
		require.NotNil(t, resp.Blobs)
		require.Len(t, resp.Blobs.Blobs, 1)
		require.Equal(t, blobpb.BlobStatus_BLOB_STATUS_READY, resp.Blobs.Blobs[0].Status)
		require.NotNil(t, resp.Blobs.Blobs[0].Metadata)

		// Revoking the grant stops it resolving through the profile, even though the
		// caller is still covered by it — the grant, not coverage, is what authorizes.
		require.NoError(t, access.Revoke(context.Background(), readyID, blob.PrincipalForProfile(ownerID), blob.PermissionRead))

		req = &blobpb.GetBlobsRequest{
			BlobIds: &blobpb.BlobIdBatch{BlobIds: []*blobpb.BlobId{readyID}},
			Context: &blobpb.AccessContext{Scope: &blobpb.AccessContext_Profile{Profile: ownerID}},
		}
		require.NoError(t, other.Auth(req, &req.Auth))

		resp, err = h.server.GetBlobs(context.Background(), req)
		require.NoError(t, err)
		require.Nil(t, resp.Blobs)
	})

	t.Run("a profile context does not resolve a blob never granted to it", func(t *testing.T) {
		// Coverage is universal for a profile, so the grant is the only thing standing
		// between a caller and any blob id they can name. A blob the owner uploaded but
		// never published as their picture must not resolve through their profile.
		_, other := registerUser(t, accounts)

		req := &blobpb.GetBlobsRequest{
			BlobIds: &blobpb.BlobIdBatch{BlobIds: []*blobpb.BlobId{readyID}},
			Context: &blobpb.AccessContext{Scope: &blobpb.AccessContext_Profile{Profile: ownerID}},
		}
		require.NoError(t, other.Auth(req, &req.Auth))

		resp, err := h.server.GetBlobs(context.Background(), req)
		require.NoError(t, err)
		require.Nil(t, resp.Blobs)
	})

	t.Run("a profile grant does not authorize another user's profile context", func(t *testing.T) {
		// The grant names one profile; naming a different one must not resolve it.
		strangerID, other := registerUser(t, accounts)
		require.NoError(t, access.Grant(context.Background(), &blob.Grant{
			BlobID: readyID, Principal: blob.PrincipalForProfile(ownerID), Permission: blob.PermissionRead,
		}))
		t.Cleanup(func() {
			require.NoError(t, access.Revoke(context.Background(), readyID, blob.PrincipalForProfile(ownerID), blob.PermissionRead))
		})

		req := &blobpb.GetBlobsRequest{
			BlobIds: &blobpb.BlobIdBatch{BlobIds: []*blobpb.BlobId{readyID}},
			Context: &blobpb.AccessContext{Scope: &blobpb.AccessContext_Profile{Profile: strangerID}},
		}
		require.NoError(t, other.Auth(req, &req.Auth))

		resp, err := h.server.GetBlobs(context.Background(), req)
		require.NoError(t, err)
		require.Nil(t, resp.Blobs)
	})

	t.Run("owner resolves a rejected blob with rejection metadata", func(t *testing.T) {
		req := &blobpb.GetBlobsRequest{BlobIds: &blobpb.BlobIdBatch{BlobIds: []*blobpb.BlobId{rejectedID}}}
		require.NoError(t, signer.Auth(req, &req.Auth))

		resp, err := h.server.GetBlobs(context.Background(), req)
		require.NoError(t, err)
		require.NotNil(t, resp.Blobs)
		require.Len(t, resp.Blobs.Blobs, 1)

		got := resp.Blobs.Blobs[0]
		require.Equal(t, blobpb.BlobStatus_BLOB_STATUS_REJECTED, got.Status)
		require.Nil(t, got.Metadata)
		require.NotNil(t, got.Rejection)
		require.Equal(t, blobpb.RejectionReason_REJECTION_REASON_CORRUPT, got.Rejection.Reason)
	})

	t.Run("pending blob has status but no metadata", func(t *testing.T) {
		req := &blobpb.GetBlobsRequest{BlobIds: &blobpb.BlobIdBatch{BlobIds: []*blobpb.BlobId{pendingID}}}
		require.NoError(t, signer.Auth(req, &req.Auth))

		resp, err := h.server.GetBlobs(context.Background(), req)
		require.NoError(t, err)
		require.NotNil(t, resp.Blobs)
		require.Len(t, resp.Blobs.Blobs, 1)
		require.Equal(t, blobpb.BlobStatus_BLOB_STATUS_PENDING, resp.Blobs.Blobs[0].Status)
		require.Nil(t, resp.Blobs.Blobs[0].Metadata)
	})

	t.Run("unknown ids resolve to an unset batch", func(t *testing.T) {
		req := &blobpb.GetBlobsRequest{BlobIds: &blobpb.BlobIdBatch{BlobIds: []*blobpb.BlobId{
			{Value: model.MustGenerateUserID().Value},
		}}}
		require.NoError(t, signer.Auth(req, &req.Auth))

		resp, err := h.server.GetBlobs(context.Background(), req)
		require.NoError(t, err)
		require.Equal(t, blobpb.GetBlobsResponse_OK, resp.Result)
		require.Nil(t, resp.Blobs)
	})

	t.Run("missing auth is rejected", func(t *testing.T) {
		req := &blobpb.GetBlobsRequest{BlobIds: &blobpb.BlobIdBatch{BlobIds: []*blobpb.BlobId{readyID}}}
		_, err := h.server.GetBlobs(context.Background(), req)
		require.Equal(t, codes.InvalidArgument, status.Code(err))
	})
}

func testRenditionGeneration(t *testing.T, accounts account.Store, blobs blob.Store, storage blob.ObjectStorage, access blob.AccessStore, resolver *fakeResolver, upload uploadFunc) {
	h := newHarness(t, accounts, blobs, storage, access, resolver, nil)
	_, signer := registerUser(t, accounts)
	ctx := context.Background()

	// readyBlob uploads and finalizes an image, returning the READY original record.
	readyBlob := func(t *testing.T, mimeType string, data []byte) *blob.Blob {
		blobID, target := initiate(t, h, signer, mimeType, uint64(len(data)))
		upload(target, data)
		require.Equal(t, blobpb.BlobStatus_BLOB_STATUS_READY, complete(t, h, signer, blobID))
		record, err := blobs.GetByID(ctx, blobID)
		require.NoError(t, err)
		return record
	}

	t.Run("a large opaque image generates the full ladder as WebP", func(t *testing.T) {
		original := readyBlob(t, "image/png", makePNG(t, 2000, 1000))

		require.Equal(t, blob.RenditionOriginal, original.Rendition)
		require.Nil(t, original.ParentID)
		require.NotEmpty(t, original.Image.Blurhash)

		// The manifest is the ladder, small to large, scaled from the 2000x1000 source.
		want := []struct {
			role blob.RenditionType
			w, h uint32
		}{
			{blob.RenditionThumbnail, 32, 16},
			{blob.RenditionThumbnail, 160, 80},
			{blob.RenditionThumbnail, 320, 160},
			{blob.RenditionDisplay, 800, 400},
			{blob.RenditionDisplay, 1600, 800},
		}
		require.Len(t, original.Renditions, len(want))
		for i, ref := range original.Renditions {
			require.Equal(t, want[i].role, ref.Rendition)
			require.Equal(t, "image/webp", ref.MimeType, "an opaque source yields lossy WebP renditions")
			require.NotNil(t, ref.Image)
			require.EqualValues(t, want[i].w, ref.Image.Width)
			require.EqualValues(t, want[i].h, ref.Image.Height)
			require.Positive(t, ref.SizeBytes)
			require.NotEmpty(t, ref.StorageKey)

			// Every manifest entry is backed by a real, servable child rendition blob
			// that points back at the original and inherits its BlurHash.
			child, err := blobs.GetByID(ctx, ref.ID)
			require.NoError(t, err)
			require.Equal(t, blob.StateReady, child.State)
			require.Equal(t, ref.Rendition, child.Rendition)
			require.NotNil(t, child.ParentID)
			require.Equal(t, original.ID.Value, child.ParentID.Value)
			require.Equal(t, ref.MimeType, child.MimeType)
			require.EqualValues(t, want[i].w, child.Image.Width)
			require.EqualValues(t, want[i].h, child.Image.Height)
			require.Equal(t, original.Image.Blurhash, child.Image.Blurhash)
		}
	})

	t.Run("a transparent image generates renditions as lossless WebP", func(t *testing.T) {
		original := readyBlob(t, "image/png", makeTransparentPNG(t, 400, 400))

		require.NotEmpty(t, original.Renditions)
		for _, ref := range original.Renditions {
			require.Equal(t, "image/webp", ref.MimeType, "a transparent source yields WebP renditions")
		}
	})

	t.Run("a mid-size image fills the gap up to the next rung at its own size", func(t *testing.T) {
		// 500 on the longest side clears all three thumbnail rungs but falls between the
		// 320 thumbnail and the 800 display. So the three thumbnails are generated, plus
		// a single DISPLAY at the original's own 500x400 size (the next rung's role),
		// never upscaled to 800.
		original := readyBlob(t, "image/png", makePNG(t, 500, 400))

		require.Len(t, original.Renditions, 4)
		require.Equal(t, blob.RenditionThumbnail, original.Renditions[0].Rendition)
		require.EqualValues(t, 32, original.Renditions[0].Image.Width)
		require.EqualValues(t, 26, original.Renditions[0].Image.Height)
		require.Equal(t, blob.RenditionThumbnail, original.Renditions[1].Rendition)
		require.EqualValues(t, 160, original.Renditions[1].Image.Width)
		require.EqualValues(t, 128, original.Renditions[1].Image.Height)
		require.Equal(t, blob.RenditionThumbnail, original.Renditions[2].Rendition)
		require.EqualValues(t, 320, original.Renditions[2].Image.Width)
		require.EqualValues(t, 256, original.Renditions[2].Image.Height)
		require.Equal(t, blob.RenditionDisplay, original.Renditions[3].Rendition)
		require.EqualValues(t, 500, original.Renditions[3].Image.Width)
		require.EqualValues(t, 400, original.Renditions[3].Image.Height)
	})

	t.Run("an image between the thumbnail rungs still gets a DISPLAY", func(t *testing.T) {
		// 200 on the longest side clears the 32 and 160 thumbnails but tops out within
		// the thumbnail role at the 320 rung. The ladder must keep climbing to the
		// DISPLAY role regardless, emitting it at the original's own size rather than
		// stopping once the thumbnails are covered.
		original := readyBlob(t, "image/png", makePNG(t, 200, 160))

		require.Len(t, original.Renditions, 4)
		require.Equal(t, blob.RenditionThumbnail, original.Renditions[0].Rendition)
		require.EqualValues(t, 32, original.Renditions[0].Image.Width)
		require.EqualValues(t, 26, original.Renditions[0].Image.Height)
		require.Equal(t, blob.RenditionThumbnail, original.Renditions[1].Rendition)
		require.EqualValues(t, 160, original.Renditions[1].Image.Width)
		require.EqualValues(t, 128, original.Renditions[1].Image.Height)
		require.Equal(t, blob.RenditionThumbnail, original.Renditions[2].Rendition)
		require.EqualValues(t, 200, original.Renditions[2].Image.Width)
		require.EqualValues(t, 160, original.Renditions[2].Image.Height)
		require.Equal(t, blob.RenditionDisplay, original.Renditions[3].Rendition)
		require.EqualValues(t, 200, original.Renditions[3].Image.Width)
		require.EqualValues(t, 160, original.Renditions[3].Image.Height)
	})

	t.Run("a small image still gets the 32 thumbnail plus one rendition per role at its own size", func(t *testing.T) {
		// Under every rung but the 32 thumbnail, which is small enough to remain a real
		// downscale. Above it the original is never upscaled — but each ROLE's smallest
		// rung that reaches it is still processed at its own 100x80 size, since
		// re-encoding to WebP can shrink the bytes over the ORIGINAL. Both roles collapse
		// to that one size, so the ladder is the 32 thumbnail, one more THUMBNAIL, and
		// one DISPLAY.
		original := readyBlob(t, "image/png", makePNG(t, 100, 80))

		require.Len(t, original.Renditions, 3)
		require.Equal(t, blob.RenditionThumbnail, original.Renditions[0].Rendition)
		require.EqualValues(t, 32, original.Renditions[0].Image.Width)
		require.EqualValues(t, 26, original.Renditions[0].Image.Height)

		require.Equal(t, blob.RenditionThumbnail, original.Renditions[1].Rendition)
		require.Equal(t, blob.RenditionDisplay, original.Renditions[2].Rendition)
		for _, ref := range original.Renditions[1:] {
			require.EqualValues(t, 100, ref.Image.Width)
			require.EqualValues(t, 80, ref.Image.Height)
		}
		for _, ref := range original.Renditions {
			require.Equal(t, "image/webp", ref.MimeType)
		}
	})

	t.Run("every role in the ladder is present whatever the image size", func(t *testing.T) {
		// The guarantee the sizes above spot-check, asserted directly across the range:
		// a client resolving any role never has to fall back to the ORIGINAL.
		for _, size := range []struct{ w, h uint32 }{
			{1, 1}, {100, 80}, {200, 160}, {320, 320}, {500, 400}, {2000, 1000},
		} {
			original := readyBlob(t, "image/png", makePNG(t, int(size.w), int(size.h)))

			roles := make(map[blob.RenditionType]bool)
			for _, ref := range original.Renditions {
				roles[ref.Rendition] = true
			}
			require.True(t, roles[blob.RenditionThumbnail], "%dx%d has no THUMBNAIL", size.w, size.h)
			require.True(t, roles[blob.RenditionDisplay], "%dx%d has no DISPLAY", size.w, size.h)
		}
	})

	t.Run("re-completing does not duplicate or alter the manifest", func(t *testing.T) {
		data := makePNG(t, 1000, 500)
		blobID, target := initiate(t, h, signer, "image/png", uint64(len(data)))
		upload(target, data)
		require.Equal(t, blobpb.BlobStatus_BLOB_STATUS_READY, complete(t, h, signer, blobID))

		before, err := blobs.GetByID(ctx, blobID)
		require.NoError(t, err)

		// A repeated completion finds the blob terminal and leaves the manifest as-is.
		require.Equal(t, blobpb.BlobStatus_BLOB_STATUS_READY, complete(t, h, signer, blobID))
		after, err := blobs.GetByID(ctx, blobID)
		require.NoError(t, err)

		require.Len(t, after.Renditions, len(before.Renditions))
		for i := range before.Renditions {
			require.Equal(t, before.Renditions[i].ID.Value, after.Renditions[i].ID.Value)
		}
	})
}

// initiate runs InitiateExternalUpload and returns the reserved id and target.
func initiate(t *testing.T, h *harness, signer model.KeyPair, mimeType string, sizeBytes uint64) (*blobpb.BlobId, *blobpb.UploadTarget) {
	req := &blobpb.InitiateExternalUploadRequest{MimeType: mimeType, SizeBytes: sizeBytes}
	require.NoError(t, signer.Auth(req, &req.Auth))

	resp, err := h.server.InitiateExternalUpload(context.Background(), req)
	require.NoError(t, err)
	require.Equal(t, blobpb.InitiateExternalUploadResponse_OK, resp.Result)
	return resp.BlobId, resp.UploadTarget
}

// completeResponse runs CompleteExternalUpload and returns the full OK response.
func completeResponse(t *testing.T, h *harness, signer model.KeyPair, blobID *blobpb.BlobId) *blobpb.CompleteExternalUploadResponse {
	req := &blobpb.CompleteExternalUploadRequest{BlobId: blobID}
	require.NoError(t, signer.Auth(req, &req.Auth))

	resp, err := h.server.CompleteExternalUpload(context.Background(), req)
	require.NoError(t, err)
	require.Equal(t, blobpb.CompleteExternalUploadResponse_OK, resp.Result)
	return resp
}

// completeAndProcess finishes an upload end-to-end: it completes it (which only
// queues the finalization work), drains the worker, and returns the terminal
// response reported by the idempotent re-complete.
func completeAndProcess(t *testing.T, h *harness, signer model.KeyPair, blobID *blobpb.BlobId) *blobpb.CompleteExternalUploadResponse {
	resp := completeResponse(t, h, signer, blobID)
	if resp.Status == blobpb.BlobStatus_BLOB_STATUS_READY || resp.Status == blobpb.BlobStatus_BLOB_STATUS_REJECTED {
		// Already finalized (a re-complete of a terminal blob); there is no
		// queued work to drive.
		return resp
	}
	require.Equal(t, blobpb.BlobStatus_BLOB_STATUS_PROCESSING, resp.Status)
	h.drain(t)
	return completeResponse(t, h, signer, blobID)
}

// complete finishes an upload end-to-end (see completeAndProcess) and returns
// the terminal status.
func complete(t *testing.T, h *harness, signer model.KeyPair, blobID *blobpb.BlobId) blobpb.BlobStatus {
	return completeAndProcess(t, h, signer, blobID).Status
}

// requireRejected asserts that completing the blob (and running the worker)
// rejects it with the given reason, surfaced in the terminal completion
// response's rejection metadata.
func requireRejected(t *testing.T, h *harness, signer model.KeyPair, blobID *blobpb.BlobId, reason blobpb.RejectionReason) {
	resp := completeAndProcess(t, h, signer, blobID)
	require.Equal(t, blobpb.BlobStatus_BLOB_STATUS_REJECTED, resp.Status)
	require.NotNil(t, resp.RejectionMetadata)
	require.Equal(t, reason, resp.RejectionMetadata.Reason)
}

func makePNG(t *testing.T, width, height int) []byte {
	img := image.NewRGBA(image.Rect(0, 0, width, height))
	for y := range height {
		for x := range width {
			img.Set(x, y, color.RGBA{R: uint8(x * 8), G: uint8(y * 8), B: 128, A: 255})
		}
	}
	var buf bytes.Buffer
	require.NoError(t, png.Encode(&buf, img))
	return buf.Bytes()
}

// makeTransparentPNG returns an opaque-free PNG (every pixel is partly
// transparent), so InspectImage derives HasAlpha=true and its renditions are
// encoded as PNG rather than JPEG.
func makeTransparentPNG(t *testing.T, width, height int) []byte {
	img := image.NewRGBA(image.Rect(0, 0, width, height))
	for y := range height {
		for x := range width {
			img.Set(x, y, color.RGBA{R: uint8(x * 8), G: uint8(y * 8), B: 128, A: 128})
		}
	}
	var buf bytes.Buffer
	require.NoError(t, png.Encode(&buf, img))
	return buf.Bytes()
}

// makePNGWithExif returns a PNG that kept an eXIf chunk — an image a client
// uploaded without stripping its metadata. The chunk is spliced in after the
// IHDR with a valid CRC, so the image still decodes.
func makePNGWithExif(t *testing.T, width, height int) []byte {
	base := makePNG(t, width, height)
	const afterIHDR = 8 + 12 + 13 // signature + IHDR (length+type+crc) + IHDR data

	payload := []byte("gps coordinates go here")

	var chunk bytes.Buffer
	require.NoError(t, binary.Write(&chunk, binary.BigEndian, uint32(len(payload))))
	chunk.WriteString("eXIf")
	chunk.Write(payload)
	crc := crc32.ChecksumIEEE(append([]byte("eXIf"), payload...))
	require.NoError(t, binary.Write(&chunk, binary.BigEndian, crc))

	out := make([]byte, 0, len(base)+chunk.Len())
	out = append(out, base[:afterIHDR]...)
	out = append(out, chunk.Bytes()...)
	return append(out, base[afterIHDR:]...)
}

// fakeResolver is a controllable blob.PrincipalResolver for the server suite: a
// (principal, user) pair resolves as covered only after allow records it.
type fakeResolver struct {
	covered map[string]bool
}

func newFakeResolver() *fakeResolver {
	return &fakeResolver{covered: make(map[string]bool)}
}

func (r *fakeResolver) allow(principal blob.Principal, user *commonpb.UserId) {
	r.covered[resolverKey(principal, user)] = true
}

func (r *fakeResolver) Covers(ctx context.Context, principal blob.Principal, user *commonpb.UserId) (bool, error) {
	// A profile is public, so there is no membership to fake: defer to the real
	// resolver, the way the production CompositeResolver routes it.
	if principal.Type == blob.PrincipalTypeProfile {
		return blob.NewProfileResolver().Covers(ctx, principal, user)
	}
	return r.covered[resolverKey(principal, user)], nil
}

func resolverKey(principal blob.Principal, user *commonpb.UserId) string {
	return fmt.Sprintf("%d|%q|%q", int(principal.Type), principal.ID, user.Value)
}

type fakeModerator struct {
	flagged bool
	// categories, when set, are returned as the flagged categories (each given a
	// distinct score) so the mapping into a proto FlaggedCategory can be exercised.
	categories []string
}

func (m *fakeModerator) ClassifyText(context.Context, string) (*moderation.Result, error) {
	return &moderation.Result{}, nil
}

func (m *fakeModerator) ClassifyImage(context.Context, []byte) (*moderation.Result, error) {
	result := &moderation.Result{Flagged: m.flagged}
	if len(m.categories) > 0 {
		result.FlaggedCategories = m.categories
		result.CategoryScores = make(map[string]float64, len(m.categories))
		for i, category := range m.categories {
			result.CategoryScores[category] = float64(i + 1)
		}
	}
	return result, nil
}

func (m *fakeModerator) ClassifyCurrencyName(context.Context, string) (*moderation.Result, error) {
	return &moderation.Result{}, nil
}
