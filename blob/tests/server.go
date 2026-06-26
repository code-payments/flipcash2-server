package tests

import (
	"bytes"
	"context"
	"image"
	"image/color"
	"image/png"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	blobpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/blob/v1"
	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"

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
	upload uploadFunc,
	teardown func(),
) {
	for _, tf := range []func(t *testing.T, accounts account.Store, blobs blob.Store, storage blob.ObjectStorage, upload uploadFunc){
		testInitiateExternalUpload,
		testUploadLifecycle,
		testFinalizationRejections,
		testModeration,
		testGetBlobs,
	} {
		tf(t, accounts, blobs, storage, upload)
		teardown()
	}
}

func newServer(accounts account.Store, blobs blob.Store, storage blob.ObjectStorage, moderator moderation.Client, t *testing.T) *blob.Server {
	log := zaptest.NewLogger(t)
	authn := auth.NewKeyPairAuthenticator(log)
	authz := account.NewAuthorizer(log, accounts, authn)
	return blob.NewServer(log, authz, accounts, blobs, storage, moderator, false)
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

func testInitiateExternalUpload(t *testing.T, accounts account.Store, blobs blob.Store, storage blob.ObjectStorage, _ uploadFunc) {
	server := newServer(accounts, blobs, storage, nil, t)
	imageBytes := makePNG(t, 8, 8)

	t.Run("unregistered is denied", func(t *testing.T) {
		signer := model.MustGenerateKeyPair()
		_, err := accounts.Bind(context.Background(), model.MustGenerateUserID(), signer.Proto())
		require.NoError(t, err)

		req := &blobpb.InitiateExternalUploadRequest{MimeType: "image/png", SizeBytes: uint64(len(imageBytes))}
		require.NoError(t, signer.Auth(req, &req.Auth))

		resp, err := server.InitiateExternalUpload(context.Background(), req)
		require.NoError(t, err)
		require.Equal(t, blobpb.InitiateExternalUploadResponse_DENIED, resp.Result)
	})

	t.Run("unsupported mime type is denied", func(t *testing.T) {
		_, signer := registerUser(t, accounts)
		req := &blobpb.InitiateExternalUploadRequest{MimeType: "application/pdf", SizeBytes: 1024}
		require.NoError(t, signer.Auth(req, &req.Auth))

		resp, err := server.InitiateExternalUpload(context.Background(), req)
		require.NoError(t, err)
		require.Equal(t, blobpb.InitiateExternalUploadResponse_DENIED, resp.Result)
	})

	t.Run("oversize is denied", func(t *testing.T) {
		_, signer := registerUser(t, accounts)
		req := &blobpb.InitiateExternalUploadRequest{MimeType: "image/png", SizeBytes: blob.MaxOriginalSizeBytes + 1}
		require.NoError(t, signer.Auth(req, &req.Auth))

		resp, err := server.InitiateExternalUpload(context.Background(), req)
		require.NoError(t, err)
		require.Equal(t, blobpb.InitiateExternalUploadResponse_DENIED, resp.Result)
	})

	t.Run("success reserves a pending original", func(t *testing.T) {
		_, signer := registerUser(t, accounts)
		req := &blobpb.InitiateExternalUploadRequest{MimeType: "image/png", SizeBytes: uint64(len(imageBytes))}
		require.NoError(t, signer.Auth(req, &req.Auth))

		resp, err := server.InitiateExternalUpload(context.Background(), req)
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

func testUploadLifecycle(t *testing.T, accounts account.Store, blobs blob.Store, storage blob.ObjectStorage, upload uploadFunc) {
	server := newServer(accounts, blobs, storage, nil, t)
	_, signer := registerUser(t, accounts)
	imageBytes := makePNG(t, 12, 9)

	blobID, target := initiate(t, server, signer, "image/png", uint64(len(imageBytes)))

	t.Run("complete before upload reports not uploaded", func(t *testing.T) {
		req := &blobpb.CompleteExternalUploadRequest{BlobId: blobID}
		require.NoError(t, signer.Auth(req, &req.Auth))

		resp, err := server.CompleteExternalUpload(context.Background(), req)
		require.NoError(t, err)
		require.Equal(t, blobpb.CompleteExternalUploadResponse_NOT_UPLOADED, resp.Result)
	})

	t.Run("complete by a different user is not found", func(t *testing.T) {
		_, otherSigner := registerUser(t, accounts)
		req := &blobpb.CompleteExternalUploadRequest{BlobId: blobID}
		require.NoError(t, otherSigner.Auth(req, &req.Auth))

		resp, err := server.CompleteExternalUpload(context.Background(), req)
		require.NoError(t, err)
		require.Equal(t, blobpb.CompleteExternalUploadResponse_NOT_FOUND, resp.Result)
	})

	t.Run("complete after upload is ready and idempotent", func(t *testing.T) {
		upload(target, imageBytes)

		req := &blobpb.CompleteExternalUploadRequest{BlobId: blobID}
		require.NoError(t, signer.Auth(req, &req.Auth))

		resp, err := server.CompleteExternalUpload(context.Background(), req)
		require.NoError(t, err)
		require.Equal(t, blobpb.CompleteExternalUploadResponse_OK, resp.Result)
		require.Equal(t, blobpb.BlobStatus_BLOB_STATUS_READY, resp.Status)

		// The derived image metadata is recorded.
		record, err := blobs.GetByID(context.Background(), blobID)
		require.NoError(t, err)
		require.NotNil(t, record.Image)
		require.EqualValues(t, 12, record.Image.Width)
		require.EqualValues(t, 9, record.Image.Height)
		require.NotEmpty(t, record.Image.Blurhash)

		// Calling again returns the same terminal status.
		req2 := &blobpb.CompleteExternalUploadRequest{BlobId: blobID}
		require.NoError(t, signer.Auth(req2, &req2.Auth))
		resp2, err := server.CompleteExternalUpload(context.Background(), req2)
		require.NoError(t, err)
		require.Equal(t, blobpb.BlobStatus_BLOB_STATUS_READY, resp2.Status)
	})

	t.Run("complete of an unknown id is not found", func(t *testing.T) {
		req := &blobpb.CompleteExternalUploadRequest{BlobId: &blobpb.BlobId{Value: model.MustGenerateUserID().Value}}
		require.NoError(t, signer.Auth(req, &req.Auth))

		resp, err := server.CompleteExternalUpload(context.Background(), req)
		require.NoError(t, err)
		require.Equal(t, blobpb.CompleteExternalUploadResponse_NOT_FOUND, resp.Result)
	})
}

func testFinalizationRejections(t *testing.T, accounts account.Store, blobs blob.Store, storage blob.ObjectStorage, upload uploadFunc) {
	server := newServer(accounts, blobs, storage, nil, t)
	_, signer := registerUser(t, accounts)

	t.Run("non-image bytes are rejected", func(t *testing.T) {
		junk := []byte("this is definitely not an image")
		blobID, target := initiate(t, server, signer, "image/png", uint64(len(junk)))
		upload(target, junk)

		require.Equal(t, blobpb.BlobStatus_BLOB_STATUS_REJECTED, complete(t, server, signer, blobID))
	})

	t.Run("mime type mismatch is rejected", func(t *testing.T) {
		imageBytes := makePNG(t, 4, 4)
		// Declare gif, upload png of the matching size: the bytes decode, but as a
		// different type than was pinned.
		blobID, target := initiate(t, server, signer, "image/gif", uint64(len(imageBytes)))
		upload(target, imageBytes)

		require.Equal(t, blobpb.BlobStatus_BLOB_STATUS_REJECTED, complete(t, server, signer, blobID))
	})

	t.Run("size mismatch is rejected", func(t *testing.T) {
		imageBytes := makePNG(t, 4, 4)
		blobID, target := initiate(t, server, signer, "image/png", uint64(len(imageBytes)+1))
		upload(target, imageBytes)

		require.Equal(t, blobpb.BlobStatus_BLOB_STATUS_REJECTED, complete(t, server, signer, blobID))
	})
}

func testModeration(t *testing.T, accounts account.Store, blobs blob.Store, storage blob.ObjectStorage, upload uploadFunc) {
	_, signer := registerUser(t, accounts)
	imageBytes := makePNG(t, 6, 6)

	t.Run("flagged image is rejected", func(t *testing.T) {
		server := newServer(accounts, blobs, storage, &fakeModerator{flagged: true}, t)
		blobID, target := initiate(t, server, signer, "image/png", uint64(len(imageBytes)))
		upload(target, imageBytes)

		require.Equal(t, blobpb.BlobStatus_BLOB_STATUS_REJECTED, complete(t, server, signer, blobID))
	})

	t.Run("clean image is ready", func(t *testing.T) {
		server := newServer(accounts, blobs, storage, &fakeModerator{flagged: false}, t)
		blobID, target := initiate(t, server, signer, "image/png", uint64(len(imageBytes)))
		upload(target, imageBytes)

		require.Equal(t, blobpb.BlobStatus_BLOB_STATUS_READY, complete(t, server, signer, blobID))
	})
}

func testGetBlobs(t *testing.T, accounts account.Store, blobs blob.Store, storage blob.ObjectStorage, upload uploadFunc) {
	server := newServer(accounts, blobs, storage, nil, t)
	_, signer := registerUser(t, accounts)
	imageBytes := makePNG(t, 10, 5)

	// A READY blob owned by the uploader.
	readyID, target := initiate(t, server, signer, "image/png", uint64(len(imageBytes)))
	upload(target, imageBytes)
	require.Equal(t, blobpb.BlobStatus_BLOB_STATUS_READY, complete(t, server, signer, readyID))

	// A PENDING blob (reserved, never uploaded).
	pendingID, _ := initiate(t, server, signer, "image/png", uint64(len(imageBytes)))

	t.Run("owner resolves a ready blob with a fresh download url and metadata", func(t *testing.T) {
		req := &blobpb.GetBlobsRequest{BlobIds: &blobpb.BlobIdBatch{BlobIds: []*blobpb.BlobId{readyID}}}
		require.NoError(t, signer.Auth(req, &req.Auth))

		resp, err := server.GetBlobs(context.Background(), req)
		require.NoError(t, err)
		require.Equal(t, blobpb.GetBlobsResponse_OK, resp.Result)
		require.NotNil(t, resp.Blobs)
		require.Len(t, resp.Blobs.Blobs, 1)

		got := resp.Blobs.Blobs[0]
		require.Equal(t, blobpb.BlobStatus_BLOB_STATUS_READY, got.Status)
		require.NotNil(t, got.Metadata)
		require.Equal(t, "image/png", got.Metadata.MimeType)
		require.EqualValues(t, len(imageBytes), got.Metadata.SizeBytes)
		require.NotEmpty(t, got.Metadata.DownloadUrl)
		image := got.Metadata.GetImage()
		require.NotNil(t, image)
		require.EqualValues(t, 10, image.Width)
		require.EqualValues(t, 5, image.Height)
	})

	t.Run("a non-owner cannot resolve someone else's blob", func(t *testing.T) {
		// Access is scoped to the uploader: a non-owner holding the id sees it as if
		// it did not exist, so the batch comes back unset rather than leaking the
		// blob's existence.
		_, other := registerUser(t, accounts)
		req := &blobpb.GetBlobsRequest{BlobIds: &blobpb.BlobIdBatch{BlobIds: []*blobpb.BlobId{readyID}}}
		require.NoError(t, other.Auth(req, &req.Auth))

		resp, err := server.GetBlobs(context.Background(), req)
		require.NoError(t, err)
		require.Equal(t, blobpb.GetBlobsResponse_OK, resp.Result)
		require.Nil(t, resp.Blobs)
	})

	t.Run("pending blob has status but no metadata", func(t *testing.T) {
		req := &blobpb.GetBlobsRequest{BlobIds: &blobpb.BlobIdBatch{BlobIds: []*blobpb.BlobId{pendingID}}}
		require.NoError(t, signer.Auth(req, &req.Auth))

		resp, err := server.GetBlobs(context.Background(), req)
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

		resp, err := server.GetBlobs(context.Background(), req)
		require.NoError(t, err)
		require.Equal(t, blobpb.GetBlobsResponse_OK, resp.Result)
		require.Nil(t, resp.Blobs)
	})

	t.Run("missing auth is rejected", func(t *testing.T) {
		req := &blobpb.GetBlobsRequest{BlobIds: &blobpb.BlobIdBatch{BlobIds: []*blobpb.BlobId{readyID}}}
		_, err := server.GetBlobs(context.Background(), req)
		require.Equal(t, codes.InvalidArgument, status.Code(err))
	})
}

// initiate runs InitiateExternalUpload and returns the reserved id and target.
func initiate(t *testing.T, server *blob.Server, signer model.KeyPair, mimeType string, sizeBytes uint64) (*blobpb.BlobId, *blobpb.UploadTarget) {
	req := &blobpb.InitiateExternalUploadRequest{MimeType: mimeType, SizeBytes: sizeBytes}
	require.NoError(t, signer.Auth(req, &req.Auth))

	resp, err := server.InitiateExternalUpload(context.Background(), req)
	require.NoError(t, err)
	require.Equal(t, blobpb.InitiateExternalUploadResponse_OK, resp.Result)
	return resp.BlobId, resp.UploadTarget
}

// complete runs CompleteExternalUpload and returns the resulting status.
func complete(t *testing.T, server *blob.Server, signer model.KeyPair, blobID *blobpb.BlobId) blobpb.BlobStatus {
	req := &blobpb.CompleteExternalUploadRequest{BlobId: blobID}
	require.NoError(t, signer.Auth(req, &req.Auth))

	resp, err := server.CompleteExternalUpload(context.Background(), req)
	require.NoError(t, err)
	require.Equal(t, blobpb.CompleteExternalUploadResponse_OK, resp.Result)
	return resp.Status
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

type fakeModerator struct {
	flagged bool
}

func (m *fakeModerator) ClassifyText(context.Context, string) (*moderation.Result, error) {
	return &moderation.Result{}, nil
}

func (m *fakeModerator) ClassifyImage(context.Context, []byte) (*moderation.Result, error) {
	return &moderation.Result{Flagged: m.flagged}, nil
}

func (m *fakeModerator) ClassifyCurrencyName(context.Context, string) (*moderation.Result, error) {
	return &moderation.Result{}, nil
}
