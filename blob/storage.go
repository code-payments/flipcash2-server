package blob

import (
	"context"
	"errors"
	"fmt"

	blobpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/blob/v1"
)

// ErrObjectNotFound is returned by ObjectStorage.GetUploaded when no bytes have
// been uploaded for the key yet (the upload never happened or is incomplete).
var ErrObjectNotFound = errors.New("object not found")

// ObjectStorage is the bytes backend behind blobs. It spans two object stores:
// an UPLOAD store clients write to directly, and an ORIGIN store — fronted by a
// CDN — that only validated bytes are promoted into. Quarantining untrusted
// uploads this way means nothing is ever served until the server has read it
// back, validated it, and promoted it.
//
// The server never proxies blob bytes: it presigns upload targets, reads
// uploaded bytes back to validate them, promotes the good ones, and mints signed
// CDN URLs for serving. It is deliberately provider-agnostic. The production
// implementation is two S3 buckets (presigned PUT into the upload bucket,
// GetObject to read it back, a server-side copy into the origin bucket) with a
// CloudFront CDN in front of the origin bucket; an in-memory implementation
// backs tests.
type ObjectStorage interface {
	// PresignUpload mints a short-lived, presigned target the client uploads the
	// bytes to directly, into the UPLOAD store under the given key. The target
	// pins the declared content type and exact size, so storage rejects any
	// upload that does not match.
	PresignUpload(ctx context.Context, key, mimeType string, sizeBytes uint64) (*blobpb.UploadTarget, error)

	// GetUploaded returns the bytes a client uploaded under the key in the UPLOAD
	// store, or ErrObjectNotFound if no bytes are present yet. Finalization reads
	// them back to validate the content before promoting it.
	GetUploaded(ctx context.Context, key string) ([]byte, error)

	// CopyToOrigin copies a validated object from the UPLOAD store to the ORIGIN
	// store under the same key, making it servable through the CDN. It overwrites
	// any object already served under the key, so it is safe to call more than
	// once. The upload copy is intentionally left in place — the caller removes it
	// with DeleteUpload only after the blob's terminal state is durably recorded,
	// so an interrupted finalization is always replayable from the upload bytes.
	CopyToOrigin(ctx context.Context, key string) error

	// DeleteUpload removes an object from the UPLOAD store. It is best-effort
	// cleanup run after a blob reaches a terminal state, and is idempotent:
	// deleting an absent key is not an error.
	DeleteUpload(ctx context.Context, key string) error

	// SignDownloadURL mints a fresh, short-lived CDN URL for fetching a promoted
	// object's bytes from the ORIGIN store. It is authorized at mint time and
	// expires on its own, so callers mint a new one rather than persisting it.
	SignDownloadURL(ctx context.Context, key string) (string, error)
}

// StorageKey derives the object key for an image blob's bytes from its id and
// mime type. Images can have server-derived renditions (display, thumbnail, ...),
// so an image's bytes live under a per-media-item directory keyed by its id,
// leaving room to group its renditions under the same prefix:
//
//	images/<uuid>/original.jpg
//
// Only images are supported today, so a non-image mime type is rejected outright
// rather than being silently forced into the image layout. Other kinds (videos,
// files) may want a different prefix and layout — keyed off the kind, not merely
// a resolvable extension — so adding one has to make a deliberate decision here.
// The extension is derived from the (immutable) mime type, and the same key is
// used in both the upload and origin stores.
func StorageKey(id *blobpb.BlobId, mimeType string) (string, error) {
	// Gate on the image kind explicitly, not merely on a resolvable extension: if
	// mimeTypeToExtension later grows video/file entries, this still rejects them
	// so they cannot inherit the image layout by accident.
	if err := id.Validate(); err != nil {
		return "", err
	}
	if !SupportedImageMimeTypes[mimeType] {
		return "", fmt.Errorf("unsupported non-image mime type %q for storage key", mimeType)
	}
	ext := extensionForMimeType(mimeType)
	if ext == "" {
		// An image kind with no registered extension means the two image maps drifted.
		return "", fmt.Errorf("missing extension for image mime type %q", mimeType)
	}
	return fmt.Sprintf("images/%s/original%s", blobIDString(id), ext), nil
}
