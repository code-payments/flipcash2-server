package blob

import (
	"context"

	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"
)

// End-to-end encrypted blobs are the one kind of blob the server cannot read.
// A client encrypts an image for a DM (see messagingpb.EncryptedContent, which
// fixes the format) and uploads the ciphertext as an opaque octet stream, so
// the server derives nothing from the bytes: no metadata, no renditions, no
// moderation, no privacy-metadata check. What it does instead is pin the blob
// to the DM at reservation — the caller must be a member, and the chat must be
// a DM — check nothing but the size at finalization, grant the DM read access
// in the same step that makes the blob READY, and refuse the blob on every
// surface other than encrypted content in that chat (see validateAttachable).
// Everything specific to that kind is gathered here; the pipeline arms live
// beside their plaintext counterparts.
const (
	// MaxEncryptedBlobSizeBytes bounds the declared size of an end-to-end
	// encrypted upload. It is the only constraint the server enforces on such a
	// blob (blobpb.EncryptedConstraints), pinned into the presigned upload so
	// storage rejects anything larger, and re-checked against the stored bytes
	// at finalization. The server cannot tell what kind of media a ciphertext
	// holds, so one ceiling covers every kind: the largest plaintext ceiling
	// across the kinds a client may encrypt, which today is the image one. The
	// scheme's nonce and tag are not added on top; they are a rounding error
	// against a ceiling this size and not worth a constant that ties this
	// package to the scheme. When a larger kind (video) becomes encryptable this
	// grows to its ceiling.
	MaxEncryptedBlobSizeBytes = MaxOriginalImageSizeBytes

	// maxEncryptedImageDimension and maxEncryptedImagePixels are the bounds
	// the policy advises a sender to downscale an image to before encrypting
	// it. The server cannot check them — they are advisory by construction —
	// and an encrypted blob has no renditions, so a recipient downloads and
	// decodes exactly what the sender uploaded for every view of it. The
	// longest side matches the largest DISPLAY rung the server would have
	// derived for a plaintext image (see imageRenditionSpecs): the full-screen
	// view, and the most any client renders.
	maxEncryptedImageDimension = 1600
	maxEncryptedImagePixels    = maxEncryptedImageDimension * maxEncryptedImageDimension
)

// DMMembership is the slice of the chat domain the upload path needs to admit
// an end-to-end encrypted upload: whether a user is currently a member of a
// chat that is a DM. It is declared here (consumer side) so this package need
// not import chat — which imports this package to attach pictures and match
// its errors — and the chat package supplies an adapter over its store that
// also owns the DM-versus-group rule, so the chat ID discriminator is never
// duplicated here.
type DMMembership interface {
	// IsDMMember reports whether chatID names a DM and userID is a member of
	// it. A group chat ID, an unknown chat, or a non-member is false with no
	// error.
	IsDMMember(ctx context.Context, chatID *commonpb.ChatId, userID *commonpb.UserId) (bool, error)
}
