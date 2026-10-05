package blob

import (
	"context"

	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"
)

// End-to-end encrypted blobs are the one kind of blob the server cannot read.
// A client encrypts an image for a chat (see messagingpb.EncryptedContent,
// which fixes the format) and uploads the ciphertext as an opaque octet
// stream, so the server derives nothing from the bytes: no metadata, no
// renditions, no moderation, no privacy-metadata check. What it does instead
// is pin the blob to the chat at reservation — the chat must take encrypted
// content and the caller must be able to send it there (see
// EncryptedUploadGate) — check nothing but the size at finalization, grant
// the chat read access in the same step that makes the blob READY, and refuse
// the blob on every surface other than encrypted content in that chat (see
// validateAttachable). Everything specific to that kind is gathered here; the
// pipeline arms live beside their plaintext counterparts.
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

// EncryptedUploadGate is the slice of the chat domain the upload path needs
// to admit an end-to-end encrypted upload: whether a chat takes encrypted
// content, and whether the caller may send it there right now. Which chats
// do — a DM, and a private group once it has its chat key — and what admits
// a sender are the chat domain's rules, and change there. It is declared here
// (consumer side) so this package need not import chat — which imports this
// package to attach pictures and match its errors — and the chat package
// supplies the adapter (chat.NewBlobEncryptedUploadGate), so neither the chat
// ID discriminator nor the rules are duplicated here.
type EncryptedUploadGate interface {
	// CanUploadEncrypted reports whether chatID takes end-to-end encrypted
	// content and userID may send it there. A chat that takes none, an
	// unknown chat, or a caller who may not send in it is false with no
	// error, so a refused caller learns nothing of the chat.
	CanUploadEncrypted(ctx context.Context, chatID *commonpb.ChatId, userID *commonpb.UserId) (bool, error)
}
