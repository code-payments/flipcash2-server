package moderation

import (
	"bytes"
	"crypto/ed25519"
	"crypto/sha256"
	"errors"
	"time"

	"google.golang.org/protobuf/proto"

	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"
	moderationpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/moderation/v1"
)

const (
	maxAttestationAge = 24 * time.Hour
)

var (
	ErrInvalidAttestation  = errors.New("invalid attestation")
	ErrInvalidSignature    = errors.New("invalid attestation signature")
	ErrInvalidAttestor     = errors.New("attestor public key mismatch")
	ErrInvalidContentHash  = errors.New("content hash mismatch")
	ErrExpiredAttestation  = errors.New("attestation is expired")
	ErrContentKindMismatch = errors.New("content was moderated as a different kind")
)

// ValidateAttestation validates a moderation attestation against the expected
// attestor public key and content, used as kind. Content must be a string (for
// text) or []byte (for images).
//
// The attestation must name kind: each kind is moderated under its own checks,
// so content moderated as one kind (e.g. a currency description, which skips a
// name's impersonation checks) must not pass where another is used (e.g. as the
// currency's name). kind is the caller's, decided by where the content is used,
// never by the client; it must not be UNKNOWN_CONTENT_KIND.
//
// acceptUnknownKind admits an attestation naming no kind, from a legacy client's
// request, for any kind. That is safe because a legacy request gets the checks
// every kind once shared, under which any text short enough to be a currency
// name was checked as one. It exists for the transition only: once legacy
// clients are gone, pass false, and after maxAttestationAge no legacy
// attestation remains valid anyway.
func ValidateAttestation(attestation *moderationpb.ModerationAttestation, attestor *commonpb.PublicKey, kind moderationpb.ContentKind, content any, acceptUnknownKind bool) error {
	if attestation == nil {
		return ErrInvalidAttestation
	}

	if kind == moderationpb.ContentKind_UNKNOWN_CONTENT_KIND {
		return ErrInvalidAttestation
	}

	// Verify the attestor public key matches the expected key
	if !bytes.Equal(attestation.Attestor.GetValue(), attestor.GetValue()) {
		return ErrInvalidAttestor
	}

	// Verify the content hash matches the expected content
	var expectedHash [sha256.Size]byte
	switch v := content.(type) {
	case string:
		expectedHash = sha256.Sum256([]byte(v))
	case []byte:
		expectedHash = sha256.Sum256(v)
	default:
		return ErrInvalidAttestation
	}

	if !bytes.Equal(attestation.ContentHash, expectedHash[:]) {
		return ErrInvalidContentHash
	}

	// Verify the content was moderated as the kind it is used as
	switch {
	case attestation.Kind == kind:
	case attestation.Kind == moderationpb.ContentKind_UNKNOWN_CONTENT_KIND && acceptUnknownKind:
	default:
		return ErrContentKindMismatch
	}

	if time.Since(attestation.Timestamp.AsTime()) > maxAttestationAge {
		return ErrExpiredAttestation
	}

	// Verify the signature
	sig := attestation.Signature
	attestation.Signature = nil
	defer func() { attestation.Signature = sig }()

	b, err := proto.Marshal(attestation)
	if err != nil {
		return ErrInvalidAttestation
	}

	if !ed25519.Verify(ed25519.PublicKey(attestor.GetValue()), b, sig.GetValue()) {
		return ErrInvalidSignature
	}

	return nil
}
