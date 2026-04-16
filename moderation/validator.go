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
	ErrInvalidAttestation = errors.New("invalid attestation")
	ErrInvalidSignature   = errors.New("invalid attestation signature")
	ErrInvalidAttestor    = errors.New("attestor public key mismatch")
	ErrInvalidContentHash = errors.New("content hash mismatch")
	ErrExpiredAttestation = errors.New("attestation is expired")
)

// ValidateAttestation validates a moderation attestation against the expected
// attestor public key and content. Content must be a string (for text) or
// []byte (for images).
func ValidateAttestation(attestation *moderationpb.ModerationAttestation, attestor *commonpb.PublicKey, content any) error {
	if attestation == nil {
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
