package moderation

import (
	"crypto/sha256"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/timestamppb"

	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"
	moderationpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/moderation/v1"

	"github.com/code-payments/flipcash2-server/model"
)

func TestValidateAttestation_Text(t *testing.T) {
	attestor := model.MustGenerateKeyPair()
	userID := model.MustGenerateUserID()
	text := "hello world"

	attestation := signTestAttestation(t, attestor, text, userID, time.Now())

	err := ValidateAttestation(attestation, attestor.Proto(), text)
	require.NoError(t, err)
}

func TestValidateAttestation_Image(t *testing.T) {
	attestor := model.MustGenerateKeyPair()
	userID := model.MustGenerateUserID()
	imageData := []byte("fake-image-data")

	attestation := signTestAttestation(t, attestor, imageData, userID, time.Now())

	err := ValidateAttestation(attestation, attestor.Proto(), imageData)
	require.NoError(t, err)
}

func TestValidateAttestation_NilAttestation(t *testing.T) {
	attestor := model.MustGenerateKeyPair()

	err := ValidateAttestation(nil, attestor.Proto(), "text")
	assert.ErrorIs(t, err, ErrInvalidAttestation)
}

func TestValidateAttestation_WrongAttestor(t *testing.T) {
	attestor := model.MustGenerateKeyPair()
	otherKey := model.MustGenerateKeyPair()
	userID := model.MustGenerateUserID()
	text := "hello world"

	attestation := signTestAttestation(t, attestor, text, userID, time.Now())

	err := ValidateAttestation(attestation, otherKey.Proto(), text)
	assert.ErrorIs(t, err, ErrInvalidAttestor)
}

func TestValidateAttestation_Expired(t *testing.T) {
	attestor := model.MustGenerateKeyPair()
	userID := model.MustGenerateUserID()
	text := "hello world"

	attestation := signTestAttestation(t, attestor, text, userID, time.Now().Add(-maxAttestationAge-time.Second))

	err := ValidateAttestation(attestation, attestor.Proto(), text)
	assert.ErrorIs(t, err, ErrExpiredAttestation)
}

func TestValidateAttestation_WrongTextContent(t *testing.T) {
	attestor := model.MustGenerateKeyPair()
	userID := model.MustGenerateUserID()
	text := "hello world"

	attestation := signTestAttestation(t, attestor, text, userID, time.Now())

	err := ValidateAttestation(attestation, attestor.Proto(), "different text")
	assert.ErrorIs(t, err, ErrInvalidContentHash)
}

func TestValidateAttestation_WrongImageContent(t *testing.T) {
	attestor := model.MustGenerateKeyPair()
	userID := model.MustGenerateUserID()
	imageData := []byte("image-data")

	attestation := signTestAttestation(t, attestor, imageData, userID, time.Now())

	err := ValidateAttestation(attestation, attestor.Proto(), []byte("different-image"))
	assert.ErrorIs(t, err, ErrInvalidContentHash)
}

func TestValidateAttestation_TamperedSignature(t *testing.T) {
	attestor := model.MustGenerateKeyPair()
	userID := model.MustGenerateUserID()
	text := "hello world"

	attestation := signTestAttestation(t, attestor, text, userID, time.Now())
	attestation.Signature.Value[0] ^= 0xff

	err := ValidateAttestation(attestation, attestor.Proto(), text)
	assert.ErrorIs(t, err, ErrInvalidSignature)
}

func TestValidateAttestation_UnsupportedContentType(t *testing.T) {
	attestor := model.MustGenerateKeyPair()
	userID := model.MustGenerateUserID()

	attestation := signTestAttestation(t, attestor, "text", userID, time.Now())

	err := ValidateAttestation(attestation, attestor.Proto(), 12345)
	assert.ErrorIs(t, err, ErrInvalidAttestation)
}

func signTestAttestation(t *testing.T, attestor model.KeyPair, content any, userID *commonpb.UserId, ts time.Time) *moderationpb.ModerationAttestation {
	t.Helper()

	var hash [sha256.Size]byte
	switch v := content.(type) {
	case string:
		hash = sha256.Sum256([]byte(v))
	case []byte:
		hash = sha256.Sum256(v)
	}

	attestation := &moderationpb.ModerationAttestation{
		ContentHash: hash[:],
		Timestamp:   timestamppb.New(ts),
		UserId:      userID,
		Attestor:    attestor.Proto(),
	}

	require.NoError(t, attestor.Sign(attestation, &attestation.Signature))
	return attestation
}
