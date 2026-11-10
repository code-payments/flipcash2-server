package moderation

import (
	"crypto/sha256"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"
	moderationpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/moderation/v1"

	"github.com/code-payments/flipcash2-server/model"
)

func TestValidateAttestation_Text(t *testing.T) {
	attestor := model.MustGenerateKeyPair()
	userID := model.MustGenerateUserID()
	text := "hello world"

	attestation := signTestAttestation(t, attestor, moderationpb.ContentKind_CURRENCY_NAME, text, userID, time.Now())

	err := ValidateAttestation(attestation, attestor.Proto(), moderationpb.ContentKind_CURRENCY_NAME, text, false)
	require.NoError(t, err)
}

func TestValidateAttestation_Image(t *testing.T) {
	attestor := model.MustGenerateKeyPair()
	userID := model.MustGenerateUserID()
	imageData := []byte("fake-image-data")

	attestation := signTestAttestation(t, attestor, moderationpb.ContentKind_CURRENCY_ICON, imageData, userID, time.Now())

	err := ValidateAttestation(attestation, attestor.Proto(), moderationpb.ContentKind_CURRENCY_ICON, imageData, false)
	require.NoError(t, err)
}

func TestValidateAttestation_NilAttestation(t *testing.T) {
	attestor := model.MustGenerateKeyPair()

	err := ValidateAttestation(nil, attestor.Proto(), moderationpb.ContentKind_CURRENCY_NAME, "text", false)
	assert.ErrorIs(t, err, ErrInvalidAttestation)
}

func TestValidateAttestation_WrongAttestor(t *testing.T) {
	attestor := model.MustGenerateKeyPair()
	otherKey := model.MustGenerateKeyPair()
	userID := model.MustGenerateUserID()
	text := "hello world"

	attestation := signTestAttestation(t, attestor, moderationpb.ContentKind_CURRENCY_NAME, text, userID, time.Now())

	err := ValidateAttestation(attestation, otherKey.Proto(), moderationpb.ContentKind_CURRENCY_NAME, text, false)
	assert.ErrorIs(t, err, ErrInvalidAttestor)
}

func TestValidateAttestation_Expired(t *testing.T) {
	attestor := model.MustGenerateKeyPair()
	userID := model.MustGenerateUserID()
	text := "hello world"

	attestation := signTestAttestation(t, attestor, moderationpb.ContentKind_CURRENCY_NAME, text, userID, time.Now().Add(-maxAttestationAge-time.Second))

	err := ValidateAttestation(attestation, attestor.Proto(), moderationpb.ContentKind_CURRENCY_NAME, text, false)
	assert.ErrorIs(t, err, ErrExpiredAttestation)
}

func TestValidateAttestation_WrongTextContent(t *testing.T) {
	attestor := model.MustGenerateKeyPair()
	userID := model.MustGenerateUserID()
	text := "hello world"

	attestation := signTestAttestation(t, attestor, moderationpb.ContentKind_CURRENCY_NAME, text, userID, time.Now())

	err := ValidateAttestation(attestation, attestor.Proto(), moderationpb.ContentKind_CURRENCY_NAME, "different text", false)
	assert.ErrorIs(t, err, ErrInvalidContentHash)
}

func TestValidateAttestation_WrongImageContent(t *testing.T) {
	attestor := model.MustGenerateKeyPair()
	userID := model.MustGenerateUserID()
	imageData := []byte("image-data")

	attestation := signTestAttestation(t, attestor, moderationpb.ContentKind_CURRENCY_ICON, imageData, userID, time.Now())

	err := ValidateAttestation(attestation, attestor.Proto(), moderationpb.ContentKind_CURRENCY_ICON, []byte("different-image"), false)
	assert.ErrorIs(t, err, ErrInvalidContentHash)
}

func TestValidateAttestation_TamperedSignature(t *testing.T) {
	attestor := model.MustGenerateKeyPair()
	userID := model.MustGenerateUserID()
	text := "hello world"

	attestation := signTestAttestation(t, attestor, moderationpb.ContentKind_CURRENCY_NAME, text, userID, time.Now())
	attestation.Signature.Value[0] ^= 0xff

	err := ValidateAttestation(attestation, attestor.Proto(), moderationpb.ContentKind_CURRENCY_NAME, text, false)
	assert.ErrorIs(t, err, ErrInvalidSignature)
}

func TestValidateAttestation_UnsupportedContentType(t *testing.T) {
	attestor := model.MustGenerateKeyPair()
	userID := model.MustGenerateUserID()

	attestation := signTestAttestation(t, attestor, moderationpb.ContentKind_CURRENCY_NAME, "text", userID, time.Now())

	err := ValidateAttestation(attestation, attestor.Proto(), moderationpb.ContentKind_CURRENCY_NAME, 12345, false)
	assert.ErrorIs(t, err, ErrInvalidAttestation)
}

var allContentKinds = []moderationpb.ContentKind{
	moderationpb.ContentKind_CURRENCY_NAME,
	moderationpb.ContentKind_CURRENCY_SYMBOL,
	moderationpb.ContentKind_CURRENCY_DESCRIPTION,
	moderationpb.ContentKind_CURRENCY_ICON,
}

func TestValidateAttestation_KindMismatch(t *testing.T) {
	attestor := model.MustGenerateKeyPair()
	userID := model.MustGenerateUserID()
	text := "Flipcash"

	for _, moderatedAs := range allContentKinds {
		attestation := signTestAttestation(t, attestor, moderatedAs, text, userID, time.Now())
		for _, usedAs := range allContentKinds {
			for _, acceptUnknownKind := range []bool{false, true} {
				err := ValidateAttestation(attestation, attestor.Proto(), usedAs, text, acceptUnknownKind)
				if moderatedAs == usedAs {
					assert.NoError(t, err, "moderated as %s, used as %s", moderatedAs, usedAs)
				} else {
					assert.ErrorIs(t, err, ErrContentKindMismatch, "moderated as %s, used as %s", moderatedAs, usedAs)
				}
			}
		}
	}
}

func TestValidateAttestation_UnknownKind(t *testing.T) {
	attestor := model.MustGenerateKeyPair()
	userID := model.MustGenerateUserID()
	text := "hello world"

	attestation := signTestAttestation(t, attestor, moderationpb.ContentKind_UNKNOWN_CONTENT_KIND, text, userID, time.Now())

	for _, usedAs := range allContentKinds {
		assert.NoError(t, ValidateAttestation(attestation, attestor.Proto(), usedAs, text, true), "used as %s", usedAs)
		assert.ErrorIs(t, ValidateAttestation(attestation, attestor.Proto(), usedAs, text, false), ErrContentKindMismatch, "used as %s", usedAs)
	}
}

func TestValidateAttestation_CallerMustNameKind(t *testing.T) {
	attestor := model.MustGenerateKeyPair()
	userID := model.MustGenerateUserID()
	text := "hello world"

	for _, moderatedAs := range append([]moderationpb.ContentKind{moderationpb.ContentKind_UNKNOWN_CONTENT_KIND}, allContentKinds...) {
		attestation := signTestAttestation(t, attestor, moderatedAs, text, userID, time.Now())
		err := ValidateAttestation(attestation, attestor.Proto(), moderationpb.ContentKind_UNKNOWN_CONTENT_KIND, text, true)
		assert.ErrorIs(t, err, ErrInvalidAttestation, "moderated as %s", moderatedAs)
	}
}

func TestValidateAttestation_TamperedKind(t *testing.T) {
	attestor := model.MustGenerateKeyPair()
	userID := model.MustGenerateUserID()
	text := "Flipcash"

	attestation := signTestAttestation(t, attestor, moderationpb.ContentKind_CURRENCY_DESCRIPTION, text, userID, time.Now())
	attestation.Kind = moderationpb.ContentKind_CURRENCY_NAME

	err := ValidateAttestation(attestation, attestor.Proto(), moderationpb.ContentKind_CURRENCY_NAME, text, false)
	assert.ErrorIs(t, err, ErrInvalidSignature)
}

func signTestAttestation(t *testing.T, attestor model.KeyPair, kind moderationpb.ContentKind, content any, userID *commonpb.UserId, ts time.Time) *moderationpb.ModerationAttestation {
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
		Kind:        kind,
	}

	require.NoError(t, attestor.Sign(attestation, &attestation.Signature))
	return attestation
}

func signRawTestAttestation(t *testing.T, attestor model.KeyPair, kind moderationpb.ContentKind, content any) []byte {
	t.Helper()

	raw, err := proto.Marshal(signTestAttestation(t, attestor, kind, content, model.MustGenerateUserID(), time.Now()))
	require.NoError(t, err)
	return raw
}
