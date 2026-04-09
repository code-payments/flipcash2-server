package moderation

import (
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	moderationpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/moderation/v1"

	"github.com/code-payments/flipcash2-server/auth"
	"github.com/code-payments/flipcash2-server/model"
)

type mockClient struct {
	textResult         *Result
	imageResult        *Result
	currencyNameResult *Result
	err                error
}

func (m *mockClient) ClassifyText(_ context.Context, _ string) (*Result, error) {
	return m.textResult, m.err
}

func (m *mockClient) ClassifyImage(_ context.Context, _ []byte) (*Result, error) {
	return m.imageResult, m.err
}

func (m *mockClient) ClassifyCurrencyName(_ context.Context, _ string) (*Result, error) {
	return m.currencyNameResult, m.err
}

func TestModerateText_Allowed(t *testing.T) {
	ctx := context.Background()
	log := zaptest.NewLogger(t)

	authz := auth.NewStaticAuthorizer(log)
	attestor := model.MustGenerateKeyPair()
	notFlagged := &Result{Flagged: false}
	client := &mockClient{textResult: notFlagged, currencyNameResult: notFlagged}
	server := NewServer(log, authz, client, attestor)

	userID := model.MustGenerateUserID()
	keyPair := model.MustGenerateKeyPair()
	authz.Add(userID, keyPair)

	req := &moderationpb.ModerateTextRequest{
		Text: "hello world",
	}
	require.NoError(t, keyPair.Auth(req, &req.Auth))

	resp, err := server.ModerateText(ctx, req)
	require.NoError(t, err)

	assert.Equal(t, moderationpb.ModerateTextResponse_OK, resp.Result)
	assert.True(t, resp.IsAllowed)
	require.NotNil(t, resp.Attestation)

	// Verify attestation content
	expectedHash := sha256.Sum256([]byte("hello world"))
	assert.Equal(t, expectedHash[:], resp.Attestation.ContentHash)
	assert.Equal(t, userID.Value, resp.Attestation.UserId.Value)
	assert.Equal(t, attestor.Proto().Value, resp.Attestation.Attestor.Value)
	assert.NotNil(t, resp.Attestation.Timestamp)

	// Verify attestation signature
	verifyAttestation(t, resp.Attestation, attestor)
}

func TestModerateText_Flagged(t *testing.T) {
	ctx := context.Background()
	log := zaptest.NewLogger(t)

	authz := auth.NewStaticAuthorizer(log)
	attestor := model.MustGenerateKeyPair()
	flagged := &Result{
		Flagged:           true,
		FlaggedCategories: []string{"hate"},
		CategoryScores:    map[string]float64{"hate": 3.0},
	}
	client := &mockClient{textResult: flagged}
	server := NewServer(log, authz, client, attestor)

	userID := model.MustGenerateUserID()
	keyPair := model.MustGenerateKeyPair()
	authz.Add(userID, keyPair)

	req := &moderationpb.ModerateTextRequest{
		Text: "bad content",
	}
	require.NoError(t, keyPair.Auth(req, &req.Auth))

	resp, err := server.ModerateText(ctx, req)
	require.NoError(t, err)

	assert.Equal(t, moderationpb.ModerateTextResponse_OK, resp.Result)
	assert.False(t, resp.IsAllowed)
	assert.Nil(t, resp.Attestation)
}

func TestModerateText_Unauthorized(t *testing.T) {
	ctx := context.Background()
	log := zaptest.NewLogger(t)

	authz := auth.NewStaticAuthorizer(log)
	attestor := model.MustGenerateKeyPair()
	notFlagged := &Result{Flagged: false}
	client := &mockClient{textResult: notFlagged, currencyNameResult: notFlagged}
	server := NewServer(log, authz, client, attestor)

	// Use an unregistered key pair
	keyPair := model.MustGenerateKeyPair()

	req := &moderationpb.ModerateTextRequest{
		Text: "hello world",
	}
	require.NoError(t, keyPair.Auth(req, &req.Auth))

	_, err := server.ModerateText(ctx, req)
	require.Error(t, err)
	assert.Equal(t, codes.PermissionDenied, status.Code(err))
}

func TestModerateText_CurrencyNameFlagged(t *testing.T) {
	ctx := context.Background()
	log := zaptest.NewLogger(t)

	authz := auth.NewStaticAuthorizer(log)
	attestor := model.MustGenerateKeyPair()
	client := &mockClient{
		textResult: &Result{Flagged: false},
		currencyNameResult: &Result{
			Flagged:           true,
			FlaggedCategories: []string{"cryptocurrency"},
			CategoryScores:    map[string]float64{"cryptocurrency": 0.95},
		},
	}
	server := NewServer(log, authz, client, attestor)

	userID := model.MustGenerateUserID()
	keyPair := model.MustGenerateKeyPair()
	authz.Add(userID, keyPair)

	req := &moderationpb.ModerateTextRequest{
		Text: "Bitcoin",
	}
	require.NoError(t, keyPair.Auth(req, &req.Auth))

	resp, err := server.ModerateText(ctx, req)
	require.NoError(t, err)

	assert.Equal(t, moderationpb.ModerateTextResponse_OK, resp.Result)
	assert.False(t, resp.IsAllowed)
	assert.Nil(t, resp.Attestation)
}

func TestModerateText_CurrencyNameSkippedForLongText(t *testing.T) {
	ctx := context.Background()
	log := zaptest.NewLogger(t)

	authz := auth.NewStaticAuthorizer(log)
	attestor := model.MustGenerateKeyPair()
	client := &mockClient{
		textResult: &Result{Flagged: false},
		currencyNameResult: &Result{
			Flagged:           true,
			FlaggedCategories: []string{"cryptocurrency"},
			CategoryScores:    map[string]float64{"cryptocurrency": 0.95},
		},
	}
	server := NewServer(log, authz, client, attestor)

	userID := model.MustGenerateUserID()
	keyPair := model.MustGenerateKeyPair()
	authz.Add(userID, keyPair)

	req := &moderationpb.ModerateTextRequest{
		Text: "this is a long text that exceeds the currency name length limit",
	}
	require.NoError(t, keyPair.Auth(req, &req.Auth))

	resp, err := server.ModerateText(ctx, req)
	require.NoError(t, err)

	assert.Equal(t, moderationpb.ModerateTextResponse_OK, resp.Result)
	assert.True(t, resp.IsAllowed)
	require.NotNil(t, resp.Attestation)
}

func TestModerateImage_Allowed(t *testing.T) {
	ctx := context.Background()
	log := zaptest.NewLogger(t)

	authz := auth.NewStaticAuthorizer(log)
	attestor := model.MustGenerateKeyPair()
	client := &mockClient{imageResult: &Result{Flagged: false}}
	server := NewServer(log, authz, client, attestor)

	userID := model.MustGenerateUserID()
	keyPair := model.MustGenerateKeyPair()
	authz.Add(userID, keyPair)

	imageData := append([]byte{0xFF, 0xD8, 0xFF}, []byte("fake-image-data")...)
	req := &moderationpb.ModerateImageRequest{
		ImageData: imageData,
	}
	require.NoError(t, keyPair.Auth(req, &req.Auth))

	resp, err := server.ModerateImage(ctx, req)
	require.NoError(t, err)

	assert.Equal(t, moderationpb.ModerateImageResponse_OK, resp.Result)
	assert.True(t, resp.IsAllowed)
	require.NotNil(t, resp.Attestation)

	// Verify attestation content
	expectedHash := sha256.Sum256(imageData)
	assert.Equal(t, expectedHash[:], resp.Attestation.ContentHash)
	assert.Equal(t, userID.Value, resp.Attestation.UserId.Value)
	assert.Equal(t, attestor.Proto().Value, resp.Attestation.Attestor.Value)
	assert.NotNil(t, resp.Attestation.Timestamp)

	// Verify attestation signature
	verifyAttestation(t, resp.Attestation, attestor)
}

func TestModerateImage_Flagged(t *testing.T) {
	ctx := context.Background()
	log := zaptest.NewLogger(t)

	authz := auth.NewStaticAuthorizer(log)
	attestor := model.MustGenerateKeyPair()
	client := &mockClient{imageResult: &Result{
		Flagged:           true,
		FlaggedCategories: []string{"general_nsfw"},
		CategoryScores:    map[string]float64{"general_nsfw": 0.95},
	}}
	server := NewServer(log, authz, client, attestor)

	userID := model.MustGenerateUserID()
	keyPair := model.MustGenerateKeyPair()
	authz.Add(userID, keyPair)

	imageData := append([]byte{0x89, 0x50, 0x4E, 0x47, 0x0D, 0x0A, 0x1A, 0x0A}, []byte("fake-nsfw-image")...)
	req := &moderationpb.ModerateImageRequest{
		ImageData: imageData,
	}
	require.NoError(t, keyPair.Auth(req, &req.Auth))

	resp, err := server.ModerateImage(ctx, req)
	require.NoError(t, err)

	assert.Equal(t, moderationpb.ModerateImageResponse_OK, resp.Result)
	assert.False(t, resp.IsAllowed)
	assert.Nil(t, resp.Attestation)
}

func TestModerateImage_InvalidFormat(t *testing.T) {
	ctx := context.Background()
	log := zaptest.NewLogger(t)

	authz := auth.NewStaticAuthorizer(log)
	attestor := model.MustGenerateKeyPair()
	client := &mockClient{imageResult: &Result{Flagged: false}}
	server := NewServer(log, authz, client, attestor)

	userID := model.MustGenerateUserID()
	keyPair := model.MustGenerateKeyPair()
	authz.Add(userID, keyPair)

	req := &moderationpb.ModerateImageRequest{
		ImageData: []byte("not-an-image"),
	}
	require.NoError(t, keyPair.Auth(req, &req.Auth))

	resp, err := server.ModerateImage(ctx, req)
	require.NoError(t, err)

	assert.Equal(t, moderationpb.ModerateImageResponse_OK, resp.Result)
	assert.False(t, resp.IsAllowed)
	assert.Nil(t, resp.Attestation)
}

func TestModerateImage_Unauthorized(t *testing.T) {
	ctx := context.Background()
	log := zaptest.NewLogger(t)

	authz := auth.NewStaticAuthorizer(log)
	attestor := model.MustGenerateKeyPair()
	client := &mockClient{imageResult: &Result{Flagged: false}}
	server := NewServer(log, authz, client, attestor)

	// Use an unregistered key pair
	keyPair := model.MustGenerateKeyPair()

	req := &moderationpb.ModerateImageRequest{
		ImageData: []byte("image"),
	}
	require.NoError(t, keyPair.Auth(req, &req.Auth))

	_, err := server.ModerateImage(ctx, req)
	require.Error(t, err)
	assert.Equal(t, codes.PermissionDenied, status.Code(err))
}

func verifyAttestation(t *testing.T, attestation *moderationpb.ModerationAttestation, attestor model.KeyPair) {
	t.Helper()

	sig := attestation.Signature
	attestation.Signature = nil

	b, err := proto.Marshal(attestation)
	require.NoError(t, err)

	assert.True(t, ed25519.Verify(attestor.Public(), b, sig.Value))

	attestation.Signature = sig
}
