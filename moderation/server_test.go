package moderation

import (
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"errors"
	"strings"
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
	"github.com/code-payments/ocp-server/solana/currencycreator"
)

type mockClient struct {
	textResult         *Result
	imageResult        *Result
	currencyNameResult *Result
	usernameResult     *Result
	displayNameResult  *Result
	err                error

	// calls records each classifier called, in order
	calls []string
}

func (m *mockClient) ClassifyText(_ context.Context, _ string) (*Result, error) {
	m.calls = append(m.calls, "ClassifyText")
	return m.textResult, m.err
}

func (m *mockClient) ClassifyImage(_ context.Context, _ []byte) (*Result, error) {
	m.calls = append(m.calls, "ClassifyImage")
	return m.imageResult, m.err
}

func (m *mockClient) ClassifyCurrencyName(_ context.Context, _ string) (*Result, error) {
	m.calls = append(m.calls, "ClassifyCurrencyName")
	return m.currencyNameResult, m.err
}

func (m *mockClient) ClassifyUsername(_ context.Context, _ string) (*Result, error) {
	m.calls = append(m.calls, "ClassifyUsername")
	return m.usernameResult, m.err
}

func (m *mockClient) ClassifyDisplayName(_ context.Context, _ string) (*Result, error) {
	m.calls = append(m.calls, "ClassifyDisplayName")
	return m.displayNameResult, m.err
}

func (m *mockClient) ClassifyGroupTitle(_ context.Context, _ string) (*Result, error) {
	return nil, errors.New("not implemented")
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

	assert.Equal(t, moderationpb.ModerateImageResponse_UNSUPPORTED_FORMAT, resp.Result)
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

func newKindTestServer(t *testing.T, client *mockClient) (*Server, model.KeyPair, model.KeyPair) {
	t.Helper()

	log := zaptest.NewLogger(t)
	authz := auth.NewStaticAuthorizer(log)
	attestor := model.MustGenerateKeyPair()
	keyPair := model.MustGenerateKeyPair()
	authz.Add(model.MustGenerateUserID(), keyPair)

	return NewServer(log, authz, client, attestor), attestor, keyPair
}

func moderateText(t *testing.T, server *Server, keyPair model.KeyPair, kind moderationpb.ContentKind, text string) (*moderationpb.ModerateTextResponse, error) {
	t.Helper()

	req := &moderationpb.ModerateTextRequest{
		Text: text,
		Kind: kind,
	}
	require.NoError(t, keyPair.Auth(req, &req.Auth))
	return server.ModerateText(context.Background(), req)
}

var flaggedAsCryptocurrency = &Result{
	Flagged:           true,
	FlaggedCategories: []string{"cryptocurrency"},
	CategoryScores:    map[string]float64{"cryptocurrency": 0.95},
}

func TestModerateText_LegacyAttestationNamesNoKind(t *testing.T) {
	notFlagged := &Result{Flagged: false}
	client := &mockClient{textResult: notFlagged, currencyNameResult: notFlagged}
	server, attestor, keyPair := newKindTestServer(t, client)

	resp, err := moderateText(t, server, keyPair, moderationpb.ContentKind_UNKNOWN_CONTENT_KIND, "hello world")
	require.NoError(t, err)
	require.True(t, resp.IsAllowed)
	assert.Equal(t, moderationpb.ContentKind_UNKNOWN_CONTENT_KIND, resp.Attestation.Kind)
	assert.Equal(t, []string{"ClassifyText", "ClassifyCurrencyName"}, client.calls)
	verifyAttestation(t, resp.Attestation, attestor)
}

func TestModerateText_CurrencyName(t *testing.T) {
	for _, kind := range []moderationpb.ContentKind{
		moderationpb.ContentKind_CURRENCY_NAME,
		moderationpb.ContentKind_CURRENCY_SYMBOL,
	} {
		t.Run(kind.String(), func(t *testing.T) {
			// Allowed: every check runs, and the attestation names the kind
			notFlagged := &Result{Flagged: false}
			client := &mockClient{textResult: notFlagged, currencyNameResult: notFlagged}
			server, attestor, keyPair := newKindTestServer(t, client)

			resp, err := moderateText(t, server, keyPair, kind, "Moon")
			require.NoError(t, err)
			require.True(t, resp.IsAllowed)
			require.NotNil(t, resp.Attestation)
			assert.Equal(t, kind, resp.Attestation.Kind)
			assert.Equal(t, []string{"ClassifyText", "ClassifyCurrencyName"}, client.calls)
			verifyAttestation(t, resp.Attestation, attestor)

			// Flagged by the currency name classifier
			client = &mockClient{textResult: notFlagged, currencyNameResult: flaggedAsCryptocurrency}
			server, _, keyPair = newKindTestServer(t, client)

			resp, err = moderateText(t, server, keyPair, kind, "BTC")
			require.NoError(t, err)
			assert.False(t, resp.IsAllowed)
			assert.Nil(t, resp.Attestation)
			assert.Equal(t, moderationpb.FlaggedCategory_IMPERSONATION, resp.FlaggedCategory)

			// Flagged as a reserved platform name
			client = &mockClient{textResult: notFlagged, currencyNameResult: notFlagged}
			server, _, keyPair = newKindTestServer(t, client)

			resp, err = moderateText(t, server, keyPair, kind, "USDF")
			require.NoError(t, err)
			assert.False(t, resp.IsAllowed)
			assert.Nil(t, resp.Attestation)
			assert.Equal(t, moderationpb.FlaggedCategory_IMPERSONATION, resp.FlaggedCategory)

			// The generic classifier declining is fatal
			client = &mockClient{err: ErrUnsupportedLanguage}
			server, _, keyPair = newKindTestServer(t, client)

			resp, err = moderateText(t, server, keyPair, kind, "Moon")
			require.NoError(t, err)
			assert.Equal(t, moderationpb.ModerateTextResponse_UNSUPPORTED_LANGUAGE, resp.Result)
			assert.False(t, resp.IsAllowed)
			assert.Nil(t, resp.Attestation)
		})
	}
}

func TestModerateText_TooLongForKind(t *testing.T) {
	for kind, text := range map[moderationpb.ContentKind]string{
		moderationpb.ContentKind_CURRENCY_NAME:   strings.Repeat("a", currencycreator.MaxCurrencyConfigAccountNameLength+1),
		moderationpb.ContentKind_CURRENCY_SYMBOL: strings.Repeat("A", currencycreator.MaxCurrencyConfigAccountSymbolLength+1),
	} {
		client := &mockClient{}
		server, _, keyPair := newKindTestServer(t, client)

		_, err := moderateText(t, server, keyPair, kind, text)
		assert.Equal(t, codes.InvalidArgument, status.Code(err), kind.String())
		assert.Empty(t, client.calls, kind.String())
	}

	// At the limit is fine
	for kind, text := range map[moderationpb.ContentKind]string{
		moderationpb.ContentKind_CURRENCY_NAME:   strings.Repeat("a", currencycreator.MaxCurrencyConfigAccountNameLength),
		moderationpb.ContentKind_CURRENCY_SYMBOL: strings.Repeat("A", currencycreator.MaxCurrencyConfigAccountSymbolLength),
	} {
		notFlagged := &Result{Flagged: false}
		client := &mockClient{textResult: notFlagged, currencyNameResult: notFlagged}
		server, _, keyPair := newKindTestServer(t, client)

		resp, err := moderateText(t, server, keyPair, kind, text)
		require.NoError(t, err, kind.String())
		assert.True(t, resp.IsAllowed, kind.String())
	}
}

func TestModerateText_CurrencyDescriptionSkipsNameChecks(t *testing.T) {
	// Short enough to be a name, naming a token and the platform: a name's
	// checks would refuse it, a description's do not run them
	client := &mockClient{textResult: &Result{Flagged: false}, currencyNameResult: flaggedAsCryptocurrency}
	server, attestor, keyPair := newKindTestServer(t, client)

	resp, err := moderateText(t, server, keyPair, moderationpb.ContentKind_CURRENCY_DESCRIPTION, "Like Bitcoin, on Flipcash")
	require.NoError(t, err)
	require.True(t, resp.IsAllowed)
	require.NotNil(t, resp.Attestation)
	assert.Equal(t, moderationpb.ContentKind_CURRENCY_DESCRIPTION, resp.Attestation.Kind)
	assert.Equal(t, []string{"ClassifyText"}, client.calls)
	verifyAttestation(t, resp.Attestation, attestor)

	// The generic classifier still decides
	client = &mockClient{textResult: &Result{
		Flagged:           true,
		FlaggedCategories: []string{"hate"},
		CategoryScores:    map[string]float64{"hate": 3.0},
	}}
	server, _, keyPair = newKindTestServer(t, client)

	resp, err = moderateText(t, server, keyPair, moderationpb.ContentKind_CURRENCY_DESCRIPTION, "bad content")
	require.NoError(t, err)
	assert.False(t, resp.IsAllowed)
	assert.Nil(t, resp.Attestation)
	assert.Equal(t, moderationpb.FlaggedCategory_NSFW, resp.FlaggedCategory)

	// And declining is fatal
	client = &mockClient{err: ErrUnsupportedLanguage}
	server, _, keyPair = newKindTestServer(t, client)

	resp, err = moderateText(t, server, keyPair, moderationpb.ContentKind_CURRENCY_DESCRIPTION, "a description")
	require.NoError(t, err)
	assert.Equal(t, moderationpb.ModerateTextResponse_UNSUPPORTED_LANGUAGE, resp.Result)
}

func TestModerateText_ClassifierError(t *testing.T) {
	for _, kind := range []moderationpb.ContentKind{
		moderationpb.ContentKind_UNKNOWN_CONTENT_KIND,
		moderationpb.ContentKind_CURRENCY_NAME,
		moderationpb.ContentKind_CURRENCY_SYMBOL,
		moderationpb.ContentKind_CURRENCY_DESCRIPTION,
	} {
		client := &mockClient{err: errors.New("boom")}
		server, _, keyPair := newKindTestServer(t, client)

		_, err := moderateText(t, server, keyPair, kind, "Moon")
		assert.Equal(t, codes.Internal, status.Code(err), kind.String())
	}
}

func TestModerateText_ImageKind(t *testing.T) {
	client := &mockClient{}
	server, _, keyPair := newKindTestServer(t, client)

	_, err := moderateText(t, server, keyPair, moderationpb.ContentKind_CURRENCY_ICON, "Moon")
	assert.Equal(t, codes.InvalidArgument, status.Code(err))
	assert.Empty(t, client.calls)
}

func TestModerateImage_Kind(t *testing.T) {
	imageData := append([]byte{0xFF, 0xD8, 0xFF}, []byte("fake-image-data")...)

	for _, kind := range []moderationpb.ContentKind{
		moderationpb.ContentKind_UNKNOWN_CONTENT_KIND,
		moderationpb.ContentKind_CURRENCY_ICON,
	} {
		client := &mockClient{imageResult: &Result{Flagged: false}}
		server, attestor, keyPair := newKindTestServer(t, client)

		req := &moderationpb.ModerateImageRequest{ImageData: imageData, Kind: kind}
		require.NoError(t, keyPair.Auth(req, &req.Auth))

		resp, err := server.ModerateImage(context.Background(), req)
		require.NoError(t, err, kind.String())
		require.True(t, resp.IsAllowed, kind.String())
		assert.Equal(t, kind, resp.Attestation.Kind)
		verifyAttestation(t, resp.Attestation, attestor)
	}

	for _, kind := range []moderationpb.ContentKind{
		moderationpb.ContentKind_CURRENCY_NAME,
		moderationpb.ContentKind_CURRENCY_SYMBOL,
		moderationpb.ContentKind_CURRENCY_DESCRIPTION,
	} {
		client := &mockClient{imageResult: &Result{Flagged: false}}
		server, _, keyPair := newKindTestServer(t, client)

		req := &moderationpb.ModerateImageRequest{ImageData: imageData, Kind: kind}
		require.NoError(t, keyPair.Auth(req, &req.Auth))

		_, err := server.ModerateImage(context.Background(), req)
		assert.Equal(t, codes.InvalidArgument, status.Code(err), kind.String())
		assert.Empty(t, client.calls, kind.String())
	}
}
