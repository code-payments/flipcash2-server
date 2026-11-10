package moderation

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	moderationpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/moderation/v1"

	"github.com/code-payments/flipcash2-server/model"
	ocp_integration "github.com/code-payments/ocp-server/ocp/integration"
)

var ocpKindsByContentKind = map[moderationpb.ContentKind]ocp_integration.ModeratedContentKind{
	moderationpb.ContentKind_CURRENCY_NAME:        ocp_integration.ModeratedContentKindCurrencyName,
	moderationpb.ContentKind_CURRENCY_SYMBOL:      ocp_integration.ModeratedContentKindCurrencySymbol,
	moderationpb.ContentKind_CURRENCY_DESCRIPTION: ocp_integration.ModeratedContentKindCurrencyDescription,
	moderationpb.ContentKind_CURRENCY_ICON:        ocp_integration.ModeratedContentKindCurrencyIcon,
}

// A description short enough to be a name, which a name's checks would refuse,
// is moderated as a description and must not pass as the currency's name.
func TestIntegration_DescriptionAttestationIsNotAName(t *testing.T) {
	ctx := context.Background()
	text := "Bitcoin"

	client := &mockClient{textResult: &Result{Flagged: false}, currencyNameResult: flaggedAsCryptocurrency}
	server, attestor, keyPair := newKindTestServer(t, client)

	resp, err := moderateText(t, server, keyPair, moderationpb.ContentKind_CURRENCY_DESCRIPTION, text)
	require.NoError(t, err)
	require.True(t, resp.IsAllowed)
	raw, err := proto.Marshal(resp.Attestation)
	require.NoError(t, err)

	for _, acceptUnknownKind := range []bool{false, true} {
		integration := NewIntegration(attestor.Proto(), acceptUnknownKind)

		ok, err := integration.ValidateAttestation(ctx, nil, raw, ocp_integration.ModeratedContentKindCurrencyDescription, text)
		require.NoError(t, err)
		assert.True(t, ok)

		for _, kind := range []ocp_integration.ModeratedContentKind{
			ocp_integration.ModeratedContentKindCurrencyName,
			ocp_integration.ModeratedContentKindCurrencySymbol,
			ocp_integration.ModeratedContentKindCurrencyIcon,
		} {
			ok, err := integration.ValidateAttestation(ctx, nil, raw, kind, text)
			require.NoError(t, err)
			assert.False(t, ok, kind.String())
		}
	}

	// Moderated as a name, it is refused outright
	resp, err = moderateText(t, server, keyPair, moderationpb.ContentKind_CURRENCY_NAME, text)
	require.NoError(t, err)
	assert.False(t, resp.IsAllowed)
}

func TestIntegration_KindMapping(t *testing.T) {
	ctx := context.Background()
	attestor := model.MustGenerateKeyPair()
	text := "Moon"

	for moderatedAs := range ocpKindsByContentKind {
		raw := signRawTestAttestation(t, attestor, moderatedAs, text)
		for contentKind, usedAs := range ocpKindsByContentKind {
			ok, err := NewIntegration(attestor.Proto(), false).ValidateAttestation(ctx, nil, raw, usedAs, text)
			require.NoError(t, err)
			assert.Equal(t, moderatedAs == contentKind, ok, "moderated as %s, used as %s", moderatedAs, usedAs)
		}
	}
}

func TestIntegration_LegacyAttestation(t *testing.T) {
	ctx := context.Background()
	attestor := model.MustGenerateKeyPair()
	text := "Moon"

	raw := signRawTestAttestation(t, attestor, moderationpb.ContentKind_UNKNOWN_CONTENT_KIND, text)
	for _, usedAs := range ocpKindsByContentKind {
		ok, err := NewIntegration(attestor.Proto(), true).ValidateAttestation(ctx, nil, raw, usedAs, text)
		require.NoError(t, err)
		assert.True(t, ok, usedAs.String())

		ok, err = NewIntegration(attestor.Proto(), false).ValidateAttestation(ctx, nil, raw, usedAs, text)
		require.NoError(t, err)
		assert.False(t, ok, usedAs.String())
	}
}

func TestIntegration_UnsupportedKind(t *testing.T) {
	ctx := context.Background()
	attestor := model.MustGenerateKeyPair()
	text := "Moon"

	raw := signRawTestAttestation(t, attestor, moderationpb.ContentKind_UNKNOWN_CONTENT_KIND, text)
	for _, kind := range []ocp_integration.ModeratedContentKind{
		ocp_integration.ModeratedContentKindUnknown,
		ocp_integration.ModeratedContentKindCurrencyIcon + 1,
	} {
		ok, err := NewIntegration(attestor.Proto(), true).ValidateAttestation(ctx, nil, raw, kind, text)
		assert.Error(t, err)
		assert.False(t, ok)
	}
}

func TestIntegration_MissingOrMalformedAttestation(t *testing.T) {
	ctx := context.Background()
	integration := NewIntegration(model.MustGenerateKeyPair().Proto(), true)

	for _, raw := range [][]byte{nil, {0xff, 0xff}} {
		ok, err := integration.ValidateAttestation(ctx, nil, raw, ocp_integration.ModeratedContentKindCurrencyName, "Moon")
		require.NoError(t, err)
		assert.False(t, ok)
	}
}
