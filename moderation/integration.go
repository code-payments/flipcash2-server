package moderation

import (
	"context"
	"fmt"

	"google.golang.org/protobuf/proto"

	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"
	moderationpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/moderation/v1"

	ocp_common "github.com/code-payments/ocp-server/ocp/common"
	ocp_integration "github.com/code-payments/ocp-server/ocp/integration"
)

type Integration struct {
	attestor          *commonpb.PublicKey
	acceptUnknownKind bool
}

// NewIntegration returns OCP's moderation integration, which accepts content
// whose attestation attestor signed for the kind OCP uses it as.
// acceptUnknownKind also accepts an attestation naming no kind, from a legacy
// client, for any kind; it is for the transition only (see ValidateAttestation).
func NewIntegration(attestor *commonpb.PublicKey, acceptUnknownKind bool) ocp_integration.Moderation {
	return &Integration{
		attestor:          attestor,
		acceptUnknownKind: acceptUnknownKind,
	}
}

func (i *Integration) ValidateAttestation(_ context.Context, _ *ocp_common.Account, rawAttestation []byte, kind ocp_integration.ModeratedContentKind, content any) (bool, error) {
	expected, ok := contentKindFromOCP(kind)
	if !ok {
		// OCP names the kind at each call site, so this is a kind added to OCP
		// that this integration was not taught: fail loudly rather than
		// accept content under checks never chosen for it.
		return false, fmt.Errorf("unsupported moderated content kind: %s", kind)
	}

	if len(rawAttestation) == 0 {
		return false, nil
	}

	var attestation moderationpb.ModerationAttestation
	if err := proto.Unmarshal(rawAttestation, &attestation); err != nil {
		return false, nil
	}

	err := ValidateAttestation(&attestation, i.attestor, expected, content, i.acceptUnknownKind)
	if err != nil {
		return false, nil
	}
	return true, nil
}

// contentKindFromOCP maps the kind OCP uses content as to the kind it must have
// been moderated as.
func contentKindFromOCP(kind ocp_integration.ModeratedContentKind) (moderationpb.ContentKind, bool) {
	switch kind {
	case ocp_integration.ModeratedContentKindCurrencyName:
		return moderationpb.ContentKind_CURRENCY_NAME, true
	case ocp_integration.ModeratedContentKindCurrencySymbol:
		return moderationpb.ContentKind_CURRENCY_SYMBOL, true
	case ocp_integration.ModeratedContentKindCurrencyDescription:
		return moderationpb.ContentKind_CURRENCY_DESCRIPTION, true
	case ocp_integration.ModeratedContentKindCurrencyIcon:
		return moderationpb.ContentKind_CURRENCY_ICON, true
	}
	return moderationpb.ContentKind_UNKNOWN_CONTENT_KIND, false
}
