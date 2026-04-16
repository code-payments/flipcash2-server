package moderation

import (
	"context"

	"google.golang.org/protobuf/proto"

	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"
	moderationpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/moderation/v1"

	ocp_common "github.com/code-payments/ocp-server/ocp/common"
	ocp_integration "github.com/code-payments/ocp-server/ocp/integration"
)

type Integration struct {
	attestor *commonpb.PublicKey
}

func NewIntegration(attestor *commonpb.PublicKey) ocp_integration.Moderation {
	return &Integration{
		attestor: attestor,
	}
}

func (i *Integration) ValidateAttestation(_ context.Context, _ *ocp_common.Account, rawAttestation []byte, content any) (bool, error) {
	if len(rawAttestation) == 0 {
		return false, nil
	}

	var attestation moderationpb.ModerationAttestation
	if err := proto.Unmarshal(rawAttestation, &attestation); err != nil {
		return false, nil
	}

	err := ValidateAttestation(&attestation, i.attestor, content)
	if err != nil {
		return false, nil
	}
	return true, nil
}
