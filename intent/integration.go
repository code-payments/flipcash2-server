package intent

import (
	"context"

	ocp_transactionpb "github.com/code-payments/ocp-protobuf-api/generated/go/transaction/v1"

	ocp_common "github.com/code-payments/ocp-server/ocp/common"
	ocp_data "github.com/code-payments/ocp-server/ocp/data"
	ocp_intent "github.com/code-payments/ocp-server/ocp/data/intent"
	ocp_transaction "github.com/code-payments/ocp-server/ocp/rpc/transaction"
)

type Integration struct {
	ocpData ocp_data.Provider
}

func NewIntegration(ocpData ocp_data.Provider) ocp_transaction.SubmitIntentIntegration {
	return &Integration{
		ocpData: ocpData,
	}
}

func (i *Integration) AllowCreation(ctx context.Context, intentRecord *ocp_intent.Record, metadata *ocp_transactionpb.Metadata, actions []*ocp_transactionpb.Action) error {
	switch intentRecord.IntentType {
	case ocp_intent.OpenAccounts:
		switch metadata.GetOpenAccounts().AccountSet {
		case ocp_transactionpb.OpenAccountsMetadata_USER:
		case ocp_transactionpb.OpenAccountsMetadata_POOL:
			return ocp_transaction.NewIntentDeniedError("pool account opening is disabled")
		default:
			return ocp_transaction.NewIntentDeniedError("unsupported account set")
		}
		return nil

	case ocp_intent.SendPublicPayment:
		if intentRecord.MintAccount == ocp_common.CoreMintAccount.PublicKey().ToBase58() && !intentRecord.SendPublicPaymentMetadata.IsWithdrawal {
			return ocp_transaction.NewIntentDeniedError("core mint account is restricted to withdrawals and swap fundings")
		}

		return nil

	case ocp_intent.ReceivePaymentsPublicly:
		return nil

	default:
		return ocp_transaction.NewIntentDeniedError("flipcash does not support the intent type")
	}
}

func (i *Integration) OnSuccess(ctx context.Context, intentRecord *ocp_intent.Record) error {
	return nil
}
