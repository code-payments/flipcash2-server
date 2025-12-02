package intent

import (
	"context"

	ocptransactionpb "github.com/code-payments/ocp-protobuf-api/generated/go/transaction/v1"

	ocpdata "github.com/code-payments/ocp-server/ocp/data"
	ocpintent "github.com/code-payments/ocp-server/ocp/data/intent"
	ocptransaction "github.com/code-payments/ocp-server/ocp/rpc/transaction"
)

type Integration struct {
	ocpData ocpdata.Provider
}

func NewIntegration(ocpData ocpdata.Provider) ocptransaction.SubmitIntentIntegration {
	return &Integration{
		ocpData: ocpData,
	}
}

func (i *Integration) AllowCreation(ctx context.Context, intentRecord *ocpintent.Record, metadata *ocptransactionpb.Metadata, actions []*ocptransactionpb.Action) error {
	switch intentRecord.IntentType {
	case ocpintent.OpenAccounts:
		switch metadata.GetOpenAccounts().AccountSet {
		case ocptransactionpb.OpenAccountsMetadata_USER:
		case ocptransactionpb.OpenAccountsMetadata_POOL:
			return ocptransaction.NewIntentDeniedError("pool account opening is disabled")
		default:
			return ocptransaction.NewIntentDeniedError("unsupported account set")
		}
		return nil

	case ocpintent.SendPublicPayment:
		return nil

	case ocpintent.ReceivePaymentsPublicly:
		return nil

	default:
		return ocptransaction.NewIntentDeniedError("flipcash does not support the intent type")
	}
}

func (i *Integration) OnSuccess(ctx context.Context, intentRecord *ocpintent.Record) error {
	return nil
}
