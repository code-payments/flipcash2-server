package intent

import (
	"context"

	"google.golang.org/protobuf/proto"

	intentpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/intent/v1"
	ocp_transactionpb "github.com/code-payments/ocp-protobuf-api/generated/go/transaction/v1"

	"github.com/code-payments/flipcash2-server/account"
	"github.com/code-payments/flipcash2-server/profile"
	"github.com/code-payments/ocp-server/ocp/data/intent"
	ocp_intent "github.com/code-payments/ocp-server/ocp/data/intent"
	ocp_task "github.com/code-payments/ocp-server/ocp/data/task"
	ocp_integration "github.com/code-payments/ocp-server/ocp/integration"
	ocp_transaction "github.com/code-payments/ocp-server/ocp/rpc/transaction"
)

type Integration struct {
	accounts account.Store
	profiles profile.Store
}

func NewIntegration(
	accounts account.Store,
	profiles profile.Store,
) ocp_integration.SubmitIntent {
	return &Integration{
		accounts: accounts,
		profiles: profiles,
	}
}

func (i *Integration) AllowCreation(ctx context.Context, intentRecord *ocp_intent.Record, metadata *ocp_transactionpb.Metadata, actions []*ocp_transactionpb.Action) error {
	hasAppMetadata := len(intentRecord.AppMetadata) > 0

	if hasAppMetadata && intentRecord.IntentType != intent.SendPublicPayment {
		return ocp_transaction.NewIntentDeniedError("flipcash app metadata only supported for public payment intents")
	}

	var appMetadata intentpb.AppMetadata
	if hasAppMetadata {
		err := proto.Unmarshal(intentRecord.AppMetadata, &appMetadata)
		if err != nil {
			return ocp_transaction.NewIntentValidationError("invalid flipcash app metadata")
		}

		if err := appMetadata.Validate(); err != nil {
			return ocp_transaction.NewIntentValidationError("invalid flipcash app metadata")
		}
	}

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
		if !hasAppMetadata {
			return nil
		}

		switch appMetadata.Domain.(type) {
		case *intentpb.AppMetadata_Chat:
			switch appMetadata.GetChat().GetType().(type) {
			case *intentpb.ChatMetadata_ContactDmPayment_:
				return i.validateContactDmAppMetadata(ctx, intentRecord, &appMetadata)
			case *intentpb.ChatMetadata_TipDmPayment_:
				return i.validateTipDmAppMetadata(ctx, intentRecord, &appMetadata)
			default:
				return ocp_transaction.NewIntentDeniedError("unsupported chat metadata type")
			}
		default:
			return ocp_transaction.NewIntentDeniedError("flipcash app metadata domain not supported")
		}

	case ocp_intent.ReceivePaymentsPublicly:
		return nil

	default:
		return ocp_transaction.NewIntentDeniedError("flipcash does not support the intent type")
	}
}

// GetTasksToSchedule returns the guaranteed work derived from a submitted
// intent. A DM payment (contact or tip) schedules the cash message injected
// into the DM between the sender and recipient; execution is handled by
// task.Executor.
func (i *Integration) GetTasksToSchedule(ctx context.Context, intentRecord *ocp_intent.Record) ([]*ocp_task.Record, error) {
	if intentRecord.IntentType == ocp_intent.SendPublicPayment {
		if GetContactDmPayment(intentRecord) != nil {
			return []*ocp_task.Record{NewSendContactDmPaymentMessageTask(intentRecord)}, nil
		}
		if GetTipDmPayment(intentRecord) != nil {
			return []*ocp_task.Record{NewSendTipDmPaymentMessageTask(intentRecord)}, nil
		}
	}

	return nil, nil
}

func (i *Integration) OnSuccess(ctx context.Context, intentRecord *ocp_intent.Record) error {
	return nil
}
