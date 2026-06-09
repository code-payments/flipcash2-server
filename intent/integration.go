package intent

import (
	"context"

	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	intentpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/intent/v1"
	ocp_transactionpb "github.com/code-payments/ocp-protobuf-api/generated/go/transaction/v1"

	"github.com/code-payments/flipcash2-server/account"
	"github.com/code-payments/flipcash2-server/chat"
	"github.com/code-payments/flipcash2-server/contact"
	"github.com/code-payments/flipcash2-server/messaging"
	"github.com/code-payments/flipcash2-server/profile"
	"github.com/code-payments/ocp-server/ocp/data/intent"
	ocp_intent "github.com/code-payments/ocp-server/ocp/data/intent"
	ocp_integration "github.com/code-payments/ocp-server/ocp/integration"
	ocp_transaction "github.com/code-payments/ocp-server/ocp/rpc/transaction"
)

type Integration struct {
	log *zap.Logger

	accounts account.Store
	profiles profile.Store
	contacts contact.Store
	chats    chat.Store

	sender *messaging.Sender

	phoneHashPepper []byte
}

func NewIntegration(
	log *zap.Logger,
	accounts account.Store,
	profiles profile.Store,
	contacts contact.Store,
	chats chat.Store,
	sender *messaging.Sender,
	phoneHashPepper []byte,
) ocp_integration.SubmitIntent {
	return &Integration{
		log:             log,
		accounts:        accounts,
		profiles:        profiles,
		contacts:        contacts,
		chats:           chats,
		sender:          sender,
		phoneHashPepper: phoneHashPepper,
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
			return i.validateContactDmAppMetadata(ctx, intentRecord, &appMetadata)
		default:
			return ocp_transaction.NewIntentDeniedError("flipcash app metadata domain not supported")
		}

	case ocp_intent.ReceivePaymentsPublicly:
		return nil

	default:
		return ocp_transaction.NewIntentDeniedError("flipcash does not support the intent type")
	}
}

func (i *Integration) OnSuccess(ctx context.Context, intentRecord *ocp_intent.Record) error {
	i.maybeSendContactPaymentMessage(ctx, intentRecord)

	return nil
}
