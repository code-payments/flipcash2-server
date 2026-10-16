package intent

import (
	"context"
	"errors"

	"google.golang.org/protobuf/proto"

	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"
	intentpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/intent/v1"
	ocp_transactionpb "github.com/code-payments/ocp-protobuf-api/generated/go/transaction/v1"

	"github.com/code-payments/flipcash2-server/account"
	"github.com/code-payments/flipcash2-server/chat"
	"github.com/code-payments/flipcash2-server/profile"
	ocp_common "github.com/code-payments/ocp-server/ocp/common"
	ocp_currency_util "github.com/code-payments/ocp-server/ocp/currency"
	"github.com/code-payments/ocp-server/ocp/data/intent"
	ocp_intent "github.com/code-payments/ocp-server/ocp/data/intent"
	ocp_task "github.com/code-payments/ocp-server/ocp/data/task"
	ocp_integration "github.com/code-payments/ocp-server/ocp/integration"
	ocp_transaction "github.com/code-payments/ocp-server/ocp/rpc/transaction"
)

type Integration struct {
	accounts account.Store
	chats    chat.Store
	profiles profile.Store

	// access gates group payments on the payer's standing to speak and the
	// recipient's membership (see validateGroupAppMetadata).
	access *chat.Access

	mintDataProvider *ocp_currency_util.MintDataProvider

	// teamUserID is the Flipcash team account (see flipcashteam), nil when
	// there is none. No DM payment is allowed to or from it (see
	// validateDmAppMetadata).
	teamUserID *commonpb.UserId
}

// NewIntegration constructs the intent integration. access is the one
// chat.Access the parent builds and shares with the chat and messaging
// servers. teamUserID is the Flipcash team account, which the parent resolves
// once at startup with flipcashteam.GetUserID and passes to the chat store,
// the chat RuleEvaluator, the chat server and the Sender too; nil names no
// one.
func NewIntegration(
	accounts account.Store,
	chats chat.Store,
	profiles profile.Store,
	access *chat.Access,
	mintDataProvider *ocp_currency_util.MintDataProvider,
	teamUserID *commonpb.UserId,
) ocp_integration.SubmitIntent {
	return &Integration{
		accounts: accounts,
		chats:    chats,
		profiles: profiles,

		access: access,

		mintDataProvider: mintDataProvider,

		teamUserID: teamUserID,
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
				// return i.validateContactDmAppMetadata(ctx, intentRecord, &appMetadata)
				return ocp_transaction.NewIntentDeniedError("contact send feature is disabled")
			case *intentpb.ChatMetadata_DmPayment_:
				return i.validateDmAppMetadata(ctx, intentRecord, &appMetadata)
			case *intentpb.ChatMetadata_GroupPayment_:
				return i.validateGroupAppMetadata(ctx, intentRecord, &appMetadata)
			default:
				return ocp_transaction.NewIntentDeniedError("unsupported chat metadata type")
			}
		default:
			return ocp_transaction.NewIntentDeniedError("flipcash app metadata domain not supported")
		}

	case ocp_intent.ReceivePaymentsPublicly:
		return nil

	case ocp_intent.SendPublicMultiPayment:
		// App metadata was refused above, since only a SendPublicPayment
		// carries it
		return i.validateMultiPayment(ctx, intentRecord)

	default:
		return ocp_transaction.NewIntentDeniedError("flipcash does not support the intent type")
	}
}

// GetTasksToSchedule returns the guaranteed work derived from a submitted
// intent. A DM payment of either type (contact DM or DM) schedules the cash
// message injected into the DM between the sender and recipient, and a group
// payment the one injected into the group; execution is handled by
// task.Executor.
func (i *Integration) GetTasksToSchedule(ctx context.Context, intentRecord *ocp_intent.Record) ([]*ocp_task.Record, error) {
	if intentRecord.IntentType == ocp_intent.SendPublicPayment {
		if GetContactDmPayment(intentRecord.AppMetadata) != nil {
			return []*ocp_task.Record{NewSendContactDmPaymentMessageTask(intentRecord)}, nil
		}
		if GetDmPayment(intentRecord.AppMetadata) != nil {
			return []*ocp_task.Record{NewSendDmPaymentMessageTask(intentRecord)}, nil
		}
		if GetGroupPayment(intentRecord.AppMetadata) != nil {
			return []*ocp_task.Record{NewSendGroupPaymentMessageTask(intentRecord)}, nil
		}
	}

	return nil, nil
}

func (i *Integration) OnSuccess(ctx context.Context, intentRecord *ocp_intent.Record) error {
	return nil
}

// validateMultiPayment allows a multi payment only from a staff user. It is a
// transitional gate while multi payments roll out, so opening them to
// everyone is removing this check.
func (i *Integration) validateMultiPayment(ctx context.Context, intentRecord *ocp_intent.Record) error {
	payer, err := ocp_common.NewAccountFromPublicKeyString(intentRecord.InitiatorOwnerAccount)
	if err != nil {
		return err
	}

	payerUserID, err := i.accounts.GetUserId(ctx, &commonpb.PublicKey{Value: payer.PublicKey().ToBytes()})
	if errors.Is(err, account.ErrNotFound) {
		return ocp_transaction.NewIntentDeniedError("payer is not a flipcash user")
	} else if err != nil {
		return err
	}

	isStaff, err := i.accounts.IsStaff(ctx, payerUserID)
	if err != nil {
		return err
	}
	if !isStaff {
		return ocp_transaction.NewIntentDeniedError("multi payments are limited to staff")
	}

	return nil
}
