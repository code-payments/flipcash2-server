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
	"github.com/code-payments/flipcash2-server/messaging"
	"github.com/code-payments/flipcash2-server/paymentrequest"
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
	messages messaging.Store
	profiles profile.Store

	// requests holds payment requests, read to validate a requested payment
	// against the share it pays (see validateRequestedPaymentAppMetadata).
	requests paymentrequest.Store

	// access gates group payments on the payer's standing to speak and the
	// recipient's membership (see validateGroupAppMetadata).
	access *chat.Access

	mintDataProvider *ocp_currency_util.MintDataProvider

	// teamUserID is the Flipcash team account (see flipcashteam), nil when
	// there is none. No DM payment is allowed to or from it (see
	// validateDmAppMetadata).
	teamUserID *commonpb.UserId
}

// NewIntegration constructs the intent integration. messages is the
// messaging store, read to validate reaction boosts, and requests the payment
// request store, read to validate a payment of a share. access is the one
// chat.Access the parent builds and shares with the chat and messaging
// servers. teamUserID is the Flipcash team account, which the parent resolves
// once at startup with flipcashteam.GetUserID and passes to the chat store,
// the chat RuleEvaluator, the chat server and the Sender too; nil names no
// one.
func NewIntegration(
	accounts account.Store,
	chats chat.Store,
	messages messaging.Store,
	profiles profile.Store,
	requests paymentrequest.Store,
	access *chat.Access,
	mintDataProvider *ocp_currency_util.MintDataProvider,
	teamUserID *commonpb.UserId,
) ocp_integration.SubmitIntent {
	return &Integration{
		accounts: accounts,
		chats:    chats,
		profiles: profiles,

		messages: messages,
		requests: requests,

		access: access,

		mintDataProvider: mintDataProvider,

		teamUserID: teamUserID,
	}
}

func (i *Integration) AllowCreation(ctx context.Context, intentRecord *ocp_intent.Record, metadata *ocp_transactionpb.Metadata, actions []*ocp_transactionpb.Action) error {
	hasAppMetadata := len(intentRecord.AppMetadata) > 0

	// A SendPublicPayment carries any chat payment; a SendPublicMultiPayment
	// carries a reaction boost or a group payment alone (see below), never a
	// requested payment, which pays one person.
	if hasAppMetadata && intentRecord.IntentType != intent.SendPublicPayment && intentRecord.IntentType != intent.SendPublicMultiPayment {
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
			case *intentpb.ChatMetadata_ReactionBoost_:
				return i.validateReactionBoostAppMetadata(ctx, intentRecord, &appMetadata)
			case *intentpb.ChatMetadata_RequestedPayment_:
				return i.validateRequestedPaymentAppMetadata(ctx, intentRecord, &appMetadata)
			default:
				return ocp_transaction.NewIntentDeniedError("unsupported chat metadata type")
			}
		default:
			return ocp_transaction.NewIntentDeniedError("flipcash app metadata domain not supported")
		}

	case ocp_intent.ReceivePaymentsPublicly:
		return nil

	case ocp_intent.SendPublicMultiPayment:
		// A multi payment is made for one of two things: a reaction boost
		// that pays both the message's sender and the group's creator, open
		// to everyone, or a group payment to several members at once, which
		// is behind its staff gate. One carrying any other app metadata (a
		// requested payment, which pays its request's creator alone,
		// included), or none, is refused before anything is read.
		switch appMetadata.GetChat().GetType().(type) {
		case *intentpb.ChatMetadata_ReactionBoost_:
			return i.validateReactionBoostAppMetadata(ctx, intentRecord, &appMetadata)
		case *intentpb.ChatMetadata_GroupPayment_:
			if err := i.validateStaffOnlyMultiPayment(ctx, intentRecord); err != nil {
				return err
			}
			return i.validateGroupAppMetadata(ctx, intentRecord, &appMetadata)
		default:
			return ocp_transaction.NewIntentDeniedError("multi payments are only supported for reaction boosts and group payments")
		}

	default:
		return ocp_transaction.NewIntentDeniedError("flipcash does not support the intent type")
	}
}

// GetTasksToSchedule returns the guaranteed work derived from a submitted
// intent. A DM payment of either type (contact DM or DM) schedules the cash
// message injected into the DM between the sender and recipient, and a group
// payment, made as either payment intent, the one injected into the group; a
// reaction boost, made as either payment intent, schedules the task that
// records the boost; and a requested payment, a SendPublicPayment alone, the
// task that marks its share paid and posts its cash message. Execution is
// handled by task.Executor.
func (i *Integration) GetTasksToSchedule(ctx context.Context, intentRecord *ocp_intent.Record) ([]*ocp_task.Record, error) {
	if intentRecord.IntentType == ocp_intent.SendPublicMultiPayment {
		if GetReactionBoost(intentRecord.AppMetadata) != nil {
			return []*ocp_task.Record{NewRecordReactionBoostTask(intentRecord)}, nil
		}
		if GetGroupPayment(intentRecord.AppMetadata) != nil {
			return []*ocp_task.Record{NewSendGroupPaymentMessageTask(intentRecord)}, nil
		}
	}

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
		if GetReactionBoost(intentRecord.AppMetadata) != nil {
			return []*ocp_task.Record{NewRecordReactionBoostTask(intentRecord)}, nil
		}
		if GetRequestedPayment(intentRecord.AppMetadata) != nil {
			return []*ocp_task.Record{NewRecordRequestedPaymentTask(intentRecord)}, nil
		}
	}

	return nil, nil
}

func (i *Integration) OnSuccess(ctx context.Context, intentRecord *ocp_intent.Record) error {
	return nil
}

// validateStaffOnlyMultiPayment allows a multi payment only from a staff
// user. It is a transitional gate while group payments to several members
// roll out, so opening them to everyone is removing this check. It gates
// group payments alone: a reaction boost that pays two people is open to
// everyone.
func (i *Integration) validateStaffOnlyMultiPayment(ctx context.Context, intentRecord *ocp_intent.Record) error {
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
