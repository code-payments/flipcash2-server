package intent

import (
	"bytes"
	"context"
	"errors"

	"github.com/google/uuid"
	"google.golang.org/protobuf/proto"

	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"
	intentpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/intent/v1"

	"github.com/code-payments/flipcash2-server/account"
	"github.com/code-payments/flipcash2-server/chat"
	"github.com/code-payments/flipcash2-server/profile"
	ocp_common "github.com/code-payments/ocp-server/ocp/common"
	ocp_intent "github.com/code-payments/ocp-server/ocp/data/intent"
	ocp_task "github.com/code-payments/ocp-server/ocp/data/task"
	ocp_transaction "github.com/code-payments/ocp-server/ocp/rpc/transaction"
)

// TaskTypeSendContactDmPaymentMessage is the task that injects the cash
// message into the sender and recipient's DM after a contact DM payment.
//
// The OCP task system treats the type as an opaque app-owned namespace, so
// values must be globally unique across the app and stable forever — pending
// tasks in the DB reference them. Type 0 is reserved as invalid by the base
// system.
const TaskTypeSendContactDmPaymentMessage uint32 = 1

// NewSendContactDmPaymentMessageTask creates the task that injects the cash
// message into the sender and recipient's DM after a contact DM payment. Only
// the intent ID is carried, via the reference ID; the executor reloads the
// authoritative intent record at execution time.
func NewSendContactDmPaymentMessageTask(intentRecord *ocp_intent.Record) *ocp_task.Record {
	intentID := intentRecord.IntentId
	return &ocp_task.Record{
		TaskId:      uuid.NewString(),
		Type:        TaskTypeSendContactDmPaymentMessage,
		ReferenceId: &intentID,
	}
}

// GetChatMetadata extracts the chat metadata from the intent record's
// additional app metadata, if present. It returns nil when the intent carries
// no app metadata, the metadata fails to decode, or it is not chat metadata.
func GetChatMetadata(intentRecord *ocp_intent.Record) *intentpb.ChatMetadata {
	if len(intentRecord.AppMetadata) == 0 {
		return nil
	}

	var appMetadata intentpb.AppMetadata
	if err := proto.Unmarshal(intentRecord.AppMetadata, &appMetadata); err != nil {
		return nil
	}

	return appMetadata.GetChat()
}

// GetContactDmPayment extracts the contact DM payment from the intent record's
// additional app metadata, if present. It returns nil when the intent carries no
// app metadata, the metadata fails to decode, or it is not a contact DM payment.
func GetContactDmPayment(intentRecord *ocp_intent.Record) *intentpb.ChatMetadata_ContactDmPayment {
	return GetChatMetadata(intentRecord).GetContactDmPayment()
}

// validateContactDmAppMetadata enforces that a SendPublicPayment carrying chat
// app metadata is a well-formed contact DM payment. The metadata later gates
// the task that injects the cash message into the DM (scheduled via
// GetTasksToSchedule, executed by task.Executor), so it must be consistent
// with the payment it accompanies and cannot be trusted from the client alone.
func (i *Integration) validateContactDmAppMetadata(ctx context.Context, intentRecord *ocp_intent.Record, appMetadata *intentpb.AppMetadata) error {
	chatMetadata := appMetadata.GetChat()
	contactPayment := chatMetadata.GetContactDmPayment()
	if contactPayment == nil {
		return ocp_transaction.NewIntentDeniedError("unsupported chat metadata type")
	}

	// A contact DM payment is a direct user-to-user payment. Withdrawals, remote
	// sends, and swap sells are routed elsewhere and never carry this metadata.
	paymentMetadata := intentRecord.SendPublicPaymentMetadata
	if paymentMetadata.IsWithdrawal || paymentMetadata.IsIndirectSend || paymentMetadata.IsSwapSell {
		return ocp_transaction.NewIntentDeniedError("contact dm payment must be a direct payment")
	}

	// Contact DMs are only valid between two Flipcash users, so the payment must
	// resolve to a registered recipient.
	if len(paymentMetadata.DestinationOwnerAccount) == 0 {
		return ocp_transaction.NewIntentDeniedError("contact dm payment recipient is not a flipcash user")
	}

	if contactPayment.GetSource().GetValue() == contactPayment.GetDestination().GetValue() {
		return ocp_transaction.NewIntentDeniedError("payment is a no-op between the same phone number")
	}

	senderOwner, err := ocp_common.NewAccountFromPublicKeyString(intentRecord.InitiatorOwnerAccount)
	if err != nil {
		return errors.New("invalid initiator owner account")
	}
	recipientOwner, err := ocp_common.NewAccountFromPublicKeyString(paymentMetadata.DestinationOwnerAccount)
	if err != nil {
		return errors.New("invalid destination owner account")
	}

	senderUserID, err := i.accounts.GetUserId(ctx, &commonpb.PublicKey{Value: senderOwner.PublicKey().ToBytes()})
	if errors.Is(err, account.ErrNotFound) {
		return ocp_transaction.NewIntentDeniedError("sender is not a flipcash user")
	} else if err != nil {
		return err
	}

	// Validate the sender actually owns the source phone number
	actualSenderUserID, err := i.profiles.GetUserIdByPhoneNumberForPayment(ctx, contactPayment.GetSource().GetValue())
	if errors.Is(err, profile.ErrNotFound) {
		return ocp_transaction.NewIntentDeniedError("source phone number is not linked for payment")
	} else if err != nil {
		return err
	}
	if !bytes.Equal(actualSenderUserID.Value, senderUserID.Value) {
		return ocp_transaction.NewIntentDeniedError("sender is not linked to the source phone number")
	}

	recipientUserID, err := i.accounts.GetUserId(ctx, &commonpb.PublicKey{Value: recipientOwner.PublicKey().ToBytes()})
	if errors.Is(err, account.ErrNotFound) {
		return ocp_transaction.NewIntentDeniedError("recipient is not a flipcash user")
	} else if err != nil {
		return err
	}

	// Validate the recipient actually owns the destination phone number
	actualRecipientUserID, err := i.profiles.GetUserIdByPhoneNumberForPayment(ctx, contactPayment.GetDestination().GetValue())
	if errors.Is(err, profile.ErrNotFound) {
		return ocp_transaction.NewIntentDeniedError("destination phone number is not linked for payment")
	} else if err != nil {
		return err
	}
	if !bytes.Equal(actualRecipientUserID.Value, recipientUserID.Value) {
		return ocp_transaction.NewIntentDeniedError("recipient is not linked to the destination phone number")
	}

	// The chat must be the canonical DM between the sender and recipient.
	expectedChatID := chat.MustDeriveDmChatID(senderUserID, recipientUserID)
	if !bytes.Equal(chatMetadata.GetChatId().GetValue(), expectedChatID.Value) {
		return ocp_transaction.NewIntentValidationError("chat id does not match the dm between sender and recipient")
	}

	return nil
}
