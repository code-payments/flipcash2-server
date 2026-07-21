package intent

import (
	"bytes"
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"google.golang.org/protobuf/proto"

	chatpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/chat/v1"
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

// The OCP task system treats the type as an opaque app-owned namespace, so
// values must be globally unique across the app and stable forever — pending
// tasks in the DB reference them. Type 0 is reserved as invalid by the base
// system.
const (
	// TaskTypeSendContactDmPaymentMessage is the task that injects the cash
	// message into the sender and recipient's DM after a contact DM payment.
	TaskTypeSendContactDmPaymentMessage uint32 = 1

	// TaskTypeSendTipDmPaymentMessage is the task that injects the cash message
	// into the sender and recipient's tip DM after a tip DM payment.
	TaskTypeSendTipDmPaymentMessage uint32 = 2
)

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

// NewSendTipDmPaymentMessageTask creates the task that injects the cash
// message into the sender and recipient's tip DM after a tip DM payment. Only
// the intent ID is carried, via the reference ID; the executor reloads the
// authoritative intent record at execution time.
func NewSendTipDmPaymentMessageTask(intentRecord *ocp_intent.Record) *ocp_task.Record {
	intentID := intentRecord.IntentId
	return &ocp_task.Record{
		TaskId:      uuid.NewString(),
		Type:        TaskTypeSendTipDmPaymentMessage,
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

// GetTipDmPayment extracts the tip DM payment from the intent record's
// additional app metadata, if present. It returns nil when the intent carries no
// app metadata, the metadata fails to decode, or it is not a tip DM payment.
func GetTipDmPayment(intentRecord *ocp_intent.Record) *intentpb.ChatMetadata_TipDmPayment {
	return GetChatMetadata(intentRecord).GetTipDmPayment()
}

// resolveDirectDmPaymentParties enforces the checks shared by every DM payment
// type — the payment must be a direct user-to-user payment between two Flipcash
// users — and resolves the sender and recipient user IDs. kind names the DM
// type in denial messages.
func (i *Integration) resolveDirectDmPaymentParties(ctx context.Context, intentRecord *ocp_intent.Record, kind string) (senderUserID, recipientUserID *commonpb.UserId, err error) {
	// A DM payment is a direct user-to-user payment. Withdrawals, remote sends,
	// and swap sells are routed elsewhere and never carry chat metadata.
	paymentMetadata := intentRecord.SendPublicPaymentMetadata
	if paymentMetadata.IsWithdrawal || paymentMetadata.IsIndirectSend || paymentMetadata.IsSwapSell {
		return nil, nil, ocp_transaction.NewIntentDeniedError(fmt.Sprintf("%s payment must be a direct payment", kind))
	}

	// DMs are only valid between two Flipcash users, so the payment must resolve
	// to a registered recipient.
	if len(paymentMetadata.DestinationOwnerAccount) == 0 {
		return nil, nil, ocp_transaction.NewIntentDeniedError(fmt.Sprintf("%s payment recipient is not a flipcash user", kind))
	}

	senderOwner, err := ocp_common.NewAccountFromPublicKeyString(intentRecord.InitiatorOwnerAccount)
	if err != nil {
		return nil, nil, errors.New("invalid initiator owner account")
	}
	recipientOwner, err := ocp_common.NewAccountFromPublicKeyString(paymentMetadata.DestinationOwnerAccount)
	if err != nil {
		return nil, nil, errors.New("invalid destination owner account")
	}

	senderUserID, err = i.accounts.GetUserId(ctx, &commonpb.PublicKey{Value: senderOwner.PublicKey().ToBytes()})
	if errors.Is(err, account.ErrNotFound) {
		return nil, nil, ocp_transaction.NewIntentDeniedError("sender is not a flipcash user")
	} else if err != nil {
		return nil, nil, err
	}

	recipientUserID, err = i.accounts.GetUserId(ctx, &commonpb.PublicKey{Value: recipientOwner.PublicKey().ToBytes()})
	if errors.Is(err, account.ErrNotFound) {
		return nil, nil, ocp_transaction.NewIntentDeniedError("recipient is not a flipcash user")
	} else if err != nil {
		return nil, nil, err
	}

	return senderUserID, recipientUserID, nil
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

	if contactPayment.GetSource().GetValue() == contactPayment.GetDestination().GetValue() {
		return ocp_transaction.NewIntentDeniedError("payment is a no-op between the same phone number")
	}

	senderUserID, recipientUserID, err := i.resolveDirectDmPaymentParties(ctx, intentRecord, "contact dm")
	if err != nil {
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
	expectedChatID := chat.MustDeriveDmChatID(chatpb.ChatType_CONTACT_DM, senderUserID, recipientUserID)
	if !bytes.Equal(chatMetadata.GetChatId().GetValue(), expectedChatID.Value) {
		return ocp_transaction.NewIntentValidationError("chat id does not match the dm between sender and recipient")
	}

	return nil
}

// validateTipDmAppMetadata enforces that a SendPublicPayment carrying chat app
// metadata is a well-formed tip DM payment. Unlike contact DMs, tip DMs are
// keyed on user IDs alone — neither party is required to have a phone number
// linked for payment, since a tip can come from a stranger who only has the
// recipient's tip card.
func (i *Integration) validateTipDmAppMetadata(ctx context.Context, intentRecord *ocp_intent.Record, appMetadata *intentpb.AppMetadata) error {
	chatMetadata := appMetadata.GetChat()
	if chatMetadata.GetTipDmPayment() == nil {
		return ocp_transaction.NewIntentDeniedError("unsupported chat metadata type")
	}

	if err := validateMinimumTipAmount(intentRecord.SendPublicPaymentMetadata); err != nil {
		return err
	}

	senderUserID, recipientUserID, err := i.resolveDirectDmPaymentParties(ctx, intentRecord, "tip dm")
	if err != nil {
		return err
	}

	if bytes.Equal(senderUserID.Value, recipientUserID.Value) {
		return ocp_transaction.NewIntentDeniedError("payment is a no-op tip to yourself")
	}

	// The chat must be the canonical tip DM between the sender and recipient.
	expectedChatID := chat.MustDeriveDmChatID(chatpb.ChatType_TIP_DM, senderUserID, recipientUserID)
	if !bytes.Equal(chatMetadata.GetChatId().GetValue(), expectedChatID.Value) {
		return ocp_transaction.NewIntentValidationError("chat id does not match the tip dm between sender and recipient")
	}

	return nil
}
