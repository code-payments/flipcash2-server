package intent

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"

	"github.com/google/uuid"
	"google.golang.org/protobuf/proto"

	chatpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/chat/v1"
	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"
	intentpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/intent/v1"
	messagingpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/messaging/v1"

	"github.com/code-payments/flipcash2-server/account"
	"github.com/code-payments/flipcash2-server/chat"
	"github.com/code-payments/flipcash2-server/profile"
	"github.com/code-payments/flipcash2-server/tip"
	currency_lib "github.com/code-payments/ocp-server/currency"
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

// GetChatMetadata extracts the chat metadata from an intent's additional app
// metadata, if present. It returns nil when there is no app metadata, the
// metadata fails to decode, or it is not chat metadata.
//
// This and the accessors below take the blob rather than the intent it came
// from, because the intent is not the only thing that carries it: a transaction
// history record passes the same bytes through unmodified, and decodes to the
// same metadata the intent was submitted with.
func GetChatMetadata(appMetadata []byte) *intentpb.ChatMetadata {
	if len(appMetadata) == 0 {
		return nil
	}

	var decoded intentpb.AppMetadata
	if err := proto.Unmarshal(appMetadata, &decoded); err != nil {
		return nil
	}

	return decoded.GetChat()
}

// GetContactDmPayment extracts the contact DM payment from app metadata, if
// present. It returns nil when there is no app metadata, the metadata fails to
// decode, or it is not a contact DM payment.
func GetContactDmPayment(appMetadata []byte) *intentpb.ChatMetadata_ContactDmPayment {
	return GetChatMetadata(appMetadata).GetContactDmPayment()
}

// GetTipDmPayment extracts the tip DM payment from app metadata, if present. It
// returns nil when there is no app metadata, the metadata fails to decode, or it
// is not a tip DM payment.
func GetTipDmPayment(appMetadata []byte) *intentpb.ChatMetadata_TipDmPayment {
	return GetChatMetadata(appMetadata).GetTipDmPayment()
}

// GetDmPaymentVerb reports how a DM payment should be rendered. A tip DM
// payment is only a tip when it was sent from the recipient's tip card — the
// same DM also carries ordinary payments sent from within the chat itself.
// Everything else, contact DM payments included, is a plain send.
//
// Both the cash message injected into the DM and the sender's activity feed
// entry render off this, so they cannot disagree about what a payment was.
func GetDmPaymentVerb(appMetadata []byte) messagingpb.CashContent_Verb {
	tipDmPayment := GetTipDmPayment(appMetadata)
	if tipDmPayment == nil {
		return messagingpb.CashContent_SENT
	}

	// TIPCARD is the zero value, so a tip DM payment that leaves the location
	// unset is treated as having come from the tip card.
	if tipDmPayment.GetLocation() == intentpb.ChatMetadata_TipDmPayment_TIPCARD {
		return messagingpb.CashContent_TIPPED
	}
	return messagingpb.CashContent_SENT
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

// validateMinimumTipAmount enforces the minimum tip amount for the payment's
// exchange currency. Clients surface the minimum as the first tip preset, but
// the amount is ultimately client-chosen, so the floor is enforced here too.
// Currencies without a preset fall back to a USD floor applied to the payment's
// USD market value, so no currency is left without a minimum.
func validateMinimumTipAmount(paymentMetadata *ocp_intent.SendPublicPaymentMetadata) error {
	currencyCode := paymentMetadata.ExchangeCurrency
	amount := paymentMetadata.NativeAmount
	presets, ok := tip.PresetsFor(currencyCode)
	minimum := presets.Minimum
	if !ok {
		// USD always has presets; tip's tests pin the row the fallback reads.
		usdPresets, _ := tip.PresetsFor(currency_lib.USD)
		currencyCode, amount, minimum = currency_lib.USD, paymentMetadata.UsdMarketValue, usdPresets.Minimum
	}

	// The amount reaching us is a fiat value derived from a quoted exchange
	// rate, so it can land a fraction of a minor unit under the advertised
	// minimum through rounding alone. Allow half of the currency's smallest
	// transferable unit of slack so those tips aren't denied.
	tolerance := 0.5 * math.Pow10(-currency_lib.GetDecimals(currencyCode))

	if amount < minimum-tolerance {
		return ocp_transaction.NewIntentDeniedError(fmt.Sprintf(
			"tip amount is below the minimum of %s %s",
			strconv.FormatFloat(minimum, 'f', -1, 64),
			strings.ToUpper(string(currencyCode)),
		))
	}

	return nil
}
