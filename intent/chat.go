package intent

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"google.golang.org/protobuf/proto"

	chatpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/chat/v1"
	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"
	intentpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/intent/v1"
	messagingpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/messaging/v1"

	"github.com/code-payments/flipcash2-server/account"
	"github.com/code-payments/flipcash2-server/chat"
	"github.com/code-payments/flipcash2-server/messaging"
	"github.com/code-payments/flipcash2-server/profile"
	"github.com/code-payments/flipcash2-server/send"
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

	// TaskTypeSendDmPaymentMessage is the task that injects the cash message
	// into the sender and recipient's DM after a DM payment.
	TaskTypeSendDmPaymentMessage uint32 = 2

	// TaskTypeSendGroupPaymentMessage is the task that injects the cash
	// message into the group after a group payment.
	TaskTypeSendGroupPaymentMessage uint32 = 3

	// TaskTypeRecordReactionBoost is the task that records a reaction boost
	// against the payer's reaction once its payment is made (see
	// messaging.Sender.RecordBoost).
	TaskTypeRecordReactionBoost uint32 = 4
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

// NewSendDmPaymentMessageTask creates the task that injects the cash
// message into the sender and recipient's DM after a DM payment. Only
// the intent ID is carried, via the reference ID; the executor reloads the
// authoritative intent record at execution time.
func NewSendDmPaymentMessageTask(intentRecord *ocp_intent.Record) *ocp_task.Record {
	intentID := intentRecord.IntentId
	return &ocp_task.Record{
		TaskId:      uuid.NewString(),
		Type:        TaskTypeSendDmPaymentMessage,
		ReferenceId: &intentID,
	}
}

// NewSendGroupPaymentMessageTask creates the task that injects the cash
// message into the group after a group payment. Only the intent ID is
// carried, via the reference ID; the executor reloads the authoritative intent
// record at execution time.
func NewSendGroupPaymentMessageTask(intentRecord *ocp_intent.Record) *ocp_task.Record {
	intentID := intentRecord.IntentId
	return &ocp_task.Record{
		TaskId:      uuid.NewString(),
		Type:        TaskTypeSendGroupPaymentMessage,
		ReferenceId: &intentID,
	}
}

// NewRecordReactionBoostTask creates the task that records a reaction boost
// after its payment. Only the intent ID is carried, via the reference ID; the
// executor reloads the authoritative intent record at execution time.
func NewRecordReactionBoostTask(intentRecord *ocp_intent.Record) *ocp_task.Record {
	intentID := intentRecord.IntentId
	return &ocp_task.Record{
		TaskId:      uuid.NewString(),
		Type:        TaskTypeRecordReactionBoost,
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

// GetDmPayment extracts the DM payment from app metadata, if present. It
// returns nil when there is no app metadata, the metadata fails to decode, or it
// is not a DM payment.
func GetDmPayment(appMetadata []byte) *intentpb.ChatMetadata_DmPayment {
	return GetChatMetadata(appMetadata).GetDmPayment()
}

// GetGroupPayment extracts the group payment from app metadata, if present. It
// returns nil when there is no app metadata, the metadata fails to decode, or
// it is not a group payment.
func GetGroupPayment(appMetadata []byte) *intentpb.ChatMetadata_GroupPayment {
	return GetChatMetadata(appMetadata).GetGroupPayment()
}

// GetReactionBoost extracts the reaction boost from app metadata, if present.
// It returns nil when there is no app metadata, the metadata fails to decode,
// or it is not a reaction boost.
func GetReactionBoost(appMetadata []byte) *intentpb.ChatMetadata_ReactionBoost {
	return GetChatMetadata(appMetadata).GetReactionBoost()
}

// IsUserChatPayment reports whether app metadata is a chat payment whose
// parties know each other by user ID: a DM payment, or a group payment, whose
// cash message names both in the group. A contact DM payment is not one, since
// its parties know each other by phone number; nor is a payment carrying no
// chat metadata, such as one handed off in person, where no identity was
// exchanged at all.
func IsUserChatPayment(appMetadata []byte) bool {
	chatMetadata := GetChatMetadata(appMetadata)
	return chatMetadata.GetDmPayment() != nil || chatMetadata.GetGroupPayment() != nil
}

// sendByDefaultCutoff is when an unset DM payment action came to mean SEND.
// A DM payment created at or after it is a tip only when its action says TIP,
// whatever its location. One created before it keeps the rule it was made
// under: an unset action defaults by location, a tip from the Flipcard (the
// zero value, so also an unset location) and a send from within the chat. The
// cash message injected into the DM was rendered once under the old rule, and
// the activity feed renders on every read, so the feed keeps the old rule for
// old payments to agree with the DM.
var sendByDefaultCutoff = time.Date(2026, time.September, 30, 0, 0, 0, 0, time.UTC)

// GetChatPaymentVerb reports how a chat payment created at createdAt should be
// rendered. A DM payment is a tip only when its action is TIP; SEND and the
// unset DEFAULT are plain sends, from either location, except for payments
// created before sendByDefaultCutoff. Everything else, contact DM and group
// payments included, is a plain send: a group payment's TIP is denied at
// validation (see validateGroupAppMetadata).
//
// Both the cash message injected into the chat and the sender's activity feed
// entry render off this, so they cannot disagree about what a payment was.
// createdAt is the intent's creation time, which OCP sets before validation
// and copies onto the intent's history records, so every caller passes the
// same time for the same payment.
func GetChatPaymentVerb(appMetadata []byte, createdAt time.Time) messagingpb.CashContent_Verb {
	dmPayment := GetDmPayment(appMetadata)
	if dmPayment == nil {
		return messagingpb.CashContent_SENT
	}

	switch dmPayment.GetAction() {
	case intentpb.PaymentAction_SEND:
		return messagingpb.CashContent_SENT
	case intentpb.PaymentAction_TIP:
		return messagingpb.CashContent_TIPPED
	}

	if createdAt.Before(sendByDefaultCutoff) && dmPayment.GetLocation() == intentpb.ChatMetadata_DmPayment_FLIPCARD {
		return messagingpb.CashContent_TIPPED
	}
	return messagingpb.CashContent_SENT
}

// resolveDirectChatPaymentParties enforces the checks shared by every chat
// payment type — the payment must be a direct user-to-user payment between two
// Flipcash users — and resolves the sender and recipient user IDs. kind names
// the payment type in denial messages.
func (i *Integration) resolveDirectChatPaymentParties(ctx context.Context, intentRecord *ocp_intent.Record, kind string) (senderUserID, recipientUserID *commonpb.UserId, err error) {
	// A chat payment is a direct user-to-user payment. Withdrawals, remote sends,
	// and swap sells are routed elsewhere and never carry chat metadata.
	paymentMetadata := intentRecord.SendPublicPaymentMetadata
	if paymentMetadata.IsWithdrawal || paymentMetadata.IsIndirectSend || paymentMetadata.IsSwapSell {
		return nil, nil, ocp_transaction.NewIntentDeniedError(fmt.Sprintf("%s payment must be a direct payment", kind))
	}

	// Chat payments are only valid between two Flipcash users, so the payment
	// must resolve to a registered recipient.
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

	senderUserID, recipientUserID, err := i.resolveDirectChatPaymentParties(ctx, intentRecord, "contact dm")
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

// validateDmAppMetadata enforces that a SendPublicPayment carrying chat app
// metadata is a well-formed DM payment. Unlike contact DMs, these DMs are
// keyed on user IDs alone — neither party is required to have a phone number
// linked for payment, since a payment can come from a stranger who only has
// the recipient's Flipcard.
//
// Any payment, tip or send, initializes the DM, so it may target a chat that
// doesn't exist yet; whether it is a tip only changes how it is rendered (see
// GetChatPaymentVerb), never which rules apply. Only the payment that
// initializes the chat is held to a floor: the recipient's minimum DM chat
// initialization fee where they have set one, the per-currency preset minimum
// otherwise. The fee is what the recipient asks of anyone reaching them for
// the first time, and it can be no lower than its own currency's preset
// minimum (profile.ValidateMinDmChatInitFee), so it stands in for the preset
// rather than stacking on it. Once the chat exists the gate has been paid, and
// every later payment into it may be any amount. No payment either way between
// a user and the Flipcash team account is allowed, tip or send.
func (i *Integration) validateDmAppMetadata(ctx context.Context, intentRecord *ocp_intent.Record, appMetadata *intentpb.AppMetadata) error {
	chatMetadata := appMetadata.GetChat()
	dmPayment := chatMetadata.GetDmPayment()
	if dmPayment == nil {
		return ocp_transaction.NewIntentDeniedError("unsupported chat metadata type")
	}

	senderUserID, recipientUserID, err := i.resolveDirectChatPaymentParties(ctx, intentRecord, "dm")
	if err != nil {
		return err
	}

	if bytes.Equal(senderUserID.Value, recipientUserID.Value) {
		return ocp_transaction.NewIntentDeniedError("payment to yourself is a no-op")
	}

	// Nobody messages the Flipcash team, and a DM payment is a message: the
	// task it schedules puts a cash message in the DM, which the DM's Never
	// speaker rule does not stop (see chat.RuleEvaluator), since the task
	// sends through the messaging Sender, which no rule gates. Refused here,
	// before any money moves, rather than by the task, which could only drop
	// the message of a payment already made. The team sends nothing this way
	// either.
	if i.isTeamAccount(senderUserID) || i.isTeamAccount(recipientUserID) {
		return ocp_transaction.NewIntentDeniedError("dm payments with the flipcash team are not allowed")
	}

	// The chat must be the canonical DM between the sender and recipient.
	expectedChatID := chat.MustDeriveDmChatID(chatpb.ChatType_DM, senderUserID, recipientUserID)
	if !bytes.Equal(chatMetadata.GetChatId().GetValue(), expectedChatID.Value) {
		return ocp_transaction.NewIntentValidationError("chat id does not match the dm between sender and recipient")
	}

	_, err = i.chats.GetChatByID(ctx, expectedChatID)
	switch {
	case err == nil:
		// An initialized chat has already had its gate paid: every later
		// payment is any amount.
		return nil
	case errors.Is(err, chat.ErrChatNotFound):
	default:
		return err
	}

	// This payment is what initializes the chat, so it must clear whatever the
	// recipient asks of anyone reaching them for the first time: their fee
	// where they have set one, the preset minimum otherwise.
	var fee *commonpb.FiatPaymentAmount
	recipientProfile, err := i.profiles.GetFullProfile(ctx, recipientUserID, false)
	switch {
	case err == nil:
		fee = recipientProfile.MinDmChatInitFee
	case errors.Is(err, profile.ErrNotFound):
		// A user the profile store doesn't know has set nothing.
	default:
		return err
	}

	if fee == nil {
		return validateDefaultMinimumToChat(intentRecord.SendPublicPaymentMetadata)
	}
	return i.validateMinDmChatInitFee(ctx, intentRecord.SendPublicPaymentMetadata, fee)
}

// validateGroupAppMetadata enforces that a SendPublicPayment carrying chat app
// metadata is a well-formed group payment: a direct payment from one member of
// a group to another. The task it schedules posts the cash message into the
// group as the payer, through the messaging Sender, which no rule gates, so
// every gate a send would face is applied here, before any money moves, rather
// than by the task, which could only drop the message of a payment already
// made.
//
// The payer must be able to speak in the group (chat.Access.SpeakerStanding:
// a member who satisfies its rules, so not a non-creator in a creator-only
// group, nor anyone in a private group without its key), and the recipient
// must be a member, whatever the rules say of them, since receiving is not
// speaking. A group that does not exist has no members, so it fails the
// payer's check. Standing and membership are judged now, not when the task
// runs: a payer who leaves, or a recipient who does, before the message lands
// still gets it, since it records a payment that has happened.
//
// There is no floor on the amount: the payer can already speak in the group
// for free, so a payment is no wider a channel than a message. Only a send is
// supported today, so TIP is denied. No payment either way between a user and
// the Flipcash team account is allowed, as for DMs.
func (i *Integration) validateGroupAppMetadata(ctx context.Context, intentRecord *ocp_intent.Record, appMetadata *intentpb.AppMetadata) error {
	chatMetadata := appMetadata.GetChat()
	groupPayment := chatMetadata.GetGroupPayment()
	if groupPayment == nil {
		return ocp_transaction.NewIntentDeniedError("unsupported chat metadata type")
	}

	switch groupPayment.GetAction() {
	case intentpb.PaymentAction_DEFAULT, intentpb.PaymentAction_SEND:
	default:
		return ocp_transaction.NewIntentDeniedError("group payment action is not supported")
	}

	chatID := chatMetadata.GetChatId()
	if !chat.IsGroupChatID(chatID) {
		return ocp_transaction.NewIntentValidationError("chat id is not a group")
	}

	senderUserID, recipientUserID, err := i.resolveDirectChatPaymentParties(ctx, intentRecord, "group")
	if err != nil {
		return err
	}

	if bytes.Equal(senderUserID.Value, recipientUserID.Value) {
		return ocp_transaction.NewIntentDeniedError("payment to yourself is a no-op")
	}

	if i.isTeamAccount(senderUserID) || i.isTeamAccount(recipientUserID) {
		return ocp_transaction.NewIntentDeniedError("group payments with the flipcash team are not allowed")
	}

	canSpeak, err := i.access.CanSpeak(ctx, chatID, senderUserID)
	if err != nil {
		return err
	}
	if !canSpeak {
		return ocp_transaction.NewIntentDeniedError("sender cannot send in the group")
	}

	isMember, err := i.access.IsMember(ctx, chatID, recipientUserID)
	if err != nil {
		return err
	}
	if !isMember {
		return ocp_transaction.NewIntentDeniedError("recipient is not a member of the group")
	}

	return nil
}

// ErrNotBoostPayment is returned by GetBoostPayment for an intent that cannot
// carry a boost's payment.
var ErrNotBoostPayment = errors.New("intent is not a boost payment")

// BoostPayment is the payment behind a reaction boost, read off its intent
// record by GetBoostPayment: who paid, who it pays, and the whole payment.
// It is what boost validation and the boost task read instead of the intent's
// own metadata, so that the one place that knows which intents carry a boost,
// and how each names its destinations, is GetBoostPayment.
type BoostPayment struct {
	// Payer is the owner account that made the payment.
	Payer *ocp_common.Account

	// Destinations are the payment's destinations, one per payee, in the
	// payment's own order.
	Destinations []BoostDestination

	// Amount is the whole payment, every destination's part together.
	Amount *commonpb.CryptoPaymentAmount

	// UsdMarketValue is the whole payment's value in USD, what the boost
	// grows the reaction's totals by.
	UsdMarketValue float64
}

// BoostDestination is one destination of a boost's payment: the owner account
// paid and how much.
type BoostDestination struct {
	Owner  *ocp_common.Account
	Quarks uint64
}

// GetBoostPayment reads the payment behind a reaction boost off its intent
// record. Today that is a direct SendPublicPayment, which has one
// destination, so only a boost that pays one person can be made (see
// messaging.BoostPayees): a payment that must pay both the group's creator and
// the message's sender needs a payment with two destinations, which OCP does
// not offer yet, and is refused by messaging.ValidateBoostPayouts as a payout
// missing. A payment with several destinations is read here, as one
// BoostDestination each, when OCP has one.
//
// It reads nothing but the record and returns ErrNotBoostPayment (wrapped)
// for an intent that is not a direct payment.
func GetBoostPayment(intentRecord *ocp_intent.Record) (*BoostPayment, error) {
	switch intentRecord.IntentType {
	case ocp_intent.SendPublicPayment:
	default:
		return nil, fmt.Errorf("%w: intent type %d", ErrNotBoostPayment, intentRecord.IntentType)
	}

	metadata := intentRecord.SendPublicPaymentMetadata
	if metadata == nil {
		return nil, fmt.Errorf("%w: missing payment metadata", ErrNotBoostPayment)
	}
	// A boost pays its payees directly. Withdrawals, remote sends and swap
	// sells are routed elsewhere.
	if metadata.IsWithdrawal || metadata.IsIndirectSend || metadata.IsSwapSell {
		return nil, fmt.Errorf("%w: not a direct payment", ErrNotBoostPayment)
	}
	if len(metadata.DestinationOwnerAccount) == 0 {
		return nil, fmt.Errorf("%w: destination has no owner", ErrNotBoostPayment)
	}

	payer, err := ocp_common.NewAccountFromPublicKeyString(intentRecord.InitiatorOwnerAccount)
	if err != nil {
		return nil, fmt.Errorf("invalid initiator owner account: %w", err)
	}
	destinationOwner, err := ocp_common.NewAccountFromPublicKeyString(metadata.DestinationOwnerAccount)
	if err != nil {
		return nil, fmt.Errorf("invalid destination owner account: %w", err)
	}
	mint, err := ocp_common.NewAccountFromPublicKeyString(intentRecord.MintAccount)
	if err != nil {
		return nil, fmt.Errorf("invalid mint account: %w", err)
	}

	return &BoostPayment{
		Payer:        payer,
		Destinations: []BoostDestination{{Owner: destinationOwner, Quarks: metadata.Quantity}},
		Amount: &commonpb.CryptoPaymentAmount{
			Currency:     string(metadata.ExchangeCurrency),
			NativeAmount: metadata.NativeAmount,
			Quarks:       metadata.Quantity,
			Mint:         &commonpb.PublicKey{Value: mint.PublicKey().ToBytes()},
		},
		UsdMarketValue: metadata.UsdMarketValue,
	}, nil
}

// validateReactionBoostAppMetadata enforces that a payment carrying chat app
// metadata is a well-formed reaction boost: a payment by a member of a group
// to put money behind their reaction with an emoji on one of its messages,
// divided between the group's creator and the message's sender as the boost
// rule allows (messaging.ValidateBoostPayouts). The task it schedules records
// the boost through the messaging Sender, which gates on nothing, so every
// gate is applied here, before any money moves; the task could only drop the
// record of a payment already made.
//
// The payer must be a member of the group, the same gate as adding a
// reaction (a reaction is a member's alone; speaking is not required). The
// message must exist and be reactable, which a deleted message still is: its
// tombstone keeps its sender, who is paid like the sender of any other. The emoji
// must be one emoji, judged as a reaction is, and the payer must already have
// reacted with it on the message: a boost puts money behind a reaction the
// payer holds, it does not add one. So the emoji is already on the message
// and no boost needs room under messaging.MaxReactionTypesPerMessage. A payer
// who removes the reaction after this check still has it back when the boost
// lands (Store.AddBoost adds a missing reaction, past the cap if it must),
// since the money has moved by then. The payment must be at least its
// currency's minimum transfer value (see validateMinimumBoost).
// Standing is judged now, not when the task runs: a payer who leaves before
// the boost lands is still credited, since the payment has happened.
//
// No block check is applied, as for group payments. No boost to or from the
// Flipcash team account is allowed.
func (i *Integration) validateReactionBoostAppMetadata(ctx context.Context, intentRecord *ocp_intent.Record, appMetadata *intentpb.AppMetadata) error {
	chatMetadata := appMetadata.GetChat()
	boost := chatMetadata.GetReactionBoost()
	if boost == nil {
		return ocp_transaction.NewIntentDeniedError("unsupported chat metadata type")
	}

	chatID := chatMetadata.GetChatId()
	if !chat.IsGroupChatID(chatID) {
		return ocp_transaction.NewIntentValidationError("chat id is not a group")
	}

	emoji := boost.GetEmoji().GetValue()
	if err := messaging.ValidateEmoji(emoji); err != nil {
		return ocp_transaction.NewIntentDeniedError("boost emoji is not a single emoji")
	}

	payment, err := GetBoostPayment(intentRecord)
	if errors.Is(err, ErrNotBoostPayment) {
		return ocp_transaction.NewIntentDeniedError(fmt.Sprintf("boost must be a direct payment: %v", err))
	} else if err != nil {
		return err
	}

	if err := validateMinimumBoost(payment); err != nil {
		return err
	}

	payerUserID, err := i.accounts.GetUserId(ctx, &commonpb.PublicKey{Value: payment.Payer.PublicKey().ToBytes()})
	if errors.Is(err, account.ErrNotFound) {
		return ocp_transaction.NewIntentDeniedError("payer is not a flipcash user")
	} else if err != nil {
		return err
	}
	if i.isTeamAccount(payerUserID) {
		return ocp_transaction.NewIntentDeniedError("boosts with the flipcash team are not allowed")
	}

	payouts := make([]messaging.BoostPayout, len(payment.Destinations))
	for j, destination := range payment.Destinations {
		payeeUserID, err := i.accounts.GetUserId(ctx, &commonpb.PublicKey{Value: destination.Owner.PublicKey().ToBytes()})
		if errors.Is(err, account.ErrNotFound) {
			return ocp_transaction.NewIntentDeniedError("boost recipient is not a flipcash user")
		} else if err != nil {
			return err
		}
		if i.isTeamAccount(payeeUserID) {
			return ocp_transaction.NewIntentDeniedError("boosts with the flipcash team are not allowed")
		}
		payouts[j] = messaging.BoostPayout{UserID: payeeUserID, Quarks: destination.Quarks}
	}

	// A group that does not exist has no members, so this also refuses one.
	isMember, err := i.access.IsMember(ctx, chatID, payerUserID)
	if err != nil {
		return err
	}
	if !isMember {
		return ocp_transaction.NewIntentDeniedError("payer is not a member of the group")
	}

	rules, err := i.chats.GetGroupRules(ctx, chatID)
	if errors.Is(err, chat.ErrChatNotFound) {
		return ocp_transaction.NewIntentDeniedError("group not found")
	} else if err != nil {
		return err
	}

	// Read after membership, as AddReaction does, so a non-member cannot
	// probe which message IDs exist.
	message, err := i.messages.GetMessage(ctx, chatID, boost.GetMessageId())
	if errors.Is(err, messaging.ErrMessageNotFound) {
		return ocp_transaction.NewIntentDeniedError("message not found")
	} else if err != nil {
		return err
	}
	if !message.IsReactable() {
		return ocp_transaction.NewIntentDeniedError("message cannot be reacted to")
	}

	if _, err := messaging.ValidateBoostPayouts(payerUserID, rules.CreatorID, message.SenderID, payouts); err != nil {
		return ocp_transaction.NewIntentDeniedError(fmt.Sprintf("boost payouts are not allowed: %v", err))
	}

	selfReactions, err := i.messages.GetSelfReactions(ctx, chatID, payerUserID, []*messagingpb.MessageId{boost.GetMessageId()})
	if err != nil {
		return err
	}
	if !hasEmoji(selfReactions, emoji) {
		return ocp_transaction.NewIntentDeniedError("payer has not reacted with the emoji")
	}

	return nil
}

// hasEmoji reports whether any of a viewer's reactions is with emoji.
func hasEmoji(reactions []messaging.SelfReaction, emoji string) bool {
	for _, reaction := range reactions {
		if reaction.Emoji == emoji {
			return true
		}
	}
	return false
}

// validateMinimumBoost enforces the least a reaction boost may pay: the
// minimum transfer value of the payment's currency (send.MinimumTransferValue,
// a cent for USD). The amount is a fiat value that can be derived from a
// quoted rate and land a fraction of a minor unit short, so, as for the preset
// minimums, half a minor unit of slack is allowed: an amount that rounds to at
// least one minor unit passes. A payment must also
// be worth at least a micro-dollar, the unit a boost is recorded in (see
// messaging.MicrosFromUsd): one worth less would credit nothing, which the
// store refuses (messaging.ErrBoostEmpty) after the money has moved, so it is
// refused here instead. Only the minor unit of a currency worth very little
// can fall below that.
func validateMinimumBoost(payment *BoostPayment) error {
	currency := currency_lib.Code(payment.Amount.Currency)
	minimum := send.MinimumTransferValue(currency)
	if payment.Amount.NativeAmount < minimum-0.5*minimum {
		return ocp_transaction.NewIntentDeniedError(fmt.Sprintf(
			"boost amount is below the minimum of %s %s",
			strconv.FormatFloat(minimum, 'f', -1, 64),
			strings.ToUpper(string(currency)),
		))
	}
	if messaging.MicrosFromUsd(payment.UsdMarketValue) == 0 {
		return ocp_transaction.NewIntentDeniedError("boost amount is worth less than a micro-dollar")
	}
	return nil
}

// isTeamAccount reports whether userID is the Flipcash team account (see
// NewIntegration).
func (i *Integration) isTeamAccount(userID *commonpb.UserId) bool {
	return i.teamUserID != nil && bytes.Equal(userID.GetValue(), i.teamUserID.Value)
}

// validateMinDmChatInitFee enforces the recipient's minimum DM chat
// initialization fee on the payment initializing the chat. The fee is the
// only floor on that payment — the preset minimum of the payment's currency is
// not applied beside it — so a payment that clears the fee is accepted even where
// it would fall under its own currency's preset. A payment in the fee's
// currency is compared as is. Any other payment
// is compared by converting its USD market value into the fee's currency at
// the latest live exchange rate, so a sender may pay in whatever currency they
// hold. A fee in a currency with no live rate cannot be compared against, so
// the payment is denied rather than let through unchecked.
func (i *Integration) validateMinDmChatInitFee(ctx context.Context, paymentMetadata *ocp_intent.SendPublicPaymentMetadata, fee *commonpb.FiatPaymentAmount) error {
	if fee == nil {
		return nil
	}

	feeCurrency := currency_lib.Code(fee.Currency)

	var amount float64
	if paymentMetadata.ExchangeCurrency == feeCurrency {
		amount = paymentMetadata.NativeAmount
	} else {
		liveRates, err := i.mintDataProvider.GetLiveExchangeRates(ctx)
		if err != nil {
			return err
		}
		// Rates are quoted as fee-currency units per core mint unit, and the
		// core mint is a USD stablecoin, so this is the rate from USD.
		rate, ok := liveRates.Rates[string(feeCurrency)]
		if !ok || rate <= 0 {
			return ocp_transaction.NewIntentDeniedError(fmt.Sprintf(
				"no exchange rate available for the recipient's chat initialization fee in %s",
				strings.ToUpper(string(feeCurrency)),
			))
		}
		amount = paymentMetadata.UsdMarketValue * rate
	}

	// Same rounding slack as the preset minimum: the fiat amount is derived
	// from a quoted rate and can land a fraction of a minor unit short.
	tolerance := 0.5 * send.MinimumTransferValue(feeCurrency)

	if amount < fee.NativeAmount-tolerance {
		return ocp_transaction.NewIntentDeniedError(fmt.Sprintf(
			"payment amount is below the recipient's chat initialization fee of %s %s",
			strconv.FormatFloat(fee.NativeAmount, 'f', -1, 64),
			strings.ToUpper(string(feeCurrency)),
		))
	}

	return nil
}

// validateDefaultMinimumToChat enforces the send preset minimum for the
// payment's exchange currency on the payment that initializes a chat with a
// recipient who has set no minimum DM chat initialization fee. Clients surface
// the minimum as the first send preset, but the amount is ultimately
// client-chosen, so the floor is enforced here too.
// Currencies without a preset fall back to a USD floor applied to the payment's
// USD market value, so no currency is left without a minimum.
func validateDefaultMinimumToChat(paymentMetadata *ocp_intent.SendPublicPaymentMetadata) error {
	currencyCode := paymentMetadata.ExchangeCurrency
	amount := paymentMetadata.NativeAmount
	presets, ok := send.PresetsFor(currencyCode)
	minimum := presets.Minimum
	if !ok {
		// USD always has presets; the send package's tests pin the row the
		// fallback reads.
		usdPresets, _ := send.PresetsFor(currency_lib.USD)
		currencyCode, amount, minimum = currency_lib.USD, paymentMetadata.UsdMarketValue, usdPresets.Minimum
	}

	// The amount reaching us is a fiat value derived from a quoted exchange
	// rate, so it can land a fraction of a minor unit under the advertised
	// minimum through rounding alone. Allow half of the currency's smallest
	// transferable unit of slack so those payments aren't denied.
	tolerance := 0.5 * send.MinimumTransferValue(currencyCode)

	if amount < minimum-tolerance {
		return ocp_transaction.NewIntentDeniedError(fmt.Sprintf(
			"payment amount is below the minimum of %s %s",
			strconv.FormatFloat(minimum, 'f', -1, 64),
			strings.ToUpper(string(currencyCode)),
		))
	}

	return nil
}
