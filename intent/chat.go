package intent

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/mr-tron/base58"
	"google.golang.org/protobuf/proto"

	chatpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/chat/v1"
	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"
	intentpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/intent/v1"
	messagingpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/messaging/v1"

	"github.com/code-payments/flipcash2-server/account"
	"github.com/code-payments/flipcash2-server/chat"
	"github.com/code-payments/flipcash2-server/messaging"
	"github.com/code-payments/flipcash2-server/paymentrequest"
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

	// TaskTypeRecordRequestedPayment is the task that marks a payment
	// request's share paid by a requested payment and injects its cash
	// message into the group.
	TaskTypeRecordRequestedPayment uint32 = 5
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

// NewRecordRequestedPaymentTask creates the task that marks a payment
// request's share paid after a requested payment, and injects its cash
// message into the group. Only the intent ID is carried, via the reference ID;
// the executor reloads the authoritative intent record at execution time.
func NewRecordRequestedPaymentTask(intentRecord *ocp_intent.Record) *ocp_task.Record {
	intentID := intentRecord.IntentId
	return &ocp_task.Record{
		TaskId:      uuid.NewString(),
		Type:        TaskTypeRecordRequestedPayment,
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

// GetRequestedPayment extracts the requested payment, a payment of the payer's
// share of a payment request, from app metadata, if present. It returns nil
// when there is no app metadata, the metadata fails to decode, or it is not a
// requested payment.
func GetRequestedPayment(appMetadata []byte) *intentpb.ChatMetadata_RequestedPayment {
	return GetChatMetadata(appMetadata).GetRequestedPayment()
}

// IsUserChatPayment reports whether app metadata is a chat payment whose
// parties know each other by user ID: a DM payment, a group payment, whose
// cash message names the payer and every recipient in the group, a reaction
// boost, whose payer is shown on the reaction to the group and whose payees
// are the group's creator and the message's sender, or a requested payment,
// whose activity entries name the payer and the request's creator. A contact
// DM payment is not one, since its parties know each other by phone number; nor
// is a payment carrying no chat metadata, such as one handed off in person,
// where no identity was exchanged at all.
func IsUserChatPayment(appMetadata []byte) bool {
	chatMetadata := GetChatMetadata(appMetadata)
	return chatMetadata.GetDmPayment() != nil ||
		chatMetadata.GetGroupPayment() != nil ||
		chatMetadata.GetReactionBoost() != nil ||
		chatMetadata.GetRequestedPayment() != nil
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
// created before sendByDefaultCutoff. Everything else, contact DM, group and
// requested payments included, is a plain send: a group payment's TIP is
// denied at validation (see validateGroupAppMetadata), and a requested payment
// carries no action.
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

// MaxGroupPaymentReceivers is the most members one group payment may pay: the
// most receivers a cash message can list (messagingpb.CashContent_Payments).
// OCP caps a multi payment's destinations at the same number today; the check
// is Flipcash's own, so a payment that passed validation can always be posted.
const MaxGroupPaymentReceivers = 10

// validateGroupAppMetadata enforces that a payment carrying chat app metadata
// is a well-formed group payment: a direct payment from one member of a group
// to one or more others. A payment to one member is a SendPublicPayment and a
// payment to several at once (2 to MaxGroupPaymentReceivers) a
// SendPublicMultiPayment. The task it schedules posts one cash message into
// the group as the payer, through the messaging Sender, which no rule gates,
// so every gate a send would face is applied here, before any money moves,
// rather than by the task, which could only drop the message of a payment
// already made.
//
// The payer must be able to speak in the group (chat.Access.SpeakerStanding:
// a member who satisfies its rules, so not a non-creator in a creator-only
// group, nor anyone in a private group without its key), and every recipient
// must be a member, whatever the rules say of them, since receiving is not
// speaking. A group that does not exist has no members, so it fails the
// payer's check. No recipient may be the payer or appear twice, so the cash
// message names distinct members other than its sender. Standing and
// membership are judged now, not when the task runs: a payer who leaves, or a
// recipient who does, before the message lands still gets it, since it
// records a payment that has happened.
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

	payment, err := GetDirectPayment(intentRecord)
	if errors.Is(err, ErrNotDirectPayment) {
		return ocp_transaction.NewIntentDeniedError(fmt.Sprintf("group payment must be a direct payment: %v", err))
	} else if err != nil {
		return err
	}
	if len(payment.Destinations) > MaxGroupPaymentReceivers {
		return ocp_transaction.NewIntentDeniedError(fmt.Sprintf("group payment pays more than %d members", MaxGroupPaymentReceivers))
	}

	senderUserID, err := i.accounts.GetUserId(ctx, &commonpb.PublicKey{Value: payment.Payer.PublicKey().ToBytes()})
	if errors.Is(err, account.ErrNotFound) {
		return ocp_transaction.NewIntentDeniedError("sender is not a flipcash user")
	} else if err != nil {
		return err
	}

	pubKeys := make([]*commonpb.PublicKey, len(payment.Destinations))
	for j, destination := range payment.Destinations {
		pubKeys[j] = &commonpb.PublicKey{Value: destination.Owner.PublicKey().ToBytes()}
	}
	userIDsByKey, err := i.accounts.GetUserIds(ctx, pubKeys)
	if err != nil {
		return err
	}

	recipientUserIDs := make([]*commonpb.UserId, len(payment.Destinations))
	seen := make(map[string]struct{}, len(payment.Destinations))
	for j, destination := range payment.Destinations {
		recipientUserID, ok := userIDsByKey[destination.Owner.PublicKey().ToBase58()]
		if !ok {
			return ocp_transaction.NewIntentDeniedError("recipient is not a flipcash user")
		}
		if bytes.Equal(senderUserID.Value, recipientUserID.Value) {
			return ocp_transaction.NewIntentDeniedError("payment to yourself is a no-op")
		}
		if _, ok := seen[string(recipientUserID.Value)]; ok {
			return ocp_transaction.NewIntentDeniedError("group payment pays a recipient more than once")
		}
		seen[string(recipientUserID.Value)] = struct{}{}
		recipientUserIDs[j] = recipientUserID
	}

	if i.isTeamAccount(senderUserID) {
		return ocp_transaction.NewIntentDeniedError("group payments with the flipcash team are not allowed")
	}
	for _, recipientUserID := range recipientUserIDs {
		if i.isTeamAccount(recipientUserID) {
			return ocp_transaction.NewIntentDeniedError("group payments with the flipcash team are not allowed")
		}
	}

	canSpeak, err := i.access.CanSpeak(ctx, chatID, senderUserID)
	if err != nil {
		return err
	}
	if !canSpeak {
		return ocp_transaction.NewIntentDeniedError("sender cannot send in the group")
	}

	for _, recipientUserID := range recipientUserIDs {
		isMember, err := i.access.IsMember(ctx, chatID, recipientUserID)
		if err != nil {
			return err
		}
		if !isMember {
			return ocp_transaction.NewIntentDeniedError("recipient is not a member of the group")
		}
	}

	return nil
}

// validateRequestedPaymentAppMetadata enforces that a SendPublicPayment
// carrying chat app metadata is a well-formed requested payment: a payment by
// a debtor of a payment request of their own unpaid share, exactly, to the
// request's creator. The task it schedules marks the share paid, rechecking
// nothing, so every gate is applied here, before any money moves, rather than
// by the task, which could only drop the record of a payment already made. A multi payment never carries one (see AllowCreation): a
// share is paid to one person.
//
// The checks, in order:
//   - the chat ID is a group's (a validation error otherwise);
//   - the payment is a direct payment to a single destination;
//   - the payer is a Flipcash user;
//   - the request exists and was made in that group;
//   - the request is open as of now (paymentrequest.PaymentRequest.IsOpenAt:
//     not completed, closed, or past its ends_at);
//   - the payer holds a share of the request, and it is unpaid (the creator's
//     own share is paid from creation, so the creator never passes);
//   - the intent's ID is the one the request holds for the payer's share
//     (paymentrequest.PaymentRequest.PaymentIntentIDs). OCP stores an intent
//     ID at most once, in the transaction that moves the money, so this is
//     what makes a share payable at most once: two payments of it, however
//     close together, cannot both commit, which the unpaid check alone,
//     read before either commits and before either's task marks the share,
//     cannot promise;
//   - the destination's owner is the request's creator;
//   - neither the payer nor the creator is the Flipcash team account;
//   - the payment is in the request's currency, read as a boost's is, off the
//     payment's exchange currency (any mint may pay); and
//   - the payment's native amount (its one destination's, as a boost's
//     payouts are read) is the share's native_amount to within half the
//     currency's minor unit (send.MinimumTransferValue), the slack OCP allows
//     between a payment's amount and its quarks, with a few ulps of float
//     error forgiven. A share is paid all at once, so a partial payment or an
//     overpayment is refused.
//
// Membership is not checked beyond the share: a share names a member of the
// group as of the request's creation, and the contract does not require the
// payer or the creator to be one now, so a debtor who has since left may still
// pay what they owe and a creator who has left is still paid. Nor is there a
// floor beyond the share itself (a share is at least the currency's minimum
// transfer value when the request is created), nor a block check, as for
// group payments. The payment is always shown as SENT (see
// GetChatPaymentVerb).
//
// Standing is judged now, not when the task runs: a request closed or expired
// between this check and the task still has the share recorded (see
// paymentrequest.Store.PayShare), since the money has moved by then.
func (i *Integration) validateRequestedPaymentAppMetadata(ctx context.Context, intentRecord *ocp_intent.Record, appMetadata *intentpb.AppMetadata) error {
	chatMetadata := appMetadata.GetChat()
	requested := chatMetadata.GetRequestedPayment()
	if requested == nil {
		return ocp_transaction.NewIntentDeniedError("unsupported chat metadata type")
	}

	chatID := chatMetadata.GetChatId()
	if !chat.IsGroupChatID(chatID) {
		return ocp_transaction.NewIntentValidationError("chat id is not a group")
	}

	payment, err := GetDirectPayment(intentRecord)
	if errors.Is(err, ErrNotDirectPayment) {
		return ocp_transaction.NewIntentDeniedError(fmt.Sprintf("requested payment must be a direct payment: %v", err))
	} else if err != nil {
		return err
	}
	if len(payment.Destinations) != 1 {
		return ocp_transaction.NewIntentDeniedError("requested payment must be a single payment")
	}
	destination := payment.Destinations[0]

	payerUserID, err := i.accounts.GetUserId(ctx, &commonpb.PublicKey{Value: payment.Payer.PublicKey().ToBytes()})
	if errors.Is(err, account.ErrNotFound) {
		return ocp_transaction.NewIntentDeniedError("payer is not a flipcash user")
	} else if err != nil {
		return err
	}

	request, err := i.requests.Get(ctx, requested.GetPaymentRequestId())
	if errors.Is(err, paymentrequest.ErrNotFound) {
		return ocp_transaction.NewIntentDeniedError("payment request not found")
	} else if err != nil {
		return err
	}
	if !bytes.Equal(request.ChatID.GetValue(), chatID.GetValue()) {
		return ocp_transaction.NewIntentDeniedError("payment request was not made in the chat")
	}
	if !request.IsOpenAt(time.Now()) {
		return ocp_transaction.NewIntentDeniedError("payment request is not open")
	}

	share := request.Share(payerUserID)
	if share == nil {
		return ocp_transaction.NewIntentDeniedError("payer has no share")
	}
	if share.GetPaid() != nil {
		return ocp_transaction.NewIntentDeniedError("share is already paid")
	}
	if intentRecord.IntentId != base58.Encode(request.PaymentIntentID(payerUserID)) {
		return ocp_transaction.NewIntentDeniedError("payment must use the share's payment intent id")
	}

	payeeUserID, err := i.accounts.GetUserId(ctx, &commonpb.PublicKey{Value: destination.Owner.PublicKey().ToBytes()})
	switch {
	case errors.Is(err, account.ErrNotFound):
		return ocp_transaction.NewIntentDeniedError("payment must be to the request's creator")
	case err != nil:
		return err
	case !bytes.Equal(payeeUserID.Value, request.Creator.GetValue()):
		return ocp_transaction.NewIntentDeniedError("payment must be to the request's creator")
	}

	if i.isTeamAccount(payerUserID) || i.isTeamAccount(request.Creator) {
		return ocp_transaction.NewIntentDeniedError("requested payments with the flipcash team are not allowed")
	}

	currency := currency_lib.Code(payment.Amount.Currency)
	if string(currency) != request.Split.GetCurrency() {
		return ocp_transaction.NewIntentDeniedError("payment must be in the request's currency")
	}

	expected := share.GetNativeAmount()
	slack := 0.5*send.MinimumTransferValue(currency) + 4*(math.Nextafter(expected, math.Inf(1))-expected)
	if math.IsNaN(destination.NativeAmount) || math.Abs(destination.NativeAmount-expected) > slack {
		return ocp_transaction.NewIntentDeniedError(fmt.Sprintf(
			"payment must pay the share exactly: paid %s, share is %s %s",
			strconv.FormatFloat(destination.NativeAmount, 'f', -1, 64),
			strconv.FormatFloat(expected, 'f', -1, 64),
			strings.ToUpper(string(currency)),
		))
	}

	return nil
}

// ErrNotDirectPayment is returned by GetDirectPayment for an intent that is not
// a direct payment.
var ErrNotDirectPayment = errors.New("intent is not a direct payment")

// DirectPayment is a direct payment from one user to one or more others, read
// off its intent record by GetDirectPayment: who paid, who it pays, and the
// whole payment. It is what group payment, boost and requested payment
// validation, and the tasks they schedule, read instead of the intent's own
// metadata, so that the one place that knows which intents are direct
// payments, and how each names its destinations, is GetDirectPayment.
type DirectPayment struct {
	// Payer is the owner account that made the payment.
	Payer *ocp_common.Account

	// Destinations are the payment's destinations, one per payee, in the
	// payment's own order.
	Destinations []DirectDestination

	// Amount is the whole payment, every destination's part together.
	Amount *commonpb.CryptoPaymentAmount

	// UsdMarketValue is the whole payment's value in USD (for a boost, what
	// it grows the reaction's totals by).
	UsdMarketValue float64
}

// DirectDestination is one destination of a direct payment: the owner account
// paid and how much, in quarks and in the payment's currency.
type DirectDestination struct {
	Owner        *ocp_common.Account
	Quarks       uint64
	NativeAmount float64
}

// GetDirectPayment reads a direct payment off its intent record. A payment to
// one person is a SendPublicPayment, with one destination; a payment to
// several at once is a SendPublicMultiPayment, one DirectDestination per
// destination. Which of the two a payment must be is judged by its validator
// (validateGroupAppMetadata, validateReactionBoostAppMetadata,
// validateRequestedPaymentAppMetadata), not here.
//
// It reads nothing but the record and returns ErrNotDirectPayment (wrapped)
// for an intent that is neither, or a SendPublicPayment that is not direct.
func GetDirectPayment(intentRecord *ocp_intent.Record) (*DirectPayment, error) {
	switch intentRecord.IntentType {
	case ocp_intent.SendPublicPayment:
		return getSingleDirectPayment(intentRecord)
	case ocp_intent.SendPublicMultiPayment:
		return getMultiDirectPayment(intentRecord)
	default:
		return nil, fmt.Errorf("%w: intent type %d", ErrNotDirectPayment, intentRecord.IntentType)
	}
}

// getSingleDirectPayment is GetDirectPayment for a SendPublicPayment.
func getSingleDirectPayment(intentRecord *ocp_intent.Record) (*DirectPayment, error) {
	metadata := intentRecord.SendPublicPaymentMetadata
	if metadata == nil {
		return nil, fmt.Errorf("%w: missing payment metadata", ErrNotDirectPayment)
	}
	// Withdrawals, remote sends and swap sells are routed elsewhere, never
	// directly to a user.
	if metadata.IsWithdrawal || metadata.IsIndirectSend || metadata.IsSwapSell {
		return nil, fmt.Errorf("%w: not a direct payment", ErrNotDirectPayment)
	}
	if len(metadata.DestinationOwnerAccount) == 0 {
		return nil, fmt.Errorf("%w: destination has no owner", ErrNotDirectPayment)
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

	return &DirectPayment{
		Payer:        payer,
		Destinations: []DirectDestination{{Owner: destinationOwner, Quarks: metadata.Quantity, NativeAmount: metadata.NativeAmount}},
		Amount: &commonpb.CryptoPaymentAmount{
			Currency:     string(metadata.ExchangeCurrency),
			NativeAmount: metadata.NativeAmount,
			Quarks:       metadata.Quantity,
			Mint:         &commonpb.PublicKey{Value: mint.PublicKey().ToBytes()},
		},
		UsdMarketValue: metadata.UsdMarketValue,
	}, nil
}

// getMultiDirectPayment is GetDirectPayment for a SendPublicMultiPayment. Its
// totals are the whole payment's, every destination's part together, which
// OCP sums when it records the intent. A multi payment has no indirect form,
// so it is always direct.
func getMultiDirectPayment(intentRecord *ocp_intent.Record) (*DirectPayment, error) {
	metadata := intentRecord.SendPublicMultiPaymentMetadata
	if metadata == nil {
		return nil, fmt.Errorf("%w: missing payment metadata", ErrNotDirectPayment)
	}
	if len(metadata.Payments) == 0 {
		return nil, fmt.Errorf("%w: no destinations", ErrNotDirectPayment)
	}

	payer, err := ocp_common.NewAccountFromPublicKeyString(intentRecord.InitiatorOwnerAccount)
	if err != nil {
		return nil, fmt.Errorf("invalid initiator owner account: %w", err)
	}
	mint, err := ocp_common.NewAccountFromPublicKeyString(intentRecord.MintAccount)
	if err != nil {
		return nil, fmt.Errorf("invalid mint account: %w", err)
	}

	destinations := make([]DirectDestination, len(metadata.Payments))
	for j, payment := range metadata.Payments {
		if len(payment.DestinationOwnerAccount) == 0 {
			return nil, fmt.Errorf("%w: destination has no owner", ErrNotDirectPayment)
		}
		owner, err := ocp_common.NewAccountFromPublicKeyString(payment.DestinationOwnerAccount)
		if err != nil {
			return nil, fmt.Errorf("invalid destination owner account: %w", err)
		}
		destinations[j] = DirectDestination{Owner: owner, Quarks: payment.Quantity, NativeAmount: payment.NativeAmount}
	}

	return &DirectPayment{
		Payer:        payer,
		Destinations: destinations,
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
// or a DM to put money behind their reaction with an emoji on one of its
// messages. In a group it is paid to the message's sender and the group's
// creator exactly as the boost rule divides it
// (messaging.ValidateBoostPayouts: 80% to the sender and 20% to the creator,
// or the whole to one of them when the message is the creator's or the
// creator boosts); in a DM it is a boost of the other member's message, paid
// to them whole (messaging.ValidateDmBoostPayouts). No one boosts their own
// message. A boost that pays one person is a SendPublicPayment and one that
// pays both a SendPublicMultiPayment, and each intent carries its case alone
// (so a DM boost is always a SendPublicPayment). The task it schedules
// records the boost through the messaging Sender, which gates on nothing, so
// every gate is applied here, before any money moves; the task could only
// drop the record of a payment already made.
//
// The payer must be a member of the chat, the same gate as adding a
// reaction (a reaction is a member's alone; speaking is not required). The
// message must exist and be reactable, which a deleted message still is: its
// tombstone keeps its sender, who is paid like the sender of any other. The emoji
// must be one emoji, judged as a reaction is, and the payer must already have
// reacted with it on the message: a boost puts money behind a reaction the
// payer holds, it does not add one. So the emoji is already on the message
// and no boost needs room under messaging.MaxReactionTypesPerMessage. A payer
// who removes the reaction after this check still has it back when the boost
// lands (Store.AddBoost adds a missing reaction, past the cap if it must),
// since the money has moved by then. Every share of the payment must be at
// least its currency's minimum transfer value (see validateMinimumBoost).
// Standing is judged now, not when the task runs: a payer who leaves before
// the boost lands is still credited, since the payment has happened.
//
// No block check is applied, as for DM and group payments, and a DM boost
// pays no DM fee: the DM exists, so its gate has been paid. No boost to or
// from the Flipcash team account is allowed.
func (i *Integration) validateReactionBoostAppMetadata(ctx context.Context, intentRecord *ocp_intent.Record, appMetadata *intentpb.AppMetadata) error {
	chatMetadata := appMetadata.GetChat()
	boost := chatMetadata.GetReactionBoost()
	if boost == nil {
		return ocp_transaction.NewIntentDeniedError("unsupported chat metadata type")
	}

	chatID := chatMetadata.GetChatId()
	isGroup := chat.IsGroupChatID(chatID)
	if !isGroup && len(chatID.GetValue()) != chat.DmChatIDSize {
		return ocp_transaction.NewIntentValidationError("chat id is not a group or a dm")
	}

	emoji := boost.GetEmoji().GetValue()
	if err := messaging.ValidateEmoji(emoji); err != nil {
		return ocp_transaction.NewIntentDeniedError("boost emoji is not a single emoji")
	}

	payment, err := GetDirectPayment(intentRecord)
	if errors.Is(err, ErrNotDirectPayment) {
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
		// The payee's fiat amount, which OCP has checked the destination's
		// quarks are worth, so the split is judged on what each is paid.
		payouts[j] = messaging.BoostPayout{UserID: payeeUserID, Amount: destination.NativeAmount}
	}

	// A chat that does not exist has no members, so this also refuses one.
	isMember, err := i.access.IsMember(ctx, chatID, payerUserID)
	if err != nil {
		return err
	}
	if !isMember {
		return ocp_transaction.NewIntentDeniedError("payer is not a member of the chat")
	}

	var creatorID *commonpb.UserId
	if isGroup {
		rules, err := i.chats.GetGroupRules(ctx, chatID)
		if errors.Is(err, chat.ErrChatNotFound) {
			return ocp_transaction.NewIntentDeniedError("group not found")
		} else if err != nil {
			return err
		}
		creatorID = rules.CreatorID
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

	var payees []messaging.BoostShare
	if isGroup {
		payees, err = messaging.BoostPayees(payerUserID, creatorID, message.SenderID)
	} else {
		payees, err = messaging.DmBoostPayees(payerUserID, message.SenderID)
	}
	if errors.Is(err, messaging.ErrBoostOwnMessage) {
		return ocp_transaction.NewIntentDeniedError("payer cannot boost their own message")
	} else if err != nil {
		return ocp_transaction.NewIntentDeniedError(fmt.Sprintf("boost payouts are not allowed: %v", err))
	}
	// A boost that pays one person is a SendPublicPayment, and one that pays
	// two a SendPublicMultiPayment, each and only.
	switch {
	case intentRecord.IntentType == ocp_intent.SendPublicPayment && len(payees) != 1:
		return ocp_transaction.NewIntentDeniedError("a boost that pays both the message's sender and the group's creator must be a multi payment")
	case intentRecord.IntentType == ocp_intent.SendPublicMultiPayment && len(payees) != 2:
		return ocp_transaction.NewIntentDeniedError("a boost that pays one person must be a single payment")
	}
	// Each share is held to half the currency's smallest unit, the slack OCP
	// allows between a payment's amount and its quarks.
	tolerance := 0.5 * send.MinimumTransferValue(currency_lib.Code(payment.Amount.Currency))
	if isGroup {
		_, err = messaging.ValidateBoostPayouts(payerUserID, creatorID, message.SenderID, payouts, tolerance)
	} else {
		_, err = messaging.ValidateDmBoostPayouts(payerUserID, message.SenderID, payouts, tolerance)
	}
	if err != nil {
		return ocp_transaction.NewIntentDeniedError(fmt.Sprintf("boost payouts are not allowed: %v", err))
	}

	reacted, err := messaging.HoldsReaction(ctx, i.messages, chatID, boost.GetMessageId(), payerUserID, emoji)
	if err != nil {
		return err
	}
	if !reacted {
		return ocp_transaction.NewIntentDeniedError("payer has not reacted with the emoji")
	}

	return nil
}

// validateMinimumBoost enforces the least a reaction boost may pay: every
// share of it, each destination's part, must be at least the minimum
// transfer value of the payment's currency (send.MinimumTransferValue, a cent
// for USD), so a boost paying the sender 80% and the creator 20% is at least
// five times that (5 cents). A boost paying one person has one share, the
// whole payment. The amounts are fiat values that can be derived from a
// quoted rate and land a fraction of a minor unit short, so, as for the
// preset minimums, half a minor unit of slack is allowed: a share that
// rounds to at least one minor unit passes. A payment must also be worth at
// least a micro-dollar, the unit a boost is recorded in (see
// messaging.MicrosFromUsd): one worth less would credit nothing, which the
// store refuses (messaging.ErrBoostEmpty) after the money has moved, so it is
// refused here instead. Only the minor unit of a currency worth very little
// can fall below that.
func validateMinimumBoost(payment *DirectPayment) error {
	currency := currency_lib.Code(payment.Amount.Currency)
	minimum := send.MinimumTransferValue(currency)
	for _, destination := range payment.Destinations {
		if destination.NativeAmount < minimum-0.5*minimum {
			what := "boost amount"
			if len(payment.Destinations) > 1 {
				what = "boost share"
			}
			return ocp_transaction.NewIntentDeniedError(fmt.Sprintf(
				"%s is below the minimum of %s %s",
				what,
				strconv.FormatFloat(minimum, 'f', -1, 64),
				strings.ToUpper(string(currency)),
			))
		}
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
