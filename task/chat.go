package task

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/mr-tron/base58"
	"google.golang.org/protobuf/types/known/timestamppb"

	chatpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/chat/v1"
	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"
	intentpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/intent/v1"
	messagingpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/messaging/v1"
	paymentrequestpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/paymentrequest/v1"

	"github.com/code-payments/flipcash2-server/chat"
	"github.com/code-payments/flipcash2-server/intent"
	"github.com/code-payments/flipcash2-server/messaging"
	ocp_intent "github.com/code-payments/ocp-server/ocp/data/intent"
	ocp_task "github.com/code-payments/ocp-server/ocp/data/task"
)

// sendContactDmPaymentMessage injects the cash message for a contact DM
// payment into the DM between the sender and recipient.
func (e *Executor) sendContactDmPaymentMessage(ctx context.Context, record *ocp_task.Record) error {
	return e.sendDmPaymentMessageOfType(ctx, record, chatpb.ChatType_CONTACT_DM)
}

// sendDmPaymentMessage injects the cash message for a DM payment into the DM
// between the sender and recipient.
func (e *Executor) sendDmPaymentMessage(ctx context.Context, record *ocp_task.Record) error {
	return e.sendDmPaymentMessageOfType(ctx, record, chatpb.ChatType_DM)
}

// sendDmPaymentMessageOfType injects the cash message for a DM payment into
// the canonical DM of the given type between the sender and recipient,
// creating the DM if this is the payment that opens it. A returned error means
// the task is retried with backoff.
func (e *Executor) sendDmPaymentMessageOfType(ctx context.Context, record *ocp_task.Record, chatType chatpb.ChatType) error {
	payment, err := e.loadChatPayment(ctx, record)
	if err != nil {
		return err
	}

	// A DM payment pays the other member of the DM alone, so it is never a
	// multi payment.
	if len(payment.recipients) != 1 {
		return fmt.Errorf("dm payment has %d recipients", len(payment.recipients))
	}
	recipientUserID := payment.recipients[0].userID

	// The metadata must match the DM the payment is being injected into.
	switch chatType {
	case chatpb.ChatType_CONTACT_DM:
		if payment.chatMetadata.GetContactDmPayment() == nil {
			return errors.New("intent is not a contact dm payment")
		}
	case chatpb.ChatType_DM:
		if payment.chatMetadata.GetDmPayment() == nil {
			return errors.New("intent is not a dm payment")
		}
	default:
		return fmt.Errorf("unsupported dm chat type %d", chatType)
	}

	// The message must land in the canonical DM of this type between the two
	// users, which is the chat the client referenced in the validated app
	// metadata.
	chatID := chat.MustDeriveDmChatID(chatType, payment.senderUserID, recipientUserID)
	if !bytes.Equal(payment.chatMetadata.GetChatId().GetValue(), chatID.Value) {
		return errors.New("chat id does not match the dm between sender and recipient")
	}

	// Best-effort create the canonical DM between the two users. An earlier
	// message, a concurrent payment, or a prior attempt of this task may have
	// already created it, so an existing chat is the expected steady state, not
	// a failure. A DM with the team account excludes it from the feed, which
	// the store does on its own (see chat.FeedExclusions).
	err = e.chats.PutChat(ctx, &chat.Chat{
		ID:           chatID,
		Type:         chatType,
		Members:      []*commonpb.UserId{payment.senderUserID, recipientUserID},
		LastActivity: time.Now().UTC(),
	})
	if err != nil && !errors.Is(err, chat.ErrChatExists) {
		return fmt.Errorf("failed to create dm chat: %w", err)
	}

	return e.sendCashMessage(ctx, chatID, payment)
}

// sendGroupPaymentMessage injects the cash message for a group payment, to
// one member or to several at once, into the group it was made in. A returned
// error means the task is retried with backoff.
//
// Nothing about the group is checked again: the payer's standing to speak and
// each recipient's membership were judged when the payment was validated (see
// intent.Integration), and the payment has happened since, so its message
// lands even if any of them has left the group in between. A group is never
// created here; the payment was made in one that exists.
func (e *Executor) sendGroupPaymentMessage(ctx context.Context, record *ocp_task.Record) error {
	payment, err := e.loadChatPayment(ctx, record)
	if err != nil {
		return err
	}

	if payment.chatMetadata.GetGroupPayment() == nil {
		return errors.New("intent is not a group payment")
	}

	chatID := payment.chatMetadata.GetChatId()
	if !chat.IsGroupChatID(chatID) {
		return errors.New("chat id is not a group")
	}

	return e.sendCashMessage(ctx, chatID, payment)
}

// recordReactionBoost records a reaction boost against the payer's reaction
// once its payment is made (messaging.Sender.RecordBoost), crediting the
// payment's USD market value to the emoji's total and the payer's, and
// broadcasting one BOOSTED update. A returned error means the task is retried
// with backoff.
//
// Nothing about the group or the message is checked again: the payer's
// membership, the message and the emoji's room on it were judged when the
// payment was validated (see intent.Integration), and the payment has
// happened since, so the boost lands even if the payer has left or the
// message has been deleted in between. Who was paid is not read here at all:
// the payouts were judged at validation, and the boost credits the whole
// payment.
//
// Idempotency under at-least-once delivery comes from the store, which
// records a boost once per intent ID on the message; a repeat changes nothing
// and broadcasts nothing. Refusals that a validated payment never produces
// (an emoji that is not one, a DM, a payment worth nothing) are returned like
// any failure, so they are retried and surfaced rather than dropping the
// record of a payment that was made.
func (e *Executor) recordReactionBoost(ctx context.Context, record *ocp_task.Record) error {
	_, rawIntentID, intentRecord, err := e.loadIntent(ctx, record)
	if err != nil {
		return err
	}

	boost := intent.GetReactionBoost(intentRecord.AppMetadata)
	if boost == nil {
		return errors.New("intent is not a reaction boost")
	}

	chatID := intent.GetChatMetadata(intentRecord.AppMetadata).GetChatId()
	if !chat.IsGroupChatID(chatID) {
		return errors.New("chat id is not a group")
	}

	payment, err := intent.GetDirectPayment(intentRecord)
	if err != nil {
		return err
	}

	payerUserID, err := e.accounts.GetUserId(ctx, &commonpb.PublicKey{Value: payment.Payer.PublicKey().ToBytes()})
	if err != nil {
		return fmt.Errorf("failed to get payer user id: %w", err)
	}

	if _, _, err := e.sender.RecordBoost(
		ctx,
		chatID,
		boost.GetMessageId(),
		payerUserID,
		boost.GetEmoji().GetValue(),
		messaging.BoostInput{
			IntentID:  &commonpb.IntentId{Value: rawIntentID},
			Amount:    payment.Amount,
			UsdMicros: messaging.MicrosFromUsd(payment.UsdMarketValue),
			Ts:        time.Now().UTC(),
		},
	); err != nil {
		return fmt.Errorf("failed to record boost: %w", err)
	}

	return nil
}

// recordRequestedPayment records a requested payment: it marks the payer's
// share of the payment request paid by the payment's intent, as of when the
// intent was created (paymentrequest.Store.PayShare, which completes the
// request if it was the last unpaid share), then injects one cash message into the group as the
// payer, naming the request's creator as its single receiver and carrying the
// request's ID. A returned error means the task is retried with backoff.
//
// Nothing about the request or the group is checked again: the request being
// open, the payer's unpaid share, the amount, the currency and the payee were
// judged when the payment was validated (see intent.Integration), and the
// payment has happened since. So the share is recorded whatever the request's
// status now (a close or an expiry in between must not lose a payment; the
// store records a payment's share whatever the status), and the message lands
// whoever has left the group. A share the creator marked paid by hand in
// between is replaced by this payment (see paymentrequest.PaidShareSupersedes),
// so the record shows the money that arrived. A share already paid by a
// payment (a repeat of this task) is the store's no-op, and the message is
// still sent: the money moved either way, and the message records that it
// did.
//
// The share is marked before the message is sent, so a failure between the
// two is retried as a no-op mark and then the one message; the reverse order
// could post a message for a share never marked. Idempotency under
// at-least-once delivery comes from the store for the share (its first
// payment stands) and, for the message, from the task's UUID as its client
// message ID (see sendCashMessage). Refusals that a validated payment never
// produces (a request that does not exist, a payer with no share, a payment
// to several people) are returned like any failure, so they are retried and
// surfaced rather than dropping the record of a payment that was made. That
// includes a payment whose intent is not the share's (the store refuses it,
// see paymentrequest.ValidatePaymentIntent): only a path that skipped intent
// validation produces one, and it fails here on every retry rather than
// passing as a no-op on a share already paid.
func (e *Executor) recordRequestedPayment(ctx context.Context, record *ocp_task.Record) error {
	payment, err := e.loadChatPayment(ctx, record)
	if err != nil {
		return err
	}

	requested := payment.chatMetadata.GetRequestedPayment()
	if requested == nil {
		return errors.New("intent is not a requested payment")
	}

	// A share is paid to the request's creator alone, so a requested payment
	// is never a multi payment.
	if len(payment.recipients) != 1 {
		return fmt.Errorf("requested payment has %d recipients", len(payment.recipients))
	}

	chatID := payment.chatMetadata.GetChatId()
	if !chat.IsGroupChatID(chatID) {
		return errors.New("chat id is not a group")
	}

	// The share was paid when the payment was made, not when this task runs:
	// a retried task must not move paid_at (nor, for the last share, the
	// request's closed_at) later, past an ends_at the payment was validated
	// before.
	requestID := requested.GetPaymentRequestId()
	if _, _, err := e.requests.PayShare(ctx, requestID, payment.senderUserID, &paymentrequestpb.PaidShare{
		PaidAt: timestamppb.New(payment.intentRecord.CreatedAt),
		How: &paymentrequestpb.PaidShare_Intent{
			Intent: &commonpb.IntentId{Value: payment.rawIntentID},
		},
	}, time.Now()); err != nil {
		// paymentrequest.ErrNotFound, ErrNotAShare and ErrWrongIntent
		// included: a validated payment never produces them, so they are
		// surfaced, not dropped.
		return fmt.Errorf("failed to mark share paid: %w", err)
	}

	payment.paymentRequestID = requestID
	return e.sendCashMessage(ctx, chatID, payment)
}

// chatPayment is a validated chat payment, reloaded for the task that injects
// its cash message.
type chatPayment struct {
	taskID       uuid.UUID
	rawIntentID  []byte
	intentRecord *ocp_intent.Record
	chatMetadata *intentpb.ChatMetadata

	senderUserID *commonpb.UserId

	// recipients are who the payment paid, one per destination, in the
	// payment's own order: one for a SendPublicPayment, several for a
	// SendPublicMultiPayment.
	recipients []chatPaymentRecipient

	// amount is the whole payment, every recipient's part together.
	amount *commonpb.CryptoPaymentAmount

	// paymentRequestID is the payment request whose share the payment paid,
	// set by recordRequestedPayment alone and carried on its cash message;
	// nil for every other payment.
	paymentRequestID *paymentrequestpb.PaymentRequestId
}

// chatPaymentRecipient is one recipient of a chat payment and their part of
// it, in the payment's currency and mint.
type chatPaymentRecipient struct {
	userID       *commonpb.UserId
	nativeAmount float64
	quarks       uint64
}

// loadChatPayment reloads the authoritative intent record a payment message
// task references, and resolves the payment's sender and recipients. It checks
// only what every chat payment shares; which chat the message goes to, and how
// many recipients it may name, is the caller's.
func (e *Executor) loadChatPayment(ctx context.Context, record *ocp_task.Record) (*chatPayment, error) {
	taskID, rawIntentID, intentRecord, err := e.loadIntent(ctx, record)
	if err != nil {
		return nil, err
	}

	// The task is only ever scheduled for a validated chat payment, which is
	// always a direct payment between Flipcash users carrying the matching
	// chat payment app metadata (enforced in intent.Integration.AllowCreation):
	// a SendPublicPayment to one of them, or a SendPublicMultiPayment to
	// several in a group.
	payment, err := intent.GetDirectPayment(intentRecord)
	if err != nil {
		return nil, err
	}

	senderUserID, err := e.accounts.GetUserId(ctx, &commonpb.PublicKey{Value: payment.Payer.PublicKey().ToBytes()})
	if err != nil {
		return nil, fmt.Errorf("failed to get sender user id: %w", err)
	}

	pubKeys := make([]*commonpb.PublicKey, len(payment.Destinations))
	for j, destination := range payment.Destinations {
		pubKeys[j] = &commonpb.PublicKey{Value: destination.Owner.PublicKey().ToBytes()}
	}
	userIDsByKey, err := e.accounts.GetUserIds(ctx, pubKeys)
	if err != nil {
		return nil, fmt.Errorf("failed to get recipient user ids: %w", err)
	}

	recipients := make([]chatPaymentRecipient, len(payment.Destinations))
	for j, destination := range payment.Destinations {
		userID, ok := userIDsByKey[destination.Owner.PublicKey().ToBase58()]
		if !ok {
			return nil, fmt.Errorf("recipient %s is not a flipcash user", destination.Owner.PublicKey().ToBase58())
		}
		recipients[j] = chatPaymentRecipient{
			userID:       userID,
			nativeAmount: destination.NativeAmount,
			quarks:       destination.Quarks,
		}
	}

	return &chatPayment{
		taskID:       taskID,
		rawIntentID:  rawIntentID,
		intentRecord: intentRecord,
		chatMetadata: intent.GetChatMetadata(intentRecord.AppMetadata),
		senderUserID: senderUserID,
		recipients:   recipients,
		amount:       payment.Amount,
	}, nil
}

// sendCashMessage sends the payment's cash message into chatID as the
// payer: carrying the whole payment as its total, and naming its recipient as
// its single receiver, or, for a payment to several, listing each recipient
// with their part, in the payment's order, as its multi receiver. A requested
// payment's message also carries the request's ID (payment.paymentRequestID);
// every other cash message leaves it unset.
//
// Idempotency under at-least-once delivery comes from the messaging layer:
// sends dedupe on (chatID, clientMessageID), and the client message ID is the
// task's UUID, which is stable across retries.
func (e *Executor) sendCashMessage(ctx context.Context, chatID *commonpb.ChatId, payment *chatPayment) error {
	cash := &messagingpb.CashContent{
		IntentId:    &commonpb.IntentId{Value: payment.rawIntentID},
		TotalAmount: payment.amount,
		// The verb is how the client renders the payment.
		Verb:             intent.GetChatPaymentVerb(payment.intentRecord.AppMetadata, payment.intentRecord.CreatedAt),
		PaymentRequestId: payment.paymentRequestID,
	}
	switch len(payment.recipients) {
	case 0:
		return errors.New("payment has no recipients")
	case 1:
		cash.Recipient = &messagingpb.CashContent_SingleReceiver{SingleReceiver: payment.recipients[0].userID}
	default:
		items := make([]*messagingpb.CashContent_Payment, len(payment.recipients))
		for j, recipient := range payment.recipients {
			items[j] = &messagingpb.CashContent_Payment{
				Receiver:     recipient.userID,
				NativeAmount: recipient.nativeAmount,
				Quarks:       recipient.quarks,
			}
		}
		cash.Recipient = &messagingpb.CashContent_MultiReceiver{MultiReceiver: &messagingpb.CashContent_Payments{Items: items}}
	}

	content := []*messagingpb.Content{{
		Type: &messagingpb.Content_Cash{Cash: cash},
	}}

	if _, err := e.sender.Send(
		ctx,
		chatID,
		payment.senderUserID,
		content,
		&messagingpb.ClientMessageId{Value: payment.taskID[:]},
		true,
	); err != nil {
		return fmt.Errorf("failed to send payment message: %w", err)
	}

	return nil
}

// loadIntent reloads the authoritative intent record a task references by
// its reference ID, returning it with the task's UUID and the intent's raw ID.
func (e *Executor) loadIntent(ctx context.Context, record *ocp_task.Record) (taskID uuid.UUID, rawIntentID []byte, intentRecord *ocp_intent.Record, err error) {
	taskID, err = uuid.Parse(record.TaskId)
	if err != nil {
		return uuid.UUID{}, nil, nil, fmt.Errorf("task id is not a uuid: %w", err)
	}

	if record.ReferenceId == nil {
		return uuid.UUID{}, nil, nil, errors.New("task is missing the intent id reference")
	}

	intentID := *record.ReferenceId
	rawIntentID, err = base58.Decode(intentID)
	if err != nil {
		return uuid.UUID{}, nil, nil, fmt.Errorf("invalid intent id: %w", err)
	}

	intentRecord, err = e.ocpData.GetIntent(ctx, intentID)
	if err != nil {
		return uuid.UUID{}, nil, nil, fmt.Errorf("failed to get intent record: %w", err)
	}

	return taskID, rawIntentID, intentRecord, nil
}
