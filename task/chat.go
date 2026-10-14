package task

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/mr-tron/base58"

	chatpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/chat/v1"
	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"
	intentpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/intent/v1"
	messagingpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/messaging/v1"

	"github.com/code-payments/flipcash2-server/chat"
	"github.com/code-payments/flipcash2-server/intent"
	ocp_common "github.com/code-payments/ocp-server/ocp/common"
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
	chatID := chat.MustDeriveDmChatID(chatType, payment.senderUserID, payment.recipientUserID)
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
		Members:      []*commonpb.UserId{payment.senderUserID, payment.recipientUserID},
		LastActivity: time.Now().UTC(),
	})
	if err != nil && !errors.Is(err, chat.ErrChatExists) {
		return fmt.Errorf("failed to create dm chat: %w", err)
	}

	return e.sendCashMessage(ctx, chatID, payment)
}

// sendGroupPaymentMessage injects the cash message for a group payment into
// the group it was made in. A returned error means the task is retried with
// backoff.
//
// Nothing about the group is checked again: the payer's standing to speak and
// the recipient's membership were judged when the payment was validated (see
// intent.Integration), and the payment has happened since, so its message
// lands even if either has left the group in between. A group is never
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

// chatPayment is a validated chat payment, reloaded for the task that injects
// its cash message.
type chatPayment struct {
	taskID       uuid.UUID
	rawIntentID  []byte
	intentRecord *ocp_intent.Record
	chatMetadata *intentpb.ChatMetadata

	senderUserID    *commonpb.UserId
	recipientUserID *commonpb.UserId
	mint            *ocp_common.Account
}

// loadChatPayment reloads the authoritative intent record a payment message
// task references, and resolves the payment's sender and recipient. It checks
// only what every chat payment shares; which chat the message goes to is the
// caller's.
func (e *Executor) loadChatPayment(ctx context.Context, record *ocp_task.Record) (*chatPayment, error) {
	taskID, err := uuid.Parse(record.TaskId)
	if err != nil {
		return nil, fmt.Errorf("task id is not a uuid: %w", err)
	}

	if record.ReferenceId == nil {
		return nil, errors.New("task is missing the intent id reference")
	}

	intentID := *record.ReferenceId
	rawIntentID, err := base58.Decode(intentID)
	if err != nil {
		return nil, fmt.Errorf("invalid intent id: %w", err)
	}

	intentRecord, err := e.ocpData.GetIntent(ctx, intentID)
	if err != nil {
		return nil, fmt.Errorf("failed to get intent record: %w", err)
	}

	// The task is only ever scheduled for a validated chat payment, which is
	// always a direct SendPublicPayment between two Flipcash users carrying the
	// matching chat payment app metadata (enforced in
	// intent.Integration.AllowCreation).
	metadata := intentRecord.SendPublicPaymentMetadata
	if intentRecord.IntentType != ocp_intent.SendPublicPayment || metadata == nil {
		return nil, errors.New("intent is not a send public payment")
	}

	senderOwner, err := ocp_common.NewAccountFromPublicKeyString(intentRecord.InitiatorOwnerAccount)
	if err != nil {
		return nil, fmt.Errorf("invalid initiator owner account: %w", err)
	}
	recipientOwner, err := ocp_common.NewAccountFromPublicKeyString(metadata.DestinationOwnerAccount)
	if err != nil {
		return nil, fmt.Errorf("invalid destination owner account: %w", err)
	}
	mint, err := ocp_common.NewAccountFromPublicKeyString(intentRecord.MintAccount)
	if err != nil {
		return nil, fmt.Errorf("invalid mint account: %w", err)
	}

	senderUserID, err := e.accounts.GetUserId(ctx, &commonpb.PublicKey{Value: senderOwner.PublicKey().ToBytes()})
	if err != nil {
		return nil, fmt.Errorf("failed to get sender user id: %w", err)
	}
	recipientUserID, err := e.accounts.GetUserId(ctx, &commonpb.PublicKey{Value: recipientOwner.PublicKey().ToBytes()})
	if err != nil {
		return nil, fmt.Errorf("failed to get recipient user id: %w", err)
	}

	return &chatPayment{
		taskID:          taskID,
		rawIntentID:     rawIntentID,
		intentRecord:    intentRecord,
		chatMetadata:    intent.GetChatMetadata(intentRecord.AppMetadata),
		senderUserID:    senderUserID,
		recipientUserID: recipientUserID,
		mint:            mint,
	}, nil
}

// sendCashMessage sends the payment's cash message into chatID as the
// payer, naming the recipient as its receiver.
//
// Idempotency under at-least-once delivery comes from the messaging layer:
// sends dedupe on (chatID, clientMessageID), and the client message ID is the
// task's UUID, which is stable across retries.
func (e *Executor) sendCashMessage(ctx context.Context, chatID *commonpb.ChatId, payment *chatPayment) error {
	metadata := payment.intentRecord.SendPublicPaymentMetadata

	content := []*messagingpb.Content{{
		Type: &messagingpb.Content_Cash{
			Cash: &messagingpb.CashContent{
				IntentId: &commonpb.IntentId{Value: payment.rawIntentID},
				Amount: &commonpb.CryptoPaymentAmount{
					Currency:     string(metadata.ExchangeCurrency),
					NativeAmount: metadata.NativeAmount,
					Quarks:       metadata.Quantity,
					Mint:         &commonpb.PublicKey{Value: payment.mint.PublicKey().ToBytes()},
				},
				Receiver: payment.recipientUserID,
				// The verb is how the client renders the payment.
				Verb: intent.GetChatPaymentVerb(payment.intentRecord.AppMetadata, payment.intentRecord.CreatedAt),
			},
		},
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
