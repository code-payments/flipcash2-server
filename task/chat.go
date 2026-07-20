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
	messagingpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/messaging/v1"

	"github.com/code-payments/flipcash2-server/chat"
	"github.com/code-payments/flipcash2-server/intent"
	ocp_common "github.com/code-payments/ocp-server/ocp/common"
	ocp_intent "github.com/code-payments/ocp-server/ocp/data/intent"
	ocp_task "github.com/code-payments/ocp-server/ocp/data/task"
)

// sendContactDmPaymentMessage injects the cash message for a contact DM
// payment into the DM between the sender and recipient. A returned error
// means the task is retried with backoff.
//
// Idempotency under at-least-once delivery comes from the messaging layer:
// sends dedupe on (chatID, clientMessageID), and the client message ID is the
// task's UUID, which is stable across retries.
func (e *Executor) sendContactDmPaymentMessage(ctx context.Context, record *ocp_task.Record) error {
	taskID, err := uuid.Parse(record.TaskId)
	if err != nil {
		return fmt.Errorf("task id is not a uuid: %w", err)
	}

	if record.ReferenceId == nil {
		return errors.New("task is missing the intent id reference")
	}

	intentID := *record.ReferenceId
	rawIntentID, err := base58.Decode(intentID)
	if err != nil {
		return fmt.Errorf("invalid intent id: %w", err)
	}

	intentRecord, err := e.ocpData.GetIntent(ctx, intentID)
	if err != nil {
		return fmt.Errorf("failed to get intent record: %w", err)
	}

	// The task is only ever scheduled for a validated contact DM payment, which
	// is always a direct SendPublicPayment between two Flipcash users carrying
	// contact DM payment app metadata (enforced in
	// intent.Integration.AllowCreation).
	metadata := intentRecord.SendPublicPaymentMetadata
	if intentRecord.IntentType != ocp_intent.SendPublicPayment || metadata == nil {
		return errors.New("intent is not a send public payment")
	}
	chatMetadata := intent.GetChatMetadata(intentRecord)
	if chatMetadata.GetContactDmPayment() == nil {
		return errors.New("intent is not a contact dm payment")
	}

	senderOwner, err := ocp_common.NewAccountFromPublicKeyString(intentRecord.InitiatorOwnerAccount)
	if err != nil {
		return fmt.Errorf("invalid initiator owner account: %w", err)
	}
	recipientOwner, err := ocp_common.NewAccountFromPublicKeyString(metadata.DestinationOwnerAccount)
	if err != nil {
		return fmt.Errorf("invalid destination owner account: %w", err)
	}
	mintAccount, err := ocp_common.NewAccountFromPublicKeyString(intentRecord.MintAccount)
	if err != nil {
		return fmt.Errorf("invalid mint account: %w", err)
	}

	senderUserID, err := e.accounts.GetUserId(ctx, &commonpb.PublicKey{Value: senderOwner.PublicKey().ToBytes()})
	if err != nil {
		return fmt.Errorf("failed to get sender user id: %w", err)
	}
	recipientUserID, err := e.accounts.GetUserId(ctx, &commonpb.PublicKey{Value: recipientOwner.PublicKey().ToBytes()})
	if err != nil {
		return fmt.Errorf("failed to get recipient user id: %w", err)
	}

	// The message must land in the canonical DM between the two users, which is
	// the chat the client referenced in the validated app metadata.
	chatID := chat.MustDeriveDmChatID(chatpb.ChatType_CONTACT_DM, senderUserID, recipientUserID)
	if !bytes.Equal(chatMetadata.GetChatId().GetValue(), chatID.Value) {
		return errors.New("chat id does not match the dm between sender and recipient")
	}

	// Best-effort create the canonical DM between the two users. An earlier
	// message, a concurrent payment, or a prior attempt of this task may have
	// already created it, so an existing chat is the expected steady state, not
	// a failure.
	err = e.chats.PutChat(ctx, &chat.Chat{
		ID:           chatID,
		Type:         chatpb.ChatType_CONTACT_DM,
		Members:      []*commonpb.UserId{senderUserID, recipientUserID},
		LastActivity: time.Now().UTC(),
	})
	if err != nil && !errors.Is(err, chat.ErrChatExists) {
		return fmt.Errorf("failed to create contact dm chat: %w", err)
	}

	content := []*messagingpb.Content{{
		Type: &messagingpb.Content_Cash{
			Cash: &messagingpb.CashContent{
				IntentId: &commonpb.IntentId{Value: rawIntentID},
				Amount: &commonpb.CryptoPaymentAmount{
					Currency:     string(metadata.ExchangeCurrency),
					NativeAmount: metadata.NativeAmount,
					Quarks:       metadata.Quantity,
					Mint:         &commonpb.PublicKey{Value: mintAccount.PublicKey().ToBytes()},
				},
			},
		},
	}}

	if _, err := e.sender.Send(
		ctx,
		chatID,
		senderUserID,
		content,
		&messagingpb.ClientMessageId{Value: taskID[:]},
		true,
	); err != nil {
		return fmt.Errorf("failed to send contact payment message: %w", err)
	}

	return nil
}
