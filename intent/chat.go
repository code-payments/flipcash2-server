package intent

import (
	"bytes"
	"context"
	"errors"
	"time"

	"github.com/mr-tron/base58"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	chatpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/chat/v1"
	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"
	intentpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/intent/v1"
	messagingpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/messaging/v1"

	"github.com/code-payments/flipcash2-server/account"
	"github.com/code-payments/flipcash2-server/chat"
	"github.com/code-payments/flipcash2-server/messaging"
	"github.com/code-payments/flipcash2-server/profile"
	ocp_common "github.com/code-payments/ocp-server/ocp/common"
	"github.com/code-payments/ocp-server/ocp/data/intent"
	ocp_intent "github.com/code-payments/ocp-server/ocp/data/intent"
	ocp_transaction "github.com/code-payments/ocp-server/ocp/rpc/transaction"
)

// validateContactDmAppMetadata enforces that a SendPublicPayment carrying chat
// app metadata is a well-formed contact DM payment. The metadata is later used
// to render the DM and route the recipient's push (see maybeSendContactPaymentPush),
// so it must be consistent with the payment it accompanies and cannot be trusted
// from the client alone.
//
// The proto's own required-field rules (chat_id, the payment type oneof, the
// contact phone number) are already enforced by appMetadata.Validate() in
// AllowCreation before this is reached.
func (i *Integration) validateContactDmAppMetadata(ctx context.Context, intentRecord *ocp_intent.Record, appMetadata *intentpb.AppMetadata) error {
	chatMetadata := appMetadata.GetChat()
	contactPayment := chatMetadata.GetContactDmPayment()
	if contactPayment == nil {
		return ocp_transaction.NewIntentDeniedError("unsupported chat metadata type")
	}

	// A contact DM payment is a direct user-to-user payment. Withdrawals, remote
	// sends, and swap sells are routed elsewhere and never carry this metadata.
	paymentMetadata := intentRecord.SendPublicPaymentMetadata
	if paymentMetadata.IsWithdrawal || paymentMetadata.IsRemoteSend || paymentMetadata.IsSwapSell {
		return ocp_transaction.NewIntentDeniedError("contact dm payment must be a direct payment")
	}

	// Contact DMs are only valid between two Flipcash users, so the payment must
	// resolve to a registered recipient.
	if len(paymentMetadata.DestinationOwnerAccount) == 0 {
		return ocp_transaction.NewIntentDeniedError("contact dm payment recipient is not a flipcash user")
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
	recipientUserID, err := i.accounts.GetUserId(ctx, &commonpb.PublicKey{Value: recipientOwner.PublicKey().ToBytes()})
	if errors.Is(err, account.ErrNotFound) {
		return ocp_transaction.NewIntentDeniedError("recipient is not a flipcash user")
	} else if err != nil {
		return err
	}

	// The sender must have a phone number linked for payment. The recipient
	// surfaces this number as the DM's identity so a contact DM payment without
	// one has no contact to attribute it to.
	senderProfile, err := i.profiles.GetProfile(ctx, senderUserID, true)
	if errors.Is(err, profile.ErrNotFound) {
		return ocp_transaction.NewIntentDeniedError("sender has no phone number linked for payment")
	} else if err != nil {
		return err
	}
	if senderProfile.PhoneNumber == nil {
		return ocp_transaction.NewIntentDeniedError("sender has no phone number linked for payment")
	}
	isLinkedForPayment, err := i.profiles.IsPhoneNumberLinkedForPayment(ctx, senderUserID, senderProfile.PhoneNumber.Value)
	if err != nil {
		return err
	}
	if !isLinkedForPayment {
		return ocp_transaction.NewIntentDeniedError("sender has no phone number linked for payment")
	}

	// The contact being paid must be the payment's actual recipient: the supplied
	// phone number must be linked for payment to the destination owner. This keeps
	// the metadata honest about who is being paid, consistent with how contact
	// payments are routed by phone-number lookup.
	contactUserID, err := i.profiles.GetUserIdByPhoneNumberForPayment(ctx, contactPayment.GetContact().GetValue())
	if errors.Is(err, profile.ErrNotFound) {
		return ocp_transaction.NewIntentDeniedError("contact phone number is not linked for payment")
	} else if err != nil {
		return err
	}
	if !bytes.Equal(contactUserID.Value, recipientUserID.Value) {
		return ocp_transaction.NewIntentDeniedError("contact does not match payment recipient")
	}

	// The chat must be the canonical DM between the sender and recipient.
	expectedChatID := chat.MustDeriveDmChatID(senderUserID, recipientUserID)
	if !bytes.Equal(chatMetadata.GetChatId().GetValue(), expectedChatID.Value) {
		return ocp_transaction.NewIntentValidationError("chat id does not match the dm between sender and recipient")
	}

	return nil
}

func (i *Integration) maybeSendContactPaymentMessage(ctx context.Context, intentRecord *ocp_intent.Record) {
	if intentRecord.IntentType != intent.SendPublicPayment {
		return
	}

	metadata := intentRecord.SendPublicPaymentMetadata
	if metadata.IsWithdrawal || metadata.IsRemoteSend || metadata.IsSwapSell {
		return
	}

	if len(metadata.DestinationOwnerAccount) == 0 {
		return
	}

	var appMetadata intentpb.AppMetadata
	if len(intentRecord.AppMetadata) == 0 {
		return
	}
	if err := proto.Unmarshal(intentRecord.AppMetadata, &appMetadata); err != nil {
		return
	}
	if appMetadata.GetChat().GetContactDmPayment() == nil {
		return
	}

	log := i.log.With(
		zap.String("intent_id", intentRecord.IntentId),
		zap.String("initiator_owner", intentRecord.InitiatorOwnerAccount),
		zap.String("destination_owner", metadata.DestinationOwnerAccount),
	)

	senderOwner, err := ocp_common.NewAccountFromPublicKeyString(intentRecord.InitiatorOwnerAccount)
	if err != nil {
		log.Warn("Invalid initiator owner account", zap.Error(err))
		return
	}
	recipientOwner, err := ocp_common.NewAccountFromPublicKeyString(metadata.DestinationOwnerAccount)
	if err != nil {
		log.Warn("Invalid destination owner account", zap.Error(err))
		return
	}
	mintAccount, err := ocp_common.NewAccountFromPublicKeyString(intentRecord.MintAccount)
	if err != nil {
		log.Warn("Invalid mint account", zap.Error(err))
		return
	}

	senderUserID, err := i.accounts.GetUserId(ctx, &commonpb.PublicKey{Value: senderOwner.PublicKey().ToBytes()})
	if errors.Is(err, account.ErrNotFound) {
		return
	} else if err != nil {
		log.Warn("Failed to get sender user id", zap.Error(err))
		return
	}

	recipientUserID, err := i.accounts.GetUserId(ctx, &commonpb.PublicKey{Value: recipientOwner.PublicKey().ToBytes()})
	if errors.Is(err, account.ErrNotFound) {
		return
	} else if err != nil {
		log.Warn("Failed to get recipient user id", zap.Error(err))
		return
	}

	rawIntentID, err := base58.Decode(intentRecord.IntentId)
	if err != nil {
		log.Warn("Invalid intent id", zap.Error(err))
		return
	}

	chatID := chat.MustDeriveDmChatID(senderUserID, recipientUserID)

	// Best-effort create the canonical DM between the two users. An earlier
	// message or a concurrent payment may have already created it, so an existing
	// chat is the expected steady state, not a failure.
	err = i.chats.PutChat(ctx, &chat.Chat{
		ID:           chatID,
		Type:         chatpb.Metadata_DM,
		Members:      []*commonpb.UserId{senderUserID, recipientUserID},
		LastActivity: time.Now().UTC(),
	})
	if err != nil && !errors.Is(err, chat.ErrChatExists) {
		log.Warn("Failed to create contact dm chat", zap.Error(err))
		return
	}

	intentID := &commonpb.IntentId{Value: rawIntentID}
	content := []*messagingpb.Content{{
		Type: &messagingpb.Content_Cash{
			Cash: &messagingpb.CashContent{
				IntentId: intentID,
				Amount: &commonpb.CryptoPaymentAmount{
					Currency:     string(metadata.ExchangeCurrency),
					NativeAmount: metadata.NativeAmount,
					Quarks:       metadata.Quantity,
					Mint:         &commonpb.PublicKey{Value: mintAccount.PublicKey().ToBytes()},
				},
			},
		},
	}}

	if _, err := i.sender.Send(
		ctx,
		chatID,
		senderUserID,
		content,
		messaging.IntentIdToClientMessageId(intentID),
		true,
	); err != nil {
		log.Warn("Failed to send contact payment message", zap.Error(err))
	}
}
