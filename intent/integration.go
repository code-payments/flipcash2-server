package intent

import (
	"context"
	"errors"

	"github.com/mr-tron/base58"
	"go.uber.org/zap"

	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"
	messagingpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/messaging/v1"
	ocp_transactionpb "github.com/code-payments/ocp-protobuf-api/generated/go/transaction/v1"

	"github.com/code-payments/flipcash2-server/account"
	"github.com/code-payments/flipcash2-server/chat"
	"github.com/code-payments/flipcash2-server/contact"
	"github.com/code-payments/flipcash2-server/phone"
	"github.com/code-payments/flipcash2-server/profile"
	"github.com/code-payments/flipcash2-server/push"
	ocp_common "github.com/code-payments/ocp-server/ocp/common"
	ocp_data "github.com/code-payments/ocp-server/ocp/data"
	"github.com/code-payments/ocp-server/ocp/data/intent"
	ocp_intent "github.com/code-payments/ocp-server/ocp/data/intent"
	ocp_integration "github.com/code-payments/ocp-server/ocp/integration"
	ocp_transaction "github.com/code-payments/ocp-server/ocp/rpc/transaction"
)

type Integration struct {
	log *zap.Logger

	ocpData  ocp_data.Provider
	accounts account.Store
	profiles profile.Store
	contacts contact.Store

	pusher push.Pusher

	phoneHashPepper []byte
}

func NewIntegration(
	log *zap.Logger,
	ocpData ocp_data.Provider,
	accounts account.Store,
	profiles profile.Store,
	contacts contact.Store,
	pusher push.Pusher,
	phoneHashPepper []byte,
) ocp_integration.SubmitIntent {
	return &Integration{
		log:             log,
		ocpData:         ocpData,
		accounts:        accounts,
		profiles:        profiles,
		contacts:        contacts,
		pusher:          pusher,
		phoneHashPepper: phoneHashPepper,
	}
}

func (i *Integration) AllowCreation(ctx context.Context, intentRecord *ocp_intent.Record, metadata *ocp_transactionpb.Metadata, actions []*ocp_transactionpb.Action) error {
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
		return nil

	case ocp_intent.ReceivePaymentsPublicly:
		return nil

	default:
		return ocp_transaction.NewIntentDeniedError("flipcash does not support the intent type")
	}
}

func (i *Integration) OnSuccess(ctx context.Context, intentRecord *ocp_intent.Record) error {
	i.maybeSendContactPaymentPush(ctx, intentRecord)

	return nil
}

func (i *Integration) maybeSendContactPaymentPush(ctx context.Context, intentRecord *ocp_intent.Record) {
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

	senderProfile, err := i.profiles.GetProfile(ctx, senderUserID, true)
	if errors.Is(err, profile.ErrNotFound) {
		return
	} else if err != nil {
		log.Warn("Failed to get sender profile", zap.Error(err))
		return
	}
	if senderProfile.PhoneNumber == nil {
		return
	}

	// Only surface the sender's phone number to the recipient if the sender has
	// linked it for payment, consistent with contact resolution and discovery.
	isLinkedForPayment, err := i.profiles.IsPhoneNumberLinkedForPayment(ctx, senderUserID, senderProfile.PhoneNumber.Value)
	if err != nil {
		log.Warn("Failed to check sender payment link status", zap.Error(err))
		return
	}
	if !isLinkedForPayment {
		return
	}

	senderPhoneHash := phone.SecureHash(senderProfile.PhoneNumber, i.phoneHashPepper)
	isContact, err := i.contacts.IsContact(ctx, recipientUserID, senderPhoneHash)
	if err != nil {
		log.Warn("Failed to check if sender is in recipient's contacts", zap.Error(err))
		return
	}
	if !isContact {
		return
	}

	rawIntentID, err := base58.Decode(intentRecord.IntentId)
	if err != nil {
		log.Warn("Invalid intent id", zap.Error(err))
		return
	}

	chatID := chat.MustDeriveDmChatID(senderUserID, recipientUserID)
	message := &messagingpb.Message{
		Content: []*messagingpb.Content{{
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
		}},
	}

	if err := push.SendContactDmPush(
		ctx,
		i.pusher,
		i.ocpData,
		chatID,
		message,
		senderProfile.PhoneNumber,
		recipientUserID,
	); err != nil {
		log.Warn("Failed to send contact payment push", zap.Error(err))
	}
}
