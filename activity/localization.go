package activity

import (
	"context"
	"errors"

	activitypb "github.com/code-payments/flipcash2-protobuf-api/generated/go/activity/v1"

	ocp_common "github.com/code-payments/ocp-server/ocp/common"
	ocp_data "github.com/code-payments/ocp-server/ocp/data"
)

func InjectLocalizedText(ctx context.Context, ocpData ocp_data.Provider, userOwnerAccount *ocp_common.Account, notification *activitypb.Notification) error {
	var localizedText string
	switch typed := notification.AdditionalMetadata.(type) {

	case *activitypb.Notification_DirectlySentCrypto:
		localizedText = "Gave"

		if typed.DirectlySentCrypto.GetDestinationIdentifier() != nil {
			localizedText = "Sent"
		}

	case *activitypb.Notification_ReceivedCrypto:
		localizedText = "Received"

	case *activitypb.Notification_WithdrewCrypto:
		localizedText = "Withdrew"

		switch typed.WithdrewCrypto.SwapState {
		case activitypb.SwapState_SWAP_STATE_SUCCEEDED, activitypb.SwapState_SWAP_STATE_NONE:
			localizedText = "Withdrew"
		case activitypb.SwapState_SWAP_STATE_FAILED:
			localizedText = "Withdrawal Failed"
		case activitypb.SwapState_SWAP_STATE_PENDING:
			localizedText = "Withdrawing"
		default:
			return errors.New("unsupported swap state")
		}

	case *activitypb.Notification_DepositedCrypto:
		localizedText = "Added"

	case *activitypb.Notification_IndirectlySentCrypto:
		if typed.IndirectlySentCrypto.CanInitiateCancelAction {
			localizedText = "Sending"
		} else {
			localizedText = "Sent"

			giftCardVaultAccount, err := ocp_common.NewAccountFromPublicKeyBytes(typed.IndirectlySentCrypto.Vault.Value)
			if err != nil {
				return err
			}

			intentRecord, err := ocpData.GetGiftCardClaimedIntent(ctx, giftCardVaultAccount.PublicKey().ToBase58())
			if err != nil {
				return err
			}

			if intentRecord.InitiatorOwnerAccount == userOwnerAccount.PublicKey().ToBase58() {
				if intentRecord.ReceivePaymentsPubliclyMetadata.IsIssuerVoidingGiftCard {
					localizedText = "Cancelled"
				}
				if intentRecord.ReceivePaymentsPubliclyMetadata.IsReturned {
					localizedText = "Returned"
				}
			}
		}

	case *activitypb.Notification_BoughtCrypto:
		switch typed.BoughtCrypto.SwapState {
		case activitypb.SwapState_SWAP_STATE_SUCCEEDED:
			localizedText = "Purchased"
		case activitypb.SwapState_SWAP_STATE_FAILED:
			localizedText = "Purchase Failed"
		case activitypb.SwapState_SWAP_STATE_PENDING:
			localizedText = "Purchasing"
		default:
			return errors.New("unsupported swap state")
		}

	case *activitypb.Notification_SoldCrypto:
		switch typed.SoldCrypto.SwapState {
		case activitypb.SwapState_SWAP_STATE_SUCCEEDED:
			localizedText = "Sold"
		case activitypb.SwapState_SWAP_STATE_FAILED:
			localizedText = "Sell Failed"
		case activitypb.SwapState_SWAP_STATE_PENDING:
			localizedText = "Selling"
		default:
			return errors.New("unsupported swap state")
		}

	default:
		return errors.New("unsupported notification type")
	}

	notification.LocalizedText = localizedText

	return nil
}
