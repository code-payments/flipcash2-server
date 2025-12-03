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
	case *activitypb.Notification_WelcomeBonus:
		localizedText = "Welcome Bonus"

	case *activitypb.Notification_GaveCrypto:
		localizedText = "Gave"

	case *activitypb.Notification_ReceivedCrypto:
		localizedText = "Received"

	case *activitypb.Notification_WithdrewCrypto:
		localizedText = "Withdrew"

	case *activitypb.Notification_DepositedCrypto:
		localizedText = "Added"

	case *activitypb.Notification_SentCrypto:
		if typed.SentCrypto.CanInitiateCancelAction {
			localizedText = "Sending"
		} else {
			localizedText = "Sent"

			giftCardVaultAccount, err := ocp_common.NewAccountFromPublicKeyBytes(typed.SentCrypto.Vault.Value)
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

	default:
		return errors.New("unsupported notification type")
	}

	notification.LocalizedText = localizedText

	return nil
}
