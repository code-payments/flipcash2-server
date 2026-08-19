package activity

import (
	"context"
	"errors"

	activitypb "github.com/code-payments/flipcash2-protobuf-api/generated/go/activity/v1"
	messagingpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/messaging/v1"

	"github.com/code-payments/flipcash2-server/intent"
	ocp_common "github.com/code-payments/ocp-server/ocp/common"
	ocp_data "github.com/code-payments/ocp-server/ocp/data"
	"github.com/code-payments/ocp-server/ocp/data/history"
)

// injectLocalizedTextForHistory sets the notification's display text from a
// transaction history record.
//
// It is a pure function of the record, where InjectLocalizedText has to read
// the gift card's claiming intent to tell a cancellation from a return. A
// history record already carries that outcome as its state, so nothing here
// goes looking for it.
func injectLocalizedTextForHistory(record *history.Record, notification *activitypb.Notification) error {
	var localizedText string
	switch record.Type {
	case history.DirectlySent:
		localizedText = "Gave"

		if notification.GetDirectlySentCrypto().GetDestinationIdentifier() != nil {
			switch intent.GetDmPaymentVerb(record.AppMetadata) {
			case messagingpb.CashContent_TIPPED:
				localizedText = "Tipped"
			default:
				localizedText = "Sent"
			}
		}

	case history.DirectlyReceived, history.IndirectlyReceived:
		localizedText = "Received"

	case history.IndirectlySent:
		switch record.State {
		case history.StatePending:
			localizedText = "Sending"
		case history.StateVoided:
			localizedText = "Cancelled"
		case history.StateReturned:
			localizedText = "Returned"
		default:
			localizedText = "Sent"
		}

	case history.Deposited:
		localizedText = "Added"

	case history.Withdrawn:
		switch toSwapState(record) {
		case activitypb.SwapState_SWAP_STATE_SUCCEEDED, activitypb.SwapState_SWAP_STATE_NONE:
			localizedText = "Withdrew"
		case activitypb.SwapState_SWAP_STATE_FAILED:
			localizedText = "Withdrawal Failed"
		case activitypb.SwapState_SWAP_STATE_PENDING:
			localizedText = "Withdrawing"
		default:
			return errors.New("unsupported swap state")
		}

	case history.Swap:
		// A swap reads the same whichever way it went, since the notification
		// carries both of its legs for the client to render.
		switch toSwapState(record) {
		case activitypb.SwapState_SWAP_STATE_SUCCEEDED:
			localizedText = "Converted"
		case activitypb.SwapState_SWAP_STATE_FAILED:
			localizedText = "Conversion Failed"
		case activitypb.SwapState_SWAP_STATE_PENDING:
			localizedText = "Converting"
		default:
			return errors.New("unsupported swap state")
		}

	default:
		return errors.New("unsupported notification type")
	}

	notification.LocalizedText = localizedText

	return nil
}

// InjectLocalizedText sets the notification's display text. cashMessageVerb
// applies only to a payment sent to a known recipient, where it distinguishes a
// tip from a plain send; it is ignored for every other notification type.
// Callers derive it with intent.GetDmPaymentVerb so the feed and the DM cash
// message agree.
func InjectLocalizedText(ctx context.Context, ocpData ocp_data.Provider, userOwnerAccount *ocp_common.Account, notification *activitypb.Notification, cashMessageVerb messagingpb.CashContent_Verb) error {
	var localizedText string
	switch typed := notification.AdditionalMetadata.(type) {

	case *activitypb.Notification_DirectlySentCrypto:
		localizedText = "Gave"

		if typed.DirectlySentCrypto.GetDestinationIdentifier() != nil {
			switch cashMessageVerb {
			case messagingpb.CashContent_TIPPED:
				localizedText = "Tipped"
			default:
				localizedText = "Sent"
			}
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
