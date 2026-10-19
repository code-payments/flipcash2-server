package activity

import (
	"errors"

	activitypb "github.com/code-payments/flipcash2-protobuf-api/generated/go/activity/v1"
	messagingpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/messaging/v1"

	"github.com/code-payments/flipcash2-server/intent"
	"github.com/code-payments/ocp-server/ocp/data/history"
)

// injectLocalizedTextForHistory sets the notification's display text from a
// transaction history record.
//
// It is a pure function of the record. A gift card's outcome, such as a
// cancellation or a return, is already the record's state, so nothing here
// goes looking for it.
func injectLocalizedTextForHistory(record *history.Record, notification *activitypb.Notification) error {
	var localizedText string
	switch record.Type {
	case history.DirectlySent:
		switch {
		case intent.GetReactionBoost(record.AppMetadata) != nil:
			// A boost paying one person names them beside it, the boosted
			// message's sender or the group's creator. One paying both is a
			// multi payment, whose record is the total and names no one.
			localizedText = "Boosted"
		case intent.GetGroupPayment(record.AppMetadata) != nil:
			// A group payment is always a send. One paying a single member
			// names them beside it; one paying several is a multi payment,
			// whose record is the total and names no one, so it must not
			// read Gave, which is a payment handed off in person.
			localizedText = "Sent"
		case notification.GetDirectlySentCrypto().GetDestinationIdentifier() == nil:
			localizedText = "Gave"
		case intent.GetChatPaymentVerb(record.AppMetadata, record.CreatedAt) == messagingpb.CashContent_TIPPED:
			localizedText = "Tipped"
		default:
			localizedText = "Sent"
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
