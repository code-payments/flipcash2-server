package activity

import (
	"context"
	"encoding/binary"
	"errors"
	"strings"

	"github.com/mr-tron/base58"
	"google.golang.org/protobuf/types/known/timestamppb"

	activitypb "github.com/code-payments/flipcash2-protobuf-api/generated/go/activity/v1"
	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"

	"github.com/code-payments/flipcash2-server/intent"
	ocp_query "github.com/code-payments/ocp-server/database/query"
	ocp_common "github.com/code-payments/ocp-server/ocp/common"
	"github.com/code-payments/ocp-server/ocp/data/history"
)

// notificationIDSize is fixed by the proto, which pins a notification ID to
// exactly 32 bytes.
const notificationIDSize = 32

// toNotificationID encodes a history record's ID, which is a 64 bit integer,
// into the 32 bytes a notification ID has to be. The integer occupies the
// trailing 8 bytes so that encoded IDs compare in the same order as the
// integers behind them.
func toNotificationID(recordID uint64) *activitypb.NotificationId {
	value := make([]byte, notificationIDSize)
	binary.BigEndian.PutUint64(value[notificationIDSize-8:], recordID)
	return &activitypb.NotificationId{Value: value}
}

// fromNotificationID reverses toNotificationID. It reports ok false for
// anything that is not an ID this package produced, including an ID minted by
// the intent-derived feed, whose 32 bytes are a base58 decoded intent ID and
// carry no record to look up.
func fromNotificationID(id *activitypb.NotificationId) (recordID uint64, ok bool) {
	value := id.GetValue()
	if len(value) != notificationIDSize {
		return 0, false
	}

	for _, b := range value[:notificationIDSize-8] {
		if b != 0 {
			return 0, false
		}
	}

	recordID = binary.BigEndian.Uint64(value[notificationIDSize-8:])
	if recordID == 0 {
		return 0, false
	}
	return recordID, true
}

// toNotifications converts a page of history records for the owner reading
// them. Records the feed does not surface are dropped, so a page can come back
// shorter than the records handed in.
//
// counterpartyUserIDs maps an owner account to the user ID it belongs to, for
// the records whose counterparty is identified that way. It is resolved for the
// whole page by resolveHistoryCounterparties rather than per record.
func toNotifications(
	records []*history.Record,
	counterpartyUserIDs map[string]*commonpb.UserId,
) ([]*activitypb.Notification, error) {
	notifications := make([]*activitypb.Notification, 0, len(records))
	for _, record := range records {
		notification, err := toNotification(record, counterpartyUserIDs)
		if err != nil {
			return nil, err
		}
		if notification == nil {
			continue
		}
		notifications = append(notifications, notification)
	}
	return notifications, nil
}

// toNotification converts a single record, returning nil for one the feed does
// not surface. Unlike the intent-derived path it reads no other record: a
// swap's outcome and a gift card's disposition are already the record's state,
// rather than something to go and look up.
func toNotification(
	record *history.Record,
	counterpartyUserIDs map[string]*commonpb.UserId,
) (*activitypb.Notification, error) {
	mintAccount, err := ocp_common.NewAccountFromPublicKeyString(record.MintAccount)
	if err != nil {
		return nil, err
	}

	notification := &activitypb.Notification{
		Id:    toNotificationID(record.Id),
		Ts:    timestamppb.New(record.CreatedAt),
		State: toNotificationState(record.State),
		PaymentAmount: &commonpb.CryptoPaymentAmount{
			Currency:     strings.ToLower(string(record.ExchangeCurrency)),
			NativeAmount: record.NativeAmount,
			Quarks:       record.Quantity,
			Mint:         &commonpb.PublicKey{Value: mintAccount.ToProto().Value},
		},
	}

	switch record.Type {
	case history.DirectlySent:
		metadata := &activitypb.DirectlySentCryptoNotificationMetadata{}
		if contactPayment := intent.GetContactDmPayment(record.AppMetadata); contactPayment.GetDestination() != nil {
			// A contact DM identifies the recipient by phone number, which the
			// client resolves against the viewer's address book.
			metadata.DestinationIdentifier = &activitypb.DirectlySentCryptoNotificationMetadata_Phone{
				Phone: contactPayment.GetDestination(),
			}
		} else if userID, ok := counterpartyUserIDs[counterpartyOwnerAccount(record)]; ok {
			// A tip DM identifies the recipient by user ID instead, so the client
			// can render their profile without either party's phone number, which
			// stays private in a tip DM.
			metadata.DestinationIdentifier = &activitypb.DirectlySentCryptoNotificationMetadata_UserId{
				UserId: userID,
			}
		}
		notification.AdditionalMetadata = &activitypb.Notification_DirectlySentCrypto{DirectlySentCrypto: metadata}

	case history.DirectlyReceived, history.IndirectlyReceived:
		metadata := &activitypb.ReceivedCryptoNotificationMetadata{}
		if contactPayment := intent.GetContactDmPayment(record.AppMetadata); contactPayment.GetSource() != nil {
			metadata.SourceIdentifier = &activitypb.ReceivedCryptoNotificationMetadata_Phone{
				Phone: contactPayment.GetSource(),
			}
		} else if userID, ok := counterpartyUserIDs[counterpartyOwnerAccount(record)]; ok {
			metadata.SourceIdentifier = &activitypb.ReceivedCryptoNotificationMetadata_UserId{
				UserId: userID,
			}
		}
		notification.AdditionalMetadata = &activitypb.Notification_ReceivedCrypto{ReceivedCrypto: metadata}

	case history.IndirectlySent:
		if record.GiftCardVault == nil {
			return nil, errors.New("gift card record is missing its vault")
		}
		vaultAccount, err := ocp_common.NewAccountFromPublicKeyString(*record.GiftCardVault)
		if err != nil {
			return nil, err
		}
		notification.AdditionalMetadata = &activitypb.Notification_IndirectlySentCrypto{IndirectlySentCrypto: &activitypb.IndirectlySentCryptoNotificationMetadata{
			Vault: &commonpb.PublicKey{Value: vaultAccount.ToProto().Value},
			// Only a card still waiting to be claimed can be cancelled, and that
			// is exactly what leaves the record pending.
			CanInitiateCancelAction: record.State == history.StatePending,
		}}

	case history.Deposited:
		notification.AdditionalMetadata = &activitypb.Notification_DepositedCrypto{DepositedCrypto: &activitypb.DepositedCryptoNotificationMetadata{}}

	case history.Withdrawn:
		// A withdrawal that converted mints carries the conversion too, so a
		// client can show what was given up and what came back rather than only
		// that a swap was involved.
		swapMetadata, err := toSwapMetadata(record, mintAccount)
		if err != nil {
			return nil, err
		}
		notification.AdditionalMetadata = &activitypb.Notification_WithdrewCrypto{WithdrewCrypto: &activitypb.WithdrewCryptoNotificationMetadata{
			SwapState:    toSwapState(record),
			SwapMetadata: swapMetadata,
		}}

	case history.Swap:
		// A swap is one notification carrying both of its legs, rather than the
		// bought and sold pair that modelled the two halves separately.
		swapMetadata, err := toSwapMetadata(record, mintAccount)
		if err != nil {
			return nil, err
		}
		if swapMetadata == nil {
			return nil, errors.New("swap record is missing its destination leg")
		}
		notification.PaymentAmount = nil
		notification.AdditionalMetadata = &activitypb.Notification_SwappedCrypto{SwappedCrypto: swapMetadata}

	default:
		return nil, nil
	}

	if err := injectLocalizedTextForHistory(record, notification); err != nil {
		return nil, err
	}

	return notification, nil
}

// toSwapMetadata describes the conversion behind a record, or nil for a record
// that converted nothing.
//
// The destination amount is only known once the swap executes, which is what
// the proto's oneof is for: until then only the mint is reported, and a nil
// destination quantity is not a swap that received nothing.
//
// A record carries one valuation, of the leg the owner gave up, so the value of
// what came back is that less the fees the trade cost. Reporting the same value
// on both legs would say the swap was free, and reporting none would lose the
// only figure a client can show.
func toSwapMetadata(record *history.Record, mintAccount *ocp_common.Account) (*activitypb.SwappedCryptoNotificationMetadata, error) {
	if record.DestinationMintAccount == nil {
		return nil, nil
	}

	destinationMint, err := ocp_common.NewAccountFromPublicKeyString(*record.DestinationMintAccount)
	if err != nil {
		return nil, err
	}

	exchangeCurrency := strings.ToLower(string(record.ExchangeCurrency))

	var feeNativeAmount float64
	for _, fee := range record.Fees {
		feeNativeAmount += fee.NativeAmount
	}

	metadata := &activitypb.SwappedCryptoNotificationMetadata{
		From: &commonpb.CryptoPaymentAmount{
			Currency:     exchangeCurrency,
			NativeAmount: record.NativeAmount,
			Quarks:       record.Quantity,
			Mint:         &commonpb.PublicKey{Value: mintAccount.ToProto().Value},
		},
		Fee: &commonpb.FiatPaymentAmount{
			Currency:     exchangeCurrency,
			NativeAmount: feeNativeAmount,
		},
		SwapState: toSwapState(record),
	}

	if record.DestinationQuantity == nil {
		metadata.To = &activitypb.SwappedCryptoNotificationMetadata_ToMint{
			ToMint: &commonpb.PublicKey{Value: destinationMint.ToProto().Value},
		}
	} else {
		receivedNativeAmount := record.NativeAmount - feeNativeAmount
		if receivedNativeAmount < 0 {
			receivedNativeAmount = 0
		}

		metadata.To = &activitypb.SwappedCryptoNotificationMetadata_ToAmount{
			ToAmount: &commonpb.CryptoPaymentAmount{
				Currency:     exchangeCurrency,
				NativeAmount: receivedNativeAmount,
				Quarks:       *record.DestinationQuantity,
				Mint:         &commonpb.PublicKey{Value: destinationMint.ToProto().Value},
			},
		}
	}

	return metadata, nil
}

// toNotificationState reports whether a client should expect the notification to
// change. Only a swap in flight or a gift card yet to be claimed still can.
func toNotificationState(state history.State) activitypb.NotificationState {
	if state == history.StatePending {
		return activitypb.NotificationState_NOTIFICATION_STATE_PENDING
	}
	return activitypb.NotificationState_NOTIFICATION_STATE_COMPLETED
}

// toSwapState maps a record's state onto the swap state the proto carries. A
// withdrawal that converted no mints has no swap behind it, which the proto
// spells SWAP_STATE_NONE.
func toSwapState(record *history.Record) activitypb.SwapState {
	if record.Type == history.Withdrawn && record.DestinationMintAccount == nil {
		return activitypb.SwapState_SWAP_STATE_NONE
	}

	switch record.State {
	case history.StatePending:
		return activitypb.SwapState_SWAP_STATE_PENDING
	case history.StateFailed:
		return activitypb.SwapState_SWAP_STATE_FAILED
	default:
		return activitypb.SwapState_SWAP_STATE_SUCCEEDED
	}
}

// resolveHistoryCounterparties returns the user ID of the other party to each
// tip DM payment in records, keyed by that party's owner account, resolved for
// the whole page in one lookup rather than once per record.
//
// Only tip DM payments are resolved. A payment handed off in person also has a
// counterparty whose owner account would resolve, but no identity was exchanged
// there, so naming them would disclose one that neither party shared.
func (s *Server) resolveHistoryCounterparties(ctx context.Context, records []*history.Record) (map[string]*commonpb.UserId, error) {
	var pubKeys []*commonpb.PublicKey
	for _, record := range records {
		if intent.GetTipDmPayment(record.AppMetadata) == nil {
			continue
		}

		counterparty := counterpartyOwnerAccount(record)
		if len(counterparty) == 0 {
			continue
		}

		ownerAccount, err := ocp_common.NewAccountFromPublicKeyString(counterparty)
		if err != nil {
			return nil, err
		}
		pubKeys = append(pubKeys, &commonpb.PublicKey{Value: ownerAccount.PublicKey().ToBytes()})
	}

	if len(pubKeys) == 0 {
		return nil, nil
	}

	// Keyed by base58 public key, which is what an owner account already is, so
	// callers can look up by the owner account they hold.
	return s.accounts.GetUserIds(ctx, pubKeys)
}

// getPagedNotificationsFromHistory serves a page of the feed from the
// transaction history. Where the intent-derived path reads a balance per gift
// card and joins the swap table per swap, this reads one page and converts it,
// so the cost of a page does not depend on what is in it.
func (s *Server) getPagedNotificationsFromHistory(
	ctx context.Context,
	pubKey *commonpb.PublicKey,
	queryOptions *commonpb.QueryOptions,
	limit int,
) ([]*activitypb.Notification, error) {
	ownerAccount, err := ownerAccountFromAuth(pubKey)
	if err != nil {
		return nil, err
	}

	direction := ocp_query.Ascending
	if queryOptions.GetOrder() == commonpb.QueryOptions_DESC {
		direction = ocp_query.Descending
	}

	opts := []ocp_query.Option{
		ocp_query.WithDirection(direction),
		ocp_query.WithLimit(uint64(limit)),
	}
	if token := queryOptions.GetPagingToken(); token != nil {
		cursor, err := s.cursorFromPagingToken(ctx, ownerAccount, token)
		if err != nil {
			return nil, err
		}
		opts = append(opts, ocp_query.WithCursor(cursor))
	}

	records, err := s.ocpData.GetAllTransactionHistoryByOwner(ctx, ownerAccount, opts...)
	if err == history.ErrNotFound {
		return nil, nil
	} else if err != nil {
		return nil, err
	}

	counterpartyUserIDs, err := s.resolveHistoryCounterparties(ctx, records)
	if err != nil {
		return nil, err
	}

	return toNotifications(records, counterpartyUserIDs)
}

// getBatchNotificationsFromHistory serves specific notifications by ID.
//
// The store's batch read is not scoped to an owner, so the records it returns
// are checked here. A record belonging to someone else is reported as not
// found rather than denied: telling the two apart would answer whether an event
// the caller had no part in exists.
func (s *Server) getBatchNotificationsFromHistory(
	ctx context.Context,
	pubKey *commonpb.PublicKey,
	ids []*activitypb.NotificationId,
) ([]*activitypb.Notification, error) {
	ownerAccount, err := ownerAccountFromAuth(pubKey)
	if err != nil {
		return nil, err
	}

	recordIDs := make([]uint64, 0, len(ids))
	for _, id := range ids {
		recordID, ok := fromNotificationID(id)
		if !ok {
			return nil, errNotificationNotFound
		}
		recordIDs = append(recordIDs, recordID)
	}

	records, err := s.ocpData.GetAllTransactionHistoryByIds(ctx, recordIDs)
	if err == history.ErrNotFound {
		return nil, errNotificationNotFound
	} else if err != nil {
		return nil, err
	}

	for _, record := range records {
		if record.OwnerAccount != ownerAccount {
			return nil, errNotificationNotFound
		}
	}

	// Every requested ID has to resolve, since a caller asking for a specific
	// notification is not paging and cannot tell a dropped one from one that was
	// never theirs.
	if len(records) != len(recordIDs) {
		return nil, errNotificationNotFound
	}

	counterpartyUserIDs, err := s.resolveHistoryCounterparties(ctx, records)
	if err != nil {
		return nil, err
	}

	return toNotifications(records, counterpartyUserIDs)
}

// counterpartyOwnerAccount returns the other party's owner account, or empty
// when the event had none.
func counterpartyOwnerAccount(record *history.Record) string {
	if record.CounterpartyOwnerAccount == nil {
		return ""
	}
	return *record.CounterpartyOwnerAccount
}

// cursorFromPagingToken resolves the notification a client paged to into the
// position the history is ordered by.
//
// The response carries no paging token of its own, so a client pages by sending
// back a notification ID. That names a record rather than a position, and the
// history is ordered by event time, so the record has to be read to learn the
// time that goes with it. The intent-derived path pays the same cost, reading
// the intent a token names to recover its row.
//
// A token naming a record that is not the caller's is refused rather than
// resolved, so paging cannot be used to confirm that someone else's record
// exists.
func (s *Server) cursorFromPagingToken(ctx context.Context, ownerAccount string, token *commonpb.PagingToken) (ocp_query.Cursor, error) {
	recordID, ok := fromNotificationID(&activitypb.NotificationId{Value: token.Value})
	if !ok {
		return nil, errInvalidPagingToken
	}

	records, err := s.ocpData.GetAllTransactionHistoryByIds(ctx, []uint64{recordID})
	if err == history.ErrNotFound {
		return nil, errInvalidPagingToken
	} else if err != nil {
		return nil, err
	}

	if len(records) != 1 || records[0].OwnerAccount != ownerAccount {
		return nil, errInvalidPagingToken
	}

	return history.ToCursor(records[0].CreatedAt, records[0].Id), nil
}

// ownerAccountFromAuth returns the base58 owner account the request was
// authenticated as, which is the owner whose history it may read.
func ownerAccountFromAuth(pubKey *commonpb.PublicKey) (string, error) {
	if pubKey == nil {
		return "", errors.New("auth is missing a public key")
	}
	return base58.Encode(pubKey.Value), nil
}
