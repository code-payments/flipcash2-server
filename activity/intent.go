package activity

import (
	"context"

	"github.com/mr-tron/base58"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	activitypb "github.com/code-payments/flipcash2-protobuf-api/generated/go/activity/v1"
	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"
	messagingpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/messaging/v1"

	"github.com/code-payments/flipcash2-server/intent"
	ocp_query "github.com/code-payments/ocp-server/database/query"
	ocp_balance "github.com/code-payments/ocp-server/ocp/balance"
	ocp_common "github.com/code-payments/ocp-server/ocp/common"
	ocp_data "github.com/code-payments/ocp-server/ocp/data"
	ocp_intent "github.com/code-payments/ocp-server/ocp/data/intent"
	ocp_swap "github.com/code-payments/ocp-server/ocp/data/swap"
	"github.com/code-payments/ocp-server/usdc"
)

func (s *Server) getNotificationsFromPagedIntents(ctx context.Context, log *zap.Logger, userID *commonpb.UserId, pubKey *commonpb.PublicKey, pagingToken *string, direction ocp_query.Ordering, limit int) ([]*activitypb.Notification, error) {
	userOwnerAccount, err := ocp_common.NewAccountFromPublicKeyBytes(pubKey.Value)
	if err != nil {
		return nil, err
	}

	queryOptions := []ocp_query.Option{
		ocp_query.WithDirection(direction),
		ocp_query.WithLimit(uint64(limit)),
	}
	if pagingToken != nil {
		intentRecord, err := s.ocpData.GetIntent(ctx, *pagingToken)
		if err != nil {
			return nil, err
		}
		queryOptions = append(queryOptions, ocp_query.WithCursor(ocp_query.ToCursor(uint64(intentRecord.Id))))
	}

	intentRecords, err := s.ocpData.GetAllIntentsByOwner(
		ctx,
		userOwnerAccount.PublicKey().ToBase58(),
		queryOptions...,
	)
	if err == ocp_intent.ErrIntentNotFound {
		return nil, nil
	} else if err != nil {
		return nil, err
	}
	return s.toLocalizedNotifications(ctx, log, userID, userOwnerAccount, intentRecords)
}

func (s *Server) getNotificationsFromBatchIntents(ctx context.Context, log *zap.Logger, userID *commonpb.UserId, pubKey *commonpb.PublicKey, ids []*activitypb.NotificationId) ([]*activitypb.Notification, error) {
	userOwnerAccount, err := ocp_common.NewAccountFromPublicKeyBytes(pubKey.Value)
	if err != nil {
		return nil, status.Error(codes.Internal, "")
	}

	// todo: fetch via a batched DB called
	var intentRecords []*ocp_intent.Record
	for _, id := range ids {
		intentID := base58.Encode(id.Value)

		log := log.With(zap.String("notification_id", intentID))

		intentRecord, err := s.ocpData.GetIntent(ctx, intentID)
		switch err {
		case nil:
		case ocp_intent.ErrIntentNotFound:
			return nil, errNotificationNotFound
		default:
			log.Warn("Failed to get intent", zap.Error(err))
			return nil, err
		}

		var destinationOwner string
		switch intentRecord.IntentType {
		case ocp_intent.SendPublicPayment:
			destinationOwner = intentRecord.SendPublicPaymentMetadata.DestinationOwnerAccount
		case ocp_intent.ReceivePaymentsPublicly:
		case ocp_intent.ExternalDeposit:
		default:
			return nil, errNotificationNotFound
		}
		if userOwnerAccount.PublicKey().ToBase58() != intentRecord.InitiatorOwnerAccount && userOwnerAccount.PublicKey().ToBase58() != destinationOwner {
			return nil, errDeniedNotificationAccess
		}
		intentRecords = append(intentRecords, intentRecord)
	}

	return s.toLocalizedNotifications(ctx, log, userID, userOwnerAccount, intentRecords)
}

type unlocalizedNotification struct {
	notification    *activitypb.Notification
	cashMessageVerb messagingpb.CashContent_Verb
}

func (s *Server) toLocalizedNotifications(ctx context.Context, log *zap.Logger, userID *commonpb.UserId, userOwnerAccount *ocp_common.Account, intentRecords []*ocp_intent.Record) ([]*activitypb.Notification, error) {
	counterpartyUserIDs, err := s.resolveTipDmCounterparties(ctx, userOwnerAccount, intentRecords)
	if err != nil {
		return nil, err
	}

	var unlocalized []unlocalizedNotification
	for _, intentRecord := range intentRecords {
		rawNotificationID, err := base58.Decode(intentRecord.IntentId)
		if err != nil {
			return nil, err
		}

		notification := &activitypb.Notification{
			Id:            &activitypb.NotificationId{Value: rawNotificationID},
			LocalizedText: "",
			Ts:            timestamppb.New(intentRecord.CreatedAt),
			State:         activitypb.NotificationState_NOTIFICATION_STATE_COMPLETED,
		}

		mintAccount, err := ocp_common.NewAccountFromPublicKeyString(intentRecord.MintAccount)
		if err != nil {
			return nil, err
		}

		switch intentRecord.IntentType {
		case ocp_intent.SendPublicPayment:
			intentMetadata := intentRecord.SendPublicPaymentMetadata
			notification.PaymentAmount = &commonpb.CryptoPaymentAmount{
				Currency:     string(intentMetadata.ExchangeCurrency),
				NativeAmount: intentMetadata.NativeAmount,
				Quarks:       intentMetadata.Quantity,
			}

			destinationAccount, err := ocp_common.NewAccountFromPublicKeyString(intentMetadata.DestinationTokenAccount)
			if err != nil {
				return nil, err
			}

			if intentRecord.InitiatorOwnerAccount == userOwnerAccount.PublicKey().ToBase58() {
				if intentMetadata.IsIndirectSend {
					isClaimed, err := isGiftCardClaimed(ctx, s.ocpData, destinationAccount)
					if err != nil {
						return nil, err
					}

					notification.AdditionalMetadata = &activitypb.Notification_IndirectlySentCrypto{IndirectlySentCrypto: &activitypb.IndirectlySentCryptoNotificationMetadata{
						Vault:                   &commonpb.PublicKey{Value: destinationAccount.ToProto().Value},
						CanInitiateCancelAction: !isClaimed,
					}}
					if !isClaimed {
						notification.State = activitypb.NotificationState_NOTIFICATION_STATE_PENDING
					}
				} else if intentMetadata.IsSwapSell {
					swapRecord, err := s.ocpData.GetSwapByFundingId(ctx, intentRecord.IntentId)
					if err != nil {
						return nil, err
					}

					var swapState activitypb.SwapState
					switch swapRecord.State {
					case ocp_swap.StateFinalized:
						swapState = activitypb.SwapState_SWAP_STATE_SUCCEEDED
					case ocp_swap.StateFailed, ocp_swap.StateCancelled:
						swapState = activitypb.SwapState_SWAP_STATE_FAILED
					default:
						swapState = activitypb.SwapState_SWAP_STATE_PENDING
						notification.State = activitypb.NotificationState_NOTIFICATION_STATE_PENDING
					}

					if swapRecord.ToMint == usdc.Mint {
						notification.AdditionalMetadata = &activitypb.Notification_WithdrewCrypto{WithdrewCrypto: &activitypb.WithdrewCryptoNotificationMetadata{
							SwapState: swapState,
						}}
					} else {
						notification.AdditionalMetadata = &activitypb.Notification_SoldCrypto{SoldCrypto: &activitypb.SoldCryptoNotificationMetadata{
							SwapState: swapState,
						}}
					}
				} else if intentMetadata.IsWithdrawal {
					notification.AdditionalMetadata = &activitypb.Notification_WithdrewCrypto{WithdrewCrypto: &activitypb.WithdrewCryptoNotificationMetadata{
						SwapState: activitypb.SwapState_SWAP_STATE_NONE,
					}}
				} else {
					directlySentMetadata := &activitypb.DirectlySentCryptoNotificationMetadata{}
					if contactPayment := intent.GetContactDmPayment(intentRecord.AppMetadata); contactPayment.GetDestination() != nil {
						// A contact DM identifies the recipient by phone number, which
						// the client resolves against the viewer's address book.
						directlySentMetadata.DestinationIdentifier = &activitypb.DirectlySentCryptoNotificationMetadata_Phone{
							Phone: contactPayment.GetDestination(),
						}
					} else if recipientUserID, ok := counterpartyUserIDs[intentMetadata.DestinationOwnerAccount]; ok {
						// A tip DM identifies the recipient by user ID instead, so the
						// client can render their profile without either party's phone
						// number, which stays private in a tip DM.
						directlySentMetadata.DestinationIdentifier = &activitypb.DirectlySentCryptoNotificationMetadata_UserId{
							UserId: recipientUserID,
						}
					}
					notification.AdditionalMetadata = &activitypb.Notification_DirectlySentCrypto{DirectlySentCrypto: directlySentMetadata}
				}
			} else {
				if intentMetadata.IsWithdrawal {
					notification.AdditionalMetadata = &activitypb.Notification_DepositedCrypto{DepositedCrypto: &activitypb.DepositedCryptoNotificationMetadata{}}
				} else {
					receivedMetadata := &activitypb.ReceivedCryptoNotificationMetadata{}
					if contactPayment := intent.GetContactDmPayment(intentRecord.AppMetadata); contactPayment.GetSource() != nil {
						receivedMetadata.SourceIdentifier = &activitypb.ReceivedCryptoNotificationMetadata_Phone{
							Phone: contactPayment.GetSource(),
						}
					} else if senderUserID, ok := counterpartyUserIDs[intentRecord.InitiatorOwnerAccount]; ok {
						receivedMetadata.SourceIdentifier = &activitypb.ReceivedCryptoNotificationMetadata_UserId{
							UserId: senderUserID,
						}
					}
					notification.AdditionalMetadata = &activitypb.Notification_ReceivedCrypto{ReceivedCrypto: receivedMetadata}
				}
			}

		case ocp_intent.ReceivePaymentsPublicly:
			intentMetadata := intentRecord.ReceivePaymentsPubliclyMetadata

			if intentMetadata.IsIssuerVoidingGiftCard || intentMetadata.IsReturned {
				continue
			}

			notification.PaymentAmount = &commonpb.CryptoPaymentAmount{
				Currency:     string(intentMetadata.OriginalExchangeCurrency),
				NativeAmount: intentMetadata.OriginalNativeAmount,
				Quarks:       intentMetadata.Quantity,
			}
			notification.AdditionalMetadata = &activitypb.Notification_ReceivedCrypto{ReceivedCrypto: &activitypb.ReceivedCryptoNotificationMetadata{}}

		case ocp_intent.ExternalDeposit:
			intentMetadata := intentRecord.ExternalDepositMetadata

			// Skip internal return of funds
			if intentMetadata.IsReturned {
				continue
			}

			// Hide small, potentially spam deposits
			if !intentMetadata.IsSwapBuy && intentMetadata.UsdMarketValue < 0.01 {
				continue
			}

			notification.PaymentAmount = &commonpb.CryptoPaymentAmount{
				Currency:     string(intentMetadata.ExchangeCurrency),
				NativeAmount: intentMetadata.NativeAmount,
				Quarks:       intentMetadata.Quantity,
			}

			if intentMetadata.IsSwapBuy {
				notification.AdditionalMetadata = &activitypb.Notification_BoughtCrypto{BoughtCrypto: &activitypb.BoughtCryptoNotificationMetadata{
					SwapState: activitypb.SwapState_SWAP_STATE_SUCCEEDED,
				}}
			} else {
				notification.AdditionalMetadata = &activitypb.Notification_DepositedCrypto{DepositedCrypto: &activitypb.DepositedCryptoNotificationMetadata{}}
			}

		default:
			continue
		}

		if notification.PaymentAmount != nil {
			notification.PaymentAmount.Mint = &commonpb.PublicKey{Value: mintAccount.ToProto().Value}
		}

		unlocalized = append(unlocalized, unlocalizedNotification{
			notification:    notification,
			cashMessageVerb: intent.GetDmPaymentVerb(intentRecord.AppMetadata),
		})
	}

	notifications := make([]*activitypb.Notification, 0, len(unlocalized))
	for _, unlocalized := range unlocalized {
		log := log.With(zap.String("notification_id", NotificationIDString(unlocalized.notification.Id)))

		err := InjectLocalizedText(ctx, s.ocpData, userOwnerAccount, unlocalized.notification, unlocalized.cashMessageVerb)
		if err != nil {
			log.Warn("Failed to inject localized notification text", zap.Error(err))
			return nil, err
		}

		notifications = append(notifications, unlocalized.notification)
	}
	return notifications, nil
}

// resolveTipDmCounterparties returns the user ID of the other party to each tip
// DM payment in intentRecords, keyed by that party's owner account. Unlike a
// contact DM, a tip DM carries no phone number for either side, so the feed
// identifies the counterparty by user ID — resolved here for the whole page in
// one lookup rather than once per notification.
//
// Only tip DM payments are resolved. A payment handed off in person also has an
// owner account that would resolve, but no identity was exchanged there, so
// naming the counterparty would disclose one that neither party shared.
func (s *Server) resolveTipDmCounterparties(ctx context.Context, userOwnerAccount *ocp_common.Account, intentRecords []*ocp_intent.Record) (map[string]*commonpb.UserId, error) {
	var pubKeys []*commonpb.PublicKey
	for _, intentRecord := range intentRecords {
		if intentRecord.IntentType != ocp_intent.SendPublicPayment || intent.GetTipDmPayment(intentRecord.AppMetadata) == nil {
			continue
		}

		counterpartyOwnerAccount := intentRecord.SendPublicPaymentMetadata.DestinationOwnerAccount
		if intentRecord.InitiatorOwnerAccount != userOwnerAccount.PublicKey().ToBase58() {
			counterpartyOwnerAccount = intentRecord.InitiatorOwnerAccount
		}
		if len(counterpartyOwnerAccount) == 0 {
			continue
		}

		ownerAccount, err := ocp_common.NewAccountFromPublicKeyString(counterpartyOwnerAccount)
		if err != nil {
			return nil, err
		}
		pubKeys = append(pubKeys, &commonpb.PublicKey{Value: ownerAccount.PublicKey().ToBytes()})
	}

	if len(pubKeys) == 0 {
		return nil, nil
	}

	// Keyed by base58 public key, which is what an owner account already is, so
	// callers can look up by the owner account they hold. Owner accounts with no
	// binding are absent.
	return s.accounts.GetUserIds(ctx, pubKeys)
}

// isGiftCardClaimed reports whether a gift card has been taken, which is read
// off its balance because nothing records the claim against the card itself.
// The transaction history stores the card's disposition as the state of the
// record instead, so this has no counterpart there.
func isGiftCardClaimed(ctx context.Context, ocpData ocp_data.Provider, giftCardVaultAccount *ocp_common.Account) (bool, error) {
	balance, err := ocp_balance.CalculateFromCache(ctx, ocpData, giftCardVaultAccount)
	if err == ocp_balance.ErrNotManagedByCode {
		return true, nil
	} else if err != nil {
		return false, err
	}
	return balance == 0, nil
}
