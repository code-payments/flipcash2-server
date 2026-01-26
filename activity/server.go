package activity

import (
	"context"
	"errors"

	"github.com/mr-tron/base58"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	activitypb "github.com/code-payments/flipcash2-protobuf-api/generated/go/activity/v1"
	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"

	"github.com/code-payments/flipcash2-server/auth"
	"github.com/code-payments/flipcash2-server/model"
	ocp_query "github.com/code-payments/ocp-server/database/query"
	ocp_common "github.com/code-payments/ocp-server/ocp/common"
	ocp_data "github.com/code-payments/ocp-server/ocp/data"
	ocp_intent "github.com/code-payments/ocp-server/ocp/data/intent"
	ocp_transaction "github.com/code-payments/ocp-server/ocp/rpc/transaction"
	"github.com/code-payments/ocp-server/pointer"
)

const (
	defaultMaxNotifications = 100
)

var (
	errNotificationNotFound     = errors.New("notification not found")
	errDeniedNotificationAccess = errors.New("notification access is denied")
)

type Server struct {
	log *zap.Logger

	authz auth.Authorizer

	ocpData ocp_data.Provider

	activitypb.UnimplementedActivityFeedServer
}

func NewServer(
	log *zap.Logger,
	authz auth.Authorizer,
	ocpData ocp_data.Provider,
) *Server {
	return &Server{
		log: log,

		authz: authz,

		ocpData: ocpData,
	}
}

func (s *Server) GetLatestNotifications(ctx context.Context, req *activitypb.GetLatestNotificationsRequest) (*activitypb.GetLatestNotificationsResponse, error) {
	userID, err := s.authz.Authorize(ctx, req, &req.Auth)
	if err != nil {
		return nil, err
	}

	log := s.log.With(
		zap.String("user_id", model.UserIDString(userID)),
		zap.String("activity_feed_type", req.Type.String()),
	)

	notifications, err := s.getPagedNotifications(ctx, log, userID, req.Auth.GetKeyPair().PubKey, &commonpb.QueryOptions{
		PageSize: req.MaxItems,
		Order:    commonpb.QueryOptions_DESC,
	})
	if err != nil {
		return nil, status.Error(codes.Internal, "")
	}
	return &activitypb.GetLatestNotificationsResponse{Notifications: notifications}, nil
}

func (s *Server) GetPagedNotifications(ctx context.Context, req *activitypb.GetPagedNotificationsRequest) (*activitypb.GetPagedNotificationsResponse, error) {
	userID, err := s.authz.Authorize(ctx, req, &req.Auth)
	if err != nil {
		return nil, err
	}

	log := s.log.With(
		zap.String("user_id", model.UserIDString(userID)),
		zap.String("activity_feed_type", req.Type.String()),
	)

	notifications, err := s.getPagedNotifications(ctx, log, userID, req.Auth.GetKeyPair().PubKey, req.QueryOptions)
	if err != nil {
		return nil, status.Error(codes.Internal, "")
	}
	return &activitypb.GetPagedNotificationsResponse{Notifications: notifications}, nil
}

func (s *Server) GetBatchNotifications(ctx context.Context, req *activitypb.GetBatchNotificationsRequest) (*activitypb.GetBatchNotificationsResponse, error) {
	userID, err := s.authz.Authorize(ctx, req, &req.Auth)
	if err != nil {
		return nil, err
	}

	log := s.log.With(
		zap.String("user_id", model.UserIDString(userID)),
		zap.Int("notification_count", len(req.Ids)),
	)

	notifications, err := s.getBatchNotifications(ctx, log, userID, req.Auth.GetKeyPair().PubKey, req.Ids)
	switch err {
	case nil:
		return &activitypb.GetBatchNotificationsResponse{Notifications: notifications}, nil
	case errDeniedNotificationAccess:
		return &activitypb.GetBatchNotificationsResponse{Result: activitypb.GetBatchNotificationsResponse_DENIED}, nil
	case errNotificationNotFound:
		return &activitypb.GetBatchNotificationsResponse{Result: activitypb.GetBatchNotificationsResponse_NOT_FOUND}, nil
	default:
		return nil, status.Error(codes.Internal, "")
	}
}

func (s *Server) getPagedNotifications(ctx context.Context, log *zap.Logger, userID *commonpb.UserId, pubKey *commonpb.PublicKey, queryOptions *commonpb.QueryOptions) ([]*activitypb.Notification, error) {
	limit := defaultMaxNotifications
	if queryOptions.PageSize > 0 {
		limit = int(queryOptions.PageSize)
	}

	direction := ocp_query.Ascending
	if queryOptions.Order == commonpb.QueryOptions_DESC {
		direction = ocp_query.Descending
	}

	var pagingToken *string
	if queryOptions.PagingToken != nil {
		pagingToken = pointer.String(base58.Encode(queryOptions.PagingToken.Value))
	}

	notifications, err := s.getNotificationsFromPagedIntents(ctx, log, userID, pubKey, pagingToken, direction, limit)
	if err != nil {
		log.Warn("Failed to get notifications", zap.Error(err))
		return nil, err
	}
	return notifications, nil
}

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

func (s *Server) getBatchNotifications(ctx context.Context, log *zap.Logger, userID *commonpb.UserId, pubKey *commonpb.PublicKey, ids []*activitypb.NotificationId) ([]*activitypb.Notification, error) {
	notifications, err := s.getNotificationsFromBatchIntents(ctx, log, userID, pubKey, ids)
	if err != nil {
		log.Warn("Failed to get notifications", zap.Error(err))
		return nil, err
	}
	return notifications, nil
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

func (s *Server) toLocalizedNotifications(ctx context.Context, log *zap.Logger, userID *commonpb.UserId, userOwnerAccount *ocp_common.Account, intentRecords []*ocp_intent.Record) ([]*activitypb.Notification, error) {
	welcomeBonusIntentID := ocp_transaction.GetAirdropIntentId(ocp_transaction.AirdropTypeWelcomeBonus, userOwnerAccount.PublicKey().ToBase58())

	var notifications []*activitypb.Notification
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
				if intentMetadata.IsRemoteSend {
					isClaimed, err := isGiftCardClaimed(ctx, s.ocpData, destinationAccount)
					if err != nil {
						return nil, err
					}

					notification.AdditionalMetadata = &activitypb.Notification_SentCrypto{SentCrypto: &activitypb.SentCryptoNotificationMetadata{
						Vault:                   &commonpb.PublicKey{Value: destinationAccount.ToProto().Value},
						CanInitiateCancelAction: !isClaimed,
					}}
					if !isClaimed {
						notification.State = activitypb.NotificationState_NOTIFICATION_STATE_PENDING
					}
				} else if intentMetadata.IsSwapSell {
					notification.AdditionalMetadata = &activitypb.Notification_SoldCrypto{SoldCrypto: &activitypb.SoldCryptoNotificationMetadata{}}
				} else if intentMetadata.IsWithdrawal {
					notification.AdditionalMetadata = &activitypb.Notification_WithdrewCrypto{WithdrewCrypto: &activitypb.WithdrewCryptoNotificationMetadata{}}
				} else {
					notification.AdditionalMetadata = &activitypb.Notification_GaveCrypto{GaveCrypto: &activitypb.GaveCryptoNotificationMetadata{}}
				}
			} else {
				if intentRecord.IntentId == welcomeBonusIntentID {
					notification.AdditionalMetadata = &activitypb.Notification_WelcomeBonus{WelcomeBonus: &activitypb.WelcomeBonusNotificationMetadata{}}
				} else if intentMetadata.IsWithdrawal {
					notification.AdditionalMetadata = &activitypb.Notification_DepositedCrypto{DepositedCrypto: &activitypb.DepositedCryptoNotificationMetadata{}}
				} else {
					notification.AdditionalMetadata = &activitypb.Notification_ReceivedCrypto{ReceivedCrypto: &activitypb.ReceivedCryptoNotificationMetadata{}}
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
				notification.AdditionalMetadata = &activitypb.Notification_BoughtCrypto{BoughtCrypto: &activitypb.BoughtCryptoNotificationMetadata{}}
			} else {
				notification.AdditionalMetadata = &activitypb.Notification_DepositedCrypto{DepositedCrypto: &activitypb.DepositedCryptoNotificationMetadata{}}
			}

		default:
			continue
		}

		if notification.PaymentAmount != nil {
			notification.PaymentAmount.Mint = &commonpb.PublicKey{Value: mintAccount.ToProto().Value}
		}

		notifications = append(notifications, notification)
	}

	for _, notification := range notifications {
		log := log.With(zap.String("notification_id", NotificationIDString(notification.Id)))

		err := InjectLocalizedText(ctx, s.ocpData, userOwnerAccount, notification)
		if err != nil {
			log.Warn("Failed to inject localized notification text", zap.Error(err))
			return nil, err
		}
	}
	return notifications, nil
}
