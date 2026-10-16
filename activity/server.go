package activity

import (
	"context"
	"errors"

	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	activitypb "github.com/code-payments/flipcash2-protobuf-api/generated/go/activity/v1"
	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"

	"github.com/code-payments/flipcash2-server/account"
	"github.com/code-payments/flipcash2-server/auth"
	"github.com/code-payments/flipcash2-server/model"
	ocp_data "github.com/code-payments/ocp-server/ocp/data"
)

const (
	defaultMaxNotifications = 100
)

var (
	errNotificationNotFound = errors.New("notification not found")
	errInvalidPagingToken   = errors.New("paging token is invalid")
)

type Server struct {
	log *zap.Logger

	authz auth.Authorizer

	accounts account.Store

	ocpData ocp_data.Provider

	activitypb.UnimplementedActivityFeedServer
}

func NewServer(
	log *zap.Logger,
	authz auth.Authorizer,
	accounts account.Store,
	ocpData ocp_data.Provider,
) *Server {
	return &Server{
		log: log,

		authz: authz,

		accounts: accounts,

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

	notifications, err := s.getPagedNotifications(ctx, log, req.Auth.GetKeyPair().PubKey, &commonpb.QueryOptions{
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

	notifications, err := s.getPagedNotifications(ctx, log, req.Auth.GetKeyPair().PubKey, req.QueryOptions)
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

	notifications, err := s.getBatchNotifications(ctx, log, req.Auth.GetKeyPair().PubKey, req.Ids)
	switch err {
	case nil:
		return &activitypb.GetBatchNotificationsResponse{Notifications: notifications}, nil
	case errNotificationNotFound:
		return &activitypb.GetBatchNotificationsResponse{Result: activitypb.GetBatchNotificationsResponse_NOT_FOUND}, nil
	default:
		return nil, status.Error(codes.Internal, "")
	}
}

func (s *Server) getPagedNotifications(ctx context.Context, log *zap.Logger, pubKey *commonpb.PublicKey, queryOptions *commonpb.QueryOptions) ([]*activitypb.Notification, error) {
	limit := defaultMaxNotifications
	if queryOptions.PageSize > 0 {
		limit = int(queryOptions.PageSize)
	}

	notifications, err := s.getPagedNotificationsFromHistory(ctx, pubKey, queryOptions, limit)
	if err != nil {
		log.Warn("Failed to get notifications from transaction history", zap.Error(err))
		return nil, err
	}
	return notifications, nil
}

func (s *Server) getBatchNotifications(ctx context.Context, log *zap.Logger, pubKey *commonpb.PublicKey, ids []*activitypb.NotificationId) ([]*activitypb.Notification, error) {
	notifications, err := s.getBatchNotificationsFromHistory(ctx, pubKey, ids)
	if err != nil && err != errNotificationNotFound {
		log.Warn("Failed to get notifications from transaction history", zap.Error(err))
	}
	return notifications, err
}
