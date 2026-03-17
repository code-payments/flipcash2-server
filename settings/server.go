package settings

import (
	"context"
	"errors"

	"go.uber.org/zap"
	"golang.org/x/text/language"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	settingspb "github.com/code-payments/flipcash2-protobuf-api/generated/go/settings/v1"

	"github.com/code-payments/flipcash2-server/auth"
	"github.com/code-payments/flipcash2-server/model"
)

type Server struct {
	log   *zap.Logger
	authz auth.Authorizer
	store Store

	settingspb.UnimplementedSettingsServer
}

func NewServer(log *zap.Logger, authz auth.Authorizer, store Store) *Server {
	return &Server{
		log:   log,
		authz: authz,
		store: store,
	}
}

func (s *Server) UpdateSettings(ctx context.Context, req *settingspb.UpdateSettingsRequest) (*settingspb.UpdateSettingsResponse, error) {
	userID, err := s.authz.Authorize(ctx, req, &req.Auth)
	if err != nil {
		return nil, err
	}

	log := s.log.With(zap.String("user_id", model.UserIDString(userID)))

	if req.Locale != nil {
		if _, err := language.Parse(req.Locale.Value); err != nil {
			return &settingspb.UpdateSettingsResponse{Result: settingspb.UpdateSettingsResponse_INVALID_LOCALE}, nil
		}

		if err := s.store.SetLocale(ctx, userID, req.Locale); err != nil {
			if errors.Is(err, ErrNotFound) {
				return &settingspb.UpdateSettingsResponse{Result: settingspb.UpdateSettingsResponse_DENIED}, nil
			}
			log.Warn("Failed to set locale", zap.Error(err))
			return nil, status.Error(codes.Internal, "failed to set locale")
		}
	}

	if req.Region != nil {
		if len(req.Region.Value) == 0 {
			return &settingspb.UpdateSettingsResponse{Result: settingspb.UpdateSettingsResponse_INVALID_REGION}, nil
		}

		if err := s.store.SetRegion(ctx, userID, req.Region); err != nil {
			if errors.Is(err, ErrNotFound) {
				return &settingspb.UpdateSettingsResponse{Result: settingspb.UpdateSettingsResponse_DENIED}, nil
			}
			log.Warn("Failed to set region", zap.Error(err))
			return nil, status.Error(codes.Internal, "failed to set region")
		}
	}

	return &settingspb.UpdateSettingsResponse{Result: settingspb.UpdateSettingsResponse_OK}, nil
}
