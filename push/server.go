package push

import (
	"context"

	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pushpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/push/v1"

	"github.com/code-payments/flipcash2-server/auth"
	"github.com/code-payments/flipcash2-server/model"
)

type Server struct {
	log    *zap.Logger
	authz  auth.Authorizer
	tokens TokenStore

	pushpb.UnimplementedPushServer
}

func NewServer(log *zap.Logger, authz auth.Authorizer, tokens TokenStore) *Server {
	return &Server{
		log:    log,
		authz:  authz,
		tokens: tokens,
	}
}

func (s *Server) AddToken(ctx context.Context, req *pushpb.AddTokenRequest) (*pushpb.AddTokenResponse, error) {
	userID, err := s.authz.Authorize(ctx, req, &req.Auth)
	if err != nil {
		return nil, err
	}

	if err = s.tokens.AddToken(ctx, userID, req.AppInstall, req.TokenType, req.PushToken); err != nil {
		s.log.Warn("Failed to add push token", zap.String("user_id", model.UserIDString(userID)), zap.Error(err))
		return nil, status.Errorf(codes.Internal, "")
	}

	return &pushpb.AddTokenResponse{}, nil
}

func (s *Server) DeleteTokens(ctx context.Context, req *pushpb.DeleteTokensRequest) (*pushpb.DeleteTokensResponse, error) {
	userID, err := s.authz.Authorize(ctx, req, &req.Auth)
	if err != nil {
		return nil, err
	}

	log := s.log.With(zap.String("user_id", model.UserIDString(userID)), zap.String("app_install", req.AppInstall.Value))

	pushTokens, err := s.tokens.GetTokens(ctx, userID)
	if err != nil {
		log.Warn("Failed to get push tokens", zap.Error(err))
		return nil, status.Error(codes.Internal, "")
	}

	for _, pushToken := range pushTokens {
		log = log.With(zap.String("push_token", pushToken.Token))

		if pushToken.AppInstallID != req.AppInstall.Value {
			continue
		}

		if err = s.tokens.DeleteToken(ctx, pushToken.Type, pushToken.Token); err != nil {
			log.Warn("Failed to get delete push token", zap.Error(err))
			return nil, status.Error(codes.Internal, "failed to delete push token")
		}
	}

	return &pushpb.DeleteTokensResponse{}, nil
}
