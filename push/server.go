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
	pusher Pusher

	pushpb.UnimplementedPushServer
}

func NewServer(log *zap.Logger, authz auth.Authorizer, tokens TokenStore, pusher Pusher) *Server {
	return &Server{
		log:    log,
		authz:  authz,
		tokens: tokens,
		pusher: pusher,
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

	// Only the caller's own token for the install goes: another user's token
	// for the same install or with the same value is theirs to remove.
	if err := s.tokens.DeleteToken(ctx, userID, req.AppInstall); err != nil {
		s.log.Warn("Failed to delete push token",
			zap.String("user_id", model.UserIDString(userID)),
			zap.String("app_install", req.AppInstall.Value),
			zap.Error(err),
		)
		return nil, status.Error(codes.Internal, "")
	}

	return &pushpb.DeleteTokensResponse{}, nil
}

// SendTestPush sends the test notification to the caller's own devices. Any
// authenticated user may call it; it reaches no one else.
func (s *Server) SendTestPush(ctx context.Context, req *pushpb.SendTestPushRequest) (*pushpb.SendTestPushResponse, error) {
	userID, err := s.authz.Authorize(ctx, req, &req.Auth)
	if err != nil {
		return nil, err
	}

	if err = SendTestPush(ctx, s.pusher, userID); err != nil {
		s.log.Warn("Failed to send test push", zap.String("user_id", model.UserIDString(userID)), zap.Error(err))
		return nil, status.Error(codes.Internal, "")
	}

	return &pushpb.SendTestPushResponse{}, nil
}
