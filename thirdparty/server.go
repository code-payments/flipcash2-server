package thirdparty

import (
	"context"
	"crypto/ed25519"

	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	thirdpartypb "github.com/code-payments/flipcash2-protobuf-api/generated/go/thirdparty/v1"

	"github.com/code-payments/flipcash2-server/account"
	"github.com/code-payments/flipcash2-server/auth"
	"github.com/code-payments/flipcash2-server/model"
	"github.com/code-payments/flipcash2-server/profile"
)

type Server struct {
	log *zap.Logger

	authz auth.Authorizer

	accounts account.Store
	profiles profile.Store

	coinbaseApiKey     string
	coinbasePrivateKey ed25519.PrivateKey

	thirdpartypb.UnimplementedThirdPartyServer
}

func NewServer(
	log *zap.Logger,
	authz auth.Authorizer,
	accounts account.Store,
	profiles profile.Store,
	coinbaseApiKey string,
	coinbasePrivateKey ed25519.PrivateKey,
) *Server {
	return &Server{
		log: log,

		authz: authz,

		accounts: accounts,
		profiles: profiles,

		coinbaseApiKey:     coinbaseApiKey,
		coinbasePrivateKey: coinbasePrivateKey,
	}
}

func (s *Server) GetJwt(ctx context.Context, req *thirdpartypb.GetJwtRequest) (*thirdpartypb.GetJwtResponse, error) {
	userID, err := s.authz.Authorize(ctx, req, &req.Auth)
	if err != nil {
		return nil, err
	}

	log := s.log.With(
		zap.String("user_id", model.UserIDString(userID)),
		zap.String("provider", req.ApiKey.Provider.String()),
		zap.String("method", req.Method),
		zap.String("host", req.Host),
		zap.String("path", req.Path),
	)

	isRegistered, err := s.accounts.IsRegistered(ctx, userID)
	if err != nil {
		log.With(zap.Error(err)).Warn("Failure getting user registration status")
		return nil, status.Error(codes.Internal, "failure getting user registration status")
	}
	if !isRegistered {
		return &thirdpartypb.GetJwtResponse{Result: thirdpartypb.GetJwtResponse_DENIED}, nil
	}

	var jwt string
	switch req.ApiKey.Provider {
	case thirdpartypb.Provider_COINBASE:
		if req.ApiKey.Value != s.coinbaseApiKey {
			return &thirdpartypb.GetJwtResponse{Result: thirdpartypb.GetJwtResponse_INVALID_API_KEY}, nil
		}

		userProfile, err := s.profiles.GetProfile(ctx, userID, true)
		if err == profile.ErrNotFound {
			return &thirdpartypb.GetJwtResponse{Result: thirdpartypb.GetJwtResponse_PHONE_VERIFICATION_REQUIRED}, nil
		} else if err != nil {
			log.With(zap.Error(err)).Warn("Failed to get user profile")
			return nil, status.Error(codes.Internal, "failed to get user profile")
		}

		if userProfile.PhoneNumber == nil {
			return &thirdpartypb.GetJwtResponse{Result: thirdpartypb.GetJwtResponse_PHONE_VERIFICATION_REQUIRED}, nil
		}
		if userProfile.EmailAddress == nil {
			return &thirdpartypb.GetJwtResponse{Result: thirdpartypb.GetJwtResponse_EMAIL_VERIFICATION_REQUIRED}, nil
		}

		jwt, err = getCoinbaseJwt(
			req.Method,
			req.Host,
			req.Path,
			s.coinbaseApiKey,
			s.coinbasePrivateKey,
		)
		if err != nil {
			log.With(zap.Error(err)).Warn("Failed to generate jwt")
			return nil, status.Error(codes.Internal, "failed to generate jwt")
		}
	default:
		return &thirdpartypb.GetJwtResponse{Result: thirdpartypb.GetJwtResponse_UNSUPPORTED_PROVIDER}, nil
	}

	return &thirdpartypb.GetJwtResponse{Jwt: &thirdpartypb.Jwt{Value: jwt}}, nil
}
