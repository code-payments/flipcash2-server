package profile

import (
	"bytes"
	"context"
	"errors"
	"strings"

	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"
	profilepb "github.com/code-payments/flipcash2-protobuf-api/generated/go/profile/v1"

	"github.com/code-payments/flipcash2-server/account"
	"github.com/code-payments/flipcash2-server/auth"
	"github.com/code-payments/flipcash2-server/model"
	"github.com/code-payments/flipcash2-server/social/x"
)

type Server struct {
	log *zap.Logger

	authz auth.Authorizer

	accounts account.Store
	profiles Store

	xClient *x.Client

	profilepb.UnimplementedProfileServer
}

func NewServer(log *zap.Logger, authz auth.Authorizer, accounts account.Store, profiles Store, xClient *x.Client) *Server {
	return &Server{
		log: log,

		authz: authz,

		accounts: accounts,
		profiles: profiles,

		xClient: xClient,
	}
}

func (s *Server) GetProfile(ctx context.Context, req *profilepb.GetProfileRequest) (*profilepb.GetProfileResponse, error) {
	log := s.log.With(zap.String("user_id", model.UserIDString(req.UserId)))

	var requestingUserID *commonpb.UserId
	var err error
	if req.Auth != nil {
		requestingUserID, err = s.authz.Authorize(ctx, req, &req.Auth)
		if err != nil {
			return nil, err
		}
		log = s.log.With(zap.String("requesting_user_id", model.UserIDString(req.UserId)))
	}

	includePrivateFields := requestingUserID != nil && bytes.Equal(req.UserId.Value, requestingUserID.Value)

	profile, err := s.profiles.GetProfile(ctx, req.UserId, includePrivateFields)
	if errors.Is(err, ErrNotFound) {
		return &profilepb.GetProfileResponse{Result: profilepb.GetProfileResponse_NOT_FOUND}, nil
	} else if err != nil {
		log.Warn("Failed to get profile", zap.Error(err))
		return nil, status.Error(codes.Internal, "failed to get profile")
	}

	return &profilepb.GetProfileResponse{UserProfile: profile}, nil
}

func (s *Server) SetDisplayName(ctx context.Context, req *profilepb.SetDisplayNameRequest) (*profilepb.SetDisplayNameResponse, error) {
	userID, err := s.authz.Authorize(ctx, req, &req.Auth)
	if err != nil {
		return nil, err
	}

	log := s.log.With(
		zap.String("user_id", model.UserIDString(userID)),
		zap.String("display_name", req.DisplayName),
	)

	isRegistered, err := s.accounts.IsRegistered(ctx, userID)
	if err != nil {
		log.Info("Failed to get registration flag")
		return nil, status.Errorf(codes.Internal, "failed to get registration flag")
	} else if !isRegistered {
		return &profilepb.SetDisplayNameResponse{Result: profilepb.SetDisplayNameResponse_DENIED}, nil
	}

	if err := s.profiles.SetDisplayName(ctx, userID, req.DisplayName); err != nil {
		if errors.Is(err, ErrInvalidDisplayName) {
			log.Info("Invalid display name")
			return nil, status.Error(codes.InvalidArgument, "invalid display name")
		}

		s.log.Warn("Failed to set display name", zap.Error(err))
		return nil, status.Error(codes.Internal, "failed to set display name")
	}

	return &profilepb.SetDisplayNameResponse{}, nil
}

func (s *Server) LinkSocialAccount(ctx context.Context, req *profilepb.LinkSocialAccountRequest) (*profilepb.LinkSocialAccountResponse, error) {
	userID, err := s.authz.Authorize(ctx, req, &req.Auth)
	if err != nil {
		return nil, err
	}

	log := s.log.With(zap.String("user_id", model.UserIDString(userID)))

	isRegistered, err := s.accounts.IsRegistered(ctx, userID)
	if err != nil {
		log.Info("Failed to get registration flag")
		return nil, status.Errorf(codes.Internal, "failed to get registration flag")
	} else if !isRegistered {
		return &profilepb.LinkSocialAccountResponse{Result: profilepb.LinkSocialAccountResponse_DENIED}, nil
	}

	switch typed := req.LinkingToken.Type.(type) {
	case *profilepb.LinkSocialAccountRequest_LinkingToken_X:
		log = log.With(zap.String("social_account_type", "x"))

		xUser, err := s.xClient.GetMyUser(ctx, typed.X.AccessToken)
		if err != nil {
			if strings.Contains(strings.ToLower(err.Error()), "http status code: 403") {
				return &profilepb.LinkSocialAccountResponse{Result: profilepb.LinkSocialAccountResponse_INVALID_LINKING_TOKEN}, nil
			}

			log.Warn("Failed to get user from x", zap.Error(err))
			return nil, status.Error(codes.Internal, "failed to get user from x")
		}

		protoXUser := xUser.ToProto()

		if err := protoXUser.Validate(); err != nil {
			log.Warn("Failed to validate proto profile", zap.Error(err))
			return nil, status.Error(codes.Internal, "failed to validate proto profile")
		}

		err = s.profiles.LinkXAccount(ctx, userID, protoXUser, typed.X.AccessToken)
		switch err {
		case nil:
		case ErrExistingSocialLink:
			return &profilepb.LinkSocialAccountResponse{Result: profilepb.LinkSocialAccountResponse_EXISTING_LINK}, nil
		default:
			log.Warn("failed to link account", zap.Error(err))
			return nil, status.Error(codes.Internal, "failed to link account")
		}

		return &profilepb.LinkSocialAccountResponse{SocialProfile: &profilepb.SocialProfile{
			Type: &profilepb.SocialProfile_X{
				X: protoXUser,
			},
		}}, nil
	default:
		return nil, status.Error(codes.Unimplemented, "unsupported linking token type")
	}
}

func (s *Server) UnlinkSocialAccount(ctx context.Context, req *profilepb.UnlinkSocialAccountRequest) (*profilepb.UnlinkSocialAccountResponse, error) {
	userID, err := s.authz.Authorize(ctx, req, &req.Auth)
	if err != nil {
		return nil, err
	}

	log := s.log.With(zap.String("user_id", model.UserIDString(userID)))

	switch typed := req.SocialIdentifier.(type) {
	case *profilepb.UnlinkSocialAccountRequest_XUserId:
		log = log.With(zap.String("x_user_id", typed.XUserId))

		err = s.profiles.UnlinkXAccount(ctx, userID, typed.XUserId)
		if err == ErrNotFound {
			return &profilepb.UnlinkSocialAccountResponse{Result: profilepb.UnlinkSocialAccountResponse_DENIED}, nil
		} else if err != nil {
			log.Warn("Failed to unlink account", zap.Error(err))
			return nil, status.Error(codes.Internal, "failed to unlink account")
		}

		return &profilepb.UnlinkSocialAccountResponse{}, nil
	default:
		return nil, status.Error(codes.Unimplemented, "unsupported social identifier")
	}
}
