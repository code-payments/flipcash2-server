package resolver

import (
	"context"
	"errors"

	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"
	resolverpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/resolver/v1"

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

	resolverpb.UnimplementedResolverServer
}

func NewServer(
	log *zap.Logger,
	authz auth.Authorizer,
	accounts account.Store,
	profiles profile.Store,
) *Server {
	return &Server{
		log:      log,
		authz:    authz,
		accounts: accounts,
		profiles: profiles,
	}
}

func (s *Server) Resolve(ctx context.Context, req *resolverpb.ResolveRequest) (*resolverpb.ResolveResponse, error) {
	userID, err := s.authz.Authorize(ctx, req, &req.Auth)
	if err != nil {
		return nil, err
	}

	log := s.log.With(zap.String("user_id", model.UserIDString(userID)))

	isRegistered, err := s.accounts.IsRegistered(ctx, userID)
	if err != nil {
		log.With(zap.Error(err)).Warn("Failure getting user registration status")
		return nil, status.Error(codes.Internal, "")
	}
	if !isRegistered {
		return &resolverpb.ResolveResponse{Result: resolverpb.ResolveResponse_DENIED}, nil
	}

	var targetUserID *commonpb.UserId
	switch typed := req.Identifier.GetKind().(type) {
	case *resolverpb.Identifier_Phone:
		targetUserID, err = s.profiles.GetUserIdByPhoneNumberForPayment(ctx, typed.Phone.Value)
		if errors.Is(err, profile.ErrNotFound) {
			return &resolverpb.ResolveResponse{Result: resolverpb.ResolveResponse_NOT_FOUND}, nil
		} else if err != nil {
			log.With(zap.Error(err)).Warn("Failure looking up user by phone number")
			return nil, status.Error(codes.Internal, "")
		}
	case *resolverpb.Identifier_UserId:
		targetUserID = typed.UserId
	default:
		return nil, status.Error(codes.InvalidArgument, "unsupported identifier")
	}

	pubKey, err := s.getPaymentAddress(ctx, log, targetUserID)
	if err != nil {
		return nil, err
	}
	if pubKey == nil {
		return &resolverpb.ResolveResponse{Result: resolverpb.ResolveResponse_NOT_FOUND}, nil
	}

	return &resolverpb.ResolveResponse{
		Result: resolverpb.ResolveResponse_OK,
		Resolution: &resolverpb.Resolution{
			Kind: &resolverpb.Resolution_Address{
				Address: pubKey,
			},
		},
	}, nil
}

func (s *Server) getPaymentAddress(ctx context.Context, log *zap.Logger, userID *commonpb.UserId) (*commonpb.PublicKey, error) {
	pubKeys, err := s.accounts.GetPubKeys(ctx, userID)
	if err != nil {
		log.With(zap.Error(err)).Warn("Failure getting public keys for resolved user")
		return nil, status.Error(codes.Internal, "")
	}
	if len(pubKeys) == 0 {
		return nil, nil
	}
	return pubKeys[0], nil
}
