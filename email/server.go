package email

import (
	"context"

	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	emailpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/email/v1"

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

	verifier Verifier

	emailpb.UnimplementedEmailVerificationServer
}

func NewServer(
	log *zap.Logger,
	authz auth.Authorizer,
	accounts account.Store,
	profiles profile.Store,
	verifier Verifier,
) *Server {
	return &Server{
		log: log,

		authz: authz,

		accounts: accounts,
		profiles: profiles,

		verifier: verifier,
	}
}

func (s *Server) SendVerificationCode(ctx context.Context, req *emailpb.SendVerificationCodeRequest) (*emailpb.SendVerificationCodeResponse, error) {
	userID, err := s.authz.Authorize(ctx, req, &req.Auth)
	if err != nil {
		return nil, err
	}

	log := s.log.With(
		zap.String("user_id", model.UserIDString(userID)),
		zap.String("email_address", req.EmailAddress.Value),
	)

	isRegistered, err := s.accounts.IsRegistered(ctx, userID)
	if err != nil {
		log.With(zap.Error(err)).Warn("Failure getting user registration status")
		return nil, status.Error(codes.Internal, "failure getting user registration status")
	}
	if !isRegistered {
		return &emailpb.SendVerificationCodeResponse{Result: emailpb.SendVerificationCodeResponse_DENIED}, nil
	}

	var result emailpb.SendVerificationCodeResponse_Result
	_, err = s.verifier.SendCode(ctx, req.EmailAddress.Value, req.ClientData)
	switch err {
	case nil:
		result = emailpb.SendVerificationCodeResponse_OK
	case ErrInvalidEmail:
		result = emailpb.SendVerificationCodeResponse_INVALID_EMAIL_ADDRESS
	case ErrRateLimited:
		result = emailpb.SendVerificationCodeResponse_RATE_LIMITED
	default:
		log.With(zap.Error(err)).Warn("Failure sending verification code")
		return nil, status.Error(codes.Internal, "failure sending verification code")
	}

	return &emailpb.SendVerificationCodeResponse{Result: result}, nil
}

func (s *Server) CheckVerificationCode(ctx context.Context, req *emailpb.CheckVerificationCodeRequest) (*emailpb.CheckVerificationCodeResponse, error) {
	userID, err := s.authz.Authorize(ctx, req, &req.Auth)
	if err != nil {
		return nil, err
	}

	log := s.log.With(
		zap.String("user_id", model.UserIDString(userID)),
		zap.String("email_address", req.EmailAddress.Value),
	)

	isRegistered, err := s.accounts.IsRegistered(ctx, userID)
	if err != nil {
		log.With(zap.Error(err)).Warn("Failure getting user registration status")
		return nil, status.Error(codes.Internal, "failure getting user registration status")
	}
	if !isRegistered {
		return &emailpb.CheckVerificationCodeResponse{Result: emailpb.CheckVerificationCodeResponse_DENIED}, nil
	}

	var result emailpb.CheckVerificationCodeResponse_Result
	err = s.verifier.Check(ctx, req.EmailAddress.Value, req.Code.Value)
	switch err {
	case nil:
		result = emailpb.CheckVerificationCodeResponse_OK

		err = s.profiles.SetEmailAddress(ctx, userID, req.EmailAddress.Value)
		if err != nil {
			log.With(zap.Error(err)).Warn("Failure linking email address")
			return nil, status.Error(codes.Internal, "failure linking email address")
		}
	case ErrInvalidVerificationCode:
		result = emailpb.CheckVerificationCodeResponse_INVALID_CODE
	case ErrNoVerification:
		result = emailpb.CheckVerificationCodeResponse_NO_VERIFICATION
	default:
		log.With(zap.Error(err)).Warn("Failure checking verification code")
		return nil, status.Error(codes.Internal, "failure checking verification code")
	}

	return &emailpb.CheckVerificationCodeResponse{
		Result: result,
	}, nil
}

func (s *Server) Unlink(ctx context.Context, req *emailpb.UnlinkRequest) (*emailpb.UnlinkResponse, error) {
	userID, err := s.authz.Authorize(ctx, req, &req.Auth)
	if err != nil {
		return nil, err
	}

	log := s.log.With(
		zap.String("user_id", model.UserIDString(userID)),
		zap.String("email_address", req.EmailAddress.Value),
	)

	isRegistered, err := s.accounts.IsRegistered(ctx, userID)
	if err != nil {
		log.With(zap.Error(err)).Warn("Failure getting user registration status")
		return nil, status.Error(codes.Internal, "failure getting user registration status")
	}
	if !isRegistered {
		return &emailpb.UnlinkResponse{Result: emailpb.UnlinkResponse_DENIED}, nil
	}

	err = s.profiles.UnlinkEmailAddress(ctx, userID, req.EmailAddress.Value)
	if err != nil {
		log.With(zap.Error(err)).Warn("Failure unlinking email address")
		return nil, status.Error(codes.Internal, "failure unlinking email address")
	}

	return &emailpb.UnlinkResponse{}, nil
}
