package phone

import (
	"context"

	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	phonepb "github.com/code-payments/flipcash2-protobuf-api/generated/go/phone/v1"

	"github.com/code-payments/flipcash2-server/account"
	"github.com/code-payments/flipcash2-server/auth"
	"github.com/code-payments/flipcash2-server/model"
	"github.com/code-payments/flipcash2-server/profile"
)

const (
	androidAppHash = "todo"
)

type Server struct {
	log *zap.Logger

	authz auth.Authorizer

	accounts account.Store
	profiles profile.Store

	verifier Verifier

	phonepb.UnimplementedPhoneVerificationServer
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

func (s *Server) SendVerificationCode(ctx context.Context, req *phonepb.SendVerificationCodeRequest) (*phonepb.SendVerificationCodeResponse, error) {
	userID, err := s.authz.Authorize(ctx, req, &req.Auth)
	if err != nil {
		return nil, err
	}

	log := s.log.With(
		zap.String("user_id", model.UserIDString(userID)),
		zap.String("phone_number", req.PhoneNumber.Value),
		zap.String("platform", req.Platform.String()),
	)

	isRegistered, err := s.accounts.IsRegistered(ctx, userID)
	if err != nil {
		log.With(zap.Error(err)).Warn("Failure getting user registration status")
		return nil, status.Error(codes.Internal, "failure getting user registration status")
	}
	if !isRegistered {
		return &phonepb.SendVerificationCodeResponse{Result: phonepb.SendVerificationCodeResponse_DENIED}, nil
	}

	var result phonepb.SendVerificationCodeResponse_Result
	_, _, err = s.verifier.SendCode(ctx, req.PhoneNumber.Value, nil) // todo: Send app hash when platform is GOOGLE
	switch err {
	case nil:
		result = phonepb.SendVerificationCodeResponse_OK
	case ErrInvalidNumber:
		result = phonepb.SendVerificationCodeResponse_INVALID_PHONE_NUMBER
	case ErrUnsupportedPhoneType:
		result = phonepb.SendVerificationCodeResponse_UNSUPPORTED_PHONE_TYPE
	case ErrRateLimited:
		result = phonepb.SendVerificationCodeResponse_RATE_LIMITED
	default:
		log.With(zap.Error(err)).Warn("Failure sending verification code")
		return nil, status.Error(codes.Internal, "failure sending verification code")
	}

	return &phonepb.SendVerificationCodeResponse{Result: result}, nil
}

func (s *Server) CheckVerificationCode(ctx context.Context, req *phonepb.CheckVerificationCodeRequest) (*phonepb.CheckVerificationCodeResponse, error) {
	userID, err := s.authz.Authorize(ctx, req, &req.Auth)
	if err != nil {
		return nil, err
	}

	log := s.log.With(
		zap.String("user_id", model.UserIDString(userID)),
		zap.String("phone_number", req.PhoneNumber.Value),
	)

	isRegistered, err := s.accounts.IsRegistered(ctx, userID)
	if err != nil {
		log.With(zap.Error(err)).Warn("Failure getting user registration status")
		return nil, status.Error(codes.Internal, "failure getting user registration status")
	}
	if !isRegistered {
		return &phonepb.CheckVerificationCodeResponse{Result: phonepb.CheckVerificationCodeResponse_DENIED}, nil
	}

	var result phonepb.CheckVerificationCodeResponse_Result
	err = s.verifier.Check(ctx, req.PhoneNumber.Value, req.Code.Value)
	switch err {
	case nil:
		result = phonepb.CheckVerificationCodeResponse_OK

		err = s.profiles.SetPhoneNumber(ctx, userID, req.PhoneNumber.Value)
		if err != nil {
			log.With(zap.Error(err)).Warn("Failure linking phone number")
			return nil, status.Error(codes.Internal, "failure linking phone number")
		}
	case ErrInvalidVerificationCode:
		result = phonepb.CheckVerificationCodeResponse_INVALID_CODE
	case ErrNoVerification:
		result = phonepb.CheckVerificationCodeResponse_NO_VERIFICATION
	default:
		log.With(zap.Error(err)).Warn("Failure checking verification code")
		return nil, status.Error(codes.Internal, "failure checking verification code")
	}

	return &phonepb.CheckVerificationCodeResponse{
		Result: result,
	}, nil
}

func (s *Server) Unlink(ctx context.Context, req *phonepb.UnlinkRequest) (*phonepb.UnlinkResponse, error) {
	userID, err := s.authz.Authorize(ctx, req, &req.Auth)
	if err != nil {
		return nil, err
	}

	log := s.log.With(
		zap.String("user_id", model.UserIDString(userID)),
		zap.String("phone_number", req.PhoneNumber.Value),
	)

	isRegistered, err := s.accounts.IsRegistered(ctx, userID)
	if err != nil {
		log.With(zap.Error(err)).Warn("Failure getting user registration status")
		return nil, status.Error(codes.Internal, "failure getting user registration status")
	}
	if !isRegistered {
		return &phonepb.UnlinkResponse{Result: phonepb.UnlinkResponse_DENIED}, nil
	}

	err = s.profiles.UnlinkPhoneNumber(ctx, userID, req.PhoneNumber.Value)
	if err != nil {
		log.With(zap.Error(err)).Warn("Failure unlinking email address")
		return nil, status.Error(codes.Internal, "failure unlinking email address")
	}

	return &phonepb.UnlinkResponse{}, nil
}
