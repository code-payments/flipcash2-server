package phone

import (
	"bytes"
	"context"
	"errors"

	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"
	phonepb "github.com/code-payments/flipcash2-protobuf-api/generated/go/phone/v1"

	"github.com/code-payments/flipcash2-server/account"
	"github.com/code-payments/flipcash2-server/auth"
	"github.com/code-payments/flipcash2-server/contact"
	"github.com/code-payments/flipcash2-server/model"
	"github.com/code-payments/flipcash2-server/profile"
	"github.com/code-payments/flipcash2-server/push"
)

const (
	androidAppHash  = "todo"
	mockPhoneNumber = "+15005550000"
)

type Server struct {
	log *zap.Logger

	authz auth.Authorizer

	accounts account.Store
	profiles profile.Store
	contacts contact.Store

	verifier Verifier
	pusher   push.Pusher

	hashPepper []byte

	phonepb.UnimplementedPhoneVerificationServer
}

func NewServer(
	log *zap.Logger,
	authz auth.Authorizer,
	accounts account.Store,
	profiles profile.Store,
	contacts contact.Store,
	verifier Verifier,
	pusher push.Pusher,
	hashPepper []byte,
) *Server {
	return &Server{
		log: log,

		authz: authz,

		accounts: accounts,
		profiles: profiles,
		contacts: contacts,

		verifier: verifier,
		pusher:   pusher,

		hashPepper: hashPepper,
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

	if req.PhoneNumber.Value == mockPhoneNumber {
		return &phonepb.SendVerificationCodeResponse{Result: phonepb.SendVerificationCodeResponse_OK}, nil
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

	if req.PhoneNumber.Value == mockPhoneNumber {
		return &phonepb.CheckVerificationCodeResponse{Result: phonepb.CheckVerificationCodeResponse_OK}, nil
	}

	var result phonepb.CheckVerificationCodeResponse_Result
	err = s.verifier.Check(ctx, req.PhoneNumber.Value, req.Code.Value)
	switch err {
	case nil:
		result = phonepb.CheckVerificationCodeResponse_OK

		phoneHash := s.hashPhoneNumber(req.PhoneNumber)
		err = s.profiles.LinkPhoneNumber(ctx, userID, req.PhoneNumber.Value, phoneHash)
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

	isStaff, err := s.accounts.IsStaff(ctx, userID)
	if err != nil {
		log.With(zap.Error(err)).Warn("Failure getting user staff status")
		return nil, status.Error(codes.Internal, "failure getting user staff status")
	}
	if !isStaff {
		return &phonepb.UnlinkResponse{Result: phonepb.UnlinkResponse_DENIED}, nil
	}

	err = s.profiles.UnlinkPhoneNumber(ctx, userID, req.PhoneNumber.Value)
	if err != nil {
		log.With(zap.Error(err)).Warn("Failure unlinking email address")
		return nil, status.Error(codes.Internal, "failure unlinking email address")
	}

	return &phonepb.UnlinkResponse{}, nil
}

func (s *Server) LinkForPayment(ctx context.Context, req *phonepb.LinkForPaymentRequest) (*phonepb.LinkForPaymentResponse, error) {
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
		return &phonepb.LinkForPaymentResponse{Result: phonepb.LinkForPaymentResponse_DENIED}, nil
	}

	flipped, err := s.profiles.LinkPhoneNumberForPayment(ctx, userID, req.PhoneNumber.Value)
	switch {
	case err == nil:
		if flipped {
			phoneHash := s.hashPhoneNumber(req.PhoneNumber)
			go s.notifyContactsOfJoin(context.Background(), log, userID, req.PhoneNumber, phoneHash)
		}
		return &phonepb.LinkForPaymentResponse{Result: phonepb.LinkForPaymentResponse_OK}, nil
	case errors.Is(err, profile.ErrNotFound):
		return &phonepb.LinkForPaymentResponse{Result: phonepb.LinkForPaymentResponse_NOT_ASSOCIATED}, nil
	default:
		log.With(zap.Error(err)).Warn("Failure linking phone number for payment")
		return nil, status.Error(codes.Internal, "failure linking phone number for payment")
	}
}

func (s *Server) hashPhoneNumber(phoneNumber *phonepb.PhoneNumber) *commonpb.Hash {
	return SecureHash(phoneNumber, s.hashPepper)
}

func (s *Server) notifyContactsOfJoin(
	ctx context.Context,
	log *zap.Logger,
	joiningUserID *commonpb.UserId,
	phoneNumber *phonepb.PhoneNumber,
	phoneHash *commonpb.Hash,
) {
	recipients, err := s.contacts.GetUserIdsByPhoneHash(ctx, phoneHash)
	if err != nil {
		log.With(zap.Error(err)).Warn("Failure looking up contact list owners for join notification")
		return
	}

	var filtered []*commonpb.UserId
	for _, r := range recipients {
		if bytes.Equal(r.Value, joiningUserID.Value) {
			continue
		}
		filtered = append(filtered, r)
	}
	if len(filtered) == 0 {
		return
	}

	const batchSize = 1000
	for i := 0; i < len(filtered); i += batchSize {
		end := min(i+batchSize, len(filtered))
		if err := push.SendContactJoinedFlipcashPush(ctx, s.pusher, phoneNumber, filtered[i:end]...); err != nil {
			log.With(zap.Error(err)).Warn("Failure sending contact joined Flipcash push")
		}
	}
}
