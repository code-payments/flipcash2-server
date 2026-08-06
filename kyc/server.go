package kyc

import (
	"context"
	"errors"
	"time"

	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	kycpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/kyc/v1"
	thirdpartypb "github.com/code-payments/flipcash2-protobuf-api/generated/go/thirdparty/v1"

	"github.com/code-payments/flipcash2-server/account"
	"github.com/code-payments/flipcash2-server/auth"
	"github.com/code-payments/flipcash2-server/model"
	"github.com/code-payments/flipcash2-server/offramp/bridge"
)

type Server struct {
	log *zap.Logger

	authz auth.Authorizer

	accounts account.Store
	store    Store

	bridgeClient *bridge.Client

	kycpb.UnimplementedKycServiceServer
}

func NewServer(
	log *zap.Logger,
	authz auth.Authorizer,
	accounts account.Store,
	store Store,
	bridgeClient *bridge.Client,
) *Server {
	return &Server{
		log: log,

		authz: authz,

		accounts: accounts,
		store:    store,

		bridgeClient: bridgeClient,
	}
}

func (s *Server) GetAgreementLinks(ctx context.Context, req *kycpb.GetAgreementLinksRequest) (*kycpb.GetAgreementLinksResponse, error) {
	userID, err := s.authz.Authorize(ctx, req, &req.Auth)
	if err != nil {
		return nil, err
	}

	log := s.log.With(
		zap.String("user_id", model.UserIDString(userID)),
		zap.String("partner", req.Partner.String()),
	)

	if req.Partner != thirdpartypb.Partner_BRIDGE {
		return nil, status.Error(codes.InvalidArgument, "unsupported partner")
	}
	if req.SubmissionType != kycpb.SubmissionType_INDIVIDUAL {
		return nil, status.Error(codes.InvalidArgument, "unsupported submission type")
	}

	isRegistered, err := s.accounts.IsRegistered(ctx, userID)
	if err != nil {
		log.Warn("Failure getting user registration status", zap.Error(err))
		return nil, status.Error(codes.Internal, "failure getting user registration status")
	}
	if !isRegistered {
		return &kycpb.GetAgreementLinksResponse{Result: kycpb.GetAgreementLinksResponse_DENIED}, nil
	}

	tosLink, err := s.bridgeClient.CreateTOSLink(ctx, bridge.TOSLinkIdempotencyKey(model.UserIDString(userID), time.Now()))
	if err != nil {
		log.Warn("Failed to create tos link", zap.Error(bridge.SanitizeError(err)))
		return nil, status.Error(codes.Internal, "failed to create agreement link")
	}

	return &kycpb.GetAgreementLinksResponse{
		Result: kycpb.GetAgreementLinksResponse_OK,
		Links: []*kycpb.AgreementLink{{
			Type: kycpb.AgreementLink_TOS,
			Url:  tosLink.URL,
		}},
	}, nil
}

func (s *Server) SubmitKyc(ctx context.Context, req *kycpb.SubmitKycRequest) (*kycpb.SubmitKycResponse, error) {
	userID, err := s.authz.Authorize(ctx, req, &req.Auth)
	if err != nil {
		return nil, err
	}

	log := s.log.With(
		zap.String("user_id", model.UserIDString(userID)),
		zap.String("partner", req.Partner.String()),
	)

	if req.Partner != thirdpartypb.Partner_BRIDGE {
		return nil, status.Error(codes.InvalidArgument, "unsupported partner")
	}
	submission := req.GetIndividual()
	if submission == nil {
		return nil, status.Error(codes.InvalidArgument, "unsupported submission type")
	}

	isRegistered, err := s.accounts.IsRegistered(ctx, userID)
	if err != nil {
		log.Warn("Failure getting user registration status", zap.Error(err))
		return nil, status.Error(codes.Internal, "failure getting user registration status")
	}
	if !isRegistered {
		return &kycpb.SubmitKycResponse{Result: kycpb.SubmitKycResponse_DENIED}, nil
	}

	// An existing submission reports its live state instead of resubmitting;
	// corrections go through UpdateKyc.
	record, err := s.store.Get(ctx, userID, req.Partner)
	if err == nil {
		customer, err := s.bridgeClient.GetCustomer(ctx, record.CustomerID)
		if err != nil {
			log.Warn("Failed to get customer", zap.Error(bridge.SanitizeError(err)))
			return nil, status.Error(codes.Internal, "failed to get verification state")
		}
		return &kycpb.SubmitKycResponse{Result: kycpb.SubmitKycResponse_EXISTING_SUBMISSION, KycState: bridge.KycStateFromCustomer(customer)}, nil
	} else if !errors.Is(err, ErrNotFound) {
		log.Warn("Failed to get kyc record", zap.Error(err))
		return nil, status.Error(codes.Internal, "failed to get kyc record")
	}

	createReq, err := bridge.CustomerRequestFromProtoSubmission(model.UserIDString(userID), submission)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	customer, err := s.bridgeClient.CreateCustomer(ctx, bridge.CustomerIdempotencyKey(model.UserIDString(userID), submission), createReq)
	if err != nil {
		if result, ok := bridge.SubmitKycResultFromError(err); ok {
			return &kycpb.SubmitKycResponse{Result: result}, nil
		}
		log.Warn("Failed to create customer", zap.Error(bridge.SanitizeError(err)))
		return nil, status.Error(codes.Internal, "failed to submit kyc")
	}

	if err := s.store.Create(ctx, &Record{
		UserID:     userID,
		Partner:    req.Partner,
		CustomerID: customer.ID,
		CreatedAt:  time.Now().UTC(),
	}); err != nil {
		if errors.Is(err, ErrExists) {
			// Lost a concurrent submit; the stored mapping wins. The loser is
			// the same customer whenever Bridge deduplicated on the
			// idempotency key, and an orphan (recoverable via
			// client_reference_id) otherwise.
			stored, getErr := s.store.Get(ctx, userID, req.Partner)
			if getErr != nil {
				log.Warn("Failed to get kyc record after losing concurrent submit", zap.Error(getErr))
				return nil, status.Error(codes.Internal, "failed to submit kyc")
			}
			if stored.CustomerID != customer.ID {
				log.Warn("Discarding customer from concurrent submit",
					zap.String("customer_id", customer.ID),
					zap.String("stored_customer_id", stored.CustomerID))
				customer, err = s.bridgeClient.GetCustomer(ctx, stored.CustomerID)
				if err != nil {
					log.Warn("Failed to get customer", zap.Error(bridge.SanitizeError(err)))
					return nil, status.Error(codes.Internal, "failed to get verification state")
				}
			}
		} else {
			// The Bridge customer exists but the mapping was not persisted;
			// surface loudly for reconciliation via client_reference_id.
			log.Error("Failed to persist kyc record for created customer",
				zap.String("customer_id", customer.ID),
				zap.Error(err))
			return nil, status.Error(codes.Internal, "failed to submit kyc")
		}
	}

	return &kycpb.SubmitKycResponse{
		Result:   kycpb.SubmitKycResponse_OK,
		KycState: bridge.KycStateFromCustomer(customer),
	}, nil
}

func (s *Server) UpdateKyc(ctx context.Context, req *kycpb.UpdateKycRequest) (*kycpb.UpdateKycResponse, error) {
	userID, err := s.authz.Authorize(ctx, req, &req.Auth)
	if err != nil {
		return nil, err
	}

	log := s.log.With(
		zap.String("user_id", model.UserIDString(userID)),
		zap.String("partner", req.Partner.String()),
	)

	if req.Partner != thirdpartypb.Partner_BRIDGE {
		return nil, status.Error(codes.InvalidArgument, "unsupported partner")
	}
	update := req.GetIndividual()
	if update == nil {
		return nil, status.Error(codes.InvalidArgument, "unsupported update type")
	}

	isRegistered, err := s.accounts.IsRegistered(ctx, userID)
	if err != nil {
		log.Warn("Failure getting user registration status", zap.Error(err))
		return nil, status.Error(codes.Internal, "failure getting user registration status")
	}
	if !isRegistered {
		return &kycpb.UpdateKycResponse{Result: kycpb.UpdateKycResponse_DENIED}, nil
	}

	record, err := s.store.Get(ctx, userID, req.Partner)
	if errors.Is(err, ErrNotFound) {
		return &kycpb.UpdateKycResponse{Result: kycpb.UpdateKycResponse_NOT_STARTED}, nil
	} else if err != nil {
		log.Warn("Failed to get kyc record", zap.Error(err))
		return nil, status.Error(codes.Internal, "failed to get kyc record")
	}

	if bridge.IsEmptyUpdate(update) {
		return &kycpb.UpdateKycResponse{Result: kycpb.UpdateKycResponse_NOTHING_TO_UPDATE}, nil
	}

	updateReq, err := bridge.UpdateCustomerRequestFromProtoUpdate(update)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	customer, err := s.bridgeClient.UpdateCustomer(ctx, record.CustomerID, updateReq)
	if err != nil {
		log.Warn("Failed to update customer", zap.Error(bridge.SanitizeError(err)))
		return nil, status.Error(codes.Internal, "failed to update kyc")
	}

	return &kycpb.UpdateKycResponse{
		Result:   kycpb.UpdateKycResponse_OK,
		KycState: bridge.KycStateFromCustomer(customer),
	}, nil
}

func (s *Server) GetKycStatus(ctx context.Context, req *kycpb.GetKycStatusRequest) (*kycpb.GetKycStatusResponse, error) {
	userID, err := s.authz.Authorize(ctx, req, &req.Auth)
	if err != nil {
		return nil, err
	}

	log := s.log.With(
		zap.String("user_id", model.UserIDString(userID)),
		zap.String("partner", req.Partner.String()),
	)

	if req.Partner != thirdpartypb.Partner_BRIDGE {
		return nil, status.Error(codes.InvalidArgument, "unsupported partner")
	}
	if req.SubmissionType != kycpb.SubmissionType_INDIVIDUAL {
		return nil, status.Error(codes.InvalidArgument, "unsupported submission type")
	}

	isRegistered, err := s.accounts.IsRegistered(ctx, userID)
	if err != nil {
		log.Warn("Failure getting user registration status", zap.Error(err))
		return nil, status.Error(codes.Internal, "failure getting user registration status")
	}
	if !isRegistered {
		return &kycpb.GetKycStatusResponse{Result: kycpb.GetKycStatusResponse_DENIED}, nil
	}

	record, err := s.store.Get(ctx, userID, req.Partner)
	if errors.Is(err, ErrNotFound) {
		return &kycpb.GetKycStatusResponse{Result: kycpb.GetKycStatusResponse_NOT_STARTED}, nil
	} else if err != nil {
		log.Warn("Failed to get kyc record", zap.Error(err))
		return nil, status.Error(codes.Internal, "failed to get kyc record")
	}

	customer, err := s.bridgeClient.GetCustomer(ctx, record.CustomerID)
	if err != nil {
		log.Warn("Failed to get customer", zap.Error(bridge.SanitizeError(err)))
		return nil, status.Error(codes.Internal, "failed to get verification state")
	}

	return &kycpb.GetKycStatusResponse{
		Result:   kycpb.GetKycStatusResponse_OK,
		KycState: bridge.KycStateFromCustomer(customer),
	}, nil
}
