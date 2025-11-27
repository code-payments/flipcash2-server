package iap

import (
	"bytes"
	"context"
	"encoding/base64"
	"strings"
	"time"

	"github.com/pkg/errors"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"
	iappb "github.com/code-payments/flipcash2-protobuf-api/generated/go/iap/v1"

	"github.com/code-payments/flipcash2-server/account"
	"github.com/code-payments/flipcash2-server/auth"
	"github.com/code-payments/flipcash2-server/database"
	"github.com/code-payments/flipcash2-server/model"
)

const (
	CreateAccountProductID     = "com.flipcash.iap.createAccount"
	CreateAccountBonusGoogleID = "com.flipcash.iap.createAccountWithWelcomeBonus"
	CreateAccountBonusAppleID  = "com.flipcash.iap.createAccountBonus"
)

type Server struct {
	log            *zap.Logger
	authz          auth.Authorizer
	accounts       account.Store
	iaps           Store
	appleVerifier  Verifier
	googleVerifier Verifier

	iappb.UnimplementedIapServer
}

func NewServer(
	log *zap.Logger,
	authz auth.Authorizer,
	accounts account.Store,
	iaps Store,
	appleVerifier Verifier,
	googleVerifier Verifier,
) *Server {
	return &Server{
		log:            log,
		authz:          authz,
		accounts:       accounts,
		iaps:           iaps,
		appleVerifier:  appleVerifier,
		googleVerifier: googleVerifier,
	}
}

func (s *Server) OnPurchaseCompleted(ctx context.Context, req *iappb.OnPurchaseCompletedRequest) (*iappb.OnPurchaseCompletedResponse, error) {
	userID, err := s.authz.Authorize(ctx, req, &req.Auth)
	if err != nil {
		return nil, err
	}

	var verifier Verifier
	switch req.Platform {
	case commonpb.Platform_APPLE:
		verifier = s.appleVerifier
	case commonpb.Platform_GOOGLE:
		verifier = s.googleVerifier
	default:
		return &iappb.OnPurchaseCompletedResponse{Result: iappb.OnPurchaseCompletedResponse_DENIED}, nil
	}

	log := s.log.With(
		zap.String("user_id", model.UserIDString(userID)),
		zap.String("platform", req.Platform.String()),
		zap.String("receipt", req.Receipt.Value),
		zap.String("product", req.Metadata.Product),
	)

	log.Debug("Got a receipt")

	var product Product
	switch req.Metadata.Product {
	case CreateAccountProductID, strings.ToLower(CreateAccountProductID):
		product = ProductCreateAccount
	case CreateAccountBonusGoogleID, strings.ToLower(CreateAccountBonusGoogleID):
		if req.Platform == commonpb.Platform_APPLE {
			return &iappb.OnPurchaseCompletedResponse{Result: iappb.OnPurchaseCompletedResponse_INVALID_METADATA}, nil
		}

		// Product no longer supported
		return &iappb.OnPurchaseCompletedResponse{Result: iappb.OnPurchaseCompletedResponse_DENIED}, nil
	case CreateAccountBonusAppleID, strings.ToLower(CreateAccountBonusAppleID):
		if req.Platform == commonpb.Platform_GOOGLE {
			return &iappb.OnPurchaseCompletedResponse{Result: iappb.OnPurchaseCompletedResponse_INVALID_METADATA}, nil
		}

		// Product no longer supported
		return &iappb.OnPurchaseCompletedResponse{Result: iappb.OnPurchaseCompletedResponse_DENIED}, nil
	default:
		log.Warn("Invalid product in metadata")
		return &iappb.OnPurchaseCompletedResponse{Result: iappb.OnPurchaseCompletedResponse_INVALID_METADATA}, nil
	}

	isVerified, err := verifier.VerifyReceipt(ctx, req.Receipt.Value, req.Metadata.Product)
	if err != nil {
		log.Warn("Failed to verify receipt", zap.Error(err))
		return nil, status.Error(codes.Internal, "failed to verify receipt")
	} else if !isVerified {
		log.Warn("Receipt failed validation")
		return &iappb.OnPurchaseCompletedResponse{Result: iappb.OnPurchaseCompletedResponse_INVALID_RECEIPT}, nil
	}

	receiptID, err := verifier.GetReceiptIdentifier(ctx, req.Receipt.Value)
	if err != nil {
		log.Warn("Failed to get receipt ID", zap.Error(err))
		return nil, status.Error(codes.Internal, "failed to get receipt ID")
	}

	log = log.With(
		zap.String("receipt_id", base64.StdEncoding.EncodeToString(receiptID)),
	)

	// Note: purchase is always assumed to be fulfilled
	purchase, err := s.iaps.GetPurchaseByID(ctx, receiptID)
	if err == nil {
		if bytes.Equal(userID.Value, purchase.User.Value) {
			// Purchase is already fulfilled for this user, so it's a no-op. Return success
			return &iappb.OnPurchaseCompletedResponse{}, nil
		}
		log.Warn("Denying attempt to use an already fulfilled receipt")
		return &iappb.OnPurchaseCompletedResponse{Result: iappb.OnPurchaseCompletedResponse_DENIED}, nil
	} else if err != ErrNotFound {
		log.Warn("Failed to check existing purchase", zap.Error(err))
		return nil, status.Error(codes.Internal, "failed to check existing purchase")
	}

	err = database.ExecuteTxWithinCtx(ctx, func(ctx context.Context) error {
		switch product {
		case ProductCreateAccount, ProductCreateAccountBonusGoogle, ProductCreateAccountBonusApple:
			err = s.accounts.SetRegistrationFlag(ctx, userID, true)
			if err != nil {
				return errors.Wrap(err, "error setting registration flag")
			}
		default:
			return errors.New("product not implemented")
		}

		err = s.iaps.CreatePurchase(ctx, &Purchase{
			ReceiptID:       receiptID,
			Platform:        req.Platform,
			User:            userID,
			Product:         product,
			PaymentAmount:   float64(req.Metadata.Amount),
			PaymentCurrency: strings.ToLower(req.Metadata.Currency),
			State:           StateFulfilled,
			CreatedAt:       time.Now(),
		})
		if err != nil {
			return errors.Wrap(err, "ereror creating purchase")
		}

		return nil
	})
	if err != nil {
		purchase, err2 := s.iaps.GetPurchaseByID(ctx, receiptID)
		if err2 == nil && bytes.Equal(userID.Value, purchase.User.Value) {
			return &iappb.OnPurchaseCompletedResponse{}, nil
		}

		log.Warn("Failed to execute purchase fulfillment database transaction", zap.Error(err))
		return nil, status.Error(codes.Internal, "failed to execute purchase fulfillment database transaction")
	}

	return &iappb.OnPurchaseCompletedResponse{}, nil
}
