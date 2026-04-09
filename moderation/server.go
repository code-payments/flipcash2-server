package moderation

import (
	"context"
	"crypto/sha256"
	"strings"

	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"
	moderationpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/moderation/v1"

	"github.com/code-payments/flipcash2-server/auth"
	"github.com/code-payments/flipcash2-server/model"
	"github.com/code-payments/ocp-server/solana/currencycreator"
)

var blockedCurrencyNames = []string{
	"flipcash",
	"usdf",
}

var (
	jpegMagic = []byte{0xFF, 0xD8, 0xFF}
	pngMagic  = []byte{0x89, 0x50, 0x4E, 0x47, 0x0D, 0x0A, 0x1A, 0x0A}
)

type Server struct {
	log      *zap.Logger
	authz    auth.Authorizer
	client   Client
	attestor model.KeyPair

	moderationpb.UnimplementedModerationServer
}

func NewServer(log *zap.Logger, authz auth.Authorizer, client Client, attestor model.KeyPair) *Server {
	return &Server{
		log:      log,
		authz:    authz,
		client:   client,
		attestor: attestor,
	}
}

func (s *Server) ModerateText(ctx context.Context, req *moderationpb.ModerateTextRequest) (*moderationpb.ModerateTextResponse, error) {
	userID, err := s.authz.Authorize(ctx, req, &req.Auth)
	if err != nil {
		return nil, err
	}

	log := s.log.With(zap.String("user_id", model.UserIDString(userID)))

	result, err := s.client.ClassifyText(ctx, req.Text)
	if err != nil {
		log.Warn("Failed to classify text", zap.Error(err))
		return nil, status.Error(codes.Internal, "")
	}

	if !result.Flagged && len(req.Text) <= currencycreator.MaxCurrencyConfigAccountNameLength {
		currencyNameResult, err := s.client.ClassifyCurrencyName(ctx, req.Text)
		if err != nil {
			log.Warn("Failed to classify currency name", zap.Error(err))
			return nil, status.Error(codes.Internal, "")
		}
		if currencyNameResult.Flagged {
			result = currencyNameResult
		}
	}

	if !result.Flagged && len(req.Text) <= currencycreator.MaxCurrencyConfigAccountNameLength && isBlockedCurrencyName(req.Text) {
		result = &Result{
			Flagged:           true,
			FlaggedCategories: []string{"platform_impersonation"},
			CategoryScores:    map[string]float64{"platform_impersonation": 1.0},
		}
	}

	isAllowed := !result.Flagged

	resp := &moderationpb.ModerateTextResponse{
		Result:    moderationpb.ModerateTextResponse_OK,
		IsAllowed: isAllowed,
	}

	if isAllowed {
		resp.Attestation = s.signAttestation(log, req.Text, userID)
	} else {
		log.Info("Text is flagged", zap.Strings("categories", result.FlaggedCategories))
	}

	return resp, nil
}

func (s *Server) ModerateImage(ctx context.Context, req *moderationpb.ModerateImageRequest) (*moderationpb.ModerateImageResponse, error) {
	userID, err := s.authz.Authorize(ctx, req, &req.Auth)
	if err != nil {
		return nil, err
	}

	log := s.log.With(zap.String("user_id", model.UserIDString(userID)))

	if !isJPEG(req.ImageData) && !isPNG(req.ImageData) {
		return &moderationpb.ModerateImageResponse{
			Result:    moderationpb.ModerateImageResponse_OK,
			IsAllowed: false,
		}, nil
	}

	result, err := s.client.ClassifyImage(ctx, req.ImageData)
	if err != nil {
		log.Warn("Failed to classify image", zap.Error(err))
		return nil, status.Error(codes.Internal, "")
	}

	isAllowed := !result.Flagged

	resp := &moderationpb.ModerateImageResponse{
		Result:    moderationpb.ModerateImageResponse_OK,
		IsAllowed: isAllowed,
	}

	if isAllowed {
		resp.Attestation = s.signAttestation(log, req.ImageData, userID)
	} else {
		log.Info("Image is flagged", zap.Strings("categories", result.FlaggedCategories))
	}

	return resp, nil
}

func (s *Server) signAttestation(log *zap.Logger, content any, userID *commonpb.UserId) *moderationpb.ModerationAttestation {
	var hash [sha256.Size]byte
	switch v := content.(type) {
	case string:
		hash = sha256.Sum256([]byte(v))
	case []byte:
		hash = sha256.Sum256(v)
	}

	attestation := &moderationpb.ModerationAttestation{
		ContentHash: hash[:],
		Timestamp:   timestamppb.Now(),
		UserId:      userID,
		Attestor:    s.attestor.Proto(),
	}

	if err := s.attestor.Sign(attestation, &attestation.Signature); err != nil {
		log.Warn("Failed to sign attestation", zap.Error(err))
		return nil
	}

	return attestation
}

func isJPEG(data []byte) bool {
	return len(data) >= len(jpegMagic) && string(data[:len(jpegMagic)]) == string(jpegMagic)
}

func isPNG(data []byte) bool {
	return len(data) >= len(pngMagic) && string(data[:len(pngMagic)]) == string(pngMagic)
}

func isBlockedCurrencyName(name string) bool {
	lower := strings.ToLower(name)
	for _, blocked := range blockedCurrencyNames {
		if strings.Contains(lower, blocked) {
			return true
		}
	}
	return false
}
