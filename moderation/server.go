package moderation

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
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

// A WebP file opens "RIFF", four bytes of file size, then "WEBP" and the first
// chunk's name, which starts "VP8".
const (
	riffMagic      = "RIFF"
	webpMagic      = "WEBPVP8"
	webpFormOffset = len(riffMagic) + 4
	webpHeaderSize = webpFormOffset + len(webpMagic)
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

// ModerateText moderates text under the checks of the kind the request names
// (req.Kind) and signs that kind into the attestation, so the attestation is
// valid only where content of that kind is used (see ValidateAttestation).
//
// A request naming no kind comes from a legacy client and gets the checks every
// kind once shared (moderateLegacyText).
func (s *Server) ModerateText(ctx context.Context, req *moderationpb.ModerateTextRequest) (*moderationpb.ModerateTextResponse, error) {
	userID, err := s.authz.Authorize(ctx, req, &req.Auth)
	if err != nil {
		return nil, err
	}

	log := s.log.With(
		zap.String("user_id", model.UserIDString(userID)),
		zap.Stringer("kind", req.Kind),
	)

	var result *Result
	switch req.Kind {
	case moderationpb.ContentKind_UNKNOWN_CONTENT_KIND:
		result, err = s.moderateLegacyText(ctx, req.Text)
	case moderationpb.ContentKind_CURRENCY_NAME:
		if len(req.Text) > currencycreator.MaxCurrencyConfigAccountNameLength {
			return nil, status.Error(codes.InvalidArgument, "text exceeds the maximum currency name length")
		}
		result, err = s.moderateCurrencyName(ctx, req.Text)
	case moderationpb.ContentKind_CURRENCY_SYMBOL:
		if len(req.Text) > currencycreator.MaxCurrencyConfigAccountSymbolLength {
			return nil, status.Error(codes.InvalidArgument, "text exceeds the maximum currency symbol length")
		}
		result, err = s.moderateCurrencySymbol(ctx, req.Text)
	case moderationpb.ContentKind_CURRENCY_DESCRIPTION:
		result, err = s.moderateCurrencyDescription(ctx, req.Text)
	default:
		return nil, status.Error(codes.InvalidArgument, "unsupported content kind for text")
	}
	switch {
	case err == nil:
	case errors.Is(err, ErrUnsupportedLanguage):
		return &moderationpb.ModerateTextResponse{Result: moderationpb.ModerateTextResponse_UNSUPPORTED_LANGUAGE}, nil
	default:
		log.Warn("Failed to moderate text", zap.Error(err))
		return nil, status.Error(codes.Internal, "")
	}

	isAllowed := !result.Flagged

	resp := &moderationpb.ModerateTextResponse{
		Result:    moderationpb.ModerateTextResponse_OK,
		IsAllowed: isAllowed,
	}

	if isAllowed {
		resp.Attestation = s.signAttestation(log, req.Kind, req.Text, userID)
	} else {
		log.Info("Text is flagged", zap.Strings("categories", result.FlaggedCategories))
		resp.FlaggedCategory = HighestFlaggedCategory(result)
	}

	return resp, nil
}

// moderateLegacyText applies the checks every kind of text shared before
// requests named one: any text short enough to be a currency name gets a
// currency name's checks, and anything longer the generic ones. Its attestation
// carries no kind, so it is accepted for every kind while legacy attestations
// are (see ValidateAttestation), and the length rule is what keeps that safe:
// text that could be used as a name was checked as one.
func (s *Server) moderateLegacyText(ctx context.Context, text string) (*Result, error) {
	if len(text) <= currencycreator.MaxCurrencyConfigAccountNameLength {
		return s.moderateCurrencyName(ctx, text)
	}
	return s.moderateCurrencyDescription(ctx, text)
}

// moderateCurrencyName checks a currency's name: the generic classifier, then
// the currency name classifier (trademarks, brands, impersonation, misleading
// claims), then the names reserved for the platform. The first to flag decides.
//
// The generic classifier's ErrUnsupportedLanguage stays fatal: the currency
// name classifier does not score harmful content, so the generic classifier is
// the only one that would see it.
func (s *Server) moderateCurrencyName(ctx context.Context, name string) (*Result, error) {
	result, err := s.client.ClassifyText(ctx, name)
	if err != nil {
		return nil, fmt.Errorf("classify text: %w", err)
	}
	if result.Flagged {
		return result, nil
	}

	result, err = s.client.ClassifyCurrencyName(ctx, name)
	if err != nil {
		return nil, fmt.Errorf("classify currency name: %w", err)
	}
	if result.Flagged {
		return result, nil
	}

	if isBlockedCurrencyName(name) {
		return &Result{
			Flagged:           true,
			FlaggedCategories: []string{"platform_impersonation"},
			CategoryScores:    map[string]float64{"platform_impersonation": 1.0},
		}, nil
	}

	return result, nil
}

// moderateCurrencySymbol checks a currency's symbol under a name's checks: a
// symbol is a short name for the currency (a ticker), and squatting another
// token's or a platform's ticker is the same harm as squatting its name. The
// currency name classifier judges it as a name, which a ticker like BTC or USDF
// still matches.
func (s *Server) moderateCurrencySymbol(ctx context.Context, symbol string) (*Result, error) {
	return s.moderateCurrencyName(ctx, symbol)
}

// moderateCurrencyDescription checks a currency's description with the generic
// classifier alone. A description is prose about the currency, and may name
// other brands, tokens or the platform without claiming to be them, so it gets
// none of a name's impersonation checks, whatever its length.
func (s *Server) moderateCurrencyDescription(ctx context.Context, description string) (*Result, error) {
	result, err := s.client.ClassifyText(ctx, description)
	if err != nil {
		return nil, fmt.Errorf("classify text: %w", err)
	}
	return result, nil
}

func (s *Server) ModerateImage(ctx context.Context, req *moderationpb.ModerateImageRequest) (*moderationpb.ModerateImageResponse, error) {
	userID, err := s.authz.Authorize(ctx, req, &req.Auth)
	if err != nil {
		return nil, err
	}

	log := s.log.With(
		zap.String("user_id", model.UserIDString(userID)),
		zap.Stringer("kind", req.Kind),
	)

	// Every image kind (today a currency's icon, or none named by a legacy
	// client) gets the same checks; the kind is still signed into the
	// attestation so it is valid only where that kind is used.
	switch req.Kind {
	case moderationpb.ContentKind_UNKNOWN_CONTENT_KIND, moderationpb.ContentKind_CURRENCY_ICON:
	default:
		return nil, status.Error(codes.InvalidArgument, "unsupported content kind for image")
	}

	// JPEG, PNG and WebP are taken, told apart by the file's signature alone.
	// Nothing here decodes the image, so passing this check does not make a
	// file well formed or a still; whoever stores it decodes it (OCP, for a
	// currency's icon, which keeps a re-encoding of what its decoder read).
	if !isJPEG(req.ImageData) && !isPNG(req.ImageData) && !isWebP(req.ImageData) {
		return &moderationpb.ModerateImageResponse{
			Result: moderationpb.ModerateImageResponse_UNSUPPORTED_FORMAT,
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
		resp.Attestation = s.signAttestation(log, req.Kind, req.ImageData, userID)
	} else {
		log.Info("Image is flagged", zap.Strings("categories", result.FlaggedCategories))
		resp.FlaggedCategory = HighestFlaggedCategory(result)
	}

	return resp, nil
}

// signAttestation signs an attestation that content was moderated as kind, for
// userID. Callers sign only after applying that kind's checks.
func (s *Server) signAttestation(log *zap.Logger, kind moderationpb.ContentKind, content any, userID *commonpb.UserId) *moderationpb.ModerationAttestation {
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
		Kind:        kind,
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

// isWebP reports whether data opens as a WebP file does: a RIFF container of
// form WEBP whose first chunk is one of the three a WebP file starts with (VP8
// for lossy, VP8L for lossless, VP8X for the extended format). The four bytes
// between are the file's size, which is not checked. This is the signature
// Go's WebP decoders register, so what is taken here is what a decoder
// recognizes.
func isWebP(data []byte) bool {
	return len(data) >= webpHeaderSize &&
		string(data[:len(riffMagic)]) == riffMagic &&
		string(data[webpFormOffset:webpHeaderSize]) == webpMagic
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
