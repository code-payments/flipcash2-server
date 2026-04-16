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
		resp.FlaggedCategory = getHighestFlaggedCategory(result)
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
		resp.Attestation = s.signAttestation(log, req.ImageData, userID)
	} else {
		log.Info("Image is flagged", zap.Strings("categories", result.FlaggedCategories))
		resp.FlaggedCategory = getHighestFlaggedCategory(result)
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

func getHighestFlaggedCategory(result *Result) moderationpb.FlaggedCategory {
	var highestScore float64
	highestFlaggedCategory := moderationpb.FlaggedCategory_OTHER
	for _, flaggedCategory := range result.FlaggedCategories {
		mapped := mapFlaggedCategory(flaggedCategory)
		if mapped == moderationpb.FlaggedCategory_OTHER {
			continue
		}
		score := result.CategoryScores[flaggedCategory]
		if score > highestScore {
			highestScore = score
			highestFlaggedCategory = mapped
		}
	}
	return highestFlaggedCategory
}

func mapFlaggedCategory(flaggedCategory string) moderationpb.FlaggedCategory {
	switch flaggedCategory {
	case
		"cryptocurrency",
		"exchange_platform",
		"fiat_currency",
		"financial_service",
		"general_trademark",
		"government_affiliation",
		"impersonation",
		"platform_impersonation",
		"public_figure",
		"stablecoin",
		"tech_company":
		return moderationpb.FlaggedCategory_IMPERSONATION

	case
		"misleading_backing":
		return moderationpb.FlaggedCategory_MISLEADING

	case
		"a_little_bloody",
		"animal_genitalia_and_human",
		"animal_genitalia_only",
		"animated_alcohol",
		"animated_animal_genitalia",
		"animated_corpse",
		"animated_gun",
		"bullying",
		"child_exploitation",
		"culinary_knife_in_hand",
		"culinary_knife_not_in_hand",
		"child_safety",
		"drugs",
		"general_nsfw",
		"general_suggestive",
		"gun_in_hand",
		"gun_not_in_hand",
		"hanging",
		"hate",
		"human_corpse",
		"illicit_injectables",
		"kissing",
		"knife_in_hand",
		"knife_not_in_hand",
		"licking",
		"medical_injectables",
		"minor_explicitly_mentioned",
		"minor_implicitly_mentioned",
		"noose",
		"other_blood",
		"profanity",
		"recreational_pills",
		"self_harm",
		"self_harm_intent",
		"sexual",
		"sexual_description",
		"very_bloody",
		"violence",
		"violent_description",
		"weapons",
		"yes_alcohol",
		"yes_animal_abuse",
		"yes_bodysuit",
		"yes_bra",
		"yes_breast",
		"yes_bulge",
		"yes_butt",
		"yes_child_present",
		"yes_child_safety",
		"yes_cleavage",
		"yes_confederate",
		"yes_drinking_alcohol",
		"yes_emaciated_body",
		"yes_female_nudity",
		"yes_female_swimwear",
		"yes_female_underwear",
		"yes_fight",
		"yes_gambling",
		"yes_genitals",
		"yes_kkk",
		"yes_male_nudity",
		"yes_male_shirtless",
		"yes_male_underwear",
		"yes_marijuana",
		"yes_middle_finger",
		"yes_miniskirt",
		"yes_nazi",
		"yes_negligee",
		"yes_panties",
		"yes_pills",
		"yes_realistic_nsfw",
		"yes_self_harm",
		"yes_sex_toy",
		"yes_sexual_activity",
		"yes_sexual_intent",
		"yes_smoking",
		"yes_sports_bra",
		"yes_sportswear_bottoms",
		"yes_terrorist",
		"yes_undressed":
		return moderationpb.FlaggedCategory_NSFW

	case
		"gibberish",
		"phone_number",
		"promotions",
		"redirection",
		"spam",
		"yes_qr_code":
		return moderationpb.FlaggedCategory_SPAM
	}

	return moderationpb.FlaggedCategory_OTHER
}
