package account

import (
	"bytes"
	"context"
	"errors"
	"slices"
	"strings"
	"time"

	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"

	accountpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/account/v1"
	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"

	"github.com/code-payments/flipcash2-server/auth"
	"github.com/code-payments/flipcash2-server/database"
	"github.com/code-payments/flipcash2-server/model"
	"github.com/code-payments/flipcash2-server/rpc"
	"github.com/code-payments/flipcash2-server/tip"
	ocp_client "github.com/code-payments/ocp-server/grpc/client"
	ocp_common "github.com/code-payments/ocp-server/ocp/common"
)

// todo: env configs
const (
	loginWindow                 = 2 * time.Minute
	requireIapOnAccountCreation = false

	minIosBuildNumber          = 256
	staffMinIosBuildNumber     = 256
	minAndroidBuildNumber      = 2790
	staffMinAndroidBuildNumber = 2790

	defaultBillExchangeDataTimeout = 5 * time.Minute

	RequireCoinbaseEmailVerification = false
)

var (
	DefaultNewCurrencyPurchaseAmount = ocp_common.ToCoreMintQuarks(10)
	DefaultNewCurrencyFeeAmount      = ocp_common.ToCoreMintQuarks(10)
	DefaulWithdrawalFeeAmount        = ocp_common.ToCoreMintQuarks(5) / 10
	MinHolderValue                   = ocp_common.ToCoreMintQuarks(10)
)

type onRampProviderConfig struct {
	Provider accountpb.UserFlags_OnRampProvider
	// MinVersion, when non-nil, gates this provider behind a minimum client
	// version. Clients on older versions (or without a parseable user-agent)
	// will not have this provider included.
	MinVersion *ocp_client.Version
}

var (
	defaultOnRampProviders = []accountpb.UserFlags_OnRampProvider{
		accountpb.UserFlags_PHANTOM,
		accountpb.UserFlags_MANUAL_DEPOSIT,
	}
	onRampProvidersByCountryAndPlatform = map[string]map[commonpb.Platform][]onRampProviderConfig{
		"us": {
			commonpb.Platform_APPLE: {
				{
					Provider:   accountpb.UserFlags_COINBASE_VIRTUAL,
					MinVersion: &ocp_client.Version{Major: 1, Minor: 9, Patch: 0},
				},
			},
			commonpb.Platform_GOOGLE: {
				{
					Provider:   accountpb.UserFlags_COINBASE_VIRTUAL,
					MinVersion: &ocp_client.Version{Major: 2026, Minor: 5, Patch: 3},
				},
			},
		},
	}

	staffAppleOnRampProviders = []accountpb.UserFlags_OnRampProvider{
		accountpb.UserFlags_COINBASE_VIRTUAL,
		accountpb.UserFlags_PHANTOM,
		accountpb.UserFlags_BASE,
		accountpb.UserFlags_SOLFLARE,
		accountpb.UserFlags_BACKPACK,
		accountpb.UserFlags_MANUAL_DEPOSIT,
	}
	staffGoogleOnRampProviders = []accountpb.UserFlags_OnRampProvider{
		accountpb.UserFlags_COINBASE_VIRTUAL,
		accountpb.UserFlags_PHANTOM,
		accountpb.UserFlags_BASE,
		accountpb.UserFlags_SOLFLARE,
		accountpb.UserFlags_BACKPACK,
		accountpb.UserFlags_MANUAL_DEPOSIT,
	}

	minVersionForPhoneNumberSendByPlatform = map[commonpb.Platform]*ocp_client.Version{
		commonpb.Platform_APPLE: {Major: 1, Minor: 13, Patch: 0},
	}

	tipPresets []*accountpb.TipPresets
)

func init() {
	entries := tip.All()
	tipPresets = make([]*accountpb.TipPresets, 0, len(entries))
	for _, entry := range entries {
		tipPresets = append(tipPresets, &accountpb.TipPresets{
			Region:  &commonpb.Region{Value: string(entry.Region)},
			Minimum: entry.Presets.Minimum,
			Low:     entry.Presets.Low,
			Medium:  entry.Presets.Medium,
			High:    entry.Presets.High,
		})
	}
}

type Server struct {
	log      *zap.Logger
	store    Store
	verifier auth.Authenticator

	accountpb.UnimplementedAccountServer
}

func NewServer(log *zap.Logger, store Store, verifier auth.Authenticator) *Server {
	return &Server{
		log:      log,
		store:    store,
		verifier: verifier,
	}
}

func (s *Server) Register(ctx context.Context, req *accountpb.RegisterRequest) (*accountpb.RegisterResponse, error) {
	verify := &accountpb.RegisterRequest{
		PublicKey: req.PublicKey,
	}
	err := s.verifier.Verify(ctx, verify, &commonpb.Auth{
		Kind: &commonpb.Auth_KeyPair_{
			KeyPair: &commonpb.Auth_KeyPair{
				PubKey:    req.PublicKey,
				Signature: req.Signature,
			},
		},
	})
	if err != nil {
		return nil, err
	}

	userID, err := model.GenerateUserId()
	if err != nil {
		return nil, status.Error(codes.Internal, "failed to generate user id")
	}

	var prev *commonpb.UserId
	err = database.ExecuteTxWithinCtx(ctx, func(ctx context.Context) error {
		prev, err = s.store.Bind(ctx, userID, req.PublicKey)
		if err != nil {
			return err
		}

		if !requireIapOnAccountCreation {
			return s.store.SetRegistrationFlag(ctx, prev, true)
		}
		return nil
	})
	if err != nil {
		return nil, status.Error(codes.Internal, "")
	}

	return &accountpb.RegisterResponse{
		UserId: prev,
	}, nil
}

func (s *Server) Login(ctx context.Context, req *accountpb.LoginRequest) (*accountpb.LoginResponse, error) {
	t := req.Timestamp.AsTime()
	if t.After(time.Now().Add(loginWindow)) {
		return &accountpb.LoginResponse{Result: accountpb.LoginResponse_INVALID_TIMESTAMP}, nil
	} else if t.Before(time.Now().Add(-loginWindow)) {
		return &accountpb.LoginResponse{Result: accountpb.LoginResponse_INVALID_TIMESTAMP}, nil
	}

	a := req.Auth
	req.Auth = nil
	if err := s.verifier.Verify(ctx, req, a); err != nil {
		if status.Code(err) == codes.Unauthenticated {
			return &accountpb.LoginResponse{Result: accountpb.LoginResponse_DENIED}, nil
		}

		return nil, err
	}

	keyPair := a.GetKeyPair()
	if keyPair == nil {
		return nil, status.Error(codes.InvalidArgument, "missing keypair")
	}
	if err := keyPair.Validate(); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid keypair: %v", err)
	}

	userID, err := s.store.GetUserId(ctx, keyPair.GetPubKey())
	if errors.Is(err, ErrNotFound) {
		return &accountpb.LoginResponse{Result: accountpb.LoginResponse_DENIED}, nil
	} else if err != nil {
		return nil, status.Error(codes.Internal, "")
	}

	return &accountpb.LoginResponse{Result: accountpb.LoginResponse_OK, UserId: userID}, nil
}

func (s *Server) GetUserFlags(ctx context.Context, req *accountpb.GetUserFlagsRequest) (*accountpb.GetUserFlagsResponse, error) {
	authorized, err := s.store.GetPubKeys(ctx, req.UserId)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to get keys")
	}

	if len(authorized) == 0 {
		// Don't leak that the user does not exist.
		return &accountpb.GetUserFlagsResponse{Result: accountpb.GetUserFlagsResponse_DENIED}, nil
	}

	var signerAuthorized bool
	for _, key := range authorized {
		if bytes.Equal(key.Value, req.GetAuth().GetKeyPair().PubKey.Value) {
			signerAuthorized = true
			break
		}
	}

	if !signerAuthorized {
		return &accountpb.GetUserFlagsResponse{Result: accountpb.GetUserFlagsResponse_DENIED}, nil
	}

	isStaff, err := s.store.IsStaff(ctx, req.UserId)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to get staff flag")
	}

	isRegistered, err := s.store.IsRegistered(ctx, req.UserId)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to get registration flag")
	}

	var preferredOnRampProviderForUser accountpb.UserFlags_OnRampProvider
	var supportedOnRampProvidersForUser []accountpb.UserFlags_OnRampProvider
	if isStaff {
		switch req.Platform {
		case commonpb.Platform_APPLE:
			supportedOnRampProvidersForUser = staffAppleOnRampProviders
		case commonpb.Platform_GOOGLE:
			supportedOnRampProvidersForUser = staffGoogleOnRampProviders
		}
	} else {
		supportedOnRampProvidersForUser = getSupportedOnRampProviders(ctx, req.CountryCode, req.Platform)
	}
	if slices.Contains(supportedOnRampProvidersForUser, accountpb.UserFlags_COINBASE_VIRTUAL) {
		preferredOnRampProviderForUser = accountpb.UserFlags_COINBASE_VIRTUAL
	}

	var minBuildNumber int
	switch req.Platform {
	case commonpb.Platform_APPLE:
		minBuildNumber = minIosBuildNumber
		if isStaff {
			minBuildNumber = staffMinIosBuildNumber
		}
	case commonpb.Platform_GOOGLE:
		minBuildNumber = minAndroidBuildNumber
		if isStaff {
			minBuildNumber = staffMinAndroidBuildNumber
		}
	}

	return &accountpb.GetUserFlagsResponse{
		Result: accountpb.GetUserFlagsResponse_OK,
		UserFlags: &accountpb.UserFlags{
			IsStaff:                          isStaff,
			IsRegisteredAccount:              isRegistered,
			RequiresIapForRegistration:       requireIapOnAccountCreation,
			SupportedOnRampProviders:         supportedOnRampProvidersForUser,
			PreferredOnRampProvider:          preferredOnRampProviderForUser,
			MinBuildNumber:                   uint32(minBuildNumber),
			BillExchangeDataTimeout:          durationpb.New(defaultBillExchangeDataTimeout),
			NewCurrencyPurchaseAmount:        DefaultNewCurrencyPurchaseAmount,
			NewCurrencyFeeAmount:             DefaultNewCurrencyFeeAmount,
			WithdrawalFeeAmount:              DefaulWithdrawalFeeAmount,
			PreferredOnRampUsdcLiquidityPool: accountpb.UserFlags_COINBASE_STABLE_SWAPPER,
			MinimumHolderValue:               MinHolderValue,
			RequireCoinbaseEmailVerification: RequireCoinbaseEmailVerification,
			EnablePhoneNumberSend:            isPhoneNumberSendEnabled(ctx, req.Platform),
			TipPresets:                       tipPresets,
		},
	}, nil
}

func (s *Server) GetUnauthenticatedUserFlags(ctx context.Context, req *accountpb.GetUnauthenticatedUserFlagsRequest) (*accountpb.GetUnauthenticatedUserFlagsResponse, error) {
	supportedOnRampProvidersForUser := getSupportedOnRampProviders(ctx, req.CountryCode, req.Platform)

	var preferredOnRampProviderForUser accountpb.UserFlags_OnRampProvider
	if slices.Contains(supportedOnRampProvidersForUser, accountpb.UserFlags_COINBASE_VIRTUAL) {
		preferredOnRampProviderForUser = accountpb.UserFlags_COINBASE_VIRTUAL
	}

	var minBuildNumber int
	switch req.Platform {
	case commonpb.Platform_APPLE:
		minBuildNumber = minIosBuildNumber
	case commonpb.Platform_GOOGLE:
		minBuildNumber = minAndroidBuildNumber
	}

	return &accountpb.GetUnauthenticatedUserFlagsResponse{
		Result: accountpb.GetUnauthenticatedUserFlagsResponse_OK,
		UserFlags: &accountpb.UserFlags{
			IsStaff:                          false,
			IsRegisteredAccount:              false,
			RequiresIapForRegistration:       requireIapOnAccountCreation,
			SupportedOnRampProviders:         supportedOnRampProvidersForUser,
			PreferredOnRampProvider:          preferredOnRampProviderForUser,
			MinBuildNumber:                   uint32(minBuildNumber),
			NewCurrencyPurchaseAmount:        DefaultNewCurrencyPurchaseAmount,
			NewCurrencyFeeAmount:             DefaultNewCurrencyFeeAmount,
			WithdrawalFeeAmount:              DefaulWithdrawalFeeAmount,
			PreferredOnRampUsdcLiquidityPool: accountpb.UserFlags_COINBASE_STABLE_SWAPPER,
			MinimumHolderValue:               MinHolderValue,
			RequireCoinbaseEmailVerification: RequireCoinbaseEmailVerification,
			EnablePhoneNumberSend:            isPhoneNumberSendEnabled(ctx, req.Platform),
			TipPresets:                       tipPresets,
		},
	}, nil
}

func isPhoneNumberSendEnabled(ctx context.Context, platform commonpb.Platform) bool {
	minVersion, ok := minVersionForPhoneNumberSendByPlatform[platform]
	if !ok {
		return false
	}

	clientVersion := getClientVersion(ctx)
	if clientVersion == nil {
		return false
	}
	return clientVersion.GreaterThanOrEqualTo(minVersion)
}

func getSupportedOnRampProviders(ctx context.Context, countryCode *commonpb.CountryCode, platform commonpb.Platform) []accountpb.UserFlags_OnRampProvider {
	defaultSupported := make([]accountpb.UserFlags_OnRampProvider, len(defaultOnRampProviders))
	copy(defaultSupported, defaultOnRampProviders)

	if countryCode == nil {
		return defaultSupported
	}

	if platform == commonpb.Platform_UNKNOWN {
		return defaultSupported
	}

	byCountry, ok := onRampProvidersByCountryAndPlatform[strings.ToLower(countryCode.Value)]
	if !ok || len(byCountry) == 0 {
		return defaultSupported
	}

	byPlatform, ok := byCountry[platform]
	if !ok || len(byPlatform) == 0 {
		return defaultSupported
	}

	clientVersion := getClientVersion(ctx)

	filtered := make([]accountpb.UserFlags_OnRampProvider, 0, len(byPlatform))
	for _, entry := range byPlatform {
		if entry.MinVersion != nil {
			if clientVersion == nil || clientVersion.Before(entry.MinVersion) {
				continue
			}
		}
		filtered = append(filtered, entry.Provider)
	}

	allSupported := make([]accountpb.UserFlags_OnRampProvider, 0, len(filtered)+len(defaultSupported))
	allSupported = append(allSupported, filtered...)         // Country and platform specific providers take priority
	allSupported = append(allSupported, defaultSupported...) // Followed by default global providers
	return allSupported
}

// getClientVersion extracts the client's version from the gRPC user-agent, or
// returns nil if no parseable Flipcash user-agent is present.
func getClientVersion(ctx context.Context) *ocp_client.Version {
	var clientVersion *ocp_client.Version
	if userAgent, err := ocp_client.GetUserAgent(ctx, rpc.UserAgentName); err == nil {
		clientVersion = &userAgent.Version
	}
	if userAgent, err := ocp_client.GetUserAgent(ctx, rpc.UserAgentName+"/Core"); err == nil {
		clientVersion = &userAgent.Version
	}
	return clientVersion
}
