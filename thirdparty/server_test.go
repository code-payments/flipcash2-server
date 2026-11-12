package thirdparty

import (
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"
	thirdpartypb "github.com/code-payments/flipcash2-protobuf-api/generated/go/thirdparty/v1"

	"github.com/code-payments/flipcash2-server/account"
	account_memory "github.com/code-payments/flipcash2-server/account/memory"
	"github.com/code-payments/flipcash2-server/auth"
	"github.com/code-payments/flipcash2-server/model"
	profile_memory "github.com/code-payments/flipcash2-server/profile/memory"
	"github.com/code-payments/flipcash2-server/testutil"
)

// A CDP key ID is a UUID, and the proto requires the 36 characters of one.
const (
	testCoinbaseKeyID  = "11111111-2222-3333-4444-555555555555"
	otherCoinbaseKeyID = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
)

func TestGetJwt_Coinbase(t *testing.T) {
	ctx := context.Background()
	log := zaptest.NewLogger(t)

	accounts := account_memory.NewInMemory()
	profiles := profile_memory.NewInMemory()
	authz := account.NewAuthorizer(log, accounts, auth.NewKeyPairAuthenticator(log))

	seed := sha256.Sum256([]byte("thirdparty test key"))
	coinbaseKey := ed25519.NewKeyFromSeed(seed[:])

	serv := NewServer(log, authz, accounts, profiles, testCoinbaseKeyID, coinbaseKey)
	cc := testutil.RunGRPCServer(t, log, testutil.WithService(func(s *grpc.Server) {
		thirdpartypb.RegisterThirdPartyServer(s, serv)
	}))
	client := thirdpartypb.NewThirdPartyClient(cc)

	getJwt := func(t *testing.T, keyPair model.KeyPair, apiKey, method, host, path string) (*thirdpartypb.GetJwtResponse, error) {
		req := &thirdpartypb.GetJwtRequest{
			ApiKey: &thirdpartypb.ApiKey{Provider: thirdpartypb.Provider_COINBASE, Value: apiKey},
			Method: method,
			Host:   host,
			Path:   path,
		}
		require.NoError(t, keyPair.Auth(req, &req.Auth))
		return client.GetJwt(ctx, req)
	}

	// requireURI checks the token is one Coinbase would take for the request
	// named by uri: signed by the server's key, and bound to that request.
	requireURI := func(t *testing.T, resp *thirdpartypb.GetJwtResponse, uri string) {
		require.Equal(t, thirdpartypb.GetJwtResponse_OK, resp.Result)
		require.NotNil(t, resp.Jwt)

		parts := strings.Split(resp.Jwt.Value, ".")
		require.Len(t, parts, 3)
		signature, err := base64.RawURLEncoding.DecodeString(parts[2])
		require.NoError(t, err)
		require.True(t, ed25519.Verify(coinbaseKey.Public().(ed25519.PublicKey), []byte(parts[0]+"."+parts[1]), signature))

		payload, err := base64.RawURLEncoding.DecodeString(parts[1])
		require.NoError(t, err)
		var claims struct {
			URI string `json:"uri"`
		}
		require.NoError(t, json.Unmarshal(payload, &claims))
		require.Equal(t, uri, claims.URI)
	}

	requireDenied := func(t *testing.T, resp *thirdpartypb.GetJwtResponse) {
		require.Equal(t, thirdpartypb.GetJwtResponse_DENIED, resp.Result)
		require.Nil(t, resp.Jwt)
	}

	// A registered user with a verified phone number: everything GetJwt asks
	// of a caller but the request itself.
	verified := model.MustGenerateKeyPair()
	verifiedID := model.MustGenerateUserID()
	_, err := accounts.Bind(ctx, verifiedID, verified.Proto())
	require.NoError(t, err)
	require.NoError(t, accounts.SetRegistrationFlag(ctx, verifiedID, true))
	require.NoError(t, profiles.LinkPhoneNumber(ctx, verifiedID, "+12223334444", &commonpb.Hash{Value: []byte("phone-hash")}))

	// A registered user with no phone number.
	unverified := model.MustGenerateKeyPair()
	unverifiedID := model.MustGenerateUserID()
	_, err = accounts.Bind(ctx, unverifiedID, unverified.Proto())
	require.NoError(t, err)
	require.NoError(t, accounts.SetRegistrationFlag(ctx, unverifiedID, true))

	t.Run("the apps' requests are signed", func(t *testing.T) {
		for _, tc := range []struct {
			name               string
			method, host, path string
			uri                string
		}{
			{
				"android places an order",
				"POST", "api.cdp.coinbase.com/platform/", "v2/onramp/orders",
				"POST api.cdp.coinbase.com/platform/v2/onramp/orders",
			},
			{
				"android reads an order",
				"GET", "api.cdp.coinbase.com/platform/", "v2/onramp/orders/" + testOrderID,
				"GET api.cdp.coinbase.com/platform/v2/onramp/orders/" + testOrderID,
			},
			{
				"ios places an order",
				"POST", "api.cdp.coinbase.com/", "platform/v2/onramp/orders",
				"POST api.cdp.coinbase.com/platform/v2/onramp/orders",
			},
			{
				"ios reads an order",
				"GET", "api.cdp.coinbase.com/", "platform/v2/onramp/orders/" + testOrderID,
				"GET api.cdp.coinbase.com/platform/v2/onramp/orders/" + testOrderID,
			},
		} {
			t.Run(tc.name, func(t *testing.T) {
				resp, err := getJwt(t, verified, testCoinbaseKeyID, tc.method, tc.host, tc.path)
				require.NoError(t, err)
				requireURI(t, resp, tc.uri)
			})
		}
	})

	t.Run("any other request is denied", func(t *testing.T) {
		for _, tc := range []struct {
			name               string
			method, host, path string
		}{
			{"list customers", "GET", "api.cdp.coinbase.com/platform/", "v2/customers"},
			{"read a customer", "GET", "api.cdp.coinbase.com/", "platform/v2/customers/customer_x"},
			{"create a customer", "POST", "api.cdp.coinbase.com/platform/", "v2/customers"},
			{"update a customer", "PUT", "api.cdp.coinbase.com/platform/", "v2/customers/customer_x"},
			{"read the orders", "GET", "api.cdp.coinbase.com/platform/", "v2/onramp/orders"},
			{"another host", "POST", "sandbox.cdp.coinbase.com/platform/", "v2/onramp/orders"},
			{"past an order", "GET", "api.cdp.coinbase.com/platform/", "v2/onramp/orders/" + testOrderID + "/../../customers"},
		} {
			t.Run(tc.name, func(t *testing.T) {
				resp, err := getJwt(t, verified, testCoinbaseKeyID, tc.method, tc.host, tc.path)
				require.NoError(t, err)
				requireDenied(t, resp)
			})
		}
	})

	t.Run("the request is judged before the profile", func(t *testing.T) {
		// A caller with no phone number learns their request is refused, not
		// that verifying a phone number would get it signed.
		resp, err := getJwt(t, unverified, testCoinbaseKeyID, "GET", "api.cdp.coinbase.com/platform/", "v2/customers")
		require.NoError(t, err)
		requireDenied(t, resp)

		resp, err = getJwt(t, unverified, testCoinbaseKeyID, "POST", "api.cdp.coinbase.com/platform/", "v2/onramp/orders")
		require.NoError(t, err)
		require.Equal(t, thirdpartypb.GetJwtResponse_PHONE_VERIFICATION_REQUIRED, resp.Result)
		require.Nil(t, resp.Jwt)
	})

	t.Run("a wrong api key is still its own result", func(t *testing.T) {
		resp, err := getJwt(t, verified, otherCoinbaseKeyID, "GET", "api.cdp.coinbase.com/platform/", "v2/customers")
		require.NoError(t, err)
		require.Equal(t, thirdpartypb.GetJwtResponse_INVALID_API_KEY, resp.Result)
		require.Nil(t, resp.Jwt)
	})

	t.Run("an unregistered user is denied an allowed request", func(t *testing.T) {
		keyPair := model.MustGenerateKeyPair()
		userID := model.MustGenerateUserID()
		_, err := accounts.Bind(ctx, userID, keyPair.Proto())
		require.NoError(t, err)

		resp, err := getJwt(t, keyPair, testCoinbaseKeyID, "POST", "api.cdp.coinbase.com/platform/", "v2/onramp/orders")
		require.NoError(t, err)
		requireDenied(t, resp)
	})

	t.Run("an unknown key pair is refused", func(t *testing.T) {
		_, err := getJwt(t, model.MustGenerateKeyPair(), testCoinbaseKeyID, "POST", "api.cdp.coinbase.com/platform/", "v2/onramp/orders")
		require.Equal(t, codes.PermissionDenied, status.Code(err))
	})
}
