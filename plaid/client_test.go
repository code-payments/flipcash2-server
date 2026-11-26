package plaid_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"

	"github.com/code-payments/flipcash2-server/plaid"
)

const (
	testClientID = "test-client-id"
	testSecret   = "test-secret-do-not-log"
	testUserID   = "0ba20545-0587-44a4-a43e-725937f5f15c"
	testAccess   = "access-sandbox-de3ce8ef-33f8-452c-a685-8671031fc0f6"
)

type recorded struct {
	method string
	path   string
	header http.Header
	body   map[string]any
}

// recorder is a server that records each request and answers with a canned
// status and body.
type recorder struct {
	*httptest.Server
	mu       sync.Mutex
	requests []recorded
	status   int
	body     string
}

func newRecorder(t *testing.T, status int, body string) *recorder {
	r := &recorder{status: status, body: body}
	r.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		data, _ := io.ReadAll(req.Body)
		var decoded map[string]any
		_ = json.Unmarshal(data, &decoded)
		r.mu.Lock()
		r.requests = append(r.requests, recorded{req.Method, req.URL.Path, req.Header.Clone(), decoded})
		status, body := r.status, r.body
		r.mu.Unlock()
		w.WriteHeader(status)
		_, _ = io.WriteString(w, body)
	}))
	t.Cleanup(r.Close)
	return r
}

func (r *recorder) respond(status int, body string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.status, r.body = status, body
}

func (r *recorder) config() plaid.Config {
	return plaid.Config{
		BaseURL:    r.URL,
		ClientID:   testClientID,
		Secret:     testSecret,
		HTTPClient: r.Client(),
		Link: plaid.LinkConfig{
			ClientName:         "Flipcash",
			RedirectURI:        "https://app.example.com/plaid/oauth",
			AndroidPackageName: "com.example.app",
			Webhook:            "https://api.example.com/plaid/webhook",
		},
	}
}

func (r *recorder) client(t *testing.T) *plaid.Client {
	c, err := plaid.NewClient(zap.NewNop(), r.config())
	require.NoError(t, err)
	return c
}

func (r *recorder) last(t *testing.T) recorded {
	r.mu.Lock()
	defer r.mu.Unlock()
	require.NotEmpty(t, r.requests)
	return r.requests[len(r.requests)-1]
}

func (r *recorder) count() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.requests)
}

// jsonOf round-trips v so it compares with a decoded request body.
func jsonOf(t *testing.T, v any) map[string]any {
	data, err := json.Marshal(v)
	require.NoError(t, err)
	var out map[string]any
	require.NoError(t, json.Unmarshal(data, &out))
	return out
}

const linkTokenResponse = `{"link_token":"link-sandbox-af1a0311-da53-4636-b754-dd15cc058176","expiration":"2026-11-25T14:00:00Z","request_id":"req-1"}`

func TestNewClient_RefusesBadConfig(t *testing.T) {
	good := plaid.Config{
		BaseURL:  plaid.SandboxBaseURL,
		ClientID: "id",
		Secret:   "secret",
		Link:     plaid.LinkConfig{ClientName: "Flipcash"},
	}
	_, err := plaid.NewClient(zap.NewNop(), good)
	require.NoError(t, err)

	for name, mutate := range map[string]func(*plaid.Config){
		"no base url":         func(c *plaid.Config) { c.BaseURL = "" },
		"trailing slash":      func(c *plaid.Config) { c.BaseURL = plaid.SandboxBaseURL + "/" },
		"query":               func(c *plaid.Config) { c.BaseURL = plaid.SandboxBaseURL + "?x=1" },
		"relative":            func(c *plaid.Config) { c.BaseURL = "sandbox.plaid.com" },
		"no client id":        func(c *plaid.Config) { c.ClientID = "" },
		"no secret":           func(c *plaid.Config) { c.Secret = "" },
		"no client name":      func(c *plaid.Config) { c.Link.ClientName = "" },
		"client name too big": func(c *plaid.Config) { c.Link.ClientName = strings.Repeat("x", 31) },
		"redirect with query": func(c *plaid.Config) { c.Link.RedirectURI = "https://app.example.com/oauth?x=1" },
		"relative redirect":   func(c *plaid.Config) { c.Link.RedirectURI = "/oauth" },
		"relative webhook":    func(c *plaid.Config) { c.Link.Webhook = "/webhook" },
	} {
		t.Run(name, func(t *testing.T) {
			cfg := good
			mutate(&cfg)
			_, err := plaid.NewClient(zap.NewNop(), cfg)
			assert.ErrorIs(t, err, plaid.ErrInvalidArgument)
		})
	}

	_, err = plaid.NewClient(nil, good)
	assert.ErrorIs(t, err, plaid.ErrInvalidArgument)
}

func TestCreateLinkToken_IOS(t *testing.T) {
	r := newRecorder(t, http.StatusOK, linkTokenResponse)
	c := r.client(t)

	token, err := c.CreateLinkToken(context.Background(), testUserID, plaid.PlatformIOS)
	require.NoError(t, err)
	assert.Equal(t, "link-sandbox-af1a0311-da53-4636-b754-dd15cc058176", token.Token)
	assert.Equal(t, time.Date(2026, 11, 25, 14, 0, 0, 0, time.UTC), token.Expiration.UTC())
	assert.Equal(t, "req-1", token.RequestID)

	req := r.last(t)
	assert.Equal(t, http.MethodPost, req.method)
	assert.Equal(t, "/link/token/create", req.path)
	assert.Equal(t, "application/json", req.header.Get("Content-Type"))
	assert.Equal(t, plaid.DefaultAPIVersion, req.header.Get("Plaid-Version"))
	assert.Equal(t, jsonOf(t, map[string]any{
		"client_id":                     testClientID,
		"secret":                        testSecret,
		"client_name":                   "Flipcash",
		"language":                      "en",
		"country_codes":                 []string{"US"},
		"user":                          map[string]any{"client_user_id": testUserID},
		"products":                      []string{"auth"},
		"additional_consented_products": []string{"identity"},
		"webhook":                       "https://api.example.com/plaid/webhook",
		"redirect_uri":                  "https://app.example.com/plaid/oauth",
		"account_filters": map[string]any{
			"depository": map[string]any{"account_subtypes": []string{"checking", "savings"}},
		},
		"auth": map[string]any{
			"auth_type_select_enabled":        false,
			"automated_microdeposits_enabled": false,
			"instant_match_enabled":           true,
			"same_day_microdeposits_enabled":  false,
			"instant_microdeposits_enabled":   false,
			"reroute_to_credentials":          "FORCED",
			"database_match_enabled":          false,
			"database_insights_enabled":       false,
		},
	}), req.body)
}

func TestCreateLinkToken_Android(t *testing.T) {
	r := newRecorder(t, http.StatusOK, linkTokenResponse)
	c := r.client(t)

	_, err := c.CreateLinkToken(context.Background(), testUserID, plaid.PlatformAndroid)
	require.NoError(t, err)

	body := r.last(t).body
	assert.Equal(t, "com.example.app", body["android_package_name"])
	assert.NotContains(t, body, "redirect_uri")
	assert.Equal(t, []any{"auth"}, body["products"])
}

func TestCreateLinkToken_OmitsUnsetPlatformFieldsAndWebhook(t *testing.T) {
	r := newRecorder(t, http.StatusOK, linkTokenResponse)
	cfg := r.config()
	cfg.Link = plaid.LinkConfig{ClientName: "Flipcash"}
	c, err := plaid.NewClient(zap.NewNop(), cfg)
	require.NoError(t, err)

	for _, platform := range []plaid.Platform{plaid.PlatformIOS, plaid.PlatformAndroid} {
		_, err := c.CreateLinkToken(context.Background(), testUserID, platform)
		require.NoError(t, err)
		body := r.last(t).body
		assert.NotContains(t, body, "redirect_uri")
		assert.NotContains(t, body, "android_package_name")
		assert.NotContains(t, body, "webhook")
	}
}

func TestCreateUpdateLinkToken(t *testing.T) {
	for _, tc := range []struct {
		platform plaid.Platform
		field    string
		value    string
		absent   string
	}{
		{plaid.PlatformIOS, "redirect_uri", "https://app.example.com/plaid/oauth", "android_package_name"},
		{plaid.PlatformAndroid, "android_package_name", "com.example.app", "redirect_uri"},
	} {
		r := newRecorder(t, http.StatusOK, linkTokenResponse)
		c := r.client(t)

		_, err := c.CreateUpdateLinkToken(context.Background(), testUserID, testAccess, tc.platform)
		require.NoError(t, err)

		expected := map[string]any{
			"client_id":     testClientID,
			"secret":        testSecret,
			"client_name":   "Flipcash",
			"language":      "en",
			"country_codes": []string{"US"},
			"user":          map[string]any{"client_user_id": testUserID},
			"access_token":  testAccess,
			"update":        map[string]any{"account_selection_enabled": true},
			"account_filters": map[string]any{
				"depository": map[string]any{"account_subtypes": []string{"checking", "savings"}},
			},
			tc.field: tc.value,
		}
		assert.Equal(t, jsonOf(t, expected), r.last(t).body)
	}
}

func TestCreateLinkToken_RefusesBadArguments(t *testing.T) {
	r := newRecorder(t, http.StatusOK, linkTokenResponse)
	c := r.client(t)
	ctx := context.Background()

	_, err := c.CreateLinkToken(ctx, "", plaid.PlatformIOS)
	assert.ErrorIs(t, err, plaid.ErrInvalidArgument)
	_, err = c.CreateLinkToken(ctx, testUserID, plaid.Platform(0))
	assert.ErrorIs(t, err, plaid.ErrInvalidArgument)
	_, err = c.CreateUpdateLinkToken(ctx, testUserID, "", plaid.PlatformIOS)
	assert.ErrorIs(t, err, plaid.ErrInvalidArgument)
	_, err = c.ExchangePublicToken(ctx, "")
	assert.ErrorIs(t, err, plaid.ErrInvalidArgument)
	_, err = c.GetItem(ctx, "")
	assert.ErrorIs(t, err, plaid.ErrInvalidArgument)
	_, err = c.GetAccounts(ctx, "")
	assert.ErrorIs(t, err, plaid.ErrInvalidArgument)
	assert.ErrorIs(t, c.RemoveItem(ctx, ""), plaid.ErrInvalidArgument)
	_, err = c.GetInstitution(ctx, "")
	assert.ErrorIs(t, err, plaid.ErrInvalidArgument)
	_, err = c.CreateProcessorToken(ctx, testAccess, "", "dwolla")
	assert.ErrorIs(t, err, plaid.ErrInvalidArgument)
	_, err = c.CreateProcessorToken(ctx, testAccess, "acct", "")
	assert.ErrorIs(t, err, plaid.ErrInvalidArgument)

	assert.Zero(t, r.count(), "nothing reaches Plaid")
}

func TestExchangePublicToken(t *testing.T) {
	r := newRecorder(t, http.StatusOK, `{"access_token":"`+testAccess+`","item_id":"item-1","request_id":"req-2"}`)
	c := r.client(t)

	res, err := c.ExchangePublicToken(context.Background(), "public-sandbox-1")
	require.NoError(t, err)
	assert.Equal(t, &plaid.PublicTokenExchange{AccessToken: testAccess, ItemID: "item-1", RequestID: "req-2"}, res)

	req := r.last(t)
	assert.Equal(t, "/item/public_token/exchange", req.path)
	assert.Equal(t, jsonOf(t, map[string]any{
		"client_id": testClientID, "secret": testSecret, "public_token": "public-sandbox-1",
	}), req.body)
}

func TestGetItem(t *testing.T) {
	r := newRecorder(t, http.StatusOK, `{
		"item": {
			"item_id": "item-1",
			"institution_id": "ins_109508",
			"institution_name": "First Platypus Bank",
			"webhook": "",
			"auth_method": "INSTANT_AUTH",
			"error": {
				"error_type": "ITEM_ERROR",
				"error_code": "ITEM_LOGIN_REQUIRED",
				"error_code_reason": null,
				"error_message": "the login details of this item have changed",
				"display_message": null,
				"request_id": "req-x"
			},
			"available_products": ["identity"],
			"billed_products": ["auth"],
			"products": ["auth"],
			"consented_products": ["auth", "identity"],
			"consent_expiration_time": "2027-01-02T03:04:05Z",
			"update_type": "background"
		},
		"status": {"last_webhook": null},
		"request_id": "req-3"
	}`)
	c := r.client(t)

	item, err := c.GetItem(context.Background(), testAccess)
	require.NoError(t, err)
	assert.Equal(t, "item-1", item.ItemID)
	assert.Equal(t, "ins_109508", item.InstitutionID)
	assert.Equal(t, "First Platypus Bank", item.InstitutionName)
	assert.Equal(t, "INSTANT_AUTH", item.AuthMethod)
	assert.Equal(t, []string{"auth"}, item.Products)
	assert.Equal(t, []string{"auth", "identity"}, item.ConsentedProducts)
	require.NotNil(t, item.ConsentExpirationTime)
	assert.Equal(t, time.Date(2027, 1, 2, 3, 4, 5, 0, time.UTC), item.ConsentExpirationTime.UTC())
	require.NotNil(t, item.Error)
	assert.Equal(t, plaid.ErrorTypeItemError, item.Error.ErrorType)
	assert.True(t, plaid.IsItemLoginRequired(item.Error.Err()))

	req := r.last(t)
	assert.Equal(t, "/item/get", req.path)
	assert.Equal(t, jsonOf(t, map[string]any{
		"client_id": testClientID, "secret": testSecret, "access_token": testAccess,
	}), req.body)

	r.respond(http.StatusOK, `{"item":{"item_id":"item-1","error":null,"consent_expiration_time":null,"institution_id":null},"request_id":"req-4"}`)
	item, err = c.GetItem(context.Background(), testAccess)
	require.NoError(t, err)
	assert.Nil(t, item.Error)
	assert.Nil(t, item.Error.Err())
	assert.Nil(t, item.ConsentExpirationTime)
	assert.Empty(t, item.InstitutionID)
}

func TestGetAccounts(t *testing.T) {
	r := newRecorder(t, http.StatusOK, `{
		"accounts": [
			{
				"account_id": "acct-1",
				"balances": {"available": 100, "current": 110, "iso_currency_code": "USD"},
				"mask": "0000",
				"name": "Plaid Checking",
				"official_name": "Plaid Gold Standard 0% Interest Checking",
				"type": "depository",
				"subtype": "checking",
				"persistent_account_id": "8cfb8beb89b774ee43b090625f0d61d0814322b43bff984eaf60386e"
			},
			{
				"account_id": "acct-2",
				"balances": {"available": null, "current": 210},
				"mask": "1111",
				"name": "Plaid Saving",
				"official_name": null,
				"type": "depository",
				"subtype": "savings"
			}
		],
		"item": {"item_id": "item-1", "institution_id": "ins_56"},
		"request_id": "req-5"
	}`)
	c := r.client(t)

	res, err := c.GetAccounts(context.Background(), testAccess)
	require.NoError(t, err)
	assert.Equal(t, "item-1", res.Item.ItemID)
	assert.Equal(t, "req-5", res.RequestID)
	assert.Equal(t, []plaid.Account{
		{
			AccountID:           "acct-1",
			PersistentAccountID: "8cfb8beb89b774ee43b090625f0d61d0814322b43bff984eaf60386e",
			Name:                "Plaid Checking",
			OfficialName:        "Plaid Gold Standard 0% Interest Checking",
			Mask:                "0000",
			Type:                plaid.AccountTypeDepository,
			Subtype:             "checking",
		},
		{
			AccountID: "acct-2",
			Name:      "Plaid Saving",
			Mask:      "1111",
			Type:      plaid.AccountTypeDepository,
			Subtype:   "savings",
		},
	}, res.Accounts)

	req := r.last(t)
	assert.Equal(t, "/accounts/get", req.path)
	assert.Equal(t, jsonOf(t, map[string]any{
		"client_id": testClientID, "secret": testSecret, "access_token": testAccess,
	}), req.body)
}

func TestRemoveItem(t *testing.T) {
	r := newRecorder(t, http.StatusOK, `{"request_id":"req-6"}`)
	c := r.client(t)

	require.NoError(t, c.RemoveItem(context.Background(), testAccess))
	req := r.last(t)
	assert.Equal(t, "/item/remove", req.path)
	assert.Equal(t, jsonOf(t, map[string]any{
		"client_id": testClientID, "secret": testSecret, "access_token": testAccess,
	}), req.body)
}

func TestGetInstitution(t *testing.T) {
	r := newRecorder(t, http.StatusOK, `{"institution":{"institution_id":"ins_56","name":"Chase","products":["auth"],"country_codes":["US"],"oauth":true},"request_id":"req-7"}`)
	c := r.client(t)

	inst, err := c.GetInstitution(context.Background(), "ins_56")
	require.NoError(t, err)
	assert.Equal(t, &plaid.Institution{InstitutionID: "ins_56", Name: "Chase", OAuth: true}, inst)

	req := r.last(t)
	assert.Equal(t, "/institutions/get_by_id", req.path)
	assert.Equal(t, jsonOf(t, map[string]any{
		"client_id": testClientID, "secret": testSecret, "institution_id": "ins_56", "country_codes": []string{"US"},
	}), req.body)
}

func TestCreateProcessorToken(t *testing.T) {
	r := newRecorder(t, http.StatusOK, `{"processor_token":"processor-sandbox-0asd1-a92nc","request_id":"req-8"}`)
	c := r.client(t)

	token, err := c.CreateProcessorToken(context.Background(), testAccess, "acct-1", "dwolla")
	require.NoError(t, err)
	assert.Equal(t, "processor-sandbox-0asd1-a92nc", token)

	req := r.last(t)
	assert.Equal(t, "/processor/token/create", req.path)
	assert.Equal(t, jsonOf(t, map[string]any{
		"client_id": testClientID, "secret": testSecret, "access_token": testAccess,
		"account_id": "acct-1", "processor": "dwolla",
	}), req.body)
}

func TestAPIVersion_Configurable(t *testing.T) {
	r := newRecorder(t, http.StatusOK, `{"request_id":"req"}`)
	cfg := r.config()
	cfg.APIVersion = "2099-01-01"
	c, err := plaid.NewClient(zap.NewNop(), cfg)
	require.NoError(t, err)

	require.NoError(t, c.RemoveItem(context.Background(), testAccess))
	assert.Equal(t, "2099-01-01", r.last(t).header.Get("Plaid-Version"))
}

func TestErrors_Parsed(t *testing.T) {
	r := newRecorder(t, http.StatusBadRequest, `{
		"error_type": "ITEM_ERROR",
		"error_code": "ITEM_LOGIN_REQUIRED",
		"error_message": "the login details of this item have changed",
		"display_message": "Please sign in again.",
		"request_id": "HNTDNrA8F1shFEW"
	}`)
	c := r.client(t)

	_, err := c.GetAccounts(context.Background(), testAccess)
	require.Error(t, err)
	perr, ok := plaid.AsError(err)
	require.True(t, ok)
	assert.Equal(t, &plaid.Error{
		StatusCode:     http.StatusBadRequest,
		Type:           plaid.ErrorTypeItemError,
		Code:           plaid.CodeItemLoginRequired,
		Message:        "the login details of this item have changed",
		DisplayMessage: "Please sign in again.",
		RequestID:      "HNTDNrA8F1shFEW",
	}, perr)
	assert.Equal(t, "plaid: http 400 ITEM_ERROR ITEM_LOGIN_REQUIRED (request id HNTDNrA8F1shFEW)", err.Error())
	assert.Equal(t, plaid.CodeItemLoginRequired, plaid.ErrorCode(err))
	assert.True(t, plaid.IsItemLoginRequired(err))
	assert.True(t, plaid.IsItemLoginRequired(fmt.Errorf("wrapped: %w", err)))
	assert.False(t, plaid.IsRetryable(err))

	// A body that is not Plaid's still yields the status.
	r.respond(http.StatusBadGateway, "<html>bad gateway</html>")
	_, err = c.GetItem(context.Background(), testAccess)
	perr, ok = plaid.AsError(err)
	require.True(t, ok)
	assert.Equal(t, &plaid.Error{StatusCode: http.StatusBadGateway}, perr)
	assert.Equal(t, "plaid: http 502", err.Error())
	assert.True(t, plaid.IsRetryable(err))
}

func TestErrors_Predicates(t *testing.T) {
	mk := func(status int, errorType plaid.ErrorType, code string) error {
		return &plaid.Error{StatusCode: status, Type: errorType, Code: code}
	}
	for _, tc := range []struct {
		err       error
		predicate func(error) bool
		retryable bool
	}{
		{mk(400, plaid.ErrorTypeItemError, "ITEM_NOT_FOUND"), plaid.IsItemNotFound, false},
		{mk(400, plaid.ErrorTypeItemError, "ITEM_LOGIN_REQUIRED"), plaid.IsItemLoginRequired, false},
		{mk(400, plaid.ErrorTypeItemError, "ITEM_LOCKED"), plaid.IsItemLocked, false},
		{mk(400, plaid.ErrorTypeItemError, "NO_ACCOUNTS"), plaid.IsNoAccounts, false},
		{mk(400, plaid.ErrorTypeInvalidInput, "INVALID_PUBLIC_TOKEN"), plaid.IsInvalidPublicToken, false},
		{mk(400, plaid.ErrorTypeInvalidInput, "INVALID_ACCESS_TOKEN"), plaid.IsInvalidAccessToken, false},
		{mk(429, plaid.ErrorTypeRateLimitExceeded, "ITEM_GET_LIMIT"), plaid.IsRateLimited, true},
		{mk(429, "", ""), plaid.IsRateLimited, true},
		{mk(400, plaid.ErrorTypeInstitutionError, "INSTITUTION_DOWN"), plaid.IsInstitutionDown, true},
		{mk(400, plaid.ErrorTypeInstitutionError, "INSTITUTION_NOT_RESPONDING"), plaid.IsInstitutionDown, true},
		{mk(400, plaid.ErrorTypeInstitutionError, "INSTITUTION_NOT_AVAILABLE"), plaid.IsInstitutionDown, true},
		{mk(400, plaid.ErrorTypeInvalidInput, "INVALID_API_KEYS"), plaid.IsConfigError, false},
		{mk(400, plaid.ErrorTypeInvalidInput, "UNAUTHORIZED_ENVIRONMENT"), plaid.IsConfigError, false},
		{mk(400, plaid.ErrorTypeInvalidInput, "INVALID_PRODUCT"), plaid.IsConfigError, false},
		{mk(500, plaid.ErrorTypeAPIError, "INTERNAL_SERVER_ERROR"), plaid.IsRetryable, true},
	} {
		t.Run(plaid.ErrorCode(tc.err)+fmt.Sprint(tc.err.(*plaid.Error).StatusCode), func(t *testing.T) {
			assert.True(t, tc.predicate(tc.err))
			assert.Equal(t, tc.retryable, plaid.IsRetryable(tc.err))
		})
	}

	other := mk(400, plaid.ErrorTypeItemError, "ITEM_LOGIN_REQUIRED")
	for _, p := range []func(error) bool{
		plaid.IsItemNotFound, plaid.IsItemLocked, plaid.IsNoAccounts, plaid.IsInvalidPublicToken,
		plaid.IsInvalidAccessToken, plaid.IsRateLimited, plaid.IsInstitutionDown, plaid.IsConfigError,
	} {
		assert.False(t, p(other))
	}

	assert.Equal(t, "", plaid.ErrorCode(errors.New("not plaid")))
	assert.True(t, plaid.IsRetryable(errors.New("transport failure")))
	assert.False(t, plaid.IsRetryable(context.Canceled))
	assert.False(t, plaid.IsRetryable(fmt.Errorf("%w: x", plaid.ErrInvalidArgument)))
	assert.False(t, plaid.IsRetryable(nil))
}

func TestMalformedResponse(t *testing.T) {
	r := newRecorder(t, http.StatusOK, `{"access_token": "`+testAccess+`", "item_id": 7}`)
	c := r.client(t)

	_, err := c.ExchangePublicToken(context.Background(), "public-sandbox-1")
	assert.ErrorIs(t, err, plaid.ErrMalformedResponse)
	assert.NotContains(t, err.Error(), testAccess)
	assert.True(t, plaid.IsRetryable(err))

	r.respond(http.StatusOK, `{"request_id":"req-9"}`)
	_, err = c.CreateLinkToken(context.Background(), testUserID, plaid.PlatformIOS)
	assert.ErrorIs(t, err, plaid.ErrMalformedResponse)
}

func TestContextCanceled(t *testing.T) {
	r := newRecorder(t, http.StatusOK, `{"request_id":"req"}`)
	c := r.client(t)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := c.RemoveItem(ctx, testAccess)
	assert.ErrorIs(t, err, context.Canceled)
	assert.False(t, plaid.IsRetryable(err))
}

// TestNoSecretsInLogsOrErrors runs every call, succeeding and failing, with
// tokens in both requests and responses (and in Plaid's free-text error
// message), and checks that no token or the secret reaches a log line or an
// error string.
func TestNoSecretsInLogsOrErrors(t *testing.T) {
	const (
		publicToken    = "public-sandbox-SECRET-PUBLIC-TOKEN"
		accessToken    = "access-sandbox-SECRET-ACCESS-TOKEN"
		linkToken      = "link-sandbox-SECRET-LINK-TOKEN"
		processorToken = "processor-sandbox-SECRET-PROCESSOR-TOKEN"
	)
	secrets := []string{publicToken, accessToken, linkToken, processorToken, testSecret, "SECRET"}

	core, logs := observer.New(zapcore.DebugLevel)
	r := newRecorder(t, http.StatusOK, "")
	c, err := plaid.NewClient(zap.New(core), r.config())
	require.NoError(t, err)
	sandbox, err := plaid.NewSandbox(c)
	require.NoError(t, err)

	ctx := context.Background()
	calls := map[string]func() error{
		"CreateLinkToken": func() error {
			_, err := c.CreateLinkToken(ctx, testUserID, plaid.PlatformIOS)
			return err
		},
		"CreateUpdateLinkToken": func() error {
			_, err := c.CreateUpdateLinkToken(ctx, testUserID, accessToken, plaid.PlatformAndroid)
			return err
		},
		"ExchangePublicToken": func() error {
			_, err := c.ExchangePublicToken(ctx, publicToken)
			return err
		},
		"GetItem": func() error {
			_, err := c.GetItem(ctx, accessToken)
			return err
		},
		"GetAccounts": func() error {
			_, err := c.GetAccounts(ctx, accessToken)
			return err
		},
		"RemoveItem": func() error { return c.RemoveItem(ctx, accessToken) },
		"GetInstitution": func() error {
			_, err := c.GetInstitution(ctx, "ins_56")
			return err
		},
		"CreateProcessorToken": func() error {
			_, err := c.CreateProcessorToken(ctx, accessToken, "acct-1", "dwolla")
			return err
		},
		"SandboxCreatePublicToken": func() error {
			_, err := sandbox.CreatePublicToken(ctx, "ins_109508")
			return err
		},
		"SandboxResetLogin":  func() error { return sandbox.ResetLogin(ctx, accessToken) },
		"SandboxFireWebhook": func() error { return sandbox.FireWebhook(ctx, accessToken, "NEW_ACCOUNTS_AVAILABLE") },
	}

	responses := []struct {
		status int
		body   string
	}{
		// Success, with every token in the body.
		{http.StatusOK, fmt.Sprintf(`{"link_token":%q,"expiration":"2026-11-25T14:00:00Z","access_token":%q,"item_id":"item-1",`+
			`"public_token":%q,"processor_token":%q,"item":{"item_id":"item-1"},"institution":{"institution_id":"ins_56","name":"Chase"},"request_id":"req-ok"}`,
			linkToken, accessToken, publicToken, processorToken)},
		// A Plaid error whose free text quotes tokens.
		{http.StatusBadRequest, fmt.Sprintf(`{"error_type":"INVALID_INPUT","error_code":"INVALID_ACCESS_TOKEN",`+
			`"error_message":"could not find %s or %s or %s","display_message":%q,"request_id":"req-err"}`,
			accessToken, publicToken, linkToken, processorToken)},
		// A 200 that does not decode, with tokens in it.
		{http.StatusOK, fmt.Sprintf(`{"link_token":%q,"access_token":%q,"public_token":%q,"processor_token":%q,"item_id":7,`+
			`"item":"%s","institution":"%s","expiration":"%s","request_id":"req-bad"}`,
			linkToken, accessToken, publicToken, processorToken, accessToken, accessToken, accessToken)},
		// A proxy's page quoting a token.
		{http.StatusBadGateway, "<html>" + accessToken + "</html>"},
	}

	for _, res := range responses {
		r.respond(res.status, res.body)
		for name, call := range calls {
			err := call()
			if err == nil {
				continue
			}
			for _, s := range secrets {
				assert.NotContains(t, err.Error(), s, "%s error (http %d)", name, res.status)
				assert.NotContains(t, fmt.Sprintf("%+v", err), s, "%s error (http %d)", name, res.status)
			}
		}
	}

	entries := logs.All()
	require.NotEmpty(t, entries, "the client logs its calls")
	sawFailure := false
	for _, entry := range entries {
		line := entry.Message
		for _, f := range entry.Context {
			assert.Contains(t, []string{"endpoint", "http_status", "request_id", "error_type", "error_code"}, f.Key,
				"log fields are limited to the endpoint, status, request id, error type and code")
			line += fmt.Sprintf(" %s=%v%s", f.Key, f.Integer, f.String)
			if f.Key == "error_code" && f.String == "INVALID_ACCESS_TOKEN" {
				sawFailure = true
			}
		}
		for _, s := range secrets {
			assert.NotContains(t, line, s)
		}
	}
	assert.True(t, sawFailure, "a Plaid error is logged with its code")
}

func TestSandbox_RefusesProduction(t *testing.T) {
	c, err := plaid.NewClient(zap.NewNop(), plaid.Config{
		BaseURL:  plaid.ProductionBaseURL,
		ClientID: "id",
		Secret:   "secret",
		Link:     plaid.LinkConfig{ClientName: "Flipcash"},
	})
	require.NoError(t, err)
	_, err = plaid.NewSandbox(c)
	assert.ErrorIs(t, err, plaid.ErrInvalidArgument)
}

func TestSandbox_Encoding(t *testing.T) {
	r := newRecorder(t, http.StatusOK, `{"public_token":"public-sandbox-1","request_id":"req"}`)
	c := r.client(t)
	sandbox, err := plaid.NewSandbox(c)
	require.NoError(t, err)
	ctx := context.Background()

	token, err := sandbox.CreatePublicToken(ctx, "ins_109508")
	require.NoError(t, err)
	assert.Equal(t, "public-sandbox-1", token)
	assert.Equal(t, "/sandbox/public_token/create", r.last(t).path)
	assert.Equal(t, jsonOf(t, map[string]any{
		"client_id": testClientID, "secret": testSecret, "institution_id": "ins_109508", "initial_products": []string{"auth"},
	}), r.last(t).body)

	require.NoError(t, sandbox.ResetLogin(ctx, testAccess))
	assert.Equal(t, "/sandbox/item/reset_login", r.last(t).path)
	assert.Equal(t, testAccess, r.last(t).body["access_token"])

	require.NoError(t, sandbox.FireWebhook(ctx, testAccess, "NEW_ACCOUNTS_AVAILABLE"))
	assert.Equal(t, "/sandbox/item/fire_webhook", r.last(t).path)
	assert.Equal(t, "NEW_ACCOUNTS_AVAILABLE", r.last(t).body["webhook_code"])
}
