// Package plaid is a client for the parts of Plaid's API
// (https://plaid.com/docs/api/) through which a Flipcash user links their
// own US bank accounts: Plaid's Auth product, so that a later Coinbase
// offramp can pay out to an account Plaid has verified. Flipcash never reads
// account or routing numbers; it hands Coinbase a processor token instead.
//
// The client is hand-written over Plaid's JSON-over-HTTPS API (every
// endpoint is a POST with a JSON body carrying client_id and secret) rather
// than built on plaid-go. It covers what the bank service uses:
//
//   - /link/token/create, in two shapes: a new link (CreateLinkToken) and
//     update mode for an existing Item (CreateUpdateLinkToken). Both carry
//     Flipcash's fixed Link policy, which is this package's, not the
//     caller's (see link.go);
//   - /item/public_token/exchange, /item/get, /accounts/get, /item/remove;
//   - /institutions/get_by_id, for a bank's name;
//   - /processor/token/create, for handing one account to a payment
//     processor.
//
// Sandbox-only endpoints are on Sandbox, for tests. Webhooks (and their JWT
// verification) are not handled here yet.
//
// API is the interface the bank service consumes; Client implements it, and
// plaidtest is an in-process fake of Plaid's API that a real Client talks to.
//
// # Logging and errors
//
// Plaid's tokens are credentials: an access token reads a user's bank data
// for as long as the Item lives, a public token becomes one, a processor
// token lets a third party pull account and routing numbers, and a link
// token opens Link for a user. So the client never logs, and never puts in
// an error, any request or response body, any token, or the secret. A log
// line carries the endpoint, the HTTP status, and Plaid's request_id,
// error_type and error_code, nothing else. Error() is built from the same
// fields and is safe to log; Plaid's free-text error_message and
// display_message are kept on Error apart, for the caller to decide about,
// and are never part of Error().
//
// Calls honour the context and are never retried here: callers decide,
// through IsRetryable and the predicates in errors.go. Timeouts come from
// the http.Client (DefaultTimeout when none is given) and the context.
//
// # Plaid behaviours callers rely on
//
//   - An Item is a user's connection to one bank (institution_id); its
//     access token never expires on its own, but an Item can be removed:
//     by /item/remove, by the user at my.plaid.com, or by Plaid on a data
//     deletion request. Every later call on it then fails ITEM_NOT_FOUND
//     (IsItemNotFound), which is what "gone at Plaid" means.
//   - /item/get answers 200 with item.error set when the Item needs
//     attention (ITEM_LOGIN_REQUIRED and the rest): an Item in error is not
//     a failed call there. Item-data endpoints such as /accounts/get fail
//     with the error instead. Item.Error.Err converts it so the same
//     predicates apply.
//   - A public token expires 30 minutes after Link returns it, and an
//     unknown or expired one fails INVALID_PUBLIC_TOKEN. Plaid's docs do
//     not say what exchanging one a second time does; the fake and the
//     plaidsandbox test take it to fail INVALID_PUBLIC_TOKEN too, so a
//     caller must not count on exchanging twice.
//   - A link token expires after 4 hours for a new link and 30 minutes in
//     update mode (LinkToken.Expiration says which).
//   - An account_id is stable only while Plaid can reconcile the account
//     with the bank; Plaid assigns a new one when it cannot (a renamed
//     account, say), and a re-linked bank gets new ones. Closed accounts
//     drop out of /accounts/get. persistent_account_id survives both, but
//     only at banks that issue tokenized account numbers (Chase, PNC, US
//     Bank), and is empty elsewhere.
//   - consent_expiration_time is set only at the few US banks whose OAuth
//     consent expires.
//   - Plaid rate-limits per Item and per endpoint (ITEM_GET_LIMIT,
//     ACCOUNTS_LIMIT and others, all of error_type RATE_LIMIT_EXCEEDED,
//     HTTP 429): IsRateLimited.
package plaid

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"go.uber.org/zap"

	"github.com/code-payments/ocp-server/metrics"
)

const (
	// SandboxBaseURL is Plaid's Sandbox, with test institutions and users.
	SandboxBaseURL = "https://sandbox.plaid.com"
	// ProductionBaseURL is live Plaid.
	ProductionBaseURL = "https://production.plaid.com"

	// DefaultAPIVersion is the Plaid-Version every request pins when Config
	// names none: the newest version, which Plaid's own client libraries
	// pin. Without the header Plaid uses the default set in its Dashboard,
	// which can change underneath the client.
	DefaultAPIVersion = "2020-09-14"

	// DefaultTimeout bounds a request when Config.HTTPClient is nil.
	DefaultTimeout = 30 * time.Second

	apiVersionHeader = "Plaid-Version"

	// maxResponseBytes bounds how much of a response is read. Plaid's
	// responses for these endpoints are a few kilobytes.
	maxResponseBytes = 1 << 20

	metricsStructName = "plaid.client"
)

// Endpoints, which are also what a log line names.
const (
	pathLinkTokenCreate      = "/link/token/create"
	pathPublicTokenExchange  = "/item/public_token/exchange"
	pathItemGet              = "/item/get"
	pathAccountsGet          = "/accounts/get"
	pathItemRemove           = "/item/remove"
	pathInstitutionsGetByID  = "/institutions/get_by_id"
	pathProcessorTokenCreate = "/processor/token/create"
)

// Config configures a Client.
type Config struct {
	// BaseURL is SandboxBaseURL, ProductionBaseURL, or a test server's. It
	// must be an absolute http(s) URL with no query, fragment or trailing
	// slash.
	BaseURL string
	// ClientID and Secret are Flipcash's Plaid API keys for that
	// environment. Plaid's secrets differ per environment.
	ClientID string
	Secret   string
	// APIVersion is sent as the Plaid-Version header; empty for
	// DefaultAPIVersion.
	APIVersion string
	// HTTPClient carries timeouts and transport; nil for a client with
	// DefaultTimeout.
	HTTPClient *http.Client
	// Link holds the deployment's Link settings.
	Link LinkConfig
}

// Client calls Plaid's API as one Plaid client ID. It is safe for concurrent
// use.
type Client struct {
	log        *zap.Logger
	httpClient *http.Client
	baseURL    *url.URL
	clientID   string
	secret     string
	apiVersion string
	link       LinkConfig
}

// NewClient returns a client for cfg. Bad configuration is refused, not
// cleaned up.
func NewClient(log *zap.Logger, cfg Config) (*Client, error) {
	if log == nil {
		return nil, fmt.Errorf("%w: nil logger", ErrInvalidArgument)
	}
	u, err := url.Parse(cfg.BaseURL)
	if err != nil || (u.Scheme != "https" && u.Scheme != "http") || u.Host == "" || u.User != nil ||
		u.RawQuery != "" || u.Fragment != "" || strings.HasSuffix(u.Path, "/") || u.Opaque != "" {
		return nil, fmt.Errorf("%w: base url must be an absolute http(s) url with no query, fragment or trailing slash", ErrInvalidArgument)
	}
	if cfg.ClientID == "" {
		return nil, fmt.Errorf("%w: empty client id", ErrInvalidArgument)
	}
	if cfg.Secret == "" {
		return nil, fmt.Errorf("%w: empty secret", ErrInvalidArgument)
	}
	if err := cfg.Link.validate(); err != nil {
		return nil, err
	}
	httpClient := cfg.HTTPClient
	if httpClient == nil {
		httpClient = &http.Client{Timeout: DefaultTimeout}
	}
	apiVersion := cfg.APIVersion
	if apiVersion == "" {
		apiVersion = DefaultAPIVersion
	}
	return &Client{
		log:        log,
		httpClient: httpClient,
		baseURL:    u,
		clientID:   cfg.ClientID,
		secret:     cfg.Secret,
		apiVersion: apiVersion,
		link:       cfg.Link,
	}, nil
}

// credentials authenticates a request. Plaid takes them in the body (or in
// headers); every request struct embeds them.
type credentials struct {
	ClientID string `json:"client_id"`
	Secret   string `json:"secret"`
}

func (c *Client) credentials() credentials {
	return credentials{ClientID: c.clientID, Secret: c.secret}
}

// trace wraps one API method in the repo's method tracing.
func trace[T any](ctx context.Context, method string, call func() (T, error)) (T, error) {
	tracer := metrics.TraceMethodCall(ctx, metricsStructName, method)
	defer tracer.End()

	res, err := call()
	tracer.OnError(err)
	return res, err
}

// do POSTs body to path and decodes a 200 response into out (when out is
// not nil). Any other status is an *Error.
//
// Nothing read from or written to the wire is logged or put into an error,
// except the fields of Error() (see the package doc).
func (c *Client) do(ctx context.Context, path string, body any, out any) error {
	encoded, err := json.Marshal(body)
	if err != nil {
		// Never wrap the marshal error: it can quote a value.
		return fmt.Errorf("%w: request could not be encoded", ErrInvalidArgument)
	}

	u := *c.baseURL
	u.Path = c.baseURL.Path + path

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, u.String(), bytes.NewReader(encoded))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	req.Header.Set(apiVersionHeader, c.apiVersion)

	// A transport error names the URL (base URL and endpoint, no
	// credentials) and its cause; the body is never part of it.
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	data, err := io.ReadAll(io.LimitReader(resp.Body, maxResponseBytes))
	if err != nil {
		return err
	}

	if resp.StatusCode != http.StatusOK {
		perr := decodeError(resp.StatusCode, data)
		c.log.Info(
			"plaid request failed",
			zap.String("endpoint", path),
			zap.Int("http_status", perr.StatusCode),
			zap.String("request_id", perr.RequestID),
			zap.String("error_type", string(perr.Type)),
			zap.String("error_code", perr.Code),
		)
		return perr
	}

	var meta struct {
		RequestID string `json:"request_id"`
	}
	_ = json.Unmarshal(data, &meta)
	c.log.Debug("plaid request succeeded", zap.String("endpoint", path), zap.String("request_id", meta.RequestID))

	if out == nil {
		return nil
	}
	if err := json.Unmarshal(data, out); err != nil {
		// The decoder's message can quote the body, which holds tokens;
		// leave it out.
		return fmt.Errorf("%w: %s response could not be decoded (request id %s)", ErrMalformedResponse, path, meta.RequestID)
	}
	return nil
}

// decodeError reads Plaid's error body if there is one. A body that is not
// one (a proxy's page) still yields an *Error carrying the status.
func decodeError(statusCode int, data []byte) *Error {
	var body struct {
		ErrorType      ErrorType `json:"error_type"`
		ErrorCode      string    `json:"error_code"`
		ErrorMessage   string    `json:"error_message"`
		DisplayMessage *string   `json:"display_message"`
		RequestID      string    `json:"request_id"`
	}
	_ = json.Unmarshal(data, &body)
	e := &Error{
		StatusCode: statusCode,
		Type:       body.ErrorType,
		Code:       body.ErrorCode,
		Message:    body.ErrorMessage,
		RequestID:  body.RequestID,
	}
	if body.DisplayMessage != nil {
		e.DisplayMessage = *body.DisplayMessage
	}
	return e
}

func requireNonEmpty(name, value string) error {
	if value == "" {
		return fmt.Errorf("%w: empty %s", ErrInvalidArgument, name)
	}
	return nil
}
