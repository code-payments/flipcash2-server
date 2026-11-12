// Package customers is a client for Coinbase Developer Platform's Customers
// API (https://docs.cdp.coinbase.com/customers-kyc/overview), through which
// Coinbase verifies the identity of a Flipcash user (KYC) so they can later
// cash out to a bank account. The server collects the user's information and
// passes it on server to server; Coinbase decides.
//
// The client covers create, get, update and delete, which is all the server
// uses: list and the requirements preview are not built. It speaks the REST
// API directly with typed requests and responses (model.go), signing each
// request with the shared CDP JWT (package cdp).
//
// The client never logs, and never puts a request body or any submitted
// value into an error: see Error for what an error carries. Timeouts and
// retries are the caller's, through the http.Client, the context and
// IsRetryable.
//
// customerstest is an in-process fake of the API for tests.
package customers

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"strings"

	"github.com/google/uuid"

	"github.com/code-payments/ocp-server/metrics"

	"github.com/code-payments/flipcash2-server/cdp"
)

const (
	// SandboxBaseURL is the CDP Sandbox, where Coinbase's magic values
	// force each verification outcome.
	SandboxBaseURL = "https://sandbox.cdp.coinbase.com/platform"
	// ProductionBaseURL is live CDP.
	ProductionBaseURL = "https://api.cdp.coinbase.com/platform"

	customersPath = "/v2/customers"

	idempotencyKeyHeader = "X-Idempotency-Key"

	// maxResponseBytes bounds how much of a response is read. A customer
	// is a few kilobytes.
	maxResponseBytes = 1 << 20

	metricsStructName = "cdp.customers.client"
)

// customerIDPattern is Coinbase's CustomerId. Checking it keeps a caller's
// value from ever changing the request's path.
var customerIDPattern = regexp.MustCompile(`^customer_[a-f0-9\-]{36}$`)

// IsCustomerID reports whether id has the shape of a Coinbase customer ID,
// "customer_" followed by 36 lowercase hex digits and dashes. It judges the
// shape alone, not whether Coinbase knows the customer.
func IsCustomerID(id string) bool {
	return customerIDPattern.MatchString(id)
}

// Client calls the Customers API as one CDP API key. It is safe for
// concurrent use.
type Client struct {
	httpClient *http.Client
	baseURL    *url.URL
	keyID      string
	privateKey ed25519.PrivateKey
}

// NewClient returns a client for the API at baseURL (SandboxBaseURL,
// ProductionBaseURL, or a test server's), authenticating as the CDP API key
// keyID / privateKey. httpClient carries the caller's timeouts and
// transport.
//
// baseURL must be an absolute http(s) URL with no query, fragment or
// trailing slash; it is refused rather than cleaned up.
func NewClient(httpClient *http.Client, baseURL string, keyID string, privateKey ed25519.PrivateKey) (*Client, error) {
	if httpClient == nil {
		return nil, fmt.Errorf("%w: nil http client", ErrInvalidArgument)
	}
	u, err := url.Parse(baseURL)
	if err != nil || (u.Scheme != "https" && u.Scheme != "http") || u.Host == "" || u.User != nil ||
		u.RawQuery != "" || u.Fragment != "" || strings.HasSuffix(u.Path, "/") || u.Opaque != "" {
		return nil, fmt.Errorf("%w: base url must be an absolute http(s) url with no query, fragment or trailing slash", ErrInvalidArgument)
	}
	if keyID == "" {
		return nil, fmt.Errorf("%w: empty api key id", ErrInvalidArgument)
	}
	if len(privateKey) != ed25519.PrivateKeySize {
		return nil, fmt.Errorf("%w: private key is not an ed25519 private key", ErrInvalidArgument)
	}
	return &Client{
		httpClient: httpClient,
		baseURL:    u,
		keyID:      keyID,
		privateKey: privateKey,
	}, nil
}

// Create creates an individual customer from input, which may be empty (a
// customer with nothing requested), or request capabilities and submit
// identity in the same call.
//
// idempotencyKey is required, a UUID v4 (Coinbase's documented format): a
// create sent again under the same key returns the customer the first one
// made, so a create whose response was lost can be retried safely. The same
// key with a different input is a 422 ErrorTypeIdempotency.
func (c *Client) Create(ctx context.Context, idempotencyKey string, input *CustomerInput) (*Customer, error) {
	tracer := metrics.TraceMethodCall(ctx, metricsStructName, "Create")
	defer tracer.End()

	res, err := c.create(ctx, idempotencyKey, input)
	tracer.OnError(err)
	return res, err
}

func (c *Client) create(ctx context.Context, idempotencyKey string, input *CustomerInput) (*Customer, error) {
	if err := validateIdempotencyKey(idempotencyKey); err != nil {
		return nil, err
	}
	if input == nil {
		input = &CustomerInput{}
	}
	if err := validateInput(input); err != nil {
		return nil, err
	}
	body := struct {
		Type CustomerType `json:"type"`
		*CustomerInput
	}{CustomerTypeIndividual, input}

	var customer Customer
	if err := c.do(ctx, http.MethodPost, customersPath, idempotencyKey, body, http.StatusCreated, &customer); err != nil {
		return nil, err
	}
	return &customer, nil
}

// Get reads a customer.
func (c *Client) Get(ctx context.Context, customerID string) (*Customer, error) {
	tracer := metrics.TraceMethodCall(ctx, metricsStructName, "Get")
	defer tracer.End()

	res, err := c.get(ctx, customerID)
	tracer.OnError(err)
	return res, err
}

func (c *Client) get(ctx context.Context, customerID string) (*Customer, error) {
	if err := validateCustomerID(customerID); err != nil {
		return nil, err
	}
	var customer Customer
	if err := c.do(ctx, http.MethodGet, customersPath+"/"+customerID, "", nil, http.StatusOK, &customer); err != nil {
		return nil, err
	}
	return &customer, nil
}

// Update applies input to a customer: what it sets is submitted, the rest is
// left as it was. It returns the customer after the update, whose
// requirements may already reflect it (a submitted field pending, the tos
// requirement appearing once the address is in).
//
// idempotencyKey is optional (empty for none); when set it must be a UUID v4.
func (c *Client) Update(ctx context.Context, customerID string, idempotencyKey string, input *CustomerInput) (*Customer, error) {
	tracer := metrics.TraceMethodCall(ctx, metricsStructName, "Update")
	defer tracer.End()

	res, err := c.update(ctx, customerID, idempotencyKey, input)
	tracer.OnError(err)
	return res, err
}

func (c *Client) update(ctx context.Context, customerID string, idempotencyKey string, input *CustomerInput) (*Customer, error) {
	if err := validateCustomerID(customerID); err != nil {
		return nil, err
	}
	if idempotencyKey != "" {
		if err := validateIdempotencyKey(idempotencyKey); err != nil {
			return nil, err
		}
	}
	if input == nil {
		return nil, fmt.Errorf("%w: nil update", ErrInvalidArgument)
	}
	if err := validateInput(input); err != nil {
		return nil, err
	}
	var customer Customer
	if err := c.do(ctx, http.MethodPut, customersPath+"/"+customerID, idempotencyKey, input, http.StatusOK, &customer); err != nil {
		return nil, err
	}
	return &customer, nil
}

// Delete deletes a customer and everything Coinbase holds for it. Deleting
// one already deleted succeeds; a customer that never existed is
// IsNotFound.
func (c *Client) Delete(ctx context.Context, customerID string) error {
	tracer := metrics.TraceMethodCall(ctx, metricsStructName, "Delete")
	defer tracer.End()

	err := c.delete(ctx, customerID)
	tracer.OnError(err)
	return err
}

func (c *Client) delete(ctx context.Context, customerID string) error {
	if err := validateCustomerID(customerID); err != nil {
		return err
	}
	return c.do(ctx, http.MethodDelete, customersPath+"/"+customerID, "", nil, http.StatusNoContent, nil)
}

// do sends one signed request and decodes a wantStatus response into out
// (when out is not nil). Any other status is an *Error.
func (c *Client) do(ctx context.Context, method, path, idempotencyKey string, body any, wantStatus int, out any) error {
	u := *c.baseURL
	u.Path = c.baseURL.Path + path

	var reader io.Reader
	if body != nil {
		encoded, err := json.Marshal(body)
		if err != nil {
			// Never wrap the marshal error: it can quote a value.
			return fmt.Errorf("%w: request could not be encoded", ErrInvalidArgument)
		}
		reader = bytes.NewReader(encoded)
	}

	req, err := http.NewRequestWithContext(ctx, method, u.String(), reader)
	if err != nil {
		return err
	}

	// The uri claim is "METHOD host/path", with no query string.
	token, err := cdp.NewJWT(method, u.Host, u.Path, c.keyID, c.privateKey)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Accept", "application/json")
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if idempotencyKey != "" {
		req.Header.Set(idempotencyKeyHeader, idempotencyKey)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	data, err := io.ReadAll(io.LimitReader(resp.Body, maxResponseBytes))
	if err != nil {
		return err
	}

	if resp.StatusCode != wantStatus {
		return decodeError(resp.StatusCode, data)
	}
	if out == nil {
		return nil
	}
	if err := json.Unmarshal(data, out); err != nil {
		// The decoder's message can quote the body; leave it out.
		return fmt.Errorf("%w: http %d body is not a customer", ErrMalformedResponse, resp.StatusCode)
	}
	return nil
}

// decodeError reads Coinbase's error body if there is one. A body that is not
// one (a proxy's page) still yields an *Error carrying the status.
func decodeError(statusCode int, data []byte) *Error {
	var body struct {
		ErrorType     ErrorType `json:"errorType"`
		ErrorMessage  string    `json:"errorMessage"`
		CorrelationID string    `json:"correlationId"`
	}
	_ = json.Unmarshal(data, &body)
	return &Error{
		StatusCode:    statusCode,
		Type:          body.ErrorType,
		Message:       body.ErrorMessage,
		CorrelationID: body.CorrelationID,
	}
}

func validateCustomerID(customerID string) error {
	if !IsCustomerID(customerID) {
		return fmt.Errorf("%w: malformed customer id", ErrInvalidArgument)
	}
	return nil
}

func validateIdempotencyKey(key string) error {
	parsed, err := uuid.Parse(key)
	if err != nil || parsed.Version() != 4 || parsed.String() != key {
		return fmt.Errorf("%w: idempotency key must be a lowercase uuid v4", ErrInvalidArgument)
	}
	return nil
}

func validateInput(input *CustomerInput) error {
	for _, a := range input.TaxAttestations {
		if a.UsW9 == nil {
			return fmt.Errorf("%w: tax attestation sets no form", ErrInvalidArgument)
		}
	}
	return nil
}
