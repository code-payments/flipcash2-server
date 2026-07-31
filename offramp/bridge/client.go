package bridge

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

const (
	// ProductionBaseURL is the base URL for the production Bridge API.
	ProductionBaseURL = "https://api.bridge.xyz"

	// SandboxBaseURL is the base URL for the sandbox Bridge API. The sandbox
	// has no real money movement, does not send payment webhooks, and accepts
	// arbitrary signed agreement IDs.
	SandboxBaseURL = "https://api.sandbox.bridge.xyz"

	apiVersion = "/v0"

	metricsStructName = "offramp.bridge.client"

	// defaultTimeout is sized for customer creation calls carrying base64 ID
	// document images, which are much larger than typical JSON round trips.
	defaultTimeout = 2 * time.Minute
)

// Client is an HTTP client to the Bridge API.
//
// Requests to Bridge may carry sensitive PII (SSNs, ID document images, bank
// account numbers). Request and response bodies must never be logged.
type Client struct {
	baseURL    string
	apiKey     string
	httpClient *http.Client
}

type Option func(*Client)

// WithBaseURL overrides the API base URL (e.g. SandboxBaseURL or a test server).
func WithBaseURL(baseURL string) Option {
	return func(c *Client) {
		c.baseURL = baseURL
	}
}

// WithHTTPClient overrides the underlying HTTP client.
func WithHTTPClient(httpClient *http.Client) Option {
	return func(c *Client) {
		c.httpClient = httpClient
	}
}

// NewClient returns a new Bridge client
func NewClient(apiKey string, opts ...Option) *Client {
	c := &Client{
		baseURL: ProductionBaseURL,
		apiKey:  apiKey,
		httpClient: &http.Client{
			Timeout: defaultTimeout,
		},
	}
	for _, opt := range opts {
		opt(c)
	}
	return c
}

// do executes a single API call. POSTs must provide a deterministic
// idempotencyKey derived from the operation being performed, so retries cannot
// create duplicate resources. Pass an empty idempotencyKey for other methods:
// Bridge honors idempotency keys on POST only.
func (c *Client) do(ctx context.Context, method, path, idempotencyKey string, reqBody, respBody any) error {
	var body io.Reader
	if reqBody != nil {
		marshalled, err := json.Marshal(reqBody)
		if err != nil {
			return err
		}
		body = bytes.NewReader(marshalled)
	}

	req, err := http.NewRequestWithContext(ctx, method, c.baseURL+apiVersion+path, body)
	if err != nil {
		return err
	}

	req.Header.Set("Api-Key", c.apiKey)
	req.Header.Set("Accept", "application/json")
	if reqBody != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if idempotencyKey != "" {
		req.Header.Set("Idempotency-Key", idempotencyKey)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	respBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}

	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		return newError(resp.StatusCode, respBytes)
	}

	if respBody != nil {
		if err := json.Unmarshal(respBytes, respBody); err != nil {
			return fmt.Errorf("decoding bridge response: %w", err)
		}
	}
	return nil
}
