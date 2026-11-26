package plaid

import (
	"context"
	"fmt"
)

// Sandbox calls Plaid's Sandbox-only endpoints, for tests. NewSandbox
// refuses a Client for Production, which would answer them SANDBOX_ONLY
// anyway.
type Sandbox struct {
	c *Client
}

// NewSandbox returns the Sandbox endpoints for c.
func NewSandbox(c *Client) (*Sandbox, error) {
	if c.baseURL.String() == ProductionBaseURL {
		return nil, fmt.Errorf("%w: sandbox endpoints on a production client", ErrInvalidArgument)
	}
	return &Sandbox{c: c}, nil
}

const (
	pathSandboxPublicTokenCreate = "/sandbox/public_token/create"
	pathSandboxItemResetLogin    = "/sandbox/item/reset_login"
	pathSandboxItemFireWebhook   = "/sandbox/item/fire_webhook"
)

type sandboxPublicTokenCreateRequest struct {
	credentials
	InstitutionID   string   `json:"institution_id"`
	InitialProducts []string `json:"initial_products"`
}

type sandboxPublicTokenCreateResponse struct {
	PublicToken string `json:"public_token"`
	RequestID   string `json:"request_id"`
}

type sandboxFireWebhookRequest struct {
	credentials
	AccessToken string `json:"access_token"`
	WebhookCode string `json:"webhook_code"`
}

// CreatePublicToken creates a Sandbox Item at institutionID for Plaid's
// default test user (user_good), with products ["auth"], skipping Link, and
// returns its public token.
func (s *Sandbox) CreatePublicToken(ctx context.Context, institutionID string) (string, error) {
	if err := requireNonEmpty("institution id", institutionID); err != nil {
		return "", err
	}
	var res sandboxPublicTokenCreateResponse
	req := sandboxPublicTokenCreateRequest{
		credentials:     s.c.credentials(),
		InstitutionID:   institutionID,
		InitialProducts: linkProducts,
	}
	if err := s.c.do(ctx, pathSandboxPublicTokenCreate, req, &res); err != nil {
		return "", err
	}
	if res.PublicToken == "" {
		return "", fmt.Errorf("%w: %s returned no public token (request id %s)", ErrMalformedResponse, pathSandboxPublicTokenCreate, res.RequestID)
	}
	return res.PublicToken, nil
}

// ResetLogin puts a Sandbox Item into ITEM_LOGIN_REQUIRED.
func (s *Sandbox) ResetLogin(ctx context.Context, accessToken string) error {
	if err := requireNonEmpty("access token", accessToken); err != nil {
		return err
	}
	req := accessTokenRequest{credentials: s.c.credentials(), AccessToken: accessToken}
	return s.c.do(ctx, pathSandboxItemResetLogin, req, nil)
}

// FireWebhook has Plaid send a Sandbox Item's webhook webhookCode (e.g.
// NEW_ACCOUNTS_AVAILABLE, PENDING_DISCONNECT, USER_PERMISSION_REVOKED) to
// the Item's webhook URL.
func (s *Sandbox) FireWebhook(ctx context.Context, accessToken, webhookCode string) error {
	if err := requireNonEmpty("access token", accessToken); err != nil {
		return err
	}
	if err := requireNonEmpty("webhook code", webhookCode); err != nil {
		return err
	}
	req := sandboxFireWebhookRequest{credentials: s.c.credentials(), AccessToken: accessToken, WebhookCode: webhookCode}
	return s.c.do(ctx, pathSandboxItemFireWebhook, req, nil)
}
