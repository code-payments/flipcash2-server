package bridge

import (
	"context"
	"net/http"

	"github.com/code-payments/ocp-server/metrics"
)

// CreateExternalAccount registers a fiat bank account for a customer from
// user-entered details.
func (c *Client) CreateExternalAccount(ctx context.Context, idempotencyKey, customerID string, req *CreateExternalAccountRequest) (*ExternalAccount, error) {
	tracer := metrics.TraceMethodCall(ctx, metricsStructName, "CreateExternalAccount")
	defer tracer.End()

	var res ExternalAccount
	err := c.do(ctx, http.MethodPost, "/customers/"+customerID+"/external_accounts", idempotencyKey, req, &res)
	tracer.OnError(SanitizeError(err))
	if err != nil {
		return nil, err
	}
	return &res, nil
}

// GetExternalAccounts lists a page of a customer's fiat bank accounts, newest
// first. Bridge's default page size is 10; pass WithLimit and cursor options
// to page through larger sets.
func (c *Client) GetExternalAccounts(ctx context.Context, customerID string, opts ...ListOption) ([]ExternalAccount, error) {
	tracer := metrics.TraceMethodCall(ctx, metricsStructName, "GetExternalAccounts")
	defer tracer.End()

	var res list[ExternalAccount]
	err := c.do(ctx, http.MethodGet, listQuery("/customers/"+customerID+"/external_accounts", opts), "", nil, &res)
	tracer.OnError(SanitizeError(err))
	if err != nil {
		return nil, err
	}
	return res.Data, nil
}

// CreatePlaidLinkRequest starts a Bridge-hosted Plaid Link session for a
// customer. The returned link token initializes the Plaid SDK on the client.
func (c *Client) CreatePlaidLinkRequest(ctx context.Context, idempotencyKey, customerID string) (*PlaidLinkRequest, error) {
	tracer := metrics.TraceMethodCall(ctx, metricsStructName, "CreatePlaidLinkRequest")
	defer tracer.End()

	var res PlaidLinkRequest
	err := c.do(ctx, http.MethodPost, "/customers/"+customerID+"/plaid_link_requests", idempotencyKey, struct{}{}, &res)
	tracer.OnError(SanitizeError(err))
	if err != nil {
		return nil, err
	}
	return &res, nil
}

// ExchangePlaidPublicToken completes a Plaid Link session by exchanging the
// public token from the Plaid SDK. linkToken is the session's link_token from
// CreatePlaidLinkRequest (the session's callback_url resolves to this same
// endpoint). Bridge then creates external accounts for the linked bank
// accounts asynchronously; poll GetExternalAccounts for the results.
func (c *Client) ExchangePlaidPublicToken(ctx context.Context, idempotencyKey, linkToken, publicToken string) error {
	tracer := metrics.TraceMethodCall(ctx, metricsStructName, "ExchangePlaidPublicToken")
	defer tracer.End()

	req := struct {
		PublicToken string `json:"public_token"`
	}{
		PublicToken: publicToken,
	}
	err := c.do(ctx, http.MethodPost, "/plaid_exchange_public_token/"+linkToken, idempotencyKey, req, nil)
	tracer.OnError(SanitizeError(err))
	return err
}
