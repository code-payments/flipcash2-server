package bridge

import (
	"context"
	"net/http"

	"github.com/code-payments/ocp-server/metrics"
)

// CreateTOSLink creates a hosted terms-of-service acceptance session for a new
// customer. The client opens the URL in a webview and captures the resulting
// signed agreement ID, which is required to create the customer.
func (c *Client) CreateTOSLink(ctx context.Context, idempotencyKey string) (*TOSLink, error) {
	tracer := metrics.TraceMethodCall(ctx, metricsStructName, "CreateTOSLink")
	defer tracer.End()

	var res TOSLink
	err := c.do(ctx, http.MethodPost, "/customers/tos_links", idempotencyKey, struct{}{}, &res)
	tracer.OnError(SanitizeError(err))
	if err != nil {
		return nil, err
	}
	return &res, nil
}

// CreateCustomer creates an individual customer, submitting identity data for
// KYC verification.
func (c *Client) CreateCustomer(ctx context.Context, idempotencyKey string, req *CreateCustomerRequest) (*Customer, error) {
	tracer := metrics.TraceMethodCall(ctx, metricsStructName, "CreateCustomer")
	defer tracer.End()

	var res Customer
	err := c.do(ctx, http.MethodPost, "/customers", idempotencyKey, req, &res)
	tracer.OnError(SanitizeError(err))
	if err != nil {
		return nil, err
	}
	return &res, nil
}

// GetCustomer fetches a customer's current state, including KYC status and
// endorsement requirements.
func (c *Client) GetCustomer(ctx context.Context, customerID string) (*Customer, error) {
	tracer := metrics.TraceMethodCall(ctx, metricsStructName, "GetCustomer")
	defer tracer.End()

	var res Customer
	err := c.do(ctx, http.MethodGet, "/customers/"+customerID, "", nil, &res)
	tracer.OnError(SanitizeError(err))
	if err != nil {
		return nil, err
	}
	return &res, nil
}

// UpdateCustomer updates a subset of a customer's data, e.g. resubmitting ID
// document images after a verification issue. No idempotency key: Bridge
// honors them on POST only.
func (c *Client) UpdateCustomer(ctx context.Context, customerID string, req *UpdateCustomerRequest) (*Customer, error) {
	tracer := metrics.TraceMethodCall(ctx, metricsStructName, "UpdateCustomer")
	defer tracer.End()

	var res Customer
	err := c.do(ctx, http.MethodPut, "/customers/"+customerID, "", req, &res)
	tracer.OnError(SanitizeError(err))
	if err != nil {
		return nil, err
	}
	return &res, nil
}
