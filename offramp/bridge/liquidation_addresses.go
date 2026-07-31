package bridge

import (
	"context"
	"net/http"

	"github.com/code-payments/ocp-server/metrics"
)

// CreateLiquidationAddress creates a permanent on-chain address for a customer
// that automatically converts deposits and pays out to the configured external
// account. Bridge rejects duplicates for the same (chain, currency,
// destination) combination with a conflict error.
func (c *Client) CreateLiquidationAddress(ctx context.Context, idempotencyKey, customerID string, req *CreateLiquidationAddressRequest) (*LiquidationAddress, error) {
	tracer := metrics.TraceMethodCall(ctx, metricsStructName, "CreateLiquidationAddress")
	defer tracer.End()

	var res LiquidationAddress
	err := c.do(ctx, http.MethodPost, "/customers/"+customerID+"/liquidation_addresses", idempotencyKey, req, &res)
	tracer.OnError(SanitizeError(err))
	if err != nil {
		return nil, err
	}
	return &res, nil
}

// GetLiquidationAddresses lists a page of a customer's liquidation addresses,
// newest first. Bridge's default page size is 10; pass WithLimit and cursor
// options to page through larger sets.
func (c *Client) GetLiquidationAddresses(ctx context.Context, customerID string, opts ...ListOption) ([]LiquidationAddress, error) {
	tracer := metrics.TraceMethodCall(ctx, metricsStructName, "GetLiquidationAddresses")
	defer tracer.End()

	var res list[LiquidationAddress]
	err := c.do(ctx, http.MethodGet, listQuery("/customers/"+customerID+"/liquidation_addresses", opts), "", nil, &res)
	tracer.OnError(SanitizeError(err))
	if err != nil {
		return nil, err
	}
	return res.Data, nil
}

// GetDrains lists a page of the drain history for a liquidation address,
// newest first. Each drain is one deposit tracked through conversion and
// payout. Bridge's default page size is 10; pass WithLimit and cursor options
// to page, WithUpdatedAfter to sweep recent changes (the reconciliation
// worker's webhook backstop), or WithTxHash to attribute a specific deposit.
func (c *Client) GetDrains(ctx context.Context, customerID, liquidationAddressID string, opts ...ListOption) ([]Drain, error) {
	tracer := metrics.TraceMethodCall(ctx, metricsStructName, "GetDrains")
	defer tracer.End()

	var res list[Drain]
	err := c.do(ctx, http.MethodGet, listQuery("/customers/"+customerID+"/liquidation_addresses/"+liquidationAddressID+"/drains", opts), "", nil, &res)
	tracer.OnError(SanitizeError(err))
	if err != nil {
		return nil, err
	}
	return res.Data, nil
}
