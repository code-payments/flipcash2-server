package coinbase

import (
	"slices"

	kyccoinbasepb "github.com/code-payments/flipcash2-protobuf-api/generated/go/kyc/coinbase/v1"

	"github.com/code-payments/flipcash2-server/cdp/customers"
)

// productSets is the set of Coinbase capabilities each product needs.
//
// A product is Flipcash's own name for a set of capabilities: Coinbase has no
// notion of a product, it grants or refuses each capability on its own, and a
// user's state for a product is its set seen as one (see deriveState).
//
// PRODUCT_OFFRAMP's set is our reading of what cashing out to a bank needs:
// holding the user's crypto, fiat and stablecoin, converting stablecoin to
// fiat, and moving stablecoin in and fiat out. It is to be confirmed with
// Coinbase; changing it changes which users read as started and verified.
var productSets = map[kyccoinbasepb.Product][]customers.CapabilityName{
	kyccoinbasepb.Product_PRODUCT_OFFRAMP: {
		customers.CapabilityCustodyCrypto,
		customers.CapabilityCustodyFiat,
		customers.CapabilityCustodyStablecoin,
		customers.CapabilityTransferStablecoin,
		customers.CapabilityTradeStablecoin,
		customers.CapabilityTransferFiat,
	},
}

// productCapabilities returns the Coinbase capabilities a product needs, in a
// fixed order. False for a product with no set. The slice is a copy, so a
// caller cannot change the table.
func productCapabilities(product kyccoinbasepb.Product) ([]customers.CapabilityName, bool) {
	set, ok := productSets[product]
	if !ok {
		return nil, false
	}
	return slices.Clone(set), true
}
