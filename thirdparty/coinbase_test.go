package thirdparty

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

const testOrderID = "123e4567-e89b-12d3-a456-426614174000"

func TestIsAllowedCoinbaseRequest_Allowed(t *testing.T) {
	for _, tc := range []struct {
		name               string
		method, host, path string
	}{
		// The apps' own requests, split as each app sends them.
		{"android places an order", "POST", "api.cdp.coinbase.com/platform/", "v2/onramp/orders"},
		{"android reads an order", "GET", "api.cdp.coinbase.com/platform/", "v2/onramp/orders/" + testOrderID},
		{"ios places an order", "POST", "api.cdp.coinbase.com/", "platform/v2/onramp/orders"},
		{"ios reads an order", "GET", "api.cdp.coinbase.com/", "platform/v2/onramp/orders/" + testOrderID},

		// The claim is the join, so another split of the same request is the
		// same token.
		{"another split, order placed", "POST", "api.cdp.coinbase.com/platform/v2/onramp/", "orders"},
		{"another split, order read", "GET", "api.cdp.coinbase.com/platform/v2/onramp/orders/", testOrderID},

		{"an order id of hyphens and hex", "GET", "api.cdp.coinbase.com/platform/", "v2/onramp/orders/00000000-0000-0000-0000-000000000000"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			assert.True(t, isAllowedCoinbaseRequest(tc.method, tc.host, tc.path))
		})
	}
}

func TestIsAllowedCoinbaseRequest_Refused(t *testing.T) {
	const (
		androidHost = "api.cdp.coinbase.com/platform/"
		iosHost     = "api.cdp.coinbase.com/"
	)

	for _, tc := range []struct {
		name               string
		method, host, path string
	}{
		// What the restriction is for: the key's other reach at Coinbase.
		{"list customers", "GET", androidHost, "v2/customers"},
		{"list customers, ios split", "GET", iosHost, "platform/v2/customers"},
		{"read a customer", "GET", androidHost, "v2/customers/customer_x"},
		{"create a customer", "POST", androidHost, "v2/customers"},
		{"update a customer", "PUT", androidHost, "v2/customers/customer_x"},
		{"delete a customer", "DELETE", androidHost, "v2/customers/customer_x"},

		// The rest of the Onramp API.
		{"onramp session", "POST", androidHost, "v2/onramp/sessions"},
		{"onramp limits", "POST", androidHost, "v2/onramp/limits"},
		{"onramp verification", "POST", androidHost, "v2/onramp/verifications"},
		{"sibling of orders", "POST", androidHost, "v2/onramp/orders2"},

		// The right resource under the wrong method.
		{"read the collection", "GET", androidHost, "v2/onramp/orders"},
		{"post to an order", "POST", androidHost, "v2/onramp/orders/" + testOrderID},
		{"put the collection", "PUT", androidHost, "v2/onramp/orders"},
		{"put an order", "PUT", androidHost, "v2/onramp/orders/" + testOrderID},
		{"delete an order", "DELETE", androidHost, "v2/onramp/orders/" + testOrderID},
		{"head an order", "HEAD", androidHost, "v2/onramp/orders/" + testOrderID},
		{"lowercase post", "post", androidHost, "v2/onramp/orders"},
		{"lowercase get", "get", androidHost, "v2/onramp/orders/" + testOrderID},
		{"padded get", "GET ", androidHost, "v2/onramp/orders/" + testOrderID},

		// Nothing is normalized.
		{"doubled slash", "POST", androidHost, "/v2/onramp/orders"},
		{"trailing slash", "POST", androidHost, "v2/onramp/orders/"},
		{"query on the collection", "POST", androidHost, "v2/onramp/orders?x=1"},
		{"query on an order", "GET", androidHost, "v2/onramp/orders/" + testOrderID + "?x=1"},
		{"fragment on an order", "GET", androidHost, "v2/onramp/orders/" + testOrderID + "#x"},
		{"scheme", "POST", "https://api.cdp.coinbase.com/platform/", "v2/onramp/orders"},
		{"port", "POST", "api.cdp.coinbase.com:443/platform/", "v2/onramp/orders"},
		{"uppercase host", "POST", "API.CDP.COINBASE.COM/platform/", "v2/onramp/orders"},
		{"missing platform prefix", "POST", iosHost, "v2/onramp/orders"},
		{"leading space", "POST", " " + androidHost, "v2/onramp/orders"},
		{"trailing space", "POST", androidHost, "v2/onramp/orders "},
		{"trailing newline", "POST", androidHost, "v2/onramp/orders\n"},

		// Another host.
		{"sandbox host", "POST", "sandbox.cdp.coinbase.com/platform/", "v2/onramp/orders"},
		{"host as a path of another", "POST", "evil.example/api.cdp.coinbase.com/platform/", "v2/onramp/orders"},
		{"host as userinfo of another", "POST", "api.cdp.coinbase.com@evil.example/platform/", "v2/onramp/orders"},
		{"host as a label of another", "POST", "api.cdp.coinbase.com.evil.example/platform/", "v2/onramp/orders"},

		// An order ID that is not one.
		{"empty order id", "GET", androidHost, "v2/onramp/orders/"},
		{"short order id", "GET", androidHost, "v2/onramp/orders/" + testOrderID[:35]},
		{"long order id", "GET", androidHost, "v2/onramp/orders/" + testOrderID + "0"},
		{"uppercase order id", "GET", androidHost, "v2/onramp/orders/123E4567-E89B-12D3-A456-426614174000"},
		{"order id with a newline", "GET", androidHost, "v2/onramp/orders/" + testOrderID + "\n"},
		{"path under an order", "GET", androidHost, "v2/onramp/orders/" + testOrderID + "/x"},
		{"traversal after an order", "GET", androidHost, "v2/onramp/orders/" + testOrderID + "/../../customers"},
		{"traversal as the order", "GET", androidHost, "v2/onramp/orders/../customers"},
		{"36 characters of traversal", "GET", androidHost, "v2/onramp/orders/../../../../../customers/customer_xx"},
		{"escaped traversal", "GET", androidHost, "v2/onramp/orders/%2e%2e%2fcustomers%2fcustomer_xxxxxxx"},

		// What app builds below the server's minimums once asked for.
		{"legacy token", "POST", "api.developer.coinbase.com/", "onramp/v1/token"},
		{"legacy buy options", "GET", "api.developer.coinbase.com/", "onramp/v1/buy/options"},
		{"legacy order", "POST", "api.developer.coinbase.com/", "onramp/v2/onramp/order"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			assert.False(t, isAllowedCoinbaseRequest(tc.method, tc.host, tc.path))
		})
	}
}
