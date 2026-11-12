package thirdparty

import (
	"net/http"
	"regexp"
	"strings"
)

// The Coinbase requests GetJwt signs.
//
// A token from GetJwt is signed with the server's Coinbase key and handed to
// the app, so whatever request it names, the app's user can make as Flipcash.
// GetJwt therefore signs only the requests the apps make, which are the two of
// a Coinbase Onramp purchase, placing an order and reading one back:
//
//	POST api.cdp.coinbase.com/platform/v2/onramp/orders
//	GET  api.cdp.coinbase.com/platform/v2/onramp/orders/<order ID>
//
// Anything else is refused, so the key reaches nothing more through this RPC
// whatever else Coinbase permits it.
//
// A request is judged as the token's `uri` claim names it: cdp.NewJWT writes
// "METHOD host/path" with the request's host and path joined exactly as given,
// and that join is what is compared. The apps split it differently (Android
// sends the /platform/ prefix on the host, iOS on the path), and any split of
// an allowed request yields the same claim, so no split is preferred.
//
// Nothing is normalized. A method in another case, a doubled or trailing
// slash, a query string, a scheme or another host names a different request
// and is refused.
//
// The list is what the apps at or above the server's minimum builds send
// (checked against both apps' sources, 2026-11-11). A new Coinbase call an app
// is to make must be added here before that app ships.

// coinbaseOnrampOrders is Coinbase's Onramp orders collection, as a token's
// `uri` claim names it: host and path, no scheme and no query.
const coinbaseOnrampOrders = "api.cdp.coinbase.com/platform/v2/onramp/orders"

// coinbaseOnrampOrderID is Coinbase's own pattern for an Onramp order ID, the
// orderId path parameter of its API specification (a UUID as Coinbase writes
// one). It admits no slash, so an ID never reaches past the one order.
var coinbaseOnrampOrderID = regexp.MustCompile(`^[a-f0-9-]{36}$`)

// isAllowedCoinbaseRequest reports whether GetJwt signs a token for the
// Coinbase request the given method, host and path name.
func isAllowedCoinbaseRequest(method, host, path string) bool {
	resource := host + path
	switch method {
	case http.MethodPost:
		return resource == coinbaseOnrampOrders
	case http.MethodGet:
		orderID, ok := strings.CutPrefix(resource, coinbaseOnrampOrders+"/")
		return ok && coinbaseOnrampOrderID.MatchString(orderID)
	default:
		return false
	}
}
