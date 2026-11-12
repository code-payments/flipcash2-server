package coinbase

import (
	"crypto/sha256"
	"errors"
	"net/netip"

	"github.com/google/uuid"

	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"

	"github.com/code-payments/flipcash2-server/cdp/customers"
)

// customerIdempotencyKeyDomain separates the hash customerIdempotencyKey takes
// from any other hash of a user ID.
const customerIdempotencyKeyDomain = "flipcash-coinbase-kyc-customer-v1"

// customerIdempotencyKey is the idempotency key under which a user's Coinbase
// customer is created: the first 16 bytes of SHA-256(domain ‖ user ID),
// stamped as a version 4, RFC 4122 variant UUID (Coinbase's documented key
// format), as its lowercase canonical string.
//
// It is derived, not random, so that every retry and every racing StartKyc
// for a user creates under the same key, and Coinbase answers them all with
// one customer. The create it guards carries no data about the user, so a key
// anyone could compute exposes nothing. Coinbase remembers a key for 24 hours
// (its idempotency documentation): a start retried after that, whose first
// attempt created a customer but failed to record it, creates a second
// customer that nothing refers to.
func customerIdempotencyKey(userID *commonpb.UserId) string {
	sum := sha256.Sum256(append([]byte(customerIdempotencyKeyDomain), userID.GetValue()...))
	var id uuid.UUID
	copy(id[:], sum[:16])
	id[6] = (id[6] & 0x0f) | 0x40 // version 4
	id[8] = (id[8] & 0x3f) | 0x80 // RFC 4122 variant
	return id.String()
}

// startInput is the update that starts a product: its capabilities requested,
// with the requester's IP, which Coinbase requires on every write that
// requests one. An empty set, which would start nothing, and an invalid IP are
// errors.
func startInput(capabilities []customers.CapabilityName, requesterIP netip.Addr) (*customers.CustomerInput, error) {
	if len(capabilities) == 0 {
		return nil, errors.New("no capabilities to request")
	}
	if !requesterIP.IsValid() {
		return nil, errors.New("invalid requester ip address")
	}
	requested := make(map[customers.CapabilityName]customers.CapabilityRequest, len(capabilities))
	for _, name := range capabilities {
		requested[name] = customers.CapabilityRequest{Requested: true}
	}
	return &customers.CustomerInput{
		Capabilities: requested,
		Compliance:   &customers.Compliance{RequesterIPAddress: requesterIP.Unmap().String()},
	}, nil
}

// nonUserPrefixes are ranges outside the private, loopback, link-local,
// multicast and unspecified ones that no end user's connection reaches the
// internet from, so an address in one was not read from the user's own hop.
var nonUserPrefixes = []netip.Prefix{
	netip.MustParsePrefix("0.0.0.0/8"),     // "this network"
	netip.MustParsePrefix("100.64.0.0/10"), // carrier-grade NAT, inside a carrier's or a cloud's network
	netip.MustParsePrefix("192.0.0.0/24"),  // IETF protocol assignments
	netip.MustParsePrefix("198.18.0.0/15"), // benchmarking
	netip.MustParsePrefix("240.0.0.0/4"),   // reserved, and the broadcast address
	netip.MustParsePrefix("64:ff9b::/96"),  // NAT64, a translator's address for an IPv4 host
}

// validRequesterIP reports whether addr may be sent to Coinbase as the end
// user's IP address: valid, and, after Unmap, a global unicast address with
// no IPv6 zone that is not private, loopback, link-local or unspecified, and
// is in none of nonUserPrefixes: "this network" (0.0.0.0/8), carrier-grade
// NAT (100.64.0.0/10), IETF protocol assignments (192.0.0.0/24),
// benchmarking (198.18.0.0/15), reserved and broadcast (240.0.0.0/4) and
// NAT64 (64:ff9b::/96). An IPv4-mapped IPv6 address is judged as the IPv4
// address it maps.
//
// Coinbase requires the end user's own address as a compliance signal. An
// address no end user connects from means the request's address was taken
// from the wrong hop (a load balancer or proxy in front of the server, or a
// network inside a carrier's or a cloud's), which should fail loudly rather
// than feed Coinbase a wrong signal.
//
// The documentation ranges (192.0.2.0/24, 198.51.100.0/24, 203.0.113.0/24,
// 2001:db8::/32) are accepted. Tests and Sandbox runs send addresses from
// them, and no proxy writes one for a real connection, so accepting them lets
// no wrong signal from a real request through.
func validRequesterIP(addr netip.Addr) bool {
	addr = addr.Unmap()
	if !addr.IsValid() ||
		addr.Zone() != "" ||
		!addr.IsGlobalUnicast() ||
		addr.IsPrivate() ||
		addr.IsLoopback() ||
		addr.IsLinkLocalUnicast() ||
		addr.IsUnspecified() {
		return false
	}
	for _, prefix := range nonUserPrefixes {
		if prefix.Contains(addr) {
			return false
		}
	}
	return true
}
