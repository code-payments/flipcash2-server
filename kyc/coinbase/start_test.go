package coinbase

import (
	"context"
	"net/netip"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"

	"github.com/code-payments/flipcash2-server/cdp/customers"
	"github.com/code-payments/flipcash2-server/cdp/customers/customerstest"
)

func testUserID(b byte) *commonpb.UserId {
	value := make([]byte, 16)
	for i := range value {
		value[i] = b
	}
	return &commonpb.UserId{Value: value}
}

func TestCustomerIdempotencyKey(t *testing.T) {
	a, b := testUserID(1), testUserID(2)

	key := customerIdempotencyKey(a)
	require.Equal(t, key, customerIdempotencyKey(&commonpb.UserId{Value: append([]byte(nil), a.Value...)}))
	require.NotEqual(t, key, customerIdempotencyKey(b))

	// Pinned: a key that changed would let a retried start create a second
	// customer for a user.
	require.Equal(t, "7ec32c5a-8794-46cc-bb4b-9ace0ff75c56", key)

	for _, userID := range []*commonpb.UserId{a, b, testUserID(0xff), {Value: []byte{0}}} {
		parsed, err := uuid.Parse(customerIdempotencyKey(userID))
		require.NoError(t, err)
		require.Equal(t, uuid.Version(4), parsed.Version())
		require.Equal(t, uuid.RFC4122, parsed.Variant())
		require.Equal(t, parsed.String(), customerIdempotencyKey(userID))
	}
}

func TestCustomerIdempotencyKey_Fake(t *testing.T) {
	fake := customerstest.NewServer()
	defer fake.Close()
	client := fake.Client()
	ctx := context.Background()

	first, err := client.Create(ctx, customerIdempotencyKey(testUserID(1)), nil)
	require.NoError(t, err)
	again, err := client.Create(ctx, customerIdempotencyKey(testUserID(1)), nil)
	require.NoError(t, err)
	require.Equal(t, first.ID, again.ID)

	other, err := client.Create(ctx, customerIdempotencyKey(testUserID(2)), nil)
	require.NoError(t, err)
	require.NotEqual(t, first.ID, other.ID)
	require.Len(t, fake.CustomerIDs(), 2)
}

func TestStartInput(t *testing.T) {
	set := offrampSet()

	input, err := startInput(set, netip.MustParseAddr("::ffff:203.0.113.42"))
	require.NoError(t, err)
	require.Equal(t, &customers.CustomerInput{
		Capabilities: map[customers.CapabilityName]customers.CapabilityRequest{
			customers.CapabilityCustodyCrypto:      {Requested: true},
			customers.CapabilityCustodyFiat:        {Requested: true},
			customers.CapabilityCustodyStablecoin:  {Requested: true},
			customers.CapabilityTransferStablecoin: {Requested: true},
			customers.CapabilityTradeStablecoin:    {Requested: true},
			customers.CapabilityTransferFiat:       {Requested: true},
		},
		Compliance: &customers.Compliance{RequesterIPAddress: "203.0.113.42"},
	}, input)

	input, err = startInput(set, netip.MustParseAddr("2001:db8::42"))
	require.NoError(t, err)
	require.Equal(t, "2001:db8::42", input.Compliance.RequesterIPAddress)

	_, err = startInput(set, netip.Addr{})
	require.Error(t, err)
	_, err = startInput(nil, netip.MustParseAddr("203.0.113.42"))
	require.Error(t, err)
}

func TestValidRequesterIP(t *testing.T) {
	for _, tc := range []struct {
		addr string
		ok   bool
	}{
		// Ordinary public addresses.
		{"8.8.8.8", true},
		{"::ffff:8.8.8.8", true},
		{"2606:4700::1111", true},
		// The documentation ranges, which tests and Sandbox runs send.
		{"192.0.2.1", true},
		{"198.51.100.1", true},
		{"203.0.113.42", true},
		{"2001:db8::1", true},
		// Just outside the ranges refused below.
		{"1.0.0.1", true},
		{"100.63.255.255", true},
		{"100.128.0.1", true},
		{"192.0.1.255", true},
		{"198.17.255.255", true},
		{"198.20.0.1", true},
		{"223.255.255.254", true},

		// "This network".
		{"0.0.0.1", false},
		{"0.255.255.255", false},
		// Carrier-grade NAT.
		{"100.64.0.1", false},
		{"100.127.255.254", false},
		{"::ffff:100.64.0.1", false},
		// IETF protocol assignments.
		{"192.0.0.8", false},
		// Benchmarking.
		{"198.18.0.1", false},
		{"198.19.255.254", false},
		// Reserved, and broadcast.
		{"240.0.0.1", false},
		{"255.255.255.254", false},
		// NAT64.
		{"64:ff9b::808:808", false},
		{"64:ff9b::ffff:ffff", false},

		{"10.0.0.1", false},
		{"172.16.0.1", false},
		{"192.168.1.1", false},
		{"::ffff:10.0.0.1", false},
		{"fd00::1", false},
		{"127.0.0.1", false},
		{"::1", false},
		{"::ffff:127.0.0.1", false},
		{"169.254.1.1", false},
		{"fe80::1", false},
		{"fe80::1%eth0", false},
		{"2001:db8::1%eth0", false},
		{"0.0.0.0", false},
		{"::", false},
		{"224.0.0.1", false},
		{"ff02::1", false},
		{"255.255.255.255", false},
	} {
		t.Run(tc.addr, func(t *testing.T) {
			require.Equal(t, tc.ok, validRequesterIP(netip.MustParseAddr(tc.addr)))
		})
	}
	require.False(t, validRequesterIP(netip.Addr{}))
}
