package messaging

import (
	"math"
	"testing"

	"github.com/stretchr/testify/require"

	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"
)

func TestBoostPayees(t *testing.T) {
	payer, creator, sender := userID(1), userID(2), userID(3)

	for _, tc := range []struct {
		name                   string
		payer, creator, sender *commonpb.UserId
		want                   []BoostShare
		err                    error
	}{
		{name: "third party pays both", payer: payer, creator: creator, sender: sender, want: []BoostShare{{sender, 8000}, {creator, 2000}}},
		{name: "sender is the creator, paid the whole", payer: payer, creator: creator, sender: creator, want: []BoostShare{{creator, 10000}}},
		{name: "payer is the creator, sender paid the whole", payer: payer, creator: payer, sender: sender, want: []BoostShare{{sender, 10000}}},
		{name: "message has no sender", payer: payer, creator: creator, sender: nil, want: []BoostShare{{creator, 10000}}},
		{name: "payer is the sender", payer: payer, creator: creator, sender: payer, err: ErrBoostOwnMessage},
		{name: "creator boosting their own message", payer: payer, creator: payer, sender: payer, err: ErrBoostOwnMessage},
		{name: "creator boosting a message with no sender", payer: payer, creator: payer, sender: nil, err: ErrBoostNoPayee},
		{name: "no creator", payer: payer, creator: nil, sender: sender, err: ErrBoostNoCreator},
		{name: "no payer", payer: nil, creator: creator, sender: sender, err: ErrBoostNoPayer},
		{name: "no payer is judged before no creator", payer: nil, creator: nil, sender: sender, err: ErrBoostNoPayer},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := BoostPayees(tc.payer, tc.creator, tc.sender)
			if tc.err != nil {
				require.ErrorIs(t, err, tc.err)
				require.Nil(t, got)
				return
			}
			require.NoError(t, err)
			require.Len(t, got, len(tc.want))
			for i := range tc.want {
				require.True(t, sameUser(tc.want[i].UserID, got[i].UserID), "payee %d", i)
				require.Equal(t, tc.want[i].Bps, got[i].Bps, "payee %d", i)
			}
		})
	}
}

func TestValidateBoostPayouts(t *testing.T) {
	payer, creator, sender, stranger := userID(1), userID(2), userID(3), userID(4)

	// Half a cent, the tolerance intent validation passes for USD.
	const tolerance = 0.005

	for _, tc := range []struct {
		name            string
		payer           bool // true to pass no payer
		creator, sender *commonpb.UserId
		payouts         []BoostPayout
		total           float64
		err             error
	}{
		{
			name: "80/20 to sender and creator", creator: creator, sender: sender,
			payouts: []BoostPayout{{sender, 0.80}, {creator, 0.20}}, total: 1.00,
		},
		{
			name: "order does not matter", creator: creator, sender: sender,
			payouts: []BoostPayout{{creator, 0.20}, {sender, 0.80}}, total: 1.00,
		},
		{
			name: "the smallest split", creator: creator, sender: sender,
			payouts: []BoostPayout{{sender, 0.04}, {creator, 0.01}}, total: 0.05,
		},
		{
			name: "creator rounded down to the cent", creator: creator, sender: sender,
			// A fifth of $0.07 is $0.014.
			payouts: []BoostPayout{{sender, 0.06}, {creator, 0.01}}, total: 0.07,
		},
		{
			name: "creator rounded up to the cent", creator: creator, sender: sender,
			// A fifth of $0.08 is $0.016.
			payouts: []BoostPayout{{sender, 0.06}, {creator, 0.02}}, total: 0.08,
		},
		{
			name: "half a cent either way", creator: creator, sender: sender,
			// A fifth of $0.025 is $0.005, half a cent from both $0.00 and
			// $0.01.
			payouts: []BoostPayout{{sender, 0.015}, {creator, 0.01}}, total: 0.025,
		},
		{
			name: "creator a cent over", creator: creator, sender: sender,
			payouts: []BoostPayout{{sender, 0.79}, {creator, 0.21}}, err: ErrBoostShareMismatch,
		},
		{
			name: "creator a cent under", creator: creator, sender: sender,
			payouts: []BoostPayout{{sender, 0.81}, {creator, 0.19}}, err: ErrBoostShareMismatch,
		},
		{
			name: "creator just past half a cent", creator: creator, sender: sender,
			// A fifth of $0.08 is $0.016, so $0.022 is $0.006 over.
			payouts: []BoostPayout{{sender, 0.058}, {creator, 0.022}}, err: ErrBoostShareMismatch,
		},
		{
			name: "20/80 the other way", creator: creator, sender: sender,
			payouts: []BoostPayout{{sender, 0.20}, {creator, 0.80}}, err: ErrBoostShareMismatch,
		},
		{
			name: "even split", creator: creator, sender: sender,
			payouts: []BoostPayout{{creator, 0.50}, {sender, 0.50}}, err: ErrBoostShareMismatch,
		},
		{
			name: "large amounts", creator: creator, sender: sender,
			payouts: []BoostPayout{{sender, 8_000_000.08}, {creator, 2_000_000.02}}, total: 10_000_000.10,
		},
		{
			name: "sender is the creator, paid once", creator: creator, sender: creator,
			payouts: []BoostPayout{{creator, 1.00}}, total: 1.00,
		},
		{
			name: "payer is the creator, sender takes all", creator: payer, sender: sender,
			payouts: []BoostPayout{{sender, 1.00}}, total: 1.00,
		},
		{
			name: "one payee is not held to a share", creator: creator, sender: creator,
			payouts: []BoostPayout{{creator, 0.01}}, total: 0.01,
		},
		{
			name: "payer is the sender", creator: creator, sender: payer,
			payouts: []BoostPayout{{creator, 1.00}}, err: ErrBoostOwnMessage,
		},
		{
			name: "creator boosting their own message", creator: payer, sender: payer,
			payouts: []BoostPayout{{stranger, 1.00}}, err: ErrBoostOwnMessage,
		},
		{
			name: "one payee paid twice is not a split", creator: creator, sender: creator,
			payouts: []BoostPayout{{creator, 0.80}, {creator, 0.20}}, err: ErrBoostPayoutMismatch,
		},
		{
			name: "sender missing", creator: creator, sender: sender,
			payouts: []BoostPayout{{creator, 1.00}}, err: ErrBoostPayoutMismatch,
		},
		{
			name: "creator missing", creator: creator, sender: sender,
			payouts: []BoostPayout{{sender, 1.00}}, err: ErrBoostPayoutMismatch,
		},
		{
			name: "stranger paid", creator: creator, sender: sender,
			payouts: []BoostPayout{{sender, 0.80}, {stranger, 0.20}}, err: ErrBoostPayoutMismatch,
		},
		{
			name: "payer paid", creator: creator, sender: sender,
			payouts: []BoostPayout{{sender, 0.80}, {payer, 0.20}}, err: ErrBoostPayoutMismatch,
		},
		{
			name: "creator named twice", creator: creator, sender: sender,
			payouts: []BoostPayout{{creator, 0.80}, {creator, 0.20}}, err: ErrBoostPayoutMismatch,
		},
		{
			name: "three payouts", creator: creator, sender: sender,
			payouts: []BoostPayout{{sender, 0.80}, {creator, 0.10}, {stranger, 0.10}}, err: ErrBoostPayoutMismatch,
		},
		{
			name: "no payouts", creator: creator, sender: sender,
			payouts: nil, err: ErrBoostPayoutMismatch,
		},
		{
			name: "empty payout", creator: creator, sender: sender,
			payouts: []BoostPayout{{sender, 1.00}, {creator, 0}}, err: ErrBoostPayoutEmpty,
		},
		{
			name: "negative payout", creator: creator, sender: sender,
			payouts: []BoostPayout{{sender, 1.20}, {creator, -0.20}}, err: ErrBoostPayoutEmpty,
		},
		{
			name: "not a number", creator: creator, sender: sender,
			payouts: []BoostPayout{{sender, math.NaN()}, {creator, 0.20}}, err: ErrBoostPayoutEmpty,
		},
		{
			name: "infinite", creator: creator, sender: sender,
			payouts: []BoostPayout{{sender, math.Inf(1)}, {creator, 0.20}}, err: ErrBoostPayoutEmpty,
		},
		{
			name: "no one to pay", creator: payer, sender: nil,
			payouts: []BoostPayout{{creator, 1.00}}, err: ErrBoostNoPayee,
		},
		{
			name: "no creator", creator: nil, sender: sender,
			payouts: []BoostPayout{{sender, 1.00}}, err: ErrBoostNoCreator,
		},
		{
			name: "no payer", payer: true, creator: creator, sender: sender,
			payouts: []BoostPayout{{sender, 0.80}, {creator, 0.20}}, err: ErrBoostNoPayer,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p := payer
			if tc.payer {
				p = nil
			}
			total, err := ValidateBoostPayouts(p, tc.creator, tc.sender, tc.payouts, tolerance)
			if tc.err != nil {
				require.ErrorIs(t, err, tc.err)
				require.Zero(t, total)
				return
			}
			require.NoError(t, err)
			require.InDelta(t, tc.total, total, 1e-9)
		})
	}
}

// userID is a distinct 16-byte user ID per n.
func TestDmBoostPayees(t *testing.T) {
	payer, sender := userID(1), userID(2)

	got, err := DmBoostPayees(payer, sender)
	require.NoError(t, err)
	require.Len(t, got, 1)
	require.True(t, sameUser(sender, got[0].UserID))
	require.EqualValues(t, 10000, got[0].Bps)

	_, err = DmBoostPayees(payer, payer)
	require.ErrorIs(t, err, ErrBoostOwnMessage)
	_, err = DmBoostPayees(payer, nil)
	require.ErrorIs(t, err, ErrBoostNoPayee)
	_, err = DmBoostPayees(nil, sender)
	require.ErrorIs(t, err, ErrBoostNoPayer)
}

func TestValidateDmBoostPayouts(t *testing.T) {
	payer, sender, stranger := userID(1), userID(2), userID(3)
	const tolerance = 0.005

	total, err := ValidateDmBoostPayouts(payer, sender, []BoostPayout{{sender, 0.01}}, tolerance)
	require.NoError(t, err)
	require.Equal(t, 0.01, total)

	for name, tc := range map[string]struct {
		sender  *commonpb.UserId
		payouts []BoostPayout
		err     error
	}{
		"own message":     {payer, []BoostPayout{{payer, 1.00}}, ErrBoostOwnMessage},
		"stranger paid":   {sender, []BoostPayout{{stranger, 1.00}}, ErrBoostPayoutMismatch},
		"payer paid":      {sender, []BoostPayout{{payer, 1.00}}, ErrBoostPayoutMismatch},
		"sender split":    {sender, []BoostPayout{{sender, 0.80}, {sender, 0.20}}, ErrBoostPayoutMismatch},
		"sender and more": {sender, []BoostPayout{{sender, 0.80}, {stranger, 0.20}}, ErrBoostPayoutMismatch},
		"no payouts":      {sender, nil, ErrBoostPayoutMismatch},
		"empty payout":    {sender, []BoostPayout{{sender, 0}}, ErrBoostPayoutEmpty},
	} {
		_, err := ValidateDmBoostPayouts(payer, tc.sender, tc.payouts, tolerance)
		require.ErrorIs(t, err, tc.err, name)
	}
}

func userID(n byte) *commonpb.UserId {
	id := make([]byte, 16)
	id[15] = n
	return &commonpb.UserId{Value: id}
}

func TestUsdMicros(t *testing.T) {
	require.Equal(t, uint64(0), MicrosFromUsd(0))
	require.Equal(t, uint64(0), MicrosFromUsd(-1))
	require.Equal(t, uint64(0), MicrosFromUsd(math.NaN()))
	require.Equal(t, uint64(0), MicrosFromUsd(math.Inf(1)))
	require.Equal(t, uint64(1_000_000), MicrosFromUsd(1))
	require.Equal(t, uint64(4_500_000), MicrosFromUsd(4.5))
	require.Equal(t, uint64(1), MicrosFromUsd(0.0000005), "half a micro rounds up")
	require.Equal(t, uint64(0), MicrosFromUsd(0.0000004), "under half a micro is nothing")
	require.Equal(t, uint64(123_456_789), MicrosFromUsd(123.456789))

	require.Equal(t, float64(0), UsdFromMicros(0))
	require.InDelta(t, 4.5, UsdFromMicros(4_500_000), 1e-12)
	require.InDelta(t, 123.456789, UsdFromMicros(123_456_789), 1e-9)
}
