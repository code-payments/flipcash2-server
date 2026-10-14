package messaging

import (
	"math"
	"testing"

	"github.com/stretchr/testify/require"

	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"
)

func TestMinimumBoostShare(t *testing.T) {
	for _, tc := range []struct {
		total, want uint64
	}{
		{0, 0},
		{1, 1},  // a tenth of one quark rounds up to one
		{9, 1},  // 0.9 rounds up
		{10, 1}, // exact
		{11, 2}, // 1.1 rounds up
		{1_000_000, 100_000},
		{1_000_001, 100_001},
		{^uint64(0), 1844674407370955162}, // no overflow at the top of the range
	} {
		require.Equal(t, tc.want, MinimumBoostShare(tc.total), "total %d", tc.total)
	}
}

func TestBoostPayees(t *testing.T) {
	payer, creator, sender := userID(1), userID(2), userID(3)

	for _, tc := range []struct {
		name                   string
		payer, creator, sender *commonpb.UserId
		want                   []*commonpb.UserId
		err                    error
	}{
		{name: "third party pays both", payer: payer, creator: creator, sender: sender, want: []*commonpb.UserId{creator, sender}},
		{name: "payer is the sender", payer: payer, creator: creator, sender: payer, want: []*commonpb.UserId{creator}},
		{name: "payer is the creator", payer: payer, creator: payer, sender: sender, want: []*commonpb.UserId{sender}},
		{name: "sender is the creator", payer: payer, creator: creator, sender: creator, want: []*commonpb.UserId{creator}},
		{name: "message has no sender", payer: payer, creator: creator, sender: nil, want: []*commonpb.UserId{creator}},
		{name: "payer is creator and sender", payer: payer, creator: payer, sender: payer, err: ErrBoostNoPayee},
		{name: "payer is creator of a message with no sender", payer: payer, creator: payer, sender: nil, err: ErrBoostNoPayee},
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
				require.True(t, sameUser(tc.want[i], got[i]), "payee %d", i)
			}
		})
	}
}

func TestValidateBoostPayouts(t *testing.T) {
	payer, creator, sender, stranger := userID(1), userID(2), userID(3), userID(4)

	for _, tc := range []struct {
		name            string
		payer           bool // true to pass no payer
		creator, sender *commonpb.UserId
		payouts         []BoostPayout
		total           uint64
		err             error
	}{
		{
			name: "90/10 to sender and creator", creator: creator, sender: sender,
			payouts: []BoostPayout{{sender, 900}, {creator, 100}}, total: 1000,
		},
		{
			name: "10/90 the other way", creator: creator, sender: sender,
			payouts: []BoostPayout{{sender, 100}, {creator, 900}}, total: 1000,
		},
		{
			name: "even split", creator: creator, sender: sender,
			payouts: []BoostPayout{{creator, 500}, {sender, 500}}, total: 1000,
		},
		{
			name: "creator under the minimum", creator: creator, sender: sender,
			payouts: []BoostPayout{{sender, 950}, {creator, 50}}, err: ErrBoostShareBelowMinimum,
		},
		{
			name: "sender under the minimum", creator: creator, sender: sender,
			payouts: []BoostPayout{{sender, 50}, {creator, 950}}, err: ErrBoostShareBelowMinimum,
		},
		{
			name: "minimum rounds up", creator: creator, sender: sender,
			// A tenth of 1001 is 100.1, so 100 is short and 101 is enough.
			payouts: []BoostPayout{{sender, 901}, {creator, 100}}, err: ErrBoostShareBelowMinimum,
		},
		{
			name: "minimum met after rounding up", creator: creator, sender: sender,
			payouts: []BoostPayout{{sender, 900}, {creator, 101}}, total: 1001,
		},
		{
			name: "payer is the sender, creator takes all", creator: creator, sender: payer,
			payouts: []BoostPayout{{creator, 1000}}, total: 1000,
		},
		{
			name: "payer is the creator, sender takes all", creator: payer, sender: sender,
			payouts: []BoostPayout{{sender, 1000}}, total: 1000,
		},
		{
			name: "sender is the creator, paid once", creator: creator, sender: creator,
			payouts: []BoostPayout{{creator, 1000}}, total: 1000,
		},
		{
			name: "one payee is not held to a share", creator: creator, sender: payer,
			payouts: []BoostPayout{{creator, 1}}, total: 1,
		},
		{
			name: "one payee paid twice is not a split", creator: creator, sender: payer,
			payouts: []BoostPayout{{creator, 900}, {creator, 100}}, err: ErrBoostPayoutMismatch,
		},
		{
			name: "sender missing", creator: creator, sender: sender,
			payouts: []BoostPayout{{creator, 1000}}, err: ErrBoostPayoutMismatch,
		},
		{
			name: "creator missing", creator: creator, sender: sender,
			payouts: []BoostPayout{{sender, 1000}}, err: ErrBoostPayoutMismatch,
		},
		{
			name: "stranger paid", creator: creator, sender: sender,
			payouts: []BoostPayout{{sender, 900}, {stranger, 100}}, err: ErrBoostPayoutMismatch,
		},
		{
			name: "payer paid", creator: creator, sender: sender,
			payouts: []BoostPayout{{creator, 900}, {payer, 100}}, err: ErrBoostPayoutMismatch,
		},
		{
			name: "creator named twice", creator: creator, sender: sender,
			payouts: []BoostPayout{{creator, 900}, {creator, 100}}, err: ErrBoostPayoutMismatch,
		},
		{
			name: "three payouts", creator: creator, sender: sender,
			payouts: []BoostPayout{{sender, 800}, {creator, 100}, {stranger, 100}}, err: ErrBoostPayoutMismatch,
		},
		{
			name: "no payouts", creator: creator, sender: sender,
			payouts: nil, err: ErrBoostPayoutMismatch,
		},
		{
			name: "empty payout", creator: creator, sender: sender,
			payouts: []BoostPayout{{sender, 1000}, {creator, 0}}, err: ErrBoostPayoutEmpty,
		},
		{
			name: "one quark cannot be split", creator: creator, sender: sender,
			payouts: []BoostPayout{{sender, 1}, {creator, 0}}, err: ErrBoostPayoutEmpty,
		},
		{
			name: "no one to pay", creator: payer, sender: payer,
			payouts: []BoostPayout{{creator, 1000}}, err: ErrBoostNoPayee,
		},
		{
			name: "no creator", creator: nil, sender: sender,
			payouts: []BoostPayout{{sender, 1000}}, err: ErrBoostNoCreator,
		},
		{
			name: "no payer", payer: true, creator: creator, sender: sender,
			payouts: []BoostPayout{{sender, 900}, {creator, 100}}, err: ErrBoostNoPayer,
		},
		{
			name: "overflow", creator: creator, sender: sender,
			payouts: []BoostPayout{{sender, ^uint64(0)}, {creator, 1}}, err: ErrBoostTotalOverflow,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p := payer
			if tc.payer {
				p = nil
			}
			total, err := ValidateBoostPayouts(p, tc.creator, tc.sender, tc.payouts)
			if tc.err != nil {
				require.ErrorIs(t, err, tc.err)
				require.Zero(t, total)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.total, total)
		})
	}
}

// userID is a distinct 16-byte user ID per n.
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
