package messaging

import (
	"math"
	"testing"

	"github.com/stretchr/testify/require"

	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"
)

func TestCreatorBoostShare(t *testing.T) {
	for _, tc := range []struct {
		total, want uint64
	}{
		{0, 0},
		{4, 0}, // a fifth of four quarks rounds down to nothing
		{5, 1},
		{9, 1}, // 1.8 rounds down
		{10, 2},
		{1_000_000, 200_000},
		{1_000_004, 200_000},
		{^uint64(0), 3689348814741910323}, // no overflow at the top of the range
	} {
		require.Equal(t, tc.want, CreatorBoostShare(tc.total), "total %d", tc.total)
	}
}

func TestBoostSplit(t *testing.T) {
	payer, creator, sender := userID(1), userID(2), userID(3)

	for _, tc := range []struct {
		name                   string
		payer, creator, sender *commonpb.UserId
		total                  uint64
		want                   []BoostPayout
		err                    error
	}{
		{name: "third party pays both", payer: payer, creator: creator, sender: sender, total: 1000, want: []BoostPayout{{sender, 800}, {creator, 200}}},
		{name: "sender takes the rounding", payer: payer, creator: creator, sender: sender, total: 1004, want: []BoostPayout{{sender, 804}, {creator, 200}}},
		{name: "smallest split", payer: payer, creator: creator, sender: sender, total: 5, want: []BoostPayout{{sender, 4}, {creator, 1}}},
		{name: "too small to split", payer: payer, creator: creator, sender: sender, total: 4, err: ErrBoostPayoutEmpty},
		{name: "sender is the creator, paid the whole", payer: payer, creator: creator, sender: creator, total: 1000, want: []BoostPayout{{creator, 1000}}},
		{name: "payer is the creator, sender paid the whole", payer: payer, creator: payer, sender: sender, total: 1000, want: []BoostPayout{{sender, 1000}}},
		{name: "one payee is not split", payer: payer, creator: payer, sender: sender, total: 1, want: []BoostPayout{{sender, 1}}},
		{name: "message has no sender", payer: payer, creator: creator, sender: nil, total: 1000, want: []BoostPayout{{creator, 1000}}},
		{name: "payer is the sender", payer: payer, creator: creator, sender: payer, total: 1000, err: ErrBoostOwnMessage},
		{name: "creator boosting their own message", payer: payer, creator: payer, sender: payer, total: 1000, err: ErrBoostOwnMessage},
		{name: "creator boosting a message with no sender", payer: payer, creator: payer, sender: nil, total: 1000, err: ErrBoostNoPayee},
		{name: "no creator", payer: payer, creator: nil, sender: sender, total: 1000, err: ErrBoostNoCreator},
		{name: "no payer", payer: nil, creator: creator, sender: sender, total: 1000, err: ErrBoostNoPayer},
		{name: "no payer is judged before no creator", payer: nil, creator: nil, sender: sender, total: 1000, err: ErrBoostNoPayer},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := BoostSplit(tc.payer, tc.creator, tc.sender, tc.total)
			if tc.err != nil {
				require.ErrorIs(t, err, tc.err)
				require.Nil(t, got)
				return
			}
			require.NoError(t, err)
			require.Len(t, got, len(tc.want))
			for i := range tc.want {
				require.True(t, sameUser(tc.want[i].UserID, got[i].UserID), "payee %d", i)
				require.Equal(t, tc.want[i].Quarks, got[i].Quarks, "payee %d", i)
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
			name: "80/20 to sender and creator", creator: creator, sender: sender,
			payouts: []BoostPayout{{sender, 800}, {creator, 200}}, total: 1000,
		},
		{
			name: "order does not matter", creator: creator, sender: sender,
			payouts: []BoostPayout{{creator, 200}, {sender, 800}}, total: 1000,
		},
		{
			name: "sender takes the rounding", creator: creator, sender: sender,
			// A fifth of 1004 is 200.8, rounded down to 200 for the creator.
			payouts: []BoostPayout{{sender, 804}, {creator, 200}}, total: 1004,
		},
		{
			name: "creator rounded up", creator: creator, sender: sender,
			// 200.8 is within a quark of 201 too.
			payouts: []BoostPayout{{sender, 803}, {creator, 201}}, total: 1004,
		},
		{
			name: "creator rounded down past a quark", creator: creator, sender: sender,
			// 199 is 1.8 quarks under 200.8.
			payouts: []BoostPayout{{sender, 805}, {creator, 199}}, err: ErrBoostShareMismatch,
		},
		{
			name: "creator rounded up past a quark", creator: creator, sender: sender,
			// 202 is 1.2 quarks over 200.8.
			payouts: []BoostPayout{{sender, 802}, {creator, 202}}, err: ErrBoostShareMismatch,
		},
		{
			name: "creator a quark under an exact share", creator: creator, sender: sender,
			payouts: []BoostPayout{{sender, 801}, {creator, 199}}, total: 1000,
		},
		{
			name: "creator a quark over an exact share", creator: creator, sender: sender,
			payouts: []BoostPayout{{sender, 799}, {creator, 201}}, total: 1000,
		},
		{
			name: "creator two quarks under an exact share", creator: creator, sender: sender,
			payouts: []BoostPayout{{sender, 802}, {creator, 198}}, err: ErrBoostShareMismatch,
		},
		{
			name: "creator two quarks over an exact share", creator: creator, sender: sender,
			payouts: []BoostPayout{{sender, 798}, {creator, 202}}, err: ErrBoostShareMismatch,
		},
		{
			name: "tolerance at the top of the range", creator: creator, sender: sender,
			// A fifth of 2^64-1 is exactly 3689348814741910323.
			payouts: []BoostPayout{{sender, ^uint64(0) - 3689348814741910324}, {creator, 3689348814741910324}}, total: ^uint64(0),
		},
		{
			name: "20/80 the other way", creator: creator, sender: sender,
			payouts: []BoostPayout{{sender, 200}, {creator, 800}}, err: ErrBoostShareMismatch,
		},
		{
			name: "even split", creator: creator, sender: sender,
			payouts: []BoostPayout{{creator, 500}, {sender, 500}}, err: ErrBoostShareMismatch,
		},
		{
			name: "sender is the creator, paid once", creator: creator, sender: creator,
			payouts: []BoostPayout{{creator, 1000}}, total: 1000,
		},
		{
			name: "payer is the creator, sender takes all", creator: payer, sender: sender,
			payouts: []BoostPayout{{sender, 1000}}, total: 1000,
		},
		{
			name: "payer is the creator, creator's part not kept", creator: payer, sender: sender,
			payouts: []BoostPayout{{sender, 800}}, total: 800,
		},
		{
			name: "one payee is not held to a share", creator: creator, sender: creator,
			payouts: []BoostPayout{{creator, 1}}, total: 1,
		},
		{
			name: "payer is the sender", creator: creator, sender: payer,
			payouts: []BoostPayout{{creator, 1000}}, err: ErrBoostOwnMessage,
		},
		{
			name: "creator boosting their own message", creator: payer, sender: payer,
			payouts: []BoostPayout{{stranger, 1000}}, err: ErrBoostOwnMessage,
		},
		{
			name: "one payee paid twice is not a split", creator: creator, sender: creator,
			payouts: []BoostPayout{{creator, 800}, {creator, 200}}, err: ErrBoostPayoutMismatch,
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
			payouts: []BoostPayout{{sender, 800}, {stranger, 200}}, err: ErrBoostPayoutMismatch,
		},
		{
			name: "payer paid", creator: creator, sender: sender,
			payouts: []BoostPayout{{sender, 800}, {payer, 200}}, err: ErrBoostPayoutMismatch,
		},
		{
			name: "creator named twice", creator: creator, sender: sender,
			payouts: []BoostPayout{{creator, 800}, {creator, 200}}, err: ErrBoostPayoutMismatch,
		},
		{
			name: "three payouts", creator: creator, sender: sender,
			payouts: []BoostPayout{{sender, 800}, {creator, 100}, {stranger, 100}}, err: ErrBoostPayoutMismatch,
		},
		{
			name: "no payouts", creator: creator, sender: sender,
			payouts: nil, err: ErrBoostPayoutEmpty,
		},
		{
			name: "empty payout", creator: creator, sender: sender,
			payouts: []BoostPayout{{sender, 1000}, {creator, 0}}, err: ErrBoostPayoutEmpty,
		},
		{
			name: "too small to split", creator: creator, sender: sender,
			payouts: []BoostPayout{{sender, 3}, {creator, 1}}, err: ErrBoostPayoutEmpty,
		},
		{
			name: "no one to pay", creator: payer, sender: nil,
			payouts: []BoostPayout{{creator, 1000}}, err: ErrBoostNoPayee,
		},
		{
			name: "no creator", creator: nil, sender: sender,
			payouts: []BoostPayout{{sender, 1000}}, err: ErrBoostNoCreator,
		},
		{
			name: "no payer", payer: true, creator: creator, sender: sender,
			payouts: []BoostPayout{{sender, 800}, {creator, 200}}, err: ErrBoostNoPayer,
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
