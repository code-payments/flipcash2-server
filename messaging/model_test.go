package messaging

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	messagingpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/messaging/v1"
)

func TestReactionLess(t *testing.T) {
	// 👍 (F0 9F 91 8D) < 😂 (F0 9F 98 82) bytewise; ❤️ (E2 9D A4 EF B8 8F) sorts before both.
	for _, tc := range []struct {
		name string
		a, b *Reaction
		less bool
	}{
		{"higher count first", &Reaction{Emoji: "😂", Count: 2}, &Reaction{Emoji: "👍", Count: 1}, true},
		{"lower count last", &Reaction{Emoji: "👍", Count: 1}, &Reaction{Emoji: "😂", Count: 2}, false},
		{"tie by emoji bytes", &Reaction{Emoji: "👍", Count: 1}, &Reaction{Emoji: "😂", Count: 1}, true},
		{"tie by emoji bytes, reversed", &Reaction{Emoji: "😂", Count: 1}, &Reaction{Emoji: "👍", Count: 1}, false},
		{"tie: shorter encoding first", &Reaction{Emoji: "❤️", Count: 1}, &Reaction{Emoji: "👍", Count: 1}, true},
		{"version never decides", &Reaction{Emoji: "😂", Count: 1, Version: 1}, &Reaction{Emoji: "👍", Count: 1, Version: 9}, false},
		{"equal is not less", &Reaction{Emoji: "👍", Count: 1}, &Reaction{Emoji: "👍", Count: 1}, false},
		{"money beats count", &Reaction{Emoji: "😂", Count: 1, BoostUsdMicros: 1}, &Reaction{Emoji: "👍", Count: 100}, true},
		{"money beats count, reversed", &Reaction{Emoji: "👍", Count: 100}, &Reaction{Emoji: "😂", Count: 1, BoostUsdMicros: 1}, false},
		{"more money first", &Reaction{Emoji: "😂", Count: 1, BoostUsdMicros: 2_000_000}, &Reaction{Emoji: "👍", Count: 5, BoostUsdMicros: 1_000_000}, true},
		{"money tie falls to count", &Reaction{Emoji: "😂", Count: 2, BoostUsdMicros: 5}, &Reaction{Emoji: "👍", Count: 1, BoostUsdMicros: 5}, true},
		{"money and count tie fall to emoji", &Reaction{Emoji: "👍", Count: 1, BoostUsdMicros: 5}, &Reaction{Emoji: "😂", Count: 1, BoostUsdMicros: 5}, true},
	} {
		require.Equal(t, tc.less, ReactionLess(tc.a, tc.b), tc.name)
	}
}

func TestReactorLess(t *testing.T) {
	a, b := userID(1), userID(2)
	for _, tc := range []struct {
		name string
		x, y *Reactor
		less bool
	}{
		{"newer first", &Reactor{UserID: a, Version: 5}, &Reactor{UserID: b, Version: 4}, true},
		{"older last", &Reactor{UserID: a, Version: 4}, &Reactor{UserID: b, Version: 5}, false},
		{"any boost beats no boost", &Reactor{UserID: a, Version: 1, BoostUsdMicros: 1}, &Reactor{UserID: b, Version: 99}, true},
		{"any boost beats no boost, reversed", &Reactor{UserID: a, Version: 99}, &Reactor{UserID: b, Version: 1, BoostUsdMicros: 1}, false},
		{"more boost first", &Reactor{UserID: a, Version: 1, BoostUsdMicros: 20}, &Reactor{UserID: b, Version: 9, BoostUsdMicros: 10}, true},
		{"boost tie falls to version", &Reactor{UserID: a, Version: 9, BoostUsdMicros: 10}, &Reactor{UserID: b, Version: 1, BoostUsdMicros: 10}, true},
		{"full tie falls to user id", &Reactor{UserID: a, Version: 1, BoostUsdMicros: 10}, &Reactor{UserID: b, Version: 1, BoostUsdMicros: 10}, true},
		{"full tie falls to user id, reversed", &Reactor{UserID: b, Version: 1, BoostUsdMicros: 10}, &Reactor{UserID: a, Version: 1, BoostUsdMicros: 10}, false},
		{"reacted_ts never decides", &Reactor{UserID: a, Version: 1, ReactedTs: time.Unix(9, 0)}, &Reactor{UserID: b, Version: 2, ReactedTs: time.Unix(1, 0)}, false},
	} {
		require.Equal(t, tc.less, ReactorLess(tc.x, tc.y), tc.name)
	}
}

func TestSampleFromReactors(t *testing.T) {
	// More than the sample holds, shuffled: the sample is the list's head —
	// boosters by amount, then the newest plain reactors — and nothing more.
	var reactors []*Reactor
	for v := uint64(1); v <= uint64(MaxSampleReactors)+4; v++ {
		reactors = append(reactors, &Reactor{UserID: userID(byte(v)), Version: v})
	}
	// The two oldest reactors boosted, the smaller boost by the older.
	reactors[0].BoostUsdMicros = 1_000_000
	reactors[1].BoostUsdMicros = 5_000_000
	for i, j := range []int{3, 0, 7, 2, 9, 1, 5, 11, 4, 10, 6, 8} {
		reactors[i], reactors[j] = reactors[j], reactors[i]
	}

	sample := SampleFromReactors(reactors)
	require.Len(t, sample, MaxSampleReactors)
	require.Equal(t, uint64(2), sample[0].Version, "biggest booster first")
	require.Equal(t, uint64(1), sample[1].Version, "then the smaller booster")
	top := uint64(MaxSampleReactors) + 4
	for i := 2; i < MaxSampleReactors; i++ {
		require.Equal(t, top-uint64(i-2), sample[i].Version, "then plain reactors newest first at %d", i)
	}
}

func TestReaction_ToProto_Boost(t *testing.T) {
	r := &Reaction{
		Emoji:          "🔥",
		Count:          2,
		Version:        7,
		BoostUsdMicros: 12_345_678,
		Self:           &Reactor{UserID: userID(1), Version: 3, BoostUsdMicros: 2_345_678},
		SampleReactors: []*Reactor{{UserID: userID(2), Version: 7, BoostUsdMicros: 10_000_000}, {UserID: userID(1), Version: 3, BoostUsdMicros: 2_345_678}},
	}
	out := r.ToProto()
	require.InDelta(t, 12.345678, out.BoostUsd, 1e-9)
	require.InDelta(t, 2.345678, out.SelfReactor.BoostUsd, 1e-9)
	require.InDelta(t, 10, out.SampleReactors[0].BoostUsd, 1e-9)
	require.Zero(t, (&Reactor{UserID: userID(3), Version: 1}).ToProto().BoostUsd)
}

func TestReactionSummary_ToProto_Order(t *testing.T) {
	// Deliberately out of wire order, with a count tie to exercise the tie-break.
	summary := &ReactionSummary{
		MessageID: &messagingpb.MessageId{Value: 7},
		Reactions: []*Reaction{
			{Emoji: "😂", Count: 1, Version: 5},
			{Emoji: "👍", Count: 3, Version: 1},
			{Emoji: "❤️", Count: 1, Version: 2},
			{Emoji: "🔥", Count: 1, Version: 9, BoostUsdMicros: 1},
		},
	}
	want := []string{"🔥", "👍", "❤️", "😂"}

	// The same order however many times it is projected, and the model slice is
	// left exactly as it was.
	for i := 0; i < 3; i++ {
		out := summary.ToProto()
		require.Len(t, out.Reactions, len(want))
		for j, emoji := range want {
			require.Equal(t, emoji, out.Reactions[j].Emoji.Value, "position %d", j)
		}
	}
	require.Equal(t, "😂", summary.Reactions[0].Emoji)
	require.Equal(t, "👍", summary.Reactions[1].Emoji)
	require.Equal(t, "❤️", summary.Reactions[2].Emoji)
	require.Equal(t, "🔥", summary.Reactions[3].Emoji)
}
