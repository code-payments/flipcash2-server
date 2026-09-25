package messaging

import (
	"testing"

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
	} {
		require.Equal(t, tc.less, ReactionLess(tc.a, tc.b), tc.name)
	}
}

func TestReactionSummary_ToProto_Order(t *testing.T) {
	// Deliberately out of wire order, with a count tie to exercise the tie-break.
	summary := &ReactionSummary{
		MessageID: &messagingpb.MessageId{Value: 7},
		Reactions: []*Reaction{
			{Emoji: "😂", Count: 1, Version: 5},
			{Emoji: "👍", Count: 3, Version: 1},
			{Emoji: "❤️", Count: 1, Version: 2},
		},
	}
	want := []string{"👍", "❤️", "😂"}

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
}
