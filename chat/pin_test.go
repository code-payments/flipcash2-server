package chat

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	chatpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/chat/v1"
	messagingpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/messaging/v1"

	"github.com/code-payments/flipcash2-server/model"
)

func TestPin_ToProto(t *testing.T) {
	pin := Pin{
		MessageID: &messagingpb.MessageId{Value: 42},
		PinnedBy:  model.MustGenerateUserID(),
		PinnedAt:  time.Unix(1_700_000_000, 123_456_789).UTC(),
	}
	message := &messagingpb.Message{
		MessageId:     &messagingpb.MessageId{Value: 42},
		SenderId:      model.MustGenerateUserID(),
		Content:       []*messagingpb.Content{{Type: &messagingpb.Content_Text{Text: &messagingpb.TextContent{Text: "hi"}}}},
		Ts:            timestamppb.Now(),
		EventSequence: 1,
	}

	got := pin.ToProto(message)
	require.NoError(t, got.Validate())
	require.Equal(t, uint64(42), got.MessageId.Value)
	require.Equal(t, pin.PinnedBy.Value, got.PinnedBy.Value)
	require.True(t, got.PinnedAt.AsTime().Equal(pin.PinnedAt))
	require.Same(t, message, got.Message)

	// The projection shares no memory with the pin.
	got.MessageId.Value = 7
	got.PinnedBy.Value[0] ^= 0xff
	require.Equal(t, uint64(42), pin.MessageID.Value)
	require.NotEqual(t, pin.PinnedBy.Value[0], got.PinnedBy.Value[0])
}

func TestPinSummary_ToProto(t *testing.T) {
	// No pin: latest omitted, and the message ignored.
	empty := PinSummary{Version: 3}.ToProto(&messagingpb.Message{})
	require.True(t, proto.Equal(&chatpb.PinSummary{Version: 3}, empty))
	require.True(t, proto.Equal(&chatpb.PinSummary{}, PinSummary{}.ToProto(nil)))

	pin := Pin{
		MessageID: &messagingpb.MessageId{Value: 9},
		PinnedBy:  model.MustGenerateUserID(),
		PinnedAt:  time.Unix(1_700_000_000, 1).UTC(),
	}
	message := &messagingpb.Message{MessageId: &messagingpb.MessageId{Value: 9}}
	got := PinSummary{Latest: &pin, Count: 2, Version: 5}.ToProto(message)
	require.True(t, proto.Equal(&chatpb.PinSummary{Latest: pin.ToProto(message), Count: 2, Version: 5}, got))
	require.Same(t, message, got.Latest.Message)
}

func TestPinPosition_Compare(t *testing.T) {
	at := func(s int64) time.Time { return time.Unix(1_700_000_000+s, 0).UTC() }
	id := func(v uint64) *messagingpb.MessageId { return &messagingpb.MessageId{Value: v} }

	// Later first, then the greater message ID first.
	require.Negative(t, PinPosition{PinnedAt: at(2), MessageID: id(1)}.Compare(PinPosition{PinnedAt: at(1), MessageID: id(9)}))
	require.Positive(t, PinPosition{PinnedAt: at(1), MessageID: id(9)}.Compare(PinPosition{PinnedAt: at(2), MessageID: id(1)}))
	require.Negative(t, PinPosition{PinnedAt: at(1), MessageID: id(9)}.Compare(PinPosition{PinnedAt: at(1), MessageID: id(8)}))
	require.Positive(t, PinPosition{PinnedAt: at(1), MessageID: id(8)}.Compare(PinPosition{PinnedAt: at(1), MessageID: id(9)}))
	require.Zero(t, PinPosition{PinnedAt: at(1), MessageID: id(8)}.Compare(PinPosition{PinnedAt: at(1), MessageID: id(8)}))
}

func TestPinTime(t *testing.T) {
	local := time.Unix(1_700_000_000, 123_456_789).In(time.FixedZone("x", 3600))
	require.Equal(t, time.Unix(1_700_000_000, 123_456_789).UTC(), PinTime(local))

	now := time.Now()
	require.True(t, PinTime(now).Equal(now))
	require.Equal(t, time.UTC, PinTime(now).Location())
}
