package redact

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"
	messagingpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/messaging/v1"
)

func messageFixture(content ...*messagingpb.Content) *messagingpb.Message {
	return &messagingpb.Message{
		MessageId:     messageID,
		SenderId:      &commonpb.UserId{Value: bytes.Repeat([]byte{6}, 16)},
		Content:       content,
		Ts:            timestamppb.New(timestamppb.Now().AsTime()),
		UnreadSeq:     11,
		LastEditedTs:  timestamppb.New(timestamppb.Now().AsTime()),
		EventSequence: 57,
		Reactions: &messagingpb.ReactionSummary{
			MessageId: messageID,
			Reactions: []*messagingpb.EmojiReaction{{Emoji: &messagingpb.Emoji{Value: "🔥"}, Count: 3, Version: 4}},
		},
	}
}

func TestMessage(t *testing.T) {
	in := messageFixture(textFixture())
	before := proto.Clone(in)

	out, err := Message(chatID, in)
	require.NoError(t, err)
	assert.True(t, proto.Equal(before, in), "input modified")

	// The message's identity and envelope survive; its words do not.
	assert.True(t, out.Redacted)
	assert.True(t, proto.Equal(in.MessageId, out.MessageId))
	assert.True(t, proto.Equal(in.SenderId, out.SenderId))
	assert.True(t, proto.Equal(in.Ts, out.Ts))
	assert.True(t, proto.Equal(in.LastEditedTs, out.LastEditedTs))
	assert.Equal(t, in.UnreadSeq, out.UnreadSeq)
	assert.Equal(t, in.EventSequence, out.EventSequence)
	require.Len(t, out.Content, 1)
	assert.Equal(t, Text(contentSeed, secret), out.Content[0].GetText().GetText())

	// Reactions are an overlay a redacted viewer may not read: not carried.
	assert.Nil(t, out.Reactions)

	// The copies are copies: nothing in the output aliases the input.
	assert.NotSame(t, in.MessageId, out.MessageId)
	assert.NotSame(t, in.SenderId, out.SenderId)
	assert.NotSame(t, in.Ts, out.Ts)
	assert.NotSame(t, in.LastEditedTs, out.LastEditedTs)

	// A system message has no sender and was never edited; neither is invented.
	system := messageFixture(systemFixture())
	system.SenderId = nil
	system.LastEditedTs = nil
	out, err = Message(chatID, system)
	require.NoError(t, err)
	assert.Nil(t, out.SenderId)
	assert.Nil(t, out.LastEditedTs)
	assert.True(t, proto.Equal(system.Content[0], out.Content[0]))

	// Content that cannot be redacted fails the whole message.
	_, err = Message(chatID, messageFixture(&messagingpb.Content{}))
	assert.ErrorIs(t, err, ErrUnsupportedContent)
}

// TestMessage_Idempotent: a redacted message is its own redaction, so the
// chokepoint may run over a message more than once.
func TestMessage_Idempotent(t *testing.T) {
	for _, c := range fixturesForAllKinds() {
		once, err := Message(chatID, messageFixture(c))
		require.NoError(t, err)
		twice, err := Message(chatID, once)
		require.NoError(t, err)
		assert.True(t, proto.Equal(once, twice), "%v", c)
	}
}
