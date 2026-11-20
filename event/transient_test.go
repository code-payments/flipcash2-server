package event

import (
	"testing"

	"github.com/stretchr/testify/require"

	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"
	eventpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/event/v1"
	messagingpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/messaging/v1"
)

func TestIsTypingOnly(t *testing.T) {
	chatID := &commonpb.ChatId{Value: make([]byte, 16)}
	typing := &messagingpb.IsTypingNotificationBatch{
		IsTypingNotifications: []*messagingpb.IsTypingNotification{{
			UserId: &commonpb.UserId{Value: []byte("typer")},
		}},
	}
	chatUpdate := func(update *eventpb.ChatUpdate) *eventpb.Event {
		update.Chat = chatID
		return &eventpb.Event{Type: &eventpb.Event_ChatUpdate{ChatUpdate: update}}
	}

	require.True(t, isTypingOnly(chatUpdate(&eventpb.ChatUpdate{IsTypingNotifications: typing})))

	// Anything riding beside the typing notifications keeps the update whole.
	require.False(t, isTypingOnly(chatUpdate(&eventpb.ChatUpdate{
		IsTypingNotifications: typing,
		PointerUpdates:        &messagingpb.PointerBatch{},
	})))
	require.False(t, isTypingOnly(chatUpdate(&eventpb.ChatUpdate{
		IsTypingNotifications: typing,
		Events:                &messagingpb.EventBatch{},
	})))

	// No typing notifications at all.
	require.False(t, isTypingOnly(chatUpdate(&eventpb.ChatUpdate{Events: &messagingpb.EventBatch{}})))
	require.False(t, isTypingOnly(chatUpdate(&eventpb.ChatUpdate{})))
	require.False(t, isTypingOnly(&eventpb.Event{Type: &eventpb.Event_Test{Test: &eventpb.TestEvent{}}}))
}
