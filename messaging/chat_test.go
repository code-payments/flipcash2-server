package messaging_test

import (
	"context"
	"crypto/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"
	messagingpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/messaging/v1"

	"github.com/code-payments/flipcash2-server/chat"
	"github.com/code-payments/flipcash2-server/messaging"
	"github.com/code-payments/flipcash2-server/messaging/memory"
	"github.com/code-payments/flipcash2-server/model"
)

func TestChatMessagingReader(t *testing.T) {
	ctx := context.Background()
	store := memory.NewInMemory()
	reader := messaging.NewChatMessagingReader(store)

	chatA := randomChatID()
	chatB := randomChatID()
	sender := model.MustGenerateUserID()

	_, err := store.PutMessage(ctx, chatA, sender, textContent("a1"), time.Unix(1, 0), randomClientID(), true)
	require.NoError(t, err)
	a2, err := store.PutMessage(ctx, chatA, sender, textContent("a2"), time.Unix(2, 0), randomClientID(), true)
	require.NoError(t, err)
	b1, err := store.PutMessage(ctx, chatB, sender, textContent("b1"), time.Unix(1, 0), randomClientID(), true)
	require.NoError(t, err)

	// LastMessages: one ref per chat comes back keyed by chat ID, as proto.
	got, err := reader.LastMessages(ctx, []chat.MessageRef{
		{ChatID: chatA, MessageID: a2.ID},
		{ChatID: chatB, MessageID: b1.ID},
	})
	require.NoError(t, err)
	require.Len(t, got, 2)
	require.Equal(t, a2.ID.Value, got[string(chatA.Value)].MessageId.Value)
	require.Equal(t, "a2", got[string(chatA.Value)].Content[0].GetText().Text)
	require.Equal(t, b1.ID.Value, got[string(chatB.Value)].MessageId.Value)

	// Empty refs → empty map, no error.
	none, err := reader.LastMessages(ctx, nil)
	require.NoError(t, err)
	require.Empty(t, none)

	// Pointers: delegated, keyed by chat; a chat with none is absent.
	_, err = store.AdvancePointer(ctx, chatA, sender, messagingpb.Pointer_READ, a2.ID)
	require.NoError(t, err)
	pointers, err := reader.Pointers(ctx, []*commonpb.ChatId{chatA, chatB})
	require.NoError(t, err)
	require.Len(t, pointers[string(chatA.Value)], 1)
	require.Equal(t, messagingpb.Pointer_READ, pointers[string(chatA.Value)][0].Type)
	_, ok := pointers[string(chatB.Value)]
	require.False(t, ok)
}

func textContent(text string) []*messagingpb.Content {
	return []*messagingpb.Content{{
		Type: &messagingpb.Content_Text{Text: &messagingpb.TextContent{Text: text}},
	}}
}

func randomChatID() *commonpb.ChatId {
	b := make([]byte, chat.ChatIDSize)
	if _, err := rand.Read(b); err != nil {
		panic(err)
	}
	return &commonpb.ChatId{Value: b}
}

func randomClientID() *messagingpb.ClientMessageId {
	b := make([]byte, messaging.ClientMessageIDSize)
	if _, err := rand.Read(b); err != nil {
		panic(err)
	}
	return &messagingpb.ClientMessageId{Value: b}
}
