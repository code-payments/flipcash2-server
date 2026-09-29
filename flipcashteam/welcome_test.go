package flipcashteam_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	chatpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/chat/v1"

	"github.com/code-payments/flipcash2-server/chat"
	"github.com/code-payments/flipcash2-server/flipcashteam"
	"github.com/code-payments/flipcash2-server/profile"
)

func TestSendWelcomeV1(t *testing.T) {
	e := newChatEnv(t)

	chatID, sent, err := flipcashteam.SendWelcomeV1(e.ctx, e.chats, e.sender, e.user, "alice_1")
	require.NoError(t, err)
	require.Equal(t, chat.MustDeriveDmChatID(chatpb.ChatType_TIP_DM, e.team, e.user).Value, chatID.Value)

	expected := []string{
		"Welcome to Flipcash!",
		"Tell people your Flipcash username is alice_1 to connect with you",
		"https://flipcash.com/alice_1",
		"Flipcash is the only chat app where people have to send you cash before they can message you, so you can say goodbye to spam",
		"Happy chatting!",
	}
	require.Len(t, sent, len(expected))
	for i, text := range expected {
		require.Equal(t, uint64(i+1), sent[i].MessageId.Value)
		require.Equal(t, e.team.Value, sent[i].SenderId.Value)
		require.Equal(t, text, sent[i].Content[0].GetText().Text)
	}

	// A retry, even with another handle, returns the first welcome and sends
	// nothing new.
	_, replayed, err := flipcashteam.SendWelcomeV1(e.ctx, e.chats, e.sender, e.user, "bob")
	require.NoError(t, err)
	require.Len(t, replayed, len(expected))
	require.Equal(t, expected[1], replayed[1].Content[0].GetText().Text)
	stored, err := e.messages.GetMessages(e.ctx, chatID)
	require.NoError(t, err)
	require.Len(t, stored, len(expected))
}

func TestWelcomer(t *testing.T) {
	e := newChatEnv(t)
	welcomer := flipcashteam.NewWelcomer(zap.NewNop(), e.chats, e.sender)
	chatID := chat.MustDeriveDmChatID(chatpb.ChatType_TIP_DM, e.team, e.user)

	// The request that assigned the handle may be gone by the time the welcome
	// is sent; it is sent all the same. Told twice, as racing claims would, it
	// is sent once.
	ctx, cancel := context.WithCancel(e.ctx)
	cancel()
	welcomer.OnFirstUsername(ctx, e.user, "alice_1")
	welcomer.OnFirstUsername(ctx, e.user, "alice_1")

	stored := func() int {
		msgs, err := e.messages.GetMessages(e.ctx, chatID)
		require.NoError(t, err)
		return len(msgs)
	}
	require.Eventually(t, func() bool { return stored() == 5 }, time.Second, 10*time.Millisecond)
	require.Never(t, func() bool { return stored() != 5 }, 200*time.Millisecond, 10*time.Millisecond)
}

func TestSendWelcomeV1_InvalidUsername(t *testing.T) {
	e := newChatEnv(t)

	// A handle not in canonical form is refused, never repaired: it did not
	// come from the store.
	for _, username := range []string{"", "a", "Alice_1", "has space", "has/slash", "waytoolongausername"} {
		_, _, err := flipcashteam.SendWelcomeV1(e.ctx, e.chats, e.sender, e.user, username)
		require.ErrorIs(t, err, profile.ErrInvalidUsername, "username: %q", username)
	}

	// Nothing was created.
	_, err := e.chats.GetChatByID(e.ctx, chat.MustDeriveDmChatID(chatpb.ChatType_TIP_DM, e.team, e.user))
	require.ErrorIs(t, err, chat.ErrChatNotFound)
}
