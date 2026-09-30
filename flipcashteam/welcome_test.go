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
	ocp_client "github.com/code-payments/ocp-server/grpc/client"
	ocp_headers "github.com/code-payments/ocp-server/grpc/headers"
)

func TestSendWelcomeV1(t *testing.T) {
	e := newChatEnv(t)

	chatID, sent, err := flipcashteam.SendWelcomeV1(e.ctx, e.chats, e.sender, e.user, "alice_1")
	require.NoError(t, err)
	require.Equal(t, chat.MustDeriveDmChatID(chatpb.ChatType_TIP_DM, e.team, e.user).Value, chatID.Value)

	// Every line is text but the third, a widget sharing the user's profile
	// (empty here).
	expected := []string{
		"Welcome to Flipcash!",
		"Tell people your Flipcash username is alice_1 to connect with you",
		"",
		"Flipcash is the only chat app where people have to send you cash before they can message you, so you can say goodbye to spam",
		"Happy chatting!",
	}
	require.Len(t, sent, len(expected))
	for i, text := range expected {
		require.Equal(t, uint64(i+1), sent[i].MessageId.Value)
		require.Equal(t, e.team.Value, sent[i].SenderId.Value)
		if i == 2 {
			require.Equal(t, "alice_1", sent[i].Content[0].GetWidget().GetShareProfile().GetUsername().GetValue())
			continue
		}
		require.Equal(t, text, sent[i].Content[0].GetText().Text)
	}

	// A retry, even with another handle, returns the first welcome and sends
	// nothing new.
	_, replayed, err := flipcashteam.SendWelcomeV1(e.ctx, e.chats, e.sender, e.user, "bob")
	require.NoError(t, err)
	require.Len(t, replayed, len(expected))
	require.Equal(t, expected[1], replayed[1].Content[0].GetText().Text)
	require.Equal(t, "alice_1", replayed[2].Content[0].GetWidget().GetShareProfile().GetUsername().GetValue())
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
	ctx, cancel := context.WithCancel(withUserAgent(t, e.ctx, "Flipcash/iOS/2026.9.5"))
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

// TestWelcomer_MinClientVersion: a welcome goes only to a user whose handle
// was assigned from a client that renders it, on iOS and Android alike; an
// older or unidentifiable client gets none.
func TestWelcomer_MinClientVersion(t *testing.T) {
	for _, tc := range []struct {
		userAgent string
		welcomed  bool
	}{
		{"Flipcash/iOS/2026.9.5", true},
		{"Flipcash/Android/2026.9.5", true},
		{"Flipcash/iOS/2026.10.0", true},
		{"Flipcash/Android/2027.1.0", true},
		{"Flipcash/iOS/2026.9.4", false},
		{"Flipcash/Android/2026.8.9", false},
		{"Mozilla/5.0", false},
		{"", false},
	} {
		t.Run(tc.userAgent, func(t *testing.T) {
			e := newChatEnv(t)
			welcomer := flipcashteam.NewWelcomer(zap.NewNop(), e.chats, e.sender)
			chatID := chat.MustDeriveDmChatID(chatpb.ChatType_TIP_DM, e.team, e.user)

			ctx := e.ctx
			if tc.userAgent != "" {
				ctx = withUserAgent(t, ctx, tc.userAgent)
			}
			welcomer.OnFirstUsername(ctx, e.user, "alice_1")

			stored := func() int {
				msgs, err := e.messages.GetMessages(e.ctx, chatID)
				require.NoError(t, err)
				return len(msgs)
			}
			if tc.welcomed {
				require.Eventually(t, func() bool { return stored() == 5 }, time.Second, 10*time.Millisecond)
			} else {
				require.Never(t, func() bool { return stored() != 0 }, 200*time.Millisecond, 10*time.Millisecond)
			}
		})
	}
}

// withUserAgent returns ctx carrying userAgent as the inbound user-agent
// header, as the headers interceptor leaves it on a request's context.
func withUserAgent(t *testing.T, ctx context.Context, userAgent string) context.Context {
	ctx, err := ocp_headers.ContextWithHeaders(ctx)
	require.NoError(t, err)
	require.NoError(t, ocp_headers.SetASCIIHeader(ctx, ocp_client.UserAgentHeaderName, userAgent))
	return ctx
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
