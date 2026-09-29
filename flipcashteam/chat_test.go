package flipcashteam_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"

	chatpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/chat/v1"
	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"
	eventpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/event/v1"
	messagingpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/messaging/v1"

	badge_memory "github.com/code-payments/flipcash2-server/badge/memory"
	blocklist_memory "github.com/code-payments/flipcash2-server/blocklist/memory"
	"github.com/code-payments/flipcash2-server/chat"
	chat_memory "github.com/code-payments/flipcash2-server/chat/memory"
	"github.com/code-payments/flipcash2-server/event"
	"github.com/code-payments/flipcash2-server/flipcashteam"
	"github.com/code-payments/flipcash2-server/messaging"
	messaging_memory "github.com/code-payments/flipcash2-server/messaging/memory"
	"github.com/code-payments/flipcash2-server/model"
	"github.com/code-payments/flipcash2-server/profile"
	profile_memory "github.com/code-payments/flipcash2-server/profile/memory"
	"github.com/code-payments/flipcash2-server/push"
	ocp_data "github.com/code-payments/ocp-server/ocp/data"
)

type chatEnv struct {
	ctx      context.Context
	profiles profile.Store
	chats    chat.Store
	messages messaging.Store
	sender   *messaging.Sender
	team     *commonpb.UserId
	user     *commonpb.UserId
}

func newChatEnv(t *testing.T) *chatEnv {
	return newChatEnvWithTeam(t, true)
}

// newChatEnvWithTeam builds the stores and Sender as the parent does at
// startup: the team account is resolved once, with GetUserID, and both the
// chat store and the Sender are built with it. setUp decides whether the team
// account was set up before the process started; if not, there is none.
func newChatEnvWithTeam(t *testing.T, setUp bool) *chatEnv {
	ctx := context.Background()
	profiles := profile_memory.NewInMemory()
	team := model.MustGenerateUserID()
	if setUp {
		require.NoError(t, flipcashteam.AssignUsername(ctx, profiles, team))
	}
	teamUserID, err := flipcashteam.GetUserID(ctx, profiles)
	if err != nil {
		require.ErrorIs(t, err, profile.ErrNotFound)
		teamUserID = nil
	}

	chats := chat_memory.NewInMemory(chat.WithExcludedFromFeed(teamUserID))
	messages := messaging_memory.NewInMemory()
	sender := messaging.NewSender(
		zaptest.NewLogger(t),
		badge_memory.NewInMemory(),
		chats,
		messages,
		profiles,
		blocklist_memory.NewInMemory(),
		nil,
		ocp_data.NewTestDataProvider(),
		push.NewNoOpPusher(),
		event.NewBus[*commonpb.UserId, *eventpb.Event](),
		event.NewBus[*commonpb.ChatId, *eventpb.ChatEvent](),
		messaging.WithTeamAccount(teamUserID),
	)

	return &chatEnv{
		ctx:      ctx,
		profiles: profiles,
		chats:    chats,
		messages: messages,
		sender:   sender,
		team:     team,
		user:     model.MustGenerateUserID(),
	}
}

func text(s string) *messagingpb.Content {
	return &messagingpb.Content{Type: &messagingpb.Content_Text{Text: &messagingpb.TextContent{Text: s}}}
}

func TestSendMessages(t *testing.T) {
	e := newChatEnv(t)

	chatID, sent, err := flipcashteam.SendMessages(e.ctx, e.chats, e.sender, e.user, "welcome", []*messagingpb.Content{text("hi"), text("welcome")})
	require.NoError(t, err)

	// The chat is the pair's canonical tip DM.
	require.Equal(t, chat.MustDeriveDmChatID(chatpb.ChatType_TIP_DM, e.team, e.user).Value, chatID.Value)
	record, err := e.chats.GetChatByID(e.ctx, chatID)
	require.NoError(t, err)
	require.Equal(t, chatpb.ChatType_TIP_DM, record.Type)
	isMember, err := e.chats.IsMember(e.ctx, chatID, e.user)
	require.NoError(t, err)
	require.True(t, isMember)

	// The team is a member excluded from the feed: its feed does not list
	// the chat, while the user's does.
	isMember, err = e.chats.IsMember(e.ctx, chatID, e.team)
	require.NoError(t, err)
	require.True(t, isMember)
	teamFeed, err := e.chats.GetDmFeedPage(e.ctx, e.team, chatpb.ChatType_TIP_DM, time.Now().Add(time.Hour), nil, 0)
	require.NoError(t, err)
	require.Empty(t, teamFeed)
	userFeed, err := e.chats.GetDmFeedPage(e.ctx, e.user, chatpb.ChatType_TIP_DM, time.Now().Add(time.Hour), nil, 0)
	require.NoError(t, err)
	require.Len(t, userFeed, 1)
	require.Equal(t, chatID.Value, userFeed[0].ID.Value)

	// The messages are the team's, in order.
	require.Len(t, sent, 2)
	for i, s := range []string{"hi", "welcome"} {
		require.Equal(t, uint64(i+1), sent[i].MessageId.Value)
		require.Equal(t, e.team.Value, sent[i].SenderId.Value)
		require.Equal(t, s, sent[i].Content[0].GetText().Text)
	}

	// A retry under the same key sends nothing new.
	_, replayed, err := flipcashteam.SendMessages(e.ctx, e.chats, e.sender, e.user, "welcome", []*messagingpb.Content{text("hi"), text("welcome")})
	require.NoError(t, err)
	require.Equal(t, sent[1].MessageId.Value, replayed[1].MessageId.Value)
	stored, err := e.messages.GetMessages(e.ctx, chatID)
	require.NoError(t, err)
	require.Len(t, stored, 2)

	// Another key sends into the chat that already exists.
	_, more, err := flipcashteam.SendMessages(e.ctx, e.chats, e.sender, e.user, "follow-up", []*messagingpb.Content{text("again")})
	require.NoError(t, err)
	require.Equal(t, uint64(3), more[0].MessageId.Value)
}

// createCountingStore counts the chats a chat.Store is asked to create, and
// can make every lookup miss, as a stale read of a chat just created would.
type createCountingStore struct {
	chat.Store
	puts       int
	staleReads bool
}

func (s *createCountingStore) PutChat(ctx context.Context, c *chat.Chat) error {
	s.puts++
	return s.Store.PutChat(ctx, c)
}

func (s *createCountingStore) GetChatByID(ctx context.Context, chatID *commonpb.ChatId) (*chat.Chat, error) {
	if s.staleReads {
		return nil, chat.ErrChatNotFound
	}
	return s.Store.GetChatByID(ctx, chatID)
}

func TestSendMessages_CreatesChatOnce(t *testing.T) {
	e := newChatEnv(t)
	chats := &createCountingStore{Store: e.chats}

	// The first send creates the chat.
	_, _, err := flipcashteam.SendMessages(e.ctx, chats, e.sender, e.user, "welcome", []*messagingpb.Content{text("hi")})
	require.NoError(t, err)
	require.Equal(t, 1, chats.puts)

	// Later sends find it and never ask to create it again.
	_, _, err = flipcashteam.SendMessages(e.ctx, chats, e.sender, e.user, "follow-up", []*messagingpb.Content{text("again")})
	require.NoError(t, err)
	require.Equal(t, 1, chats.puts)

	// A lookup that misses a chat that exists falls through to a create that
	// finds it, and the send goes ahead into it.
	chats.staleReads = true
	chatID, sent, err := flipcashteam.SendMessages(e.ctx, chats, e.sender, e.user, "stale", []*messagingpb.Content{text("still")})
	require.NoError(t, err)
	require.Equal(t, 2, chats.puts)
	require.Equal(t, uint64(3), sent[0].MessageId.Value)
	stored, err := e.messages.GetMessages(e.ctx, chatID)
	require.NoError(t, err)
	require.Len(t, stored, 3)
}

func TestSendMessages_RetryByPosition(t *testing.T) {
	e := newChatEnv(t)

	chatID, sent, err := flipcashteam.SendMessages(e.ctx, e.chats, e.sender, e.user, "welcome", []*messagingpb.Content{text("hi"), text("welcome")})
	require.NoError(t, err)
	require.Len(t, sent, 2)

	// Fewer messages under the same key replay the first call's leading ones,
	// whatever the retry's content.
	_, replayed, err := flipcashteam.SendMessages(e.ctx, e.chats, e.sender, e.user, "welcome", []*messagingpb.Content{text("different")})
	require.NoError(t, err)
	require.Len(t, replayed, 1)
	require.Equal(t, sent[0].MessageId.Value, replayed[0].MessageId.Value)
	require.Equal(t, "hi", replayed[0].Content[0].GetText().Text)

	// More messages under the same key fail.
	_, _, err = flipcashteam.SendMessages(e.ctx, e.chats, e.sender, e.user, "welcome", []*messagingpb.Content{text("hi"), text("welcome"), text("more")})
	require.Error(t, err)

	// Neither sent anything.
	stored, err := e.messages.GetMessages(e.ctx, chatID)
	require.NoError(t, err)
	require.Len(t, stored, 2)
}

func TestSendMessages_Refused(t *testing.T) {
	e := newChatEnv(t)
	messages := []*messagingpb.Content{text("hi")}

	_, _, err := flipcashteam.SendMessages(e.ctx, e.chats, e.sender, e.team, "welcome", messages)
	require.Error(t, err)

	_, _, err = flipcashteam.SendMessages(e.ctx, e.chats, e.sender, e.user, "", messages)
	require.Error(t, err)

	_, _, err = flipcashteam.SendMessages(e.ctx, e.chats, e.sender, e.user, "welcome", nil)
	require.Error(t, err)

	// A missing or malformed user ID is refused, never a panic.
	_, _, err = flipcashteam.SendMessages(e.ctx, e.chats, e.sender, nil, "welcome", messages)
	require.Error(t, err)
	_, _, err = flipcashteam.SendMessages(e.ctx, e.chats, e.sender, &commonpb.UserId{Value: []byte{1, 2, 3}}, "welcome", messages)
	require.Error(t, err)
}

func TestSendMessages_NoTeamAccount(t *testing.T) {
	// A process started before the team account was set up has none, and
	// sends as no one, even once the handle is given out: it takes a restart.
	e := newChatEnvWithTeam(t, false)
	require.NoError(t, flipcashteam.AssignUsername(e.ctx, e.profiles, e.team))

	_, _, err := flipcashteam.SendMessages(e.ctx, e.chats, e.sender, e.user, "welcome", []*messagingpb.Content{text("hi")})
	require.ErrorIs(t, err, flipcashteam.ErrNoTeamAccount)

	// Nothing was created.
	_, err = e.chats.GetChatByID(e.ctx, chat.MustDeriveDmChatID(chatpb.ChatType_TIP_DM, e.team, e.user))
	require.ErrorIs(t, err, chat.ErrChatNotFound)
}
