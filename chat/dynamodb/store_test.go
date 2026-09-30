//go:build integration

package dynamodb

import (
	"context"
	"crypto/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	chatpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/chat/v1"
	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"
	messagingpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/messaging/v1"

	"github.com/code-payments/flipcash2-server/chat"
	"github.com/code-payments/flipcash2-server/chat/tests"
	"github.com/code-payments/flipcash2-server/model"
)

const (
	chatsTable        = "chats_test"
	dmInboxTable      = "dm_inbox_test"
	groupMembersTable = "group_members_test"
	userStateTable    = "chat_user_state_test"
)

func TestChat_DynamoDBStore(t *testing.T) {
	require.NoError(t, CreateTables(context.Background(), testEnv.Client, chatsTable, dmInboxTable, groupMembersTable, userStateTable))

	testStore := NewInDynamoDB(testEnv.Client, chatsTable, dmInboxTable, groupMembersTable, userStateTable)
	teardown := func() {
		testStore.(*store).reset()
	}
	tests.RunStoreTests(t, testStore, teardown)
}

func TestChat_DynamoDBStoreOptions(t *testing.T) {
	require.NoError(t, CreateTables(context.Background(), testEnv.Client, chatsTable, dmInboxTable, groupMembersTable, userStateTable))

	var built []chat.Store
	t.Cleanup(func() {
		for _, s := range built {
			s.(*store).reset()
		}
	})
	tests.RunStoreOptionTests(t, func(opts ...chat.StoreOption) chat.Store {
		s := NewInDynamoDB(testEnv.Client, chatsTable, dmInboxTable, groupMembersTable, userStateTable, opts...)
		built = append(built, s)
		return s
	})
}

// TestChat_DynamoDBExclusionOutlivesConfig checks that a DM's exclusion from
// the feed is kept with the DM, not read from the store's options: a process
// built without the option, sharing the tables, advances a DM another created
// with it, leaves the excluded member's row alone, and moves the other's.
func TestChat_DynamoDBExclusionOutlivesConfig(t *testing.T) {
	ctx := context.Background()
	require.NoError(t, CreateTables(ctx, testEnv.Client, chatsTable, dmInboxTable, groupMembersTable, userStateTable))

	team := model.MustGenerateUserID()
	configured := NewInDynamoDB(testEnv.Client, chatsTable, dmInboxTable, groupMembersTable, userStateTable, chat.WithExcludedFromFeed(team))
	unconfigured := NewInDynamoDB(testEnv.Client, chatsTable, dmInboxTable, groupMembersTable, userStateTable)
	t.Cleanup(func() { configured.(*store).reset() })

	chatIDValue := make([]byte, chat.DmChatIDSize)
	_, err := rand.Read(chatIDValue)
	require.NoError(t, err)
	chatID := &commonpb.ChatId{Value: chatIDValue}
	user := model.MustGenerateUserID()
	created := time.Unix(1_700_000_100, 0).UTC()
	require.NoError(t, configured.PutChat(ctx, &chat.Chat{
		ID:           chatID,
		Type:         chatpb.ChatType_TIP_DM,
		Members:      []*commonpb.UserId{user, team},
		LastActivity: created,
	}))

	advancedTo := created.Add(time.Minute)
	advanced, _, err := unconfigured.AdvanceLastMessage(ctx, chatID, &messagingpb.MessageId{Value: 1}, advancedTo)
	require.NoError(t, err)
	require.True(t, advanced)

	snapshot := advancedTo.Add(time.Hour)
	userFeed, err := unconfigured.GetDmFeedPage(ctx, user, chatpb.ChatType_TIP_DM, snapshot, nil, 0)
	require.NoError(t, err)
	require.Len(t, userFeed, 1)
	require.True(t, userFeed[0].LastActivity.Equal(advancedTo))
	teamFeed, err := unconfigured.GetDmFeedPage(ctx, team, chatpb.ChatType_TIP_DM, snapshot, nil, 0)
	require.NoError(t, err)
	require.Empty(t, teamFeed)
}
