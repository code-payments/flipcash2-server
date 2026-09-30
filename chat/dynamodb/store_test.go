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

	testStore := NewInDynamoDB(testEnv.Client, chatsTable, dmInboxTable, groupMembersTable, userStateTable, nil)
	teardown := func() {
		testStore.(*store).reset()
	}
	// The stores newStore builds share testStore's tables, so its teardown
	// resets theirs too.
	newStore := func(excludedFromFeed []*commonpb.UserId) chat.Store {
		return NewInDynamoDB(testEnv.Client, chatsTable, dmInboxTable, groupMembersTable, userStateTable, excludedFromFeed)
	}
	tests.RunStoreTests(t, testStore, newStore, teardown)
}

// TestChat_DynamoDBExclusionOutlivesConfig checks that a DM's exclusion from
// the feed is kept with the DM, not read from the store's configuration: a
// process built excluding no one, sharing the tables, advances a DM another
// created excluding someone, leaves the excluded member's row alone, and moves
// the other's.
func TestChat_DynamoDBExclusionOutlivesConfig(t *testing.T) {
	ctx := context.Background()
	require.NoError(t, CreateTables(ctx, testEnv.Client, chatsTable, dmInboxTable, groupMembersTable, userStateTable))

	team := model.MustGenerateUserID()
	configured := NewInDynamoDB(testEnv.Client, chatsTable, dmInboxTable, groupMembersTable, userStateTable, []*commonpb.UserId{team})
	unconfigured := NewInDynamoDB(testEnv.Client, chatsTable, dmInboxTable, groupMembersTable, userStateTable, nil)
	t.Cleanup(func() { configured.(*store).reset() })

	chatIDValue := make([]byte, chat.DmChatIDSize)
	_, err := rand.Read(chatIDValue)
	require.NoError(t, err)
	chatID := &commonpb.ChatId{Value: chatIDValue}
	user := model.MustGenerateUserID()
	created := time.Unix(1_700_000_100, 0).UTC()
	require.NoError(t, configured.PutChat(ctx, &chat.Chat{
		ID:           chatID,
		Type:         chatpb.ChatType_DM,
		Members:      []*commonpb.UserId{user, team},
		LastActivity: created,
	}))

	advancedTo := created.Add(time.Minute)
	advanced, _, err := unconfigured.AdvanceLastMessage(ctx, chatID, &messagingpb.MessageId{Value: 1}, advancedTo)
	require.NoError(t, err)
	require.True(t, advanced)

	snapshot := advancedTo.Add(time.Hour)
	userFeed, err := unconfigured.GetDmFeedPage(ctx, user, chatpb.ChatType_DM, snapshot, nil, 0)
	require.NoError(t, err)
	require.Len(t, userFeed, 1)
	require.True(t, userFeed[0].LastActivity.Equal(advancedTo))
	teamFeed, err := unconfigured.GetDmFeedPage(ctx, team, chatpb.ChatType_DM, snapshot, nil, 0)
	require.NoError(t, err)
	require.Empty(t, teamFeed)
}
