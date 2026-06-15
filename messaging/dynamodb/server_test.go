//go:build integration

package dynamodb

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	badge_dynamodb "github.com/code-payments/flipcash2-server/badge/dynamodb"
	chat_dynamodb "github.com/code-payments/flipcash2-server/chat/dynamodb"
	"github.com/code-payments/flipcash2-server/messaging/tests"
	profile_memory "github.com/code-payments/flipcash2-server/profile/memory"
)

const (
	chatsTable   = "chats_test"
	dmInboxTable = "dm_inbox_test"
	badgesTable  = "badges_test"
)

func TestMessaging_DynamoDBServer(t *testing.T) {
	ctx := context.Background()

	require.NoError(t, chat_dynamodb.CreateTables(ctx, testEnv.Client, chatsTable, dmInboxTable))
	require.NoError(t, CreateTables(ctx, testEnv.Client, messagesTable, pointersTable))
	require.NoError(t, badge_dynamodb.CreateTables(ctx, testEnv.Client, badgesTable))

	chats := chat_dynamodb.NewInDynamoDB(testEnv.Client, chatsTable, dmInboxTable)
	profiles := profile_memory.NewInMemory()
	messages := NewInDynamoDB(testEnv.Client, messagesTable, pointersTable)
	badges := badge_dynamodb.NewInDynamoDB(testEnv.Client, badgesTable)
	teardown := func() {
		// Each subtest's serverEnv uses a freshly generated chatID and user IDs,
		// so leftover chat rows can't collide; only the messages store (whose IDs
		// and idempotency keys are scoped per chat) needs clearing between runs.
		messages.(*store).reset()
	}
	tests.RunServerTests(t, chats, messages, profiles, badges, teardown)
}
