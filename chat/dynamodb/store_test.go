//go:build integration

package dynamodb

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/code-payments/flipcash2-server/chat/tests"
)

const (
	chatsTable        = "chats_test"
	dmInboxTable      = "dm_inbox_test"
	groupMembersTable = "group_members_test"
)

func TestChat_DynamoDBStore(t *testing.T) {
	require.NoError(t, CreateTables(context.Background(), testEnv.Client, chatsTable, dmInboxTable, groupMembersTable))

	testStore := NewInDynamoDB(testEnv.Client, chatsTable, dmInboxTable, groupMembersTable)
	teardown := func() {
		testStore.(*store).reset()
	}
	tests.RunStoreTests(t, testStore, teardown)
}
