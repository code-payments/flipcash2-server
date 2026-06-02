//go:build integration

package dynamodb

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/code-payments/flipcash2-server/chat/tests"
)

const (
	serverChatsTable   = "chats_server_test"
	serverDmInboxTable = "dm_inbox_server_test"
)

func TestChat_DynamoDBServer(t *testing.T) {
	require.NoError(t, CreateTables(context.Background(), testEnv.Client, serverChatsTable, serverDmInboxTable))

	testStore := NewInDynamoDB(testEnv.Client, serverChatsTable, serverDmInboxTable)
	teardown := func() {
		testStore.(*store).reset()
	}
	tests.RunServerTests(t, testStore, teardown)
}
