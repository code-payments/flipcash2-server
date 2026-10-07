//go:build integration

package dynamodb

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	chat_memory "github.com/code-payments/flipcash2-server/chat/memory"
	"github.com/code-payments/flipcash2-server/poll/tests"
)

const (
	serverPollsTable = "polls_server_test"
	serverVotesTable = "poll_votes_server_test"
)

func TestPoll_DynamoDBServer(t *testing.T) {
	require.NoError(t, CreateTables(context.Background(), testEnv.Client, serverPollsTable, serverVotesTable))

	chats := chat_memory.NewInMemory(nil)
	testStore := NewInDynamoDB(testEnv.Client, serverPollsTable, serverVotesTable)
	teardown := func() {
		testStore.(*store).reset()
	}
	tests.RunServerTests(t, chats, testStore, teardown)
}
