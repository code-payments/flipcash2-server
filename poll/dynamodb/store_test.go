//go:build integration

package dynamodb

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/code-payments/flipcash2-server/poll/tests"
)

const (
	storePollsTable = "polls_store_test"
	storeVotesTable = "poll_votes_store_test"
)

func TestPoll_DynamoDBStore(t *testing.T) {
	require.NoError(t, CreateTables(context.Background(), testEnv.Client, storePollsTable, storeVotesTable))

	testStore := NewInDynamoDB(testEnv.Client, storePollsTable, storeVotesTable)
	teardown := func() {
		testStore.(*store).reset()
	}
	tests.RunStoreTests(t, testStore, teardown)
}
