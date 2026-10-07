//go:build integration

package dynamodb

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/code-payments/flipcash2-server/poll/tests"
)

const (
	workerPollsTable = "polls_worker_test"
	workerVotesTable = "poll_votes_worker_test"
)

func TestPoll_DynamoDBWorker(t *testing.T) {
	require.NoError(t, CreateTables(context.Background(), testEnv.Client, workerPollsTable, workerVotesTable))

	testStore := NewInDynamoDB(testEnv.Client, workerPollsTable, workerVotesTable)
	teardown := func() {
		testStore.(*store).reset()
	}
	tests.RunWorkerTests(t, testStore, teardown)
}
