//go:build integration

package dynamodb

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/code-payments/flipcash2-server/chat/tests"
)

func TestChat_DynamoDBHoldingsWorker(t *testing.T) {
	require.NoError(t, CreateTables(context.Background(), testEnv.Client, chatsTable, dmInboxTable, groupMembersTable, userStateTable, activityTable, keyEnvelopesTable, lobbiesTable, featuredGroupsTable, holdingsTable, invitesTable))

	testStore := NewInDynamoDB(testEnv.Client, chatsTable, dmInboxTable, groupMembersTable, userStateTable, activityTable, keyEnvelopesTable, lobbiesTable, featuredGroupsTable, holdingsTable, invitesTable, nil)
	teardown := func() {
		testStore.(*store).reset()
	}
	tests.RunHoldingsWorkerTests(t, testStore, teardown)
}
