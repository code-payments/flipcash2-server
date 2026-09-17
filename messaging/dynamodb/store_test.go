//go:build integration

package dynamodb

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/code-payments/flipcash2-server/messaging/tests"
)

const (
	messagesTable      = "messages_test"
	pointersTable      = "message_pointers_test"
	reactionsTable     = "message_reactions_test"
	reactorsTable      = "message_reactors_test"
	selfReactionsTable = "message_self_reactions_test"
)

func TestMessaging_DynamoDBStore(t *testing.T) {
	require.NoError(t, CreateTables(context.Background(), testEnv.Client, messagesTable, pointersTable, reactionsTable, reactorsTable, selfReactionsTable))

	testStore := NewInDynamoDB(testEnv.Client, messagesTable, pointersTable, reactionsTable, reactorsTable, selfReactionsTable)
	teardown := func() {
		testStore.(*store).reset()
	}
	tests.RunStoreTests(t, testStore, teardown)
}
