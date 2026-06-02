//go:build integration

package dynamodb

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/code-payments/flipcash2-server/messaging/tests"
)

const (
	messagesTable = "messages_test"
	pointersTable = "message_pointers_test"
)

func TestMessaging_DynamoDBStore(t *testing.T) {
	require.NoError(t, CreateTables(context.Background(), testEnv.Client, messagesTable, pointersTable))

	testStore := NewInDynamoDB(testEnv.Client, messagesTable, pointersTable)
	teardown := func() {
		testStore.(*store).reset()
	}
	tests.RunStoreTests(t, testStore, teardown)
}
