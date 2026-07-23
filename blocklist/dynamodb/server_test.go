//go:build integration

package dynamodb

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	account_memory "github.com/code-payments/flipcash2-server/account/memory"
	"github.com/code-payments/flipcash2-server/blocklist/tests"
)

const serverTable = "blocklist_server_test"

func TestBlocklist_DynamoDBServer(t *testing.T) {
	require.NoError(t, CreateTable(context.Background(), testEnv.Client, serverTable))

	accounts := account_memory.NewInMemory()
	testStore := NewInDynamoDB(testEnv.Client, serverTable)
	teardown := func() {
		testStore.(*store).reset()
	}
	tests.RunServerTests(t, accounts, testStore, teardown)
}
