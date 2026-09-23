//go:build integration

package dynamodb

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	account_memory "github.com/code-payments/flipcash2-server/account/memory"
	"github.com/code-payments/flipcash2-server/e2ee/tests"
)

const (
	serverDevicesTable   = "e2ee_devices_server_test"
	serverMailboxesTable = "e2ee_mailboxes_server_test"
)

func TestE2ee_DynamoDBServer(t *testing.T) {
	require.NoError(t, CreateTables(context.Background(), testEnv.Client, serverDevicesTable, serverMailboxesTable))

	accounts := account_memory.NewInMemory()
	testStore := NewInDynamoDB(testEnv.Client, serverDevicesTable, serverMailboxesTable)
	teardown := func() {
		testStore.(*store).reset()
	}
	tests.RunServerTests(t, accounts, testStore, teardown)
}
