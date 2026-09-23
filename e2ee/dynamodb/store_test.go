//go:build integration

package dynamodb

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/code-payments/flipcash2-server/e2ee/tests"
)

const (
	storeDevicesTable   = "e2ee_devices_test"
	storeMailboxesTable = "e2ee_mailboxes_test"
)

func TestE2ee_DynamoDBStore(t *testing.T) {
	require.NoError(t, CreateTables(context.Background(), testEnv.Client, storeDevicesTable, storeMailboxesTable))

	testStore := NewInDynamoDB(testEnv.Client, storeDevicesTable, storeMailboxesTable)
	teardown := func() {
		testStore.(*store).reset()
	}
	tests.RunStoreTests(t, testStore, teardown)
}
