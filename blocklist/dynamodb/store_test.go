//go:build integration

package dynamodb

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/code-payments/flipcash2-server/blocklist/tests"
)

const storeTable = "blocklist_test"

func TestBlocklist_DynamoDBStore(t *testing.T) {
	require.NoError(t, CreateTable(context.Background(), testEnv.Client, storeTable))

	testStore := NewInDynamoDB(testEnv.Client, storeTable)
	teardown := func() {
		testStore.(*store).reset()
	}
	tests.RunStoreTests(t, testStore, teardown)
}
