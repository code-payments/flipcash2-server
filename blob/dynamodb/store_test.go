//go:build integration

package dynamodb

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/code-payments/flipcash2-server/blob/tests"
)

const blobsTable = "blobs_test"

func TestBlob_DynamoDBStore(t *testing.T) {
	require.NoError(t, CreateTables(context.Background(), testEnv.Client, blobsTable))

	testStore := NewInDynamoDB(testEnv.Client, blobsTable)
	teardown := func() {
		testStore.(*store).reset()
	}
	tests.RunStoreTests(t, testStore, teardown)
}
