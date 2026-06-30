//go:build integration

package dynamodb

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/code-payments/flipcash2-server/blob/tests"
)

const aclTable = "blob_acls_test"

func TestBlobAccess_DynamoDBStore(t *testing.T) {
	require.NoError(t, CreateTables(context.Background(), testEnv.Client, blobsTable, aclTable))

	testStore := NewAccessInDynamoDB(testEnv.Client, aclTable)
	teardown := func() {
		testStore.(*accessStore).reset()
	}
	tests.RunAccessStoreTests(t, testStore, teardown)
}
