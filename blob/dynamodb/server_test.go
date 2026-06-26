//go:build integration

package dynamodb

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	account_memory "github.com/code-payments/flipcash2-server/account/memory"
	blob_memory "github.com/code-payments/flipcash2-server/blob/memory"
	"github.com/code-payments/flipcash2-server/blob/tests"
)

func TestBlob_DynamoDBServer(t *testing.T) {
	require.NoError(t, CreateTables(context.Background(), testEnv.Client, blobsTable))

	accounts := account_memory.NewInMemory()
	blobs := NewInDynamoDB(testEnv.Client, blobsTable)
	// The object storage is always the in-memory fake; only the metadata store is
	// exercised against DynamoDB here. Its keys are per-blob random ids, so leftover
	// objects across test funcs never collide and it needs no reset.
	storage := blob_memory.NewInMemoryStorage()
	teardown := func() {
		blobs.(*store).reset()
	}
	tests.RunServerTests(t, accounts, blobs, storage, storage.SimulateUpload, teardown)
}
