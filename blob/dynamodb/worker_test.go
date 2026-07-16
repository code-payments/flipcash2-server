//go:build integration

package dynamodb

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	blob_memory "github.com/code-payments/flipcash2-server/blob/memory"
	"github.com/code-payments/flipcash2-server/blob/tests"
)

func TestBlob_DynamoDBWorker(t *testing.T) {
	require.NoError(t, CreateTables(context.Background(), testEnv.Client, blobsTable, aclTable))

	blobs := NewInDynamoDB(testEnv.Client, blobsTable)
	// The object storage is always the in-memory fake; only the metadata store —
	// including the finalization queue GSI the worker polls — is exercised
	// against DynamoDB here. Its keys are per-blob random ids, so leftover
	// objects across test funcs never collide and it needs no reset.
	storage := blob_memory.NewInMemoryStorage()
	teardown := func() {
		blobs.(*store).reset()
	}
	tests.RunWorkerTests(t, blobs, storage, storage.PutObject, teardown)
}
