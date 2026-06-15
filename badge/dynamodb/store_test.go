//go:build integration

package dynamodb

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/code-payments/flipcash2-server/badge/tests"
)

const badgesTable = "badges_test"

func TestBadge_DynamoDBStore(t *testing.T) {
	require.NoError(t, CreateTables(context.Background(), testEnv.Client, badgesTable))

	testStore := NewInDynamoDB(testEnv.Client, badgesTable)
	teardown := func() {
		testStore.(*store).reset()
	}
	tests.RunStoreTests(t, testStore, teardown)
}
