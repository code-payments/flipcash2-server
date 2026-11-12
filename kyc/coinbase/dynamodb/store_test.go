//go:build integration

package dynamodb

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/code-payments/flipcash2-server/kyc/coinbase/tests"
)

const customersTable = "coinbase_customers_test"

func TestCoinbaseKyc_DynamoDBStore(t *testing.T) {
	require.NoError(t, CreateTables(context.Background(), testEnv.Client, customersTable))

	testStore := NewInDynamoDB(testEnv.Client, customersTable)
	teardown := func() {
		testStore.(*store).reset()
	}
	tests.RunStoreTests(t, testStore, teardown)
}
