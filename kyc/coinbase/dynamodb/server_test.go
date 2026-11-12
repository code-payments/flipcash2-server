//go:build integration

package dynamodb

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	account_memory "github.com/code-payments/flipcash2-server/account/memory"
	"github.com/code-payments/flipcash2-server/kyc/coinbase/tests"
)

const serverCustomersTable = "coinbase_customers_server_test"

func TestCoinbaseKyc_DynamoDBServer(t *testing.T) {
	require.NoError(t, CreateTables(context.Background(), testEnv.Client, serverCustomersTable))

	accounts := account_memory.NewInMemory()
	testStore := NewInDynamoDB(testEnv.Client, serverCustomersTable)
	teardown := func() {
		testStore.(*store).reset()
	}
	tests.RunServerTests(t, accounts, testStore, teardown)
}
