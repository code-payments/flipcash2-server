//go:build integration

package dynamodb

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	account_memory "github.com/code-payments/flipcash2-server/account/memory"
	"github.com/code-payments/flipcash2-server/kyc/tests"
)

const serverKycCustomersTable = "kyc_customers_server_test"

func TestKyc_DynamoDBServer(t *testing.T) {
	require.NoError(t, CreateTable(context.Background(), testEnv.Client, serverKycCustomersTable))

	accounts := account_memory.NewInMemory()
	testStore := NewInDynamoDB(testEnv.Client, serverKycCustomersTable)
	teardown := func() {
		testStore.(*store).reset()
	}
	tests.RunServerTests(t, accounts, testStore, teardown)
}
