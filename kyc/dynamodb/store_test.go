//go:build integration

package dynamodb

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/code-payments/flipcash2-server/kyc/tests"
)

const kycCustomersTable = "kyc_customers_test"

func TestKyc_DynamoDBStore(t *testing.T) {
	require.NoError(t, CreateTable(context.Background(), testEnv.Client, kycCustomersTable))

	testStore := NewInDynamoDB(testEnv.Client, kycCustomersTable)
	teardown := func() {
		testStore.(*store).reset()
	}
	tests.RunStoreTests(t, testStore, teardown)
}
