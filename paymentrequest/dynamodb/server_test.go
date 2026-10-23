//go:build integration

package dynamodb

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/code-payments/flipcash2-server/paymentrequest/tests"
)

const serverRequestsTable = "payment_requests_server_test"

func TestPaymentRequest_DynamoDBServer(t *testing.T) {
	require.NoError(t, CreateTables(context.Background(), testEnv.Client, serverRequestsTable))

	testStore := NewInDynamoDB(testEnv.Client, serverRequestsTable)
	teardown := func() {
		testStore.(*store).reset()
	}
	tests.RunServerTests(t, testStore, teardown)
}
