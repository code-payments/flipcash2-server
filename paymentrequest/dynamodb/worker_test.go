//go:build integration

package dynamodb

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/code-payments/flipcash2-server/paymentrequest/tests"
)

const workerRequestsTable = "payment_requests_worker_test"

func TestPaymentRequest_DynamoDBWorker(t *testing.T) {
	require.NoError(t, CreateTables(context.Background(), testEnv.Client, workerRequestsTable))

	testStore := NewInDynamoDB(testEnv.Client, workerRequestsTable)
	teardown := func() {
		testStore.(*store).reset()
	}
	tests.RunWorkerTests(t, testStore, teardown)
}
