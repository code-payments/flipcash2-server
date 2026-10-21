//go:build integration

package dynamodb

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/require"

	paymentrequestpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/paymentrequest/v1"

	"github.com/code-payments/flipcash2-server/paymentrequest"
	"github.com/code-payments/flipcash2-server/paymentrequest/tests"
)

const storeRequestsTable = "payment_requests_store_test"

func TestPaymentRequest_DynamoDBStore(t *testing.T) {
	require.NoError(t, CreateTables(context.Background(), testEnv.Client, storeRequestsTable))

	testStore := NewInDynamoDB(testEnv.Client, storeRequestsTable)
	teardown := func() {
		testStore.(*store).reset()
	}
	tests.RunStoreTests(t, testStore, teardown)
}

// TestPaymentRequest_DynamoDBStore_RefusesUnknownType checks that a request of
// a type this server does not know is refused on read rather than served as a
// split: a server that predates a type must fail on it, not guess.
func TestPaymentRequest_DynamoDBStore_RefusesUnknownType(t *testing.T) {
	ctx := context.Background()
	require.NoError(t, CreateTables(ctx, testEnv.Client, storeRequestsTable))

	testStore := NewInDynamoDB(testEnv.Client, storeRequestsTable)
	t.Cleanup(testStore.(*store).reset)

	id := &paymentrequestpb.PaymentRequestId{Value: []byte("0123456789abcdef")}
	for name, typ := range map[string]int64{"unknown": typeSplit + 1, "missing": 0} {
		t.Run(name, func(t *testing.T) {
			item := map[string]types.AttributeValue{
				attrPK:      avB(id.Value),
				attrSK:      avS(requestSK),
				attrStatus:  avNInt(statusOpen),
				attrVersion: avNInt(1),
			}
			if typ != 0 {
				item[attrType] = avNInt(typ)
			}
			_, err := testEnv.Client.PutItem(ctx, &dynamodb.PutItemInput{
				TableName: aws.String(storeRequestsTable),
				Item:      item,
			})
			require.NoError(t, err)

			_, err = testStore.Get(ctx, id)
			require.Error(t, err)
			require.NotErrorIs(t, err, paymentrequest.ErrNotFound)
		})
	}
}
