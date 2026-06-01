package dynamodb

import (
	"context"
	"errors"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// CreateTables provisions the messages and message_pointers tables. Both use a
// composite (pk, sk) string key with no secondary indexes and on-demand
// billing. It is idempotent: tables that already exist are left as-is. The call
// blocks until both tables are ACTIVE.
func CreateTables(ctx context.Context, client *dynamodb.Client, messagesTable, pointersTable string) error {
	for _, table := range []string{messagesTable, pointersTable} {
		_, err := client.CreateTable(ctx, &dynamodb.CreateTableInput{
			TableName:   aws.String(table),
			BillingMode: types.BillingModePayPerRequest,
			AttributeDefinitions: []types.AttributeDefinition{
				{AttributeName: aws.String(attrPK), AttributeType: types.ScalarAttributeTypeS},
				{AttributeName: aws.String(attrSK), AttributeType: types.ScalarAttributeTypeS},
			},
			KeySchema: []types.KeySchemaElement{
				{AttributeName: aws.String(attrPK), KeyType: types.KeyTypeHash},
				{AttributeName: aws.String(attrSK), KeyType: types.KeyTypeRange},
			},
		})
		if err != nil {
			var inUse *types.ResourceInUseException
			if errors.As(err, &inUse) {
				continue // Already exists.
			}
			return err
		}
		if err := dynamodb.NewTableExistsWaiter(client).Wait(ctx, &dynamodb.DescribeTableInput{
			TableName: aws.String(table),
		}, 2*time.Minute); err != nil {
			return err
		}
	}
	return nil
}

// reset deletes every item from both tables, for tests.
func (s *store) reset() {
	ctx := context.Background()
	for _, table := range []string{s.messagesTable, s.pointersTable} {
		if err := clearTable(ctx, s.client, table); err != nil {
			panic(err)
		}
	}
}

func clearTable(ctx context.Context, client *dynamodb.Client, table string) error {
	var startKey map[string]types.AttributeValue
	for {
		out, err := client.Scan(ctx, &dynamodb.ScanInput{
			TableName:            aws.String(table),
			ProjectionExpression: aws.String(attrPK + ", " + attrSK),
			ExclusiveStartKey:    startKey,
		})
		if err != nil {
			return err
		}

		// BatchWriteItem deletes up to 25 items per call.
		const batchSize = 25
		for start := 0; start < len(out.Items); start += batchSize {
			end := start + batchSize
			if end > len(out.Items) {
				end = len(out.Items)
			}
			requests := make([]types.WriteRequest, 0, end-start)
			for _, item := range out.Items[start:end] {
				requests = append(requests, types.WriteRequest{
					DeleteRequest: &types.DeleteRequest{Key: item},
				})
			}
			if _, err := client.BatchWriteItem(ctx, &dynamodb.BatchWriteItemInput{
				RequestItems: map[string][]types.WriteRequest{table: requests},
			}); err != nil {
				return err
			}
		}

		if len(out.LastEvaluatedKey) == 0 {
			break
		}
		startKey = out.LastEvaluatedKey
	}
	return nil
}
