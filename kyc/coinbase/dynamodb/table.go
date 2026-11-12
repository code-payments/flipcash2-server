package dynamodb

import (
	"context"
	"errors"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// CreateTables provisions the Coinbase customers table: one item per user
// keyed by pk = "user#<hex user id>", with on-demand billing and the
// by_customer GSI (hash key customer_id, projecting every attribute) that
// GetCustomerByID queries. The index is created with the table because an
// existing table is left as-is here, so an index added later would need a
// change to the live table made out of band. It is idempotent and blocks until
// the table is ACTIVE.
func CreateTables(ctx context.Context, client *dynamodb.Client, customersTable string) error {
	_, err := client.CreateTable(ctx, &dynamodb.CreateTableInput{
		TableName:   aws.String(customersTable),
		BillingMode: types.BillingModePayPerRequest,
		AttributeDefinitions: []types.AttributeDefinition{
			{AttributeName: aws.String(attrPK), AttributeType: types.ScalarAttributeTypeS},
			{AttributeName: aws.String(attrCustomerID), AttributeType: types.ScalarAttributeTypeS},
		},
		KeySchema: []types.KeySchemaElement{
			{AttributeName: aws.String(attrPK), KeyType: types.KeyTypeHash},
		},
		GlobalSecondaryIndexes: []types.GlobalSecondaryIndex{
			{
				IndexName: aws.String(gsiByCustomer),
				KeySchema: []types.KeySchemaElement{
					{AttributeName: aws.String(attrCustomerID), KeyType: types.KeyTypeHash},
				},
				Projection: &types.Projection{ProjectionType: types.ProjectionTypeAll},
			},
		},
	})
	if err != nil {
		var inUse *types.ResourceInUseException
		if !errors.As(err, &inUse) {
			return err
		}
		// Already exists; still ensure it is ACTIVE below.
	}
	return dynamodb.NewTableExistsWaiter(client).Wait(ctx, &dynamodb.DescribeTableInput{
		TableName: aws.String(customersTable),
	}, 2*time.Minute)
}

// reset deletes every item from the table, for tests.
func (s *store) reset() {
	ctx := context.Background()
	if err := clearTable(ctx, s.client, s.table, []string{attrPK}); err != nil {
		panic(err)
	}
}

func clearTable(ctx context.Context, client *dynamodb.Client, table string, keyAttrs []string) error {
	projection := keyAttrs[0]
	for _, a := range keyAttrs[1:] {
		projection += ", " + a
	}

	var startKey map[string]types.AttributeValue
	for {
		out, err := client.Scan(ctx, &dynamodb.ScanInput{
			TableName:            aws.String(table),
			ProjectionExpression: aws.String(projection),
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
				key := make(map[string]types.AttributeValue, len(keyAttrs))
				for _, a := range keyAttrs {
					key[a] = item[a]
				}
				requests = append(requests, types.WriteRequest{
					DeleteRequest: &types.DeleteRequest{Key: key},
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
