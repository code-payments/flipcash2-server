package dynamodb

import (
	"context"
	"errors"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// CreateTables provisions the payment_requests table with on-demand billing
// and its three sparse GSIs (see store for the layout). It is idempotent and
// blocks until the table is ACTIVE.
func CreateTables(ctx context.Context, client *dynamodb.Client, requestsTable string) error {
	input := &dynamodb.CreateTableInput{
		TableName:   aws.String(requestsTable),
		BillingMode: types.BillingModePayPerRequest,
		AttributeDefinitions: []types.AttributeDefinition{
			{AttributeName: aws.String(attrPK), AttributeType: types.ScalarAttributeTypeS},
			{AttributeName: aws.String(attrSK), AttributeType: types.ScalarAttributeTypeS},
			{AttributeName: aws.String(attrChat), AttributeType: types.ScalarAttributeTypeB},
			{AttributeName: aws.String(attrOpenSort), AttributeType: types.ScalarAttributeTypeB},
			{AttributeName: aws.String(attrResolvedSort), AttributeType: types.ScalarAttributeTypeB},
			{AttributeName: aws.String(attrExpiryQueue), AttributeType: types.ScalarAttributeTypeS},
			{AttributeName: aws.String(attrExpiryDueAt), AttributeType: types.ScalarAttributeTypeN},
		},
		KeySchema: []types.KeySchemaElement{
			{AttributeName: aws.String(attrPK), KeyType: types.KeyTypeHash},
			{AttributeName: aws.String(attrSK), KeyType: types.KeyTypeRange},
		},
		GlobalSecondaryIndexes: []types.GlobalSecondaryIndex{
			chatSectionIndexSchema(openIndex, attrOpenSort),
			chatSectionIndexSchema(resolvedIndex, attrResolvedSort),
			expiryQueueIndexSchema(),
		},
	}

	if _, err := client.CreateTable(ctx, input); err != nil {
		var inUse *types.ResourceInUseException
		if errors.As(err, &inUse) {
			return nil // Already exists.
		}
		return err
	}
	return dynamodb.NewTableExistsWaiter(client).Wait(ctx, &dynamodb.DescribeTableInput{
		TableName: input.TableName,
	}, 2*time.Minute)
}

// chatSectionIndexSchema is one of a chat's two section GSIs: a sparse index
// hashed on the chat and ranged on the section's sort key (open_sort or
// resolved_sort, each present exactly while the request is in that section),
// so a page of a section is one key-range Query. It projects the whole item,
// since a page returns whole requests and a projection of less would cost a
// read per request on the page.
func chatSectionIndexSchema(name, sortAttr string) types.GlobalSecondaryIndex {
	return types.GlobalSecondaryIndex{
		IndexName: aws.String(name),
		KeySchema: []types.KeySchemaElement{
			{AttributeName: aws.String(attrChat), KeyType: types.KeyTypeHash},
			{AttributeName: aws.String(sortAttr), KeyType: types.KeyTypeRange},
		},
		Projection: &types.Projection{ProjectionType: types.ProjectionTypeAll},
	}
}

// expiryQueueIndexSchema is the expiry queue GSI, as poll's tally queue is: a
// sparse index over the queue attributes (present only while a request is
// stored open), keyed on the queue's one partition (see expiryQueuePK) and
// sorted by due time so the worker's poll is a single soonest-first Query. The
// projection carries the attempt count, all the worker needs to schedule off
// of; it reads the request by ID only after claiming it.
func expiryQueueIndexSchema() types.GlobalSecondaryIndex {
	return types.GlobalSecondaryIndex{
		IndexName: aws.String(expiryQueueIndex),
		KeySchema: []types.KeySchemaElement{
			{AttributeName: aws.String(attrExpiryQueue), KeyType: types.KeyTypeHash},
			{AttributeName: aws.String(attrExpiryDueAt), KeyType: types.KeyTypeRange},
		},
		Projection: &types.Projection{
			ProjectionType:   types.ProjectionTypeInclude,
			NonKeyAttributes: []string{attrExpiryAttempts},
		},
	}
}

// reset deletes every item from the table, for tests.
func (s *store) reset() {
	ctx := context.Background()

	var startKey map[string]types.AttributeValue
	for {
		out, err := s.client.Scan(ctx, &dynamodb.ScanInput{
			TableName:                aws.String(s.table),
			ProjectionExpression:     aws.String("#pk, #sk"),
			ExpressionAttributeNames: map[string]string{"#pk": attrPK, "#sk": attrSK},
			ExclusiveStartKey:        startKey,
		})
		if err != nil {
			panic(err)
		}

		const batchSize = 25
		for start := 0; start < len(out.Items); start += batchSize {
			end := min(start+batchSize, len(out.Items))
			requests := make([]types.WriteRequest, 0, end-start)
			for _, item := range out.Items[start:end] {
				requests = append(requests, types.WriteRequest{
					DeleteRequest: &types.DeleteRequest{Key: map[string]types.AttributeValue{
						attrPK: item[attrPK],
						attrSK: item[attrSK],
					}},
				})
			}
			if _, err := s.client.BatchWriteItem(ctx, &dynamodb.BatchWriteItemInput{
				RequestItems: map[string][]types.WriteRequest{s.table: requests},
			}); err != nil {
				panic(err)
			}
		}

		if len(out.LastEvaluatedKey) == 0 {
			return
		}
		startKey = out.LastEvaluatedKey
	}
}
