package dynamodb

import (
	"context"
	"errors"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// CreateTables provisions the cluster members and claims tables with on-demand
// billing. Members are keyed by instance_id; claims are keyed by a single
// composite pk (namespace + key) so claims spread across partitions instead of
// piling a whole namespace onto one partition key. It is idempotent and blocks
// until the tables are ACTIVE.
//
// Only the members table carries a ttl attribute, garbage-collecting dead
// members' records (safe because every heartbeat refreshes it — a live row
// can never expire). TTL deletion is lazy and is never relied on for
// correctness: liveness is always judged by heartbeat-counter observation,
// and claim validity by the owner's liveness. Claim rows are deliberately
// permanent — see the table overview in store.go. Enable TTL on the members
// table's ttl attribute in real deployments; DynamoDB Local ignores it,
// which the tests don't mind.
func CreateTables(ctx context.Context, client *dynamodb.Client, membersTable, claimsTable string) error {
	inputs := []*dynamodb.CreateTableInput{
		{
			TableName:   aws.String(membersTable),
			BillingMode: types.BillingModePayPerRequest,
			AttributeDefinitions: []types.AttributeDefinition{
				{AttributeName: aws.String(attrInstanceID), AttributeType: types.ScalarAttributeTypeS},
			},
			KeySchema: []types.KeySchemaElement{
				{AttributeName: aws.String(attrInstanceID), KeyType: types.KeyTypeHash},
			},
		},
		{
			TableName:   aws.String(claimsTable),
			BillingMode: types.BillingModePayPerRequest,
			AttributeDefinitions: []types.AttributeDefinition{
				{AttributeName: aws.String(attrPK), AttributeType: types.ScalarAttributeTypeS},
			},
			KeySchema: []types.KeySchemaElement{
				{AttributeName: aws.String(attrPK), KeyType: types.KeyTypeHash},
			},
		},
	}

	for _, input := range inputs {
		if _, err := client.CreateTable(ctx, input); err != nil {
			var inUse *types.ResourceInUseException
			if errors.As(err, &inUse) {
				continue // Already exists.
			}
			return err
		}
		if err := dynamodb.NewTableExistsWaiter(client).Wait(ctx, &dynamodb.DescribeTableInput{
			TableName: input.TableName,
		}, 2*time.Minute); err != nil {
			return err
		}
	}
	return nil
}

// reset deletes every item from both tables, for tests.
func (s *store) reset() {
	ctx := context.Background()
	if err := clearTable(ctx, s.client, s.membersTable, attrInstanceID); err != nil {
		panic(err)
	}
	if err := clearTable(ctx, s.client, s.claimsTable, attrPK); err != nil {
		panic(err)
	}
}

func clearTable(ctx context.Context, client *dynamodb.Client, table, keyAttr string) error {
	var startKey map[string]types.AttributeValue
	for {
		out, err := client.Scan(ctx, &dynamodb.ScanInput{
			TableName:            aws.String(table),
			ProjectionExpression: aws.String(keyAttr),
			ExclusiveStartKey:    startKey,
		})
		if err != nil {
			return err
		}

		const batchSize = 25
		for start := 0; start < len(out.Items); start += batchSize {
			end := min(start+batchSize, len(out.Items))
			requests := make([]types.WriteRequest, 0, end-start)
			for _, item := range out.Items[start:end] {
				requests = append(requests, types.WriteRequest{
					DeleteRequest: &types.DeleteRequest{Key: map[string]types.AttributeValue{
						keyAttr: item[keyAttr],
					}},
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
