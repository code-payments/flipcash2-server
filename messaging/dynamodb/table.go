package dynamodb

import (
	"context"
	"errors"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// CreateTables provisions the messages, message_pointers, and message_reactions
// tables. All use a composite (pk, sk) string key with on-demand billing. The
// reactions table carries the reactors_by_recency GSI for most-recent-first
// reactor paging; the messages table needs no secondary index — event-ordered
// delta reads page the evt# rows as a strongly-consistent sort-key range in the
// chat's own partition. The messages table has TTL enabled on attrExpiresAt so the
// transient cmid# idempotency markers are auto-reaped. It is idempotent: tables
// that already exist are left as-is. The call blocks until all tables are ACTIVE.
func CreateTables(ctx context.Context, client *dynamodb.Client, messagesTable, pointersTable, reactionsTable string) error {
	// The messages table is a plain (pk, sk) key-value table: every access — by
	// message ID (msg#), by event sequence (evt#), the counter, and the idempotency
	// markers — is served from the chat's partition by sort key, so no GSI is needed.
	_, err := client.CreateTable(ctx, &dynamodb.CreateTableInput{
		TableName:   aws.String(messagesTable),
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
		if !errors.As(err, &inUse) {
			return err
		}
	}
	if err := dynamodb.NewTableExistsWaiter(client).Wait(ctx, &dynamodb.DescribeTableInput{
		TableName: aws.String(messagesTable),
	}, 2*time.Minute); err != nil {
		return err
	}

	// The pointers table is a plain (pk, sk) key-value table.
	_, err = client.CreateTable(ctx, &dynamodb.CreateTableInput{
		TableName:   aws.String(pointersTable),
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
		if !errors.As(err, &inUse) {
			return err
		}
	}
	if err := dynamodb.NewTableExistsWaiter(client).Wait(ctx, &dynamodb.DescribeTableInput{
		TableName: aws.String(pointersTable),
	}, 2*time.Minute); err != nil {
		return err
	}

	// The reactions table also indexes reactor rows by (reaction_key, reacted_ts)
	// so a single emoji's reactors can be paged most-recent-first.
	_, err = client.CreateTable(ctx, &dynamodb.CreateTableInput{
		TableName:   aws.String(reactionsTable),
		BillingMode: types.BillingModePayPerRequest,
		AttributeDefinitions: []types.AttributeDefinition{
			{AttributeName: aws.String(attrPK), AttributeType: types.ScalarAttributeTypeS},
			{AttributeName: aws.String(attrSK), AttributeType: types.ScalarAttributeTypeS},
			{AttributeName: aws.String(attrReactionKey), AttributeType: types.ScalarAttributeTypeS},
			{AttributeName: aws.String(attrReactedTs), AttributeType: types.ScalarAttributeTypeN},
		},
		KeySchema: []types.KeySchemaElement{
			{AttributeName: aws.String(attrPK), KeyType: types.KeyTypeHash},
			{AttributeName: aws.String(attrSK), KeyType: types.KeyTypeRange},
		},
		GlobalSecondaryIndexes: []types.GlobalSecondaryIndex{{
			IndexName: aws.String(reactorsByRecencyGSI),
			KeySchema: []types.KeySchemaElement{
				{AttributeName: aws.String(attrReactionKey), KeyType: types.KeyTypeHash},
				{AttributeName: aws.String(attrReactedTs), KeyType: types.KeyTypeRange},
			},
			// user_id is the only non-key attribute a reactor read needs.
			Projection: &types.Projection{
				ProjectionType:   types.ProjectionTypeInclude,
				NonKeyAttributes: []string{attrUserID},
			},
		}},
	})
	if err != nil {
		var inUse *types.ResourceInUseException
		if !errors.As(err, &inUse) {
			return err
		}
	}
	if err := dynamodb.NewTableExistsWaiter(client).Wait(ctx, &dynamodb.DescribeTableInput{
		TableName: aws.String(reactionsTable),
	}, 2*time.Minute); err != nil {
		return err
	}

	return ensureTTL(ctx, client, messagesTable, attrExpiresAt)
}

// ensureTTL idempotently enables DynamoDB TTL on table's attr. Enabling TTL when
// it is already enabled (or enabling) is a no-op, so re-running CreateTables is
// safe.
func ensureTTL(ctx context.Context, client *dynamodb.Client, table, attr string) error {
	desc, err := client.DescribeTimeToLive(ctx, &dynamodb.DescribeTimeToLiveInput{
		TableName: aws.String(table),
	})
	if err != nil {
		return err
	}
	if d := desc.TimeToLiveDescription; d != nil {
		switch d.TimeToLiveStatus {
		case types.TimeToLiveStatusEnabled, types.TimeToLiveStatusEnabling:
			return nil
		}
	}

	_, err = client.UpdateTimeToLive(ctx, &dynamodb.UpdateTimeToLiveInput{
		TableName: aws.String(table),
		TimeToLiveSpecification: &types.TimeToLiveSpecification{
			Enabled:       aws.Bool(true),
			AttributeName: aws.String(attr),
		},
	})
	return err
}

// reset deletes every item from all tables, for tests.
func (s *store) reset() {
	ctx := context.Background()
	for _, table := range []string{s.messagesTable, s.pointersTable, s.reactionsTable} {
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
