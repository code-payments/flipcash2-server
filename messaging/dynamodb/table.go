package dynamodb

import (
	"context"
	"errors"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// CreateTables provisions the messages, message_pointers, message_reactions,
// message_reactors and message_self_reactions tables. All use a composite (pk,
// sk) string key with on-demand billing. The reactors table carries the
// by_version LSI for strongly-consistent, most-recent-first reactor paging; the
// other four need no secondary index — event-ordered delta reads page the evt#
// rows, summary reads page the agg# rows, and a viewer's self overlay pages
// their own partition, each as a sort-key range. The messages table has TTL
// enabled on attrExpiresAt so the transient cmid# idempotency markers are
// auto-reaped. It is idempotent: tables that already exist are left as-is. The
// call blocks until all tables are ACTIVE.
func CreateTables(ctx context.Context, client *dynamodb.Client, messagesTable, pointersTable, reactionsTable, reactorsTable, selfReactionsTable string) error {
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

	// The pointers table is a plain (pk, sk) key-value table: one item per
	// (chat, member) carrying every stored pointer type (see the layout in
	// store.go).
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

	// The reactions table is a plain (pk, sk) key-value table: aggregates and cap
	// rows are read by exact key or as an agg# range within the chat's partition.
	_, err = client.CreateTable(ctx, &dynamodb.CreateTableInput{
		TableName:   aws.String(reactionsTable),
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
		TableName: aws.String(reactionsTable),
	}, 2*time.Minute); err != nil {
		return err
	}

	// The reactors table is keyed by message, and a local secondary index
	// re-sorts each message's reactor rows by emoji_version (emoji, then the version that
	// added the reactor) so one emoji's reactors page most-recent-first under a
	// strongly consistent read — a GSI could not offer one. An LSI must be
	// declared with the table, and caps each partition key's item collection at
	// 10 GB, which is why the key is the message rather than the chat.
	_, err = client.CreateTable(ctx, &dynamodb.CreateTableInput{
		TableName:   aws.String(reactorsTable),
		BillingMode: types.BillingModePayPerRequest,
		AttributeDefinitions: []types.AttributeDefinition{
			{AttributeName: aws.String(attrPK), AttributeType: types.ScalarAttributeTypeS},
			{AttributeName: aws.String(attrSK), AttributeType: types.ScalarAttributeTypeS},
			{AttributeName: aws.String(attrEmojiVersion), AttributeType: types.ScalarAttributeTypeS},
		},
		KeySchema: []types.KeySchemaElement{
			{AttributeName: aws.String(attrPK), KeyType: types.KeyTypeHash},
			{AttributeName: aws.String(attrSK), KeyType: types.KeyTypeRange},
		},
		LocalSecondaryIndexes: []types.LocalSecondaryIndex{{
			IndexName: aws.String(lsiByVersion),
			KeySchema: []types.KeySchemaElement{
				{AttributeName: aws.String(attrPK), KeyType: types.KeyTypeHash},
				{AttributeName: aws.String(attrEmojiVersion), KeyType: types.KeyTypeRange},
			},
			// The keys carry the user (sk) and version (emoji_version); the display timestamp
			// is the only other attribute a reactor read needs. Projecting it keeps
			// the index self-contained — an LSI serves unprojected attributes by
			// fetching each base row, which would multiply the page's cost.
			Projection: &types.Projection{
				ProjectionType:   types.ProjectionTypeInclude,
				NonKeyAttributes: []string{attrReactedTs},
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
		TableName: aws.String(reactorsTable),
	}, 2*time.Minute); err != nil {
		return err
	}

	// The self-reactions table is the same rows keyed by viewer (chat, user) and
	// sorted by message: a viewer's reactions across a page of messages — the
	// self_reactor overlay on a summary read — are one strongly consistent
	// range query on their own partition. It is a third table rather than a
	// GSI on the reactors table (which is eventually consistent and not part
	// of the write's transaction) or a prefix in the reactions table (whose
	// chat partition already absorbs every aggregate write in the chat, and
	// would take one more write per reaction on top). Keyed by viewer, a burst
	// on one message spreads over as many partitions as it has reactors.
	_, err = client.CreateTable(ctx, &dynamodb.CreateTableInput{
		TableName:   aws.String(selfReactionsTable),
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
		TableName: aws.String(selfReactionsTable),
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
	for _, table := range []string{s.messagesTable, s.pointersTable, s.reactionsTable, s.reactorsTable, s.selfReactionsTable} {
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
