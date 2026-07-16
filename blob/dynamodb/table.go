package dynamodb

import (
	"context"
	"errors"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// CreateTables provisions the blob domain's DynamoDB tables — the blobs table
// and the ACL table — and is the single entry point for setting them up. It is
// idempotent (existing tables are left as-is) and blocks until both are ACTIVE.
func CreateTables(ctx context.Context, client *dynamodb.Client, blobsTable, aclTable string) error {
	if err := createBlobsTable(ctx, client, blobsTable); err != nil {
		return err
	}
	return createACLTable(ctx, client, aclTable)
}

// createBlobsTable provisions the blobs table: one item per blob keyed by
// pk = "blob#<id hex>", with on-demand billing, plus the sparse finalization
// queue GSI the background worker polls. An original's renditions are recorded
// as a manifest on the original's item and resolved in the read that fetches it,
// so there is no by-parent index. It is idempotent (an existing table is left
// as-is, though a missing queue index is added to it) and blocks until the table
// and index are ACTIVE.
func createBlobsTable(ctx context.Context, client *dynamodb.Client, blobsTable string) error {
	_, err := client.CreateTable(ctx, &dynamodb.CreateTableInput{
		TableName:   aws.String(blobsTable),
		BillingMode: types.BillingModePayPerRequest,
		AttributeDefinitions: []types.AttributeDefinition{
			{AttributeName: aws.String(attrPK), AttributeType: types.ScalarAttributeTypeS},
			{AttributeName: aws.String(attrFinalizeQueue), AttributeType: types.ScalarAttributeTypeS},
			{AttributeName: aws.String(attrFinalizeDueAt), AttributeType: types.ScalarAttributeTypeN},
		},
		KeySchema: []types.KeySchemaElement{
			{AttributeName: aws.String(attrPK), KeyType: types.KeyTypeHash},
		},
		GlobalSecondaryIndexes: []types.GlobalSecondaryIndex{finalizationQueueIndexSchema()},
	})
	if err != nil {
		var inUse *types.ResourceInUseException
		if !errors.As(err, &inUse) {
			return err
		}
		// Already exists; still ensure it is ACTIVE below.
	}
	if err := dynamodb.NewTableExistsWaiter(client).Wait(ctx, &dynamodb.DescribeTableInput{
		TableName: aws.String(blobsTable),
	}, 2*time.Minute); err != nil {
		return err
	}
	if err := ensureFinalizationQueueIndex(ctx, client, blobsTable); err != nil {
		return err
	}
	return enableTTL(ctx, client, blobsTable)
}

// finalizationQueueIndexSchema is the finalization queue GSI: a sparse index
// over the queue attributes (present only while a blob awaits processing),
// keyed on the per-kind queue partition and sorted by due time so the worker's
// poll is a single soonest-first Query. The projection carries the attempt
// count (all the worker needs to schedule off of — it reads the full record by
// id only after claiming the work) and the enqueue time (backing the max-age
// gauge without touching the base table).
func finalizationQueueIndexSchema() types.GlobalSecondaryIndex {
	return types.GlobalSecondaryIndex{
		IndexName: aws.String(finalizationQueueIndex),
		KeySchema: []types.KeySchemaElement{
			{AttributeName: aws.String(attrFinalizeQueue), KeyType: types.KeyTypeHash},
			{AttributeName: aws.String(attrFinalizeDueAt), KeyType: types.KeyTypeRange},
		},
		Projection: &types.Projection{
			ProjectionType:   types.ProjectionTypeInclude,
			NonKeyAttributes: []string{attrFinalizeAttempts, attrFinalizeEnqueuedAt},
		},
	}
}

// ensureFinalizationQueueIndex adds the finalization queue GSI to a blobs table
// that predates it, and blocks until the index is ACTIVE. A table created by
// createBlobsTable already carries it, so this is a no-op there; it exists so a
// deploy against an existing production table converges without manual steps.
func ensureFinalizationQueueIndex(ctx context.Context, client *dynamodb.Client, blobsTable string) error {
	for {
		desc, err := client.DescribeTable(ctx, &dynamodb.DescribeTableInput{
			TableName: aws.String(blobsTable),
		})
		if err != nil {
			return err
		}

		var index *types.GlobalSecondaryIndexDescription
		for i := range desc.Table.GlobalSecondaryIndexes {
			if aws.ToString(desc.Table.GlobalSecondaryIndexes[i].IndexName) == finalizationQueueIndex {
				index = &desc.Table.GlobalSecondaryIndexes[i]
				break
			}
		}

		if index == nil {
			gsi := finalizationQueueIndexSchema()
			if _, err := client.UpdateTable(ctx, &dynamodb.UpdateTableInput{
				TableName: aws.String(blobsTable),
				AttributeDefinitions: []types.AttributeDefinition{
					{AttributeName: aws.String(attrFinalizeQueue), AttributeType: types.ScalarAttributeTypeS},
					{AttributeName: aws.String(attrFinalizeDueAt), AttributeType: types.ScalarAttributeTypeN},
				},
				GlobalSecondaryIndexUpdates: []types.GlobalSecondaryIndexUpdate{{
					Create: &types.CreateGlobalSecondaryIndexAction{
						IndexName:  gsi.IndexName,
						KeySchema:  gsi.KeySchema,
						Projection: gsi.Projection,
					},
				}},
			}); err != nil {
				return err
			}
			// Fall through to poll for the new index becoming ACTIVE.
		} else if index.IndexStatus == types.IndexStatusActive {
			return nil
		}

		// Backfill of an existing table can take a while; poll under the caller's
		// ctx rather than a fixed internal deadline.
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(3 * time.Second):
		}
	}
}

// createACLTable provisions the blob ACL table: one item per access-control
// entry keyed by pk = "blob#<id hex>" and
// sk = "<effect>#<perm>#<ptype>#<principal id hex>", with on-demand billing. An
// entry's existence is the authorization, so there are no other attributes, no
// secondary indexes, and no TTL — entries are durable until explicitly revoked.
// It is idempotent (an existing table is left as-is) and blocks until the table
// is ACTIVE.
func createACLTable(ctx context.Context, client *dynamodb.Client, aclTable string) error {
	_, err := client.CreateTable(ctx, &dynamodb.CreateTableInput{
		TableName:   aws.String(aclTable),
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
		// Already exists; still ensure it is ACTIVE below.
	}
	return dynamodb.NewTableExistsWaiter(client).Wait(ctx, &dynamodb.DescribeTableInput{
		TableName: aws.String(aclTable),
	}, 2*time.Minute)
}

// enableTTL turns on DynamoDB's TTL feature against the expires_at attribute, so
// blobs that never reach READY are reclaimed automatically. Enabling TTL when it
// is already enabled is an error, so it is checked first to stay idempotent.
func enableTTL(ctx context.Context, client *dynamodb.Client, table string) error {
	desc, err := client.DescribeTimeToLive(ctx, &dynamodb.DescribeTimeToLiveInput{
		TableName: aws.String(table),
	})
	if err != nil {
		return err
	}
	if desc.TimeToLiveDescription != nil {
		switch desc.TimeToLiveDescription.TimeToLiveStatus {
		case types.TimeToLiveStatusEnabled, types.TimeToLiveStatusEnabling:
			return nil
		}
	}

	_, err = client.UpdateTimeToLive(ctx, &dynamodb.UpdateTimeToLiveInput{
		TableName: aws.String(table),
		TimeToLiveSpecification: &types.TimeToLiveSpecification{
			Enabled:       aws.Bool(true),
			AttributeName: aws.String(attrExpiresAt),
		},
	})
	return err
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
