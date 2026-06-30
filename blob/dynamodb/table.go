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
// pk = "blob#<id hex>", with on-demand billing and a sparse renditions_by_parent
// GSI (hash = parent_id) for listing an ORIGINAL's renditions. It is idempotent
// (an existing table is left as-is) and blocks until the table is ACTIVE.
func createBlobsTable(ctx context.Context, client *dynamodb.Client, blobsTable string) error {
	_, err := client.CreateTable(ctx, &dynamodb.CreateTableInput{
		TableName:   aws.String(blobsTable),
		BillingMode: types.BillingModePayPerRequest,
		AttributeDefinitions: []types.AttributeDefinition{
			{AttributeName: aws.String(attrPK), AttributeType: types.ScalarAttributeTypeS},
			{AttributeName: aws.String(attrParentID), AttributeType: types.ScalarAttributeTypeS},
		},
		KeySchema: []types.KeySchemaElement{
			{AttributeName: aws.String(attrPK), KeyType: types.KeyTypeHash},
		},
		GlobalSecondaryIndexes: []types.GlobalSecondaryIndex{{
			IndexName: aws.String(renditionsByParentGSI),
			KeySchema: []types.KeySchemaElement{
				{AttributeName: aws.String(attrParentID), KeyType: types.KeyTypeHash},
			},
			// Renditions are read back in full, so project every attribute.
			Projection: &types.Projection{ProjectionType: types.ProjectionTypeAll},
		}},
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
	return enableTTL(ctx, client, blobsTable)
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
