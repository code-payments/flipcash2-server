package dynamodb

import (
	"context"
	"errors"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// CreateTables provisions the chats, dm_inbox, group_members and
// chat_user_state tables with on-demand billing. The chats table is keyed by
// pk only; dm_inbox is keyed by (pk, sk) with a GSI ordering each user's DMs
// by last_activity; group_members is keyed by (pk, sk) = (chat, user) — plus
// one "#meta" aggregates item per group — with an inverted GSI for listing a
// user's group chats, a sparse GSI of a group's joined members by join time
// (paging a large roster newest-first; see gsiByJoinedAt), and TTL on
// expires_at sweeping departed members' tombstones (see tombstoneTTL);
// chat_user_state is keyed by (pk, sk) = (user, chat) — plus one "#meta"
// aggregates item per chat — with a sparse GSI of a chat's recorded mutes by
// when they end (see gsiByMuted) and an inverted GSI of a chat's records by
// user (see gsiUserStateByUser). It is idempotent and blocks until all tables
// are ACTIVE.
func CreateTables(ctx context.Context, client *dynamodb.Client, chatsTable, dmInboxTable, groupMembersTable, userStateTable string) error {
	inputs := []*dynamodb.CreateTableInput{
		{
			TableName:   aws.String(chatsTable),
			BillingMode: types.BillingModePayPerRequest,
			AttributeDefinitions: []types.AttributeDefinition{
				{AttributeName: aws.String(attrPK), AttributeType: types.ScalarAttributeTypeS},
			},
			KeySchema: []types.KeySchemaElement{
				{AttributeName: aws.String(attrPK), KeyType: types.KeyTypeHash},
			},
		},
		{
			TableName:   aws.String(groupMembersTable),
			BillingMode: types.BillingModePayPerRequest,
			AttributeDefinitions: []types.AttributeDefinition{
				{AttributeName: aws.String(attrPK), AttributeType: types.ScalarAttributeTypeS},
				{AttributeName: aws.String(attrSK), AttributeType: types.ScalarAttributeTypeS},
				{AttributeName: aws.String(attrUser), AttributeType: types.ScalarAttributeTypeS},
				{AttributeName: aws.String(attrJoinedAt), AttributeType: types.ScalarAttributeTypeN},
			},
			KeySchema: []types.KeySchemaElement{
				{AttributeName: aws.String(attrPK), KeyType: types.KeyTypeHash},
				{AttributeName: aws.String(attrSK), KeyType: types.KeyTypeRange},
			},
			GlobalSecondaryIndexes: []types.GlobalSecondaryIndex{
				{
					// Inverted index: (user, chat). Listing a user's group chats
					// (the group feed's fan-out-on-read entry point) is a query on
					// the user's slice; membership state rides along in the
					// projection. Keyed by the sparse user attribute — present on
					// membership rows (tombstones included) and nothing else — so
					// non-membership items like the counter stay out of the index
					// (see gsiByUser).
					IndexName: aws.String(gsiByUser),
					KeySchema: []types.KeySchemaElement{
						{AttributeName: aws.String(attrUser), KeyType: types.KeyTypeHash},
						{AttributeName: aws.String(attrPK), KeyType: types.KeyTypeRange},
					},
					Projection: &types.Projection{ProjectionType: types.ProjectionTypeAll},
				},
				{
					// Sparse index of joined members ordered by join time:
					// joined_at exists only while joined, so tombstones (and the
					// counter item) never appear. It pages a large roster
					// newest-first; the whole-roster reads walk the base
					// partition (see gsiByJoinedAt).
					IndexName: aws.String(gsiByJoinedAt),
					KeySchema: []types.KeySchemaElement{
						{AttributeName: aws.String(attrPK), KeyType: types.KeyTypeHash},
						{AttributeName: aws.String(attrJoinedAt), KeyType: types.KeyTypeRange},
					},
					Projection: &types.Projection{ProjectionType: types.ProjectionTypeAll},
				},
			},
		},
		{
			TableName:   aws.String(dmInboxTable),
			BillingMode: types.BillingModePayPerRequest,
			AttributeDefinitions: []types.AttributeDefinition{
				{AttributeName: aws.String(attrPK), AttributeType: types.ScalarAttributeTypeS},
				{AttributeName: aws.String(attrSK), AttributeType: types.ScalarAttributeTypeS},
				{AttributeName: aws.String(attrFeed), AttributeType: types.ScalarAttributeTypeS},
				{AttributeName: aws.String(attrLastActivity), AttributeType: types.ScalarAttributeTypeN},
			},
			KeySchema: []types.KeySchemaElement{
				{AttributeName: aws.String(attrPK), KeyType: types.KeyTypeHash},
				{AttributeName: aws.String(attrSK), KeyType: types.KeyTypeRange},
			},
			GlobalSecondaryIndexes: []types.GlobalSecondaryIndex{
				{
					IndexName: aws.String(gsiByActivity),
					KeySchema: []types.KeySchemaElement{
						{AttributeName: aws.String(attrPK), KeyType: types.KeyTypeHash},
						{AttributeName: aws.String(attrLastActivity), KeyType: types.KeyTypeRange},
					},
					Projection: &types.Projection{ProjectionType: types.ProjectionTypeAll},
				},
				{
					IndexName: aws.String(gsiByTypeActivity),
					KeySchema: []types.KeySchemaElement{
						{AttributeName: aws.String(attrFeed), KeyType: types.KeyTypeHash},
						{AttributeName: aws.String(attrLastActivity), KeyType: types.KeyTypeRange},
					},
					Projection: &types.Projection{ProjectionType: types.ProjectionTypeAll},
				},
			},
		},
		{
			TableName:   aws.String(userStateTable),
			BillingMode: types.BillingModePayPerRequest,
			AttributeDefinitions: []types.AttributeDefinition{
				{AttributeName: aws.String(attrPK), AttributeType: types.ScalarAttributeTypeS},
				{AttributeName: aws.String(attrSK), AttributeType: types.ScalarAttributeTypeS},
				{AttributeName: aws.String(attrChat), AttributeType: types.ScalarAttributeTypeB},
				{AttributeName: aws.String(attrMutedUntil), AttributeType: types.ScalarAttributeTypeN},
			},
			KeySchema: []types.KeySchemaElement{
				{AttributeName: aws.String(attrPK), KeyType: types.KeyTypeHash},
				{AttributeName: aws.String(attrSK), KeyType: types.KeyTypeRange},
			},
			GlobalSecondaryIndexes: []types.GlobalSecondaryIndex{
				{
					// Sparse index of a chat's recorded mutes ordered by when they
					// end: muted_until exists only while a mute is recorded, so a
					// range on it above "now" is exactly the active mutes, and an
					// item whose mute was cleared leaves the index (see
					// gsiByMuted). The user is in the projected pk, so the index
					// carries nothing beyond its keys.
					IndexName: aws.String(gsiByMuted),
					KeySchema: []types.KeySchemaElement{
						{AttributeName: aws.String(attrChat), KeyType: types.KeyTypeHash},
						{AttributeName: aws.String(attrMutedUntil), KeyType: types.KeyTypeRange},
					},
					Projection: &types.Projection{ProjectionType: types.ProjectionTypeKeysOnly},
				},
				{
					// Inverted index: (chat, user), in the same user order as a
					// group's membership partition, so a chat-scoped walk of what
					// its users have set pages in lockstep with a roster walk.
					// Every record is in it, whatever state it holds, and the full
					// record is projected, so any state added later is readable
					// per chat without a new index (see gsiUserStateByUser). Both
					// indexes are keyed by the chat attribute, which only records
					// carry: the #meta item must omit it, or it leaks in.
					IndexName: aws.String(gsiUserStateByUser),
					KeySchema: []types.KeySchemaElement{
						{AttributeName: aws.String(attrChat), KeyType: types.KeyTypeHash},
						{AttributeName: aws.String(attrPK), KeyType: types.KeyTypeRange},
					},
					Projection: &types.Projection{ProjectionType: types.ProjectionTypeAll},
				},
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

	return ensureTTL(ctx, client, groupMembersTable, attrExpiresAt)
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
	if err := clearTable(ctx, s.client, s.chatsTable, []string{attrPK}); err != nil {
		panic(err)
	}
	if err := clearTable(ctx, s.client, s.dmInboxTable, []string{attrPK, attrSK}); err != nil {
		panic(err)
	}
	if err := clearTable(ctx, s.client, s.groupMembersTable, []string{attrPK, attrSK}); err != nil {
		panic(err)
	}
	if err := clearTable(ctx, s.client, s.userStateTable, []string{attrPK, attrSK}); err != nil {
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
