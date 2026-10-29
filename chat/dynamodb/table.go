package dynamodb

import (
	"context"
	"errors"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// CreateTables provisions the chats, dm_inbox, group_members,
// chat_user_state, chat_activity, chat_key_envelopes, chat_lobbies,
// chat_featured_groups, chat_holdings, chat_invites and chat_pins tables with
// on-demand billing. The chats table is keyed by pk only; dm_inbox is keyed by (pk, sk) with a GSI ordering each user's DMs
// by last_activity; group_members is keyed by (pk, sk) = (chat, user) — plus
// one "#meta" aggregates item per group — with an inverted GSI for listing a
// user's group chats, a sparse GSI of a group's joined members by join time
// (paging a large roster newest-first; see gsiByJoinedAt), and TTL on
// expires_at sweeping departed members' tombstones (see tombstoneTTL);
// chat_user_state is keyed by (pk, sk) = (user, chat) — plus one "#meta"
// aggregates item per chat — with a sparse GSI of a chat's recorded mutes by
// when they end (see gsiByMuted), an inverted GSI of a chat's records by
// user (see gsiUserStateByUser) and a sparse GSI of a chat's restricted
// users by when they were restricted (see gsiRestricted); chat_activity is keyed by (pk, sk) = (chat,
// user) with two LSIs, one by last_sent_at (lsiByLastSentAt) and one by
// activity_score (lsiByActivityScore), and TTL on
// expires_at; chat_key_envelopes is keyed by (pk, sk) = (user, chat) with no
// index; chat_lobbies is keyed by (pk, sk) = (user, chat) — plus one "#meta"
// aggregates item per chat and per user — with a sparse GSI of a chat's
// lobby by entry time (see gsiLobbyByChat); chat_featured_groups is keyed by
// (pk, sk) = (user, position) — plus one "#meta" item per user — with no
// index; chat_holdings is keyed by pk = chat with a GSI ranking a mint's
// groups by holdings (see gsiByMintRank) and a GSI of the refresh queue by
// due time (see refreshQueueIndex); chat_invites is keyed by (pk, sk) =
// (invite, item) with three sparse GSIs on the invite's record, a chat's
// invites by creation (gsiInvitesByChat), one creator's invites to a chat by
// creation (gsiInvitesByChatCreator) and a chat's invites that are neither
// revoked nor used up by expiry (gsiInvitesActive), and TTL on expires_at,
// which only an invite's copy of the chat key carries; chat_pins is keyed by
// (pk, sk) = (chat, message) — plus one "#meta" summary item per chat — with
// an LSI of a chat's pins in pin order (see lsiByPinned), which, being an LSI,
// exists only if the table was created with it. It is idempotent and
// blocks until all tables are ACTIVE. It creates missing tables only: a table that
// exists is left as it is, and so is never given an index added to its
// definition here since it was created — gsiRestricted on a live
// chat_user_state is added out of band (see the store's doc).
func CreateTables(ctx context.Context, client *dynamodb.Client, chatsTable, dmInboxTable, groupMembersTable, userStateTable, activityTable, keyEnvelopesTable, lobbiesTable, featuredGroupsTable, holdingsTable, invitesTable, pinsTable string) error {
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
				{AttributeName: aws.String(attrRestrictedSort), AttributeType: types.ScalarAttributeTypeB},
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
				{
					// Sparse index of a chat's restricted users in listing
					// order: restricted_sort exists only while a silence or a
					// ban is recorded, so a lifted restriction leaves the index
					// (see gsiRestricted). Full item projected, since a listing
					// carries each user's whole state. Added after the table
					// went live, so an existing table gets it out of band, not
					// from here (see the store's doc).
					IndexName: aws.String(gsiRestricted),
					KeySchema: []types.KeySchemaElement{
						{AttributeName: aws.String(attrChat), KeyType: types.KeyTypeHash},
						{AttributeName: aws.String(attrRestrictedSort), KeyType: types.KeyTypeRange},
					},
					Projection: &types.Projection{ProjectionType: types.ProjectionTypeAll},
				},
			},
		},
		{
			TableName:   aws.String(activityTable),
			BillingMode: types.BillingModePayPerRequest,
			AttributeDefinitions: []types.AttributeDefinition{
				{AttributeName: aws.String(attrPK), AttributeType: types.ScalarAttributeTypeS},
				{AttributeName: aws.String(attrSK), AttributeType: types.ScalarAttributeTypeS},
				{AttributeName: aws.String(attrLastSentAt), AttributeType: types.ScalarAttributeTypeN},
				{AttributeName: aws.String(attrActivityScore), AttributeType: types.ScalarAttributeTypeN},
			},
			KeySchema: []types.KeySchemaElement{
				{AttributeName: aws.String(attrPK), KeyType: types.KeyTypeHash},
				{AttributeName: aws.String(attrSK), KeyType: types.KeyTypeRange},
			},
			// LSIs, not GSIs: a group's suggestions are read right after a
			// send, and only a local index serves a strongly consistent
			// read. Both must exist from the table's creation, since an LSI
			// cannot be added later. Each projects the other's sort key, so
			// rows read from either can be ordered by both.
			LocalSecondaryIndexes: []types.LocalSecondaryIndex{
				{
					IndexName: aws.String(lsiByLastSentAt),
					KeySchema: []types.KeySchemaElement{
						{AttributeName: aws.String(attrPK), KeyType: types.KeyTypeHash},
						{AttributeName: aws.String(attrLastSentAt), KeyType: types.KeyTypeRange},
					},
					Projection: &types.Projection{
						ProjectionType:   types.ProjectionTypeInclude,
						NonKeyAttributes: []string{attrActivityScore},
					},
				},
				{
					IndexName: aws.String(lsiByActivityScore),
					KeySchema: []types.KeySchemaElement{
						{AttributeName: aws.String(attrPK), KeyType: types.KeyTypeHash},
						{AttributeName: aws.String(attrActivityScore), KeyType: types.KeyTypeRange},
					},
					Projection: &types.Projection{
						ProjectionType:   types.ProjectionTypeInclude,
						NonKeyAttributes: []string{attrLastSentAt},
					},
				},
			},
		},
		{
			TableName:   aws.String(keyEnvelopesTable),
			BillingMode: types.BillingModePayPerRequest,
			AttributeDefinitions: []types.AttributeDefinition{
				{AttributeName: aws.String(attrPK), AttributeType: types.ScalarAttributeTypeS},
				{AttributeName: aws.String(attrSK), AttributeType: types.ScalarAttributeTypeS},
			},
			KeySchema: []types.KeySchemaElement{
				{AttributeName: aws.String(attrPK), KeyType: types.KeyTypeHash},
				{AttributeName: aws.String(attrSK), KeyType: types.KeyTypeRange},
			},
		},
		{
			TableName:   aws.String(lobbiesTable),
			BillingMode: types.BillingModePayPerRequest,
			AttributeDefinitions: []types.AttributeDefinition{
				{AttributeName: aws.String(attrPK), AttributeType: types.ScalarAttributeTypeS},
				{AttributeName: aws.String(attrSK), AttributeType: types.ScalarAttributeTypeS},
				{AttributeName: aws.String(attrEnteredAt), AttributeType: types.ScalarAttributeTypeN},
			},
			KeySchema: []types.KeySchemaElement{
				{AttributeName: aws.String(attrPK), KeyType: types.KeyTypeHash},
				{AttributeName: aws.String(attrSK), KeyType: types.KeyTypeRange},
			},
			GlobalSecondaryIndexes: []types.GlobalSecondaryIndex{
				{
					// Inverted index of a chat's lobby in entry order, hashed on
					// the sk (the chat) with no attribute repeating it. Sparse
					// all the same: an item is indexed only with both keys, and
					// entered_at exists on entries alone, so the two #meta items
					// never appear. It pages a lobby earliest first (see
					// gsiLobbyByChat); the user is recovered from the projected
					// pk. A GSI, not an LSI, because the table is keyed by user
					// (see the store's doc): a local index could only reorder one
					// user's entries, never gather one chat's.
					IndexName: aws.String(gsiLobbyByChat),
					KeySchema: []types.KeySchemaElement{
						{AttributeName: aws.String(attrSK), KeyType: types.KeyTypeHash},
						{AttributeName: aws.String(attrEnteredAt), KeyType: types.KeyTypeRange},
					},
					Projection: &types.Projection{ProjectionType: types.ProjectionTypeAll},
				},
			},
		},
		{
			// No index: a user's list is one query of their partition, in
			// position order (see featuredSK). A (chat, pk) GSI for who
			// features a group can be added later, since chat is already
			// on every position and absent from #meta.
			TableName:   aws.String(featuredGroupsTable),
			BillingMode: types.BillingModePayPerRequest,
			AttributeDefinitions: []types.AttributeDefinition{
				{AttributeName: aws.String(attrPK), AttributeType: types.ScalarAttributeTypeS},
				{AttributeName: aws.String(attrSK), AttributeType: types.ScalarAttributeTypeS},
			},
			KeySchema: []types.KeySchemaElement{
				{AttributeName: aws.String(attrPK), KeyType: types.KeyTypeHash},
				{AttributeName: aws.String(attrSK), KeyType: types.KeyTypeRange},
			},
		},
		{
			TableName:   aws.String(holdingsTable),
			BillingMode: types.BillingModePayPerRequest,
			AttributeDefinitions: []types.AttributeDefinition{
				{AttributeName: aws.String(attrPK), AttributeType: types.ScalarAttributeTypeS},
				{AttributeName: aws.String(attrMint), AttributeType: types.ScalarAttributeTypeB},
				{AttributeName: aws.String(attrRank), AttributeType: types.ScalarAttributeTypeS},
				{AttributeName: aws.String(attrRefreshQueue), AttributeType: types.ScalarAttributeTypeS},
				{AttributeName: aws.String(attrRefreshDueAt), AttributeType: types.ScalarAttributeTypeN},
			},
			KeySchema: []types.KeySchemaElement{
				{AttributeName: aws.String(attrPK), KeyType: types.KeyTypeHash},
			},
			GlobalSecondaryIndexes: []types.GlobalSecondaryIndex{
				{
					IndexName: aws.String(gsiByMintRank),
					KeySchema: []types.KeySchemaElement{
						{AttributeName: aws.String(attrMint), KeyType: types.KeyTypeHash},
						{AttributeName: aws.String(attrRank), KeyType: types.KeyTypeRange},
					},
					Projection: &types.Projection{ProjectionType: types.ProjectionTypeAll},
				},
				{
					IndexName: aws.String(refreshQueueIndex),
					KeySchema: []types.KeySchemaElement{
						{AttributeName: aws.String(attrRefreshQueue), KeyType: types.KeyTypeHash},
						{AttributeName: aws.String(attrRefreshDueAt), KeyType: types.KeyTypeRange},
					},
					Projection: &types.Projection{
						ProjectionType:   types.ProjectionTypeInclude,
						NonKeyAttributes: []string{attrMint, attrRefreshAttempts},
					},
				},
			},
		},
		{
			TableName:   aws.String(invitesTable),
			BillingMode: types.BillingModePayPerRequest,
			AttributeDefinitions: []types.AttributeDefinition{
				{AttributeName: aws.String(attrPK), AttributeType: types.ScalarAttributeTypeS},
				{AttributeName: aws.String(attrSK), AttributeType: types.ScalarAttributeTypeS},
				{AttributeName: aws.String(attrChat), AttributeType: types.ScalarAttributeTypeB},
				{AttributeName: aws.String(attrCreatedSort), AttributeType: types.ScalarAttributeTypeB},
				{AttributeName: aws.String(attrCreatorSort), AttributeType: types.ScalarAttributeTypeB},
				{AttributeName: aws.String(attrActiveSort), AttributeType: types.ScalarAttributeTypeB},
			},
			KeySchema: []types.KeySchemaElement{
				{AttributeName: aws.String(attrPK), KeyType: types.KeyTypeHash},
				{AttributeName: aws.String(attrSK), KeyType: types.KeyTypeRange},
			},
			// All three are keyed by attributes only an invite's record
			// carries: the copy, and every item a join adds to the
			// partition, must omit chat and the three sorts, or they leak
			// in. Each projects the whole record, which holds no key
			// material: the secret envelope opens only with its creator's
			// account key, and the copy is an item of its own.
			GlobalSecondaryIndexes: []types.GlobalSecondaryIndex{
				{
					IndexName: aws.String(gsiInvitesByChat),
					KeySchema: []types.KeySchemaElement{
						{AttributeName: aws.String(attrChat), KeyType: types.KeyTypeHash},
						{AttributeName: aws.String(attrCreatedSort), KeyType: types.KeyTypeRange},
					},
					Projection: &types.Projection{ProjectionType: types.ProjectionTypeAll},
				},
				{
					IndexName: aws.String(gsiInvitesByChatCreator),
					KeySchema: []types.KeySchemaElement{
						{AttributeName: aws.String(attrChat), KeyType: types.KeyTypeHash},
						{AttributeName: aws.String(attrCreatorSort), KeyType: types.KeyTypeRange},
					},
					Projection: &types.Projection{ProjectionType: types.ProjectionTypeAll},
				},
				{
					// Sparse on active_sort too, which a revoke and the join
					// that takes the last use remove, so the index is the
					// chat's invites that may be ACTIVE and a count of it is
					// the cap's (see countActiveInvites). The count reads only
					// keys, but the whole record is projected so that a later
					// reader of the active invites (one creator's, say) needs
					// no new index; a projection cannot be changed in place.
					IndexName: aws.String(gsiInvitesActive),
					KeySchema: []types.KeySchemaElement{
						{AttributeName: aws.String(attrChat), KeyType: types.KeyTypeHash},
						{AttributeName: aws.String(attrActiveSort), KeyType: types.KeyTypeRange},
					},
					Projection: &types.Projection{ProjectionType: types.ProjectionTypeAll},
				},
			},
		},
		{
			TableName:   aws.String(pinsTable),
			BillingMode: types.BillingModePayPerRequest,
			AttributeDefinitions: []types.AttributeDefinition{
				{AttributeName: aws.String(attrPK), AttributeType: types.ScalarAttributeTypeS},
				{AttributeName: aws.String(attrSK), AttributeType: types.ScalarAttributeTypeS},
				{AttributeName: aws.String(attrPinnedSort), AttributeType: types.ScalarAttributeTypeB},
			},
			KeySchema: []types.KeySchemaElement{
				{AttributeName: aws.String(attrPK), KeyType: types.KeyTypeHash},
				{AttributeName: aws.String(attrSK), KeyType: types.KeyTypeRange},
			},
			// An LSI, not a GSI: a caller that pins reads the chat's pins
			// right after, and only a local index serves a strongly
			// consistent read. It cannot be added to the table later, so
			// it is created with it. Sparse on pinned_sort, which pins
			// alone carry: the #meta item must omit it, or it leaks in.
			LocalSecondaryIndexes: []types.LocalSecondaryIndex{
				{
					IndexName: aws.String(lsiByPinned),
					KeySchema: []types.KeySchemaElement{
						{AttributeName: aws.String(attrPK), KeyType: types.KeyTypeHash},
						{AttributeName: aws.String(attrPinnedSort), KeyType: types.KeyTypeRange},
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

	if err := ensureTTL(ctx, client, groupMembersTable, attrExpiresAt); err != nil {
		return err
	}
	if err := ensureTTL(ctx, client, activityTable, attrExpiresAt); err != nil {
		return err
	}
	return ensureTTL(ctx, client, invitesTable, attrExpiresAt)
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
	if err := clearTable(ctx, s.client, s.activityTable, []string{attrPK, attrSK}); err != nil {
		panic(err)
	}
	if err := clearTable(ctx, s.client, s.keyEnvelopesTable, []string{attrPK, attrSK}); err != nil {
		panic(err)
	}
	if err := clearTable(ctx, s.client, s.lobbiesTable, []string{attrPK, attrSK}); err != nil {
		panic(err)
	}
	if err := clearTable(ctx, s.client, s.featuredGroupsTable, []string{attrPK, attrSK}); err != nil {
		panic(err)
	}
	if err := clearTable(ctx, s.client, s.holdingsTable, []string{attrPK}); err != nil {
		panic(err)
	}
	if err := clearTable(ctx, s.client, s.invitesTable, []string{attrPK, attrSK}); err != nil {
		panic(err)
	}
	if err := clearTable(ctx, s.client, s.pinsTable, []string{attrPK, attrSK}); err != nil {
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
