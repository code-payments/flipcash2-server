//go:build integration

package dynamodb

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/require"

	chatpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/chat/v1"
	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"
	messagingpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/messaging/v1"

	"github.com/code-payments/flipcash2-server/chat"
	"github.com/code-payments/flipcash2-server/model"
)

// newFeedTestStore creates the tables and a store excluding excluded from the
// feed, reset when the test ends.
func newFeedTestStore(t *testing.T, excluded ...*commonpb.UserId) chat.Store {
	t.Helper()
	ctx := context.Background()
	require.NoError(t, CreateTables(ctx, testEnv.Client, chatsTable, dmInboxTable, groupMembersTable, userStateTable, activityTable, keyEnvelopesTable, lobbiesTable, featuredGroupsTable, holdingsTable, invitesTable, pinsTable))
	s := NewInDynamoDB(testEnv.Client, chatsTable, dmInboxTable, groupMembersTable, userStateTable, activityTable, keyEnvelopesTable, lobbiesTable, featuredGroupsTable, holdingsTable, invitesTable, pinsTable, excluded)
	t.Cleanup(func() { s.(*store).reset() })
	return s
}

func groupMembersItem(t *testing.T, chatID *commonpb.ChatId, sk string) map[string]types.AttributeValue {
	t.Helper()
	out, err := testEnv.Client.GetItem(context.Background(), &dynamodb.GetItemInput{
		TableName:      aws.String(groupMembersTable),
		Key:            map[string]types.AttributeValue{attrPK: avS(chatPK(chatID)), attrSK: avS(sk)},
		ConsistentRead: aws.Bool(true),
	})
	require.NoError(t, err)
	require.NotEmpty(t, out.Item)
	return out.Item
}

// requireListed checks that userID's row in chatID carries the inbox feed,
// spelled out rather than built with groupFeedKey, beside user.
func requireListed(t *testing.T, chatID *commonpb.ChatId, userID *commonpb.UserId) {
	t.Helper()
	item := groupMembersItem(t, chatID, userPK(userID))
	require.Equal(t, "user#"+hex.EncodeToString(userID.Value), asS(item[attrFeed]))
	require.Equal(t, hex.EncodeToString(userID.Value), asS(item[attrUser]))
}

// requireUnlisted checks that an item carries neither index attribute.
func requireUnlisted(t *testing.T, chatID *commonpb.ChatId, sk string) {
	t.Helper()
	item := groupMembersItem(t, chatID, sk)
	require.NotContains(t, item, attrFeed)
	require.NotContains(t, item, attrUser)
}

// setIndexKeys gives userID's row in chatID both index attributes, with their
// inbox values, as a write from before the store excluded the user left it.
func setIndexKeys(t *testing.T, chatID *commonpb.ChatId, userID *commonpb.UserId) {
	t.Helper()
	_, err := testEnv.Client.UpdateItem(context.Background(), &dynamodb.UpdateItemInput{
		TableName:                aws.String(groupMembersTable),
		Key:                      map[string]types.AttributeValue{attrPK: avS(chatPK(chatID)), attrSK: avS(userPK(userID))},
		UpdateExpression:         aws.String("SET #user = :user, #feed = :feed"),
		ConditionExpression:      aws.String("attribute_exists(#state)"),
		ExpressionAttributeNames: map[string]string{"#user": attrUser, "#feed": attrFeed, "#state": attrState},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":user": avS(hex.EncodeToString(userID.Value)),
			":feed": avS("user#" + hex.EncodeToString(userID.Value)),
		},
	})
	require.NoError(t, err)
}

// queryFeed lists the pks gsiByFeed holds under feed.
func queryFeed(t *testing.T, feed string) []string {
	t.Helper()
	out, err := testEnv.Client.Query(context.Background(), &dynamodb.QueryInput{
		TableName:                 aws.String(groupMembersTable),
		IndexName:                 aws.String(gsiByFeed),
		KeyConditionExpression:    aws.String("#feed = :feed"),
		ExpressionAttributeNames:  map[string]string{"#feed": attrFeed},
		ExpressionAttributeValues: map[string]types.AttributeValue{":feed": avS(feed)},
	})
	require.NoError(t, err)
	var pks []string
	for _, item := range out.Items {
		pks = append(pks, asS(item[attrPK]))
	}
	return pks
}

// membershipIDs lists the chat IDs GetGroupMembershipsForUser returns, hex
// encoded, failing the test on an error.
func membershipIDs(t *testing.T, s chat.Store, userID *commonpb.UserId) []string {
	t.Helper()
	memberships, err := s.GetGroupMembershipsForUser(context.Background(), userID, chat.FolderInbox)
	require.NoError(t, err)
	ids := make([]string, 0, len(memberships))
	for _, m := range memberships {
		ids = append(ids, hex.EncodeToString(m.ChatID.Value))
	}
	return ids
}

// removeFeed makes userID's row in chatID what a write from before feed left.
func removeFeed(t *testing.T, chatID *commonpb.ChatId, userID *commonpb.UserId) {
	t.Helper()
	_, err := testEnv.Client.UpdateItem(context.Background(), &dynamodb.UpdateItemInput{
		TableName:                aws.String(groupMembersTable),
		Key:                      map[string]types.AttributeValue{attrPK: avS(chatPK(chatID)), attrSK: avS(userPK(userID))},
		UpdateExpression:         aws.String("REMOVE #feed"),
		ExpressionAttributeNames: map[string]string{"#feed": attrFeed},
	})
	require.NoError(t, err)
}

// TestChat_DynamoDBGroupMemberFeed pins the feed attribute on group_members,
// the hash key of gsiByFeed, which every listing of a user's groups reads:
// every membership write sets it beside user, to the user's inbox, a departure
// (a leave or a ban) sets it on the tombstone, even to a row that lacked it,
// and neither the rows of a user excluded from the feed nor #meta ever carry
// it.
func TestChat_DynamoDBGroupMemberFeed(t *testing.T) {
	ctx := context.Background()
	team := model.MustGenerateUserID()
	s := newFeedTestStore(t, team)

	creator := model.MustGenerateUserID()
	member := model.MustGenerateUserID()
	groupID := chat.MustGenerateGroupChatID()
	require.NoError(t, s.PutChat(ctx, &chat.Chat{
		ID:           groupID,
		Type:         chatpb.ChatType_GROUP,
		Members:      []*commonpb.UserId{creator, member, team},
		Title:        "Feed",
		IsPrivate:    true,
		CreatorID:    creator,
		LastActivity: time.Now(),
	}))

	// Creation.
	requireListed(t, groupID, creator)
	requireListed(t, groupID, member)
	requireUnlisted(t, groupID, userPK(team))
	requireUnlisted(t, groupID, skMeta)

	// Join, departure and rejoin. The rejoin is of a row stripped of feed,
	// as a tombstone from before feed would be, and gives it back.
	joiner := model.MustGenerateUserID()
	changed, _, err := s.AddGroupMembers(ctx, groupID, []*commonpb.UserId{joiner})
	require.NoError(t, err)
	require.True(t, changed)
	requireListed(t, groupID, joiner)
	changed, _, err = s.RemoveGroupMember(ctx, groupID, joiner, false)
	require.NoError(t, err)
	require.True(t, changed)
	requireListed(t, groupID, joiner)
	removeFeed(t, groupID, joiner)
	changed, _, err = s.AddGroupMembers(ctx, groupID, []*commonpb.UserId{joiner})
	require.NoError(t, err)
	require.True(t, changed)
	requireListed(t, groupID, joiner)

	// A departure from a joined row stripped of feed, as a membership from
	// before feed would be, files the tombstone in the inbox.
	removeFeed(t, groupID, joiner)
	changed, _, err = s.RemoveGroupMember(ctx, groupID, joiner, false)
	require.NoError(t, err)
	require.True(t, changed)
	requireListed(t, groupID, joiner)

	// An excluded user's departure and rejoin, the rejoin of a row stripped
	// of nothing, stay unlisted.
	changed, _, err = s.RemoveGroupMember(ctx, groupID, team, false)
	require.NoError(t, err)
	require.True(t, changed)
	requireUnlisted(t, groupID, userPK(team))
	changed, _, err = s.AddGroupMembers(ctx, groupID, []*commonpb.UserId{team})
	require.NoError(t, err)
	require.True(t, changed)
	requireUnlisted(t, groupID, userPK(team))

	// Admission from the lobby.
	waiting := model.MustGenerateUserID()
	_, entered, err := s.EnterLobby(ctx, groupID, waiting, chat.LobbyLimits{LobbySize: 10, LobbiesPerUser: 10})
	require.NoError(t, err)
	require.True(t, entered)
	changed, _, err = s.AdmitFromLobby(ctx, groupID, waiting, chat.KeyEnvelope{
		Scheme:     chatpb.KeyEnvelope_X25519_XCHACHA20POLY1305,
		Nonce:      bytes.Repeat([]byte{1}, 24),
		Ciphertext: bytes.Repeat([]byte{1}, 48),
		WrappedBy:  creator,
	})
	require.NoError(t, err)
	require.True(t, changed)
	requireListed(t, groupID, waiting)

	// A join with an invite.
	now := time.Now()
	token := make([]byte, chat.InviteTokenSize)
	_, err = rand.Read(token)
	require.NoError(t, err)
	secret := &chatpb.InviteSecretEnvelope{
		Scheme:     chatpb.InviteSecretEnvelope_X25519_XCHACHA20POLY1305,
		Nonce:      bytes.Repeat([]byte{2}, 24),
		Ciphertext: bytes.Repeat([]byte{2}, 48),
	}
	invite := chat.Invite{
		ID:             chat.MustDeriveInviteID(&chatpb.InviteToken{Value: token}),
		ChatID:         groupID,
		CreatedBy:      creator,
		CreatedAt:      now,
		SecretEnvelope: secret,
	}
	_, created, err := s.CreateInvite(ctx, invite, &chatpb.InviteKeyEnvelope{
		Scheme:     chatpb.InviteKeyEnvelope_XCHACHA20POLY1305,
		Nonce:      bytes.Repeat([]byte{3}, 24),
		Ciphertext: bytes.Repeat([]byte{3}, 48),
		Signature:  bytes.Repeat([]byte{3}, 64),
	}, chat.MaxActiveInvites, now)
	require.NoError(t, err)
	require.True(t, created)
	invited := model.MustGenerateUserID()
	result, err := s.JoinWithInvite(ctx, groupID, invited, invite.ID, secret, now)
	require.NoError(t, err)
	require.True(t, result.Changed)
	requireListed(t, groupID, invited)

	// A ban of a member leaves a tombstone filed in the inbox.
	banned, err := s.BanUser(ctx, groupID, member, now, false)
	require.NoError(t, err)
	require.True(t, banned.Departed)
	requireListed(t, groupID, member)

	// None of it put feed or user on #meta.
	requireUnlisted(t, groupID, skMeta)
}

// TestChat_DynamoDBFeedIndexes checks that CreateTables defines the feed
// indexes the reads use and none of the legacy ones, that gsiByFeed lists a
// user's membership rows, tombstones included, and nothing of an excluded
// user's, and that a DM inbox row's feed is the value the legacy
// by_type_activity index was always keyed by, so that gsiByFeedActivity held
// every row unchanged.
func TestChat_DynamoDBFeedIndexes(t *testing.T) {
	ctx := context.Background()
	team := model.MustGenerateUserID()
	s := newFeedTestStore(t, team)

	indexes := func(table string) []string {
		t.Helper()
		out, err := testEnv.Client.DescribeTable(ctx, &dynamodb.DescribeTableInput{TableName: aws.String(table)})
		require.NoError(t, err)
		var names []string
		for _, gsi := range out.Table.GlobalSecondaryIndexes {
			names = append(names, aws.ToString(gsi.IndexName))
		}
		return names
	}
	require.ElementsMatch(t, []string{gsiByFeed, gsiByJoinedAt}, indexes(groupMembersTable))
	require.ElementsMatch(t, []string{gsiByFeedActivity}, indexes(dmInboxTable))

	user := model.MustGenerateUserID()
	joined := chat.MustGenerateGroupChatID()
	left := chat.MustGenerateGroupChatID()
	for _, id := range []*commonpb.ChatId{joined, left} {
		require.NoError(t, s.PutChat(ctx, &chat.Chat{
			ID:           id,
			Type:         chatpb.ChatType_GROUP,
			Members:      []*commonpb.UserId{user, team},
			Title:        "Feed",
			LastActivity: time.Now(),
		}))
	}
	changed, _, err := s.RemoveGroupMember(ctx, left, user, false)
	require.NoError(t, err)
	require.True(t, changed)

	// The index is eventually consistent.
	require.Eventually(t, func() bool {
		return len(queryFeed(t, groupFeedKey(user, false))) == 2
	}, 5*time.Second, 50*time.Millisecond)
	require.ElementsMatch(t, []string{chatPK(joined), chatPK(left)}, queryFeed(t, groupFeedKey(user, false)))
	require.Empty(t, queryFeed(t, groupFeedKey(team, false)))

	dmIDValue := make([]byte, chat.DmChatIDSize)
	_, err = rand.Read(dmIDValue)
	require.NoError(t, err)
	dmID := &commonpb.ChatId{Value: dmIDValue}
	other := model.MustGenerateUserID()
	require.NoError(t, s.PutChat(ctx, &chat.Chat{
		ID:           dmID,
		Type:         chatpb.ChatType_DM,
		Members:      []*commonpb.UserId{user, other},
		LastActivity: time.Now(),
	}))
	out, err := testEnv.Client.GetItem(ctx, &dynamodb.GetItemInput{
		TableName:      aws.String(dmInboxTable),
		Key:            map[string]types.AttributeValue{attrPK: avS(userPK(user)), attrSK: avS(chatSK(dmID))},
		ConsistentRead: aws.Bool(true),
	})
	require.NoError(t, err)
	require.Equal(t, fmt.Sprintf("user#%s#%d", hex.EncodeToString(user.Value), chatpb.ChatType_DM), asS(out.Item[attrFeed]))
	require.Equal(t, asS(out.Item[attrFeed]), dmFeedKey(user, chatpb.ChatType_DM, false))
	require.Equal(t, asS(out.Item[attrFeed])+"#archived", dmFeedKey(user, chatpb.ChatType_DM, true))
	require.Equal(t, "user#"+hex.EncodeToString(user.Value)+"#archived", groupFeedKey(user, true))
}

// TestChat_DynamoDBExcludedRowLosesIndexKeys checks that the row of a user
// the store excludes from the feed which still carries user and feed — as a
// row written before the user was excluded would — loses both on a departure
// and on a join, and is then in no listing of the user's groups.
func TestChat_DynamoDBExcludedRowLosesIndexKeys(t *testing.T) {
	ctx := context.Background()
	team := model.MustGenerateUserID()
	s := newFeedTestStore(t, team)

	creator := model.MustGenerateUserID()
	departed := chat.MustGenerateGroupChatID()
	rejoined := chat.MustGenerateGroupChatID()
	for _, id := range []*commonpb.ChatId{departed, rejoined} {
		require.NoError(t, s.PutChat(ctx, &chat.Chat{
			ID:           id,
			Type:         chatpb.ChatType_GROUP,
			Members:      []*commonpb.UserId{creator, team},
			Title:        "Feed",
			LastActivity: time.Now(),
		}))
	}

	// A joined row and a tombstone, each carrying both attributes. The
	// listing reads them, so the raw rows are what the store must clean.
	changed, _, err := s.RemoveGroupMember(ctx, rejoined, team, false)
	require.NoError(t, err)
	require.True(t, changed)
	setIndexKeys(t, departed, team)
	setIndexKeys(t, rejoined, team)
	require.Eventually(t, func() bool {
		return len(membershipIDs(t, s, team)) == 2
	}, 5*time.Second, 50*time.Millisecond)

	changed, _, err = s.RemoveGroupMember(ctx, departed, team, false)
	require.NoError(t, err)
	require.True(t, changed)
	requireUnlisted(t, departed, userPK(team))

	changed, _, err = s.AddGroupMembers(ctx, rejoined, []*commonpb.UserId{team})
	require.NoError(t, err)
	require.True(t, changed)
	requireUnlisted(t, rejoined, userPK(team))

	require.Eventually(t, func() bool {
		return len(queryFeed(t, groupFeedKey(team, false))) == 0 && len(membershipIDs(t, s, team)) == 0
	}, 5*time.Second, 50*time.Millisecond)
	chats, err := s.GetGroupChatsForUser(ctx, team, chat.FolderInbox)
	require.NoError(t, err)
	require.Empty(t, chats)
	chats, err = s.GetGroupChatsForUserByIDs(ctx, team, chat.FolderInbox, []*commonpb.ChatId{departed, rejoined})
	require.NoError(t, err)
	require.Empty(t, chats)
}

// TestChat_DynamoDBListingsReadFeed checks that the listings of a user's
// groups read gsiByFeed, not the user attribute the legacy by_user index was
// keyed by: a row carrying user but no feed — a
// row written before feed existed that the backfill missed — is in none of
// them. The backfill, which gave feed to every such row, is what makes the
// switch to gsiByFeed lose no membership.
func TestChat_DynamoDBListingsReadFeed(t *testing.T) {
	ctx := context.Background()
	s := newFeedTestStore(t)

	user := model.MustGenerateUserID()
	listed := chat.MustGenerateGroupChatID()
	legacy := chat.MustGenerateGroupChatID()
	for _, id := range []*commonpb.ChatId{listed, legacy} {
		require.NoError(t, s.PutChat(ctx, &chat.Chat{
			ID:           id,
			Type:         chatpb.ChatType_GROUP,
			Members:      []*commonpb.UserId{user},
			Title:        "Feed",
			LastActivity: time.Now(),
		}))
	}
	removeFeed(t, legacy, user)
	item := groupMembersItem(t, legacy, userPK(user))
	require.Equal(t, hex.EncodeToString(user.Value), asS(item[attrUser]))

	want := []string{hex.EncodeToString(listed.Value)}
	require.Eventually(t, func() bool {
		ids := membershipIDs(t, s, user)
		return len(ids) == 1 && ids[0] == want[0]
	}, 5*time.Second, 50*time.Millisecond)

	chats, err := s.GetGroupChatsForUser(ctx, user, chat.FolderInbox)
	require.NoError(t, err)
	require.Len(t, chats, 1)
	require.Equal(t, listed.Value, chats[0].ID.Value)

	chats, err = s.GetGroupChatsForUserByIDs(ctx, user, chat.FolderInbox, []*commonpb.ChatId{listed, legacy})
	require.NoError(t, err)
	require.Len(t, chats, 1)
	require.Equal(t, listed.Value, chats[0].ID.Value)
}

// userStateItem is userID's raw chat_user_state record on chatID, empty when
// there is none.
func userStateItem(t *testing.T, chatID *commonpb.ChatId, userID *commonpb.UserId) map[string]types.AttributeValue {
	t.Helper()
	out, err := testEnv.Client.GetItem(context.Background(), &dynamodb.GetItemInput{
		TableName:      aws.String(userStateTable),
		Key:            userStateKey(chatID, userID),
		ConsistentRead: aws.Bool(true),
	})
	require.NoError(t, err)
	return out.Item
}

// dmInboxRow is userID's raw dm_inbox row for the DM chatID.
func dmInboxRow(t *testing.T, chatID *commonpb.ChatId, userID *commonpb.UserId) map[string]types.AttributeValue {
	t.Helper()
	out, err := testEnv.Client.GetItem(context.Background(), &dynamodb.GetItemInput{
		TableName:      aws.String(dmInboxTable),
		Key:            map[string]types.AttributeValue{attrPK: avS(userPK(userID)), attrSK: avS(chatSK(chatID))},
		ConsistentRead: aws.Bool(true),
	})
	require.NoError(t, err)
	require.NotEmpty(t, out.Item)
	return out.Item
}

// queryDmFeed lists the sks gsiByFeedActivity holds under feed, most
// recently active first.
func queryDmFeed(t *testing.T, feed string) []string {
	t.Helper()
	out, err := testEnv.Client.Query(context.Background(), &dynamodb.QueryInput{
		TableName:                 aws.String(dmInboxTable),
		IndexName:                 aws.String(gsiByFeedActivity),
		KeyConditionExpression:    aws.String("#feed = :feed"),
		ExpressionAttributeNames:  map[string]string{"#feed": attrFeed},
		ExpressionAttributeValues: map[string]types.AttributeValue{":feed": avS(feed)},
		ScanIndexForward:          aws.Bool(false),
	})
	require.NoError(t, err)
	var sks []string
	for _, item := range out.Items {
		sks = append(sks, asS(item[attrSK]))
	}
	return sks
}

// TestChat_DynamoDBArchiveFiling pins how an archive is filed in the raw
// items: archiving sets archived_at on the user's chat_user_state record and
// moves their dm_inbox or group_members row's feed to the archived folder's
// key, spelled out here, so the row leaves the inbox partition of
// gsiByFeedActivity or gsiByFeed for the archived one, keeping its
// last_activity, which an advance still moves; the chat's #meta muted count
// never carries it; and a departure files the tombstone in the inbox and
// removes archived_at from the record.
func TestChat_DynamoDBArchiveFiling(t *testing.T) {
	ctx := context.Background()
	s := newFeedTestStore(t)

	user, other := model.MustGenerateUserID(), model.MustGenerateUserID()
	userHex := hex.EncodeToString(user.Value)
	dmIDValue := make([]byte, chat.DmChatIDSize)
	_, err := rand.Read(dmIDValue)
	require.NoError(t, err)
	dm := &chat.Chat{
		ID:           &commonpb.ChatId{Value: dmIDValue},
		Type:         chatpb.ChatType_DM,
		Members:      []*commonpb.UserId{user, other},
		LastActivity: time.Unix(1_700_000_000, 0).UTC(),
	}
	require.NoError(t, s.PutChat(ctx, dm))
	group := &chat.Chat{
		ID:           chat.MustGenerateGroupChatID(),
		Type:         chatpb.ChatType_GROUP,
		Members:      []*commonpb.UserId{user, other},
		Title:        "Archive",
		LastActivity: time.Now(),
	}
	require.NoError(t, s.PutChat(ctx, group))
	_, _, err = s.SetMute(ctx, group.ID, user, chat.Mute{Forever: true})
	require.NoError(t, err)

	inboxDmFeed := fmt.Sprintf("user#%s#%d", userHex, chatpb.ChatType_DM)
	archivedDmFeed := inboxDmFeed + "#archived"
	inboxGroupFeed := "user#" + userHex
	archivedGroupFeed := inboxGroupFeed + "#archived"
	require.Equal(t, []string{chatSK(dm.ID)}, queryDmFeed(t, inboxDmFeed))
	require.Equal(t, []string{chatPK(group.ID)}, queryFeed(t, inboxGroupFeed))

	archivedAt := time.Unix(1_700_000_100, 123456789).UTC()
	for _, c := range []*chat.Chat{dm, group} {
		_, changed, err := s.SetArchived(ctx, c, user, true, archivedAt)
		require.NoError(t, err)
		require.True(t, changed)
		record := userStateItem(t, c.ID, user)
		nanos, err := parseN(record[attrArchivedAt])
		require.NoError(t, err)
		require.EqualValues(t, archivedAt.UnixNano(), nanos)
		require.Equal(t, c.ID.Value, asB(record[attrChat]))
	}

	row := dmInboxRow(t, dm.ID, user)
	require.Equal(t, archivedDmFeed, asS(row[attrFeed]))
	require.Equal(t, dmFeedKey(user, chatpb.ChatType_DM, true), asS(row[attrFeed]))
	activity, err := parseN(row[attrLastActivity])
	require.NoError(t, err)
	require.EqualValues(t, dm.LastActivity.UnixNano(), activity)
	require.Equal(t, fmt.Sprintf("user#%s#%d", hex.EncodeToString(other.Value), chatpb.ChatType_DM), asS(dmInboxRow(t, dm.ID, other)[attrFeed]))
	member := groupMembersItem(t, group.ID, userPK(user))
	require.Equal(t, archivedGroupFeed, asS(member[attrFeed]))
	require.Equal(t, groupFeedKey(user, true), asS(member[attrFeed]))
	require.Equal(t, userHex, asS(member[attrUser]))

	require.Eventually(t, func() bool {
		return len(queryDmFeed(t, inboxDmFeed)) == 0 && len(queryFeed(t, inboxGroupFeed)) == 0
	}, 5*time.Second, 50*time.Millisecond)
	require.Equal(t, []string{chatSK(dm.ID)}, queryDmFeed(t, archivedDmFeed))
	require.Equal(t, []string{chatPK(group.ID)}, queryFeed(t, archivedGroupFeed))

	// An advance moves the archived row's activity, leaving it archived.
	advancedTo := dm.LastActivity.Add(time.Minute)
	advanced, _, err := s.AdvanceLastMessage(ctx, dm.ID, &messagingpb.MessageId{Value: 1}, advancedTo)
	require.NoError(t, err)
	require.True(t, advanced)
	row = dmInboxRow(t, dm.ID, user)
	require.Equal(t, archivedDmFeed, asS(row[attrFeed]))
	activity, err = parseN(row[attrLastActivity])
	require.NoError(t, err)
	require.EqualValues(t, advancedTo.UnixNano(), activity)

	// The #meta muted count is the mute's alone.
	meta, err := testEnv.Client.GetItem(ctx, &dynamodb.GetItemInput{
		TableName:      aws.String(userStateTable),
		Key:            map[string]types.AttributeValue{attrPK: avS(chatPK(group.ID)), attrSK: avS(skMeta)},
		ConsistentRead: aws.Bool(true),
	})
	require.NoError(t, err)
	require.NotContains(t, meta.Item, attrArchivedAt)
	count, err := parseInt(meta.Item[attrMutedCount])
	require.NoError(t, err)
	require.EqualValues(t, 1, count)

	// A departure files the tombstone in the inbox and removes the archive
	// from the record, in the same write, leaving the mute.
	changed, _, err := s.RemoveGroupMember(ctx, group.ID, user, false)
	require.NoError(t, err)
	require.True(t, changed)
	tombstone := groupMembersItem(t, group.ID, userPK(user))
	require.Equal(t, inboxGroupFeed, asS(tombstone[attrFeed]))
	require.Contains(t, tombstone, attrLeftAt)
	record := userStateItem(t, group.ID, user)
	require.NotContains(t, record, attrArchivedAt)
	require.Contains(t, record, attrMutedUntil)
	version, err := parseN(record[attrVersion])
	require.NoError(t, err)
	require.EqualValues(t, 3, version)
	require.Eventually(t, func() bool {
		return len(queryFeed(t, archivedGroupFeed)) == 0 && len(queryFeed(t, inboxGroupFeed)) == 1
	}, 5*time.Second, 50*time.Millisecond)

	// Unarchiving the DM files the row back in the inbox.
	_, changed, err = s.SetArchived(ctx, dm, user, false, time.Now())
	require.NoError(t, err)
	require.True(t, changed)
	require.Equal(t, inboxDmFeed, asS(dmInboxRow(t, dm.ID, user)[attrFeed]))
	require.NotContains(t, userStateItem(t, dm.ID, user), attrArchivedAt)
}
