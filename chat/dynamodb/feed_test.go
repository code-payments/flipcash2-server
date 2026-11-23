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
// which no Store method reads yet: every membership write sets it beside user,
// to the user's inbox, a departure (a leave or a ban) sets it on the
// tombstone, even to a row that lacked it, and neither the rows of a user excluded from the feed nor #meta
// ever carry it.
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

// TestChat_DynamoDBFeedIndexes checks that CreateTables defines the two feed
// indexes, that gsiByFeed lists a user's membership rows, tombstones
// included, and nothing of an excluded user's, and that a DM inbox row's feed
// is the value gsiByTypeActivity has always been keyed by, so that
// gsiByFeedActivity holds it unchanged.
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
	require.Contains(t, indexes(groupMembersTable), gsiByFeed)
	require.Contains(t, indexes(groupMembersTable), gsiByUser)
	require.Contains(t, indexes(dmInboxTable), gsiByFeedActivity)
	require.Contains(t, indexes(dmInboxTable), gsiByTypeActivity)

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

	queryFeed := func(feed string) []string {
		t.Helper()
		out, err := testEnv.Client.Query(ctx, &dynamodb.QueryInput{
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
	// The index is eventually consistent.
	require.Eventually(t, func() bool {
		return len(queryFeed(groupFeedKey(user, false))) == 2
	}, 5*time.Second, 50*time.Millisecond)
	require.ElementsMatch(t, []string{chatPK(joined), chatPK(left)}, queryFeed(groupFeedKey(user, false)))
	require.Empty(t, queryFeed(groupFeedKey(team, false)))

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
