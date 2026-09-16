//go:build integration

package dynamodb

import (
	"context"
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

// TestChat_TombstoneTTL pins the tombstone's lifetime, which no Store method
// observes: a departure stamps expires_at tombstoneTTL after left_at, a rejoin
// clears it so an active membership can never be swept, a creation-time row
// never carries it, and the table has TTL enabled on the attribute. The
// attribute is read by DynamoDB's sweeper, not by any code here, so a slip in
// any of these would delete live memberships silently.
func TestChat_TombstoneTTL(t *testing.T) {
	ctx := context.Background()
	require.NoError(t, CreateTables(ctx, testEnv.Client, chatsTable, dmInboxTable, groupMembersTable))

	testStore := NewInDynamoDB(testEnv.Client, chatsTable, dmInboxTable, groupMembersTable)
	defer testStore.(*store).reset()

	ttl, err := testEnv.Client.DescribeTimeToLive(ctx, &dynamodb.DescribeTimeToLiveInput{TableName: aws.String(groupMembersTable)})
	require.NoError(t, err)
	require.NotNil(t, ttl.TimeToLiveDescription)
	require.Contains(t, []types.TimeToLiveStatus{types.TimeToLiveStatusEnabled, types.TimeToLiveStatusEnabling}, ttl.TimeToLiveDescription.TimeToLiveStatus)
	require.Equal(t, attrExpiresAt, aws.ToString(ttl.TimeToLiveDescription.AttributeName))

	userID := model.MustGenerateUserID()
	c := &chat.Chat{
		ID:           chat.MustGenerateGroupChatID(),
		Type:         chatpb.ChatType_GROUP,
		Members:      []*commonpb.UserId{userID},
		Title:        "TTL",
		LastActivity: time.Now(),
	}
	require.NoError(t, testStore.PutChat(ctx, c))

	memberItem := func() map[string]types.AttributeValue {
		out, err := testEnv.Client.GetItem(ctx, &dynamodb.GetItemInput{
			TableName:      aws.String(groupMembersTable),
			Key:            map[string]types.AttributeValue{attrPK: avS(chatPK(c.ID)), attrSK: avS(userPK(userID))},
			ConsistentRead: aws.Bool(true),
		})
		require.NoError(t, err)
		require.NotEmpty(t, out.Item)
		return out.Item
	}

	// A creation-time row is joined and unstamped.
	require.NotContains(t, memberItem(), attrExpiresAt)

	// A departure stamps expiry at left_at plus the TTL, in epoch seconds.
	changed, _, err := testStore.RemoveGroupMember(ctx, c.ID, userID)
	require.NoError(t, err)
	require.True(t, changed)
	item := memberItem()
	leftAt, err := parseN(item[attrLeftAt])
	require.NoError(t, err)
	expiresAt, err := parseN(item[attrExpiresAt])
	require.NoError(t, err)
	require.EqualValues(t, time.Unix(0, int64(leftAt)).Add(tombstoneTTL).Unix(), expiresAt)

	// A rejoin clears it, along with left_at.
	changed, _, err = testStore.AddGroupMembers(ctx, c.ID, []*commonpb.UserId{userID})
	require.NoError(t, err)
	require.True(t, changed)
	item = memberItem()
	require.NotContains(t, item, attrExpiresAt)
	require.NotContains(t, item, attrLeftAt)
	require.Contains(t, item, attrJoinedAt)

	// Leaving again stamps it afresh.
	changed, _, err = testStore.RemoveGroupMember(ctx, c.ID, userID)
	require.NoError(t, err)
	require.True(t, changed)
	require.Contains(t, memberItem(), attrExpiresAt)
}
