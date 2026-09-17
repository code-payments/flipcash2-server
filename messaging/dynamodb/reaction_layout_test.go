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

	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"
	messagingpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/messaging/v1"

	"github.com/code-payments/flipcash2-server/chat"
	"github.com/code-payments/flipcash2-server/messaging"
	"github.com/code-payments/flipcash2-server/model"
)

// TestMessaging_DynamoDBStore_SelfReactionLayout pins the per-chat-type write
// the shared suite cannot see: a group reaction lands a viewer-keyed row in
// message_self_reactions and a DM reaction lands none. The DM's overlay is
// answered from the sample, so the row would be a transactional write nothing
// reads; a regression that restores it would pass every behavioural test and
// silently add a third item to the commonest reaction write.
func TestMessaging_DynamoDBStore_SelfReactionLayout(t *testing.T) {
	ctx := context.Background()
	require.NoError(t, CreateTables(ctx, testEnv.Client, messagesTable, pointersTable, reactionsTable, reactorsTable, selfReactionsTable))
	s := NewInDynamoDB(testEnv.Client, messagesTable, pointersTable, reactionsTable, reactorsTable, selfReactionsTable).(*store)
	defer s.reset()

	user := model.MustGenerateUserID()
	ts := time.Unix(1_700_000_000, 0).UTC()
	clientID := &messagingpb.ClientMessageId{Value: make([]byte, messaging.ClientMessageIDSize)}

	react := func(chatID *commonpb.ChatId) *messagingpb.MessageId {
		msg, _, err := s.PutMessage(ctx, chatID, user, nil, ts, clientID, true)
		require.NoError(t, err)
		_, created, tooMany, err := s.AddReaction(ctx, chatID, msg.ID, user, "👍", ts)
		require.NoError(t, err)
		require.True(t, created)
		require.False(t, tooMany)
		return msg.ID
	}
	selfRows := func(chatID *commonpb.ChatId) []map[string]types.AttributeValue {
		out, err := testEnv.Client.Query(ctx, &dynamodb.QueryInput{
			TableName:                 aws.String(selfReactionsTable),
			KeyConditionExpression:    aws.String(attrPK + " = :pk"),
			ExpressionAttributeValues: map[string]types.AttributeValue{":pk": avS(viewerPK(chatID, user))},
			ConsistentRead:            aws.Bool(true),
		})
		require.NoError(t, err)
		return out.Items
	}

	groupID := &commonpb.ChatId{Value: make([]byte, chat.GroupChatIDSize)}
	groupMsg := react(groupID)
	rows := selfRows(groupID)
	require.Len(t, rows, 1, "a group reaction keeps its viewer-keyed row")
	// The row is the viewer's own reactor entry: the add's version and time,
	// stored the way the reactor row stores them.
	require.Equal(t, avN(1), rows[0][attrSelfVersion])
	require.Equal(t, avN(uint64(ts.UnixNano())), rows[0][attrSelfReactedTs])

	dmID := &commonpb.ChatId{Value: make([]byte, 32)}
	dmMsg := react(dmID)
	require.Empty(t, selfRows(dmID), "a DM reaction writes no viewer-keyed row")

	// Both removals are whole transactions; the group's takes its row with it.
	_, removed, err := s.RemoveReaction(ctx, groupID, groupMsg, user, "👍")
	require.NoError(t, err)
	require.True(t, removed)
	require.Empty(t, selfRows(groupID))
	_, removed, err = s.RemoveReaction(ctx, dmID, dmMsg, user, "👍")
	require.NoError(t, err)
	require.True(t, removed)
}
