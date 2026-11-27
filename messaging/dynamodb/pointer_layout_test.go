//go:build integration

package dynamodb

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/require"

	chatpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/chat/v1"
	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"
	messagingpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/messaging/v1"

	"github.com/code-payments/flipcash2-server/chat"
	"github.com/code-payments/flipcash2-server/messaging"
	"github.com/code-payments/flipcash2-server/model"
)

// TestMessaging_DynamoDBStore_PointerLayout pins the per-chat-type item layout
// the shared suite is agnostic to: a DM's pointers share one #ptrs item, a
// group's are one ptr#<user> item per member. The feed's pointer read is
// billed per item, so the layout is the optimization — a shape regression
// would pass the shared suite and silently double the read.
func TestMessaging_DynamoDBStore_PointerLayout(t *testing.T) {
	ctx := context.Background()
	require.NoError(t, CreateTables(ctx, testEnv.Client, messagesTable, pointersTable, reactionsTable, reactorsTable, selfReactionsTable, boostsTable))
	s := NewInDynamoDB(testEnv.Client, messagesTable, pointersTable, reactionsTable, reactorsTable, selfReactionsTable, boostsTable).(*store)
	defer s.reset()

	userA := model.MustGenerateUserID()
	userB := model.MustGenerateUserID()

	t.Run("dm", func(t *testing.T) {
		chatID := chat.MustDeriveDmChatID(chatpb.ChatType_DM, userA, userB)

		_, advanced, err := s.AdvancePointer(ctx, chatID, userA, messagingpb.Pointer_READ, &messagingpb.MessageId{Value: 3}, 2)
		require.NoError(t, err)
		require.True(t, advanced)
		_, advanced, err = s.AdvancePointer(ctx, chatID, userB, messagingpb.Pointer_DELIVERED, &messagingpb.MessageId{Value: 2}, 1)
		require.NoError(t, err)
		require.True(t, advanced)
		_, advanced, err = s.AdvancePointer(ctx, chatID, userB, messagingpb.Pointer_READ, &messagingpb.MessageId{Value: 1}, 0)
		require.NoError(t, err)
		require.True(t, advanced)

		// One item for the chat, carrying both members' attributes and nothing
		// else: a READ pointer's pair with its unread_seq record, a DELIVERED
		// pointer's pair alone.
		items := partitionItems(t, chatID)
		require.Len(t, items, 1)
		item := items[0]
		require.Equal(t, skDmPointers, asS(item[attrSK]))

		aRead, err := dmPointerAttrs(userA, messagingpb.Pointer_READ)
		require.NoError(t, err)
		bDel, err := dmPointerAttrs(userB, messagingpb.Pointer_DELIVERED)
		require.NoError(t, err)
		bRead, err := dmPointerAttrs(userB, messagingpb.Pointer_READ)
		require.NoError(t, err)
		require.ElementsMatch(t,
			[]string{
				attrPK, attrSK,
				aRead.val, aRead.ts, aRead.unreadSeq, aRead.unreadSeqFor,
				bDel.val, bDel.ts,
				bRead.val, bRead.ts, bRead.unreadSeq, bRead.unreadSeqFor,
			},
			attributeNames(item),
		)
		require.Equal(t, "3", item[aRead.val].(*types.AttributeValueMemberN).Value)
		require.Equal(t, "2", item[aRead.unreadSeq].(*types.AttributeValueMemberN).Value)
		require.Equal(t, "3", item[aRead.unreadSeqFor].(*types.AttributeValueMemberN).Value)
		require.Equal(t, "2", item[bDel.val].(*types.AttributeValueMemberN).Value)
		require.Equal(t, "1", item[bRead.val].(*types.AttributeValueMemberN).Value)
		require.Equal(t, "0", item[bRead.unreadSeq].(*types.AttributeValueMemberN).Value)
		require.Equal(t, "1", item[bRead.unreadSeqFor].(*types.AttributeValueMemberN).Value)

		// A no-op for one member changes nothing on the item, the peer's
		// attributes included.
		before := partitionItems(t, chatID)[0]
		_, advanced, err = s.AdvancePointer(ctx, chatID, userA, messagingpb.Pointer_READ, &messagingpb.MessageId{Value: 3}, 9)
		require.NoError(t, err)
		require.False(t, advanced)
		require.Equal(t, before, partitionItems(t, chatID)[0])

		// An advance for one member rewrites only that member's attributes for
		// the type.
		_, advanced, err = s.AdvancePointer(ctx, chatID, userB, messagingpb.Pointer_READ, &messagingpb.MessageId{Value: 2}, 1)
		require.NoError(t, err)
		require.True(t, advanced)
		after := partitionItems(t, chatID)[0]
		for name, av := range before {
			switch name {
			case bRead.val, bRead.ts, bRead.unreadSeq, bRead.unreadSeqFor:
				require.NotEqual(t, av, after[name], name)
			default:
				require.Equal(t, av, after[name], name)
			}
		}

		// The batch read costs one key for the chat, and returns both members.
		got, err := s.GetPointersForChats(ctx, []messaging.PointerRef{{ChatID: chatID, Members: []*commonpb.UserId{userA, userB}}})
		require.NoError(t, err)
		require.Len(t, got[string(chatID.Value)], 3)
	})

	t.Run("group", func(t *testing.T) {
		chatID := chat.MustGenerateGroupChatID()

		_, advanced, err := s.AdvancePointer(ctx, chatID, userA, messagingpb.Pointer_READ, &messagingpb.MessageId{Value: 3}, 2)
		require.NoError(t, err)
		require.True(t, advanced)
		_, advanced, err = s.AdvancePointer(ctx, chatID, userB, messagingpb.Pointer_DELIVERED, &messagingpb.MessageId{Value: 2}, 1)
		require.NoError(t, err)
		require.True(t, advanced)

		// One item per member, under the fixed attribute names, and no #ptrs
		// item. Only the READ pointer has an unread_seq record.
		items := partitionItems(t, chatID)
		require.Len(t, items, 2)
		bySK := make(map[string]map[string]types.AttributeValue, len(items))
		for _, item := range items {
			bySK[asS(item[attrSK])] = item
		}
		require.ElementsMatch(t,
			[]string{attrPK, attrSK, attrReadVal, attrReadTS, attrReadUnreadSeq, attrReadUnreadSeqFor},
			attributeNames(bySK[pointerSK(userA)]),
		)
		require.Equal(t, "2", bySK[pointerSK(userA)][attrReadUnreadSeq].(*types.AttributeValueMemberN).Value)
		require.Equal(t, "3", bySK[pointerSK(userA)][attrReadUnreadSeqFor].(*types.AttributeValueMemberN).Value)
		require.ElementsMatch(t,
			[]string{attrPK, attrSK, attrDeliveredVal, attrDeliveredTS},
			attributeNames(bySK[pointerSK(userB)]),
		)
	})
}

// partitionItems reads every item in the chat's pointer partition.
func partitionItems(t *testing.T, chatID *commonpb.ChatId) []map[string]types.AttributeValue {
	out, err := testEnv.Client.Query(context.Background(), &dynamodb.QueryInput{
		TableName:                 aws.String(pointersTable),
		KeyConditionExpression:    aws.String(attrPK + " = :pk"),
		ExpressionAttributeValues: map[string]types.AttributeValue{":pk": avS(chatPK(chatID))},
		ConsistentRead:            aws.Bool(true),
	})
	require.NoError(t, err)
	return out.Items
}

func attributeNames(item map[string]types.AttributeValue) []string {
	names := make([]string, 0, len(item))
	for name := range item {
		names = append(names, name)
	}
	return names
}
