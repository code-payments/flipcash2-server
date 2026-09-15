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
	require.NoError(t, CreateTables(ctx, testEnv.Client, messagesTable, pointersTable, reactionsTable))
	s := NewInDynamoDB(testEnv.Client, messagesTable, pointersTable, reactionsTable).(*store)
	defer s.reset()

	userA := model.MustGenerateUserID()
	userB := model.MustGenerateUserID()

	t.Run("dm", func(t *testing.T) {
		chatID := chat.MustDeriveDmChatID(chatpb.ChatType_TIP_DM, userA, userB)

		_, advanced, err := s.AdvancePointer(ctx, chatID, userA, messagingpb.Pointer_READ, &messagingpb.MessageId{Value: 3})
		require.NoError(t, err)
		require.True(t, advanced)
		_, advanced, err = s.AdvancePointer(ctx, chatID, userB, messagingpb.Pointer_DELIVERED, &messagingpb.MessageId{Value: 2})
		require.NoError(t, err)
		require.True(t, advanced)
		_, advanced, err = s.AdvancePointer(ctx, chatID, userB, messagingpb.Pointer_READ, &messagingpb.MessageId{Value: 1})
		require.NoError(t, err)
		require.True(t, advanced)

		// One item for the chat, carrying both members' pairs and nothing else.
		items := partitionItems(t, chatID)
		require.Len(t, items, 1)
		item := items[0]
		require.Equal(t, skDmPointers, asS(item[attrSK]))

		aReadVal, aReadTS, err := dmPointerAttrs(userA, messagingpb.Pointer_READ)
		require.NoError(t, err)
		bDelVal, bDelTS, err := dmPointerAttrs(userB, messagingpb.Pointer_DELIVERED)
		require.NoError(t, err)
		bReadVal, bReadTS, err := dmPointerAttrs(userB, messagingpb.Pointer_READ)
		require.NoError(t, err)
		require.ElementsMatch(t,
			[]string{attrPK, attrSK, aReadVal, aReadTS, bDelVal, bDelTS, bReadVal, bReadTS},
			attributeNames(item),
		)
		require.Equal(t, "3", item[aReadVal].(*types.AttributeValueMemberN).Value)
		require.Equal(t, "2", item[bDelVal].(*types.AttributeValueMemberN).Value)
		require.Equal(t, "1", item[bReadVal].(*types.AttributeValueMemberN).Value)

		// A no-op for one member changes nothing on the item, the peer's pairs
		// included.
		before := partitionItems(t, chatID)[0]
		_, advanced, err = s.AdvancePointer(ctx, chatID, userA, messagingpb.Pointer_READ, &messagingpb.MessageId{Value: 3})
		require.NoError(t, err)
		require.False(t, advanced)
		require.Equal(t, before, partitionItems(t, chatID)[0])

		// An advance for one member rewrites only that member's pair.
		_, advanced, err = s.AdvancePointer(ctx, chatID, userB, messagingpb.Pointer_READ, &messagingpb.MessageId{Value: 2})
		require.NoError(t, err)
		require.True(t, advanced)
		after := partitionItems(t, chatID)[0]
		for name, av := range before {
			if name == bReadVal || name == bReadTS {
				require.NotEqual(t, av, after[name], name)
				continue
			}
			require.Equal(t, av, after[name], name)
		}

		// The batch read costs one key for the chat, and returns both members.
		got, err := s.GetPointersForChats(ctx, []messaging.PointerRef{{ChatID: chatID, Members: []*commonpb.UserId{userA, userB}}})
		require.NoError(t, err)
		require.Len(t, got[string(chatID.Value)], 3)
	})

	t.Run("group", func(t *testing.T) {
		chatID := chat.MustGenerateGroupChatID()

		_, advanced, err := s.AdvancePointer(ctx, chatID, userA, messagingpb.Pointer_READ, &messagingpb.MessageId{Value: 3})
		require.NoError(t, err)
		require.True(t, advanced)
		_, advanced, err = s.AdvancePointer(ctx, chatID, userB, messagingpb.Pointer_DELIVERED, &messagingpb.MessageId{Value: 2})
		require.NoError(t, err)
		require.True(t, advanced)

		// One item per member, under the fixed attribute names, and no #ptrs item.
		items := partitionItems(t, chatID)
		require.Len(t, items, 2)
		bySK := make(map[string]map[string]types.AttributeValue, len(items))
		for _, item := range items {
			bySK[asS(item[attrSK])] = item
		}
		require.ElementsMatch(t,
			[]string{attrPK, attrSK, attrReadVal, attrReadTS},
			attributeNames(bySK[pointerSK(userA)]),
		)
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
