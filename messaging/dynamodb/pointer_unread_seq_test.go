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
	messagingpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/messaging/v1"

	"github.com/code-payments/flipcash2-server/chat"
	"github.com/code-payments/flipcash2-server/messaging"
	"github.com/code-payments/flipcash2-server/model"
)

// TestMessaging_DynamoDBStore_PointerUnreadSeqRecord pins what the shared
// suite cannot reach: a READ pointer's unread_seq record is reported only
// while it was made for the pointer's current value. A writer that moves
// read_value without touching the record (a server from before it, during a
// rolling deploy or after a rollback) leaves one for an earlier value, which
// must read as no unread_seq rather than a wrong one, until a new advance
// records one again. A pointer written before the record existed reads with
// none at all.
func TestMessaging_DynamoDBStore_PointerUnreadSeqRecord(t *testing.T) {
	ctx := context.Background()
	require.NoError(t, CreateTables(ctx, testEnv.Client, messagesTable, pointersTable, reactionsTable, reactorsTable, selfReactionsTable, boostsTable))
	s := NewInDynamoDB(testEnv.Client, messagesTable, pointersTable, reactionsTable, reactorsTable, selfReactionsTable, boostsTable).(*store)
	defer s.reset()

	userA := model.MustGenerateUserID()
	userB := model.MustGenerateUserID()

	for _, shape := range []struct {
		name   string
		chatID *commonpb.ChatId
	}{
		{name: "dm", chatID: chat.MustDeriveDmChatID(chatpb.ChatType_DM, userA, userB)},
		{name: "group", chatID: chat.MustGenerateGroupChatID()},
	} {
		t.Run(shape.name, func(t *testing.T) {
			chatID := shape.chatID

			// requireRead asserts userID's READ pointer is at value, carrying
			// unreadSeq (noUnreadSeq for none), as both reads report it.
			requireRead := func(userID *commonpb.UserId, value uint64, unreadSeq int64) {
				t.Helper()
				pointers, err := s.GetPointers(ctx, chatID)
				require.NoError(t, err)
				byChat, err := s.GetPointersForChats(ctx, []messaging.PointerRef{{ChatID: chatID, Members: []*commonpb.UserId{userID}}})
				require.NoError(t, err)
				for _, got := range [][]*messagingpb.Pointer{pointers, byChat[string(chatID.Value)]} {
					p := findPointer(got, messagingpb.Pointer_READ, userID)
					require.NotNil(t, p)
					require.Equal(t, value, p.Value.Value)
					if unreadSeq == noUnreadSeq {
						require.Nil(t, p.UnreadSeq)
						continue
					}
					require.NotNil(t, p.UnreadSeq)
					require.EqualValues(t, unreadSeq, p.UnreadSeq.Value)
				}
			}

			_, advanced, err := s.AdvancePointer(ctx, chatID, userA, messagingpb.Pointer_READ, &messagingpb.MessageId{Value: 3}, 2)
			require.NoError(t, err)
			require.True(t, advanced)
			requireRead(userA, 3, 2)

			// An older server moves read_value to 5 and leaves the record,
			// made for 3, in place: the pointer reads at 5 with none.
			legacyAdvance(t, chatID, userA, 5)
			requireRead(userA, 5, noUnreadSeq)

			// A new advance records one for its value again.
			_, advanced, err = s.AdvancePointer(ctx, chatID, userA, messagingpb.Pointer_READ, &messagingpb.MessageId{Value: 6}, 4)
			require.NoError(t, err)
			require.True(t, advanced)
			requireRead(userA, 6, 4)

			// A pointer written before the record existed carries none, and
			// on a DM's shared item it leaves the peer's record as it was.
			legacyAdvance(t, chatID, userB, 2)
			requireRead(userB, 2, noUnreadSeq)
			requireRead(userA, 6, 4)
		})
	}
}

// legacyAdvance moves userID's READ pointer to value as a server from before
// the unread_seq record does: the same conditional update, naming the pair
// alone.
func legacyAdvance(t *testing.T, chatID *commonpb.ChatId, userID *commonpb.UserId, value uint64) {
	t.Helper()
	sk, attrs, err := pointerKey(chatID, userID, messagingpb.Pointer_READ)
	require.NoError(t, err)
	_, err = testEnv.Client.UpdateItem(context.Background(), &dynamodb.UpdateItemInput{
		TableName:           aws.String(pointersTable),
		Key:                 map[string]types.AttributeValue{attrPK: avS(chatPK(chatID)), attrSK: avS(sk)},
		UpdateExpression:    aws.String("SET #val = :v, #ts = :ts"),
		ConditionExpression: aws.String("attribute_not_exists(#val) OR #val < :v"),
		ExpressionAttributeNames: map[string]string{
			"#val": attrs.val,
			"#ts":  attrs.ts,
		},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":v":  avN(value),
			":ts": avN(uint64(time.Now().UnixNano())),
		},
	})
	require.NoError(t, err)
}

// noUnreadSeq is the unreadSeq requireRead takes for a pointer that carries
// none.
const noUnreadSeq = -1

func findPointer(pointers []*messagingpb.Pointer, pointerType messagingpb.Pointer_Type, userID *commonpb.UserId) *messagingpb.Pointer {
	for _, p := range pointers {
		if p.Type == pointerType && string(p.UserId.Value) == string(userID.Value) {
			return p
		}
	}
	return nil
}
