//go:build integration

package dynamodb

import (
	"context"
	"crypto/rand"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/require"

	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"
	messagingpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/messaging/v1"

	"github.com/code-payments/flipcash2-server/messaging"
	"github.com/code-payments/flipcash2-server/model"
)

// TestMessaging_DynamoDBStore_EventLogLayout pins the send path's row layout,
// which the shared suite cannot see: a batch writes one evt# run row keyed by
// its last event_seq, a single send writes a plain row, and idempotency markers
// live outside the chat's partition. Rows are what a send is billed for, so a
// layout regression would pass the shared suite and silently multiply the
// chat partition's write load.
func TestMessaging_DynamoDBStore_EventLogLayout(t *testing.T) {
	ctx := context.Background()
	s := newLayoutStore(t)
	defer s.reset()

	chatID := randomChatID(t)
	sender := model.MustGenerateUserID()

	batchIDs := layoutClientIDs(t, 4)
	_, _, err := s.PutMessages(ctx, chatID, layoutInputs(sender, batchIDs))
	require.NoError(t, err)
	singleID := layoutClientIDs(t, 1)[0]
	_, _, err = s.PutMessage(ctx, chatID, sender, nil, time.Unix(100, 0), singleID, true)
	require.NoError(t, err)

	// The batch's four sends are one row at its last event_seq carrying its first
	// event and naming its first message; the single send is a plain row with no
	// first_event_seq.
	events := eventRows(t, chatID)
	require.Len(t, events, 2)
	require.Equal(t, evtSK(4), asS(events[0][attrSK]))
	require.Equal(t, "1", events[0][attrMessageID].(*types.AttributeValueMemberN).Value)
	require.Equal(t, "1", events[0][attrFirstEventSeq].(*types.AttributeValueMemberN).Value)
	require.Equal(t, evtSK(5), asS(events[1][attrSK]))
	require.Equal(t, "5", events[1][attrMessageID].(*types.AttributeValueMemberN).Value)
	require.NotContains(t, events[1], attrFirstEventSeq)

	// The chat's partition holds the counter, the five messages and the two
	// event rows, and no marker.
	require.Len(t, chatPartition(t, chatID), 1+5+2)

	// Each marker is its own partition, pointing at its message.
	for i, id := range append(batchIDs, singleID) {
		out, err := testEnv.Client.GetItem(ctx, &dynamodb.GetItemInput{
			TableName:      aws.String(messagesTable),
			Key:            markerKey(chatID, id),
			ConsistentRead: aws.Bool(true),
		})
		require.NoError(t, err)
		seq, err := parseN(out.Item[attrSeq])
		require.NoError(t, err)
		require.Equal(t, uint64(i+1), seq)
	}
}

// TestMessaging_DynamoDBStore_EventLogCorruption checks that GetEventDelta fails
// a read over a log whose rows do not tile the event sequence — a hole, or two
// rows claiming one event — rather than silently skipping or repeating a
// message. Neither can be written through the store; the rows are forged.
func TestMessaging_DynamoDBStore_EventLogCorruption(t *testing.T) {
	ctx := context.Background()
	s := newLayoutStore(t)
	defer s.reset()
	sender := model.MustGenerateUserID()

	t.Run("gap", func(t *testing.T) {
		chatID := randomChatID(t)
		_, _, err := s.PutMessages(ctx, chatID, layoutInputs(sender, layoutClientIDs(t, 4)))
		require.NoError(t, err)

		// Shrink the run to cover events 2..4, leaving event 1 unrecorded.
		setFirstEventSeq(t, chatID, 4, 2)
		_, _, err = s.GetEventDelta(ctx, chatID, 0, 4, 100)
		require.ErrorContains(t, err, "event log gap")

		// A cursor past the hole reads normally.
		msgs, next, err := s.GetEventDelta(ctx, chatID, 1, 4, 100)
		require.NoError(t, err)
		require.Len(t, msgs, 3)
		require.Equal(t, uint64(4), next)
	})

	t.Run("invalid first event", func(t *testing.T) {
		chatID := randomChatID(t)
		_, _, err := s.PutMessages(ctx, chatID, layoutInputs(sender, layoutClientIDs(t, 3)))
		require.NoError(t, err)

		// No event is numbered zero, and a run cannot start past its own last
		// event.
		for _, first := range []uint64{0, 4} {
			setFirstEventSeq(t, chatID, 3, first)
			_, _, err = s.GetEventDelta(ctx, chatID, 0, 3, 100)
			require.ErrorContains(t, err, "invalid first event", "first_event_seq %d", first)
		}
	})

	t.Run("overlap", func(t *testing.T) {
		chatID := randomChatID(t)
		_, _, err := s.PutMessages(ctx, chatID, layoutInputs(sender, layoutClientIDs(t, 2)))
		require.NoError(t, err)
		_, _, err = s.PutMessages(ctx, chatID, layoutInputs(sender, layoutClientIDs(t, 2)))
		require.NoError(t, err)

		// Stretch the second run (events 3..4) back over event 2.
		setFirstEventSeq(t, chatID, 4, 2)
		_, _, err = s.GetEventDelta(ctx, chatID, 0, 4, 100)
		require.ErrorContains(t, err, "event log overlap")
	})
}

func newLayoutStore(t *testing.T) *store {
	require.NoError(t, CreateTables(context.Background(), testEnv.Client, messagesTable, pointersTable, reactionsTable, reactorsTable, selfReactionsTable))
	return NewInDynamoDB(testEnv.Client, messagesTable, pointersTable, reactionsTable, reactorsTable, selfReactionsTable).(*store)
}

func randomChatID(t *testing.T) *commonpb.ChatId {
	b := make([]byte, 32)
	_, err := rand.Read(b)
	require.NoError(t, err)
	return &commonpb.ChatId{Value: b}
}

func layoutClientIDs(t *testing.T, n int) []*messagingpb.ClientMessageId {
	ids := make([]*messagingpb.ClientMessageId, n)
	for i := range ids {
		b := make([]byte, messaging.ClientMessageIDSize)
		_, err := rand.Read(b)
		require.NoError(t, err)
		ids[i] = &messagingpb.ClientMessageId{Value: b}
	}
	return ids
}

func layoutInputs(sender *commonpb.UserId, ids []*messagingpb.ClientMessageId) []messaging.MessageInput {
	inputs := make([]messaging.MessageInput, len(ids))
	for i, id := range ids {
		inputs[i] = messaging.MessageInput{SenderID: sender, Timestamp: time.Unix(int64(i+1), 0), ClientMessageID: id, CountsTowardUnread: true}
	}
	return inputs
}

func chatPartition(t *testing.T, chatID *commonpb.ChatId) []map[string]types.AttributeValue {
	out, err := testEnv.Client.Query(context.Background(), &dynamodb.QueryInput{
		TableName:                 aws.String(messagesTable),
		KeyConditionExpression:    aws.String(attrPK + " = :pk"),
		ExpressionAttributeValues: map[string]types.AttributeValue{":pk": avS(chatPK(chatID))},
		ConsistentRead:            aws.Bool(true),
	})
	require.NoError(t, err)
	return out.Items
}

func eventRows(t *testing.T, chatID *commonpb.ChatId) []map[string]types.AttributeValue {
	out, err := testEnv.Client.Query(context.Background(), &dynamodb.QueryInput{
		TableName:              aws.String(messagesTable),
		KeyConditionExpression: aws.String(attrPK + " = :pk AND begins_with(" + attrSK + ", :prefix)"),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":pk":     avS(chatPK(chatID)),
			":prefix": avS(evtPrefix),
		},
		ConsistentRead: aws.Bool(true),
	})
	require.NoError(t, err)
	return out.Items
}

func setFirstEventSeq(t *testing.T, chatID *commonpb.ChatId, lastEventSeq, firstEventSeq uint64) {
	_, err := testEnv.Client.UpdateItem(context.Background(), &dynamodb.UpdateItemInput{
		TableName:                 aws.String(messagesTable),
		Key:                       map[string]types.AttributeValue{attrPK: avS(chatPK(chatID)), attrSK: avS(evtSK(lastEventSeq))},
		UpdateExpression:          aws.String("SET " + attrFirstEventSeq + " = :first"),
		ConditionExpression:       aws.String("attribute_exists(" + attrPK + ")"),
		ExpressionAttributeValues: map[string]types.AttributeValue{":first": avN(firstEventSeq)},
	})
	require.NoError(t, err)
}
