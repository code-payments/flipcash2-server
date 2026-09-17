//go:build integration

package dynamodb

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"
	messagingpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/messaging/v1"

	"github.com/code-payments/flipcash2-server/messaging"
	"github.com/code-payments/flipcash2-server/model"
)

// TestMessaging_DynamoDBStore_AddReaction_ActivatesAfterConcurrentEmpty pins
// that an add which loses its aggregate CAS to the emoji's last reactor leaving
// re-decides the type cap from a fresh read, not the cap it read before the
// removal. The add's own transaction carried no meta item (the emoji was active
// when it read), so the cancellation refreshes the aggregate alone; deciding
// the now-activating retry against the stale cap would refuse the slot the
// removal just freed.
func TestMessaging_DynamoDBStore_AddReaction_ActivatesAfterConcurrentEmpty(t *testing.T) {
	ctx := context.Background()
	require.NoError(t, CreateTables(ctx, testEnv.Client, messagesTable, pointersTable, reactionsTable, reactorsTable, selfReactionsTable))
	s := NewInDynamoDB(testEnv.Client, messagesTable, pointersTable, reactionsTable, reactorsTable, selfReactionsTable).(*store)
	defer s.reset()

	chatID := &commonpb.ChatId{Value: make([]byte, 32)}
	userA := model.MustGenerateUserID()
	userB := model.MustGenerateUserID()
	ts := time.Unix(1_700_000_000, 0).UTC()

	clientID := &messagingpb.ClientMessageId{Value: make([]byte, messaging.ClientMessageIDSize)}
	msg, _, err := s.PutMessage(ctx, chatID, userA, nil, ts, clientID, true)
	require.NoError(t, err)
	msgID := msg.ID

	// Fill the message to the cap, with the last slot held by B's 👍 alone.
	const target = "👍"
	for i := 0; i < messaging.MaxReactionTypesPerMessage-1; i++ {
		_, created, tooMany, err := s.AddReaction(ctx, chatID, msgID, userA, fmt.Sprintf("pre-%d", i), ts)
		require.NoError(t, err)
		require.True(t, created)
		require.False(t, tooMany)
	}
	_, created, tooMany, err := s.AddReaction(ctx, chatID, msgID, userB, target, ts)
	require.NoError(t, err)
	require.True(t, created)
	require.False(t, tooMany)

	// A adds 👍: it reads the emoji active at the cap, so its transaction has no
	// meta item. Before that transaction commits, B removes 👍, emptying the emoji
	// and freeing a slot. A's aggregate CAS then fails and it retries.
	var removed bool
	s.beforeReactionWrite = func() {
		if removed {
			return
		}
		removed = true
		s.beforeReactionWrite = nil
		_, ok, err := s.RemoveReaction(ctx, chatID, msgID, userB, target)
		require.NoError(t, err)
		require.True(t, ok)
	}
	reaction, created, tooMany, err := s.AddReaction(ctx, chatID, msgID, userA, target, ts.Add(time.Second))
	require.NoError(t, err)
	require.True(t, removed)
	require.True(t, created, "add lost to a concurrent removal of the emoji's last reactor and must retake the freed slot")
	require.False(t, tooMany)
	require.EqualValues(t, 1, reaction.Count)

	// The message sits exactly at the cap with A as 👍's only reactor.
	summary, err := s.GetReactionSummary(ctx, chatID, msgID)
	require.NoError(t, err)
	require.Len(t, summary, messaging.MaxReactionTypesPerMessage)
	reactors, _, hasMore, err := s.GetReactors(ctx, chatID, msgID, target)
	require.NoError(t, err)
	require.False(t, hasMore)
	require.Len(t, reactors, 1)
	require.Equal(t, userA.Value, reactors[0].UserID.Value)
}
