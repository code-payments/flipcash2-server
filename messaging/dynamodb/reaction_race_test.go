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
	require.NoError(t, CreateTables(ctx, testEnv.Client, messagesTable, pointersTable, reactionsTable, reactorsTable, selfReactionsTable, boostsTable))
	s := NewInDynamoDB(testEnv.Client, messagesTable, pointersTable, reactionsTable, reactorsTable, selfReactionsTable, boostsTable).(*store)
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

// TestMessaging_DynamoDBStore_RemoveReaction_LosesToConcurrentBoost pins that
// a removal whose read found a plain reaction, but whose write finds it
// boosted, is refused rather than deleting the boosted row: the delete is
// conditioned on the row carrying no boost, so the boost landing between the
// read and the transaction makes the removal lose, re-read, and judge the
// reaction permanent.
func TestMessaging_DynamoDBStore_RemoveReaction_LosesToConcurrentBoost(t *testing.T) {
	ctx := context.Background()
	require.NoError(t, CreateTables(ctx, testEnv.Client, messagesTable, pointersTable, reactionsTable, reactorsTable, selfReactionsTable, boostsTable))
	s := NewInDynamoDB(testEnv.Client, messagesTable, pointersTable, reactionsTable, reactorsTable, selfReactionsTable, boostsTable).(*store)
	defer s.reset()

	chatID := &commonpb.ChatId{Value: make([]byte, 16)}
	userA := model.MustGenerateUserID()
	ts := time.Unix(1_700_000_000, 0).UTC()

	clientID := &messagingpb.ClientMessageId{Value: make([]byte, messaging.ClientMessageIDSize)}
	msg, _, err := s.PutMessage(ctx, chatID, userA, nil, ts, clientID, true)
	require.NoError(t, err)
	msgID := msg.ID
	const emoji = "👍"

	_, created, _, err := s.AddReaction(ctx, chatID, msgID, userA, emoji, ts)
	require.NoError(t, err)
	require.True(t, created)

	// A's removal reads the reaction plain. Before its transaction commits,
	// A's own boost of it lands.
	var boosted bool
	s.beforeReactionWrite = func() {
		if boosted {
			return
		}
		boosted = true
		s.beforeReactionWrite = nil
		_, credited, created, err := s.AddBoost(ctx, chatID, msgID, userA, emoji, raceBoost(1_000_000, ts.Add(time.Second)))
		require.NoError(t, err)
		require.True(t, credited)
		require.False(t, created)
	}
	_, removed, err := s.RemoveReaction(ctx, chatID, msgID, userA, emoji)
	require.True(t, boosted)
	require.ErrorIs(t, err, messaging.ErrReactionBoosted)
	require.False(t, removed)

	// The boosted reaction stands, in every read.
	summary, err := s.GetReactionSummary(ctx, chatID, msgID)
	require.NoError(t, err)
	require.Len(t, summary, 1)
	require.EqualValues(t, 1, summary[0].Count)
	require.EqualValues(t, 1_000_000, summary[0].BoostUsdMicros)
	reactors, _, _, err := s.GetReactors(ctx, chatID, msgID, emoji)
	require.NoError(t, err)
	require.Len(t, reactors, 1)
	require.EqualValues(t, 1_000_000, reactors[0].BoostUsdMicros)
	self, err := s.GetSelfReactions(ctx, chatID, userA, []*messagingpb.MessageId{msgID})
	require.NoError(t, err)
	require.Len(t, self, 1)
	require.EqualValues(t, 1_000_000, self[0].BoostUsdMicros)
}

// TestMessaging_DynamoDBStore_AddBoost_ConcurrentBoostsSum pins that two
// boosts of one reaction under different intents, one landing between the
// other's read and write, both credit exactly once and the totals are their
// sum: the loser's reactor row condition and aggregate CAS fail, it re-reads
// the boosted state and adds to it.
func TestMessaging_DynamoDBStore_AddBoost_ConcurrentBoostsSum(t *testing.T) {
	ctx := context.Background()
	require.NoError(t, CreateTables(ctx, testEnv.Client, messagesTable, pointersTable, reactionsTable, reactorsTable, selfReactionsTable, boostsTable))
	s := NewInDynamoDB(testEnv.Client, messagesTable, pointersTable, reactionsTable, reactorsTable, selfReactionsTable, boostsTable).(*store)
	defer s.reset()

	chatID := &commonpb.ChatId{Value: make([]byte, 16)}
	userA := model.MustGenerateUserID()
	userB := model.MustGenerateUserID()
	ts := time.Unix(1_700_000_000, 0).UTC()

	clientID := &messagingpb.ClientMessageId{Value: make([]byte, messaging.ClientMessageIDSize)}
	msg, _, err := s.PutMessage(ctx, chatID, userA, nil, ts, clientID, true)
	require.NoError(t, err)
	msgID := msg.ID
	const emoji = "🔥"

	// Both of A's boosts read the reaction absent; the second lands first,
	// so the first's write finds the row already boosted and must retry on
	// top of it. B's plain reaction is there so the sample and count are
	// checked past the one booster.
	_, created, _, err := s.AddReaction(ctx, chatID, msgID, userB, emoji, ts)
	require.NoError(t, err)
	require.True(t, created)

	var landed bool
	s.beforeReactionWrite = func() {
		if landed {
			return
		}
		landed = true
		s.beforeReactionWrite = nil
		r, credited, created, err := s.AddBoost(ctx, chatID, msgID, userA, emoji, raceBoost(2_000_000, ts.Add(time.Second)))
		require.NoError(t, err)
		require.True(t, credited)
		require.True(t, created)
		require.EqualValues(t, 2_000_000, r.BoostUsdMicros)
	}
	r, credited, created, err := s.AddBoost(ctx, chatID, msgID, userA, emoji, raceBoost(3_000_000, ts.Add(2*time.Second)))
	require.NoError(t, err)
	require.True(t, landed)
	require.True(t, credited)
	require.False(t, created, "the reaction was added by the boost that landed first")
	require.EqualValues(t, 2, r.Count)
	require.EqualValues(t, 5_000_000, r.BoostUsdMicros)
	require.EqualValues(t, 5_000_000, r.Self.BoostUsdMicros)

	summary, err := s.GetReactionSummary(ctx, chatID, msgID)
	require.NoError(t, err)
	require.Len(t, summary, 1)
	require.EqualValues(t, 2, summary[0].Count)
	require.EqualValues(t, 5_000_000, summary[0].BoostUsdMicros)
	require.EqualValues(t, 3, summary[0].Version, "B's add and two boosts")
	reactors, _, _, err := s.GetReactors(ctx, chatID, msgID, emoji)
	require.NoError(t, err)
	require.Len(t, reactors, 2)
	require.Equal(t, userA.Value, reactors[0].UserID.Value)
	require.EqualValues(t, 5_000_000, reactors[0].BoostUsdMicros)
	require.Equal(t, userB.Value, reactors[1].UserID.Value)
	require.Zero(t, reactors[1].BoostUsdMicros)
}

// raceBoost is a boost worth usdMicros under a fresh intent ID.
func raceBoost(usdMicros uint64, ts time.Time) messaging.BoostInput {
	return messaging.BoostInput{
		IntentID:  &commonpb.IntentId{Value: model.MustGenerateKeyPair().Public()},
		Amount:    &commonpb.CryptoPaymentAmount{Quarks: usdMicros},
		UsdMicros: usdMicros,
		Ts:        ts,
	}
}
