//go:build integration

package dynamodb

import (
	"context"
	"encoding/hex"
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
	require.NoError(t, CreateTables(ctx, testEnv.Client, messagesTable, pointersTable, reactionsTable, reactorsTable, selfReactionsTable, boostsTable))
	s := NewInDynamoDB(testEnv.Client, messagesTable, pointersTable, reactionsTable, reactorsTable, selfReactionsTable, boostsTable).(*store)
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

// TestMessaging_DynamoDBStore_BoostLayout pins the per-table writes of a boost
// the shared suite cannot see: the reactor row leaves the by_version LSI for
// the by_boost GSI (emoji_version removed, emoji_boost and reactor_version
// set), the aggregate row carries the total and the booster's sample entry
// its own, the boost row lands in message_reaction_boosts under the intent, and a
// group's self row carries the total. A row the boost creates is laid out the
// same as one it converts.
func TestMessaging_DynamoDBStore_BoostLayout(t *testing.T) {
	ctx := context.Background()
	require.NoError(t, CreateTables(ctx, testEnv.Client, messagesTable, pointersTable, reactionsTable, reactorsTable, selfReactionsTable, boostsTable))
	s := NewInDynamoDB(testEnv.Client, messagesTable, pointersTable, reactionsTable, reactorsTable, selfReactionsTable, boostsTable).(*store)
	defer s.reset()

	chatID := &commonpb.ChatId{Value: make([]byte, chat.GroupChatIDSize)}
	converted := model.MustGenerateUserID() // reacts, then boosts
	fresh := model.MustGenerateUserID()     // boosts without reacting
	ts := time.Unix(1_700_000_000, 0).UTC()
	clientID := &messagingpb.ClientMessageId{Value: make([]byte, messaging.ClientMessageIDSize)}
	msg, _, err := s.PutMessage(ctx, chatID, converted, nil, ts, clientID, true)
	require.NoError(t, err)
	seq := msg.ID.Value
	const emoji = "🔥"

	getItem := func(table, pk, sk string) map[string]types.AttributeValue {
		out, err := testEnv.Client.GetItem(ctx, &dynamodb.GetItemInput{
			TableName:      aws.String(table),
			Key:            map[string]types.AttributeValue{attrPK: avS(pk), attrSK: avS(sk)},
			ConsistentRead: aws.Bool(true),
		})
		require.NoError(t, err)
		return out.Item
	}

	// A plain reaction: in the version LSI, nothing boost-shaped on it.
	_, created, _, err := s.AddReaction(ctx, chatID, msg.ID, converted, emoji, ts)
	require.NoError(t, err)
	require.True(t, created)
	row := getItem(reactorsTable, reactorPK(chatID, seq), userSK(converted, emoji))
	require.Equal(t, avS(emojiVersionKey(emoji, 1)), row[attrEmojiVersion])
	require.NotContains(t, row, attrEmojiBoost)
	require.NotContains(t, row, attrBoostMicros)
	require.NotContains(t, row, attrReactorVer)

	// Boosting it moves the row to the boost GSI with the add's version kept.
	intentA := &commonpb.IntentId{Value: model.MustGenerateKeyPair().Public()}
	r, credited, created, err := s.AddBoost(ctx, chatID, msg.ID, converted, emoji, messaging.BoostInput{IntentID: intentA, Amount: &commonpb.CryptoPaymentAmount{Quarks: 2_000_000}, UsdMicros: 2_000_000, Ts: ts.Add(time.Minute)})
	require.NoError(t, err)
	require.True(t, credited)
	require.False(t, created)
	require.EqualValues(t, 2, r.Version)
	row = getItem(reactorsTable, reactorPK(chatID, seq), userSK(converted, emoji))
	require.NotContains(t, row, attrEmojiVersion, "a boosted row leaves the version LSI")
	require.Equal(t, avS(emojiBoost(emoji, 2_000_000, 1)), row[attrEmojiBoost])
	require.Equal(t, avN(1), row[attrReactorVer], "the add's version, not the boost's")
	require.Equal(t, avN(2_000_000), row[attrBoostMicros])
	require.NotContains(t, row, attrEmoji, "the emoji leads emoji_boost; nothing projects it")
	require.Equal(t, avN(uint64(ts.UnixNano())), row[attrReactedTs], "the add's time, not the boost's")

	// A boost that creates the reaction lays the row out the same way, under
	// the version the boost produced.
	intentB := &commonpb.IntentId{Value: model.MustGenerateKeyPair().Public()}
	r, credited, created, err = s.AddBoost(ctx, chatID, msg.ID, fresh, emoji, messaging.BoostInput{IntentID: intentB, Amount: &commonpb.CryptoPaymentAmount{Quarks: 500_000}, UsdMicros: 500_000, Ts: ts.Add(2 * time.Minute)})
	require.NoError(t, err)
	require.True(t, credited)
	require.True(t, created)
	require.EqualValues(t, 3, r.Version)
	row = getItem(reactorsTable, reactorPK(chatID, seq), userSK(fresh, emoji))
	require.NotContains(t, row, attrEmojiVersion)
	require.Equal(t, avS(emojiBoost(emoji, 500_000, 3)), row[attrEmojiBoost])
	require.Equal(t, avN(3), row[attrReactorVer])
	require.Equal(t, avN(uint64(ts.Add(2*time.Minute).UnixNano())), row[attrReactedTs])

	// The aggregate carries the emoji's total and each sample entry its own.
	agg := getItem(reactionsTable, chatPK(chatID), aggSK(seq, emoji))
	require.Equal(t, avN(2_500_000), agg[attrBoostMicros])
	require.Equal(t, avN(2), agg[attrReactorCount])
	require.Equal(t, avN(2), agg[attrBoosterCount], "both reactors are boosters")
	sample := parseSampleMap(agg[attrSample])
	require.EqualValues(t, 2_000_000, sample[hex.EncodeToString(converted.Value)].boost)
	require.EqualValues(t, 500_000, sample[hex.EncodeToString(fresh.Value)].boost)

	// One boost row per payment, under the message and the intent, carrying
	// the version it produced and the index key built from it.
	boostRow := getItem(boostsTable, reactorPK(chatID, seq), boostSK(intentA))
	require.NotEmpty(t, boostRow)
	require.Equal(t, avN(2_000_000), boostRow[attrBoostMicros])
	require.Equal(t, avS(emoji), boostRow[attrEmoji])
	require.Equal(t, avN(2), boostRow[attrReactionVersion])
	require.Equal(t, avS(emojiVersionKey(emoji, 2)), boostRow[attrBoostEmojiVer])
	require.Equal(t, &types.AttributeValueMemberB{Value: converted.Value}, boostRow[attrBoostUser])
	require.NotEmpty(t, getItem(boostsTable, reactorPK(chatID, seq), boostSK(intentB)))

	// The group's self rows carry each booster's total.
	self := getItem(selfReactionsTable, viewerPK(chatID, converted), seqEmojiSK(seq, emoji))
	require.Equal(t, avN(2_000_000), self[attrBoostMicros])
	require.Equal(t, avN(1), self[attrSelfVersion])

	// A replay of a payment writes nothing anywhere: the version stands.
	_, credited, _, err = s.AddBoost(ctx, chatID, msg.ID, converted, emoji, messaging.BoostInput{IntentID: intentA, Amount: &commonpb.CryptoPaymentAmount{Quarks: 2_000_000}, UsdMicros: 2_000_000, Ts: ts})
	require.NoError(t, err)
	require.False(t, credited)
	agg = getItem(reactionsTable, chatPK(chatID), aggSK(seq, emoji))
	require.Equal(t, avN(3), agg[attrReactionVersion])
	require.Equal(t, avN(2_500_000), agg[attrBoostMicros])
	require.Equal(t, avN(2), agg[attrBoosterCount])
}
