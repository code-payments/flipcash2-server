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

	"github.com/code-payments/flipcash2-server/chat"
	"github.com/code-payments/flipcash2-server/model"
)

// TestChat_ActivityScoreLegacyRecords pins how records no Store method can
// write are read and scored: one written before scores existed, with no
// activity_score, and one whose score trails its last send, as a writer that
// records sends without scoring them leaves behind during a rollout. Both
// read and score as if their score were their last send.
func TestChat_ActivityScoreLegacyRecords(t *testing.T) {
	ctx := context.Background()
	require.NoError(t, CreateTables(ctx, testEnv.Client, chatsTable, dmInboxTable, groupMembersTable, userStateTable, activityTable, keyEnvelopesTable, lobbiesTable, featuredGroupsTable))

	testStore := NewInDynamoDB(testEnv.Client, chatsTable, dmInboxTable, groupMembersTable, userStateTable, activityTable, keyEnvelopesTable, lobbiesTable, featuredGroupsTable, nil)
	defer testStore.(*store).reset()

	groupID := chat.MustGenerateGroupChatID()
	lastSentAt := time.Unix(1_700_000_000, 0).UTC()

	unscored := model.MustGenerateUserID()
	trailing := model.MustGenerateUserID()
	put := func(item map[string]types.AttributeValue) {
		_, err := testEnv.Client.PutItem(ctx, &dynamodb.PutItemInput{TableName: aws.String(activityTable), Item: item})
		require.NoError(t, err)
	}
	put(map[string]types.AttributeValue{
		attrPK:         avS(chatPK(groupID)),
		attrSK:         avS(userPK(unscored)),
		attrLastSentAt: avInt(lastSentAt.UnixMilli()),
	})
	put(map[string]types.AttributeValue{
		attrPK:            avS(chatPK(groupID)),
		attrSK:            avS(userPK(trailing)),
		attrLastSentAt:    avInt(lastSentAt.UnixMilli()),
		attrActivityScore: avInt(lastSentAt.Add(-time.Hour).UnixMilli()),
	})

	scores := func() map[string]time.Time {
		t.Helper()
		senders, err := testStore.GetRecentSenders(ctx, groupID, 0)
		require.NoError(t, err)
		out := make(map[string]time.Time, len(senders))
		for _, sender := range senders {
			out[string(sender.UserID.Value)] = sender.ActivityScore
		}
		return out
	}

	// Read as scoring their last send.
	got := scores()
	require.Len(t, got, 2)
	require.True(t, got[string(unscored.Value)].Equal(lastSentAt))
	require.True(t, got[string(trailing.Value)].Equal(lastSentAt))

	// The throttle still holds against them.
	recorded, err := testStore.RecordSend(ctx, groupID, unscored, lastSentAt.Add(chat.ActivityRecordInterval/2))
	require.NoError(t, err)
	require.False(t, recorded)

	// And their next send is scored against their last.
	sentAt := lastSentAt.Add(time.Hour)
	want := chat.NextActivityScore(lastSentAt, lastSentAt, sentAt)
	for _, user := range []*commonpb.UserId{unscored, trailing} {
		recorded, err := testStore.RecordSend(ctx, groupID, user, sentAt)
		require.NoError(t, err)
		require.True(t, recorded)
	}
	got = scores()
	require.True(t, got[string(unscored.Value)].Equal(want), "got %v, want %v", got[string(unscored.Value)], want)
	require.True(t, got[string(trailing.Value)].Equal(want), "got %v, want %v", got[string(trailing.Value)], want)

	// Now in the score index too.
	out, err := testEnv.Client.Query(ctx, &dynamodb.QueryInput{
		TableName:                 aws.String(activityTable),
		IndexName:                 aws.String(lsiByActivityScore),
		KeyConditionExpression:    aws.String("#pk = :pk"),
		ExpressionAttributeNames:  map[string]string{"#pk": attrPK},
		ExpressionAttributeValues: map[string]types.AttributeValue{":pk": avS(chatPK(groupID))},
		ConsistentRead:            aws.Bool(true),
	})
	require.NoError(t, err)
	require.Len(t, out.Items, 2)
}
