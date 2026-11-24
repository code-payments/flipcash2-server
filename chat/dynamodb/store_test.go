//go:build integration

package dynamodb

import (
	"context"
	"crypto/rand"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/smithy-go/middleware"
	"github.com/stretchr/testify/require"

	chatpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/chat/v1"
	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"
	messagingpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/messaging/v1"

	"github.com/code-payments/flipcash2-server/chat"
	"github.com/code-payments/flipcash2-server/chat/tests"
	"github.com/code-payments/flipcash2-server/model"
)

const (
	chatsTable          = "chats_test"
	dmInboxTable        = "dm_inbox_test"
	groupMembersTable   = "group_members_test"
	userStateTable      = "chat_user_state_test"
	activityTable       = "chat_activity_test"
	keyEnvelopesTable   = "chat_key_envelopes_test"
	lobbiesTable        = "chat_lobbies_test"
	featuredGroupsTable = "chat_featured_groups_test"
	holdingsTable       = "chat_holdings_test"
	invitesTable        = "chat_invites_test"
	pinsTable           = "chat_pins_test"
)

func TestChat_DynamoDBStore(t *testing.T) {
	require.NoError(t, CreateTables(context.Background(), testEnv.Client, chatsTable, dmInboxTable, groupMembersTable, userStateTable, activityTable, keyEnvelopesTable, lobbiesTable, featuredGroupsTable, holdingsTable, invitesTable, pinsTable))

	testStore := NewInDynamoDB(testEnv.Client, chatsTable, dmInboxTable, groupMembersTable, userStateTable, activityTable, keyEnvelopesTable, lobbiesTable, featuredGroupsTable, holdingsTable, invitesTable, pinsTable, nil)
	teardown := func() {
		testStore.(*store).reset()
	}
	// The stores newStore builds share testStore's tables, so its teardown
	// resets theirs too.
	newStore := func(excludedFromFeed []*commonpb.UserId) chat.Store {
		return NewInDynamoDB(testEnv.Client, chatsTable, dmInboxTable, groupMembersTable, userStateTable, activityTable, keyEnvelopesTable, lobbiesTable, featuredGroupsTable, holdingsTable, invitesTable, pinsTable, excludedFromFeed)
	}
	tests.RunStoreTests(t, testStore, newStore, teardown)
}

// TestChat_DynamoDBExclusionOutlivesConfig checks that a DM's exclusion from
// the feed is kept with the DM, not read from the store's configuration: a
// process built excluding no one, sharing the tables, advances a DM another
// created excluding someone, leaves the excluded member's row alone, and moves
// the other's.
func TestChat_DynamoDBExclusionOutlivesConfig(t *testing.T) {
	ctx := context.Background()
	require.NoError(t, CreateTables(ctx, testEnv.Client, chatsTable, dmInboxTable, groupMembersTable, userStateTable, activityTable, keyEnvelopesTable, lobbiesTable, featuredGroupsTable, holdingsTable, invitesTable, pinsTable))

	team := model.MustGenerateUserID()
	configured := NewInDynamoDB(testEnv.Client, chatsTable, dmInboxTable, groupMembersTable, userStateTable, activityTable, keyEnvelopesTable, lobbiesTable, featuredGroupsTable, holdingsTable, invitesTable, pinsTable, []*commonpb.UserId{team})
	unconfigured := NewInDynamoDB(testEnv.Client, chatsTable, dmInboxTable, groupMembersTable, userStateTable, activityTable, keyEnvelopesTable, lobbiesTable, featuredGroupsTable, holdingsTable, invitesTable, pinsTable, nil)
	t.Cleanup(func() { configured.(*store).reset() })

	chatIDValue := make([]byte, chat.DmChatIDSize)
	_, err := rand.Read(chatIDValue)
	require.NoError(t, err)
	chatID := &commonpb.ChatId{Value: chatIDValue}
	user := model.MustGenerateUserID()
	created := time.Unix(1_700_000_100, 0).UTC()
	require.NoError(t, configured.PutChat(ctx, &chat.Chat{
		ID:           chatID,
		Type:         chatpb.ChatType_DM,
		Members:      []*commonpb.UserId{user, team},
		LastActivity: created,
	}))

	advancedTo := created.Add(time.Minute)
	advanced, _, err := unconfigured.AdvanceLastMessage(ctx, chatID, &messagingpb.MessageId{Value: 1}, advancedTo)
	require.NoError(t, err)
	require.True(t, advanced)

	snapshot := advancedTo.Add(time.Hour)
	userFeed, err := unconfigured.GetDmFeedPage(ctx, user, chatpb.ChatType_DM, chat.FolderInbox, snapshot, nil, 0)
	require.NoError(t, err)
	require.Len(t, userFeed, 1)
	require.True(t, userFeed[0].LastActivity.Equal(advancedTo))
	teamFeed, err := unconfigured.GetDmFeedPage(ctx, team, chatpb.ChatType_DM, chat.FolderInbox, snapshot, nil, 0)
	require.NoError(t, err)
	require.Empty(t, teamFeed)
}

// TestChat_DynamoDBAdvanceRetriesConflict checks that a DM's advance whose
// transaction loses to a concurrent one on any of its items, as an archive
// writing a member's row can make it (see SetArchived), is retried rather
// than taken for a no-op, and that one which keeps losing fails rather than
// reports that a newer advance landed. DynamoDB Local serializes
// transactions, so the conflicts are injected into the client the store
// sends through.
func TestChat_DynamoDBAdvanceRetriesConflict(t *testing.T) {
	ctx := context.Background()
	require.NoError(t, CreateTables(ctx, testEnv.Client, chatsTable, dmInboxTable, groupMembersTable, userStateTable, activityTable, keyEnvelopesTable, lobbiesTable, featuredGroupsTable, holdingsTable, invitesTable, pinsTable))

	// conflicts is how many of the next transactions fail as having lost to
	// a concurrent transaction on their second item, a member's row.
	var conflicts atomic.Int64
	client := interceptTransactions(func(_ context.Context, input *dynamodb.TransactWriteItemsInput) error {
		if conflicts.Add(-1) < 0 {
			conflicts.Store(0)
			return nil
		}
		return cancelledTransaction(len(input.TransactItems), map[int]string{1: codeTransactionConflict})
	})
	s := NewInDynamoDB(client, chatsTable, dmInboxTable, groupMembersTable, userStateTable, activityTable, keyEnvelopesTable, lobbiesTable, featuredGroupsTable, holdingsTable, invitesTable, pinsTable, nil)
	t.Cleanup(func() { s.(*store).reset() })

	chatIDValue := make([]byte, chat.DmChatIDSize)
	_, err := rand.Read(chatIDValue)
	require.NoError(t, err)
	chatID := &commonpb.ChatId{Value: chatIDValue}
	user, other := model.MustGenerateUserID(), model.MustGenerateUserID()
	created := time.Unix(1_700_000_100, 0).UTC()
	require.NoError(t, s.PutChat(ctx, &chat.Chat{
		ID:           chatID,
		Type:         chatpb.ChatType_DM,
		Members:      []*commonpb.UserId{user, other},
		LastActivity: created,
	}))

	requireActivity := func(want time.Time, wantMessage uint64) {
		t.Helper()
		stored, err := s.GetChatByID(ctx, chatID)
		require.NoError(t, err)
		require.True(t, stored.LastActivity.Equal(want))
		require.Equal(t, wantMessage, stored.LastMessageID.GetValue())
		for _, member := range []*commonpb.UserId{user, other} {
			feed, err := s.GetDmFeedPage(ctx, member, chatpb.ChatType_DM, chat.FolderInbox, want.Add(time.Hour), nil, 0)
			require.NoError(t, err)
			require.Len(t, feed, 1)
			require.True(t, feed[0].LastActivity.Equal(want))
		}
	}

	// Two lost transactions, then the advance lands.
	conflicts.Store(2)
	advancedTo := created.Add(time.Minute)
	advanced, members, err := s.AdvanceLastMessage(ctx, chatID, &messagingpb.MessageId{Value: 1}, advancedTo)
	require.NoError(t, err)
	require.True(t, advanced)
	require.Len(t, members, 2)
	require.Zero(t, conflicts.Load())
	requireActivity(advancedTo, 1)

	// Losing every attempt fails the advance, and nothing moves.
	conflicts.Store(maxTransactAttempts)
	advanced, _, err = s.AdvanceLastMessage(ctx, chatID, &messagingpb.MessageId{Value: 2}, advancedTo.Add(time.Minute))
	require.Error(t, err)
	require.False(t, advanced)
	requireActivity(advancedTo, 1)
}

// TestChat_DynamoDBUnarchiveConflictAfterDeparture checks that an unarchive
// whose transaction comes back with a conflict on the user's record and a
// refusal from their feed row is not taken for a non-member's refusal: the
// record was not judged, and the departure that landed meanwhile took the
// archive, so the unarchive is the no-op. DynamoDB Local serializes
// transactions, so the departure is made, and the cancellation injected,
// as the unarchive's transaction is sent.
func TestChat_DynamoDBUnarchiveConflictAfterDeparture(t *testing.T) {
	ctx := context.Background()
	require.NoError(t, CreateTables(ctx, testEnv.Client, chatsTable, dmInboxTable, groupMembersTable, userStateTable, activityTable, keyEnvelopesTable, lobbiesTable, featuredGroupsTable, holdingsTable, invitesTable, pinsTable))
	direct := NewInDynamoDB(testEnv.Client, chatsTable, dmInboxTable, groupMembersTable, userStateTable, activityTable, keyEnvelopesTable, lobbiesTable, featuredGroupsTable, holdingsTable, invitesTable, pinsTable, nil)
	t.Cleanup(func() { direct.(*store).reset() })

	creator, user := model.MustGenerateUserID(), model.MustGenerateUserID()
	group := &chat.Chat{
		ID:           chat.MustGenerateGroupChatID(),
		Type:         chatpb.ChatType_GROUP,
		Members:      []*commonpb.UserId{creator, user},
		Title:        "Group",
		LastActivity: time.Unix(1_700_000_100, 0).UTC(),
	}
	require.NoError(t, direct.PutChat(ctx, group))
	_, changed, err := direct.SetArchived(ctx, group, user, true, time.Unix(1_700_000_200, 0))
	require.NoError(t, err)
	require.True(t, changed)

	var armed atomic.Bool
	armed.Store(true)
	s := NewInDynamoDB(interceptTransactions(func(ctx context.Context, input *dynamodb.TransactWriteItemsInput) error {
		if !armed.CompareAndSwap(true, false) {
			return nil
		}
		changed, _, err := direct.RemoveGroupMember(ctx, group.ID, user, false)
		if err != nil || !changed {
			return fmt.Errorf("departing: changed %v, %w", changed, err)
		}
		return cancelledTransaction(len(input.TransactItems), map[int]string{0: codeTransactionConflict, 1: conditionalCheckFailedCode})
	}), chatsTable, dmInboxTable, groupMembersTable, userStateTable, activityTable, keyEnvelopesTable, lobbiesTable, featuredGroupsTable, holdingsTable, invitesTable, pinsTable, nil)

	state, changed, err := s.SetArchived(ctx, group, user, false, time.Unix(1_700_000_300, 0))
	require.NoError(t, err)
	require.False(t, changed)
	require.False(t, armed.Load())
	require.Equal(t, chat.ViewerState{Version: 2}, state)
}

// interceptTransactions is a client of the test environment that hands each
// TransactWriteItems to intercept before sending it: a nil return sends it,
// and any other error is returned in its place, unsent.
func interceptTransactions(intercept func(ctx context.Context, input *dynamodb.TransactWriteItemsInput) error) *dynamodb.Client {
	return dynamodb.New(testEnv.Client.Options(), func(o *dynamodb.Options) {
		o.APIOptions = append(o.APIOptions, func(stack *middleware.Stack) error {
			return stack.Initialize.Add(middleware.InitializeMiddlewareFunc("interceptTransactions", func(ctx context.Context, in middleware.InitializeInput, next middleware.InitializeHandler) (middleware.InitializeOutput, middleware.Metadata, error) {
				if input, ok := in.Parameters.(*dynamodb.TransactWriteItemsInput); ok {
					if err := intercept(ctx, input); err != nil {
						return middleware.InitializeOutput{}, middleware.Metadata{}, err
					}
				}
				return next.HandleInitialize(ctx, in)
			}), middleware.Before)
		})
	})
}

// cancelledTransaction is the cancellation DynamoDB returns for a
// transaction of n items, with codes giving the reason of each item that
// has one; the rest are None.
func cancelledTransaction(n int, codes map[int]string) error {
	reasons := make([]types.CancellationReason, n)
	for i := range reasons {
		reasons[i].Code = aws.String("None")
		if code, ok := codes[i]; ok {
			reasons[i].Code = aws.String(code)
		}
	}
	return &types.TransactionCanceledException{
		Message:             aws.String("injected cancellation"),
		CancellationReasons: reasons,
	}
}
