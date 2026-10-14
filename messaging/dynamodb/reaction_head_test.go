//go:build integration

package dynamodb

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"
	messagingpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/messaging/v1"

	"github.com/code-payments/flipcash2-server/database"
	"github.com/code-payments/flipcash2-server/messaging"
	"github.com/code-payments/flipcash2-server/model"
)

// TestMessaging_DynamoDBStore_GetReactors_HeadSource pins where the boosted
// head of a reactor list is read from: the aggregate's sample, with no
// by_boost query, while the emoji has at most MaxStoredSampleReactors
// boosters (the sample holds them all, as its booster count attests), and
// the by_boost GSI once one more evicts a booster — with the same list and
// the same pages either way, since a client may cross that size mid-walk.
func TestMessaging_DynamoDBStore_GetReactors_HeadSource(t *testing.T) {
	ctx := context.Background()
	require.NoError(t, CreateTables(ctx, testEnv.Client, messagesTable, pointersTable, reactionsTable, reactorsTable, selfReactionsTable, boostsTable))
	s := NewInDynamoDB(testEnv.Client, messagesTable, pointersTable, reactionsTable, reactorsTable, selfReactionsTable, boostsTable).(*store)
	defer s.reset()

	chatID := &commonpb.ChatId{Value: make([]byte, 16)}
	sender := model.MustGenerateUserID()
	ts := time.Unix(1_700_000_000, 0).UTC()
	clientID := &messagingpb.ClientMessageId{Value: make([]byte, messaging.ClientMessageIDSize)}
	msg, _, err := s.PutMessage(ctx, chatID, sender, nil, ts, clientID, true)
	require.NoError(t, err)
	msgID := msg.ID
	const emoji = "🔥"

	queries := map[string]int{}
	s.onReactorQuery = func(index string) { queries[index]++ }
	readAll := func() []*messaging.Reactor {
		all, _, hasMore, err := s.GetReactors(ctx, chatID, msgID, emoji)
		require.NoError(t, err)
		require.False(t, hasMore)
		return all
	}
	walk := func(limit int) []*messaging.Reactor {
		var walked []*messaging.Reactor
		var token *commonpb.PagingToken
		for {
			opts := []database.QueryOption{database.WithLimit(limit)}
			if token != nil {
				opts = append(opts, database.WithPagingToken(token))
			}
			gsiBefore := queries[gsiByBoost]
			page, _, more, err := s.GetReactors(ctx, chatID, msgID, emoji, opts...)
			require.NoError(t, err)
			require.LessOrEqual(t, queries[gsiByBoost]-gsiBefore, 1, "a page is at most one key-range read of the index, never a chain")
			walked = append(walked, page...)
			if !more {
				return walked
			}
			require.Len(t, page, limit)
			token = messaging.ReactorPageToken(page[len(page)-1])
		}
	}
	ids := func(reactors []*messaging.Reactor) [][]byte {
		out := make([][]byte, len(reactors))
		for i, r := range reactors {
			out[i] = r.UserID.Value
		}
		return out
	}
	fireUsers := map[string]bool{}
	fireUser := func() *commonpb.UserId {
		u := model.MustGenerateUserID()
		fireUsers[string(u.Value)] = true
		return u
	}
	onlyFire := func(reactors []*messaging.Reactor) {
		for _, r := range reactors {
			require.True(t, fireUsers[string(r.UserID.Value)], "a reactor of another emoji on the list")
		}
	}
	sorted := func(reactors []*messaging.Reactor) bool {
		for i := 1; i < len(reactors); i++ {
			if !messaging.ReactorLess(reactors[i-1], reactors[i]) {
				return false
			}
		}
		return true
	}

	// Enough plain reactors to overflow the sample on their own, then as
	// many boosters as the sample can hold, each paying a different amount
	// so the order is not the version's.
	for i := 0; i < messaging.MaxStoredSampleReactors+4; i++ {
		_, _, _, err := s.AddReaction(ctx, chatID, msgID, fireUser(), emoji, ts.Add(time.Duration(i)*time.Second))
		require.NoError(t, err)
	}
	boosters := messaging.MaxStoredSampleReactors
	for i := 0; i < boosters; i++ {
		_, credited, _, err := s.AddBoost(ctx, chatID, msgID, fireUser(), emoji, raceBoost(uint64(1+(i*7)%boosters)*1_000_000, ts.Add(time.Hour+time.Duration(i)*time.Second)))
		require.NoError(t, err)
		require.True(t, credited)
	}
	// Boosters of another emoji on the same message, paying more, between
	// and less than the 🔥 ones, so a key range that let a neighbour in
	// would show them on the list or in the page count.
	for i, micros := range []uint64{40_000_000, 8_000_000, 8_500_000, 100_000} {
		_, credited, _, err := s.AddBoost(ctx, chatID, msgID, model.MustGenerateUserID(), "👍", raceBoost(micros, ts.Add(90*time.Minute+time.Duration(i)*time.Second)))
		require.NoError(t, err)
		require.True(t, credited)
	}

	// Under the size: the whole list and every page come off the sample and
	// the plain index alone, in list order, boosters first.
	all := readAll()
	require.Len(t, all, messaging.MaxStoredSampleReactors+4+boosters)
	require.True(t, sorted(all))
	onlyFire(all)
	// (i*7)%boosters is a permutation of 0..boosters-1, so the amounts are
	// 1..boosters million once each.
	require.EqualValues(t, boosters*(boosters+1)/2*1_000_000, func() (sum uint64) {
		for _, r := range all {
			sum += r.BoostUsdMicros
		}
		return
	}())
	require.Equal(t, ids(all), ids(walk(2)), "pages of two cross the head and its boundary")
	require.Equal(t, ids(all), ids(walk(7)))
	require.Zero(t, queries[gsiByBoost], "the sample is every booster; the GSI is not read")
	require.Positive(t, queries[lsiByVersion])

	// A head cursor resumes inside the sampled head, and the last booster's
	// cursor resumes at the first plain reactor.
	tail, _, _, err := s.GetReactors(ctx, chatID, msgID, emoji, database.WithPagingToken(messaging.ReactorPageToken(all[3])))
	require.NoError(t, err)
	require.Equal(t, ids(all[4:]), ids(tail))
	tail, _, _, err = s.GetReactors(ctx, chatID, msgID, emoji, database.WithPagingToken(messaging.ReactorPageToken(all[boosters-1])))
	require.NoError(t, err)
	require.Equal(t, ids(all[boosters:]), ids(tail))
	require.Zero(t, queries[gsiByBoost])

	// One booster more than the sample holds evicts the least of them, so
	// the sample no longer has every booster and from here the head is the
	// index's. The list is the same list, and a page token issued under the
	// sample resumes under the index at the same place.
	tokenUnderSample := messaging.ReactorPageToken(all[3])
	_, credited, _, err := s.AddBoost(ctx, chatID, msgID, fireUser(), emoji, raceBoost(500_000, ts.Add(2*time.Hour)))
	require.NoError(t, err)
	require.True(t, credited)
	gsiBefore := queries[gsiByBoost]
	all = readAll()
	require.Len(t, all, messaging.MaxStoredSampleReactors+4+boosters+1)
	require.True(t, sorted(all))
	onlyFire(all)
	require.Equal(t, 1, queries[gsiByBoost]-gsiBefore, "past MaxStoredSampleReactors boosters the head is read from the GSI, in one key range")
	require.EqualValues(t, 500_000, all[boosters].BoostUsdMicros, "the newest, smallest boost is the last of the head")
	walked := walk(2)
	require.Equal(t, ids(all), ids(walked))
	onlyFire(walked)
	resumed, _, _, err := s.GetReactors(ctx, chatID, msgID, emoji, database.WithPagingToken(tokenUnderSample))
	require.NoError(t, err)
	require.Equal(t, ids(all[4:]), ids(resumed))
}
