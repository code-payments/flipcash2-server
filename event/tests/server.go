package tests

import (
	"context"
	"io"
	"maps"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	chatpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/chat/v1"
	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"
	eventpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/event/v1"
	messagingpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/messaging/v1"
	profilepb "github.com/code-payments/flipcash2-protobuf-api/generated/go/profile/v1"
	ocp_balancepb "github.com/code-payments/ocp-protobuf-api/generated/go/balance/v1"

	"github.com/code-payments/flipcash2-server/account"
	"github.com/code-payments/flipcash2-server/auth"
	badgememory "github.com/code-payments/flipcash2-server/badge/memory"
	"github.com/code-payments/flipcash2-server/balance"
	"github.com/code-payments/flipcash2-server/chat"
	chat_memory "github.com/code-payments/flipcash2-server/chat/memory"
	"github.com/code-payments/flipcash2-server/cluster"
	cluster_memory "github.com/code-payments/flipcash2-server/cluster/memory"
	"github.com/code-payments/flipcash2-server/event"
	"github.com/code-payments/flipcash2-server/model"
	"github.com/code-payments/flipcash2-server/protoutil"
	ocp_testutil "github.com/code-payments/ocp-server/testutil"
)

func RunServerTests(t *testing.T, accounts account.Store, teardown func()) {
	for _, tf := range []func(t *testing.T, accounts account.Store){
		testSingleServerHappyPath,
		testMultiServerHappyPath,
		testMultipleOpenStreams,
		testBurstDelivery,
		testKeepAlive,
		testSubscriptionRegistration,
		testGroupSubscriptionRegistration,
		testChatEventPublishing,
		testMembershipFollowsStreams,
		testMembershipReconciles,
		testChatPreview,
		testChatPreviewExpires,
		testServerShutdown,
	} {
		tf(t, accounts)
		teardown()
	}
}

const internalRpcApiKey = "valid-api-key"

func testSingleServerHappyPath(t *testing.T, accounts account.Store) {
	testEnv, cleanup := setupTest(t, accounts, false)
	defer cleanup()

	userID := model.MustGenerateUserID()
	keyPair := model.MustGenerateKeyPair()
	accounts.Bind(context.Background(), userID, keyPair.Proto())
	accounts.SetRegistrationFlag(context.Background(), userID, true)

	testEnv.client1.openUserEventStream(t, userID, keyPair)

	time.Sleep(500 * time.Millisecond)

	for range 100 {
		expected := testEnv.server1.sendTestUserEvent(userID)

		allActual := testEnv.client1.receiveEventsInRealTime(t, userID)

		require.Len(t, allActual, 1)
		assertEquivalentTestEvents(t, expected, allActual[0])
	}
}

func testMultiServerHappyPath(t *testing.T, accounts account.Store) {
	testEnv, cleanup := setupTest(t, accounts, true)
	defer cleanup()

	userID := model.MustGenerateUserID()
	keyPair := model.MustGenerateKeyPair()
	accounts.Bind(context.Background(), userID, keyPair.Proto())
	accounts.SetRegistrationFlag(context.Background(), userID, true)

	testEnv.client1.openUserEventStream(t, userID, keyPair)

	time.Sleep(500 * time.Millisecond)

	for i := range 100 {
		sender := testEnv.server1
		if i%2 == 0 {
			sender = testEnv.server2
		}

		expected := sender.sendTestUserEvent(userID)

		allActual := testEnv.client1.receiveEventsInRealTime(t, userID)
		require.Len(t, allActual, 1)
		assertEquivalentTestEvents(t, expected, allActual[0])
	}
}

// testMultipleOpenStreams pins the multi-device contract: any number of
// streams may be open for the same user — on one server or across several —
// and every one of them receives every event.
func testMultipleOpenStreams(t *testing.T, accounts account.Store) {
	testEnv, cleanup := setupTest(t, accounts, true)
	defer cleanup()

	userID := model.MustGenerateUserID()
	keyPair := model.MustGenerateKeyPair()
	accounts.Bind(context.Background(), userID, keyPair.Proto())
	accounts.SetRegistrationFlag(context.Background(), userID, true)

	for range 3 {
		testEnv.client1.openUserEventStream(t, userID, keyPair)
		testEnv.client2.openUserEventStream(t, userID, keyPair)
	}

	time.Sleep(500 * time.Millisecond)

	for i := range 20 {
		sender := testEnv.server1
		if i%2 == 0 {
			sender = testEnv.server2
		}

		expected := sender.sendTestUserEvent(userID)

		for _, client := range []*clientTestEnv{testEnv.client1, testEnv.client2} {
			for _, streamer := range client.streams[model.UserIDString(userID)] {
				events := receiveNextEvents(t, streamer)
				require.Lenf(t, events, 1, "expected[%d]: %s", i, model.EventIDString(expected.Id))
				assertEquivalentTestEvents(t, expected, events[0])
			}
		}
	}
}

// testBurstDelivery pins delivery under a burst: events published faster than
// the client reads them all arrive, each exactly once, coalesced into however
// many batches the handler needed rather than one message per event. The
// burst stays well under the stream buffer so a slow test client cannot trip
// the lag close that a larger one would.
func testBurstDelivery(t *testing.T, accounts account.Store) {
	testEnv, cleanup := setupTest(t, accounts, true)
	defer cleanup()

	userID := model.MustGenerateUserID()
	keyPair := model.MustGenerateKeyPair()
	accounts.Bind(context.Background(), userID, keyPair.Proto())
	accounts.SetRegistrationFlag(context.Background(), userID, true)

	testEnv.client1.openUserEventStream(t, userID, keyPair)

	time.Sleep(500 * time.Millisecond)

	const burst = 50
	expected := make(map[string]*eventpb.Event, burst)
	for i := range burst {
		sender := testEnv.server1
		if i%2 == 0 {
			sender = testEnv.server2
		}
		e := sender.sendTestUserEvent(userID)
		expected[model.EventIDString(e.Id)] = e
	}

	// The bus hands every publish to its own goroutine, so arrival order is
	// not part of the contract; the set is.
	got := make(map[string]*eventpb.Event, burst)
	var batches int
	for len(got) < burst {
		batch := testEnv.client1.receiveEventsInRealTime(t, userID)
		require.NotEmpty(t, batch)
		require.LessOrEqual(t, len(batch), burst)
		batches++
		for _, e := range batch {
			id := model.EventIDString(e.Id)
			_, dup := got[id]
			require.Falsef(t, dup, "event %s delivered twice", id)
			require.Containsf(t, expected, id, "unexpected event %s", id)
			assertEquivalentTestEvents(t, expected[id], e)
			got[id] = e
		}
	}
	require.Len(t, got, burst)
	require.LessOrEqual(t, batches, burst)
	t.Logf("received %d events in %d batches", burst, batches)
}

func testKeepAlive(t *testing.T, accounts account.Store) {
	testEnv, cleanup := setupTest(t, accounts, false)
	defer cleanup()

	userID := model.MustGenerateUserID()
	keyPair := model.MustGenerateKeyPair()
	accounts.Bind(context.Background(), userID, keyPair.Proto())
	accounts.SetRegistrationFlag(context.Background(), userID, true)

	testEnv.client1.openUserEventStream(t, userID, keyPair)

	pingCount := testEnv.client1.waitUntilStreamTerminationOrTimeout(t, userID, true, 30*time.Second)
	require.True(t, pingCount >= 5)

	pingCount = testEnv.client1.waitUntilStreamTerminationOrTimeout(t, userID, false, 30*time.Second)
	require.True(t, pingCount <= 2)
}

// testSubscriptionRegistration pins the registry lifecycle: a user's streams
// on one server share a single subscription row, which appears with the first
// stream and disappears only after the last one closes.
func testSubscriptionRegistration(t *testing.T, accounts account.Store) {
	testEnv, cleanup := setupTest(t, accounts, false)
	defer cleanup()

	ctx := context.Background()

	userID := model.MustGenerateUserID()
	keyPair := model.MustGenerateKeyPair()
	accounts.Bind(ctx, userID, keyPair.Proto())
	accounts.SetRegistrationFlag(ctx, userID, true)

	testEnv.client1.openUserEventStream(t, userID, keyPair)
	testEnv.client1.openUserEventStream(t, userID, keyPair)

	require.Eventually(t, func() bool {
		rows, err := testEnv.clusterStore.GetSubscribers(ctx, event.UserEventsNamespace, userID.Value)
		require.NoError(t, err)
		return len(rows) == 1 && rows[0].InstanceID == testEnv.server1.membership.Self().InstanceID
	}, 5*time.Second, 10*time.Millisecond)

	// Closing one of the two streams must keep the shared row alive.
	testEnv.client1.closeOneUserEventStream(t, userID)
	time.Sleep(500 * time.Millisecond)
	rows, err := testEnv.clusterStore.GetSubscribers(ctx, event.UserEventsNamespace, userID.Value)
	require.NoError(t, err)
	require.Len(t, rows, 1)

	// Closing the last stream removes it.
	testEnv.client1.closeOneUserEventStream(t, userID)
	require.Eventually(t, func() bool {
		rows, err := testEnv.clusterStore.GetSubscribers(ctx, event.UserEventsNamespace, userID.Value)
		require.NoError(t, err)
		return len(rows) == 0
	}, 5*time.Second, 10*time.Millisecond)
}

// testGroupSubscriptionRegistration pins the group topic lifecycle: a stream
// registers one subscription row per group chat its user is joined to, streams
// on one server share those rows, the rows disappear with the last stream, and
// a group the user is not a member of is never registered.
func testGroupSubscriptionRegistration(t *testing.T, accounts account.Store) {
	testEnv, cleanup := setupTest(t, accounts, false)
	defer cleanup()

	ctx := context.Background()

	userID := model.MustGenerateUserID()
	keyPair := model.MustGenerateKeyPair()
	accounts.Bind(ctx, userID, keyPair.Proto())
	accounts.SetRegistrationFlag(ctx, userID, true)

	groupA := putGroupChat(t, testEnv.chats, userID)
	groupB := putGroupChat(t, testEnv.chats, userID)
	groupOther := putGroupChat(t, testEnv.chats, model.MustGenerateUserID())

	testEnv.client1.openUserEventStream(t, userID, keyPair)
	testEnv.client1.openUserEventStream(t, userID, keyPair)

	// Both streams share a single row per group topic.
	for _, chatID := range []*commonpb.ChatId{groupA, groupB} {
		require.Eventually(t, func() bool {
			rows, err := testEnv.clusterStore.GetSubscribers(ctx, event.ChatEventsNamespace, chatID.Value)
			require.NoError(t, err)
			return len(rows) == 1 && rows[0].InstanceID == testEnv.server1.membership.Self().InstanceID
		}, 5*time.Second, 10*time.Millisecond)
	}

	// A group the user is not joined to gets no registration.
	rows, err := testEnv.clusterStore.GetSubscribers(ctx, event.ChatEventsNamespace, groupOther.Value)
	require.NoError(t, err)
	require.Empty(t, rows)

	// Closing one of the two streams keeps the shared rows alive.
	testEnv.client1.closeOneUserEventStream(t, userID)
	time.Sleep(500 * time.Millisecond)
	for _, chatID := range []*commonpb.ChatId{groupA, groupB} {
		rows, err := testEnv.clusterStore.GetSubscribers(ctx, event.ChatEventsNamespace, chatID.Value)
		require.NoError(t, err)
		require.Len(t, rows, 1)
	}

	// The last close removes the group rows along with the user's.
	testEnv.client1.closeOneUserEventStream(t, userID)
	require.Eventually(t, func() bool {
		rows, err := testEnv.clusterStore.GetSubscribers(ctx, event.UserEventsNamespace, userID.Value)
		require.NoError(t, err)
		if len(rows) != 0 {
			return false
		}
		for _, chatID := range []*commonpb.ChatId{groupA, groupB} {
			rows, err := testEnv.clusterStore.GetSubscribers(ctx, event.ChatEventsNamespace, chatID.Value)
			require.NoError(t, err)
			if len(rows) != 0 {
				return false
			}
		}
		return true
	}, 5*time.Second, 10*time.Millisecond)
}

// testChatEventPublishing pins the chat-keyed publish path end to end: one
// publish on a server's chat bus reaches every subscribed stream across the
// fleet — delivered locally on the publishing server and over the forwarding
// RPC to the rest — honoring exclusions on both paths, including selectively
// among several streams sharing one server's chat topic.
func testChatEventPublishing(t *testing.T, accounts account.Store) {
	testEnv, cleanup := setupTest(t, accounts, true)
	defer cleanup()

	ctx := context.Background()

	userA := model.MustGenerateUserID()
	keyPairA := model.MustGenerateKeyPair()
	accounts.Bind(ctx, userA, keyPairA.Proto())
	accounts.SetRegistrationFlag(ctx, userA, true)

	userB := model.MustGenerateUserID()
	keyPairB := model.MustGenerateKeyPair()
	accounts.Bind(ctx, userB, keyPairB.Proto())
	accounts.SetRegistrationFlag(ctx, userB, true)

	group := putGroupChat(t, testEnv.chats, userA, userB)

	// The members stream on different servers, so every publish exercises the
	// local short-circuit for one and the forwarding RPC for the other — and
	// userB also streams alongside userA on server1, so exclusion must filter
	// selectively among streams sharing one server's chat topic.
	testEnv.client1.openUserEventStream(t, userA, keyPairA)
	testEnv.client1.openUserEventStream(t, userB, keyPairB)
	testEnv.client2.openUserEventStream(t, userB, keyPairB)

	time.Sleep(500 * time.Millisecond)

	publish := func(sender *serverTestEnv, exclude ...*commonpb.UserId) *eventpb.Event {
		e := newTestEvent()
		sender.chatEventBus.OnEvent(group, &eventpb.ChatEvent{
			ChatId:         group,
			Event:          e,
			ExcludeUserIds: exclude,
		})
		return e
	}

	// receiveAll asserts the expected event arrives on userA's stream and, when
	// includeB is set, on both of userB's.
	receiveAll := func(expected *eventpb.Event, includeB bool) {
		got := testEnv.client1.receiveEventsInRealTime(t, userA)
		require.Len(t, got, 1)
		assertEquivalentTestEvents(t, expected, got[0])
		if !includeB {
			return
		}
		got = testEnv.client1.receiveEventsInRealTime(t, userB)
		require.Len(t, got, 1)
		assertEquivalentTestEvents(t, expected, got[0])
		got = testEnv.client2.receiveEventsInRealTime(t, userB)
		require.Len(t, got, 1)
		assertEquivalentTestEvents(t, expected, got[0])
	}

	// Happy path: one publish, every stream receives.
	receiveAll(publish(testEnv.server2), true)

	// Exclusion on both delivery paths at once: excluding userB suppresses
	// server2's local short-circuit and, carried in the ChatEvent, the
	// forwarded copy on server1 — where the filter must skip userB's stream
	// while still reaching userA's under the same chat key. userB's streams
	// receiving the follow-up publish as their next event is what proves the
	// suppression.
	receiveAll(publish(testEnv.server2, userB), false)
	receiveAll(publish(testEnv.server2), true)

	// The same, published from the other side: userB's exclusion applies on
	// server1's local short-circuit (selectively, alongside userA's stream)
	// and on server2's forwarded copy.
	receiveAll(publish(testEnv.server1, userB), false)
	receiveAll(publish(testEnv.server1), true)
}

// testMembershipFollowsStreams pins that a stream's chat topics track its
// user's membership while it is open: the user's own copy of a roster update,
// delivered on their user topic — forwarded from another server or published
// locally — puts every stream they have open on the chat's topic when they
// join, and takes them all off it when they leave, with the cluster rows
// following.
func testMembershipFollowsStreams(t *testing.T, accounts account.Store) {
	testEnv, cleanup := setupTest(t, accounts, true)
	defer cleanup()

	ctx := context.Background()

	userA := model.MustGenerateUserID()
	keyPairA := model.MustGenerateKeyPair()
	accounts.Bind(ctx, userA, keyPairA.Proto())
	accounts.SetRegistrationFlag(ctx, userA, true)

	userB := model.MustGenerateUserID()
	keyPairB := model.MustGenerateKeyPair()
	accounts.Bind(ctx, userB, keyPairB.Proto())
	accounts.SetRegistrationFlag(ctx, userB, true)

	// The group starts with userB alone; userA joins and leaves mid-stream.
	// userA streams from two devices on server1, userB from one on server2, so
	// every publish from server2 reaches userA over the forwarding RPC.
	group := putGroupChat(t, testEnv.chats, userB)
	testEnv.client1.openUserEventStream(t, userA, keyPairA)
	testEnv.client1.openUserEventStream(t, userA, keyPairA)
	testEnv.client2.openUserEventStream(t, userB, keyPairB)

	time.Sleep(500 * time.Millisecond)

	self := func(s *serverTestEnv) string { return s.membership.Self().InstanceID }
	chatSubscribers := func() map[string]bool {
		rows, err := testEnv.clusterStore.GetSubscribers(ctx, event.ChatEventsNamespace, group.Value)
		require.NoError(t, err)
		out := make(map[string]bool, len(rows))
		for _, row := range rows {
			out[row.InstanceID] = true
		}
		return out
	}
	waitForChatSubscribers := func(want map[string]bool) {
		require.Eventually(t, func() bool { return maps.Equal(chatSubscribers(), want) }, 5*time.Second, 10*time.Millisecond)
		// Past the publishers' subscriber cache, so the next publish resolves
		// the rows as they now stand.
		time.Sleep(100 * time.Millisecond)
	}
	publishChat := func(sender *serverTestEnv) *eventpb.Event {
		e := newTestEvent()
		sender.chatEventBus.OnEvent(group, &eventpb.ChatEvent{ChatId: group, Event: e})
		return e
	}
	// rosterUpdate builds userA's copy of a transition at the given roster
	// version.
	rosterUpdate := func(joined bool, version uint64) *eventpb.Event {
		update := &chatpb.RosterUpdate{RosterSummary: &chatpb.RosterSummary{MemberCount: 2, Version: version}}
		if joined {
			// A valid Member carries a full profile; the stream's validation
			// interceptor would otherwise refuse the event on its way out, and
			// the client would wait on it forever.
			update.Kind = &chatpb.RosterUpdate_MemberJoined_{MemberJoined: &chatpb.RosterUpdate_MemberJoined{Member: &chatpb.Member{
				UserId: userA,
				UserProfile: &profilepb.UserProfile{
					UserId:               userA,
					DisplayName:          "Alice",
					JoinTs:               timestamppb.Now(),
					TipCardCustomization: &profilepb.TipCardCustomization{Color: &commonpb.Color{Hex: "#19191A"}},
				},
			}}}
		} else {
			update.Kind = &chatpb.RosterUpdate_MemberLeft_{MemberLeft: &chatpb.RosterUpdate_MemberLeft{UserId: userA}}
		}
		e := &eventpb.Event{
			Id: model.MustGenerateEventID(),
			Ts: timestamppb.Now(),
			Type: &eventpb.Event_ChatUpdate{ChatUpdate: &eventpb.ChatUpdate{
				Chat:          group,
				RosterUpdates: &chatpb.RosterUpdateBatch{RosterUpdates: []*chatpb.RosterUpdate{update}},
			}},
		}
		require.NoError(t, e.Validate())
		return e
	}
	// receiveA asserts the next event on each of userA's streams. A test
	// event is compared less its forwarding hops; a roster update as is.
	receiveA := func(expected *eventpb.Event) {
		streamers := testEnv.client1.streams[model.UserIDString(userA)]
		require.Len(t, streamers, 2)
		for _, streamer := range streamers {
			got := receiveNextEvents(t, streamer)
			require.Len(t, got, 1)
			if expected.GetTest() != nil {
				assertEquivalentTestEvents(t, expected, got[0])
			} else {
				require.NoError(t, protoutil.ProtoEqualError(expected, got[0]))
			}
		}
	}
	receiveB := func(expected *eventpb.Event) {
		got := testEnv.client2.receiveEventsInRealTime(t, userB)
		require.Len(t, got, 1)
		assertEquivalentTestEvents(t, expected, got[0])
	}

	// Before the join only server2, hosting userB, is on the chat's topic: a
	// chat publish reaches userB alone, and userA's next event is the user
	// event published after it.
	require.Equal(t, map[string]bool{self(testEnv.server2): true}, chatSubscribers())
	receiveB(publishChat(testEnv.server2))
	receiveA(testEnv.server2.sendTestUserEvent(userA))

	// userA's copy of the join, published on server2 so it reaches server1
	// forwarded: both of userA's streams receive it, server1 registers on the
	// chat's topic, and the next chat publish reaches everyone.
	join := rosterUpdate(true, 1)
	testEnv.server2.userEventBus.OnEvent(userA, join)
	receiveA(join)
	waitForChatSubscribers(map[string]bool{self(testEnv.server1): true, self(testEnv.server2): true})
	e := publishChat(testEnv.server2)
	receiveA(e)
	receiveB(e)

	// A second copy of the join — a retry, or the other device's — changes
	// nothing: still one row, and still delivered once.
	join = rosterUpdate(true, 1)
	testEnv.server1.userEventBus.OnEvent(userA, join)
	receiveA(join)
	time.Sleep(100 * time.Millisecond)
	require.Equal(t, map[string]bool{self(testEnv.server1): true, self(testEnv.server2): true}, chatSubscribers())
	e = publishChat(testEnv.server1)
	receiveA(e)
	receiveB(e)

	// userA's copy of the departure, published locally on server1: both
	// streams receive it and come off the topic at once, server1's row goes,
	// and a chat publish from either side no longer reaches them — their next
	// event is the user event that follows.
	leave := rosterUpdate(false, 2)
	testEnv.server1.userEventBus.OnEvent(userA, leave)
	receiveA(leave)
	waitForChatSubscribers(map[string]bool{self(testEnv.server2): true})
	receiveB(publishChat(testEnv.server1))
	receiveB(publishChat(testEnv.server2))
	receiveA(testEnv.server1.sendTestUserEvent(userA))

	// Leaving again is a no-op.
	leave = rosterUpdate(false, 2)
	testEnv.server2.userEventBus.OnEvent(userA, leave)
	receiveA(leave)
	require.Equal(t, map[string]bool{self(testEnv.server2): true}, chatSubscribers())

	// Out of order: a rejoin at version 4 arrives before the departure at
	// version 3 it followed. The stale departure is delivered but moves
	// nothing — userA's streams stay on the topic, as the roster says.
	join = rosterUpdate(true, 4)
	testEnv.server2.userEventBus.OnEvent(userA, join)
	receiveA(join)
	waitForChatSubscribers(map[string]bool{self(testEnv.server1): true, self(testEnv.server2): true})
	leave = rosterUpdate(false, 3)
	testEnv.server1.userEventBus.OnEvent(userA, leave)
	receiveA(leave)
	time.Sleep(100 * time.Millisecond)
	require.Equal(t, map[string]bool{self(testEnv.server1): true, self(testEnv.server2): true}, chatSubscribers())
	e = publishChat(testEnv.server2)
	receiveA(e)
	receiveB(e)

	// Closed, the streams take their rows with them.
	testEnv.client1.closeOneUserEventStream(t, userA)
	testEnv.client1.closeOneUserEventStream(t, userA)
	waitForChatSubscribers(map[string]bool{self(testEnv.server2): true})

	// A stream opened after userA's membership has churned in the store seeds
	// its versions from the membership record: userA joins (v1), leaves (v2)
	// and rejoins (v3) before reopening, so a delayed copy of the v2
	// departure arriving afterwards is one the snapshot already reflects, and
	// moves nothing — while a v4 departure is news and does.
	for _, transition := range []func() (bool, chat.RosterSummary, error){
		func() (bool, chat.RosterSummary, error) {
			return testEnv.chats.AddGroupMembers(ctx, group, []*commonpb.UserId{userA})
		},
		func() (bool, chat.RosterSummary, error) { return testEnv.chats.RemoveGroupMember(ctx, group, userA) },
		func() (bool, chat.RosterSummary, error) {
			return testEnv.chats.AddGroupMembers(ctx, group, []*commonpb.UserId{userA})
		},
	} {
		changed, _, err := transition()
		require.NoError(t, err)
		require.True(t, changed)
	}
	roster, err := testEnv.chats.GetGroupRosterSummary(ctx, group)
	require.NoError(t, err)
	require.EqualValues(t, 3, roster.Version)

	testEnv.client1.openUserEventStream(t, userA, keyPairA)
	testEnv.client1.openUserEventStream(t, userA, keyPairA)
	waitForChatSubscribers(map[string]bool{self(testEnv.server1): true, self(testEnv.server2): true})

	leave = rosterUpdate(false, 2)
	testEnv.server2.userEventBus.OnEvent(userA, leave)
	receiveA(leave)
	time.Sleep(100 * time.Millisecond)
	require.Equal(t, map[string]bool{self(testEnv.server1): true, self(testEnv.server2): true}, chatSubscribers())
	e = publishChat(testEnv.server1)
	receiveA(e)
	receiveB(e)

	leave = rosterUpdate(false, 4)
	testEnv.server1.userEventBus.OnEvent(userA, leave)
	receiveA(leave)
	waitForChatSubscribers(map[string]bool{self(testEnv.server2): true})
	receiveB(publishChat(testEnv.server2))
	receiveA(testEnv.server1.sendTestUserEvent(userA))
}

// testMembershipReconciles pins the sweep behind the transitions a stream
// never hears about: membership moved in the store with no roster update
// published at all — the shape of a subject's copy lost to a full outbox or
// a stale subscriber set — puts the user's open streams on the chat's topic,
// and takes them off again, within the reconcile's interval.
func testMembershipReconciles(t *testing.T, accounts account.Store) {
	const reconcileInterval, reconcileTick = 200 * time.Millisecond, 25 * time.Millisecond
	testEnv, cleanup := setupTest(t, accounts, true, event.WithMembershipReconcile(reconcileInterval, reconcileTick))
	defer cleanup()

	ctx := context.Background()

	userA := model.MustGenerateUserID()
	keyPairA := model.MustGenerateKeyPair()
	accounts.Bind(ctx, userA, keyPairA.Proto())
	accounts.SetRegistrationFlag(ctx, userA, true)

	userB := model.MustGenerateUserID()
	keyPairB := model.MustGenerateKeyPair()
	accounts.Bind(ctx, userB, keyPairB.Proto())
	accounts.SetRegistrationFlag(ctx, userB, true)

	// The group starts with userB alone. userA streams from two devices on
	// server1, userB from one on server2.
	group := putGroupChat(t, testEnv.chats, userB)
	testEnv.client1.openUserEventStream(t, userA, keyPairA)
	testEnv.client1.openUserEventStream(t, userA, keyPairA)
	testEnv.client2.openUserEventStream(t, userB, keyPairB)

	time.Sleep(500 * time.Millisecond)

	self := func(s *serverTestEnv) string { return s.membership.Self().InstanceID }
	chatSubscribers := func() map[string]bool {
		rows, err := testEnv.clusterStore.GetSubscribers(ctx, event.ChatEventsNamespace, group.Value)
		require.NoError(t, err)
		out := make(map[string]bool, len(rows))
		for _, row := range rows {
			out[row.InstanceID] = true
		}
		return out
	}
	waitForChatSubscribers := func(want map[string]bool) {
		require.Eventually(t, func() bool { return maps.Equal(chatSubscribers(), want) }, 5*time.Second, 10*time.Millisecond)
		// Past the publishers' subscriber cache, so the next publish resolves
		// the rows as they now stand.
		time.Sleep(100 * time.Millisecond)
	}
	publishChat := func(sender *serverTestEnv) *eventpb.Event {
		e := newTestEvent()
		sender.chatEventBus.OnEvent(group, &eventpb.ChatEvent{ChatId: group, Event: e})
		return e
	}
	receiveA := func(expected *eventpb.Event) {
		streamers := testEnv.client1.streams[model.UserIDString(userA)]
		require.Len(t, streamers, 2)
		for _, streamer := range streamers {
			got := receiveNextEvents(t, streamer)
			require.Len(t, got, 1)
			assertEquivalentTestEvents(t, expected, got[0])
		}
	}
	receiveB := func(expected *eventpb.Event) {
		got := testEnv.client2.receiveEventsInRealTime(t, userB)
		require.Len(t, got, 1)
		assertEquivalentTestEvents(t, expected, got[0])
	}

	// A reconcile has run by now and, the store agreeing with the open, moved
	// nothing: only server2 is on the chat's topic.
	require.Equal(t, map[string]bool{self(testEnv.server2): true}, chatSubscribers())

	// userA joins in the store with no event published: the reconcile puts
	// both streams on the topic, and the next chat publish reaches them.
	changed, _, err := testEnv.chats.AddGroupMembers(ctx, group, []*commonpb.UserId{userA})
	require.NoError(t, err)
	require.True(t, changed)
	waitForChatSubscribers(map[string]bool{self(testEnv.server1): true, self(testEnv.server2): true})
	e := publishChat(testEnv.server2)
	receiveA(e)
	receiveB(e)

	// A reconcile that finds the store and the streams agreeing moves
	// nothing: the row stays, and a publish is still delivered once.
	time.Sleep(2 * reconcileInterval)
	require.Equal(t, map[string]bool{self(testEnv.server1): true, self(testEnv.server2): true}, chatSubscribers())
	e = publishChat(testEnv.server1)
	receiveA(e)
	receiveB(e)

	// userA leaves in the store, again silently: the reconcile takes both
	// streams off the topic, server1's row goes, and a chat publish from
	// either side no longer reaches them — their next event is the user event
	// that follows.
	changed, _, err = testEnv.chats.RemoveGroupMember(ctx, group, userA)
	require.NoError(t, err)
	require.True(t, changed)
	waitForChatSubscribers(map[string]bool{self(testEnv.server2): true})
	receiveB(publishChat(testEnv.server1))
	receiveB(publishChat(testEnv.server2))
	receiveA(testEnv.server1.sendTestUserEvent(userA))

	// A stream opened between a store transition and its reconcile is
	// seeded from the store while the older streams wait on the sweep: userA
	// rejoins, and a third stream opens at once — its open reads the join,
	// the two older streams learn it from the sweep — and all three end up
	// on the topic. The new stream's open is what puts server1's row back,
	// so the row says nothing about the older streams: the sweep is waited
	// out instead, the interval being the longest a user goes between reads.
	changed, _, err = testEnv.chats.AddGroupMembers(ctx, group, []*commonpb.UserId{userA})
	require.NoError(t, err)
	require.True(t, changed)
	testEnv.client1.openUserEventStream(t, userA, keyPairA)
	waitForChatSubscribers(map[string]bool{self(testEnv.server1): true, self(testEnv.server2): true})
	time.Sleep(2 * reconcileInterval)
	e = publishChat(testEnv.server2)
	streamers := testEnv.client1.streams[model.UserIDString(userA)]
	require.Len(t, streamers, 3)
	for _, streamer := range streamers {
		got := receiveNextEvents(t, streamer)
		require.Len(t, got, 1)
		assertEquivalentTestEvents(t, e, got[0])
	}
	receiveB(e)
}

// testServerShutdown pins the shutdown contract: Shutdown closes every open
// stream (which is what lets a gRPC GracefulStop return) and refuses new ones,
// while the user's streams on other servers keep receiving.
func testServerShutdown(t *testing.T, accounts account.Store) {
	testEnv, cleanup := setupTest(t, accounts, true)
	defer cleanup()

	userID := model.MustGenerateUserID()
	keyPair := model.MustGenerateKeyPair()
	accounts.Bind(context.Background(), userID, keyPair.Proto())
	accounts.SetRegistrationFlag(context.Background(), userID, true)

	testEnv.client1.openUserEventStream(t, userID, keyPair)
	testEnv.client2.openUserEventStream(t, userID, keyPair)

	time.Sleep(500 * time.Millisecond)

	testEnv.server1.server.Shutdown()

	// The open stream terminates promptly with Aborted (the handler returned).
	testEnv.client1.waitUntilStreamTerminationOrTimeout(t, userID, true, 5*time.Second)

	// A new stream against the shut-down server is refused.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	req := &eventpb.StreamEventsRequest{
		Type: &eventpb.StreamEventsRequest_Params_{
			Params: &eventpb.StreamEventsRequest_Params{
				Ts: timestamppb.Now(),
			},
		},
	}
	require.NoError(t, keyPair.Auth(req.GetParams(), &req.GetParams().Auth))
	streamer, err := testEnv.client1.client.StreamEvents(ctx)
	require.NoError(t, err)
	require.NoError(t, streamer.Send(req))
	_, err = streamer.Recv()
	require.Equal(t, codes.Unavailable, status.Code(err))

	// The user's stream on the surviving server still receives — including
	// events published through the shut-down server's bus, whose forwarder
	// keeps working until the process exits.
	expected := testEnv.server1.sendTestUserEvent(userID)
	actual := testEnv.client2.receiveEventsInRealTime(t, userID)
	require.Len(t, actual, 1)
	assertEquivalentTestEvents(t, expected, actual[0])
}

// newTestEvent builds a bare test event, with no forwarding hops yet.
func newTestEvent() *eventpb.Event {
	return &eventpb.Event{
		Id: model.MustGenerateEventID(),
		Ts: timestamppb.Now(),
		Type: &eventpb.Event_Test{
			Test: &eventpb.TestEvent{Nonce: uint64(rand.Int64())},
		},
	}
}

type testEnv struct {
	clusterStore cluster.Store
	chats        chat.Store
	accounts     *staffAccounts
	client1      *clientTestEnv
	client2      *clientTestEnv
	server1      *serverTestEnv
	server2      *serverTestEnv
}

type serverTestEnv struct {
	address      string
	membership   *cluster.Membership
	userEventBus *event.Bus[*commonpb.UserId, *eventpb.Event]
	chatEventBus *event.Bus[*commonpb.ChatId, *eventpb.ChatEvent]
	server       *event.Server
}

type clientTestEnv struct {
	client  eventpb.EventStreamingClient
	streams map[string][]*cancellableStream
}

type cancellableStream struct {
	stream eventpb.EventStreaming_StreamEventsClient
	cancel func()
}

// fastMembershipConfig mirrors the cluster suite's test tuning: convergence
// fast enough for tests, with margin over scheduler jitter.
func fastMembershipConfig() cluster.MembershipConfig {
	return cluster.MembershipConfig{
		HeartbeatInterval:   25 * time.Millisecond,
		PollInterval:        25 * time.Millisecond,
		LivenessWindow:      400 * time.Millisecond,
		SessionGapThreshold: 400 * time.Millisecond,
	}
}

// quietAfterCleanup wraps a test logger's core so it drops entries once the
// test's cleanup has begun. Stream handlers log as they wind down, and the
// gRPC teardown in cleanup lets them finish asynchronously — a handler's exit
// log landing after the test function returned is a zaptest panic, not a
// finding.
type quietAfterCleanup struct {
	zapcore.Core
	quiet *atomic.Bool
}

func (c quietAfterCleanup) Check(e zapcore.Entry, ce *zapcore.CheckedEntry) *zapcore.CheckedEntry {
	if c.quiet.Load() {
		return ce
	}
	return c.Core.Check(e, ce)
}

func (c quietAfterCleanup) With(fields []zapcore.Field) zapcore.Core {
	return quietAfterCleanup{Core: c.Core.With(fields), quiet: c.quiet}
}

func setupTest(t *testing.T, accounts account.Store, enableMultiServer bool, opts ...event.ServerOption) (env testEnv, cleanup func()) {
	quiet := new(atomic.Bool)
	log := zaptest.NewLogger(t, zaptest.WrapOptions(zap.WrapCore(func(core zapcore.Core) zapcore.Core {
		return quietAfterCleanup{Core: core, quiet: quiet}
	})))
	ctx := context.Background()

	conn1, serv1, err := ocp_testutil.NewServer(log)
	require.NoError(t, err)

	conn2, serv2, err := ocp_testutil.NewServer(log)
	require.NoError(t, err)

	env.client1 = &clientTestEnv{
		client:  eventpb.NewEventStreamingClient(conn1),
		streams: make(map[string][]*cancellableStream),
	}
	env.client2 = &clientTestEnv{
		client:  eventpb.NewEventStreamingClient(conn1),
		streams: make(map[string][]*cancellableStream),
	}
	if enableMultiServer {
		env.client2.client = eventpb.NewEventStreamingClient(conn2)
	}

	authz := account.NewAuthorizer(log, accounts, auth.NewKeyPairAuthenticator(log))

	// A shared badge store, mirroring the shared cluster backend; both server
	// instances reset the same user's badge on stream open.
	badges := badgememory.NewInMemory()

	env.clusterStore = cluster_memory.NewInMemory()

	// A shared chat store, so both servers resolve the same group memberships.
	env.chats = chat_memory.NewInMemory()

	// The access a chat stream's viewer is admitted by, over the shared chat
	// store, with a staff flag the tests can set and no admission window, so a
	// standing re-check sees a revocation at once.
	env.accounts = newStaffAccounts(accounts)
	balances := balance.NewClient(log, env.accounts, &emptyOcpBalance{})
	access := chat.NewAccess(env.chats, chat.NewRuleEvaluator(env.accounts, balances, env.chats), chat.WithListenerAdmissionTTL(0))

	newServerEnv := func(name string, conn *grpc.ClientConn) *serverTestEnv {
		membership := cluster.NewMembership(log, env.clusterStore, &cluster.Member{
			InstanceID: name,
			Address:    conn.Target(),
			Labels:     map[string]string{"role": "all"},
		}, fastMembershipConfig())
		require.NoError(t, membership.Start(ctx))

		subscriptions := cluster.NewSubscriptions(log, membership, env.clusterStore, cluster.SubscriptionsConfig{
			CacheTTL: 25 * time.Millisecond,
		})

		userEventBus := event.NewBus[*commonpb.UserId, *eventpb.Event]()
		chatEventBus := event.NewBus[*commonpb.ChatId, *eventpb.ChatEvent]()
		return &serverTestEnv{
			address:      conn.Target(),
			membership:   membership,
			userEventBus: userEventBus,
			chatEventBus: chatEventBus,
			server: event.NewServer(
				log,
				authz,
				accounts,
				badges,
				env.chats,
				access,
				subscriptions,
				userEventBus,
				chatEventBus,
				nil,
				internalRpcApiKey,
				opts...,
			),
		}
	}

	env.server1 = newServerEnv("server-1", conn1)
	env.server2 = newServerEnv("server-2", conn2)

	// Both members must be in both live views before streams open, or a
	// publisher would discard the other server's subscription rows as
	// not-yet-live.
	for _, s := range []*serverTestEnv{env.server1, env.server2} {
		require.Eventually(t, func() bool {
			return len(s.membership.Live()) == 2
		}, 5*time.Second, 10*time.Millisecond)
	}

	serv1.RegisterService(func(server *grpc.Server) {
		eventpb.RegisterEventStreamingServer(server, env.server1.server)
	})
	serv2.RegisterService(func(server *grpc.Server) {
		eventpb.RegisterEventStreamingServer(server, env.server2.server)
	})

	cleanup1, err := serv1.Serve()
	require.NoError(t, err)
	cleanup2, err := serv2.Serve()
	require.NoError(t, err)

	return env, func() {
		quiet.Store(true)
		env.server1.server.Shutdown()
		env.server2.server.Shutdown()
		cleanup1()
		cleanup2()
		env.server1.membership.Stop()
		env.server2.membership.Stop()
	}
}

func (s *serverTestEnv) sendTestUserEvent(userID *commonpb.UserId) *eventpb.Event {
	e := newTestEvent()
	s.userEventBus.OnEvent(userID, e)
	return e
}

func (c *clientTestEnv) openUserEventStream(t *testing.T, userID *commonpb.UserId, keyPair model.KeyPair) {
	key := model.UserIDString(userID)

	cancellableCtx, cancel := context.WithCancel(context.Background())

	req := &eventpb.StreamEventsRequest{
		Type: &eventpb.StreamEventsRequest_Params_{
			Params: &eventpb.StreamEventsRequest_Params{
				Ts: timestamppb.Now(),
			},
		},
	}
	require.NoError(t, keyPair.Auth(req.GetParams(), &req.GetParams().Auth))

	streamer, err := c.client.StreamEvents(cancellableCtx)
	require.NoError(t, err)

	require.NoError(t, streamer.Send(req))

	c.streams[key] = append(c.streams[key], &cancellableStream{
		stream: streamer,
		cancel: cancel,
	})
}

// receiveNextEvents pumps one stream until it yields an event batch, answering
// pings along the way.
func receiveNextEvents(t *testing.T, streamer *cancellableStream) []*eventpb.Event {
	for {
		resp, err := streamer.stream.Recv()
		require.NoError(t, err)

		switch typed := resp.Type.(type) {
		case *eventpb.StreamEventsResponse_Events:
			return typed.Events.Events
		case *eventpb.StreamEventsResponse_Ping:
			err = streamer.stream.Send(&eventpb.StreamEventsRequest{
				Type: &eventpb.StreamEventsRequest_Pong{
					Pong: &eventpb.ClientPong{
						Timestamp: timestamppb.Now(),
					},
				},
			})
			// Stream has been terminated
			if err != io.EOF {
				require.NoError(t, err)
			}
		case *eventpb.StreamEventsResponse_Error:
			require.Failf(t, "stream result code %s", typed.Error.Code.String())
		default:
			require.Fail(t, "events, ping or error wasn't set")
		}
	}
}

func (c *clientTestEnv) receiveEventsInRealTime(t *testing.T, userID *commonpb.UserId) []*eventpb.Event {
	key := model.UserIDString(userID)

	streamers, ok := c.streams[key]
	require.True(t, ok)
	require.Len(t, streamers, 1)

	return receiveNextEvents(t, streamers[0])
}

func (c *clientTestEnv) waitUntilStreamTerminationOrTimeout(t *testing.T, userID *commonpb.UserId, keepStreamAlive bool, timeout time.Duration) int {
	key := model.UserIDString(userID)

	streamers, ok := c.streams[key]
	require.True(t, ok)
	require.Len(t, streamers, 1)
	streamer := streamers[0]

	var pingCount int
	start := time.Now()
	for {
		resp, err := streamer.stream.Recv()

		status, ok := status.FromError(err)
		if ok && status.Code() == codes.Aborted {
			return pingCount
		}

		require.NoError(t, err)

		switch typed := resp.Type.(type) {
		case *eventpb.StreamEventsResponse_Ping:
			pingCount += 1

			if keepStreamAlive {
				require.NoError(t, streamer.stream.Send(&eventpb.StreamEventsRequest{
					Type: &eventpb.StreamEventsRequest_Pong{
						Pong: &eventpb.ClientPong{
							Timestamp: timestamppb.Now(),
						},
					},
				}))
			}

			if time.Since(start) > timeout {
				return pingCount
			}
		case *eventpb.StreamEventsResponse_Error:
			require.Failf(t, "stream result code %s", typed.Error.Code.String())
		case *eventpb.StreamEventsResponse_Events:
		default:
			require.Fail(t, "events, ping or error wasn't set")
		}
	}
}

// closeOneUserEventStream cancels the user's oldest open stream, leaving any
// others open.
func (c *clientTestEnv) closeOneUserEventStream(t *testing.T, userID *commonpb.UserId) {
	key := model.UserIDString(userID)
	streamers, ok := c.streams[key]
	require.True(t, ok)
	require.NotEmpty(t, streamers)

	streamers[0].cancel()
	c.streams[key] = streamers[1:]
	if len(c.streams[key]) == 0 {
		delete(c.streams, key)
	}
}

// putGroupChat creates a group chat with the given members joined and returns
// its ID.
func putGroupChat(t *testing.T, chats chat.Store, members ...*commonpb.UserId) *commonpb.ChatId {
	c := &chat.Chat{
		ID:           chat.MustGenerateGroupChatID(),
		Type:         chatpb.ChatType_GROUP,
		Members:      members,
		Title:        "Group",
		LastActivity: time.Now(),
	}
	require.NoError(t, chats.PutChat(context.Background(), c))
	return c.ID
}

func assertEquivalentTestEvents(t *testing.T, obj1, obj2 *eventpb.Event) {
	cloned1 := proto.Clone(obj1).(*eventpb.Event)
	cloned2 := proto.Clone(obj2).(*eventpb.Event)
	cloned1.GetTest().Hops = nil
	cloned2.GetTest().Hops = nil
	require.NoError(t, protoutil.ProtoEqualError(cloned1, cloned2))
}

// testChatPreview pins a preview of a group: it opens for a non-member who
// may read the group under the mode asked — a qualifying non-member in full,
// any other registered user redacted under a mode that allows it — and for no
// one else: a member is DENIED whatever the mode, having nothing to preview.
// It carries that group's updates and nothing else, across the fleet,
// honoring exclusions, with every message under the standing fixed at open.
// A group that does not exist is NOT_FOUND; a DM is DENIED to everyone, its
// members included.
func testChatPreview(t *testing.T, accounts account.Store) {
	testEnv, cleanup := setupTest(t, accounts, true)
	defer cleanup()

	ctx := context.Background()

	member, memberKeys := registerUser(t, accounts)
	staff, staffKeys := registerUser(t, accounts)
	testEnv.accounts.setStaff(staff, true)
	other, otherKeys := registerUser(t, accounts)

	// A staff-only group with member as its one member: staff may read it in
	// full without joining, other only redacted.
	group := putStaffGroupChat(t, testEnv.chats, member)
	dm := putDmChat(t, testEnv.chats, member, other)

	// Refused at open: a member under any mode, a non-member with no full
	// reading under FULL, a group that does not exist under any mode, and a
	// DM under any mode by anyone.
	for _, mode := range []messagingpb.ViewMode{messagingpb.ViewMode_FULL, messagingpb.ViewMode_FULL_OR_REDACTED, messagingpb.ViewMode_REDACTED} {
		expectChatStreamError(t, testEnv.client1.openChatPreview(t, memberKeys, group, mode), eventpb.StreamEventsResponse_StreamError_DENIED)
		expectChatStreamError(t, testEnv.client1.openChatPreview(t, memberKeys, dm, mode), eventpb.StreamEventsResponse_StreamError_DENIED)
		expectChatStreamError(t, testEnv.client1.openChatPreview(t, staffKeys, dm, mode), eventpb.StreamEventsResponse_StreamError_DENIED)
	}
	expectChatStreamError(t, testEnv.client1.openChatPreview(t, otherKeys, group, messagingpb.ViewMode_FULL), eventpb.StreamEventsResponse_StreamError_DENIED)
	expectChatStreamError(t, testEnv.client1.openChatPreview(t, staffKeys, chat.MustGenerateGroupChatID(), messagingpb.ViewMode_FULL_OR_REDACTED), eventpb.StreamEventsResponse_StreamError_NOT_FOUND)

	// Open: the qualifying non-member in full, from a device on each server;
	// other redacted, from each server too.
	staffFull1 := testEnv.client1.openChatPreview(t, staffKeys, group, messagingpb.ViewMode_FULL)
	staffFull2 := testEnv.client2.openChatPreview(t, staffKeys, group, messagingpb.ViewMode_FULL_OR_REDACTED)
	otherRedacted1 := testEnv.client1.openChatPreview(t, otherKeys, group, messagingpb.ViewMode_FULL_OR_REDACTED)
	otherRedacted2 := testEnv.client2.openChatPreview(t, otherKeys, group, messagingpb.ViewMode_REDACTED)

	time.Sleep(500 * time.Millisecond)

	publish := func(sender *serverTestEnv, e *eventpb.Event, exclude ...*commonpb.UserId) *eventpb.Event {
		sender.chatEventBus.OnEvent(group, &eventpb.ChatEvent{ChatId: group, Event: e, ExcludeUserIds: exclude})
		return e
	}
	receiveFull := func(streamer *cancellableStream, expected *eventpb.Event) {
		got := receiveNextEvents(t, streamer)
		require.Len(t, got, 1)
		require.NoError(t, protoutil.ProtoEqualError(expected, got[0]))
	}
	receiveRedacted := func(streamer *cancellableStream, sent *eventpb.Event) {
		got := receiveNextEvents(t, streamer)
		require.Len(t, got, 1)
		assertRedactedMessageSent(t, group, sent, got[0])
	}
	// receiveAll asserts a sent message arrives on every stream in full or
	// redacted as each opened: the full readers as published, the redacted
	// ones as a placeholder of the same message.
	receiveAll := func(sent *eventpb.Event, full, redacted []*cancellableStream) {
		for _, streamer := range full {
			receiveFull(streamer, sent)
		}
		for _, streamer := range redacted {
			receiveRedacted(streamer, sent)
		}
	}
	fullStreams := []*cancellableStream{staffFull1, staffFull2}
	redactedStreams := []*cancellableStream{otherRedacted1, otherRedacted2}
	allStreams := append(append([]*cancellableStream{}, fullStreams...), redactedStreams...)

	// A message sent, published from either server, reaches every stream.
	for _, sender := range []*serverTestEnv{testEnv.server1, testEnv.server2} {
		receiveAll(publish(sender, newMessageSentEvent(group, member, 1, "the words")), fullStreams, redactedStreams)
	}

	// Nothing but the group's updates: a user event and another chat's
	// update, both addressed to staff, and the DM's update addressed to
	// other, never appear on a preview — their next event is the group's.
	testEnv.server1.sendTestUserEvent(staff)
	testEnv.server1.userEventBus.OnEvent(staff, newMessageSentEvent(chat.MustGenerateGroupChatID(), staff, 1, "elsewhere"))
	testEnv.server1.userEventBus.OnEvent(other, newMessageSentEvent(dm, member, 1, "in the dm"))
	receiveAll(publish(testEnv.server1, newMessageSentEvent(group, member, 2, "more words")), fullStreams, redactedStreams)

	// An exclusion applies to a preview as to any other stream: excluding
	// staff suppresses both of their previews, on both delivery paths, while
	// the others still receive.
	excluded := publish(testEnv.server2, newMessageSentEvent(group, member, 3, "not for staff"), staff)
	receiveAll(excluded, nil, redactedStreams)
	receiveAll(publish(testEnv.server2, newMessageSentEvent(group, member, 4, "for everyone")), fullStreams, redactedStreams)

	// A roster overlay is delivered whatever the mode, as it is: a member's
	// departure reaches every preview unredacted.
	left := rosterLeftEvent(group, member, 1)
	publish(testEnv.server1, left)
	for _, streamer := range allStreams {
		receiveFull(streamer, left)
	}

	// The standing is the open's: staff who lose their flag mid-window keep
	// their full previews until the window closes, and are refused a new
	// one; other, who joins mid-window, keeps their redacted previews too,
	// and is refused a new one as a member.
	testEnv.accounts.setStaff(staff, false)
	changed, _, err := testEnv.chats.AddGroupMembers(ctx, group, []*commonpb.UserId{other})
	require.NoError(t, err)
	require.True(t, changed)
	receiveAll(publish(testEnv.server2, newMessageSentEvent(group, other, 5, "after the changes")), fullStreams, redactedStreams)
	expectChatStreamError(t, testEnv.client1.openChatPreview(t, staffKeys, group, messagingpb.ViewMode_FULL), eventpb.StreamEventsResponse_StreamError_DENIED)
	expectChatStreamError(t, testEnv.client1.openChatPreview(t, otherKeys, group, messagingpb.ViewMode_REDACTED), eventpb.StreamEventsResponse_StreamError_DENIED)
}

// testChatPreviewExpires pins the window: a preview ends with STREAM_EXPIRED
// once the server's lifetime elapses, whether or not anything was delivered,
// and a client answering every ping does not extend it. A preview opened
// after another's window closed gets a window of its own.
func testChatPreviewExpires(t *testing.T, accounts account.Store) {
	const lifetime = 2 * time.Second
	testEnv, cleanup := setupTest(t, accounts, false, event.WithChatPreviewLifetime(lifetime))
	defer cleanup()

	member, _ := registerUser(t, accounts)
	staff, staffKeys := registerUser(t, accounts)
	testEnv.accounts.setStaff(staff, true)
	group := putStaffGroupChat(t, testEnv.chats, member)

	// Two previews of the same window, one read as soon as it opens and one
	// left to accumulate; both close together.
	first := testEnv.client1.openChatPreview(t, staffKeys, group, messagingpb.ViewMode_FULL)
	second := testEnv.client1.openChatPreview(t, staffKeys, group, messagingpb.ViewMode_FULL)
	openedAt := time.Now()

	time.Sleep(lifetime / 4)
	sent := newMessageSentEvent(group, member, 1, "hello")
	testEnv.server1.chatEventBus.OnEvent(group, &eventpb.ChatEvent{ChatId: group, Event: sent})
	got := receiveNextEvents(t, first)
	require.Len(t, got, 1)
	require.NoError(t, protoutil.ProtoEqualError(sent, got[0]))
	expectChatStreamError(t, first, eventpb.StreamEventsResponse_StreamError_STREAM_EXPIRED)
	require.GreaterOrEqual(t, time.Since(openedAt), lifetime)

	got = receiveNextEvents(t, second)
	require.Len(t, got, 1)
	require.NoError(t, protoutil.ProtoEqualError(sent, got[0]))
	expectChatStreamError(t, second, eventpb.StreamEventsResponse_StreamError_STREAM_EXPIRED)
	elapsed := time.Since(openedAt)
	require.GreaterOrEqual(t, elapsed, lifetime)
	require.Less(t, elapsed, 2*lifetime)

	// A new preview opens with its own window, and is delivered to.
	again := testEnv.client1.openChatPreview(t, staffKeys, group, messagingpb.ViewMode_FULL)
	time.Sleep(250 * time.Millisecond)
	sent = newMessageSentEvent(group, member, 2, "again")
	testEnv.server1.chatEventBus.OnEvent(group, &eventpb.ChatEvent{ChatId: group, Event: sent})
	got = receiveNextEvents(t, again)
	require.Len(t, got, 1)
	require.NoError(t, protoutil.ProtoEqualError(sent, got[0]))
	expectChatStreamError(t, again, eventpb.StreamEventsResponse_StreamError_STREAM_EXPIRED)
}

// registerUser binds a fresh registered user and returns their identity.
func registerUser(t *testing.T, accounts account.Store) (*commonpb.UserId, model.KeyPair) {
	userID := model.MustGenerateUserID()
	keyPair := model.MustGenerateKeyPair()
	_, err := accounts.Bind(context.Background(), userID, keyPair.Proto())
	require.NoError(t, err)
	require.NoError(t, accounts.SetRegistrationFlag(context.Background(), userID, true))
	return userID, keyPair
}

// putStaffGroupChat creates a staff-only group with the given members joined
// and returns its ID. Staff is the one listener rule a test can satisfy
// without a balance: it opens the group to a staff non-member in full, and to
// every other registered user redacted.
func putStaffGroupChat(t *testing.T, chats chat.Store, members ...*commonpb.UserId) *commonpb.ChatId {
	c := &chat.Chat{
		ID:           chat.MustGenerateGroupChatID(),
		Type:         chatpb.ChatType_GROUP,
		Members:      members,
		Title:        "Staff group",
		IsStaffOnly:  true,
		LastActivity: time.Now(),
	}
	require.NoError(t, chats.PutChat(context.Background(), c))
	return c.ID
}

// putDmChat creates a contact DM between the two users and returns its ID.
func putDmChat(t *testing.T, chats chat.Store, a, b *commonpb.UserId) *commonpb.ChatId {
	c := &chat.Chat{
		ID:           chat.MustDeriveDmChatID(chatpb.ChatType_CONTACT_DM, a, b),
		Type:         chatpb.ChatType_CONTACT_DM,
		Members:      []*commonpb.UserId{a, b},
		LastActivity: time.Now(),
	}
	require.NoError(t, chats.PutChat(context.Background(), c))
	return c.ID
}

// newMessageSentEvent builds a chat update carrying one sent text message,
// valid on its way out of the stream.
func newMessageSentEvent(chatID *commonpb.ChatId, sender *commonpb.UserId, seq uint64, text string) *eventpb.Event {
	e := &eventpb.Event{
		Id: model.MustGenerateEventID(),
		Ts: timestamppb.Now(),
		Type: &eventpb.Event_ChatUpdate{ChatUpdate: &eventpb.ChatUpdate{
			Chat: chatID,
			Events: &messagingpb.EventBatch{Events: []*messagingpb.Event{{
				Sequence: seq,
				Count:    1,
				Ts:       timestamppb.Now(),
				Mutations: []*messagingpb.Mutation{{Type: &messagingpb.Mutation_MessageSent{MessageSent: &messagingpb.Message{
					MessageId:     &messagingpb.MessageId{Value: seq},
					SenderId:      sender,
					Content:       []*messagingpb.Content{{Type: &messagingpb.Content_Text{Text: &messagingpb.TextContent{Text: text}}}},
					Ts:            timestamppb.Now(),
					EventSequence: seq,
				}}}},
			}}},
		}},
	}
	if err := e.Validate(); err != nil {
		panic(err)
	}
	return e
}

// rosterLeftEvent builds a chat update carrying a member's departure at the
// given roster version.
func rosterLeftEvent(chatID *commonpb.ChatId, subject *commonpb.UserId, version uint64) *eventpb.Event {
	e := &eventpb.Event{
		Id: model.MustGenerateEventID(),
		Ts: timestamppb.Now(),
		Type: &eventpb.Event_ChatUpdate{ChatUpdate: &eventpb.ChatUpdate{
			Chat: chatID,
			RosterUpdates: &chatpb.RosterUpdateBatch{RosterUpdates: []*chatpb.RosterUpdate{{
				Kind:          &chatpb.RosterUpdate_MemberLeft_{MemberLeft: &chatpb.RosterUpdate_MemberLeft{UserId: subject}},
				RosterSummary: &chatpb.RosterSummary{MemberCount: 1, Version: version},
			}}},
		}},
	}
	if err := e.Validate(); err != nil {
		panic(err)
	}
	return e
}

// assertRedactedMessageSent asserts that got is the redaction of a message
// sent event built by newMessageSentEvent: the same event and message, its
// content a placeholder with Message.redacted set.
func assertRedactedMessageSent(t *testing.T, chatID *commonpb.ChatId, sent, got *eventpb.Event) {
	require.NoError(t, protoutil.ProtoEqualError(sent.Id, got.Id))
	require.NoError(t, protoutil.ProtoEqualError(chatID, got.GetChatUpdate().GetChat()))

	sentEvents := sent.GetChatUpdate().GetEvents().GetEvents()
	gotEvents := got.GetChatUpdate().GetEvents().GetEvents()
	require.Len(t, gotEvents, len(sentEvents))
	for i, sentEvent := range sentEvents {
		require.Equal(t, sentEvent.GetSequence(), gotEvents[i].GetSequence())
		require.Len(t, gotEvents[i].GetMutations(), 1)
		sentMsg := sentEvent.GetMutations()[0].GetMessageSent()
		gotMsg := gotEvents[i].GetMutations()[0].GetMessageSent()
		require.NotNil(t, gotMsg)
		require.True(t, gotMsg.GetRedacted())
		require.NoError(t, protoutil.ProtoEqualError(sentMsg.GetMessageId(), gotMsg.GetMessageId()))
		require.NoError(t, protoutil.ProtoEqualError(sentMsg.GetSenderId(), gotMsg.GetSenderId()))
		require.Len(t, gotMsg.GetContent(), 1)
		require.NotEmpty(t, gotMsg.GetContent()[0].GetText().GetText())
		require.NotEqual(t, sentMsg.GetContent()[0].GetText().GetText(), gotMsg.GetContent()[0].GetText().GetText())
	}
}

// openChatPreview opens a preview of chatID under mode and
// returns it, leaving the outcome — events, or an error at open — for the
// caller to read.
func (c *clientTestEnv) openChatPreview(t *testing.T, keyPair model.KeyPair, chatID *commonpb.ChatId, mode messagingpb.ViewMode) *cancellableStream {
	cancellableCtx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	req := &eventpb.StreamEventsRequest{
		Type: &eventpb.StreamEventsRequest_Params_{
			Params: &eventpb.StreamEventsRequest_Params{
				Ts: timestamppb.Now(),
				Target: &eventpb.StreamEventsRequest_Params_ChatPreview{
					ChatPreview: &eventpb.StreamEventsRequest_ChatPreviewParams{ChatId: chatID, ViewMode: mode},
				},
			},
		},
	}
	require.NoError(t, keyPair.Auth(req.GetParams(), &req.GetParams().Auth))

	streamer, err := c.client.StreamEvents(cancellableCtx)
	require.NoError(t, err)
	require.NoError(t, streamer.Send(req))

	return &cancellableStream{stream: streamer, cancel: cancel}
}

// expectChatStreamError pumps the stream, answering pings, until it yields a
// stream error, and asserts its code. An event batch before it is a failure.
func expectChatStreamError(t *testing.T, streamer *cancellableStream, code eventpb.StreamEventsResponse_StreamError_Code) {
	for {
		resp, err := streamer.stream.Recv()
		require.NoError(t, err)

		switch typed := resp.Type.(type) {
		case *eventpb.StreamEventsResponse_Error:
			require.Equal(t, code, typed.Error.Code)
			return
		case *eventpb.StreamEventsResponse_Ping:
			err = streamer.stream.Send(&eventpb.StreamEventsRequest{
				Type: &eventpb.StreamEventsRequest_Pong{Pong: &eventpb.ClientPong{Timestamp: timestamppb.Now()}},
			})
			if err != io.EOF {
				require.NoError(t, err)
			}
		case *eventpb.StreamEventsResponse_Events:
			require.Failf(t, "unexpected events", "expected stream error %s, got %d events", code, len(typed.Events.Events))
		default:
			require.Fail(t, "events, ping or error wasn't set")
		}
	}
}

// staffAccounts is an account.Store whose staff flag a test can set: the
// in-memory store answers IsStaff false for everyone, and has no setter.
type staffAccounts struct {
	account.Store

	mu    sync.Mutex
	staff map[string]bool
}

func newStaffAccounts(db account.Store) *staffAccounts {
	return &staffAccounts{Store: db, staff: make(map[string]bool)}
}

func (a *staffAccounts) setStaff(userID *commonpb.UserId, isStaff bool) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.staff[string(userID.Value)] = isStaff
}

func (a *staffAccounts) IsStaff(_ context.Context, userID *commonpb.UserId) (bool, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.staff[string(userID.Value)], nil
}

// emptyOcpBalance is an OCP balance service with no accounts: the groups the
// suite creates carry a staff rule alone, so no balance is ever asked for.
type emptyOcpBalance struct{}

func (emptyOcpBalance) GetBalances(context.Context, *ocp_balancepb.GetBalancesRequest, ...grpc.CallOption) (*ocp_balancepb.GetBalancesResponse, error) {
	return &ocp_balancepb.GetBalancesResponse{}, nil
}
