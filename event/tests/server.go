package tests

import (
	"context"
	"io"
	"math/rand/v2"
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

	"github.com/code-payments/flipcash2-server/account"
	"github.com/code-payments/flipcash2-server/auth"
	badgememory "github.com/code-payments/flipcash2-server/badge/memory"
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
				require.Lenf(t, events, 1, "expected[%d]: %s", i, event.EventIDString(expected.Id))
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
		expected[event.EventIDString(e.Id)] = e
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
			id := event.EventIDString(e.Id)
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
		Id: event.MustGenerateEventID(),
		Ts: timestamppb.Now(),
		Type: &eventpb.Event_Test{
			Test: &eventpb.TestEvent{Nonce: uint64(rand.Int64())},
		},
	}
}

type testEnv struct {
	clusterStore cluster.Store
	chats        chat.Store
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

func setupTest(t *testing.T, accounts account.Store, enableMultiServer bool) (env testEnv, cleanup func()) {
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
				subscriptions,
				userEventBus,
				chatEventBus,
				nil,
				internalRpcApiKey,
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
