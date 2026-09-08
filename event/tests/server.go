package tests

import (
	"context"
	"io"
	"math/rand/v2"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"
	eventpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/event/v1"

	"github.com/code-payments/flipcash2-server/account"
	"github.com/code-payments/flipcash2-server/auth"
	badgememory "github.com/code-payments/flipcash2-server/badge/memory"
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
		testKeepAlive,
		testSubscriptionRegistration,
		testServerShutdown,
	} {
		tf(t, accounts)
		teardown()
	}
}

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

type testEnv struct {
	clusterStore cluster.Store
	client1      *clientTestEnv
	client2      *clientTestEnv
	server1      *serverTestEnv
	server2      *serverTestEnv
}

type serverTestEnv struct {
	address    string
	membership *cluster.Membership
	eventBus   *event.Bus[*commonpb.UserId, *eventpb.Event]
	server     *event.Server
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

func setupTest(t *testing.T, accounts account.Store, enableMultiServer bool) (env testEnv, cleanup func()) {
	log := zaptest.NewLogger(t)
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

	internalRpcApiKey := "valid-api-key"

	authz := account.NewAuthorizer(log, accounts, auth.NewKeyPairAuthenticator(log))

	// A shared badge store, mirroring the shared cluster backend; both server
	// instances reset the same user's badge on stream open.
	badges := badgememory.NewInMemory()

	env.clusterStore = cluster_memory.NewInMemory()

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

		eventBus := event.NewBus[*commonpb.UserId, *eventpb.Event]()
		return &serverTestEnv{
			address:    conn.Target(),
			membership: membership,
			eventBus:   eventBus,
			server: event.NewServer(
				log,
				authz,
				accounts,
				badges,
				subscriptions,
				eventBus,
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
		cleanup1()
		cleanup2()
		env.server1.membership.Stop()
		env.server2.membership.Stop()
	}
}

func (s *serverTestEnv) sendTestUserEvent(userID *commonpb.UserId) *eventpb.Event {
	e := &eventpb.Event{
		Id: event.MustGenerateEventID(),
		Ts: timestamppb.Now(),
		Type: &eventpb.Event_Test{
			Test: &eventpb.TestEvent{
				Hops:  []string{s.address},
				Nonce: uint64(rand.Int64()),
			},
		},
	}
	s.eventBus.OnEvent(userID, e)
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

func assertEquivalentTestEvents(t *testing.T, obj1, obj2 *eventpb.Event) {
	cloned1 := proto.Clone(obj1).(*eventpb.Event)
	cloned2 := proto.Clone(obj2).(*eventpb.Event)
	cloned1.GetTest().Hops = nil
	cloned2.GetTest().Hops = nil
	require.NoError(t, protoutil.ProtoEqualError(cloned1, cloned2))
}
