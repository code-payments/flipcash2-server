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
	"github.com/code-payments/flipcash2-server/event"
	"github.com/code-payments/flipcash2-server/model"
	"github.com/code-payments/flipcash2-server/protoutil"
	ocptestutil "github.com/code-payments/ocp-server/testutil"
)

func RunServerTests(t *testing.T, accounts account.Store, events event.Store, teardown func()) {
	for _, tf := range []func(t *testing.T, accounts account.Store, events event.Store){
		testSingleServerHappyPath,
		testMultiServerHappyPath,
		testMultipleOpenStreams,
		testKeepAlive,
		testRendezvousRecord,
	} {
		tf(t, accounts, events)
		teardown()
	}
}

func testSingleServerHappyPath(t *testing.T, accounts account.Store, events event.Store) {
	testEnv, cleanup := setupTest(t, accounts, events, false)
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

func testMultiServerHappyPath(t *testing.T, accounts account.Store, events event.Store) {
	testEnv, cleanup := setupTest(t, accounts, events, true)
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

func testMultipleOpenStreams(t *testing.T, accounts account.Store, events event.Store) {
	for range 32 {
		func() {
			testEnv, cleanup := setupTest(t, accounts, events, true)
			defer cleanup()

			userID := model.MustGenerateUserID()
			keyPair := model.MustGenerateKeyPair()
			accounts.Bind(context.Background(), userID, keyPair.Proto())
			accounts.SetRegistrationFlag(context.Background(), userID, true)

			for range 10 {
				testEnv.client1.openUserEventStream(t, userID, keyPair)
				testEnv.client2.openUserEventStream(t, userID, keyPair)
			}

			time.Sleep(500 * time.Millisecond)

			for i := range 100 {
				sender := testEnv.server1
				if i%2 == 0 {
					sender = testEnv.server2
				}

				expected := sender.sendTestUserEvent(userID)

				fromServer1 := testEnv.client1.receiveEventsInRealTime(t, userID)
				fromServer2 := testEnv.client2.receiveEventsInRealTime(t, userID)

				allActual := append(fromServer1, fromServer2...)
				require.Lenf(t, allActual, 1, "expected[%d]: %s", i, event.EventIDString(expected.Id))
				assertEquivalentTestEvents(t, expected, allActual[0])
			}
		}()
	}
}

func testKeepAlive(t *testing.T, accounts account.Store, events event.Store) {
	testEnv, cleanup := setupTest(t, accounts, events, false)
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

func testRendezvousRecord(t *testing.T, accounts account.Store, events event.Store) {
	testEnv, cleanup := setupTest(t, accounts, events, false)
	defer cleanup()

	userID := model.MustGenerateUserID()
	keyPair := model.MustGenerateKeyPair()
	accounts.Bind(context.Background(), userID, keyPair.Proto())
	accounts.SetRegistrationFlag(context.Background(), userID, true)

	testEnv.client1.openUserEventStream(t, userID, keyPair)

	time.Sleep(500 * time.Millisecond)

	testEnv.server1.assertRendezvousRecordExists(t, userID)

	testEnv.client1.closeUserEventStream(t, userID)

	time.Sleep(500 * time.Millisecond)

	testEnv.server1.assertNoRendezvousRecord(t, userID)
}

type testEnv struct {
	client1 *clientTestEnv
	client2 *clientTestEnv
	server1 *serverTestEnv
	server2 *serverTestEnv
}

type serverTestEnv struct {
	address  string
	events   event.Store
	eventBus *event.Bus[*commonpb.UserId, *eventpb.Event]
	server   *event.Server
}

type clientTestEnv struct {
	client  eventpb.EventStreamingClient
	streams map[string][]*cancellableStream
}

type cancellableStream struct {
	stream eventpb.EventStreaming_StreamEventsClient
	cancel func()
}

func setupTest(t *testing.T, accounts account.Store, events event.Store, enableMultiServer bool) (env testEnv, cleanup func()) {
	log := zaptest.NewLogger(t)

	conn1, serv1, err := ocptestutil.NewServer(log)
	require.NoError(t, err)

	conn2, serv2, err := ocptestutil.NewServer(log)
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

	eventBus1 := event.NewBus[*commonpb.UserId, *eventpb.Event]()
	eventBus2 := event.NewBus[*commonpb.UserId, *eventpb.Event]()

	env.server1 = &serverTestEnv{
		address:  conn1.Target(),
		eventBus: eventBus1,
		events:   events,
		server: event.NewServer(
			log,
			authz,
			accounts,
			events,
			eventBus1,
			nil,
			conn1.Target(),
			internalRpcApiKey,
		),
	}
	env.server2 = &serverTestEnv{
		address:  conn2.Target(),
		events:   events,
		eventBus: eventBus2,
		server: event.NewServer(
			log,
			authz,
			accounts,
			events,
			eventBus2,
			nil,
			conn2.Target(),
			internalRpcApiKey,
		),
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

func (s *serverTestEnv) assertRendezvousRecordExists(t *testing.T, userID *commonpb.UserId) {
	rendezvous, err := s.events.GetRendezvous(context.Background(), model.UserIDString(userID))
	require.NoError(t, err)
	require.Equal(t, s.address, rendezvous.Address)
	require.True(t, rendezvous.ExpiresAt.After(time.Now()))
}

func (s *serverTestEnv) assertNoRendezvousRecord(t *testing.T, userID *commonpb.UserId) {
	_, err := s.events.GetRendezvous(t.Context(), model.UserIDString(userID))
	require.Equal(t, event.ErrRendezvousNotFound, err)
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

func (c *clientTestEnv) receiveEventsInRealTime(t *testing.T, userID *commonpb.UserId) []*eventpb.Event {
	key := model.UserIDString(userID)

	streamers, ok := c.streams[key]
	require.True(t, ok)

	for _, streamer := range streamers {
		for {
			resp, err := streamer.stream.Recv()

			status, ok := status.FromError(err)
			if ok && status.Code() == codes.Aborted {
				// Try the next open stream
				break
			}

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

	return nil
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

func (c *clientTestEnv) closeUserEventStream(t *testing.T, userID *commonpb.UserId) {
	key := model.UserIDString(userID)
	streamers, ok := c.streams[key]
	require.True(t, ok)
	for _, streamer := range streamers {
		streamer.cancel()
	}
	delete(c.streams, key)
}

func assertEquivalentTestEvents(t *testing.T, obj1, obj2 *eventpb.Event) {
	cloned1 := proto.Clone(obj1).(*eventpb.Event)
	cloned2 := proto.Clone(obj2).(*eventpb.Event)
	cloned1.GetTest().Hops = nil
	cloned2.GetTest().Hops = nil
	require.NoError(t, protoutil.ProtoEqualError(cloned1, cloned2))
}
