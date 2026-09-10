package event

import (
	"context"
	"encoding/hex"
	"errors"
	"time"

	"github.com/google/uuid"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"
	eventpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/event/v1"

	"github.com/code-payments/flipcash2-server/account"
	"github.com/code-payments/flipcash2-server/auth"
	"github.com/code-payments/flipcash2-server/badge"
	"github.com/code-payments/flipcash2-server/chat"
	"github.com/code-payments/flipcash2-server/cluster"
	"github.com/code-payments/flipcash2-server/cluster/internalrpc"
	"github.com/code-payments/flipcash2-server/model"
	"github.com/code-payments/flipcash2-server/protoutil"
)

// UserEventsNamespace is the cluster subscription namespace for per-user event
// stream topics, keyed by the raw user ID bytes. A topic's subscribers are the
// servers currently hosting at least one of that user's open streams.
const UserEventsNamespace = "user-events"

// ChatEventsNamespace is the cluster subscription namespace for per-chat
// event topics, keyed by the raw chat ID bytes. A topic's subscribers are the
// servers currently hosting at least one open stream belonging to a member of
// that chat, so a chat publisher resolves the hosting servers in one lookup
// instead of one per member. Today only group chats register here — DM
// delivery stays user-keyed — but nothing about the topic shape is
// group-specific.
const ChatEventsNamespace = "chat-events"

// userStreamKey and chatStreamKey name a topic's slot in the local streams
// registry, shared by the registration and delivery paths. The prefixes keep
// the two key families disjoint: user IDs and group chat IDs are both 16-byte
// values, so an unprefixed chat ID could alias a user's slot.
func userStreamKey(userID *commonpb.UserId) string {
	return "user:" + model.UserIDString(userID)
}

func chatStreamKey(chatID *commonpb.ChatId) string {
	return "chat:" + hex.EncodeToString(chatID.GetValue())
}

const (
	maxEventBatchSize = 1024

	streamBufferSize   = 64
	streamPingDelay    = 5 * time.Second
	streamTimeout      = time.Second
	streamSendTimeout  = 5 * time.Second
	streamPongTimeout  = 2 * streamPingDelay
	streamInitTsWindow = 2 * time.Minute

	// subscriptionCloseTimeout bounds the registry cleanup on stream teardown,
	// which runs after the stream's own context is already done. It covers all
	// of a stream's registrations (user topic plus group topics) at once:
	// releases are refcount decrements, and the topics this stream was the
	// last local holder of go out as a single batched delete (CloseAll).
	subscriptionCloseTimeout = 2 * time.Second

	forwardRpcTimeout = 250 * time.Millisecond
)

// localStream is one open stream in the local registry, carrying the user it
// belongs to: a stream appears under several keys (its user's, plus one per
// chat), and chat-keyed delivery needs the owner to honor per-user exclusions,
// which the stream object itself cannot say.
type localStream struct {
	stream Stream[[]*eventpb.Event]
	userID *commonpb.UserId
}

type StaleEventDetectorCtor[Event any] func() StaleEventDetector[Event]

type StaleEventDetector[Event any] interface {
	ShouldDrop(event Event) bool
}

type Server struct {
	log *zap.Logger

	authz auth.Authorizer

	accounts account.Store
	badges   badge.Store
	chats    chat.Store

	subscriptions *cluster.Subscriptions

	userEventBus *Bus[*commonpb.UserId, *eventpb.Event]
	chatEventBus *Bus[*commonpb.ChatId, *eventpb.ChatEvent]

	// streams fans a topic out to every open local stream (see streamRegistry).
	// It also refuses new streams once Shutdown has begun, so a stream can't
	// slip in behind the closing sweep.
	streams                 *streamRegistry
	staleEventDetectorCtors []StaleEventDetectorCtor[*eventpb.Event]

	// self is the cluster member this process registers subscription rows as:
	// its instance ID recognizes our own rows on the publish path, its address
	// labels forwarded test events. Single-sourced from the cluster runtime so
	// stream registration and self-detection can never disagree.
	self *cluster.Member

	internalAuth *internalrpc.Authenticator
	forwarder    *eventForwarder

	eventpb.UnimplementedEventStreamingServer
}

func NewServer(
	log *zap.Logger,
	authz auth.Authorizer,
	accounts account.Store,
	badges badge.Store,
	chats chat.Store,
	subscriptions *cluster.Subscriptions,
	userEventBus *Bus[*commonpb.UserId, *eventpb.Event],
	chatEventBus *Bus[*commonpb.ChatId, *eventpb.ChatEvent],
	staleEventDetectorCtors []StaleEventDetectorCtor[*eventpb.Event],
	currentRpcApiKey string,
) *Server {
	// The server hosts streams and registers subscription rows, which requires
	// a registered cluster member — an observer membership backs forwarding
	// only (use ForwardingClient for that). Fail construction loudly instead
	// of nil-dereferencing below.
	if subscriptions.Self() == nil {
		panic("event: Server requires a member-backed subscriptions runtime; an observer cannot host streams")
	}

	s := &Server{
		log: log,

		authz: authz,

		accounts: accounts,
		badges:   badges,
		chats:    chats,

		subscriptions: subscriptions,

		userEventBus: userEventBus,
		chatEventBus: chatEventBus,

		streams:                 newStreamRegistry(),
		staleEventDetectorCtors: staleEventDetectorCtors,

		self:         subscriptions.Self(),
		internalAuth: internalrpc.NewAuthenticator(currentRpcApiKey),
	}

	s.forwarder = &eventForwarder{
		log:            log,
		subscriptions:  subscriptions,
		pool:           sharedForwardingPool(log),
		apiKey:         currentRpcApiKey,
		selfInstanceID: s.self.InstanceID,
		deliverLocal:   s.deliverLocal,
	}

	userEventBus.AddHandler(HandlerFunc[*commonpb.UserId, *eventpb.Event](s.OnEvent))
	chatEventBus.AddHandler(HandlerFunc[*commonpb.ChatId, *eventpb.ChatEvent](s.OnChatEvent))

	return s
}

func (s *Server) StreamEvents(stream grpc.BidiStreamingServer[eventpb.StreamEventsRequest, eventpb.StreamEventsResponse]) error {
	ctx := stream.Context()

	req, err := protoutil.BoundedReceive[eventpb.StreamEventsRequest](
		ctx,
		stream,
		250*time.Millisecond,
	)
	if err != nil {
		return err
	}

	params := req.GetParams()
	if req.GetParams() == nil {
		return status.Error(codes.InvalidArgument, "missing parameters")
	}

	t := params.Ts.AsTime()
	if t.After(time.Now().Add(streamInitTsWindow)) || t.Before(time.Now().Add(-streamInitTsWindow)) {
		return stream.Send(&eventpb.StreamEventsResponse{Type: &eventpb.StreamEventsResponse_Error{
			Error: &eventpb.StreamEventsResponse_StreamError{Code: eventpb.StreamEventsResponse_StreamError_INVALID_TIMESTAMP},
		}})
	}

	userID, err := s.authz.Authorize(ctx, params, &params.Auth)
	if err != nil {
		return err
	}

	log := s.log.With(zap.String("user_id", model.UserIDString(userID)))

	isRegistered, err := s.accounts.IsRegistered(ctx, userID)
	if err != nil {
		log.With(zap.Error(err)).Warn("Failure getting registration flag")
		return status.Error(codes.Internal, "failure getting registration flag")
	} else if !isRegistered {
		return stream.Send(&eventpb.StreamEventsResponse{Type: &eventpb.StreamEventsResponse_Error{
			Error: &eventpb.StreamEventsResponse_StreamError{Code: eventpb.StreamEventsResponse_StreamError_DENIED},
		}})
	}

	// A stream open is the client coming to the foreground (guaranteed on app
	// open), which is when the badge resets to zero. Best-effort: a failure here
	// must not block streaming.
	if err := s.badges.Reset(ctx, userID); err != nil {
		log.With(zap.Error(err)).Warn("Failed to reset badge count on stream open")
	}

	streamID := uuid.New().String()

	log = log.With(zap.String("stream_id", streamID))

	// Sanity check whether the stream is still valid before doing expensive
	// operations
	select {
	case <-ctx.Done():
		log.Debug("Stream context cancelled; ending stream")
		return status.Error(codes.Canceled, "")
	default:
	}

	log.Debug("Initializing stream")

	staleEventDetectors := make([]StaleEventDetector[*eventpb.Event], len(s.staleEventDetectorCtors))
	for i, ctor := range s.staleEventDetectorCtors {
		staleEventDetectors[i] = ctor()
	}

	ss := NewProtoEventStream(
		streamID,
		streamBufferSize,
		func(events []*eventpb.Event) (*eventpb.EventBatch, bool) {
			if len(events) > maxEventBatchSize {
				log.Warn("Event batch size exceeds proto limit")
				return nil, false
			}

			if len(events) == 0 {
				return nil, false
			}

			var eventsToSend []*eventpb.Event
			for _, event := range events {
				log := log.With(zap.String("event_id", EventIDString(event.Id)))

				var isDropped bool
				for _, staleEventDetector := range staleEventDetectors {
					if staleEventDetector.ShouldDrop(event) {
						isDropped = true
						break
					}
				}

				if isDropped {
					log.Debug("Dropping stale event")
					continue
				}

				log.Debug("Sending event to client in batch")
				eventsToSend = append(eventsToSend, event)
			}

			if len(eventsToSend) == 0 {
				return nil, false
			}
			return &eventpb.EventBatch{Events: eventsToSend}, true
		},
	)

	// The stream serves the user's own topic plus one topic per group chat
	// they are joined to, so group publishers can resolve the hosting servers
	// by group instead of once per member. The membership set is a snapshot as
	// of stream open: a group joined or left mid-stream does not adjust the
	// registration until the client reconnects.
	groupChatIDs, err := s.chats.GetGroupChatIDsForUser(ctx, userID)
	if err != nil {
		log.With(zap.Error(err)).Warn("Failure loading group memberships for stream")
		return status.Error(codes.Internal, "failure loading group memberships")
	}

	streamKeys := make([]string, 0, 1+len(groupChatIDs))
	streamKeys = append(streamKeys, userStreamKey(userID))
	for _, chatID := range groupChatIDs {
		streamKeys = append(streamKeys, chatStreamKey(chatID))
	}

	// The local stream must be resolvable before a topic's registry row is, or
	// a publish racing the open could resolve the row yet find no stream
	// behind it — so every key is registered up front, before the first
	// Subscribe below.
	if !s.streams.add(streamID, localStream{stream: ss, userID: userID}, streamKeys) {
		log.Debug("Rejecting stream on shut-down server")
		return status.Error(codes.Unavailable, "server is draining")
	}

	removeLocalStream := func() {
		s.streams.remove(streamID, streamKeys)
	}

	// Register this server's interest in the stream's topics with the cluster,
	// so publishers on other servers forward here — one batched registration
	// covering the user topic and every group topic, so the stream-open path
	// pays a single store round trip no matter how many groups. Non-exclusive:
	// the same user (and all the more so the same group) may hold streams on
	// any number of servers simultaneously.
	topics := make([]cluster.SubscriptionTopic, 0, 1+len(groupChatIDs))
	topics = append(topics, cluster.SubscriptionTopic{Namespace: UserEventsNamespace, Key: userID.Value})
	for _, chatID := range groupChatIDs {
		topics = append(topics, cluster.SubscriptionTopic{Namespace: ChatEventsNamespace, Key: chatID.Value})
	}
	subscriptions, err := s.subscriptions.SubscribeAll(ctx, topics)
	if err != nil {
		removeLocalStream()
		if errors.Is(err, cluster.ErrSubscriptionsDraining) {
			log.Debug("Rejecting stream on draining server")
			return status.Error(codes.Unavailable, "server is draining")
		}
		log.With(zap.Error(err)).Warn("Failure registering stream subscriptions")
		return status.Error(codes.Internal, "failure registering stream subscriptions")
	}

	defer func() {
		log.Debug("Closing streamer")

		removeLocalStream()

		closeCtx, cancel := context.WithTimeout(context.Background(), subscriptionCloseTimeout)
		if err := s.subscriptions.CloseAll(closeCtx, subscriptions); err != nil {
			log.With(zap.Error(err)).Warn("Failed to close stream subscriptions")
		}
		cancel()
	}()

	sendPingCh := time.After(0)
	streamHealthCh := protoutil.MonitorStreamHealth(ctx, log, stream, streamPongTimeout, func(t *eventpb.StreamEventsRequest) bool {
		pong := t.GetPong()
		if pong == nil {
			return false
		}

		if ts := pong.GetTimestamp(); ts != nil {
			log.Debug("Received pong from client", zap.Duration("upstream_latency", time.Since(ts.AsTime())))
		}

		return true
	})

	for {
		select {
		case batch, ok := <-ss.Channel():
			if !ok {
				log.Debug("Stream closed; ending stream")
				return status.Error(codes.Aborted, "stream closed")
			}

			log.Debug("Sending events to client stream")
			err = protoutil.BoundedSend(ctx, stream, &eventpb.StreamEventsResponse{
				Type: &eventpb.StreamEventsResponse_Events{
					Events: batch,
				},
			}, streamSendTimeout)
			if err != nil {
				log.Info("Failed to send events to client stream", zap.Error(err))
				return err
			}
		case <-sendPingCh:
			log.Debug("Sending ping to client")

			sendPingCh = time.After(streamPingDelay)

			err := protoutil.BoundedSend(ctx, stream, &eventpb.StreamEventsResponse{
				Type: &eventpb.StreamEventsResponse_Ping{
					Ping: &eventpb.ServerPing{
						Timestamp: timestamppb.Now(),
						PingDelay: durationpb.New(streamPingDelay),
					},
				},
			}, streamSendTimeout)
			if err != nil {
				log.Debug("Stream is unhealthy; aborting")
				return status.Error(codes.Aborted, "terminating unhealthy stream")
			}
		case <-streamHealthCh:
			log.Debug("Stream is unhealthy; aborting")
			return status.Error(codes.Aborted, "terminating unhealthy stream")
		case <-ctx.Done():
			log.Debug("Stream context cancelled; ending stream")
			return status.Error(codes.Canceled, "")
		}
	}
}

// ForwardEvents is the internal RPC receiving events forwarded by the server
// that observed them. Delivery here is local-only: the sender already resolved
// this server as a subscriber, and re-resolving would at best repeat its work
// and at worst bounce an event between servers holding mutually stale caches.
// An event arriving for a user with no local streams (the row outlived the
// last stream by a cache window) is dropped; the client's delta sync is the
// backstop.
func (s *Server) ForwardEvents(ctx context.Context, req *eventpb.ForwardEventsRequest) (*eventpb.ForwardEventsResponse, error) {
	allowed, err := s.internalAuth.Allow(ctx)
	if err != nil {
		s.log.Warn("Failure getting RPC API key header")
		return nil, status.Error(codes.Internal, "")
	}
	if !allowed {
		return &eventpb.ForwardEventsResponse{Result: eventpb.ForwardEventsResponse_DENIED}, nil
	}

	switch batch := req.Type.(type) {
	case *eventpb.ForwardEventsRequest_UserEvents:
		for _, event := range batch.UserEvents.Events {
			s.stampTestHop(event.Event)
			s.deliverLocal(userStreamKey(event.UserId), event.Event, nil)
		}
	case *eventpb.ForwardEventsRequest_ChatEvents:
		for _, event := range batch.ChatEvents.Events {
			s.stampTestHop(event.Event)
			s.deliverLocal(chatStreamKey(event.ChatId), event.Event, event.ExcludeUserIds)
		}
	default:
		return nil, status.Error(codes.InvalidArgument, "missing event batch")
	}
	return &eventpb.ForwardEventsResponse{}, nil
}

// stampTestHop marks this server on a test event's forwarding path.
func (s *Server) stampTestHop(e *eventpb.Event) {
	if test := e.GetTest(); test != nil {
		test.Hops = append(test.Hops, s.self.Address)
	}
}

// todo: utilize batching by receiver to optimize internal forwarding RPC calls
func (s *Server) ForwardUserEvents(ctx context.Context, events ...*eventpb.UserEvent) error {
	return s.forwarder.ForwardUserEvents(ctx, events...)
}

func (s *Server) ForwardChatEvents(ctx context.Context, events ...*eventpb.ChatEvent) error {
	return s.forwarder.ForwardChatEvents(ctx, events...)
}

// Shutdown closes every open client stream — each StreamEvents handler returns
// and cleans up its subscription — and refuses new ones. Call it on shutdown
// after Subscriptions.Drain and before the gRPC server's GracefulStop: the
// streams are held open indefinitely by connected clients, so a GracefulStop
// without this never returns. Closed clients reconnect to a healthy server and
// delta sync. Idempotent.
func (s *Server) Shutdown() {
	targets := s.streams.drain()

	s.log.Debug("Closing all event streams for shutdown", zap.Int("streams", len(targets)))
	for _, stream := range targets {
		stream.Close()
	}
}

// deliverLocal notifies an event onto every local stream open for the key,
// skipping streams owned by an excluded user (e.g. the originator of a typing
// notification on a chat-keyed delivery, where the topic no longer selects
// recipients per member).
func (s *Server) deliverLocal(streamKey string, e *eventpb.Event, exclude []*commonpb.UserId) {
	for _, stream := range s.streams.targets(streamKey, exclude) {
		cloned := proto.Clone(e).(*eventpb.Event)
		if err := stream.Notify([]*eventpb.Event{cloned}, streamTimeout); err != nil {
			s.log.With(zap.Error(err)).Warn("Failed to notify event on local stream", zap.String("stream_key", streamKey))
		}
	}
}

func (s *Server) OnEvent(userID *commonpb.UserId, e *eventpb.Event) {
	s.ForwardUserEvents(context.Background(), &eventpb.UserEvent{UserId: userID, Event: e})
}

// OnChatEvent is the chat bus handler; the payload carries the chat ID and
// exclusions itself, so the bus key rides along only for the bus's shape.
func (s *Server) OnChatEvent(_ *commonpb.ChatId, e *eventpb.ChatEvent) {
	s.ForwardChatEvents(context.Background(), e)
}
