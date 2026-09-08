package event

import (
	"context"
	"errors"
	"sync"
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
	"github.com/code-payments/flipcash2-server/cluster"
	"github.com/code-payments/flipcash2-server/cluster/internalrpc"
	"github.com/code-payments/flipcash2-server/model"
	"github.com/code-payments/flipcash2-server/protoutil"
)

// UserEventsNamespace is the cluster subscription namespace for per-user event
// stream topics, keyed by the raw user ID bytes. A topic's subscribers are the
// servers currently hosting at least one of that user's open streams.
const UserEventsNamespace = "user-events"

const (
	maxEventBatchSize = 1024

	streamBufferSize   = 64
	streamPingDelay    = 5 * time.Second
	streamTimeout      = time.Second
	streamSendTimeout  = 5 * time.Second
	streamPongTimeout  = 2 * streamPingDelay
	streamInitTsWindow = 2 * time.Minute

	// subscriptionCloseTimeout bounds the registry cleanup on stream teardown,
	// which runs after the stream's own context is already done.
	subscriptionCloseTimeout = 2 * time.Second

	forwardRpcTimeout = 250 * time.Millisecond
)

type StaleEventDetectorCtor[Event any] func() StaleEventDetector[Event]

type StaleEventDetector[Event any] interface {
	ShouldDrop(event Event) bool
}

type Server struct {
	log *zap.Logger

	authz auth.Authorizer

	accounts account.Store
	badges   badge.Store

	subscriptions *cluster.Subscriptions

	eventBus *Bus[*commonpb.UserId, *eventpb.Event]

	// streams fans a topic out to every open local stream: stream key → stream
	// ID → stream. Multiple streams per key are the point (one per device);
	// the cluster subscription layer refcounts them into a single registry row.
	streamsMu               sync.RWMutex
	streams                 map[string]map[string]Stream[[]*eventpb.Event]
	staleEventDetectorCtors []StaleEventDetectorCtor[*eventpb.Event]

	// self is the cluster member this process registers subscription rows as:
	// its instance ID recognizes our own rows on the publish path, its address
	// labels forwarded test events. Single-sourced from the cluster runtime so
	// stream registration and self-detection can never disagree.
	self *cluster.Member

	internalAuth *internalrpc.Authenticator
	forwarder    *userEventForwarder

	eventpb.UnimplementedEventStreamingServer
}

func NewServer(
	log *zap.Logger,
	authz auth.Authorizer,
	accounts account.Store,
	badges badge.Store,
	subscriptions *cluster.Subscriptions,
	eventBus *Bus[*commonpb.UserId, *eventpb.Event],
	staleEventDetectorCtors []StaleEventDetectorCtor[*eventpb.Event],
	currentRpcApiKey string,
) *Server {
	s := &Server{
		log: log,

		authz: authz,

		accounts: accounts,
		badges:   badges,

		subscriptions: subscriptions,

		eventBus: eventBus,

		streams:                 make(map[string]map[string]Stream[[]*eventpb.Event]),
		staleEventDetectorCtors: staleEventDetectorCtors,

		self:         subscriptions.Self(),
		internalAuth: internalrpc.NewAuthenticator(currentRpcApiKey),
	}

	s.forwarder = &userEventForwarder{
		log:            log,
		subscriptions:  subscriptions,
		pool:           sharedForwardingPool(log),
		apiKey:         currentRpcApiKey,
		selfInstanceID: s.self.InstanceID,
		deliverLocal:   s.deliverLocal,
	}

	eventBus.AddHandler(HandlerFunc[*commonpb.UserId, *eventpb.Event](s.OnEvent))

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
	streamKey := model.UserIDString(userID)

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

	// The local stream must be resolvable before the topic's registry row is,
	// or a publish racing the open could resolve the row yet find no stream
	// behind it.
	s.streamsMu.Lock()
	byID, ok := s.streams[streamKey]
	if !ok {
		byID = make(map[string]Stream[[]*eventpb.Event])
		s.streams[streamKey] = byID
	}
	byID[streamID] = ss
	s.streamsMu.Unlock()

	removeLocalStream := func() {
		s.streamsMu.Lock()
		if byID, ok := s.streams[streamKey]; ok {
			delete(byID, streamID)
			if len(byID) == 0 {
				delete(s.streams, streamKey)
			}
		}
		s.streamsMu.Unlock()
	}

	// Register this server's interest in the user's events with the cluster,
	// so publishers on other servers forward here. Non-exclusive: the same
	// user may hold streams on any number of servers simultaneously.
	subscription, err := s.subscriptions.Subscribe(ctx, UserEventsNamespace, userID.Value)
	if err != nil {
		removeLocalStream()
		if errors.Is(err, cluster.ErrSubscriptionsDraining) {
			log.Debug("Rejecting stream on draining server")
			return status.Error(codes.Unavailable, "server is draining")
		}
		log.With(zap.Error(err)).Warn("Failure registering stream subscription")
		return status.Error(codes.Internal, "failure registering stream subscription")
	}

	defer func() {
		log.Debug("Closing streamer")

		removeLocalStream()

		closeCtx, cancel := context.WithTimeout(context.Background(), subscriptionCloseTimeout)
		if err := subscription.Close(closeCtx); err != nil {
			log.With(zap.Error(err)).Warn("Failed to close stream subscription")
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

	for _, event := range req.UserEvents.Events {
		switch typed := event.Event.Type.(type) {
		case *eventpb.Event_Test:
			typed.Test.Hops = append(typed.Test.Hops, s.self.Address)
		}

		s.deliverLocal(model.UserIDString(event.UserId), event.Event)
	}
	return &eventpb.ForwardEventsResponse{}, nil
}

// todo: utilize batching by receiver to optimize internal forwarding RPC calls
func (s *Server) ForwardUserEvents(ctx context.Context, events ...*eventpb.UserEvent) error {
	return s.forwarder.ForwardUserEvents(ctx, events...)
}

// deliverLocal notifies an event onto every local stream open for the key.
func (s *Server) deliverLocal(streamKey string, e *eventpb.Event) {
	s.streamsMu.RLock()
	targets := make([]Stream[[]*eventpb.Event], 0, len(s.streams[streamKey]))
	for _, stream := range s.streams[streamKey] {
		targets = append(targets, stream)
	}
	s.streamsMu.RUnlock()

	for _, stream := range targets {
		cloned := proto.Clone(e).(*eventpb.Event)
		if err := stream.Notify([]*eventpb.Event{cloned}, streamTimeout); err != nil {
			s.log.With(zap.Error(err)).Warn("Failed to notify event on local stream", zap.String("stream_key", streamKey))
		}
	}
}

func (s *Server) OnEvent(userID *commonpb.UserId, e *eventpb.Event) {
	s.ForwardUserEvents(context.Background(), &eventpb.UserEvent{UserId: userID, Event: e})
}
