package event

import (
	"context"
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
	"github.com/code-payments/flipcash2-server/cluster/internalrpc"
	"github.com/code-payments/flipcash2-server/model"
	"github.com/code-payments/flipcash2-server/protoutil"
)

const (
	maxEventBatchSize = 1024

	streamBufferSize   = 64
	streamPingDelay    = 5 * time.Second
	streamTimeout      = time.Second
	streamSendTimeout  = 5 * time.Second
	streamPongTimeout  = 2 * streamPingDelay
	streamInitTsWindow = 2 * time.Minute

	rendezvousExpiryTime      = 3 * time.Second
	rendezvousRefreshInterval = 2 * time.Second

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
	events   Store
	badges   badge.Store

	eventBus *Bus[*commonpb.UserId, *eventpb.Event]

	streamsMu               sync.RWMutex
	individualStreamMu      map[string]*sync.Mutex
	streams                 map[string]Stream[[]*eventpb.Event]
	staleEventDetectorCtors []StaleEventDetectorCtor[*eventpb.Event]

	broadcastAddress string
	internalAuth     *internalrpc.Authenticator
	forwarder        *userEventForwarder

	eventpb.UnimplementedEventStreamingServer
}

func NewServer(
	log *zap.Logger,
	authz auth.Authorizer,
	accounts account.Store,
	events Store,
	badges badge.Store,
	eventBus *Bus[*commonpb.UserId, *eventpb.Event],
	staleEventDetectorCtors []StaleEventDetectorCtor[*eventpb.Event],
	broadcastAddress string,
	currentRpcApiKey string,
) *Server {
	s := &Server{
		log: log,

		authz: authz,

		accounts: accounts,
		events:   events,
		badges:   badges,

		eventBus: eventBus,

		individualStreamMu:      make(map[string]*sync.Mutex),
		streams:                 make(map[string]Stream[[]*eventpb.Event]),
		staleEventDetectorCtors: staleEventDetectorCtors,

		broadcastAddress: broadcastAddress,
		internalAuth:     internalrpc.NewAuthenticator(currentRpcApiKey),
	}

	s.forwarder = &userEventForwarder{
		log:          log,
		events:       events,
		pool:         sharedForwardingPool(log),
		apiKey:       currentRpcApiKey,
		selfAddress:  broadcastAddress,
		deliverLocal: s.deliverLocal,
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

	streamID := uuid.New()
	streamKey := model.UserIDString(userID)

	log = log.With(zap.String("stream_id", streamID.String()))

	s.streamsMu.Lock()
	if existing, exists := s.streams[streamKey]; exists {
		delete(s.streams, streamKey)
		existing.Close()

		log.Debug("Closed previous stream")
	}

	log.Debug("Initializing stream")

	staleEventDetectors := make([]StaleEventDetector[*eventpb.Event], len(s.staleEventDetectorCtors))
	for i, ctor := range s.staleEventDetectorCtors {
		staleEventDetectors[i] = ctor()
	}

	ss := NewProtoEventStream(
		streamKey,
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

	s.streams[streamKey] = ss

	myStreamMu, ok := s.individualStreamMu[streamKey]
	if !ok {
		myStreamMu = &sync.Mutex{}
		s.individualStreamMu[streamKey] = myStreamMu
	}

	s.streamsMu.Unlock()

	myStreamMu.Lock()

	defer func() {
		s.streamsMu.Lock()

		log.Debug("Closing streamer")

		// We check to see if the current active stream is the one that we created.
		// If it is, we can just remove it since it's closed. Otherwise, we leave it
		// be, as another StreamEvents() call is handling it.
		liveStream := s.streams[streamKey]
		if liveStream == ss {
			delete(s.streams, streamKey)
		}

		s.streamsMu.Unlock()

		ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
		err := s.events.DeleteRendezvous(ctx, streamKey, s.broadcastAddress)
		if err != nil {
			log.With(zap.Error(err)).Warn("Failed to cleanup rendezvous record")
		}
		cancel()

		myStreamMu.Unlock()
	}()

	// Sanity check whether the stream is still valid before doing expensive operations
	select {
	case <-ctx.Done():
		log.Debug("Stream context cancelled; ending stream")
		return status.Error(codes.Canceled, "")
	default:
	}

	// Let other RPC servers know where to find the active stream via a rendezvous
	// record
	rendezvous := &Rendezvous{
		Key:       streamKey,
		Address:   s.broadcastAddress,
		ExpiresAt: time.Now().Add(rendezvousExpiryTime),
	}
	err = s.events.CreateRendezvous(ctx, rendezvous)
	if err == ErrRendezvousExists {
		log.Debug("Existing stream detected on another server aborting")
		return status.Error(codes.Aborted, "stream already exists")
	} else if err != nil {
		log.With(zap.Error(err)).Warn("Failure saving rendezvous record")
		return status.Error(codes.Internal, "failure saving rendezvous record")
	}

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

	rendezvousErrCh := make(chan error, 1)
	go func() {
		ticker := time.NewTicker(rendezvousRefreshInterval)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				expiry := time.Now().Add(rendezvousExpiryTime)
				if err := s.events.ExtendRendezvousExpiry(ctx, streamKey, s.broadcastAddress, expiry); err != nil {
					if ctx.Err() == nil {
						rendezvousErrCh <- err
					}
					return
				}

				log.Debug("Refreshed rendezvous record")
			}
		}
	}()

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
		case err := <-rendezvousErrCh:
			if err == ErrRendezvousNotFound {
				log.Debug("Existing stream detected on another server aborting")
				return status.Error(codes.Aborted, "stream already exists")
			}

			log.With(zap.Error(err)).Warn("Failure extending rendezvous record expiry")
			return status.Error(codes.Internal, "")
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
		log := s.log.With(
			zap.String("event_id", EventIDString(event.Event.Id)),
			zap.String("user_id", model.UserIDString(event.UserId)),
		)

		switch typed := event.Event.Type.(type) {
		case *eventpb.Event_Test:
			typed.Test.Hops = append(typed.Test.Hops, s.broadcastAddress)
		}

		err = s.ForwardUserEvents(context.Background(), event)
		if err != nil {
			log.With(zap.Error(err)).Warn("Failure forwarding user event")
		}
	}
	return &eventpb.ForwardEventsResponse{}, nil
}

// todo: utilize batching by receiver to optimize internal forwarding RPC calls
func (s *Server) ForwardUserEvents(ctx context.Context, events ...*eventpb.UserEvent) error {
	return s.forwarder.ForwardUserEvents(ctx, events...)
}

// deliverLocal notifies an event onto the user's stream when this server hosts
// it.
func (s *Server) deliverLocal(streamKey string, e *eventpb.Event) {
	s.streamsMu.RLock()
	stream, exists := s.streams[streamKey]
	s.streamsMu.RUnlock()

	if !exists {
		return
	}

	cloned := proto.Clone(e).(*eventpb.Event)
	if err := stream.Notify([]*eventpb.Event{cloned}, streamTimeout); err != nil {
		s.log.With(zap.Error(err)).Warn("Failed to notify event on local stream", zap.String("stream_key", streamKey))
	}
}

func (s *Server) OnEvent(userID *commonpb.UserId, e *eventpb.Event) {
	s.ForwardUserEvents(context.Background(), &eventpb.UserEvent{UserId: userID, Event: e})
}
