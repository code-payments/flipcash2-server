package event

import (
	"context"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/pkg/errors"
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
	"github.com/code-payments/flipcash2-server/model"
	"github.com/code-payments/flipcash2-server/protoutil"
	ocpheaders "github.com/code-payments/ocp-server/grpc/headers"
	ocpretry "github.com/code-payments/ocp-server/retry"
	ocpbackoff "github.com/code-payments/ocp-server/retry/backoff"
)

const (
	maxEventBatchSize = 1024

	streamBufferSize   = 64
	streamPingDelay    = 5 * time.Second
	streamTimeout      = time.Second
	streamInitTsWindow = 2 * time.Minute

	rendezvousExpiryTime      = 3 * time.Second
	rendezvousRefreshInterval = 2 * time.Second

	forwardRpcTimeout = 250 * time.Millisecond

	internalRpcApiKeyHeaderName = "x-flipcash-internal-rpc-api-key"
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

	eventBus *Bus[*commonpb.UserId, *eventpb.Event]

	streamsMu               sync.RWMutex
	individualStreamMu      map[string]*sync.Mutex
	streams                 map[string]Stream[[]*eventpb.Event]
	staleEventDetectorCtors []StaleEventDetectorCtor[*eventpb.Event]

	broadcastAddress      string
	allInternalRpcApiKeys map[string]any
	currentRpcApiKey      string

	eventpb.UnimplementedEventStreamingServer
}

func NewServer(
	log *zap.Logger,
	authz auth.Authorizer,
	accounts account.Store,
	events Store,
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

		eventBus: eventBus,

		individualStreamMu:      make(map[string]*sync.Mutex),
		streams:                 make(map[string]Stream[[]*eventpb.Event]),
		staleEventDetectorCtors: staleEventDetectorCtors,

		broadcastAddress:      broadcastAddress,
		currentRpcApiKey:      currentRpcApiKey,
		allInternalRpcApiKeys: make(map[string]any),
	}

	s.allInternalRpcApiKeys[currentRpcApiKey] = true

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

	streamID := uuid.New()
	streamKey := model.UserIDString(userID)

	log = log.With(zap.String("stream_id", streamID.String()))

	s.streamsMu.Lock()
	if existing, exists := s.streams[streamKey]; exists {
		delete(s.streams, streamKey)
		existing.Close()

		log.Info("Closed previous stream")
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
		log.Warn("Existing stream detected on another server aborting")
		return status.Error(codes.Aborted, "stream already exists")
	} else if err != nil {
		log.With(zap.Error(err)).Warn("Failure saving rendezvous record")
		return status.Error(codes.Internal, "failure saving rendezvous record")
	}

	updateRendezvousCh := time.After(rendezvousRefreshInterval)
	sendPingCh := time.After(0)
	streamHealthCh := protoutil.MonitorStreamHealth(ctx, log, stream, func(t *eventpb.StreamEventsRequest) bool {
		return t.GetPong() != nil
	})

	for {
		select {
		case batch, ok := <-ss.Channel():
			if !ok {
				log.Debug("Stream closed; ending stream")
				return status.Error(codes.Aborted, "stream closed")
			}

			log.Debug("Sending events to client stream")
			err = stream.Send(&eventpb.StreamEventsResponse{
				Type: &eventpb.StreamEventsResponse_Events{
					Events: batch,
				},
			})
			if err != nil {
				log.Info("Failed to send events to client stream", zap.Error(err))
				return err
			}
		case <-updateRendezvousCh:
			log.Debug("Refreshing rendezvous record")

			expiry := time.Now().Add(rendezvousExpiryTime)

			err = s.events.ExtendRendezvousExpiry(ctx, streamKey, s.broadcastAddress, expiry)
			if err == ErrRendezvousNotFound {
				log.Warn("Existing stream detected on another server aborting")
				return status.Error(codes.Aborted, "stream already exists")
			} else if err != nil {
				log.With(zap.Error(err)).Warn("Failure extending rendezvous record expiry")
				return status.Error(codes.Internal, "failure extending rendezvous record expiry")
			}

			updateRendezvousCh = time.After(rendezvousRefreshInterval)
		case <-sendPingCh:
			log.Debug("Sending ping to client")

			sendPingCh = time.After(streamPingDelay)

			err := stream.Send(&eventpb.StreamEventsResponse{
				Type: &eventpb.StreamEventsResponse_Ping{
					Ping: &eventpb.ServerPing{
						Timestamp: timestamppb.Now(),
						PingDelay: durationpb.New(streamPingDelay),
					},
				},
			})
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
	headerValue, err := ocpheaders.GetASCIIHeaderByName(ctx, internalRpcApiKeyHeaderName)
	if err != nil {
		s.log.Warn("Failure getting RPC API key header")
		return nil, status.Error(codes.Internal, "")
	}
	if _, ok := s.allInternalRpcApiKeys[headerValue]; !ok {
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

// todo: duplicated code with ForwardingClient
// todo: utilize batching by receiver to optimize internal forwarding RPC calls
func (s *Server) ForwardUserEvents(ctx context.Context, events ...*eventpb.UserEvent) error {
	var err error
	if !ocpheaders.AreHeadersInitialized(ctx) {
		ctx, err = ocpheaders.ContextWithHeaders(ctx)
		if err != nil {
			s.log.With(zap.Error(err)).Warn("Failure initializing headers")
			return err
		}
	}

	err = ocpheaders.SetASCIIHeader(ctx, internalRpcApiKeyHeaderName, s.currentRpcApiKey)
	if err != nil {
		s.log.With(zap.Error(err)).Warn("Failure setting RPC API key header")
		return err
	}

	for _, event := range events {
		go func() {
			ocpretry.Retry(
				func() error {
					return s.forwardUserEvent(ctx, event)
				},
				ocpretry.Limit(3),
				ocpretry.Backoff(ocpbackoff.BinaryExponential(100*time.Millisecond), 500*time.Millisecond),
			)
		}()
	}
	return nil
}

// todo: duplicated code with ForwardingClient
func (s *Server) forwardUserEvent(ctx context.Context, event *eventpb.UserEvent) error {
	log := s.log.With(
		zap.String("event_id", EventIDString(event.Event.Id)),
		zap.String("user_id", model.UserIDString(event.UserId)),
	)

	streamKey := model.UserIDString(event.UserId)

	rendezvous, err := s.events.GetRendezvous(ctx, streamKey)
	switch err {
	case nil:
		log = log.With(zap.String("receiver_address", rendezvous.Address))

		// Expired rendezvous record that likely wasn't cleaned up. Avoid forwarding,
		// since we expect a broken state.
		if time.Since(rendezvous.ExpiresAt) >= 0 {
			log.With(zap.Error(err)).Debug("Dropping event with expired rendezvous record")
			return nil
		}

		// This server is hosting the user's event stream, no forwarding required
		if rendezvous.Address == s.broadcastAddress {
			s.streamsMu.RLock()
			stream, exists := s.streams[streamKey]
			s.streamsMu.RUnlock()

			if exists {
				cloned := proto.Clone(event.Event).(*eventpb.Event)
				if err := stream.Notify([]*eventpb.Event{cloned}, streamTimeout); err != nil {
					log.Warn("Failed to notify event on local stream", zap.Error(err))
				}
			}

			return nil
		}

		// Otherwise, forward it to the server hosting the user's stream
		forwardingRpcClient, err := getForwardingRpcClient(s.log, rendezvous.Address)
		if err != nil {
			log.With(zap.Error(err)).Warn("Failure creating forwarding RPC client")
			return err
		}

		ctx, cancel := context.WithTimeout(ctx, forwardRpcTimeout)
		defer cancel()

		log.Debug("Forwarding events over RPC")

		resp, err := forwardingRpcClient.ForwardEvents(ctx, &eventpb.ForwardEventsRequest{
			UserEvents: &eventpb.UserEventBatch{
				Events: []*eventpb.UserEvent{event},
			},
		})
		if err != nil {
			log.With(zap.Error(err)).Warn("Failure forwarding event over RPC")
			return err
		} else if resp.Result != eventpb.ForwardEventsResponse_OK {
			log.With(zap.String("result", resp.Result.String())).Warn("Failure forwarding event over RPC")
			return errors.Errorf("rpc forward result %s", resp.Result)
		}

	case ErrRendezvousNotFound:
		log.Debug("Dropping event without rendezvous record")

	default:
		log.With(zap.Error(err)).Warn("Failed to get rendezvous record")
		return err
	}

	return nil
}

func (s *Server) OnEvent(userID *commonpb.UserId, e *eventpb.Event) {
	s.ForwardUserEvents(context.Background(), &eventpb.UserEvent{UserId: userID, Event: e})
}
