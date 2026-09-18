package event

import (
	"context"
	"encoding/hex"
	"errors"
	"strings"
	"sync"
	"time"

	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	chatpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/chat/v1"
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
const userStreamKeyPrefix = "user:"

func userStreamKey(userID *commonpb.UserId) string {
	return userStreamKeyPrefix + model.UserIDString(userID)
}

func chatStreamKey(chatID *commonpb.ChatId) string {
	return "chat:" + hex.EncodeToString(chatID.GetValue())
}

const (
	// maxEventBatchSize and maxEventBatchBytes cap the events coalesced into
	// one stream message. The count sits under the EventBatch proto limit
	// (1024); the byte budget keeps a batch of the largest events — a maximal
	// text message rides twice in its update, ~32KB — well under the 4MB gRPC
	// default receive limit on clients, which a count cap alone cannot promise.
	// The drain stops once the budget is reached, so a batch overshoots it by
	// at most one event.
	maxEventBatchSize  = 256
	maxEventBatchBytes = 1 << 20

	// streamBufferSize is how far a stream's handler may fall behind the
	// publish rate before Notify closes it (see EventStream). The buffer holds
	// shared event pointers — 2KB per stream at this size — so the ceiling is
	// a burst-headroom choice, not a memory one. It stays under the delta
	// sync's reset threshold so a lag-closed client catches up on the cheap
	// path rather than a full reload.
	streamBufferSize   = 256
	streamPingDelay    = 5 * time.Second
	streamSendTimeout  = 5 * time.Second
	streamPongTimeout  = 2 * streamPingDelay
	streamInitTsWindow = 2 * time.Minute

	// subscriptionCloseTimeout bounds the registry cleanup on stream teardown,
	// which runs after the stream's own context is already done. It covers all
	// of a stream's registrations (user topic plus group topics) at once:
	// releases are refcount decrements, and the topics this stream was the
	// last local holder of go out as a single batched delete (CloseAll).
	subscriptionCloseTimeout = 2 * time.Second

	// forwardRpcTimeout bounds one forwarding RPC, which now carries a batch
	// (see outboxes): the receiver's cost is a non-blocking notify per local
	// stream per event, so even a full batch across thousands of streams is
	// tens of milliseconds, and the bound is for a wedged peer, not pacing.
	forwardRpcTimeout = time.Second

	// membershipSyncTimeout bounds the cluster registration a membership
	// transition makes or releases on behalf of a user's open streams (see
	// followMembership). It runs detached from any request, on its own budget.
	membershipSyncTimeout = 2 * time.Second

	// The membership reconcile's schedule (see reconcileMembership). The
	// interval is how often each user's memberships are re-read: the longest
	// a transition this server never saw can leave a stream on the wrong
	// topics, and the read rate a server pays for its streams — one index
	// query per user per interval, so a few thousand users cost a few reads
	// a second. The tick is the sweep's cadence, and with the interval sets
	// the per-tick ration of re-reads. Workers bounds the reads in flight at
	// once, whatever a backlog makes due together.
	defaultMembershipReconcileInterval = 5 * time.Minute
	defaultMembershipReconcileTick     = time.Second
	membershipReconcileWorkers         = 8
)

// ServerOption tunes a Server at construction.
type ServerOption func(*Server)

// WithMembershipReconcile overrides the membership reconcile's schedule (see
// reconcileMembership): how often each user's memberships are re-read, and
// the sweep's tick. Tests use it to run the reconcile fast; production keeps
// the defaults.
func WithMembershipReconcile(interval, tick time.Duration) ServerOption {
	return func(s *Server) {
		s.reconcileInterval = interval
		s.reconcileTick = tick
	}
}

// localStream is one open stream in the local registry, carrying the user it
// belongs to: a stream appears under several keys (its user's, plus one per
// chat), and chat-keyed delivery needs the owner to honor per-user exclusions,
// which the stream object itself cannot say.
type localStream struct {
	stream Stream[*eventpb.Event]
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

	// access answers a chat preview's viewer's standing in the chat it names
	// (see chatPreview) — the one Access the chat and messaging servers share,
	// so a non-member's admission is evaluated and remembered once whichever
	// service they read through.
	access *chat.Access

	subscriptions *cluster.Subscriptions

	userEventBus *Bus[*commonpb.UserId, *eventpb.Event]
	chatEventBus *Bus[*commonpb.ChatId, *eventpb.ChatEvent]

	// streams fans a topic out to every open local stream (see streamRegistry).
	// It also refuses new streams once Shutdown has begun, so a stream can't
	// slip in behind the closing sweep. sessions is the per-stream view of the
	// same registrations, by user, for moving a user's streams between chat
	// topics as their membership changes (see followMembership).
	streams                 *streamRegistry
	sessions                *streamSessions
	staleEventDetectorCtors []StaleEventDetectorCtor[*eventpb.Event]

	// The membership reconcile's schedule (see reconcileMembership) and the
	// channel closed to stop its sweep, once, on Shutdown.
	reconcileInterval time.Duration
	reconcileTick     time.Duration
	reconcileStop     chan struct{}
	reconcileStopOnce sync.Once

	// chatPreviewLifetime is how long a chat preview stream lives (see
	// chatPreview).
	chatPreviewLifetime time.Duration

	// self is the cluster member this process registers subscription rows as:
	// its instance ID recognizes our own rows on the publish path, its address
	// labels forwarded test events. Single-sourced from the cluster runtime so
	// stream registration and self-detection can never disagree.
	self *cluster.Member

	internalAuth *internalrpc.Authenticator
	forwarder    *eventForwarder

	eventpb.UnimplementedEventStreamingServer
}

// NewServer constructs the event streaming server and starts its membership
// reconcile sweep (see reconcileMembership) in the background. The sweep runs
// until Shutdown, which every caller must eventually invoke — a Server that is
// constructed and dropped without it leaks the sweep's goroutine.
//
// access should be the one chat.Access the chat and messaging servers are
// built on (see chat.NewAccess): a chat stream's viewer is admitted by the
// same standing, remembered in the same window, as their reads.
func NewServer(
	log *zap.Logger,
	authz auth.Authorizer,
	accounts account.Store,
	badges badge.Store,
	chats chat.Store,
	access *chat.Access,
	subscriptions *cluster.Subscriptions,
	userEventBus *Bus[*commonpb.UserId, *eventpb.Event],
	chatEventBus *Bus[*commonpb.ChatId, *eventpb.ChatEvent],
	staleEventDetectorCtors []StaleEventDetectorCtor[*eventpb.Event],
	currentRpcApiKey string,
	opts ...ServerOption,
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
		access:   access,

		subscriptions: subscriptions,

		userEventBus: userEventBus,
		chatEventBus: chatEventBus,

		streams:                 newStreamRegistry(),
		sessions:                newStreamSessions(),
		staleEventDetectorCtors: staleEventDetectorCtors,

		reconcileInterval: defaultMembershipReconcileInterval,
		reconcileTick:     defaultMembershipReconcileTick,
		reconcileStop:     make(chan struct{}),

		chatPreviewLifetime: defaultChatPreviewLifetime,

		self:         subscriptions.Self(),
		internalAuth: internalrpc.NewAuthenticator(currentRpcApiKey),
	}
	for _, opt := range opts {
		opt(s)
	}

	s.forwarder = newEventForwarder(log, subscriptions, currentRpcApiKey, s.self.InstanceID, s.deliverLocal)

	userEventBus.AddHandler(HandlerFunc[*commonpb.UserId, *eventpb.Event](s.OnEvent))
	chatEventBus.AddHandler(HandlerFunc[*commonpb.ChatId, *eventpb.ChatEvent](s.OnChatEvent))

	go s.reconcileMembership()

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

	// Params selects what the stream is for: the signing user's every event,
	// the contract that predates the target, or a time-bounded preview of one
	// group chat under a ViewMode (see chatPreview).
	if preview := params.GetChatPreview(); preview != nil {
		return s.streamChatPreview(ctx, log, stream, userID, preview)
	}
	return s.streamUserEvents(ctx, log, stream, userID)
}

// run drives an open, registered stream to its end: it batches what the
// delivery path notifies onto ss out to the client, keeps the stream alive
// with pings and ends it when the client stops answering, and returns when
// the stream is closed, the client goes, or the send fails. A chat preview
// (preview non-nil) additionally shapes every event for its viewer (see
// chatPreview.shape) and ends with STREAM_EXPIRED when its window closes,
// whatever the client is doing: pongs keep the stream healthy, not alive.
func (s *Server) run(ctx context.Context, log *zap.Logger, stream grpc.BidiStreamingServer[eventpb.StreamEventsRequest, eventpb.StreamEventsResponse], ss *EventStream[*eventpb.Event], preview *chatPreview) error {
	staleEventDetectors := make([]StaleEventDetector[*eventpb.Event], len(s.staleEventDetectorCtors))
	for i, ctor := range s.staleEventDetectorCtors {
		staleEventDetectors[i] = ctor()
	}

	// The stream's steady-state cost is fixed at open: one send goroutine, one
	// receive goroutine, a ping ticker and a pong deadline timer — nothing is
	// allocated per event or per ping, however long the stream lives.
	sender := protoutil.NewSender[eventpb.StreamEventsResponse](stream, streamSendTimeout)
	defer sender.Close()

	// A preview's window; a user stream has none, and a nil channel never
	// fires.
	var expired <-chan time.Time
	var shape func(*eventpb.Event) (*eventpb.Event, error)
	if preview != nil {
		expiry := time.NewTimer(time.Until(preview.expiresAt))
		defer expiry.Stop()
		expired = expiry.C
		shape = preview.shape
	}

	// The pong deadline starts before the first ping goes out, so it is the
	// earlier of the two whenever a silent client's deadline and a ping tick
	// coincide (see the ping case below).
	pongTimer := time.NewTimer(streamPongTimeout)
	defer pongTimer.Stop()

	pongs := protoutil.Heartbeats(stream, func(t *eventpb.StreamEventsRequest) bool {
		pong := t.GetPong()
		if pong == nil {
			return false
		}

		if ts := pong.GetTimestamp(); ts != nil {
			log.Debug("Received pong from client", zap.Duration("upstream_latency", time.Since(ts.AsTime())))
		}

		return true
	})

	sendPing := func() error {
		log.Debug("Sending ping to client")
		return sender.Send(ctx, &eventpb.StreamEventsResponse{
			Type: &eventpb.StreamEventsResponse_Ping{
				Ping: &eventpb.ServerPing{
					Timestamp: timestamppb.Now(),
					PingDelay: durationpb.New(streamPingDelay),
				},
			},
		})
	}

	// First ping immediately, then on the tick.
	if err := sendPing(); err != nil {
		log.Debug("Stream is unhealthy; aborting")
		return status.Error(codes.Aborted, "terminating unhealthy stream")
	}
	pingTicker := time.NewTicker(streamPingDelay)
	defer pingTicker.Stop()

	for {
		select {
		case first, ok := <-ss.Channel():
			if !ok {
				log.Debug("Stream closed; ending stream")
				return status.Error(codes.Aborted, "stream closed")
			}

			// Everything that queued up during the previous send goes out in
			// this one message, so a burst costs the client one receive and
			// the handler one send regardless of how many events it spans.
			batch, err := selectEvents(log, staleEventDetectors, shape, drainReady(ss.Channel(), first, maxEventBatchSize, maxEventBatchBytes, eventSize))
			if err != nil {
				return status.Error(codes.Internal, "failure shaping events for stream")
			}
			if batch == nil {
				continue
			}

			log.Debug("Sending events to client stream", zap.Int("events", len(batch.Events)))
			err = sender.Send(ctx, &eventpb.StreamEventsResponse{
				Type: &eventpb.StreamEventsResponse_Events{
					Events: batch,
				},
			})
			if err != nil {
				log.Info("Failed to send events to client stream", zap.Error(err))
				return err
			}
		case <-expired:
			log.Debug("Chat preview window closed; ending stream")
			return sender.Send(ctx, &eventpb.StreamEventsResponse{Type: &eventpb.StreamEventsResponse_Error{
				Error: &eventpb.StreamEventsResponse_StreamError{Code: eventpb.StreamEventsResponse_StreamError_STREAM_EXPIRED},
			}})
		case <-pingTicker.C:
			// A pong deadline that has already passed takes precedence over
			// the tick: a client that has gone silent for the whole window is
			// not owed one more ping.
			select {
			case <-pongTimer.C:
				log.Debug("Stream is unhealthy; aborting")
				return status.Error(codes.Aborted, "terminating unhealthy stream")
			default:
			}

			if err := sendPing(); err != nil {
				log.Debug("Stream is unhealthy; aborting")
				return status.Error(codes.Aborted, "terminating unhealthy stream")
			}
		case _, ok := <-pongs:
			if !ok {
				log.Debug("Stream is unhealthy; aborting")
				return status.Error(codes.Aborted, "terminating unhealthy stream")
			}
			pongTimer.Reset(streamPongTimeout)
		case <-pongTimer.C:
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
// delta sync. Idempotent. It also stops the membership reconcile's sweep; a
// reconcile in flight finishes against sessions that refuse it (see
// streamSession.close).
func (s *Server) Shutdown() {
	s.reconcileStopOnce.Do(func() { close(s.reconcileStop) })

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
//
// The one event pointer is handed to every target: nothing downstream mutates
// it (the handler only filters and marshals), so a copy per stream would buy
// nothing but an allocation per member. Notify never blocks, so a chat with
// thousands of local streams is delivered in one pass whatever any single
// client is doing — and that pass runs under the registry's read lock rather
// than over a snapshot, so it allocates nothing per event either.
func (s *Server) deliverLocal(streamKey string, e *eventpb.Event, exclude []*commonpb.UserId) {
	// A user-keyed delivery may be the user's own copy of a membership
	// transition, which moves their streams between chat topics. That runs
	// before the notify, so a departure stops chat-topic delivery to the
	// leaver's streams from this event on.
	if strings.HasPrefix(streamKey, userStreamKeyPrefix) {
		s.followMembership(streamKey, e)
	}

	s.streams.each(streamKey, exclude, func(stream Stream[*eventpb.Event]) {
		err := stream.Notify(e)
		switch {
		case err == nil:
		case errors.Is(err, errStreamClosed):
			// Closed (by lag or shutdown) but not yet removed by its handler,
			// which is winding down: expected, and brief.
			s.log.Debug("Skipping closed local stream", zap.String("stream_id", stream.ID()))
		default:
			s.log.With(zap.Error(err)).Warn(
				"Failed to notify event on local stream",
				zap.String("stream_key", streamKey),
				zap.String("stream_id", stream.ID()),
			)
		}
	})
}

// followMembership keeps a user's open streams on the chat topics of the
// groups they are a member of, driven by the user's own copy of each
// membership transition: a RosterUpdate naming them as the member who joined
// or left, delivered on their user topic (see chat.Server's publishRosterUpdate).
// That copy reaches exactly the servers hosting the user's streams — locally
// off the bus, or forwarded — so every such server adjusts its own streams
// with no further fan-out.
//
// A join puts each of the user's local streams under the chat's key and
// registers this server's interest in the topic; a departure takes them out
// from under the key at once and releases the registration. The local
// registration leads and the cluster one follows on a detached goroutine, as
// a stream open does, so a publish that resolves this server's row always
// finds the streams behind it, and a leaver hears nothing further on the
// topic from the moment their departure is delivered — events published in
// between the store's transition and this delivery are the only ones that
// can still reach them.
//
// Events on the chat's topic published between the join and the registration
// landing are not delivered live to the joiner; the MemberJoined carries the
// chat's metadata, and the client's delta sync from there is the backstop, as
// it is after any reconnect. A transition addressed to someone else, or on a
// DM (which has no chat topic), is ignored.
//
// Transitions are applied by roster version, not arrival order (see
// streamSession): a stale copy still reaches the client — which applies it
// by the same rule — but moves no stream.
func (s *Server) followMembership(streamKey string, e *eventpb.Event) {
	update := e.GetChatUpdate()
	if update == nil || !chat.IsGroupChatID(update.GetChat()) {
		return
	}
	for _, roster := range update.GetRosterUpdates().GetRosterUpdates() {
		var subject *commonpb.UserId
		var joined bool
		switch kind := roster.GetKind().(type) {
		case *chatpb.RosterUpdate_MemberJoined_:
			subject, joined = kind.MemberJoined.GetMember().GetUserId(), true
		case *chatpb.RosterUpdate_MemberLeft_:
			subject = kind.MemberLeft.GetUserId()
		default:
			continue
		}
		if subject == nil || userStreamKey(subject) != streamKey {
			continue
		}
		sessions := s.sessions.forUser(streamKey)
		if len(sessions) == 0 {
			continue
		}
		version := roster.GetRosterSummary().GetVersion()
		if joined {
			go s.joinStreams(sessions, update.GetChat(), version)
		} else {
			s.leaveStreams(sessions, update.GetChat(), version)
		}
	}
}

// joinStreams attaches the sessions to the chat's topic. One cluster
// registration per stream: the subscription layer refcounts them, so only the
// first costs a row write. A registration failure stops the pass: the store
// is what failed, and the sessions not yet attached recorded nothing, so a
// later copy of the join — or the reconcile — attaches them.
func (s *Server) joinStreams(sessions []*streamSession, chatID *commonpb.ChatId, version uint64) {
	ctx, cancel := context.WithTimeout(context.Background(), membershipSyncTimeout)
	defer cancel()

	chatKey := chatStreamKey(chatID)
	for _, session := range sessions {
		if _, err := s.attachSession(ctx, session, chatID, chatKey, version); err != nil {
			return
		}
	}
}

// attachSession applies a join at the given roster version to one session:
// the local registration first (see streamSession.attach), then the cluster
// one, whose handle the session takes — or, if it ended or left the chat
// meanwhile, releases. It reports whether the session was attached, and the
// registration error if there was one, after undoing the attach so the join
// can be retried.
func (s *Server) attachSession(ctx context.Context, session *streamSession, chatID *commonpb.ChatId, chatKey string, version uint64) (bool, error) {
	if !session.attach(chatKey, version) {
		return false, nil
	}
	handle, err := s.subscriptions.Subscribe(ctx, ChatEventsNamespace, chatID.Value)
	if err != nil {
		session.rollback(chatKey, version)
		if !errors.Is(err, cluster.ErrSubscriptionsDraining) {
			s.log.With(zap.Error(err), zap.String("chat_id", model.ChatIDString(chatID))).Warn("Failure registering stream subscription for joined chat")
		}
		return false, err
	}
	if !session.setHandle(chatKey, handle) {
		if err := handle.Close(ctx); err != nil {
			s.log.With(zap.Error(err), zap.String("chat_id", model.ChatIDString(chatID))).Warn("Failed to close orphaned stream subscription")
		}
	}
	return true, nil
}

// leaveStreams detaches the sessions from the chat's topic synchronously,
// then releases their registrations together off the caller's path.
func (s *Server) leaveStreams(sessions []*streamSession, chatID *commonpb.ChatId, version uint64) {
	chatKey := chatStreamKey(chatID)
	var handles []*cluster.SubscriptionHandle
	for _, session := range sessions {
		if handle := session.detach(chatKey, version); handle != nil {
			handles = append(handles, handle)
		}
	}
	if len(handles) == 0 {
		return
	}

	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), membershipSyncTimeout)
		defer cancel()
		s.releaseHandles(ctx, handles)
	}()
}

// releaseHandles closes the handles as one batch, logging a failure: the
// registrations are gone locally either way, and a row that fails to delete
// is swept once this member stops heartbeating.
func (s *Server) releaseHandles(ctx context.Context, handles []*cluster.SubscriptionHandle) {
	if err := s.subscriptions.CloseAll(ctx, handles); err != nil {
		s.log.With(zap.Error(err)).Warn("Failed to close released stream subscriptions")
	}
}

// reconcileMembership is the sweep that re-reads users' memberships from the
// store and brings their open streams' chat topics into line, the backstop
// for the transitions followMembership never sees. A stream open merges the
// records it reads with the transitions delivered to it, but a transition
// this server was not registered for — published before the row for the
// user's topic landed — reaches it only through the read, and the read may
// predate it, by timing or by the inverted membership index's replication
// lag. And the subject's own copy of a transition is best-effort: it can be
// dropped by a full outbox, or published to a subscriber set cached before
// this server's row landed, and then never arrives at all. Either way the
// stream stays on the wrong topics for its whole life, so every user with a
// stream open is re-read every interval, counted from their first open here.
//
// The sweep runs every tick over the users the index says are due (see
// streamSessions.due): a read per user, not per stream, applied to all their
// local sessions through the same version gate the transitions use, so a
// read older than what a session has already applied moves nothing, and one
// racing a live transition converges on the newer of the two. Reads are
// bounded to membershipReconcileWorkers in flight, and a tick's batch
// finishes before the next begins, so however many users a backlog makes
// due at once, the store sees at most that many concurrent queries from
// this server.
func (s *Server) reconcileMembership() {
	ticker := time.NewTicker(s.reconcileTick)
	defer ticker.Stop()

	for {
		select {
		case <-s.reconcileStop:
			return
		case now := <-ticker.C:
			// The ration: enough users per tick to cover the population once
			// per interval, at least one.
			population := s.sessions.count()
			quota := max(1, int((int64(population)*int64(s.reconcileTick)+int64(s.reconcileInterval)-1)/int64(s.reconcileInterval)))
			userKeys := s.sessions.due(now, s.reconcileInterval, quota)
			if len(userKeys) == 0 {
				continue
			}

			var wg sync.WaitGroup
			workers := make(chan struct{}, membershipReconcileWorkers)
			for _, userKey := range userKeys {
				workers <- struct{}{}
				wg.Add(1)
				go func(userKey string) {
					defer wg.Done()
					defer func() { <-workers }()
					s.reconcileUser(userKey)
				}(userKey)
			}
			wg.Wait()
		}
	}
}

// reconcileUser re-reads one user's memberships and applies each record to
// every local session of theirs as a transition at the record's version:
// attaching joined ones, detaching departed ones, releasing what the detaches
// freed as one batch. A chat the read does not mention is left alone — a
// session cannot hold a key for a membership that never existed, and a
// record is never deleted from under a key it did. The read failing leaves
// the user due for the next tick.
func (s *Server) reconcileUser(userKey string) {
	// The start is what markReconciled records: the read's view of the store
	// is current as of then, and the sweep orders users by it.
	startedAt := time.Now()
	sessions := s.sessions.forUser(userKey)
	if len(sessions) == 0 {
		return
	}
	userID := sessions[0].local.userID
	log := s.log.With(zap.String("user_id", model.UserIDString(userID)))

	ctx, cancel := context.WithTimeout(context.Background(), membershipSyncTimeout)
	defer cancel()

	memberships, err := s.chats.GetGroupMembershipsForUser(ctx, userID)
	if err != nil {
		log.With(zap.Error(err)).Warn("Failure reading group memberships for stream reconcile")
		return
	}

	var attached, detached int
	var handles []*cluster.SubscriptionHandle
	for _, m := range memberships {
		chatKey := chatStreamKey(m.ChatID)
		for _, session := range sessions {
			if m.Joined {
				ok, err := s.attachSession(ctx, session, m.ChatID, chatKey, m.Version)
				if err != nil {
					// The store is failing; what is done stays done, and the
					// next tick picks the user up again.
					s.releaseHandles(ctx, handles)
					return
				}
				if ok {
					attached++
				}
			} else if handle := session.detach(chatKey, m.Version); handle != nil {
				detached++
				handles = append(handles, handle)
			}
		}
	}
	s.releaseHandles(ctx, handles)
	s.sessions.markReconciled(userKey, startedAt)

	// A correction is the signal that a transition went unseen; how often
	// this logs is what says whether the schedule is right.
	if attached > 0 || detached > 0 {
		log.Info("Reconciled stream group memberships",
			zap.Int("streams", len(sessions)),
			zap.Int("attached", attached),
			zap.Int("detached", detached),
		)
	}
}

// selectEvents applies the stream's stale-event detectors to a drained batch,
// then the stream's shape to what survives — the event as the stream delivers
// it, or nil to drop it (see chatPreview.shape); a nil shape delivers every
// event as published — and gathers the result for the wire. It returns nil
// when nothing remains, and an error when shape fails on an event, which
// ends the stream: nothing the stream cannot shape for its viewer is sent.
func selectEvents(log *zap.Logger, detectors []StaleEventDetector[*eventpb.Event], shape func(*eventpb.Event) (*eventpb.Event, error), events []*eventpb.Event) (*eventpb.EventBatch, error) {
	eventsToSend := make([]*eventpb.Event, 0, len(events))
	for _, event := range events {
		if isStale(detectors, event) {
			log.Debug("Dropping stale event", zap.String("event_id", model.EventIDString(event.Id)))
			continue
		}
		if shape != nil {
			shaped, err := shape(event)
			if err != nil {
				log.With(zap.Error(err)).Warn("Failure shaping event for stream", zap.String("event_id", model.EventIDString(event.Id)))
				return nil, err
			}
			if shaped == nil {
				continue
			}
			event = shaped
		}
		eventsToSend = append(eventsToSend, event)
	}

	if len(eventsToSend) == 0 {
		return nil, nil
	}
	return &eventpb.EventBatch{Events: eventsToSend}, nil
}

// eventSize is the event's wire size. Generated messages cache it, so the
// marshal that follows reuses the computation rather than repeating it.
func eventSize(e *eventpb.Event) int {
	return proto.Size(e)
}

func isStale(detectors []StaleEventDetector[*eventpb.Event], event *eventpb.Event) bool {
	for _, detector := range detectors {
		if detector.ShouldDrop(event) {
			return true
		}
	}
	return false
}

func (s *Server) OnEvent(userID *commonpb.UserId, e *eventpb.Event) {
	s.ForwardUserEvents(context.Background(), &eventpb.UserEvent{UserId: userID, Event: e})
}

// OnChatEvent is the chat bus handler; the payload carries the chat ID and
// exclusions itself, so the bus key rides along only for the bus's shape.
func (s *Server) OnChatEvent(_ *commonpb.ChatId, e *eventpb.ChatEvent) {
	s.ForwardChatEvents(context.Background(), e)
}
