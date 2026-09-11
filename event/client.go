package event

import (
	"context"
	"encoding/hex"
	stderrors "errors"
	"sync"
	"time"

	"github.com/pkg/errors"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"

	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"
	eventpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/event/v1"

	"github.com/code-payments/flipcash2-server/cluster"
	"github.com/code-payments/flipcash2-server/cluster/internalrpc"
	"github.com/code-payments/flipcash2-server/model"
	ocp_retry "github.com/code-payments/ocp-server/retry"
	ocp_backoff "github.com/code-payments/ocp-server/retry/backoff"
)

// sharedForwardingPool is the process-wide connection pool for event
// forwarding, mirroring the old package-global: Server and ForwardingClient
// instances in one process share conns to the same peers, and the pool lives
// (deliberately unclosed) for the life of the process. First caller's logger
// wins for the client interceptors.
var (
	sharedPoolOnce sync.Once
	sharedPool     *internalrpc.Pool
)

func sharedForwardingPool(log *zap.Logger) *internalrpc.Pool {
	sharedPoolOnce.Do(func() {
		sharedPool = internalrpc.NewPool(log)
	})
	return sharedPool
}

// eventForwarder routes events to every server hosting a subscribed stream,
// resolved through the cluster subscription registry — user events by the
// user's topic, chat events by the chat's. It is the single implementation
// shared by Server (which also delivers to its own local streams) and
// ForwardingClient (which never has local streams).
//
// Remote delivery goes through a per-peer outbox (see outboxes): resolution
// enqueues, and the peer's sender batches what has queued into one RPC. The
// forwarder therefore never holds more goroutines than it has live peers.
//
// Delivery is best-effort by the subscription layer's contract: a cached
// resolution may briefly miss a just-opened stream or forward toward a
// just-closed one, and a full outbox drops. The client's delta sync on stream
// open is the backstop.
type eventForwarder struct {
	log           *zap.Logger
	subscriptions *cluster.Subscriptions
	pool          *internalrpc.Pool
	apiKey        string

	// selfInstanceID and deliverLocal short-circuit the RPC when a subscriber
	// row names this process. The ID comes from the subscriptions runtime's own
	// member — the same identity its rows carry — so the match is exact, never
	// a comparison of separately-configured addresses. Zero-valued for pure
	// forwarding clients.
	selfInstanceID string
	deliverLocal   func(streamKey string, e *eventpb.Event, exclude []*commonpb.UserId)

	outboxes *outboxes
}

func newEventForwarder(
	log *zap.Logger,
	subscriptions *cluster.Subscriptions,
	apiKey string,
	selfInstanceID string,
	deliverLocal func(streamKey string, e *eventpb.Event, exclude []*commonpb.UserId),
) *eventForwarder {
	f := &eventForwarder{
		log:            log,
		subscriptions:  subscriptions,
		pool:           sharedForwardingPool(log),
		apiKey:         apiKey,
		selfInstanceID: selfInstanceID,
		deliverLocal:   deliverLocal,
	}
	f.outboxes = newOutboxes(log, f.sendBatch, defaultOutboxConfig())
	return f
}

func (f *eventForwarder) ForwardUserEvents(ctx context.Context, events ...*eventpb.UserEvent) error {
	for _, event := range events {
		go f.fanOutUserEvent(ctx, event)
	}
	return nil
}

func (f *eventForwarder) fanOutUserEvent(ctx context.Context, event *eventpb.UserEvent) {
	log := f.log.With(
		zap.String("event_id", EventIDString(event.Event.Id)),
		zap.String("user_id", model.UserIDString(event.UserId)),
	)

	subscribers, ok := f.resolve(ctx, log, UserEventsNamespace, event.UserId.Value)
	if !ok {
		return
	}

	for _, subscriber := range subscribers {
		// This server hosts streams for the user; no RPC required.
		if f.deliverLocal != nil && subscriber.InstanceID == f.selfInstanceID {
			f.deliverLocal(userStreamKey(event.UserId), event.Event, nil)
			continue
		}

		// A full outbox drops; the outbox counts and reports it (see sweep).
		f.outboxes.enqueue(subscriber.Address, forwardItem{user: event})
	}
}

func (f *eventForwarder) ForwardChatEvents(ctx context.Context, events ...*eventpb.ChatEvent) error {
	for _, event := range events {
		go f.fanOutChatEvent(ctx, event)
	}
	return nil
}

func (f *eventForwarder) fanOutChatEvent(ctx context.Context, event *eventpb.ChatEvent) {
	log := f.log.With(
		zap.String("event_id", EventIDString(event.Event.Id)),
		zap.String("chat_id", hex.EncodeToString(event.ChatId.GetValue())),
	)

	// One resolution covers the whole chat, no matter how many members it
	// has: the topic's rows name servers, not users.
	subscribers, ok := f.resolve(ctx, log, ChatEventsNamespace, event.ChatId.Value)
	if !ok {
		return
	}

	for _, subscriber := range subscribers {
		// This server hosts streams for the chat; no RPC required.
		if f.deliverLocal != nil && subscriber.InstanceID == f.selfInstanceID {
			f.deliverLocal(chatStreamKey(event.ChatId), event.Event, event.ExcludeUserIds)
			continue
		}

		// A full outbox drops; the outbox counts and reports it (see sweep).
		f.outboxes.enqueue(subscriber.Address, forwardItem{chat: event})
	}
}

// resolve looks up a topic's subscribing servers, retrying the store read.
// It returns false when there is nothing to deliver to, having logged any
// failure.
func (f *eventForwarder) resolve(ctx context.Context, log *zap.Logger, namespace string, key []byte) ([]*cluster.Subscription, bool) {
	var subscribers []*cluster.Subscription
	_, err := ocp_retry.Retry(
		func() error {
			var err error
			subscribers, err = f.subscriptions.Subscribers(ctx, namespace, key)
			return err
		},
		ocp_retry.Limit(3),
		ocp_retry.Backoff(ocp_backoff.BinaryExponential(100*time.Millisecond), 500*time.Millisecond),
	)
	if err != nil {
		log.With(zap.Error(err)).Warn("Failure resolving event stream subscribers")
		return nil, false
	}

	if len(subscribers) == 0 {
		log.Debug("Dropping event without stream subscribers")
		return nil, false
	}
	return subscribers, true
}

// sendBatch is the outbox's send: one drained batch to one peer. The request
// carries one kind of event, so a mixed batch goes as up to two RPCs, each
// retried on its own; both are attempted even if the first fails.
func (f *eventForwarder) sendBatch(address string, items []forwardItem) error {
	ctx, err := internalrpc.WithAPIKey(context.Background(), f.apiKey)
	if err != nil {
		return errors.Wrap(err, "failure setting internal RPC auth")
	}

	users, chats := splitBatch(items)

	var errs []error
	if len(users) > 0 {
		if _, err := ocp_retry.Retry(func() error {
			return f.forwardUserEvents(ctx, address, users)
		}, forwardRetryStrategies()...); err != nil {
			errs = append(errs, errors.Wrapf(err, "%d user events", len(users)))
		}
	}
	if len(chats) > 0 {
		if _, err := ocp_retry.Retry(func() error {
			return f.forwardChatEvents(ctx, address, chats)
		}, forwardRetryStrategies()...); err != nil {
			errs = append(errs, errors.Wrapf(err, "%d chat events", len(chats)))
		}
	}
	return stderrors.Join(errs...)
}

// splitBatch separates a batch by kind, each in its queued order.
func splitBatch(items []forwardItem) (users []*eventpb.UserEvent, chats []*eventpb.ChatEvent) {
	for _, item := range items {
		if item.user != nil {
			users = append(users, item.user)
		} else {
			chats = append(chats, item.chat)
		}
	}
	return users, chats
}

// forwardRetryStrategies governs a forwarding RPC's retries. Delivery on the
// receiver is not idempotent — it notifies every local stream — so a retry is
// only safe when the first attempt provably never ran, which is what
// Unavailable means: the RPC failed to reach the server (connection refused
// or dropped before a response). Anything else is ambiguous or final. A
// deadline, in particular, may expire after the receiver has delivered, and
// re-sending would then hand every stream on that server a duplicate; the
// client's delta sync covers the rare genuine loss instead. DENIED and pool
// errors will not improve on repetition.
func forwardRetryStrategies() []ocp_retry.Strategy {
	return []ocp_retry.Strategy{
		ocp_retry.RetriableGRPCCodes(codes.Unavailable),
		ocp_retry.Limit(3),
		ocp_retry.Backoff(ocp_backoff.BinaryExponential(100*time.Millisecond), 500*time.Millisecond),
	}
}

func (f *eventForwarder) forwardChatEvents(ctx context.Context, address string, events []*eventpb.ChatEvent) error {
	return f.forward(ctx, address, &eventpb.ForwardEventsRequest{
		Type: &eventpb.ForwardEventsRequest_ChatEvents{
			ChatEvents: &eventpb.ChatEventBatch{Events: events},
		},
	})
}

func (f *eventForwarder) forwardUserEvents(ctx context.Context, address string, events []*eventpb.UserEvent) error {
	return f.forward(ctx, address, &eventpb.ForwardEventsRequest{
		Type: &eventpb.ForwardEventsRequest_UserEvents{
			UserEvents: &eventpb.UserEventBatch{Events: events},
		},
	})
}

func (f *eventForwarder) forward(ctx context.Context, address string, req *eventpb.ForwardEventsRequest) error {
	conn, err := f.pool.Conn(address)
	if err != nil {
		return errors.Wrap(err, "failure creating forwarding rpc client")
	}

	ctx, cancel := context.WithTimeout(ctx, forwardRpcTimeout)
	defer cancel()

	f.log.Debug("Forwarding events over RPC", zap.String("receiver_address", address))

	resp, err := eventpb.NewEventStreamingClient(conn).ForwardEvents(ctx, req)
	if err != nil {
		return err
	} else if resp.Result != eventpb.ForwardEventsResponse_OK {
		return errors.Errorf("rpc forward result %s", resp.Result)
	}
	return nil
}
