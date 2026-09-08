package event

import (
	"context"
	"sync"
	"time"

	"github.com/pkg/errors"
	"go.uber.org/zap"

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

// userEventForwarder routes user events to every server hosting one of the
// user's event streams, resolved through the cluster subscription registry. It
// is the single implementation shared by Server (which also delivers to its
// own local streams) and ForwardingClient (which never has local streams).
//
// Delivery is best-effort by the subscription layer's contract: a cached
// resolution may briefly miss a just-opened stream or forward toward a
// just-closed one. The client's delta sync on stream open is the backstop.
type userEventForwarder struct {
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
	deliverLocal   func(streamKey string, e *eventpb.Event)
}

func (f *userEventForwarder) ForwardUserEvents(ctx context.Context, events ...*eventpb.UserEvent) error {
	ctx, err := internalrpc.WithAPIKey(ctx, f.apiKey)
	if err != nil {
		f.log.With(zap.Error(err)).Warn("Failure setting internal RPC auth")
		return err
	}

	for _, event := range events {
		go f.fanOutUserEvent(ctx, event)
	}
	return nil
}

func (f *userEventForwarder) fanOutUserEvent(ctx context.Context, event *eventpb.UserEvent) {
	log := f.log.With(
		zap.String("event_id", EventIDString(event.Event.Id)),
		zap.String("user_id", model.UserIDString(event.UserId)),
	)

	var subscribers []*cluster.Subscription
	_, err := ocp_retry.Retry(
		func() error {
			var err error
			subscribers, err = f.subscriptions.Subscribers(ctx, UserEventsNamespace, event.UserId.Value)
			return err
		},
		ocp_retry.Limit(3),
		ocp_retry.Backoff(ocp_backoff.BinaryExponential(100*time.Millisecond), 500*time.Millisecond),
	)
	if err != nil {
		log.With(zap.Error(err)).Warn("Failure resolving event stream subscribers")
		return
	}

	if len(subscribers) == 0 {
		log.Debug("Dropping event without stream subscribers")
		return
	}

	for _, subscriber := range subscribers {
		// This server hosts streams for the user; no RPC required.
		if f.deliverLocal != nil && subscriber.InstanceID == f.selfInstanceID {
			f.deliverLocal(model.UserIDString(event.UserId), event.Event)
			continue
		}

		go func() {
			log := log.With(zap.String("receiver_address", subscriber.Address))
			_, err := ocp_retry.Retry(
				func() error {
					return f.forwardUserEvent(ctx, subscriber.Address, event)
				},
				ocp_retry.Limit(3),
				ocp_retry.Backoff(ocp_backoff.BinaryExponential(100*time.Millisecond), 500*time.Millisecond),
			)
			if err != nil {
				log.With(zap.Error(err)).Warn("Failure forwarding event over RPC")
			}
		}()
	}
}

func (f *userEventForwarder) forwardUserEvent(ctx context.Context, address string, event *eventpb.UserEvent) error {
	conn, err := f.pool.Conn(address)
	if err != nil {
		return errors.Wrap(err, "failure creating forwarding rpc client")
	}

	ctx, cancel := context.WithTimeout(ctx, forwardRpcTimeout)
	defer cancel()

	f.log.Debug("Forwarding events over RPC", zap.String("receiver_address", address))

	resp, err := eventpb.NewEventStreamingClient(conn).ForwardEvents(ctx, &eventpb.ForwardEventsRequest{
		UserEvents: &eventpb.UserEventBatch{
			Events: []*eventpb.UserEvent{event},
		},
	})
	if err != nil {
		return err
	} else if resp.Result != eventpb.ForwardEventsResponse_OK {
		return errors.Errorf("rpc forward result %s", resp.Result)
	}
	return nil
}
