package event

import (
	"context"
	"sync"
	"time"

	"github.com/pkg/errors"
	"go.uber.org/zap"

	eventpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/event/v1"

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

// userEventForwarder routes user events to the server hosting each user's
// event stream, located via rendezvous records. It is the single
// implementation shared by Server (which also delivers to its own local
// streams) and ForwardingClient (which never has local streams).
type userEventForwarder struct {
	log    *zap.Logger
	events Store
	pool   *internalrpc.Pool
	apiKey string

	// selfAddress and deliverLocal short-circuit the RPC when the rendezvous
	// record points at this process. Zero-valued for pure forwarding clients.
	selfAddress  string
	deliverLocal func(streamKey string, e *eventpb.Event)
}

func (f *userEventForwarder) ForwardUserEvents(ctx context.Context, events ...*eventpb.UserEvent) error {
	ctx, err := internalrpc.WithAPIKey(ctx, f.apiKey)
	if err != nil {
		f.log.With(zap.Error(err)).Warn("Failure setting internal RPC auth")
		return err
	}

	for _, event := range events {
		go func() {
			ocp_retry.Retry(
				func() error {
					return f.forwardUserEvent(ctx, event)
				},
				ocp_retry.Limit(3),
				ocp_retry.Backoff(ocp_backoff.BinaryExponential(100*time.Millisecond), 500*time.Millisecond),
			)
		}()
	}
	return nil
}

func (f *userEventForwarder) forwardUserEvent(ctx context.Context, event *eventpb.UserEvent) error {
	log := f.log.With(
		zap.String("event_id", EventIDString(event.Event.Id)),
		zap.String("user_id", model.UserIDString(event.UserId)),
	)

	streamKey := model.UserIDString(event.UserId)

	rendezvous, err := f.events.GetRendezvous(ctx, streamKey)
	switch err {
	case nil:
		log = log.With(zap.String("receiver_address", rendezvous.Address))

		// Expired rendezvous record that likely wasn't cleaned up. Avoid forwarding,
		// since we expect a broken state.
		if time.Since(rendezvous.ExpiresAt) >= 0 {
			log.Debug("Dropping event with expired rendezvous record")
			return nil
		}

		// This server is hosting the user's event stream, no forwarding required
		if f.selfAddress != "" && rendezvous.Address == f.selfAddress {
			f.deliverLocal(streamKey, event.Event)
			return nil
		}

		// Otherwise, forward it to the server hosting the user's stream
		conn, err := f.pool.Conn(rendezvous.Address)
		if err != nil {
			log.With(zap.Error(err)).Warn("Failure creating forwarding RPC client")
			return err
		}

		ctx, cancel := context.WithTimeout(ctx, forwardRpcTimeout)
		defer cancel()

		log.Debug("Forwarding events over RPC")

		resp, err := eventpb.NewEventStreamingClient(conn).ForwardEvents(ctx, &eventpb.ForwardEventsRequest{
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
