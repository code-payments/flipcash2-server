package event

import (
	"context"
	"time"

	"github.com/pkg/errors"
	"go.uber.org/zap"

	eventpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/event/v1"

	"github.com/code-payments/flipcash2-server/model"
	ocpheaders "github.com/code-payments/ocp-server/grpc/headers"
	ocpretry "github.com/code-payments/ocp-server/retry"
	ocpbackoff "github.com/code-payments/ocp-server/retry/backoff"
)

type Forwarder interface {
	ForwardUserEvents(ctx context.Context, events ...*eventpb.UserEvent) error
}

type ForwardingClient struct {
	log *zap.Logger

	events Store

	currentRpcApiKey string
}

func NewForwardingClient(log *zap.Logger, events Store, currentRpcApiKey string) Forwarder {
	return &ForwardingClient{
		log: log,

		events: events,

		currentRpcApiKey: currentRpcApiKey,
	}
}

// todo: duplicated code with ForwardingClient
func (c *ForwardingClient) ForwardUserEvents(ctx context.Context, events ...*eventpb.UserEvent) error {
	var err error
	if !ocpheaders.AreHeadersInitialized(ctx) {
		ctx, err = ocpheaders.ContextWithHeaders(ctx)
		if err != nil {
			c.log.With(zap.Error(err)).Warn("Failure initializing headers")
			return err
		}
	}

	err = ocpheaders.SetASCIIHeader(ctx, internalRpcApiKeyHeaderName, c.currentRpcApiKey)
	if err != nil {
		c.log.With(zap.Error(err)).Warn("Failure setting RPC API key header")
		return err
	}

	for _, event := range events {
		go func() {
			ocpretry.Retry(
				func() error {
					return c.forwardUserEvent(ctx, event)
				},
				ocpretry.Limit(3),
				ocpretry.Backoff(ocpbackoff.BinaryExponential(100*time.Millisecond), 500*time.Millisecond),
			)
		}()
	}
	return nil
}

// todo: duplicated code with ForwardingClient
func (c *ForwardingClient) forwardUserEvent(ctx context.Context, event *eventpb.UserEvent) error {
	log := c.log.With(
		zap.String("event_id", EventIDString(event.Event.Id)),
		zap.String("user_id", model.UserIDString(event.UserId)),
	)

	streamKey := model.UserIDString(event.UserId)

	rendezvous, err := c.events.GetRendezvous(ctx, streamKey)
	switch err {
	case nil:
		log = log.With(zap.String("receiver_address", rendezvous.Address))

		// Expired rendezvous record that likely wasn't cleaned up. Avoid forwarding,
		// since we expect a broken state.
		if time.Since(rendezvous.ExpiresAt) >= 0 {
			log.With(zap.Error(err)).Debug("Dropping event with expired rendezvous record")
			return nil
		}

		// Forward the event to the server hosting the user's stream
		forwardingRpcClient, err := getForwardingRpcClient(c.log, rendezvous.Address)
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
