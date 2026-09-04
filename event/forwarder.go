package event

import (
	"context"

	"go.uber.org/zap"

	eventpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/event/v1"
)

type Forwarder interface {
	ForwardUserEvents(ctx context.Context, events ...*eventpb.UserEvent) error
}

// ForwardingClient forwards user events to the servers hosting their streams,
// for processes (or code paths) that host no event streams of their own.
type ForwardingClient struct {
	*userEventForwarder
}

func NewForwardingClient(log *zap.Logger, events Store, currentRpcApiKey string) Forwarder {
	return &ForwardingClient{
		userEventForwarder: &userEventForwarder{
			log:    log,
			events: events,
			pool:   sharedForwardingPool(log),
			apiKey: currentRpcApiKey,
		},
	}
}
