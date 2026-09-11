package event

import (
	"context"

	"go.uber.org/zap"

	eventpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/event/v1"

	"github.com/code-payments/flipcash2-server/cluster"
)

type Forwarder interface {
	ForwardUserEvents(ctx context.Context, events ...*eventpb.UserEvent) error
	ForwardChatEvents(ctx context.Context, events ...*eventpb.ChatEvent) error
}

// ForwardingClient forwards events to the servers hosting their subscribed
// streams, for processes (or code paths) that host no event streams of their
// own.
type ForwardingClient struct {
	*eventForwarder
}

func NewForwardingClient(log *zap.Logger, subscriptions *cluster.Subscriptions, currentRpcApiKey string) Forwarder {
	return &ForwardingClient{
		eventForwarder: newEventForwarder(log, subscriptions, currentRpcApiKey, "", nil),
	}
}
