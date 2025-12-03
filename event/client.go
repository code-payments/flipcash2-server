package event

import (
	"sync"
	"time"

	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials/insecure"

	eventpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/event/v1"

	ocp_headers "github.com/code-payments/ocp-server/grpc/headers"
	ocp_validation "github.com/code-payments/ocp-server/grpc/protobuf/validation"
)

// todo: Generic utility for handling gRPC connections like this

var (
	forwardingClientConnsMu sync.RWMutex
	forwardingClientConns   map[string]*grpc.ClientConn
)

func init() {
	forwardingClientConns = make(map[string]*grpc.ClientConn)

	go periodicallyCleanupConns()
}

func getForwardingRpcClient(log *zap.Logger, address string) (eventpb.EventStreamingClient, error) {
	forwardingClientConnsMu.RLock()
	existing, ok := forwardingClientConns[address]
	if ok {
		forwardingClientConnsMu.RUnlock()
		return eventpb.NewEventStreamingClient(existing), nil
	}
	forwardingClientConnsMu.RUnlock()

	forwardingClientConnsMu.Lock()
	defer forwardingClientConnsMu.Unlock()

	existing, ok = forwardingClientConns[address]
	if ok {
		return eventpb.NewEventStreamingClient(existing), nil
	}

	conn, err := grpc.NewClient(
		address,

		grpc.WithTransportCredentials(insecure.NewCredentials()),

		grpc.WithUnaryInterceptor(ocp_validation.UnaryClientInterceptor(log)),
		grpc.WithUnaryInterceptor(ocp_headers.UnaryClientInterceptor()),

		grpc.WithStreamInterceptor(ocp_validation.StreamClientInterceptor(log)),
		grpc.WithStreamInterceptor(ocp_headers.StreamClientInterceptor()),
	)
	if err != nil {
		return nil, err
	}

	forwardingClientConns[address] = conn
	return eventpb.NewEventStreamingClient(conn), nil
}

func periodicallyCleanupConns() {
	for {
		time.Sleep(time.Minute)

		forwardingClientConnsMu.Lock()

		for target, conn := range forwardingClientConns {
			state := conn.GetState()
			switch state {
			case connectivity.TransientFailure, connectivity.Shutdown:
				conn.Close()
				delete(forwardingClientConns, target)
			}
		}

		forwardingClientConnsMu.Unlock()
	}
}
