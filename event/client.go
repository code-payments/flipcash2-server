package event

import (
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials/insecure"

	eventpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/event/v1"

	ocpheaders "github.com/code-payments/ocp-server/pkg/grpc/headers"
	ocpvalidation "github.com/code-payments/ocp-server/pkg/grpc/protobuf/validation"
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

func getForwardingRpcClient(address string) (eventpb.EventStreamingClient, error) {
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

		grpc.WithUnaryInterceptor(ocpvalidation.UnaryClientInterceptor()),
		grpc.WithUnaryInterceptor(ocpheaders.UnaryClientInterceptor()),

		grpc.WithStreamInterceptor(ocpvalidation.StreamClientInterceptor()),
		grpc.WithStreamInterceptor(ocpheaders.StreamClientInterceptor()),
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
