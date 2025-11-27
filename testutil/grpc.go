package testutil

import (
	"context"
	"net"
	"testing"

	"google.golang.org/grpc/credentials/insecure"

	grpc_middleware "github.com/grpc-ecosystem/go-grpc-middleware"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/test/bufconn"

	"github.com/code-payments/ocp-server/pkg/grpc/headers"
	"github.com/code-payments/ocp-server/pkg/grpc/protobuf/validation"
)

func RunGRPCServer(t *testing.T, opts ...ServerOption) grpc.ClientConnInterface {
	lis := bufconn.Listen(1024 * 1024)

	cc, err := grpc.NewClient(
		"localhost:0",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return lis.DialContext(ctx)
		}),
	)
	require.NoError(t, err)

	o := serverOpts{
		unaryServerInterceptors: []grpc.UnaryServerInterceptor{
			headers.UnaryServerInterceptor(),
			validation.UnaryServerInterceptor(),
		},
		streamServerInterceptors: []grpc.StreamServerInterceptor{
			headers.StreamServerInterceptor(),
			validation.StreamServerInterceptor(),
		},
		unaryClientInterceptors: []grpc.UnaryClientInterceptor{
			validation.UnaryClientInterceptor(),
		},
		streamClientInterceptors: []grpc.StreamClientInterceptor{
			validation.StreamClientInterceptor(),
		},
	}

	for _, opt := range opts {
		opt(&o)
	}

	serv := grpc.NewServer(
		grpc.UnaryInterceptor(grpc_middleware.ChainUnaryServer(o.unaryServerInterceptors...)),
		grpc.StreamInterceptor(grpc_middleware.ChainStreamServer(o.streamServerInterceptors...)),
	)

	log, err := zap.NewDevelopment()
	require.NoError(t, err)
	for _, r := range o.registrants {
		r(serv)
	}

	go func() {
		if err := serv.Serve(lis); err != nil {
			log.Warn("Failed to shutdown test server", zap.Error(err))
		}
	}()

	t.Cleanup(func() {
		serv.Stop()
		if err := lis.Close(); err != nil {
			log.Warn("Failed to shutdown test listener", zap.Error(err))
		}
	})

	return cc
}

type serverOpts struct {
	registrants []func(*grpc.Server)

	unaryClientInterceptors  []grpc.UnaryClientInterceptor
	streamClientInterceptors []grpc.StreamClientInterceptor

	unaryServerInterceptors  []grpc.UnaryServerInterceptor
	streamServerInterceptors []grpc.StreamServerInterceptor
}

// ServerOption configures the settings when creating a test server.
type ServerOption func(o *serverOpts)

// WithUnaryClientInterceptor adds a unary client interceptor to the test client.
func WithUnaryClientInterceptor(i grpc.UnaryClientInterceptor) ServerOption {
	return func(o *serverOpts) {
		o.unaryClientInterceptors = append(o.unaryClientInterceptors, i)
	}
}

// WithStreamClientInterceptor adds a stream client interceptor to the test client.
func WithStreamClientInterceptor(i grpc.StreamClientInterceptor) ServerOption {
	return func(o *serverOpts) {
		o.streamClientInterceptors = append(o.streamClientInterceptors, i)
	}
}

// WithUnaryServerInterceptor adds a unary server interceptor to the test client.
func WithUnaryServerInterceptor(i grpc.UnaryServerInterceptor) ServerOption {
	return func(o *serverOpts) {
		o.unaryServerInterceptors = append(o.unaryServerInterceptors, i)
	}
}

// WithStreamServerInterceptor adds a stream server interceptor to the test client.
func WithStreamServerInterceptor(i grpc.StreamServerInterceptor) ServerOption {
	return func(o *serverOpts) {
		o.streamServerInterceptors = append(o.streamServerInterceptors, i)
	}
}

// WithService registers a function to be called in order to bind a service.
func WithService(f func(*grpc.Server)) ServerOption {
	return func(o *serverOpts) {
		o.registrants = append(o.registrants, f)
	}
}
