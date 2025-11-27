package protoutil

import (
	"context"
	"time"

	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

const streamKeepAliveRecvTimeout = 10 * time.Second

type Ptr[T any] interface {
	proto.Message
	*T
}

func BoundedReceive[Req any](
	ctx context.Context,
	stream grpc.ServerStream,
	timeout time.Duration,
) (*Req, error) {
	var err error
	var req = new(Req)
	doneCh := make(chan struct{})

	go func() {
		err = stream.RecvMsg(req)
		close(doneCh)
	}()

	select {
	case <-doneCh:
		return req, err
	case <-ctx.Done():
		return req, status.Error(codes.Canceled, "")
	case <-time.After(timeout):
		return req, status.Error(codes.DeadlineExceeded, "timeout receiving message")
	}
}

func MonitorStreamHealth[Req any](
	ctx context.Context,
	log *zap.Logger,
	streamer grpc.ServerStream,
	validFn func(*Req) bool,
) <-chan struct{} {
	healthCh := make(chan struct{})
	go func() {
		defer close(healthCh)

		for {
			req, err := BoundedReceive[Req](ctx, streamer, streamKeepAliveRecvTimeout)
			if err != nil {
				return
			}

			if !validFn(req) {
				return
			}

			log.Debug("receiving pong from client")
		}
	}()
	return healthCh
}
