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

type Ptr[T any] interface {
	proto.Message
	*T
}

func BoundedReceive[Req any](
	ctx context.Context,
	stream grpc.ServerStream,
	timeout time.Duration,
) (*Req, error) {
	type result struct {
		req *Req
		err error
	}

	doneCh := make(chan result, 1)
	go func() {
		req := new(Req)
		err := stream.RecvMsg(req)
		doneCh <- result{req, err}
	}()

	select {
	case r := <-doneCh:
		return r.req, r.err
	case <-ctx.Done():
		return nil, status.Error(codes.Canceled, "")
	case <-time.After(timeout):
		return nil, status.Error(codes.DeadlineExceeded, "timeout receiving message")
	}
}

func BoundedSend[Resp any](
	ctx context.Context,
	stream grpc.ServerStream,
	msg *Resp,
	timeout time.Duration,
) error {
	doneCh := make(chan error, 1)
	go func() {
		doneCh <- stream.SendMsg(msg)
	}()

	select {
	case err := <-doneCh:
		return err
	case <-ctx.Done():
		return status.Error(codes.Canceled, "")
	case <-time.After(timeout):
		return status.Error(codes.DeadlineExceeded, "timeout sending message")
	}
}

func MonitorStreamHealth[Req any](
	ctx context.Context,
	log *zap.Logger,
	streamer grpc.ServerStream,
	recvTimeout time.Duration,
	validFn func(*Req) bool,
) <-chan struct{} {
	healthCh := make(chan struct{})
	go func() {
		defer close(healthCh)

		for {
			req, err := BoundedReceive[Req](ctx, streamer, recvTimeout)
			if err != nil {
				return
			}

			if !validFn(req) {
				return
			}
		}
	}()
	return healthCh
}
