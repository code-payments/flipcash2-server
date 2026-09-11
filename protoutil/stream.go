package protoutil

import (
	"context"
	"errors"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

type Ptr[T any] interface {
	proto.Message
	*T
}

// BoundedReceive waits up to timeout for one message. It spawns a goroutine
// for the receive, so it suits a one-off (a stream's opening request) rather
// than a receive loop — see Heartbeats for the latter.
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

// ErrSenderAbandoned is returned by Sender.Send once an earlier send timed out
// or was cancelled: that send may still be in flight on the goroutine, so the
// stream can no longer be written to in order. The handler should return.
var ErrSenderAbandoned = errors.New("stream sender abandoned after a timed-out send")

// Sender serializes a server stream's sends on one goroutine that lives as
// long as the stream, so bounding a send costs nothing per message: a stuck
// SendMsg cannot be interrupted from the handler (only the client going away
// or the handler returning ends it), so a goroutine is what lets the handler
// walk away from it — but one per stream, not one per send, and the timeout
// comes from a single timer reused across sends.
//
// Send is not safe for concurrent use: it is the handler loop's, which sends
// one message at a time.
type Sender[Resp any] struct {
	stream  grpc.ServerStream
	timeout time.Duration

	msgs    chan *Resp
	results chan error
	timer   *time.Timer

	abandoned bool
}

func NewSender[Resp any](stream grpc.ServerStream, timeout time.Duration) *Sender[Resp] {
	s := &Sender[Resp]{
		stream:  stream,
		timeout: timeout,
		msgs:    make(chan *Resp),
		results: make(chan error, 1),
		timer:   time.NewTimer(timeout),
	}
	s.timer.Stop()

	// Exits once msgs is closed and any in-flight SendMsg has returned — which,
	// for a send the handler abandoned, is when the stream itself ends.
	go func() {
		for msg := range s.msgs {
			s.results <- s.stream.SendMsg(msg)
		}
	}()

	return s
}

// Send writes msg, waiting at most the sender's timeout for it to complete.
// A timeout or cancellation abandons the send: it is left to finish on its
// own once the stream ends, and every later Send fails with
// ErrSenderAbandoned.
func (s *Sender[Resp]) Send(ctx context.Context, msg *Resp) error {
	if s.abandoned {
		return ErrSenderAbandoned
	}

	// The goroutine is idle whenever Send is entered (the previous send
	// completed, or Send was never called), so this hands off immediately.
	s.msgs <- msg
	s.timer.Reset(s.timeout)

	select {
	case err := <-s.results:
		s.timer.Stop()
		return err
	case <-ctx.Done():
		s.timer.Stop()
		s.abandoned = true
		return status.Error(codes.Canceled, "")
	case <-s.timer.C:
		s.abandoned = true
		return status.Error(codes.DeadlineExceeded, "timeout sending message")
	}
}

// Close lets the send goroutine exit. Call it when the handler returns; a send
// still in flight finishes (or is cut off by the stream ending) first.
func (s *Sender[Resp]) Close() {
	close(s.msgs)
}

// Heartbeats receives on the stream for its whole life on one goroutine,
// signalling on the returned channel for each message validFn accepts and
// closing it on a receive error or a rejected message. Signals coalesce (a
// buffer of one, never blocking the receive): a heartbeat is a liveness fact,
// not a count, so a handler busy elsewhere misses nothing it needs. Enforcing
// a heartbeat deadline is the handler's, with a timer it resets per signal —
// a blocked RecvMsg cannot be interrupted, so the timeout has to be observed
// where the handler can act on it.
func Heartbeats[Req any](stream grpc.ServerStream, validFn func(*Req) bool) <-chan struct{} {
	beats := make(chan struct{}, 1)
	go func() {
		defer close(beats)

		for {
			req := new(Req)
			if err := stream.RecvMsg(req); err != nil {
				return
			}
			if !validFn(req) {
				return
			}

			select {
			case beats <- struct{}{}:
			default:
			}
		}
	}()
	return beats
}
