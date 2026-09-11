package protoutil

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// fakeServerStream scripts SendMsg and RecvMsg: sends block until released
// (or complete immediately when release is nil), receives are fed from a
// channel of results.
type fakeServerStream struct {
	grpc.ServerStream

	release chan error   // each SendMsg waits for one value; nil → returns nil at once
	recvs   chan recvRes // each RecvMsg takes one value
	sent    chan any
}

type recvRes struct {
	value int
	err   error
}

func (f *fakeServerStream) Context() context.Context { return context.Background() }

func (f *fakeServerStream) SendMsg(m any) error {
	if f.sent != nil {
		f.sent <- m
	}
	if f.release == nil {
		return nil
	}
	return <-f.release
}

func (f *fakeServerStream) RecvMsg(m any) error {
	r, ok := <-f.recvs
	if !ok {
		return errors.New("closed")
	}
	if r.err != nil {
		return r.err
	}
	*(m.(*int)) = r.value
	return nil
}

func TestSender_SendsInOrderWithoutTimingOut(t *testing.T) {
	stream := &fakeServerStream{sent: make(chan any, 8)}
	s := NewSender[int](stream, time.Second)
	defer s.Close()

	for i := range 3 {
		v := i
		require.NoError(t, s.Send(context.Background(), &v))
		require.Equal(t, &v, <-stream.sent)
	}
}

// TestSender_TimeoutAbandonsStuckSend pins the stuck-client contract: a send
// the peer never drains returns DeadlineExceeded after the timeout without
// the handler being wedged, and the sender refuses further sends since the
// stuck one still owns the stream.
func TestSender_TimeoutAbandonsStuckSend(t *testing.T) {
	stream := &fakeServerStream{release: make(chan error)}
	s := NewSender[int](stream, 20*time.Millisecond)
	defer s.Close()

	v := 1
	start := time.Now()
	err := s.Send(context.Background(), &v)
	require.Equal(t, codes.DeadlineExceeded, status.Code(err))
	require.Less(t, time.Since(start), time.Second)

	require.ErrorIs(t, s.Send(context.Background(), &v), ErrSenderAbandoned)

	// Releasing the stuck send lets the goroutine finish and exit on Close.
	stream.release <- nil
}

func TestSender_CancelledContext(t *testing.T) {
	stream := &fakeServerStream{release: make(chan error)}
	s := NewSender[int](stream, time.Minute)
	defer s.Close()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	v := 1
	require.Equal(t, codes.Canceled, status.Code(s.Send(ctx, &v)))
	require.ErrorIs(t, s.Send(context.Background(), &v), ErrSenderAbandoned)
	stream.release <- nil
}

func TestSender_SurfacesSendError(t *testing.T) {
	stream := &fakeServerStream{release: make(chan error, 1)}
	s := NewSender[int](stream, time.Second)
	defer s.Close()

	stream.release <- errors.New("broken pipe")
	v := 1
	require.EqualError(t, s.Send(context.Background(), &v), "broken pipe")

	// An ordinary failure does not abandon the sender; the next send is
	// attempted (and here succeeds).
	stream.release <- nil
	require.NoError(t, s.Send(context.Background(), &v))
}

// TestHeartbeats pins the liveness signal: each accepted message beats once,
// beats coalesce rather than block the receiver, and an error or a rejected
// message closes the channel.
func TestHeartbeats(t *testing.T) {
	stream := &fakeServerStream{recvs: make(chan recvRes, 8)}
	beats := Heartbeats(stream, func(v *int) bool { return *v > 0 })

	stream.recvs <- recvRes{value: 1}
	select {
	case _, ok := <-beats:
		require.True(t, ok)
	case <-time.After(time.Second):
		t.Fatal("no heartbeat")
	}

	// Several accepted messages while the handler is away coalesce to one
	// pending beat; the receive loop never waits on the handler.
	for range 5 {
		stream.recvs <- recvRes{value: 1}
	}
	require.Eventually(t, func() bool { return len(stream.recvs) == 0 }, time.Second, time.Millisecond)
	_, ok := <-beats
	require.True(t, ok)
	select {
	case <-beats:
		t.Fatal("beats did not coalesce")
	default:
	}

	// A rejected message ends the loop.
	stream.recvs <- recvRes{value: 0}
	select {
	case _, ok := <-beats:
		require.False(t, ok)
	case <-time.After(time.Second):
		t.Fatal("channel not closed on invalid message")
	}
}

func TestHeartbeats_ReceiveError(t *testing.T) {
	stream := &fakeServerStream{recvs: make(chan recvRes, 1)}
	beats := Heartbeats(stream, func(*int) bool { return true })
	stream.recvs <- recvRes{err: errors.New("eof")}
	select {
	case _, ok := <-beats:
		require.False(t, ok)
	case <-time.After(time.Second):
		t.Fatal("channel not closed on receive error")
	}
}
