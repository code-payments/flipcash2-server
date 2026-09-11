package event

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestEventStream_NotifyNeverBlocks pins the lag contract: Notify fills the
// buffer without waiting, and the first event past capacity closes the stream
// instead of stalling the publisher — discarding the backlog, so the handler
// observes the close immediately.
func TestEventStream_NotifyNeverBlocks(t *testing.T) {
	s := NewEventStream[int]("s", 2)

	require.NoError(t, s.Notify(1))
	require.NoError(t, s.Notify(2))

	// No reader: this must return immediately, closing the stream.
	require.ErrorIs(t, s.Notify(3), ErrStreamLagging)
	require.ErrorIs(t, s.Notify(4), errStreamClosed)

	// The backlog is discarded with the close: the handler sees the close on
	// its next receive rather than flushing to a client that has fallen behind.
	_, ok := <-s.Channel()
	require.False(t, ok)

	// Close after a lag close is a no-op, not a double close.
	s.Close()
}

func TestEventStream_NotifyAfterClose(t *testing.T) {
	s := NewEventStream[int]("s", 4)
	s.Close()
	require.ErrorIs(t, s.Notify(1), errStreamClosed)
	_, ok := <-s.Channel()
	require.False(t, ok)
}

// TestDrainReady pins batching: everything already queued rides with the
// first event, capped at maxCount, and nothing is waited for.
func TestDrainReady(t *testing.T) {
	ch := make(chan int, 8)
	unit := func(int) int { return 1 }
	const noByteCap = 1 << 30

	// Nothing queued: the batch is just the first event.
	require.Equal(t, []int{0}, drainReady(ch, 0, 4, noByteCap, unit))

	for i := 1; i <= 6; i++ {
		ch <- i
	}

	// A queued burst is coalesced, up to maxCount; the rest waits for the
	// next batch.
	require.Equal(t, []int{0, 1, 2, 3}, drainReady(ch, 0, 4, noByteCap, unit))
	require.Equal(t, []int{4, 5, 6}, drainReady(ch, <-ch, 4, noByteCap, unit))
	require.Empty(t, ch)

	// A close mid-drain ends the batch with what was collected.
	ch <- 7
	close(ch)
	require.Equal(t, []int{0, 7}, drainReady(ch, 0, 4, noByteCap, unit))
	_, ok := <-ch
	require.False(t, ok)
}

// TestDrainReady_ByteBudget pins the size cap: the batch stops growing once
// it reaches maxBytes, overshooting by at most the event that crossed it, and
// the first event rides even when it alone exceeds the budget.
func TestDrainReady_ByteBudget(t *testing.T) {
	ch := make(chan int, 8)
	size := func(e int) int { return e }

	// 10 + 10 + 10 reaches a budget of 30; 10 more would be a fourth.
	for range 5 {
		ch <- 10
	}
	require.Equal(t, []int{10, 10, 10}, drainReady(ch, 10, 100, 30, size))
	require.Len(t, ch, 3)

	// Crossing the budget mid-event keeps that event (no peek) and stops.
	require.Equal(t, []int{10, 10, 10}, drainReady(ch, 10, 100, 25, size))
	require.Len(t, ch, 1)

	// An oversized first event goes out alone rather than never.
	require.Equal(t, []int{1000}, drainReady(ch, 1000, 100, 30, size))
	require.Len(t, ch, 1)
}
