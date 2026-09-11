package event

import (
	"errors"
	"sync"
)

// ErrStreamLagging is returned by Notify when the stream's buffer is full. The
// stream is closed in the same call: its handler returns, the client
// reconnects and catches up with a delta sync, and no other stream on the
// server is delayed on its account.
var ErrStreamLagging = errors.New("stream lagging; closed")

var errStreamClosed = errors.New("cannot notify closed stream")

// Stream is one open client stream as the delivery path sees it.
type Stream[E any] interface {
	ID() string

	// Notify enqueues an event for the stream's handler without blocking: a
	// full buffer closes the stream and returns ErrStreamLagging rather than
	// waiting on it. Delivery to a topic with many streams therefore costs one
	// channel operation per stream, however slow any one client is.
	Notify(event E) error

	Close()
}

// EventStream is the bounded, non-blocking queue between the delivery path and
// a stream's handler goroutine. Events are enqueued individually and shared by
// pointer across every stream they are delivered to, so they must be treated
// as immutable once published; the handler drains whatever has accumulated
// into one batch per send (see drainReady).
type EventStream[E any] struct {
	mu sync.Mutex

	id string

	closed bool
	ch     chan E
}

func NewEventStream[E any](id string, bufferSize int) *EventStream[E] {
	return &EventStream[E]{
		id: id,
		ch: make(chan E, bufferSize),
	}
}

func (s *EventStream[E]) ID() string {
	return s.id
}

func (s *EventStream[E]) Notify(event E) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return errStreamClosed
	}

	select {
	case s.ch <- event:
		return nil
	default:
		// The handler has fallen a full buffer behind the publish rate. Cut it
		// off here instead of holding the publisher. The client will reconnect
		// and delta sync from its sequenced store, so nothing buffered is worth
		// pushing through a stream already too slow to keep up: drop it, so the
		// handler observes the close on its very next receive (after at most
		// the one send it may be in) and frees its registry slot promptly.
		// Draining under the mutex is safe — it excludes every other sender,
		// and a concurrent receive by the handler is just another reader.
		for len(s.ch) > 0 {
			<-s.ch
		}
		s.closeLocked()
		return ErrStreamLagging
	}
}

func (s *EventStream[E]) Channel() <-chan E {
	return s.ch
}

func (s *EventStream[E]) Close() {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.closeLocked()
}

func (s *EventStream[E]) closeLocked() {
	if s.closed {
		return
	}

	s.closed = true
	close(s.ch)
}

// drainReady collects first plus every event already waiting on ch, without
// blocking, into one batch of at most maxCount events. A burst that arrived
// while the handler was busy with the previous send goes out as a single
// message instead of one send per event.
//
// The batch also stops growing once its events' sizes (per size) reach
// maxBytes, so a run of large events cannot coalesce into a message the client
// refuses. The check runs after each append — a channel offers no peek — so a
// batch overshoots the budget by at most one event; first always rides
// regardless of size. A closed channel ends the batch early; the caller
// observes the close on its next receive.
func drainReady[E any](ch <-chan E, first E, maxCount, maxBytes int, size func(E) int) []E {
	events := make([]E, 1, min(maxCount, 1+len(ch)))
	events[0] = first
	bytes := size(first)

	for len(events) < maxCount && bytes < maxBytes {
		select {
		case e, ok := <-ch:
			if !ok {
				return events
			}
			events = append(events, e)
			bytes += size(e)
		default:
			return events
		}
	}
	return events
}
