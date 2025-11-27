package event

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type TestEventObserver[Key, Event any] struct {
	mu     sync.RWMutex
	events []*KeyAndEvent[Key, Event]
}

func NewTestEventObserver[Key, Event any]() *TestEventObserver[Key, Event] {
	return &TestEventObserver[Key, Event]{}
}

func (h *TestEventObserver[Key, Event]) OnEvent(key Key, event Event) {
	h.mu.Lock()
	defer h.mu.Unlock()

	h.events = append(h.events, &KeyAndEvent[Key, Event]{Key: key, Event: event})
}

func (h *TestEventObserver[Key, Event]) GetEvents(filter func(Key) bool) []*KeyAndEvent[Key, Event] {
	h.mu.RLock()
	defer h.mu.RUnlock()

	var res []*KeyAndEvent[Key, Event]
	for _, event := range h.events {
		if filter(event.Key) {
			res = append(res, event)
		}
	}
	return res
}

func (h *TestEventObserver[Key, Event]) WaitFor(t *testing.T, condition func([]*KeyAndEvent[Key, Event]) bool) {
	h.WaitForWithTimeout(t, 250*time.Millisecond, condition)
}

func (h *TestEventObserver[Key, Event]) WaitForWithTimeout(t *testing.T, timeout time.Duration, condition func([]*KeyAndEvent[Key, Event]) bool) {
	timeoutChan := time.After(timeout)
	for {
		select {
		case <-time.After(50 * time.Millisecond):
			h.mu.RLock()
			if condition(h.events) {
				h.mu.RUnlock()
				return
			}
			h.mu.RUnlock()
		case <-timeoutChan:
			require.Fail(t, "timed out waiting for event condition")
		}
	}
}

func (h *TestEventObserver[Key, Event]) Reset() {
	h.mu.Lock()
	defer h.mu.Unlock()

	h.events = nil
}
