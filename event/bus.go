package event

import "sync"

type Handler[Key, Event any] interface {
	OnEvent(key Key, e Event)
}

// HandlerFunc is an adapter to allow the use of ordinary
// functions as Handlers.
type HandlerFunc[Key, Event any] func(Key, Event)

// OnEvent calls f(key, e).
func (f HandlerFunc[Key, Event]) OnEvent(key Key, e Event) {
	f(key, e)
}

type Bus[Key, Event any] struct {
	handlersMu sync.RWMutex
	handlers   []Handler[Key, Event]
}

func NewBus[Key, Event any]() *Bus[Key, Event] {
	return &Bus[Key, Event]{
		handlersMu: sync.RWMutex{},
		handlers:   nil,
	}
}

func (b *Bus[Key, Event]) AddHandler(h Handler[Key, Event]) {
	b.handlersMu.Lock()
	b.handlers = append(b.handlers, h)
	b.handlersMu.Unlock()
}

func (b *Bus[Key, Event]) OnEvent(key Key, e Event) {
	b.handlersMu.RLock()
	// Copy handlers to prevent race conditions
	handlers := make([]Handler[Key, Event], len(b.handlers))
	copy(handlers, b.handlers)
	b.handlersMu.RUnlock()

	// Execute handlers outside the lock
	for _, h := range handlers {
		go h.OnEvent(key, e)
	}
}
