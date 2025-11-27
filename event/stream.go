package event

import (
	"errors"
	"sync"
	"time"

	"google.golang.org/protobuf/proto"
)

type Stream[E any] interface {
	ID() string
	Notify(event E, timeout time.Duration) error
	Close()
}

type ProtoEventStream[E any, P proto.Message] struct {
	sync.Mutex

	id string

	closed   bool
	ch       chan P
	selector func(E) (P, bool)
}

func NewProtoEventStream[E any, P proto.Message](
	id string,
	bufferSize int,
	selector func(event E) (P, bool),
) *ProtoEventStream[E, P] {
	return &ProtoEventStream[E, P]{
		id:       id,
		ch:       make(chan P, bufferSize),
		selector: selector,
	}
}

func (s *ProtoEventStream[E, P]) ID() string {
	return s.id
}

func (s *ProtoEventStream[E, P]) Notify(event E, timeout time.Duration) error {
	msg, ok := s.selector(event)
	if !ok {
		return nil
	}

	s.Lock()
	if s.closed {
		s.Unlock()
		return errors.New("cannot notify closed stream")
	}

	select {
	case s.ch <- msg:
	case <-time.After(timeout):
		s.Unlock()
		s.Close()
		return errors.New("timed out sending message to streamCh")
	}

	s.Unlock()
	return nil
}

func (s *ProtoEventStream[E, P]) Channel() <-chan P {
	return s.ch
}

func (s *ProtoEventStream[E, P]) Close() {
	s.Lock()
	defer s.Unlock()

	if s.closed {
		return
	}

	s.closed = true
	close(s.ch)
}
