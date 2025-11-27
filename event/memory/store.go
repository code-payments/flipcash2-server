package memory

import (
	"context"
	"sync"
	"time"

	"github.com/code-payments/flipcash2-server/event"
)

type InMemoryStore struct {
	mu sync.RWMutex

	rendezvous []*event.Rendezvous
}

func NewInMemory() event.Store {
	return &InMemoryStore{}
}

func (s *InMemoryStore) CreateRendezvous(ctx context.Context, rendezvous *event.Rendezvous) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if item := s.findByKey(rendezvous.Key); item != nil {
		if item.ExpiresAt.After(time.Now()) {
			return event.ErrRendezvousExists
		}

		item.Address = rendezvous.Address
		item.ExpiresAt = rendezvous.ExpiresAt
	} else {
		s.rendezvous = append(s.rendezvous, rendezvous.Clone())
	}

	return nil
}

func (s *InMemoryStore) GetRendezvous(ctx context.Context, key string) (*event.Rendezvous, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	res := s.findByKey(key)
	if res == nil {
		return nil, event.ErrRendezvousNotFound
	}

	if res.ExpiresAt.Before(time.Now()) {
		return nil, event.ErrRendezvousNotFound
	}

	return res.Clone(), nil
}

func (s *InMemoryStore) ExtendRendezvousExpiry(ctx context.Context, key, address string, expiresAt time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	item := s.findByKeyAndAddress(key, address)
	if item == nil {
		return event.ErrRendezvousNotFound
	}

	if item.ExpiresAt.Before(time.Now()) {
		return event.ErrRendezvousNotFound
	}

	item.ExpiresAt = expiresAt

	return nil
}

func (s *InMemoryStore) DeleteRendezvous(ctx context.Context, key, address string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	for i, item := range s.rendezvous {
		if item.Key == key && item.Address == address {
			s.rendezvous = append(s.rendezvous[:i], s.rendezvous[i+1:]...)
			return nil
		}
	}

	return nil
}

func (s *InMemoryStore) findByKey(key string) *event.Rendezvous {
	for _, item := range s.rendezvous {
		if item.Key == key {
			return item
		}
	}

	return nil
}

func (s *InMemoryStore) findByKeyAndAddress(key, address string) *event.Rendezvous {
	for _, item := range s.rendezvous {
		if item.Key == key && item.Address == address {
			return item
		}
	}

	return nil
}

func (s *InMemoryStore) reset() {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.rendezvous = nil
}
