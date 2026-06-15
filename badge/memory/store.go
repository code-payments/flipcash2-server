package memory

import (
	"context"
	"sync"

	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"

	"github.com/code-payments/flipcash2-server/badge"
)

type memory struct {
	sync.Mutex

	// Map of userID -> badge count.
	counts map[string]uint64
}

func NewInMemory() badge.Store {
	return &memory{
		counts: make(map[string]uint64),
	}
}

func (m *memory) reset() {
	m.Lock()
	defer m.Unlock()

	m.counts = make(map[string]uint64)
}

func (m *memory) Increment(_ context.Context, userID *commonpb.UserId, delta uint64) (uint64, error) {
	m.Lock()
	defer m.Unlock()

	updated := m.counts[string(userID.Value)] + delta
	m.counts[string(userID.Value)] = updated
	return updated, nil
}

func (m *memory) Get(_ context.Context, userID *commonpb.UserId) (uint64, error) {
	m.Lock()
	defer m.Unlock()

	return m.counts[string(userID.Value)], nil
}

func (m *memory) Reset(_ context.Context, userID *commonpb.UserId) error {
	m.Lock()
	defer m.Unlock()

	delete(m.counts, string(userID.Value))
	return nil
}
