package memory

import (
	"context"
	"sync"

	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"

	"github.com/code-payments/flipcash2-server/kyc/coinbase"
)

type memory struct {
	sync.Mutex

	// Map of userID -> the user's record.
	byUser map[string]*coinbase.CustomerRecord
}

// NewInMemory returns a coinbase.Store held in memory, for tests. It is safe
// for concurrent use and hands out copies of what it holds.
func NewInMemory() coinbase.Store {
	return &memory{
		byUser: make(map[string]*coinbase.CustomerRecord),
	}
}

func (m *memory) reset() {
	m.Lock()
	defer m.Unlock()

	m.byUser = make(map[string]*coinbase.CustomerRecord)
}

func (m *memory) PutCustomer(_ context.Context, record *coinbase.CustomerRecord) (*coinbase.CustomerRecord, bool, error) {
	if err := record.Validate(); err != nil {
		return nil, false, err
	}

	m.Lock()
	defer m.Unlock()

	if existing, ok := m.byUser[string(record.UserID.Value)]; ok {
		return existing.Clone(), false, nil
	}
	m.byUser[string(record.UserID.Value)] = record.Clone()
	return record.Clone(), true, nil
}

func (m *memory) GetCustomer(_ context.Context, userID *commonpb.UserId) (*coinbase.CustomerRecord, error) {
	m.Lock()
	defer m.Unlock()

	record, ok := m.byUser[string(userID.GetValue())]
	if !ok {
		return nil, coinbase.ErrNotFound
	}
	return record.Clone(), nil
}

func (m *memory) GetCustomerByID(_ context.Context, customerID string) (*coinbase.CustomerRecord, error) {
	m.Lock()
	defer m.Unlock()

	for _, record := range m.byUser {
		if record.CustomerID == customerID {
			return record.Clone(), nil
		}
	}
	return nil, coinbase.ErrNotFound
}
