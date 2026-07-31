package memory

import (
	"context"
	"encoding/hex"
	"fmt"
	"sync"

	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"
	thirdpartypb "github.com/code-payments/flipcash2-protobuf-api/generated/go/thirdparty/v1"

	"github.com/code-payments/flipcash2-server/kyc"
)

type memory struct {
	sync.Mutex

	records map[string]*kyc.Record // keyed by (user, partner)
}

// NewInMemory returns an in-memory kyc.Store, for tests.
func NewInMemory() kyc.Store {
	return &memory{
		records: make(map[string]*kyc.Record),
	}
}

func (m *memory) reset() {
	m.Lock()
	defer m.Unlock()

	m.records = make(map[string]*kyc.Record)
}

func (m *memory) Get(_ context.Context, userID *commonpb.UserId, partner thirdpartypb.Partner) (*kyc.Record, error) {
	m.Lock()
	defer m.Unlock()

	record, ok := m.records[recordKey(userID, partner)]
	if !ok {
		return nil, kyc.ErrNotFound
	}
	return record.Clone(), nil
}

func (m *memory) GetByCustomerID(_ context.Context, partner thirdpartypb.Partner, customerID string) (*kyc.Record, error) {
	m.Lock()
	defer m.Unlock()

	for _, record := range m.records {
		if record.Partner == partner && record.CustomerID == customerID {
			return record.Clone(), nil
		}
	}
	return nil, kyc.ErrNotFound
}

func (m *memory) Create(_ context.Context, record *kyc.Record) error {
	m.Lock()
	defer m.Unlock()

	key := recordKey(record.UserID, record.Partner)
	if _, ok := m.records[key]; ok {
		return kyc.ErrExists
	}
	m.records[key] = record.Clone()
	return nil
}

func recordKey(userID *commonpb.UserId, partner thirdpartypb.Partner) string {
	return fmt.Sprintf("%s#%d", hex.EncodeToString(userID.Value), partner)
}
