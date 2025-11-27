package memory

import (
	"bytes"
	"context"
	"errors"
	"sync"

	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"

	"github.com/code-payments/flipcash2-server/iap"
)

type InMemoryStore struct {
	mu        sync.RWMutex
	purchases map[string]*iap.Purchase
}

func NewInMemory() iap.Store {
	return &InMemoryStore{
		purchases: map[string]*iap.Purchase{},
	}
}

func (s *InMemoryStore) reset() {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.purchases = make(map[string]*iap.Purchase)
}

func (s *InMemoryStore) CreatePurchase(ctx context.Context, purchase *iap.Purchase) error {
	if purchase.Product == iap.ProductUnknown {
		return errors.New("product is required")
	}
	if purchase.PaymentAmount <= 0 {
		return errors.New("payment amount must be positive")
	}
	if len(purchase.PaymentCurrency) == 0 {
		return errors.New("payment currency is required")
	}
	if purchase.State != iap.StateFulfilled {
		return errors.New("state must be fulfilled")
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	_, ok := s.purchases[string(purchase.ReceiptID)]
	if ok {
		return iap.ErrExists
	}

	s.purchases[string(purchase.ReceiptID)] = purchase.Clone()

	return nil
}

func (s *InMemoryStore) GetPurchaseByID(ctx context.Context, receiptID []byte) (*iap.Purchase, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	purchase, ok := s.purchases[string(receiptID)]
	if !ok {
		return nil, iap.ErrNotFound
	}
	return purchase.Clone(), nil
}

func (s *InMemoryStore) GetPurchasesByUserAndProduct(ctx context.Context, userID *commonpb.UserId, product iap.Product) ([]*iap.Purchase, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var res []*iap.Purchase

	for _, purchase := range s.purchases {
		if !bytes.Equal(userID.Value, purchase.User.Value) {
			continue
		}

		if purchase.Product != product {
			continue
		}

		res = append(res, purchase.Clone())
	}

	if len(res) == 0 {
		return nil, iap.ErrNotFound
	}
	return res, nil
}
