package postgres

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5/pgxpool"

	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"
	"github.com/code-payments/flipcash2-server/iap"
)

type store struct {
	pool *pgxpool.Pool
}

func NewInPostgres(pool *pgxpool.Pool) iap.Store {
	return &store{
		pool: pool,
	}
}

func (s *store) CreatePurchase(ctx context.Context, purchase *iap.Purchase) error {
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

	model, err := toModel(purchase)
	if err != nil {
		return err
	}
	return model.dbPut(ctx, s.pool)
}

func (s *store) GetPurchaseByID(ctx context.Context, receiptID []byte) (*iap.Purchase, error) {
	model, err := dbGetPurchaseByID(ctx, s.pool, receiptID)
	if err != nil {
		return nil, err
	}
	return fromModel(model)
}

func (s *store) GetPurchasesByUserAndProduct(ctx context.Context, userID *commonpb.UserId, product iap.Product) ([]*iap.Purchase, error) {
	models, err := dbGetPurchasesByUserAndProduct(ctx, s.pool, userID, product)
	if err != nil {
		return nil, err
	}
	res := make([]*iap.Purchase, len(models))
	for i, model := range models {
		res[i], err = fromModel(model)
		if err != nil {
			return nil, err
		}
	}
	return res, nil
}

func (s *store) reset() {
	_, err := s.pool.Exec(context.Background(), "DELETE FROM "+iapsTableName)
	if err != nil {
		panic(err)
	}
}
