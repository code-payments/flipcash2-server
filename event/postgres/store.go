package postgres

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/code-payments/flipcash2-server/event"
)

type store struct {
	pool *pgxpool.Pool
}

func NewInPostgres(pool *pgxpool.Pool) event.Store {
	return &store{
		pool: pool,
	}
}

func (s *store) CreateRendezvous(ctx context.Context, rendezvous *event.Rendezvous) error {
	model := toRendezvousModel(rendezvous)
	return model.dbCreate(ctx, s.pool)
}

func (s *store) GetRendezvous(ctx context.Context, key string) (*event.Rendezvous, error) {
	model, err := dbGetRendezvous(ctx, s.pool, key)
	if err != nil {
		return nil, err
	}
	return fromRendezvousModel(model), nil
}

func (s *store) ExtendRendezvousExpiry(ctx context.Context, key, address string, expiresAt time.Time) error {
	return dbExtendRendezvousExpiry(ctx, s.pool, key, address, expiresAt)
}

func (s *store) DeleteRendezvous(ctx context.Context, key, address string) error {
	return dbDeleteRendezvous(ctx, s.pool, key, address)
}

func (s *store) reset() {
	_, err := s.pool.Exec(context.Background(), "DELETE FROM "+rendezvousTableName)
	if err != nil {
		panic(err)
	}
}
