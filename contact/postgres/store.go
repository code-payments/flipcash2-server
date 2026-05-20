package postgres

import (
	"context"

	"github.com/jackc/pgx/v5/pgxpool"

	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"

	"github.com/code-payments/flipcash2-server/contact"
)

type store struct {
	pool *pgxpool.Pool
}

func NewInPostgres(pool *pgxpool.Pool) contact.Store {
	return &store{
		pool: pool,
	}
}

func (s *store) GetChecksum(ctx context.Context, userID *commonpb.UserId) (*commonpb.Hash, error) {
	return dbGetChecksum(ctx, s.pool, userID)
}

func (s *store) GetHashes(ctx context.Context, userID *commonpb.UserId) ([]*commonpb.Hash, error) {
	return dbGetHashes(ctx, s.pool, userID)
}

func (s *store) ApplyDelta(
	ctx context.Context,
	userID *commonpb.UserId,
	addHashes []*commonpb.Hash,
	removeHashes []*commonpb.Hash,
	oldChecksum *commonpb.Hash,
	newChecksum *commonpb.Hash,
) error {
	return dbApplyDelta(ctx, s.pool, userID, addHashes, removeHashes, oldChecksum, newChecksum)
}

func (s *store) Replace(
	ctx context.Context,
	userID *commonpb.UserId,
	hashes []*commonpb.Hash,
	expectedChecksum *commonpb.Hash,
) error {
	return dbReplace(ctx, s.pool, userID, hashes, expectedChecksum)
}

func (s *store) reset() {
	_, err := s.pool.Exec(context.Background(), "DELETE FROM "+contactListEntriesTableName)
	if err != nil {
		panic(err)
	}
	_, err = s.pool.Exec(context.Background(), "DELETE FROM "+contactListsTableName)
	if err != nil {
		panic(err)
	}
}
