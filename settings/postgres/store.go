package postgres

import (
	"context"

	"github.com/jackc/pgx/v5/pgxpool"

	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"

	"github.com/code-payments/flipcash2-server/settings"
)

type store struct {
	pool *pgxpool.Pool
}

func NewInPostgres(pool *pgxpool.Pool) settings.Store {
	return &store{
		pool: pool,
	}
}

func (s *store) GetSettings(ctx context.Context, userID *commonpb.UserId) (*settings.Settings, error) {
	return dbGetSettings(ctx, s.pool, userID)
}

func (s *store) SetRegion(ctx context.Context, userID *commonpb.UserId, region *commonpb.Region) error {
	return dbSetRegion(ctx, s.pool, userID, region.Value)
}

func (s *store) SetLocale(ctx context.Context, userID *commonpb.UserId, locale *commonpb.Locale) error {
	return dbSetLocale(ctx, s.pool, userID, locale.Value)
}

func (s *store) reset() {
	_, err := s.pool.Exec(context.Background(), `UPDATE `+usersTableName+` SET "region" = 'usd', "locale" = 'en'`)
	if err != nil {
		panic(err)
	}
}
