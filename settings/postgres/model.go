package postgres

import (
	"context"

	"github.com/georgysavva/scany/v2/pgxscan"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"

	pg "github.com/code-payments/flipcash2-server/database/postgres"
	"github.com/code-payments/flipcash2-server/settings"
)

const (
	usersTableName = "flipcash_users"
)

type settingsRow struct {
	Region string `db:"region"`
	Locale string `db:"locale"`
}

func dbGetSettings(ctx context.Context, pool *pgxpool.Pool, userID *commonpb.UserId) (*settings.Settings, error) {
	var row settingsRow
	query := `SELECT "region", "locale" FROM ` + usersTableName + ` WHERE "id" = $1`
	err := pgxscan.Get(
		ctx,
		pool,
		&row,
		query,
		pg.Encode(userID.Value),
	)
	if pgxscan.NotFound(err) {
		return nil, settings.ErrNotFound
	} else if err != nil {
		return nil, err
	}
	return &settings.Settings{
		Region: &commonpb.Region{Value: row.Region},
		Locale: &commonpb.Locale{Value: row.Locale},
	}, nil
}

func dbSetRegion(ctx context.Context, pool *pgxpool.Pool, userID *commonpb.UserId, region string) error {
	return pg.ExecuteInTx(ctx, pool, func(tx pgx.Tx) error {
		query := `UPDATE ` + usersTableName + ` SET "region" = $1, "updatedAt" = NOW() WHERE "id" = $2`
		res, err := tx.Exec(ctx, query, region, pg.Encode(userID.Value))
		if err != nil {
			return err
		}
		if res.RowsAffected() == 0 {
			return settings.ErrNotFound
		}
		return nil
	})
}

func dbSetLocale(ctx context.Context, pool *pgxpool.Pool, userID *commonpb.UserId, locale string) error {
	return pg.ExecuteInTx(ctx, pool, func(tx pgx.Tx) error {
		query := `UPDATE ` + usersTableName + ` SET "locale" = $1, "updatedAt" = NOW() WHERE "id" = $2`
		res, err := tx.Exec(ctx, query, locale, pg.Encode(userID.Value))
		if err != nil {
			return err
		}
		if res.RowsAffected() == 0 {
			return settings.ErrNotFound
		}
		return nil
	})
}
