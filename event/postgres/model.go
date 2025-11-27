package postgres

import (
	"context"
	"time"

	"github.com/georgysavva/scany/v2/pgxscan"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/code-payments/flipcash2-server/event"

	pg "github.com/code-payments/flipcash2-server/database/postgres"
)

const (
	rendezvousTableName = "flipcash_rendezvous"
	allRendezvousFields = `"key", "address", "createdAt", "updatedAt", "expiresAt"`
)

type rendezvousModel struct {
	Key       string    `db:"key"`
	Address   string    `db:"address"`
	CreatedAt time.Time `db:"createdAt"`
	UpdatedAt time.Time `db:"updatedAt"`
	ExpiresAt time.Time `db:"expiresAt"`
}

func toRendezvousModel(rendezvous *event.Rendezvous) *rendezvousModel {
	return &rendezvousModel{
		Key:       rendezvous.Key,
		Address:   rendezvous.Address,
		ExpiresAt: rendezvous.ExpiresAt,
	}
}

func fromRendezvousModel(model *rendezvousModel) *event.Rendezvous {
	return &event.Rendezvous{
		Key:       model.Key,
		Address:   model.Address,
		ExpiresAt: model.ExpiresAt,
	}
}

func (m *rendezvousModel) dbCreate(ctx context.Context, pool *pgxpool.Pool) error {
	return pg.ExecuteInTx(ctx, pool, func(tx pgx.Tx) error {
		query := `INSERT INTO ` + rendezvousTableName + `(` + allRendezvousFields + `)
			VALUES ($1, $2, NOW(), NOW(), $3)

			ON CONFLICT ("key")
			DO UPDATE
				SET "address" = $2, "expiresAt" = $3
				WHERE ` + rendezvousTableName + `."key" = $1 AND ` + rendezvousTableName + `."expiresAt" < NOW()

			RETURNING ` + allRendezvousFields
		err := pgxscan.Get(
			ctx,
			tx,
			m,
			query,
			m.Key,
			m.Address,
			m.ExpiresAt.UTC(),
		)
		if err == nil {
			return nil
		} else if pgxscan.NotFound(err) {
			return event.ErrRendezvousExists
		}
		return err
	})
}

func dbGetRendezvous(ctx context.Context, pool *pgxpool.Pool, key string) (*rendezvousModel, error) {
	res := &rendezvousModel{}
	query := `SELECT ` + allRendezvousFields + ` FROM ` + rendezvousTableName + `
		WHERE "key" = $1 AND "expiresAt" > NOW()`
	err := pgxscan.Get(
		ctx,
		pool,
		res,
		query,
		key,
	)
	if err != nil {
		if pgxscan.NotFound(err) {
			return nil, event.ErrRendezvousNotFound
		}
		return nil, err
	}
	return res, nil
}

func dbExtendRendezvousExpiry(ctx context.Context, pool *pgxpool.Pool, key, address string, expiresAt time.Time) error {
	return pg.ExecuteInTx(ctx, pool, func(tx pgx.Tx) error {
		query := `UPDATE ` + rendezvousTableName + `
			SET "expiresAt" = $1, "updatedAt" = NOW()
			WHERE "key" = $2 AND "address" = $3 AND "expiresAt" > NOW()`
		cmd, err := tx.Exec(
			ctx,
			query,
			expiresAt.UTC(),
			key,
			address,
		)
		if err != nil {
			return err
		}
		if cmd.RowsAffected() == 0 {
			return event.ErrRendezvousNotFound
		}
		return nil
	})
}

func dbDeleteRendezvous(ctx context.Context, pool *pgxpool.Pool, key, address string) error {
	return pg.ExecuteInTx(ctx, pool, func(tx pgx.Tx) error {
		query := `DELETE FROM ` + rendezvousTableName + `
			WHERE "key" = $1 AND "address" = $2`
		_, err := tx.Exec(
			ctx,
			query,
			key,
			address,
		)
		return err
	})
}
