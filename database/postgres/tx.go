package pg

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// todo: Support multiple isolation levels, Postgres default is used for now

const (
	defaultIsolationLevel = pgx.ReadCommitted
	txContextKey          = "flipcash-pgx-tx"
)

var (
	globalPool *pgxpool.Pool

	ErrAlreadyInTx              = errors.New("already executing in existing db tx")
	ErrNotInTx                  = errors.New("not executing in existing db tx")
	ErrInvalidPool              = errors.New("provided pgx pool is not the global pool")
	ErrGlobalPoolNotInitialized = errors.New("global pgx pool is not initialized")
)

func SetupGlobalPgxPool(pool *pgxpool.Pool) {
	globalPool = pool
}

// ExecuteTxWithinCtx executes a DB transaction that's scoped to a call to fn. The transaction
// is passed along with the context. Once fn is complete, commit/rollback is called based
// on whether an error is returned.
func ExecuteTxWithinCtx(ctx context.Context, fn func(context.Context) error) error {
	if globalPool == nil {
		return fn(ctx)
	}

	existing := ctx.Value(txContextKey)
	if existing != nil {
		return ErrAlreadyInTx
	}

	tx, err := globalPool.BeginTx(ctx, pgx.TxOptions{
		IsoLevel: defaultIsolationLevel,
	})
	if err != nil {
		return err
	}
	defer tx.Rollback(context.Background())

	ctx = context.WithValue(ctx, txContextKey, tx)

	err = fn(ctx)
	if err != nil {
		return err
	}
	if ctx.Err() != nil {
		return ctx.Err()
	}
	return tx.Commit(ctx)
}

// ExecuteInTx is meant for DB store implementations to execute an operation within
// the scope of a DB transaction. This method is aware of ExecuteTxWithinCtx, and
// will dynamically decide when to use a new or existing transaction, as well as
// where the responsibilty for commit/rollback calls lie.
func ExecuteInTx(ctx context.Context, pool *pgxpool.Pool, fn func(tx pgx.Tx) error) (err error) {
	if globalPool != nil && pool != globalPool {
		return ErrInvalidPool
	}

	tx, err := getTxFromCtx(ctx)
	if err != nil && err != ErrNotInTx {
		return err
	}

	var startedNewTx bool // To determine who is responsible for commit/rollback
	if err == ErrNotInTx {
		startedNewTx = true
		tx, err = pool.BeginTx(ctx, pgx.TxOptions{
			IsoLevel: defaultIsolationLevel,
		})
		if err != nil {
			return err
		}
		defer tx.Rollback(context.Background())
	}

	err = fn(tx)
	if err != nil {
		return err
	}
	if startedNewTx {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return tx.Commit(ctx)
	}
	return nil
}

func getTxFromCtx(ctx context.Context) (pgx.Tx, error) {
	txFromCtx := ctx.Value(txContextKey)
	if txFromCtx == nil {
		return nil, ErrNotInTx
	}

	tx, ok := txFromCtx.(pgx.Tx)
	if !ok {
		return nil, errors.New("invalid type for tx")
	}
	return tx, nil
}
