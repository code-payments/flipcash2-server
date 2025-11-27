//go:build integration

package postgres

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/code-payments/flipcash2-server/account/tests"
	pg "github.com/code-payments/flipcash2-server/database/postgres"

	"github.com/jackc/pgx/v5/pgxpool"
	_ "github.com/jackc/pgx/v5/stdlib"
)

func TestAccount_PostgresServer(t *testing.T) {
	pool, err := pgxpool.New(context.Background(), testEnv.DatabaseUrl)
	require.NoError(t, err)
	defer pool.Close()

	pg.SetupGlobalPgxPool(pool)

	testStore := NewInPostgres(pool)
	teardown := func() {
		testStore.(*store).reset()
	}
	tests.RunServerTests(t, testStore, teardown)
}
