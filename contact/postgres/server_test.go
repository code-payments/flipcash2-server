//go:build integration

package postgres

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	accountpg "github.com/code-payments/flipcash2-server/account/postgres"
	"github.com/code-payments/flipcash2-server/contact/tests"
	pg "github.com/code-payments/flipcash2-server/database/postgres"
	profilepg "github.com/code-payments/flipcash2-server/profile/postgres"

	"github.com/jackc/pgx/v5/pgxpool"
	_ "github.com/jackc/pgx/v5/stdlib"
)

func TestContact_PostgresServer(t *testing.T) {
	pool, err := pgxpool.New(context.Background(), testEnv.DatabaseUrl)
	require.NoError(t, err)
	defer pool.Close()

	pg.SetupGlobalPgxPool(pool)

	accounts := accountpg.NewInPostgres(pool)
	profiles := profilepg.NewInPostgres(pool)
	testStore := NewInPostgres(pool)
	teardown := func() {
		testStore.(*store).reset()
	}
	tests.RunServerTests(t, accounts, profiles, testStore, teardown)
}
