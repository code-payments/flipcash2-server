//go:build integration

package postgres

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	account_postgres "github.com/code-payments/flipcash2-server/account/postgres"
	"github.com/code-payments/flipcash2-server/profile/tests"

	"github.com/jackc/pgx/v5/pgxpool"
	_ "github.com/jackc/pgx/v5/stdlib"
)

func TestProfile_PostgresServer(t *testing.T) {
	pool, err := pgxpool.New(context.Background(), testEnv.DatabaseUrl)
	require.NoError(t, err)
	defer pool.Close()

	accounts := account_postgres.NewInPostgres(pool)
	profiles := NewInPostgres(pool)
	teardown := func() {
		profiles.(*store).reset()
	}
	tests.RunServerTests(t, accounts, profiles, teardown)
}
