//go:build integration

package postgres

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"

	accountpg "github.com/code-payments/flipcash2-server/account/postgres"
	"github.com/code-payments/flipcash2-server/contact/tests"
	pg "github.com/code-payments/flipcash2-server/database/postgres"
	"github.com/code-payments/flipcash2-server/model"

	"github.com/jackc/pgx/v5/pgxpool"
	_ "github.com/jackc/pgx/v5/stdlib"
)

func TestContact_PostgresStore(t *testing.T) {
	pool, err := pgxpool.New(context.Background(), testEnv.DatabaseUrl)
	require.NoError(t, err)
	defer pool.Close()

	pg.SetupGlobalPgxPool(pool)

	accountStore := accountpg.NewInPostgres(pool)
	testStore := NewInPostgres(pool)

	createUser := func(t *testing.T) *commonpb.UserId {
		userID := model.MustGenerateUserID()
		_, err := accountStore.Bind(context.Background(), userID, model.MustGenerateKeyPair().Proto())
		require.NoError(t, err)
		return userID
	}
	teardown := func() {
		testStore.(*store).reset()
	}
	tests.RunStoreTests(t, testStore, createUser, teardown)
}
