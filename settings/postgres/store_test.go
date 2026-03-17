//go:build integration

package postgres

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"

	accountpg "github.com/code-payments/flipcash2-server/account/postgres"
	pg "github.com/code-payments/flipcash2-server/database/postgres"
	"github.com/code-payments/flipcash2-server/model"
	"github.com/code-payments/flipcash2-server/settings/tests"

	"github.com/jackc/pgx/v5/pgxpool"
	_ "github.com/jackc/pgx/v5/stdlib"
)

func TestSettings_PostgresStore(t *testing.T) {
	pool, err := pgxpool.New(context.Background(), testEnv.DatabaseUrl)
	require.NoError(t, err)
	defer pool.Close()

	pg.SetupGlobalPgxPool(pool)

	accountStore := accountpg.NewInPostgres(pool)
	testStore := NewInPostgres(pool)

	createUser := func(t *testing.T) *commonpb.UserId {
		userID := model.MustGenerateUserID()
		pubKey := model.MustGenerateKeyPair().Proto()
		_, err := accountStore.Bind(context.Background(), userID, pubKey)
		require.NoError(t, err)
		return userID
	}
	teardown := func() {
		testStore.(*store).reset()
	}
	tests.RunStoreTests(t, testStore, createUser, teardown)
}
