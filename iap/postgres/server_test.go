//go:build integration

package postgres

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	account "github.com/code-payments/flipcash2-server/account/postgres"
	pg "github.com/code-payments/flipcash2-server/database/postgres"
	"github.com/code-payments/flipcash2-server/iap"
	iap_memory "github.com/code-payments/flipcash2-server/iap/memory"
	"github.com/code-payments/flipcash2-server/iap/tests"

	"github.com/jackc/pgx/v5/pgxpool"
	_ "github.com/jackc/pgx/v5/stdlib"
)

func TestIap_PostgresServer(t *testing.T) {
	pool, err := pgxpool.New(context.Background(), testEnv.DatabaseUrl)
	require.NoError(t, err)
	defer pool.Close()

	pg.SetupGlobalPgxPool(pool)

	pub, priv, err := iap_memory.GenerateKeyPair()
	if err != nil {
		t.Fatalf("error generating key pair: %v", err)
	}

	product := iap.CreateAccountProductID
	verifier := iap_memory.NewMemoryVerifier(pub, product)
	validReceiptFunc := func(msg string) (string, string) {
		return iap_memory.GenerateValidReceipt(priv, msg), product
	}

	accounts := account.NewInPostgres(pool)
	iaps := NewInPostgres(pool)

	teardown := func() {
		iaps.(*store).reset()
	}

	tests.RunServerTests(t, accounts, iaps, verifier, validReceiptFunc, teardown)
}
