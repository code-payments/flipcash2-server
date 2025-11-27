package database

import (
	"context"

	pg "github.com/code-payments/flipcash2-server/database/postgres"
)

// Note: All database implementations must support tx handling
//
// todo: support other database types when we get there
func ExecuteTxWithinCtx(ctx context.Context, fn func(context.Context) error) error {
	return pg.ExecuteTxWithinCtx(ctx, fn)
}
