package postgres

import (
	"context"
	"fmt"
	"time"

	"github.com/georgysavva/scany/v2/pgxscan"
	"github.com/jackc/pgx/v5/pgxpool"

	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"
	pushpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/push/v1"

	pg "github.com/code-payments/flipcash2-server/database/postgres"
	"github.com/code-payments/flipcash2-server/push"
)

const (
	pushTokensTableName = "flipcash_pushtokens"
	allPushTokenFields  = `"userId", "appInstallId", "token", "type", "createdAt", "updatedAt"`
)

type model struct {
	UserID       string    `db:"userId"`
	AppInstallID string    `db:"appInstallId"`
	Token        string    `db:"token"`
	Type         int       `db:"type"`
	CreatedAt    time.Time `db:"createdAt"`
	UpdatedAt    time.Time `db:"updatedAt"`
}

func toModel(userID *commonpb.UserId, token push.Token) (*model, error) {
	return &model{
		UserID:       pg.Encode(userID.Value),
		AppInstallID: token.AppInstallID,
		Token:        token.Token,
		Type:         int(token.Type),
	}, nil
}

func fromModel(m *model) (push.Token, error) {
	return push.Token{
		Type:         pushpb.TokenType(m.Type),
		AppInstallID: m.AppInstallID,
		Token:        m.Token,
	}, nil
}

func (m *model) dbAdd(ctx context.Context, pool *pgxpool.Pool) error {
	query := `INSERT INTO ` + pushTokensTableName + ` (` + allPushTokenFields + `) VALUES ($1, $2, $3, $4, NOW(), NOW()) ON CONFLICT ("userId", "appInstallId") DO UPDATE SET "token" = $3, "updatedAt" = NOW() WHERE ` + pushTokensTableName + `."userId" = $1 AND ` + pushTokensTableName + `."appInstallId" = $2 RETURNING ` + allPushTokenFields
	return pgxscan.Get(
		ctx,
		pool,
		m,
		query,
		m.UserID,
		m.AppInstallID,
		m.Token,
		m.Type,
	)
}

func dbGetTokensBatch(ctx context.Context, pool *pgxpool.Pool, userIDs ...*commonpb.UserId) ([]*model, error) {
	var res []*model

	queryParameters := make([]any, len(userIDs))

	query := `SELECT ` + allPushTokenFields + ` FROM ` + pushTokensTableName + ` WHERE "userId" IN (`
	for i, userID := range userIDs {
		queryParameters[i] = pg.Encode(userID.Value)
		if i > 0 {
			query += fmt.Sprintf(",$%d", i+1)
		} else {
			query += fmt.Sprintf("$%d", i+1)
		}
	}
	query += ")"

	err := pgxscan.Select(
		ctx,
		pool,
		&res,
		query,
		queryParameters...,
	)
	if err != nil {
		if pgxscan.NotFound(err) {
			return nil, nil
		}
		return nil, err
	}
	return res, nil
}

func dbDeleteToken(ctx context.Context, pool *pgxpool.Pool, tokenType pushpb.TokenType, token string) error {
	query := `DELETE FROM ` + pushTokensTableName + ` WHERE "token" = $1 and "type" = $2`
	_, err := pool.Exec(ctx, query, token, tokenType)
	return err
}
