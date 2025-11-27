package postgres

import (
	"context"

	"github.com/jackc/pgx/v5/pgxpool"

	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"
	pushpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/push/v1"

	"github.com/code-payments/flipcash2-server/push"
)

type store struct {
	pool *pgxpool.Pool
}

func NewInPostgres(pool *pgxpool.Pool) push.TokenStore {
	return &store{
		pool: pool,
	}
}

func (s *store) GetTokens(ctx context.Context, userID *commonpb.UserId) ([]push.Token, error) {
	models, err := dbGetTokensBatch(ctx, s.pool, userID)
	if err != nil {
		return nil, err
	}

	res := make([]push.Token, len(models))
	for i, model := range models {
		res[i], err = fromModel(model)
		if err != nil {
			return nil, err
		}
	}
	return res, nil
}

func (s *store) GetTokensBatch(ctx context.Context, userIDs ...*commonpb.UserId) ([]push.Token, error) {
	models, err := dbGetTokensBatch(ctx, s.pool, userIDs...)
	if err != nil {
		return nil, err
	}

	res := make([]push.Token, len(models))
	for i, model := range models {
		res[i], err = fromModel(model)
		if err != nil {
			return nil, err
		}
	}
	return res, nil
}

func (s *store) AddToken(ctx context.Context, userID *commonpb.UserId, appInstallID *commonpb.AppInstallId, tokenType pushpb.TokenType, tokenValue string) error {
	token := push.Token{
		Type:         tokenType,
		Token:        tokenValue,
		AppInstallID: appInstallID.Value,
	}
	model, err := toModel(userID, token)
	if err != nil {
		return err
	}
	return model.dbAdd(ctx, s.pool)
}

func (s *store) DeleteToken(ctx context.Context, tokenType pushpb.TokenType, token string) error {
	return dbDeleteToken(ctx, s.pool, tokenType, token)
}

func (s *store) reset() {
	_, err := s.pool.Exec(context.Background(), "DELETE FROM "+pushTokensTableName)
	if err != nil {
		panic(err)
	}
}
