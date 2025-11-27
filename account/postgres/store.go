package postgres

import (
	"bytes"
	"context"

	"github.com/jackc/pgx/v5/pgxpool"

	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"

	"github.com/code-payments/flipcash2-server/account"
)

type store struct {
	pool *pgxpool.Pool
}

func NewInPostgres(pool *pgxpool.Pool) account.Store {
	return &store{
		pool: pool,
	}
}

func (s *store) Bind(ctx context.Context, userID *commonpb.UserId, pubKey *commonpb.PublicKey) (*commonpb.UserId, error) {
	existingUserID, err := dbGetUserId(ctx, s.pool, pubKey)
	if err != nil && err != account.ErrNotFound {
		return nil, err
	} else if err == nil {
		return existingUserID, nil
	}

	err = dbBind(ctx, s.pool, userID, pubKey)
	if err != nil {
		return nil, err
	}
	return &commonpb.UserId{Value: userID.Value}, nil
}

func (s *store) GetUserId(ctx context.Context, pubKey *commonpb.PublicKey) (*commonpb.UserId, error) {
	return dbGetUserId(ctx, s.pool, pubKey)
}

func (s *store) GetPubKeys(ctx context.Context, userID *commonpb.UserId) ([]*commonpb.PublicKey, error) {
	return dbGetPubKeys(ctx, s.pool, userID)
}

func (s *store) IsAuthorized(ctx context.Context, userID *commonpb.UserId, pubKey *commonpb.PublicKey) (bool, error) {
	linkedUserID, err := dbGetUserId(ctx, s.pool, pubKey)
	if err == account.ErrNotFound {
		return false, nil
	} else if err != nil {
		return false, err
	}
	return bytes.Equal(linkedUserID.Value, userID.Value), nil
}

func (s *store) IsStaff(ctx context.Context, userID *commonpb.UserId) (bool, error) {
	return dbIsStaff(ctx, s.pool, userID)
}

func (s *store) IsRegistered(ctx context.Context, userID *commonpb.UserId) (bool, error) {
	return dbIsRegistered(ctx, s.pool, userID)
}

func (s *store) SetRegistrationFlag(ctx context.Context, userID *commonpb.UserId, isRegistered bool) error {
	return dbSetRegistrationFlag(ctx, s.pool, userID, isRegistered)
}

func (s *store) reset() {
	_, err := s.pool.Exec(context.Background(), "DELETE FROM "+publicKeysTableName)
	if err != nil {
		panic(err)
	}

	_, err = s.pool.Exec(context.Background(), "DELETE FROM "+usersTableName)
	if err != nil {
		panic(err)
	}
}
