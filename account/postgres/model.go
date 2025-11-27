package postgres

import (
	"context"
	"strings"

	"github.com/georgysavva/scany/v2/pgxscan"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"

	"github.com/code-payments/flipcash2-server/account"
	pg "github.com/code-payments/flipcash2-server/database/postgres"
)

const (
	usersTableName = "flipcash_users"
	allUserFields  = `"id", "displayName", "phoneNumber", "emailAddress", "isStaff", "isRegistered", "createdAt", "updatedAt"`

	publicKeysTableName = "flipcash_publickeys"
	allPublicKeyFields  = `"key", "userId", "createdAt", "updatedAt"`
)

func dbBind(ctx context.Context, pool *pgxpool.Pool, userID *commonpb.UserId, pubKey *commonpb.PublicKey) error {
	return pg.ExecuteInTx(ctx, pool, func(tx pgx.Tx) error {
		upsertUserQuery := `INSERT INTO ` + usersTableName + ` (` + allUserFields + `) VALUES ($1, NULL, NULL, NULL, FALSE, FALSE, NOW(), NOW()) ON CONFLICT ("id") DO NOTHING`
		_, err := tx.Exec(ctx, upsertUserQuery, pg.Encode(userID.Value))
		if err != nil {
			return err
		}

		putPubkeyQuery := `INSERT INTO ` + publicKeysTableName + ` (` + allPublicKeyFields + `) VALUES ($1, $2, NOW(), NOW())`
		_, err = tx.Exec(ctx, putPubkeyQuery, pg.Encode(pubKey.Value, pg.Base58), pg.Encode(userID.Value))
		if err == nil {
			return nil
		} else if strings.Contains(err.Error(), "23505") { // todo: better utility for detecting unique violations with pgx.Tx
			return account.ErrManyPublicKeys
		}
		return err
	})
}

func dbGetUserId(ctx context.Context, pool *pgxpool.Pool, pubKey *commonpb.PublicKey) (*commonpb.UserId, error) {
	var encoded string
	query := `SELECT "userId" FROM ` + publicKeysTableName + ` WHERE "key" = $1`
	err := pgxscan.Get(
		ctx,
		pool,
		&encoded,
		query,
		pg.Encode(pubKey.Value, pg.Base58),
	)
	if pgxscan.NotFound(err) {
		return nil, account.ErrNotFound
	} else if err != nil {
		return nil, err
	}
	decoded, err := pg.Decode(encoded)
	if err != nil {
		return nil, err
	}
	return &commonpb.UserId{Value: decoded}, err
}

func dbGetPubKeys(ctx context.Context, pool *pgxpool.Pool, userID *commonpb.UserId) ([]*commonpb.PublicKey, error) {
	var encodedValues []string
	query := `SELECT "key" FROM ` + publicKeysTableName + ` WHERE "userId" = $1`
	err := pgxscan.Select(
		ctx,
		pool,
		&encodedValues,
		query,
		pg.Encode(userID.Value),
	)
	if pgxscan.NotFound(err) {
		return nil, nil
	} else if err != nil {
		return nil, err
	}
	if len(encodedValues) == 0 {
		return nil, nil
	}
	res := make([]*commonpb.PublicKey, len(encodedValues))
	for i, encodedValue := range encodedValues {
		decodedValue, err := pg.Decode(encodedValue)
		if err != nil {
			return nil, err
		}
		res[i] = &commonpb.PublicKey{Value: decodedValue}
	}
	return res, nil
}

func dbIsStaff(ctx context.Context, pool *pgxpool.Pool, userID *commonpb.UserId) (bool, error) {
	var res bool
	query := `SELECT "isStaff" FROM ` + usersTableName + ` WHERE "id" = $1`
	err := pgxscan.Get(
		ctx,
		pool,
		&res,
		query,
		pg.Encode(userID.Value),
	)
	if pgxscan.NotFound(err) {
		return false, nil
	}
	return res, err
}

func dbIsRegistered(ctx context.Context, pool *pgxpool.Pool, userID *commonpb.UserId) (bool, error) {
	var res bool
	query := `SELECT "isRegistered" FROM ` + usersTableName + ` WHERE "id" = $1`
	err := pgxscan.Get(
		ctx,
		pool,
		&res,
		query,
		pg.Encode(userID.Value),
	)
	if pgxscan.NotFound(err) {
		return false, nil
	}
	return res, err
}

func dbSetRegistrationFlag(ctx context.Context, pool *pgxpool.Pool, userID *commonpb.UserId, isRegistered bool) error {
	return pg.ExecuteInTx(ctx, pool, func(tx pgx.Tx) error {
		query := `UPDATE ` + usersTableName + ` SET "isRegistered" = $1 WHERE "id" = $2`
		res, err := tx.Exec(ctx, query, isRegistered, pg.Encode(userID.Value))
		if err != nil {
			return err
		}
		if res.RowsAffected() == 0 {
			return account.ErrNotFound
		}
		return nil
	})
}
