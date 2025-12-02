package postgres

import (
	"context"
	"time"

	"github.com/georgysavva/scany/v2/pgxscan"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"
	profilepb "github.com/code-payments/flipcash2-protobuf-api/generated/go/profile/v1"

	"github.com/code-payments/ocp-server/pointer"
	pg "github.com/code-payments/flipcash2-server/database/postgres"
	"github.com/code-payments/flipcash2-server/profile"
)

const (
	usersTableName = "flipcash_users"
	allUserFields  = `"id", "displayName", "phoneNumber", "emailAddress", "isStaff", "isRegistered", "createdAt", "updatedAt"`

	xProfilesTableName = "flipcash_x_profiles"
	allXUserFields     = `"id", "username", "name", "description", "profilePicUrl", "followerCount", "verifiedType",  "accessToken", "userId", "createdAt", "updatedAt"`
)

type xProfileModel struct {
	ID            string    `db:"id"`
	Username      string    `db:"username"`
	Name          *string   `db:"name"`
	Description   *string   `db:"description"`
	ProfilePicUrl string    `db:"profilePicUrl"`
	FollowerCount int       `db:"followerCount"`
	VerifiedType  int       `db:"verifiedType"`
	AccessToken   string    `db:"accessToken"`
	UserID        string    `db:"userId"`
	CreatedAt     time.Time `db:"createdAt"`
	UpdatedAt     time.Time `db:"updatedAt"`
}

func toXProfileModel(userID *commonpb.UserId, profile *profilepb.XProfile, accessToken string) (*xProfileModel, error) {
	return &xProfileModel{
		ID:            profile.Id,
		Username:      profile.Username,
		Name:          pointer.StringIfValid(len(profile.Name) > 0, profile.Name),
		Description:   pointer.StringIfValid(len(profile.Description) > 0, profile.Description),
		ProfilePicUrl: profile.ProfilePicUrl,
		FollowerCount: int(profile.FollowerCount),
		VerifiedType:  int(profile.VerifiedType),
		AccessToken:   accessToken,
		UserID:        pg.Encode(userID.Value),
	}, nil
}

func fromXProfileModel(m *xProfileModel) (*profilepb.XProfile, error) {
	return &profilepb.XProfile{
		Id:            m.ID,
		Username:      m.Username,
		Name:          *pointer.StringOrDefault(m.Name, ""),
		Description:   *pointer.StringOrDefault(m.Description, ""),
		ProfilePicUrl: m.ProfilePicUrl,
		VerifiedType:  profilepb.XProfile_VerifiedType(m.VerifiedType),
		FollowerCount: uint32(m.FollowerCount),
	}, nil
}

func dbGetDisplayName(ctx context.Context, pool *pgxpool.Pool, userID *commonpb.UserId) (*string, error) {
	var res *string
	query := `SELECT "displayName" FROM ` + usersTableName + ` WHERE "id" = $1`
	err := pgxscan.Get(
		ctx,
		pool,
		&res,
		query,
		pg.Encode(userID.Value),
	)
	if err != nil {
		if pgxscan.NotFound(err) {
			return nil, profile.ErrNotFound
		}
		return nil, err
	}
	return res, nil
}

func dbSetDisplayName(ctx context.Context, pool *pgxpool.Pool, userID *commonpb.UserId, displayName string) error {
	return pg.ExecuteInTx(ctx, pool, func(tx pgx.Tx) error {
		query := `INSERT INTO ` + usersTableName + ` (` + allUserFields + `) VALUES ($1, $2, NULL, NULL, FALSE, FALSE, NOW(), NOW()) ON CONFLICT ("id") DO UPDATE SET "displayName" = $2 WHERE ` + usersTableName + `."id" = $1`
		_, err := tx.Exec(ctx, query, pg.Encode(userID.Value), displayName)
		return err
	})
}

func dbGetPhoneNumber(ctx context.Context, pool *pgxpool.Pool, userID *commonpb.UserId) (*string, error) {
	var res *string
	query := `SELECT "phoneNumber" FROM ` + usersTableName + ` WHERE "id" = $1`
	err := pgxscan.Get(
		ctx,
		pool,
		&res,
		query,
		pg.Encode(userID.Value),
	)
	if err != nil {
		if pgxscan.NotFound(err) {
			return nil, profile.ErrNotFound
		}
		return nil, err
	}
	return res, nil
}

func dbGetEmailAddress(ctx context.Context, pool *pgxpool.Pool, userID *commonpb.UserId) (*string, error) {
	var res *string
	query := `SELECT "emailAddress" FROM ` + usersTableName + ` WHERE "id" = $1`
	err := pgxscan.Get(
		ctx,
		pool,
		&res,
		query,
		pg.Encode(userID.Value),
	)
	if err != nil {
		if pgxscan.NotFound(err) {
			return nil, profile.ErrNotFound
		}
		return nil, err
	}
	return res, nil
}

func dbSetPhoneNumber(ctx context.Context, pool *pgxpool.Pool, userID *commonpb.UserId, phoneNumber string) error {
	return pg.ExecuteInTx(ctx, pool, func(tx pgx.Tx) error {
		query := `UPDATE ` + usersTableName + ` SET "phoneNumber" = $2 WHERE "id" = $1`
		_, err := tx.Exec(ctx, query, pg.Encode(userID.Value), phoneNumber)
		return err
	})
}

func dbUnlinkPhoneNumber(ctx context.Context, pool *pgxpool.Pool, userID *commonpb.UserId, phoneNumber string) error {
	return pg.ExecuteInTx(ctx, pool, func(tx pgx.Tx) error {
		query := `UPDATE ` + usersTableName + ` SET "phoneNumber" = NULL WHERE "id" = $1 AND "phoneNumber" = $2`
		_, err := tx.Exec(ctx, query, pg.Encode(userID.Value), phoneNumber)
		return err
	})
}

func dbSetEmailAddress(ctx context.Context, pool *pgxpool.Pool, userID *commonpb.UserId, emailAddress string) error {
	return pg.ExecuteInTx(ctx, pool, func(tx pgx.Tx) error {
		query := `UPDATE ` + usersTableName + ` SET "emailAddress" = $2 WHERE "id" = $1`
		_, err := tx.Exec(ctx, query, pg.Encode(userID.Value), emailAddress)
		return err
	})
}

func dbUnlinkEmailAddress(ctx context.Context, pool *pgxpool.Pool, userID *commonpb.UserId, emailAddress string) error {
	return pg.ExecuteInTx(ctx, pool, func(tx pgx.Tx) error {
		query := `UPDATE ` + usersTableName + ` SET "emailAddress" = NULL WHERE "id" = $1 AND "emailAddress" = $2`
		_, err := tx.Exec(ctx, query, pg.Encode(userID.Value), emailAddress)
		return err
	})
}

func (m *xProfileModel) dbUpsert(ctx context.Context, pool *pgxpool.Pool) error {
	return pg.ExecuteInTx(ctx, pool, func(tx pgx.Tx) error {
		query := `INSERT INTO ` + xProfilesTableName + ` (` + allXUserFields + `) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, NOW(), NOW())
			ON CONFLICT ("id") DO UPDATE
				SET "username" = $2, "name" = $3, "description" = $4, "profilePicUrl" = $5, "followerCount" = $6, "verifiedType" = $7, "accessToken" = $8, "userId" = $9, "updatedAt" = NOW()
				WHERE ` + xProfilesTableName + `."id" = $1
			RETURNING ` + allXUserFields
		err := pgxscan.Get(
			ctx,
			tx,
			m,
			query,
			m.ID,
			m.Username,
			m.Name,
			m.Description,
			m.ProfilePicUrl,
			m.FollowerCount,
			m.VerifiedType,
			m.AccessToken,
			m.UserID,
		)
		return err
	})
}

func dbUnlinkXAccount(ctx context.Context, pool *pgxpool.Pool, userID *commonpb.UserId, xUserID string) error {
	return pg.ExecuteInTx(ctx, pool, func(tx pgx.Tx) error {
		query := `DELETE FROM ` + xProfilesTableName + ` WHERE "id" = $1 AND "userId" = $2`
		res, err := tx.Exec(ctx, query, xUserID, pg.Encode(userID.Value))
		if err != nil {
			return err
		}
		if res.RowsAffected() == 0 {
			return profile.ErrNotFound
		}
		return nil
	})
}

func dbGetXProfile(ctx context.Context, pool *pgxpool.Pool, userID *commonpb.UserId) (*xProfileModel, error) {
	res := &xProfileModel{}
	query := `SELECT ` + allXUserFields + ` FROM ` + xProfilesTableName + ` WHERE "userId" = $1`
	err := pgxscan.Get(
		ctx,
		pool,
		res,
		query,
		pg.Encode(userID.Value),
	)
	if err != nil {
		if pgxscan.NotFound(err) {
			return nil, profile.ErrNotFound
		}
		return nil, err
	}
	return res, nil
}
