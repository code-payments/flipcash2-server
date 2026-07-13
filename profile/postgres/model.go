package postgres

import (
	"context"
	"time"

	"github.com/georgysavva/scany/v2/pgxscan"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"
	phonepb "github.com/code-payments/flipcash2-protobuf-api/generated/go/phone/v1"
	profilepb "github.com/code-payments/flipcash2-protobuf-api/generated/go/profile/v1"

	pg "github.com/code-payments/flipcash2-server/database/postgres"
	"github.com/code-payments/flipcash2-server/profile"
	"github.com/code-payments/ocp-server/pointer"
)

const (
	usersTableName = "flipcash_users"
	allUserFields  = `"id", "displayName", "phoneNumber", "emailAddress", "isStaff", "isRegistered", "isPhoneNumberLinkedForPayment", "region", "locale", "createdAt", "updatedAt"`

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
		query := `INSERT INTO ` + usersTableName + ` (` + allUserFields + `) VALUES ($1, $2, NULL, NULL, FALSE, FALSE, FALSE, 'usd', 'en', NOW(), NOW()) ON CONFLICT ("id") DO UPDATE SET "displayName" = $2 WHERE ` + usersTableName + `."id" = $1`
		_, err := tx.Exec(ctx, query, pg.Encode(userID.Value), displayName)
		return err
	})
}

func dbGetDisplayNames(ctx context.Context, pool *pgxpool.Pool, userIDs []*commonpb.UserId) (map[string]string, error) {
	out := make(map[string]string)
	if len(userIDs) == 0 {
		return out, nil
	}

	encoded := make([]string, 0, len(userIDs))
	seen := make(map[string]struct{}, len(userIDs))
	for _, id := range userIDs {
		e := pg.Encode(id.Value)
		if _, ok := seen[e]; ok {
			continue
		}
		seen[e] = struct{}{}
		encoded = append(encoded, e)
	}

	var rows []struct {
		ID          string `db:"id"`
		DisplayName string `db:"displayName"`
	}
	query := `SELECT "id", "displayName" FROM ` + usersTableName + ` WHERE "id" = ANY($1::text[]) AND "displayName" IS NOT NULL`
	err := pgxscan.Select(ctx, pool, &rows, query, encoded)
	if err != nil {
		if pgxscan.NotFound(err) {
			return out, nil
		}
		return nil, err
	}

	for _, r := range rows {
		rawID, err := pg.Decode(r.ID)
		if err != nil {
			return nil, err
		}
		out[string(rawID)] = r.DisplayName
	}
	return out, nil
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

func dbLinkPhoneNumber(ctx context.Context, pool *pgxpool.Pool, userID *commonpb.UserId, phoneNumber string, phoneNumberHash *commonpb.Hash) error {
	return pg.ExecuteInTx(ctx, pool, func(tx pgx.Tx) error {
		clearQuery := `UPDATE ` + usersTableName + ` SET "phoneNumber" = NULL, "phoneNumberHash" = NULL, "isPhoneNumberLinkedForPayment" = FALSE WHERE "phoneNumber" = $1 AND "id" != $2`
		if _, err := tx.Exec(ctx, clearQuery, phoneNumber, pg.Encode(userID.Value)); err != nil {
			return err
		}

		setQuery := `UPDATE ` + usersTableName + ` SET "phoneNumber" = $2, "phoneNumberHash" = $3 WHERE "id" = $1`
		_, err := tx.Exec(ctx, setQuery, pg.Encode(userID.Value), phoneNumber, pg.Encode(phoneNumberHash.Value, pg.Hex))
		return err
	})
}

func dbUnlinkPhoneNumber(ctx context.Context, pool *pgxpool.Pool, userID *commonpb.UserId, phoneNumber string) error {
	return pg.ExecuteInTx(ctx, pool, func(tx pgx.Tx) error {
		query := `UPDATE ` + usersTableName + ` SET "phoneNumber" = NULL, "phoneNumberHash" = NULL, "isPhoneNumberLinkedForPayment" = FALSE WHERE "id" = $1 AND "phoneNumber" = $2`
		_, err := tx.Exec(ctx, query, pg.Encode(userID.Value), phoneNumber)
		return err
	})
}

func dbLinkPhoneNumberForPayment(ctx context.Context, pool *pgxpool.Pool, userID *commonpb.UserId, phoneNumber string) (bool, error) {
	var flipped bool
	err := pg.ExecuteInTx(ctx, pool, func(tx pgx.Tx) error {
		var wasLinked bool
		selectQuery := `SELECT "isPhoneNumberLinkedForPayment" FROM ` + usersTableName + ` WHERE "id" = $1 AND "phoneNumber" = $2`
		err := pgxscan.Get(ctx, tx, &wasLinked, selectQuery, pg.Encode(userID.Value), phoneNumber)
		if err != nil {
			if pgxscan.NotFound(err) {
				return profile.ErrNotFound
			}
			return err
		}

		if !wasLinked {
			updateQuery := `UPDATE ` + usersTableName + ` SET "isPhoneNumberLinkedForPayment" = TRUE WHERE "id" = $1 AND "phoneNumber" = $2`
			if _, err := tx.Exec(ctx, updateQuery, pg.Encode(userID.Value), phoneNumber); err != nil {
				return err
			}
		}

		flipped = !wasLinked
		return nil
	})
	if err != nil {
		return false, err
	}
	return flipped, nil
}

func dbIsPhoneNumberLinkedForPayment(ctx context.Context, pool *pgxpool.Pool, userID *commonpb.UserId, phoneNumber string) (bool, error) {
	var res bool
	query := `SELECT EXISTS (SELECT 1 FROM ` + usersTableName + ` WHERE "id" = $1 AND "phoneNumber" = $2 AND "isPhoneNumberLinkedForPayment" = TRUE)`
	err := pgxscan.Get(ctx, pool, &res, query, pg.Encode(userID.Value), phoneNumber)
	if err != nil {
		return false, err
	}
	return res, nil
}

func dbLinkEmailAddress(ctx context.Context, pool *pgxpool.Pool, userID *commonpb.UserId, emailAddress string) error {
	return pg.ExecuteInTx(ctx, pool, func(tx pgx.Tx) error {
		clearQuery := `UPDATE ` + usersTableName + ` SET "emailAddress" = NULL WHERE "emailAddress" = $1 AND "id" != $2`
		if _, err := tx.Exec(ctx, clearQuery, emailAddress, pg.Encode(userID.Value)); err != nil {
			return err
		}

		setQuery := `UPDATE ` + usersTableName + ` SET "emailAddress" = $2 WHERE "id" = $1`
		_, err := tx.Exec(ctx, setQuery, pg.Encode(userID.Value), emailAddress)
		return err
	})
}

func dbGetPhonesByHashes(ctx context.Context, pool *pgxpool.Pool, hashes []*commonpb.Hash) ([]*phonepb.PhoneNumber, error) {
	matches, err := dbGetPhonesByHashesInternal(ctx, pool, hashes, false)
	if err != nil {
		return nil, err
	}
	out := make([]*phonepb.PhoneNumber, len(matches))
	for i, match := range matches {
		out[i] = match.PhoneNumber
	}
	return out, nil
}

func dbGetPhonesByHashesForPayment(ctx context.Context, pool *pgxpool.Pool, hashes []*commonpb.Hash) ([]*profile.PhoneForPayment, error) {
	return dbGetPhonesByHashesInternal(ctx, pool, hashes, true)
}

func dbGetPhonesByHashesInternal(ctx context.Context, pool *pgxpool.Pool, hashes []*commonpb.Hash, forPaymentOnly bool) ([]*profile.PhoneForPayment, error) {
	if len(hashes) == 0 {
		return nil, nil
	}

	encoded := make([]string, 0, len(hashes))
	seen := make(map[string]struct{}, len(hashes))
	for _, h := range hashes {
		e := pg.Encode(h.Value, pg.Hex)
		if _, ok := seen[e]; ok {
			continue
		}
		seen[e] = struct{}{}
		encoded = append(encoded, e)
	}

	var rows []struct {
		ID        string    `db:"id"`
		Phone     string    `db:"phoneNumber"`
		CreatedAt time.Time `db:"createdAt"`
	}
	query := `SELECT "id", "phoneNumber", "createdAt" FROM ` + usersTableName + ` WHERE "phoneNumber" IS NOT NULL AND "phoneNumberHash" = ANY($1::text[])`
	if forPaymentOnly {
		query += ` AND "isPhoneNumberLinkedForPayment" = TRUE`
	}
	err := pgxscan.Select(ctx, pool, &rows, query, encoded)
	if err != nil {
		if pgxscan.NotFound(err) {
			return nil, nil
		}
		return nil, err
	}

	out := make([]*profile.PhoneForPayment, 0, len(rows))
	for _, r := range rows {
		rawID, err := pg.Decode(r.ID)
		if err != nil {
			return nil, err
		}
		out = append(out, &profile.PhoneForPayment{
			PhoneNumber: &phonepb.PhoneNumber{Value: r.Phone},
			UserID:      &commonpb.UserId{Value: rawID},
			JoinedAt:    r.CreatedAt,
		})
	}
	return out, nil
}

func dbGetPhoneNumbersForPayment(ctx context.Context, pool *pgxpool.Pool, userIDs []*commonpb.UserId) (map[string]*phonepb.PhoneNumber, error) {
	out := make(map[string]*phonepb.PhoneNumber)
	if len(userIDs) == 0 {
		return out, nil
	}

	encoded := make([]string, 0, len(userIDs))
	seen := make(map[string]struct{}, len(userIDs))
	for _, id := range userIDs {
		e := pg.Encode(id.Value)
		if _, ok := seen[e]; ok {
			continue
		}
		seen[e] = struct{}{}
		encoded = append(encoded, e)
	}

	var rows []struct {
		ID    string `db:"id"`
		Phone string `db:"phoneNumber"`
	}
	query := `SELECT "id", "phoneNumber" FROM ` + usersTableName + ` WHERE "id" = ANY($1::text[]) AND "phoneNumber" IS NOT NULL AND "isPhoneNumberLinkedForPayment" = TRUE`
	err := pgxscan.Select(ctx, pool, &rows, query, encoded)
	if err != nil {
		if pgxscan.NotFound(err) {
			return out, nil
		}
		return nil, err
	}

	for _, r := range rows {
		rawID, err := pg.Decode(r.ID)
		if err != nil {
			return nil, err
		}
		out[string(rawID)] = &phonepb.PhoneNumber{Value: r.Phone}
	}
	return out, nil
}

func dbGetUserIdByPhoneNumber(ctx context.Context, pool *pgxpool.Pool, phoneNumber string) (*commonpb.UserId, error) {
	var encoded string
	query := `SELECT "id" FROM ` + usersTableName + ` WHERE "phoneNumber" = $1`
	err := pgxscan.Get(
		ctx,
		pool,
		&encoded,
		query,
		phoneNumber,
	)
	if err != nil {
		if pgxscan.NotFound(err) {
			return nil, profile.ErrNotFound
		}
		return nil, err
	}
	decoded, err := pg.Decode(encoded)
	if err != nil {
		return nil, err
	}
	return &commonpb.UserId{Value: decoded}, nil
}

func dbGetUserIdByPhoneNumberForPayment(ctx context.Context, pool *pgxpool.Pool, phoneNumber string) (*commonpb.UserId, error) {
	var encoded string
	query := `SELECT "id" FROM ` + usersTableName + ` WHERE "phoneNumber" = $1 AND "isPhoneNumberLinkedForPayment" = TRUE`
	err := pgxscan.Get(
		ctx,
		pool,
		&encoded,
		query,
		phoneNumber,
	)
	if err != nil {
		if pgxscan.NotFound(err) {
			return nil, profile.ErrNotFound
		}
		return nil, err
	}
	decoded, err := pg.Decode(encoded)
	if err != nil {
		return nil, err
	}
	return &commonpb.UserId{Value: decoded}, nil
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
