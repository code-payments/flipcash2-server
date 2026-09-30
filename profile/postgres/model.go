package postgres

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/georgysavva/scany/v2/pgxscan"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	blobpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/blob/v1"
	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"
	profilepb "github.com/code-payments/flipcash2-protobuf-api/generated/go/profile/v1"

	pg "github.com/code-payments/flipcash2-server/database/postgres"
	"github.com/code-payments/flipcash2-server/profile"
	"github.com/code-payments/ocp-server/pointer"
)

const (
	usersTableName = "flipcash_users"
	allUserFields  = `"id", "displayName", "username", "profilePictureBlobId", "tipCardColor", "flipcardColor", "minDmChatInitFeeCurrency", "minDmChatInitFeeNativeAmount", "phoneNumber", "emailAddress", "isStaff", "isRegistered", "isPhoneNumberLinkedForPayment", "region", "locale", "createdAt", "updatedAt"`

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

func dbGetPublicProfile(ctx context.Context, pool *pgxpool.Pool, userID *commonpb.UserId) (*profilepb.UserProfile, error) {
	var res struct {
		DisplayName                  *string   `db:"displayName"`
		Username                     *string   `db:"username"`
		ProfilePictureBlobID         *string   `db:"profilePictureBlobId"`
		FlipcardColor                *string   `db:"flipcardColor"`
		MinDmChatInitFeeCurrency     *string   `db:"minDmChatInitFeeCurrency"`
		MinDmChatInitFeeNativeAmount *float64  `db:"minDmChatInitFeeNativeAmount"`
		CreatedAt                    time.Time `db:"createdAt"`
	}
	query := `SELECT "displayName", "username", "profilePictureBlobId", "flipcardColor", "minDmChatInitFeeCurrency", "minDmChatInitFeeNativeAmount", "createdAt" FROM ` + usersTableName + ` WHERE "id" = $1`
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

	userProfile := &profilepb.UserProfile{
		UserId:                proto.Clone(userID).(*commonpb.UserId),
		DisplayName:           *pointer.StringOrDefault(res.DisplayName, ""),
		JoinTs:                timestamppb.New(res.CreatedAt),
		FlipcardCustomization: profile.FlipcardCustomizationFromStored(res.FlipcardColor),
		MinDmChatInitFee:      profile.MinDmChatInitFeeFromStored(res.MinDmChatInitFeeCurrency, res.MinDmChatInitFeeNativeAmount),
	}

	if res.Username != nil {
		userProfile.Username = &commonpb.Username{Value: *res.Username}
	}

	if res.ProfilePictureBlobID != nil {
		rawBlobID, err := pg.Decode(*res.ProfilePictureBlobID)
		if err != nil {
			return nil, err
		}
		userProfile.ProfilePicture = &blobpb.Media{
			Renditions: []*blobpb.Rendition{{
				Role:   blobpb.Rendition_ORIGINAL,
				BlobId: &blobpb.BlobId{Value: rawBlobID},
			}},
		}
	}
	return userProfile, nil
}

const setDisplayNameQuery = `INSERT INTO ` + usersTableName + ` (` + allUserFields + `) VALUES ($1, $2, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, FALSE, FALSE, FALSE, 'usd', 'en', NOW(), NOW()) ON CONFLICT ("id") DO UPDATE SET "displayName" = $2 WHERE ` + usersTableName + `."id" = $1`

func dbSetDisplayName(ctx context.Context, pool *pgxpool.Pool, userID *commonpb.UserId, displayName string) error {
	return pg.ExecuteInTx(ctx, pool, func(tx pgx.Tx) error {
		_, err := tx.Exec(ctx, setDisplayNameQuery, pg.Encode(userID.Value), displayName)
		return err
	})
}

// claimDefaultUsernameQuery gives a user a default handle, but only while they
// are still eligible for one: they hold no handle. A user the
// table does not know yet is inserted with the handle and nothing else. The
// condition is evaluated against the row as last committed, so it is what
// decides eligibility, not the unlocked read ahead of it.
const claimDefaultUsernameQuery = `INSERT INTO ` + usersTableName + ` (` + allUserFields + `) VALUES ($1, NULL, $2, NULL, NULL, NULL, NULL, NULL, NULL, NULL, FALSE, FALSE, FALSE, 'usd', 'en', NOW(), NOW()) ON CONFLICT ("id") DO UPDATE SET "username" = $2 WHERE ` + usersTableName + `."username" IS NULL`

func dbSetDisplayNameWithDefaultUsername(ctx context.Context, pool *pgxpool.Pool, userID *commonpb.UserId, displayName, usernameBase string) (profile.DefaultUsernameResult, error) {
	var result profile.DefaultUsernameResult
	err := pg.ExecuteInTx(ctx, pool, func(tx pgx.Tx) error {
		encodedID := pg.Encode(userID.Value)

		// An unlocked read only spares the search for a user who already holds a
		// handle, which is final: a handle is never released without another taking
		// its place. One who looks eligible is decided by the claim's own
		// condition, so a concurrent handle for the same user wins over this
		// assignment.
		var existing *string
		query := `SELECT "username" FROM ` + usersTableName + ` WHERE "id" = $1`
		if err := pgxscan.Get(ctx, tx, &existing, query, encodedID); err != nil && !pgxscan.NotFound(err) {
			return err
		}

		if existing == nil {
			held := func(ctx context.Context, usernames []string) (map[string]struct{}, error) {
				return dbGetHeldUsernames(ctx, tx, usernames)
			}

			// Each claim runs in a savepoint so that losing the handle to another
			// user rolls back the claim alone, not the transaction. Under read
			// committed, the search that follows sees the winner's handle and moves
			// past it.
			claim := func(ctx context.Context, username string) (bool, error) {
				savepoint, err := tx.Begin(ctx)
				if err != nil {
					return false, err
				}
				tag, err := savepoint.Exec(ctx, claimDefaultUsernameQuery, encodedID, username)
				if err != nil {
					// A savepoint that cannot be rolled back leaves the transaction
					// aborted, so the search must not go on: the joined error never
					// matches ErrUsernameTaken, which ends it.
					if rollbackErr := savepoint.Rollback(ctx); rollbackErr != nil {
						return false, errors.Join(err, rollbackErr)
					}
					if isUniqueViolation(err) {
						return false, profile.ErrUsernameTaken
					}
					return false, err
				}
				if err := savepoint.Commit(ctx); err != nil {
					return false, err
				}
				return tag.RowsAffected() > 0, nil
			}

			username, err := profile.AssignDefaultUsername(ctx, usernameBase, held, claim)
			if errors.Is(err, profile.ErrNoDefaultUsername) {
				result.NoneAvailable = true
			} else if err != nil {
				return err
			}
			result.Username = username
		}

		// The claim may have inserted the user's row, which the display name then
		// updates.
		_, err := tx.Exec(ctx, setDisplayNameQuery, encodedID, displayName)
		return err
	})
	if err != nil {
		return profile.DefaultUsernameResult{}, err
	}
	return result, nil
}

// dbGetHeldUsernames is a profile.HeldUsernamesFunc over the users table. Every
// handle is matched exactly, so each one is a probe of the unique index on
// "username".
func dbGetHeldUsernames(ctx context.Context, tx pgx.Tx, usernames []string) (map[string]struct{}, error) {
	var held []string
	query := `SELECT "username" FROM ` + usersTableName + ` WHERE "username" = ANY($1::text[])`
	if err := pgxscan.Select(ctx, tx, &held, query, usernames); err != nil {
		return nil, err
	}

	result := make(map[string]struct{}, len(held))
	for _, username := range held {
		result[username] = struct{}{}
	}
	return result, nil
}

func isUniqueViolation(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "23505" // unique_violation
}

func dbSetUsername(ctx context.Context, pool *pgxpool.Pool, userID *commonpb.UserId, username string) error {
	if err := profile.ValidateUsername(username); err != nil {
		return err
	}

	return pg.ExecuteInTx(ctx, pool, func(tx pgx.Tx) error {
		query := `INSERT INTO ` + usersTableName + ` (` + allUserFields + `) VALUES ($1, NULL, $2, NULL, NULL, NULL, NULL, NULL, NULL, NULL, FALSE, FALSE, FALSE, 'usd', 'en', NOW(), NOW()) ON CONFLICT ("id") DO UPDATE SET "username" = $2 WHERE ` + usersTableName + `."id" = $1`
		_, err := tx.Exec(ctx, query, pg.Encode(userID.Value), username)
		// The id conflict is handled above, so the only unique constraint left to
		// trip is the one holding a handle to a single user. Letting the constraint
		// report the conflict is what keeps two users claiming the same handle at
		// once from both succeeding.
		if err != nil && strings.Contains(err.Error(), "23505") { // todo: better utility for detecting unique violations with pgx.Tx
			return profile.ErrUsernameTaken
		}
		return err
	})
}

func dbGetUserIdByUsername(ctx context.Context, pool *pgxpool.Pool, username string) (*commonpb.UserId, error) {
	var encoded string
	query := `SELECT "id" FROM ` + usersTableName + ` WHERE "username" = $1`
	err := pgxscan.Get(
		ctx,
		pool,
		&encoded,
		query,
		profile.NormalizeUsername(username),
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

func dbGetPublicProfiles(ctx context.Context, pool *pgxpool.Pool, userIDs []*commonpb.UserId) (map[string]*profilepb.UserProfile, error) {
	out := make(map[string]*profilepb.UserProfile)
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
		ID                           string    `db:"id"`
		DisplayName                  *string   `db:"displayName"`
		Username                     *string   `db:"username"`
		ProfilePictureBlobID         *string   `db:"profilePictureBlobId"`
		FlipcardColor                *string   `db:"flipcardColor"`
		MinDmChatInitFeeCurrency     *string   `db:"minDmChatInitFeeCurrency"`
		MinDmChatInitFeeNativeAmount *float64  `db:"minDmChatInitFeeNativeAmount"`
		CreatedAt                    time.Time `db:"createdAt"`
	}
	query := `SELECT "id", "displayName", "username", "profilePictureBlobId", "flipcardColor", "minDmChatInitFeeCurrency", "minDmChatInitFeeNativeAmount", "createdAt" FROM ` + usersTableName + ` WHERE "id" = ANY($1::text[])`
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

		userProfile := &profilepb.UserProfile{
			UserId:                &commonpb.UserId{Value: rawID},
			DisplayName:           *pointer.StringOrDefault(r.DisplayName, ""),
			JoinTs:                timestamppb.New(r.CreatedAt),
			FlipcardCustomization: profile.FlipcardCustomizationFromStored(r.FlipcardColor),
			MinDmChatInitFee:      profile.MinDmChatInitFeeFromStored(r.MinDmChatInitFeeCurrency, r.MinDmChatInitFeeNativeAmount),
		}

		if r.Username != nil {
			userProfile.Username = &commonpb.Username{Value: *r.Username}
		}

		if r.ProfilePictureBlobID != nil {
			rawBlobID, err := pg.Decode(*r.ProfilePictureBlobID)
			if err != nil {
				return nil, err
			}
			userProfile.ProfilePicture = &blobpb.Media{
				Renditions: []*blobpb.Rendition{{
					Role:   blobpb.Rendition_ORIGINAL,
					BlobId: &blobpb.BlobId{Value: rawBlobID},
				}},
			}
		}

		out[string(rawID)] = userProfile
	}
	return out, nil
}

func dbSetProfilePicture(ctx context.Context, pool *pgxpool.Pool, userID *commonpb.UserId, blobID *blobpb.BlobId) error {
	return pg.ExecuteInTx(ctx, pool, func(tx pgx.Tx) error {
		query := `INSERT INTO ` + usersTableName + ` (` + allUserFields + `) VALUES ($1, NULL, NULL, $2, NULL, NULL, NULL, NULL, NULL, NULL, FALSE, FALSE, FALSE, 'usd', 'en', NOW(), NOW()) ON CONFLICT ("id") DO UPDATE SET "profilePictureBlobId" = $2 WHERE ` + usersTableName + `."id" = $1`
		_, err := tx.Exec(ctx, query, pg.Encode(userID.Value), pg.Encode(blobID.Value))
		return err
	})
}

// dbSetFlipcardColor writes the colour to both flipcardColor, which every read
// uses, and the deprecated tipCardColor it replaced, so a server that still
// reads tipCardColor (a rollback, or an older instance mid-deploy) sees the same
// colour.
func dbSetFlipcardColor(ctx context.Context, pool *pgxpool.Pool, userID *commonpb.UserId, colorHex string) error {
	return pg.ExecuteInTx(ctx, pool, func(tx pgx.Tx) error {
		query := `INSERT INTO ` + usersTableName + ` (` + allUserFields + `) VALUES ($1, NULL, NULL, NULL, $2, $2, NULL, NULL, NULL, NULL, FALSE, FALSE, FALSE, 'usd', 'en', NOW(), NOW()) ON CONFLICT ("id") DO UPDATE SET "tipCardColor" = $2, "flipcardColor" = $2 WHERE ` + usersTableName + `."id" = $1`
		_, err := tx.Exec(ctx, query, pg.Encode(userID.Value), colorHex)
		return err
	})
}

func dbSetMinDmChatInitFee(ctx context.Context, pool *pgxpool.Pool, userID *commonpb.UserId, fee *commonpb.FiatPaymentAmount) error {
	return pg.ExecuteInTx(ctx, pool, func(tx pgx.Tx) error {
		query := `INSERT INTO ` + usersTableName + ` (` + allUserFields + `) VALUES ($1, NULL, NULL, NULL, NULL, NULL, $2, $3, NULL, NULL, FALSE, FALSE, FALSE, 'usd', 'en', NOW(), NOW()) ON CONFLICT ("id") DO UPDATE SET "minDmChatInitFeeCurrency" = $2, "minDmChatInitFeeNativeAmount" = $3 WHERE ` + usersTableName + `."id" = $1`
		_, err := tx.Exec(ctx, query, pg.Encode(userID.Value), fee.Currency, fee.NativeAmount)
		return err
	})
}

func dbGetPrivateProfile(ctx context.Context, pool *pgxpool.Pool, userID *commonpb.UserId) (phoneNumber, emailAddress *string, err error) {
	var res struct {
		PhoneNumber  *string `db:"phoneNumber"`
		EmailAddress *string `db:"emailAddress"`
	}
	query := `SELECT "phoneNumber", "emailAddress" FROM ` + usersTableName + ` WHERE "id" = $1`
	err = pgxscan.Get(
		ctx,
		pool,
		&res,
		query,
		pg.Encode(userID.Value),
	)
	if err != nil {
		if pgxscan.NotFound(err) {
			return nil, nil, profile.ErrNotFound
		}
		return nil, nil, err
	}
	return res.PhoneNumber, res.EmailAddress, nil
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

func dbGetPhonesByHashes(ctx context.Context, pool *pgxpool.Pool, hashes []*commonpb.Hash) ([]*commonpb.PhoneNumber, error) {
	matches, err := dbGetPhonesByHashesInternal(ctx, pool, hashes, false)
	if err != nil {
		return nil, err
	}
	out := make([]*commonpb.PhoneNumber, len(matches))
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
			PhoneNumber: &commonpb.PhoneNumber{Value: r.Phone},
			UserID:      &commonpb.UserId{Value: rawID},
			JoinedAt:    r.CreatedAt,
		})
	}
	return out, nil
}

func dbGetPhoneNumbersForPayment(ctx context.Context, pool *pgxpool.Pool, userIDs []*commonpb.UserId) (map[string]*commonpb.PhoneNumber, error) {
	out := make(map[string]*commonpb.PhoneNumber)
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
		out[string(rawID)] = &commonpb.PhoneNumber{Value: r.Phone}
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
