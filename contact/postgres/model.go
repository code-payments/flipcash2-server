package postgres

import (
	"context"

	"github.com/georgysavva/scany/v2/pgxscan"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"

	"github.com/code-payments/flipcash2-server/contact"
	pg "github.com/code-payments/flipcash2-server/database/postgres"
)

const (
	contactListsTableName       = "flipcash_contact_lists"
	contactListEntriesTableName = "flipcash_contact_list_entries"
)

// encodeHash hex-encodes a 32-byte commonpb.Hash with the pg short prefix.
func encodeHash(h *commonpb.Hash) string {
	return pg.Encode(h.Value, pg.Hex)
}

func decodeHash(encoded string) (*commonpb.Hash, error) {
	raw, err := pg.Decode(encoded)
	if err != nil {
		return nil, err
	}
	return &commonpb.Hash{Value: raw}, nil
}

// encodeHashes encodes a slice of hashes and de-duplicates them, since the
// underlying primary key forbids duplicates and we don't want SQL-level
// errors on caller-side dupes.
func encodeHashes(hashes []*commonpb.Hash) []string {
	if len(hashes) == 0 {
		return nil
	}
	seen := make(map[string]struct{}, len(hashes))
	out := make([]string, 0, len(hashes))
	for _, h := range hashes {
		encoded := encodeHash(h)
		if _, ok := seen[encoded]; ok {
			continue
		}
		seen[encoded] = struct{}{}
		out = append(out, encoded)
	}
	return out
}

// dbGetChecksum returns the user's stored checksum, or contact.ErrNotFound
// when no contact list row exists.
func dbGetChecksum(ctx context.Context, pool *pgxpool.Pool, userID *commonpb.UserId) (*commonpb.Hash, error) {
	var encoded string
	query := `SELECT "checksum" FROM ` + contactListsTableName + ` WHERE "userId" = $1`
	err := pgxscan.Get(ctx, pool, &encoded, query, pg.Encode(userID.Value))
	if err != nil {
		if pgxscan.NotFound(err) {
			return nil, contact.ErrNotFound
		}
		return nil, err
	}
	return decodeHash(encoded)
}

// dbGetHashes returns all phoneNumberHash entries for the user. Returns
// contact.ErrNotFound when the user has no contact_lists row at all (the
// presence of the parent row distinguishes "no upload yet" from "uploaded
// an empty set").
func dbGetHashes(ctx context.Context, pool *pgxpool.Pool, userID *commonpb.UserId) ([]*commonpb.Hash, error) {
	encodedUserID := pg.Encode(userID.Value)

	var exists bool
	err := pgxscan.Get(
		ctx, pool, &exists,
		`SELECT EXISTS(SELECT 1 FROM `+contactListsTableName+` WHERE "userId" = $1)`,
		encodedUserID,
	)
	if err != nil {
		return nil, err
	}
	if !exists {
		return nil, contact.ErrNotFound
	}

	var encoded []string
	err = pgxscan.Select(
		ctx, pool, &encoded,
		`SELECT "phoneNumberHash" FROM `+contactListEntriesTableName+` WHERE "userId" = $1`,
		encodedUserID,
	)
	if err != nil && !pgxscan.NotFound(err) {
		return nil, err
	}

	out := make([]*commonpb.Hash, 0, len(encoded))
	for _, e := range encoded {
		h, err := decodeHash(e)
		if err != nil {
			return nil, err
		}
		out = append(out, h)
	}
	return out, nil
}

// dbGetUserIdsByPhoneHash returns every userId whose contact list contains
// phoneNumberHash. Returns an empty slice (no error) when no users have the
// hash.
func dbGetUserIdsByPhoneHash(ctx context.Context, pool *pgxpool.Pool, phoneNumberHash *commonpb.Hash) ([]*commonpb.UserId, error) {
	var encoded []string
	err := pgxscan.Select(
		ctx, pool, &encoded,
		`SELECT "userId" FROM `+contactListEntriesTableName+` WHERE "phoneNumberHash" = $1`,
		encodeHash(phoneNumberHash),
	)
	if err != nil && !pgxscan.NotFound(err) {
		return nil, err
	}

	out := make([]*commonpb.UserId, 0, len(encoded))
	for _, e := range encoded {
		raw, err := pg.Decode(e)
		if err != nil {
			return nil, err
		}
		out = append(out, &commonpb.UserId{Value: raw})
	}
	return out, nil
}

// dbApplyDelta atomically applies adds/removes under compare-and-swap on the
// checksum. Returns contact.ErrChecksumDrift, contact.ErrTooManyContacts, or
// nil (which includes the idempotent retry case where the stored checksum
// already equals newChecksum).
func dbApplyDelta(
	ctx context.Context,
	pool *pgxpool.Pool,
	userID *commonpb.UserId,
	addHashes []*commonpb.Hash,
	removeHashes []*commonpb.Hash,
	oldChecksum *commonpb.Hash,
	newChecksum *commonpb.Hash,
) error {
	encodedUserID := pg.Encode(userID.Value)
	encodedZero := encodeHash(contact.ZeroChecksum())
	encodedOld := encodeHash(oldChecksum)
	encodedNew := encodeHash(newChecksum)
	encodedAdds := encodeHashes(addHashes)
	encodedRemoves := encodeHashes(removeHashes)

	return pg.ExecuteInTx(ctx, pool, func(tx pgx.Tx) error {
		// Upsert-lock the parent row. On first upload this inserts a fresh
		// row with the zero checksum; on subsequent uploads the no-op
		// DO UPDATE forces Postgres to acquire the row-level lock that a
		// real UPDATE would, serializing concurrent DeltaUploads for the
		// same user. RETURNING gives us the currently-stored checksum.
		var storedChecksum string
		err := pgxscan.Get(
			ctx, tx, &storedChecksum,
			`INSERT INTO `+contactListsTableName+` ("userId", "checksum", "createdAt", "updatedAt") VALUES ($1, $2, NOW(), NOW()) ON CONFLICT ("userId") DO UPDATE SET "userId" = EXCLUDED."userId" RETURNING "checksum"`,
			encodedUserID, encodedZero,
		)
		if err != nil {
			return err
		}

		switch storedChecksum {
		case encodedNew:
			return nil // Idempotent retry.
		case encodedOld:
			// Proceed.
		default:
			return contact.ErrChecksumDrift
		}

		if len(encodedRemoves) > 0 {
			if _, err := tx.Exec(
				ctx,
				`DELETE FROM `+contactListEntriesTableName+` WHERE "userId" = $1 AND "phoneNumberHash" = ANY($2::text[])`,
				encodedUserID, encodedRemoves,
			); err != nil {
				return err
			}
		}

		if len(encodedAdds) > 0 {
			if _, err := tx.Exec(
				ctx,
				`INSERT INTO `+contactListEntriesTableName+` ("userId", "phoneNumberHash", "createdAt") SELECT $1, h, NOW() FROM UNNEST($2::text[]) AS h ON CONFLICT ("userId", "phoneNumberHash") DO NOTHING`,
				encodedUserID, encodedAdds,
			); err != nil {
				return err
			}
		}

		var count int
		if err := pgxscan.Get(
			ctx, tx, &count,
			`SELECT COUNT(*) FROM `+contactListEntriesTableName+` WHERE "userId" = $1`,
			encodedUserID,
		); err != nil {
			return err
		}
		if count > contact.MaxContactsPerUser {
			return contact.ErrTooManyContacts
		}

		_, err = tx.Exec(
			ctx,
			`UPDATE `+contactListsTableName+` SET "checksum" = $2, "updatedAt" = NOW() WHERE "userId" = $1`,
			encodedUserID, encodedNew,
		)
		return err
	})
}

// dbReplace replaces the user's contact set entirely.
func dbReplace(
	ctx context.Context,
	pool *pgxpool.Pool,
	userID *commonpb.UserId,
	hashes []*commonpb.Hash,
	expectedChecksum *commonpb.Hash,
) error {
	if len(hashes) > contact.MaxContactsPerUser {
		return contact.ErrTooManyContacts
	}

	encodedUserID := pg.Encode(userID.Value)
	encodedChecksum := encodeHash(expectedChecksum)
	encodedHashes := encodeHashes(hashes)

	return pg.ExecuteInTx(ctx, pool, func(tx pgx.Tx) error {
		// Upsert the parent row, both to store the new checksum and to
		// take the row-level lock serializing against concurrent
		// DeltaUpload / FullUpload for the same user.
		if _, err := tx.Exec(
			ctx,
			`INSERT INTO `+contactListsTableName+` ("userId", "checksum", "createdAt", "updatedAt") VALUES ($1, $2, NOW(), NOW()) ON CONFLICT ("userId") DO UPDATE SET "checksum" = EXCLUDED."checksum", "updatedAt" = NOW()`,
			encodedUserID, encodedChecksum,
		); err != nil {
			return err
		}

		if _, err := tx.Exec(
			ctx,
			`DELETE FROM `+contactListEntriesTableName+` WHERE "userId" = $1`,
			encodedUserID,
		); err != nil {
			return err
		}

		if len(encodedHashes) > 0 {
			if _, err := tx.Exec(
				ctx,
				`INSERT INTO `+contactListEntriesTableName+` ("userId", "phoneNumberHash", "createdAt") SELECT $1, h, NOW() FROM UNNEST($2::text[]) AS h`,
				encodedUserID, encodedHashes,
			); err != nil {
				return err
			}
		}

		return nil
	})
}
