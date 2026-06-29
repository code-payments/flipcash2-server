package postgres

import (
	"context"
	"fmt"
	"time"

	"github.com/georgysavva/scany/v2/pgxscan"
	"github.com/jackc/pgx/v5"
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

type tokenModel struct {
	UserID       string    `db:"userId"`
	AppInstallID string    `db:"appInstallId"`
	Token        string    `db:"token"`
	Type         int       `db:"type"`
	CreatedAt    time.Time `db:"createdAt"`
	UpdatedAt    time.Time `db:"updatedAt"`
}

func toTokenModel(userID *commonpb.UserId, token push.Token) (*tokenModel, error) {
	return &tokenModel{
		UserID:       pg.Encode(userID.Value),
		AppInstallID: token.AppInstallID,
		Token:        token.Token,
		Type:         int(token.Type),
	}, nil
}

func fromTokenModel(m *tokenModel) (push.Token, error) {
	return push.Token{
		Type:         pushpb.TokenType(m.Type),
		AppInstallID: m.AppInstallID,
		Token:        m.Token,
	}, nil
}

func (m *tokenModel) dbAdd(ctx context.Context, pool *pgxpool.Pool) error {
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

func dbGetTokensBatch(ctx context.Context, pool *pgxpool.Pool, userIDs ...*commonpb.UserId) ([]*tokenModel, error) {
	var res []*tokenModel

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

func dbFilterUsersWithTokens(ctx context.Context, pool *pgxpool.Pool, userIDs ...*commonpb.UserId) ([]*commonpb.UserId, error) {
	if len(userIDs) == 0 {
		return nil, nil
	}

	queryParameters := make([]any, len(userIDs))
	query := `SELECT DISTINCT "userId" FROM ` + pushTokensTableName + ` WHERE "userId" IN (`
	for i, userID := range userIDs {
		queryParameters[i] = pg.Encode(userID.Value)
		if i > 0 {
			query += fmt.Sprintf(",$%d", i+1)
		} else {
			query += fmt.Sprintf("$%d", i+1)
		}
	}
	query += ")"

	var encodedUserIDs []string
	err := pgxscan.Select(ctx, pool, &encodedUserIDs, query, queryParameters...)
	if err != nil {
		if pgxscan.NotFound(err) {
			return nil, nil
		}
		return nil, err
	}

	// Build a set of matched encoded user IDs for fast lookup
	matched := make(map[string]struct{}, len(encodedUserIDs))
	for _, id := range encodedUserIDs {
		matched[id] = struct{}{}
	}

	// Return the original proto objects for matched users
	var result []*commonpb.UserId
	for _, userID := range userIDs {
		if _, ok := matched[pg.Encode(userID.Value)]; ok {
			result = append(result, userID)
		}
	}
	return result, nil
}

func dbDeleteToken(ctx context.Context, pool *pgxpool.Pool, tokenType pushpb.TokenType, token string) error {
	query := `DELETE FROM ` + pushTokensTableName + ` WHERE "token" = $1 and "type" = $2`
	_, err := pool.Exec(ctx, query, token, tokenType)
	return err
}

const (
	currencyStatesTableName = "flipcash_currency_push_states"
	allCurrencyStateFields  = `"mint", "allTimeHighSupply", "allTimeHighSlot", "lastGainPushAt", "createdAt", "updatedAt"`
)

type currencyStateModel struct {
	Mint              string     `db:"mint"`
	AllTimeHighSupply int64      `db:"allTimeHighSupply"`
	AllTimeHighSlot   int64      `db:"allTimeHighSlot"`
	LastGainPushAt    *time.Time `db:"lastGainPushAt"`
	CreatedAt         time.Time  `db:"createdAt"`
	UpdatedAt         time.Time  `db:"updatedAt"`
}

func fromCurrencyStateModel(m *currencyStateModel) (*push.CurrencyState, error) {
	mint, err := pg.Decode(m.Mint)
	if err != nil {
		return nil, err
	}
	return &push.CurrencyState{
		Mint:              &commonpb.PublicKey{Value: mint},
		AllTimeHighSupply: uint64(m.AllTimeHighSupply),
		AllTimeHighSlot:   uint64(m.AllTimeHighSlot),
		LastGainPushAt:    m.LastGainPushAt,
	}, nil
}

// dbClaimGainPush performs the atomic new-all-time-high + cooldown gate. The row
// (all-time high and last-gain-push timestamp) is updated only when the push is
// granted; otherwise the stored state is left untouched. Either way it returns
// the resulting stored state so callers can populate a local cache.
//
// A data-modifying CTE performs the conditional upsert while a sibling CTE reads
// the pre-upsert row (CTEs share one snapshot, so "stored" never sees the
// upsert's effects). The final SELECT prefers the upserted values when present
// (granted) and falls back to the stored values otherwise. The FULL OUTER JOIN
// keeps the single result row in the first-insert case, where no prior row
// exists.
func dbClaimGainPush(ctx context.Context, pool *pgxpool.Pool, mint *commonpb.PublicKey, supply, slot uint64, cooldown time.Duration) (bool, *push.CurrencyState, error) {
	encodedMint := pg.Encode(mint.Value, pg.Base58)
	var (
		granted        bool
		highSupply     int64
		highSlot       int64
		lastGainPushAt *time.Time
	)
	err := pg.ExecuteInTx(ctx, pool, func(tx pgx.Tx) error {
		query := `WITH attempted AS (
			INSERT INTO ` + currencyStatesTableName + ` AS t
				("mint", "allTimeHighSupply", "allTimeHighSlot", "lastGainPushAt", "createdAt", "updatedAt")
				VALUES ($1, $2, $3, NOW(), NOW(), NOW())
				ON CONFLICT ("mint") DO UPDATE SET
					"allTimeHighSupply" = EXCLUDED."allTimeHighSupply",
					"allTimeHighSlot"   = EXCLUDED."allTimeHighSlot",
					"lastGainPushAt"    = NOW(),
					"updatedAt"         = NOW()
				WHERE t."allTimeHighSupply" < EXCLUDED."allTimeHighSupply"
				  AND (t."lastGainPushAt" IS NULL
				       OR t."lastGainPushAt" <= NOW() - make_interval(secs => $4))
				RETURNING "allTimeHighSupply", "allTimeHighSlot", "lastGainPushAt"
		),
		stored AS (
			SELECT "allTimeHighSupply", "allTimeHighSlot", "lastGainPushAt"
			FROM ` + currencyStatesTableName + `
			WHERE "mint" = $1
		)
		SELECT
			(a."allTimeHighSupply" IS NOT NULL) AS granted,
			COALESCE(a."allTimeHighSupply", s."allTimeHighSupply") AS high_supply,
			COALESCE(a."allTimeHighSlot", s."allTimeHighSlot") AS high_slot,
			COALESCE(a."lastGainPushAt", s."lastGainPushAt") AS last_gain_push_at
		FROM attempted a
		FULL OUTER JOIN stored s ON true`

		return tx.QueryRow(ctx, query, encodedMint, int64(supply), int64(slot), cooldown.Seconds()).
			Scan(&granted, &highSupply, &highSlot, &lastGainPushAt)
	})
	if err != nil {
		return false, nil, err
	}
	return granted, &push.CurrencyState{
		Mint:              mint,
		AllTimeHighSupply: uint64(highSupply),
		AllTimeHighSlot:   uint64(highSlot),
		LastGainPushAt:    lastGainPushAt,
	}, nil
}

func dbGetCurrencyState(ctx context.Context, pool *pgxpool.Pool, mint *commonpb.PublicKey) (*currencyStateModel, error) {
	res := &currencyStateModel{}
	query := `SELECT ` + allCurrencyStateFields + ` FROM ` + currencyStatesTableName + ` WHERE "mint" = $1`
	err := pgxscan.Get(ctx, pool, res, query, pg.Encode(mint.Value, pg.Base58))
	if err != nil {
		if pgxscan.NotFound(err) {
			return nil, push.ErrCurrencyStateNotFound
		}
		return nil, err
	}
	return res, nil
}
