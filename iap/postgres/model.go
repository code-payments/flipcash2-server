package postgres

import (
	"context"
	"strings"
	"time"

	"github.com/georgysavva/scany/v2/pgxscan"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"

	pg "github.com/code-payments/flipcash2-server/database/postgres"
	"github.com/code-payments/flipcash2-server/iap"
)

const (
	iapsTableName = "flipcash_iap"
	allIapFields  = `"receiptId", "platform", "userId", "product", "paymentAmount", "paymentCurrency", "state", "createdAt"`
)

type model struct {
	ReceiptID       string    `db:"receiptId"`
	Platform        int       `db:"platform"`
	UserID          string    `db:"userId"`
	Product         int       `db:"product"`
	PaymentAmount   float64   `db:"paymentAmount"`
	PaymentCurrency string    `db:"paymentCurrency"`
	State           int       `db:"state"`
	CreatedAt       time.Time `db:"createdAt"`
}

func toModel(purchase *iap.Purchase) (*model, error) {
	return &model{
		ReceiptID:       pg.Encode(purchase.ReceiptID),
		Platform:        int(purchase.Platform),
		UserID:          pg.Encode(purchase.User.Value),
		Product:         int(purchase.Product),
		PaymentAmount:   purchase.PaymentAmount,
		PaymentCurrency: purchase.PaymentCurrency,
		State:           int(purchase.State),
	}, nil
}

func fromModel(m *model) (*iap.Purchase, error) {
	decodedReceiptID, err := pg.Decode(m.ReceiptID)
	if err != nil {
		return nil, err
	}

	decodedUserID, err := pg.Decode(m.UserID)
	if err != nil {
		return nil, err
	}

	return &iap.Purchase{
		ReceiptID:       decodedReceiptID,
		Platform:        commonpb.Platform(m.Platform),
		User:            &commonpb.UserId{Value: decodedUserID},
		Product:         iap.Product(m.Product),
		PaymentAmount:   m.PaymentAmount,
		PaymentCurrency: m.PaymentCurrency,
		State:           iap.State(m.State),
	}, nil
}

func (m *model) dbPut(ctx context.Context, pool *pgxpool.Pool) error {
	return pg.ExecuteInTx(ctx, pool, func(tx pgx.Tx) error {
		query := `INSERT INTO ` + iapsTableName + `(` + allIapFields + `) VALUES ($1, $2, $3, $4, $5, $6, $7, NOW()) RETURNING ` + allIapFields
		err := pgxscan.Get(
			ctx,
			tx,
			m,
			query,
			m.ReceiptID,
			m.Platform,
			m.UserID,
			m.Product,
			m.PaymentAmount,
			m.PaymentCurrency,
			m.State,
		)
		if err == nil {
			return nil
		} else if strings.Contains(err.Error(), "23505") { // todo: better utility for detecting unique violations with pgxscan
			return iap.ErrExists
		}
		return err
	})
}

func dbGetPurchaseByID(ctx context.Context, pool *pgxpool.Pool, receiptID []byte) (*model, error) {
	res := &model{}
	query := `SELECT ` + allIapFields + ` FROM ` + iapsTableName + ` WHERE "receiptId" = $1`
	err := pgxscan.Get(
		ctx,
		pool,
		res,
		query,
		pg.Encode(receiptID),
	)
	if err != nil {
		if pgxscan.NotFound(err) {
			return nil, iap.ErrNotFound
		}
		return nil, err
	}
	return res, nil
}

func dbGetPurchasesByUserAndProduct(ctx context.Context, pool *pgxpool.Pool, userID *commonpb.UserId, product iap.Product) ([]*model, error) {
	var res []*model
	query := `SELECT ` + allIapFields + ` FROM ` + iapsTableName + ` WHERE "userId" = $1 AND "product" = $2`
	err := pgxscan.Select(
		ctx,
		pool,
		&res,
		query,
		pg.Encode(userID.Value),
		product,
	)
	if err != nil {
		if pgxscan.NotFound(err) {
			return nil, iap.ErrNotFound
		}
		return nil, err
	}
	if len(res) == 0 {
		return nil, iap.ErrNotFound
	}
	return res, nil
}
