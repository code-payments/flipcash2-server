package iap

import (
	"context"
	"errors"
	"time"

	"google.golang.org/protobuf/proto"

	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"
)

var (
	ErrExists   = errors.New("iap already exists")
	ErrNotFound = errors.New("iap not found")
)

type Product uint8

const (
	ProductUnknown Product = iota
	ProductCreateAccount
	ProductCreateAccountBonusGoogle
	ProductCreateAccountBonusApple
)

type State uint8

const (
	StateUnknown State = iota
	StateWaitingForPayment
	StateWaitingForFulfillment
	StateFulfilled
)

type Purchase struct {
	ReceiptID       []byte
	Platform        commonpb.Platform
	User            *commonpb.UserId
	Product         Product
	PaymentAmount   float64
	PaymentCurrency string
	State           State
	CreatedAt       time.Time
}

type Store interface {
	CreatePurchase(ctx context.Context, purchase *Purchase) error
	GetPurchaseByID(ctx context.Context, receiptID []byte) (*Purchase, error)
	GetPurchasesByUserAndProduct(ctx context.Context, userID *commonpb.UserId, product Product) ([]*Purchase, error)
}

func (p *Purchase) Clone() *Purchase {
	return &Purchase{
		ReceiptID:       p.ReceiptID,
		Platform:        p.Platform,
		User:            proto.Clone(p.User).(*commonpb.UserId),
		Product:         p.Product,
		PaymentAmount:   p.PaymentAmount,
		PaymentCurrency: p.PaymentCurrency,
		State:           p.State,
		CreatedAt:       p.CreatedAt,
	}
}
