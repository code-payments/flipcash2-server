package tests

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"

	"github.com/code-payments/flipcash2-server/iap"
	"github.com/code-payments/flipcash2-server/model"
	"github.com/code-payments/flipcash2-server/protoutil"
)

func RunStoreTests(t *testing.T, s iap.Store, teardown func()) {
	for _, tf := range []func(t *testing.T, s iap.Store){
		testIapStore_HappyPath,
	} {
		tf(t, s)
		teardown()
	}
}

func testIapStore_HappyPath(t *testing.T, store iap.Store) {
	expected := &iap.Purchase{
		ReceiptID:       []byte("receipt"),
		Platform:        commonpb.Platform_APPLE,
		User:            model.MustGenerateUserID(),
		Product:         iap.ProductCreateAccountBonusApple,
		PaymentAmount:   1.23,
		PaymentCurrency: "usd",
		State:           iap.StateFulfilled,
		CreatedAt:       time.Now(),
	}

	_, err := store.GetPurchaseByID(context.Background(), expected.ReceiptID)
	require.Equal(t, iap.ErrNotFound, err)

	_, err = store.GetPurchasesByUserAndProduct(context.Background(), expected.User, iap.ProductCreateAccountBonusApple)
	require.Equal(t, iap.ErrNotFound, err)

	require.NoError(t, store.CreatePurchase(context.Background(), expected))

	actual, err := store.GetPurchaseByID(context.Background(), expected.ReceiptID)
	require.NoError(t, err)
	require.Equal(t, expected.ReceiptID, actual.ReceiptID)
	require.Equal(t, expected.Platform, actual.Platform)
	require.NoError(t, protoutil.ProtoEqualError(expected.User, actual.User))
	require.Equal(t, expected.Product, actual.Product)
	require.Equal(t, expected.PaymentAmount, actual.PaymentAmount)
	require.Equal(t, expected.PaymentCurrency, actual.PaymentCurrency)
	require.Equal(t, expected.State, actual.State)

	_, err = store.GetPurchasesByUserAndProduct(context.Background(), expected.User, iap.ProductCreateAccountBonusGoogle)
	require.Equal(t, iap.ErrNotFound, err)

	_, err = store.GetPurchasesByUserAndProduct(context.Background(), model.MustGenerateUserID(), iap.ProductCreateAccount)
	require.Equal(t, iap.ErrNotFound, err)

	byUserAndProduct, err := store.GetPurchasesByUserAndProduct(context.Background(), expected.User, iap.ProductCreateAccountBonusApple)
	require.NoError(t, err)
	require.Len(t, byUserAndProduct, 1)
	require.Equal(t, expected.ReceiptID, byUserAndProduct[0].ReceiptID)

	require.Equal(t, iap.ErrExists, store.CreatePurchase(context.Background(), expected))
}
