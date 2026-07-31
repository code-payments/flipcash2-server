package tests

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	thirdpartypb "github.com/code-payments/flipcash2-protobuf-api/generated/go/thirdparty/v1"

	"github.com/code-payments/flipcash2-server/kyc"
	"github.com/code-payments/flipcash2-server/model"
)

// RunStoreTests runs the shared kyc.Store test suite against s. teardown is
// called between tests to reset the store.
func RunStoreTests(t *testing.T, s kyc.Store, teardown func()) {
	for _, tf := range []func(t *testing.T, s kyc.Store){
		testStore_CreateAndGet,
		testStore_Create_Duplicate,
		testStore_Get_NotFound,
		testStore_Get_PartnerScoped,
		testStore_GetByCustomerID,
	} {
		tf(t, s)
		teardown()
	}
}

func testStore_CreateAndGet(t *testing.T, s kyc.Store) {
	ctx := context.Background()

	record := &kyc.Record{
		UserID:     model.MustGenerateUserID(),
		Partner:    thirdpartypb.Partner_BRIDGE,
		CustomerID: "cust_11111111-2222-3333-4444-555555555555",
		CreatedAt:  time.Now().UTC(),
	}
	require.NoError(t, s.Create(ctx, record))

	got, err := s.Get(ctx, record.UserID, thirdpartypb.Partner_BRIDGE)
	require.NoError(t, err)
	require.Equal(t, record.UserID.Value, got.UserID.Value)
	require.Equal(t, thirdpartypb.Partner_BRIDGE, got.Partner)
	require.Equal(t, record.CustomerID, got.CustomerID)
	require.True(t, got.CreatedAt.Equal(record.CreatedAt))
}

func testStore_Create_Duplicate(t *testing.T, s kyc.Store) {
	ctx := context.Background()

	record := &kyc.Record{
		UserID:     model.MustGenerateUserID(),
		Partner:    thirdpartypb.Partner_BRIDGE,
		CustomerID: "cust_original",
		CreatedAt:  time.Now().UTC(),
	}
	require.NoError(t, s.Create(ctx, record))

	// A second create for the same (user, partner) fails even with a
	// different customer ID, preserving the original mapping.
	duplicate := record.Clone()
	duplicate.CustomerID = "cust_duplicate"
	require.ErrorIs(t, s.Create(ctx, duplicate), kyc.ErrExists)

	got, err := s.Get(ctx, record.UserID, thirdpartypb.Partner_BRIDGE)
	require.NoError(t, err)
	require.Equal(t, "cust_original", got.CustomerID)
}

func testStore_Get_NotFound(t *testing.T, s kyc.Store) {
	ctx := context.Background()

	_, err := s.Get(ctx, model.MustGenerateUserID(), thirdpartypb.Partner_BRIDGE)
	require.ErrorIs(t, err, kyc.ErrNotFound)
}

func testStore_Get_PartnerScoped(t *testing.T, s kyc.Store) {
	ctx := context.Background()

	userID := model.MustGenerateUserID()
	require.NoError(t, s.Create(ctx, &kyc.Record{
		UserID:     userID,
		Partner:    thirdpartypb.Partner_BRIDGE,
		CustomerID: "cust_bridge",
		CreatedAt:  time.Now().UTC(),
	}))

	// The record is scoped to its partner.
	_, err := s.Get(ctx, userID, thirdpartypb.Partner_COINBASE)
	require.ErrorIs(t, err, kyc.ErrNotFound)

	// The same user can hold a record with a different partner.
	require.NoError(t, s.Create(ctx, &kyc.Record{
		UserID:     userID,
		Partner:    thirdpartypb.Partner_COINBASE,
		CustomerID: "cust_coinbase",
		CreatedAt:  time.Now().UTC(),
	}))

	got, err := s.Get(ctx, userID, thirdpartypb.Partner_BRIDGE)
	require.NoError(t, err)
	require.Equal(t, "cust_bridge", got.CustomerID)

	got, err = s.Get(ctx, userID, thirdpartypb.Partner_COINBASE)
	require.NoError(t, err)
	require.Equal(t, "cust_coinbase", got.CustomerID)
}

func testStore_GetByCustomerID(t *testing.T, s kyc.Store) {
	ctx := context.Background()

	record := &kyc.Record{
		UserID:     model.MustGenerateUserID(),
		Partner:    thirdpartypb.Partner_BRIDGE,
		CustomerID: "cust_11111111-2222-3333-4444-555555555555",
		CreatedAt:  time.Now().UTC(),
	}
	require.NoError(t, s.Create(ctx, record))

	got, err := s.GetByCustomerID(ctx, thirdpartypb.Partner_BRIDGE, record.CustomerID)
	require.NoError(t, err)
	require.Equal(t, record.UserID.Value, got.UserID.Value)
	require.Equal(t, thirdpartypb.Partner_BRIDGE, got.Partner)
	require.Equal(t, record.CustomerID, got.CustomerID)
	require.True(t, got.CreatedAt.Equal(record.CreatedAt))

	// The lookup is scoped to the requested partner's ID space.
	_, err = s.GetByCustomerID(ctx, thirdpartypb.Partner_COINBASE, record.CustomerID)
	require.ErrorIs(t, err, kyc.ErrNotFound)

	_, err = s.GetByCustomerID(ctx, thirdpartypb.Partner_BRIDGE, "cust_unknown")
	require.ErrorIs(t, err, kyc.ErrNotFound)
}
