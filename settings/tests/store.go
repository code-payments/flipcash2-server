package tests

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"

	"github.com/code-payments/flipcash2-server/model"
	"github.com/code-payments/flipcash2-server/settings"
)

// CreateUserFunc creates a user and returns their UserId.
type CreateUserFunc func(t *testing.T) *commonpb.UserId

func RunStoreTests(t *testing.T, s settings.Store, createUser CreateUserFunc, teardown func()) {
	for _, tf := range []func(t *testing.T, s settings.Store, createUser CreateUserFunc){
		testStore_getDefaults,
		testStore_setRegion,
		testStore_setLocale,
		testStore_notFound,
	} {
		tf(t, s, createUser)
		teardown()
	}
}

func testStore_getDefaults(t *testing.T, s settings.Store, createUser CreateUserFunc) {
	ctx := context.Background()

	userID := createUser(t)

	p, err := s.GetSettings(ctx, userID)
	require.NoError(t, err)
	require.Equal(t, settings.DefaultRegion.Value, p.Region.Value)
	require.Equal(t, settings.DefaultLocale.Value, p.Locale.Value)
}

func testStore_setRegion(t *testing.T, s settings.Store, createUser CreateUserFunc) {
	ctx := context.Background()

	userID := createUser(t)

	require.NoError(t, s.SetRegion(ctx, userID, &commonpb.Region{Value: "eur"}))

	p, err := s.GetSettings(ctx, userID)
	require.NoError(t, err)
	require.Equal(t, "eur", p.Region.Value)
	require.Equal(t, settings.DefaultLocale.Value, p.Locale.Value)

	require.NoError(t, s.SetRegion(ctx, userID, &commonpb.Region{Value: "gbp"}))

	p, err = s.GetSettings(ctx, userID)
	require.NoError(t, err)
	require.Equal(t, "gbp", p.Region.Value)
	require.Equal(t, settings.DefaultLocale.Value, p.Locale.Value)
}

func testStore_setLocale(t *testing.T, s settings.Store, createUser CreateUserFunc) {
	ctx := context.Background()

	userID := createUser(t)

	require.NoError(t, s.SetLocale(ctx, userID, &commonpb.Locale{Value: "fr"}))

	p, err := s.GetSettings(ctx, userID)
	require.NoError(t, err)
	require.Equal(t, settings.DefaultRegion.Value, p.Region.Value)
	require.Equal(t, "fr", p.Locale.Value)

	require.NoError(t, s.SetLocale(ctx, userID, &commonpb.Locale{Value: "es"}))

	p, err = s.GetSettings(ctx, userID)
	require.NoError(t, err)
	require.Equal(t, settings.DefaultRegion.Value, p.Region.Value)
	require.Equal(t, "es", p.Locale.Value)
}

func testStore_notFound(t *testing.T, s settings.Store, _ CreateUserFunc) {
	ctx := context.Background()

	userID := model.MustGenerateUserID()

	_, err := s.GetSettings(ctx, userID)
	require.ErrorIs(t, err, settings.ErrNotFound)
}
