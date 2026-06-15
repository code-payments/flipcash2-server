package tests

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/code-payments/flipcash2-server/badge"
	"github.com/code-payments/flipcash2-server/model"
)

// RunStoreTests runs the shared badge.Store test suite against s. teardown is
// called between tests to reset the store.
func RunStoreTests(t *testing.T, s badge.Store, teardown func()) {
	for _, tf := range []func(t *testing.T, s badge.Store){
		testStore_GetEmpty,
		testStore_Increment,
		testStore_IncrementAccumulates,
		testStore_Reset,
		testStore_PerUserIsolation,
	} {
		tf(t, s)
		teardown()
	}
}

func testStore_GetEmpty(t *testing.T, s badge.Store) {
	ctx := context.Background()

	count, err := s.Get(ctx, model.MustGenerateUserID())
	require.NoError(t, err)
	require.EqualValues(t, 0, count)
}

func testStore_Increment(t *testing.T, s badge.Store) {
	ctx := context.Background()
	user := model.MustGenerateUserID()

	count, err := s.Increment(ctx, user, 1)
	require.NoError(t, err)
	require.EqualValues(t, 1, count)

	count, err = s.Get(ctx, user)
	require.NoError(t, err)
	require.EqualValues(t, 1, count)
}

func testStore_IncrementAccumulates(t *testing.T, s badge.Store) {
	ctx := context.Background()
	user := model.MustGenerateUserID()

	count, err := s.Increment(ctx, user, 1)
	require.NoError(t, err)
	require.EqualValues(t, 1, count)

	// A larger delta stands in for a coalesced batch of messages.
	count, err = s.Increment(ctx, user, 5)
	require.NoError(t, err)
	require.EqualValues(t, 6, count)

	count, err = s.Get(ctx, user)
	require.NoError(t, err)
	require.EqualValues(t, 6, count)
}

func testStore_Reset(t *testing.T, s badge.Store) {
	ctx := context.Background()
	user := model.MustGenerateUserID()

	_, err := s.Increment(ctx, user, 3)
	require.NoError(t, err)

	require.NoError(t, s.Reset(ctx, user))

	count, err := s.Get(ctx, user)
	require.NoError(t, err)
	require.EqualValues(t, 0, count)

	// Counting resumes from zero after a reset.
	count, err = s.Increment(ctx, user, 2)
	require.NoError(t, err)
	require.EqualValues(t, 2, count)

	// Resetting a user that has no badge is a no-op, not an error.
	require.NoError(t, s.Reset(ctx, model.MustGenerateUserID()))
}

func testStore_PerUserIsolation(t *testing.T, s badge.Store) {
	ctx := context.Background()
	a := model.MustGenerateUserID()
	b := model.MustGenerateUserID()

	_, err := s.Increment(ctx, a, 4)
	require.NoError(t, err)

	// b is untouched by a's increment.
	count, err := s.Get(ctx, b)
	require.NoError(t, err)
	require.EqualValues(t, 0, count)

	// Resetting a must not touch b, nor b's increment touch a.
	require.NoError(t, s.Reset(ctx, a))
	_, err = s.Increment(ctx, b, 7)
	require.NoError(t, err)

	count, err = s.Get(ctx, a)
	require.NoError(t, err)
	require.EqualValues(t, 0, count)

	count, err = s.Get(ctx, b)
	require.NoError(t, err)
	require.EqualValues(t, 7, count)
}
