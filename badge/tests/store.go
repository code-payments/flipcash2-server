package tests

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"

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
		testStore_IncrementBatch,
		testStore_IncrementBatchEmpty,
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

// testStore_IncrementBatch verifies a batch increments every user, reports each
// user's own post-increment value, and accumulates on top of prior counts —
// sized past any implementation's concurrency limit so the pool wraps around.
func testStore_IncrementBatch(t *testing.T, s badge.Store) {
	ctx := context.Background()

	const numUsers = 100
	users := make([]*commonpb.UserId, numUsers)
	for i := range users {
		users[i] = model.MustGenerateUserID()
	}

	// One user starts with a prior count, so their result must carry it.
	_, err := s.Increment(ctx, users[0], 5)
	require.NoError(t, err)

	counts, err := s.IncrementBatch(ctx, users, 1)
	require.NoError(t, err)
	require.Len(t, counts, numUsers)

	for i, user := range users {
		want := uint64(1)
		if i == 0 {
			want = 6
		}
		require.EqualValues(t, want, counts[string(user.Value)], "user %d", i)

		stored, err := s.Get(ctx, user)
		require.NoError(t, err)
		require.EqualValues(t, want, stored, "user %d", i)
	}

	// A second batch with a larger delta accumulates, as Increment does.
	counts, err = s.IncrementBatch(ctx, users[:3], 4)
	require.NoError(t, err)
	require.Len(t, counts, 3)
	require.EqualValues(t, 10, counts[string(users[0].Value)])
	require.EqualValues(t, 5, counts[string(users[1].Value)])
	require.EqualValues(t, 5, counts[string(users[2].Value)])

	// Users outside the second batch are untouched.
	stored, err := s.Get(ctx, users[3])
	require.NoError(t, err)
	require.EqualValues(t, 1, stored)
}

func testStore_IncrementBatchEmpty(t *testing.T, s badge.Store) {
	ctx := context.Background()

	counts, err := s.IncrementBatch(ctx, nil, 1)
	require.NoError(t, err)
	require.Empty(t, counts)
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
