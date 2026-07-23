package tests

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"

	"github.com/code-payments/flipcash2-server/blocklist"
	"github.com/code-payments/flipcash2-server/model"
)

// RunStoreTests runs the shared blocklist.Store test suite against s. teardown
// is called between tests to reset the store.
func RunStoreTests(t *testing.T, s blocklist.Store, teardown func()) {
	for _, tf := range []func(t *testing.T, s blocklist.Store){
		testStore_BlockAndIsBlocked,
		testStore_Block_Idempotent,
		testStore_Unblock,
		testStore_Unblock_NotBlocked,
		testStore_Isolation,
		testStore_GetBlocklistPage_Order,
		testStore_GetBlocklistPage_Paging,
		testStore_GetBlocklistPage_Empty,
	} {
		tf(t, s)
		teardown()
	}
}

func testStore_BlockAndIsBlocked(t *testing.T, s blocklist.Store) {
	ctx := context.Background()

	owner := model.MustGenerateUserID()
	blocked := model.MustGenerateUserID()

	// Not blocked initially.
	is, err := s.IsBlocked(ctx, owner, blocked)
	require.NoError(t, err)
	require.False(t, is)

	added, err := s.Block(ctx, owner, blocked, at(100))
	require.NoError(t, err)
	require.True(t, added)

	is, err = s.IsBlocked(ctx, owner, blocked)
	require.NoError(t, err)
	require.True(t, is)
}

func testStore_Block_Idempotent(t *testing.T, s blocklist.Store) {
	ctx := context.Background()

	owner := model.MustGenerateUserID()
	blocked := model.MustGenerateUserID()

	added, err := s.Block(ctx, owner, blocked, at(100))
	require.NoError(t, err)
	require.True(t, added)

	// Re-blocking is a no-op: no new entry is added, and the original blocked_at
	// is preserved (not advanced to the later timestamp).
	added, err = s.Block(ctx, owner, blocked, at(200))
	require.NoError(t, err)
	require.False(t, added)

	page, err := s.GetBlocklistPage(ctx, owner, nil, 0)
	require.NoError(t, err)
	require.Len(t, page, 1)
	require.True(t, page[0].BlockedAt.Equal(at(100)))
}

func testStore_Unblock(t *testing.T, s blocklist.Store) {
	ctx := context.Background()

	owner := model.MustGenerateUserID()
	blocked := model.MustGenerateUserID()

	added, err := s.Block(ctx, owner, blocked, at(100))
	require.NoError(t, err)
	require.True(t, added)

	removed, err := s.Unblock(ctx, owner, blocked)
	require.NoError(t, err)
	require.True(t, removed)

	is, err := s.IsBlocked(ctx, owner, blocked)
	require.NoError(t, err)
	require.False(t, is)
}

func testStore_Unblock_NotBlocked(t *testing.T, s blocklist.Store) {
	ctx := context.Background()

	// Unblocking a user that was never blocked is a no-op.
	removed, err := s.Unblock(ctx, model.MustGenerateUserID(), model.MustGenerateUserID())
	require.NoError(t, err)
	require.False(t, removed)
}

func testStore_Isolation(t *testing.T, s blocklist.Store) {
	ctx := context.Background()

	ownerA := model.MustGenerateUserID()
	ownerB := model.MustGenerateUserID()
	target := model.MustGenerateUserID()

	added, err := s.Block(ctx, ownerA, target, at(100))
	require.NoError(t, err)
	require.True(t, added)

	// A blocklist is scoped to its owner: A blocked target, B did not.
	isA, err := s.IsBlocked(ctx, ownerA, target)
	require.NoError(t, err)
	require.True(t, isA)

	isB, err := s.IsBlocked(ctx, ownerB, target)
	require.NoError(t, err)
	require.False(t, isB)

	pageB, err := s.GetBlocklistPage(ctx, ownerB, nil, 0)
	require.NoError(t, err)
	require.Empty(t, pageB)
}

func testStore_GetBlocklistPage_Order(t *testing.T, s blocklist.Store) {
	ctx := context.Background()

	owner := model.MustGenerateUserID()

	// Three entries the owner blocked, plus one blocked by someone else.
	b1 := block(t, s, owner, at(100))
	b2 := block(t, s, owner, at(300))
	b3 := block(t, s, owner, at(200))
	_ = block(t, s, model.MustGenerateUserID(), at(999))

	// Most recently blocked first, and only this owner's entries.
	got, err := s.GetBlocklistPage(ctx, owner, nil, 0)
	require.NoError(t, err)
	require.Equal(t, [][]byte{b2.Value, b3.Value, b1.Value}, blockedIDs(got))
}

func testStore_GetBlocklistPage_Paging(t *testing.T, s blocklist.Store) {
	ctx := context.Background()

	owner := model.MustGenerateUserID()
	b1 := block(t, s, owner, at(100))
	b2 := block(t, s, owner, at(300))
	b3 := block(t, s, owner, at(200))

	// Page 1: most recent, limit 2 → [b2, b3].
	page1, err := s.GetBlocklistPage(ctx, owner, nil, 2)
	require.NoError(t, err)
	require.Equal(t, [][]byte{b2.Value, b3.Value}, blockedIDs(page1))

	// Page 2: resume after the last entry of page 1 (b3) → [b1].
	page2, err := s.GetBlocklistPage(ctx, owner, cursorOf(page1[len(page1)-1]), 2)
	require.NoError(t, err)
	require.Equal(t, [][]byte{b1.Value}, blockedIDs(page2))

	// Resuming after the final entry yields an empty page.
	page3, err := s.GetBlocklistPage(ctx, owner, cursorOf(page2[len(page2)-1]), 2)
	require.NoError(t, err)
	require.Empty(t, page3)
}

func testStore_GetBlocklistPage_Empty(t *testing.T, s blocklist.Store) {
	ctx := context.Background()

	got, err := s.GetBlocklistPage(ctx, model.MustGenerateUserID(), nil, 0)
	require.NoError(t, err)
	require.Empty(t, got)
}

// block blocks a freshly generated user on owner's behalf at the given time and
// returns the blocked user ID.
func block(t *testing.T, s blocklist.Store, owner *commonpb.UserId, blockedAt time.Time) *commonpb.UserId {
	blocked := model.MustGenerateUserID()
	added, err := s.Block(context.Background(), owner, blocked, blockedAt)
	require.NoError(t, err)
	require.True(t, added)
	return blocked
}

func cursorOf(e *blocklist.BlockedUser) *blocklist.Cursor {
	return &blocklist.Cursor{BlockedAt: e.BlockedAt, UserID: e.UserID}
}

// at returns a deterministic timestamp offset by the given number of seconds
// from a fixed epoch, in UTC.
func at(seconds int64) time.Time {
	return time.Unix(1_700_000_000+seconds, 0).UTC()
}

func blockedIDs(entries []*blocklist.BlockedUser) [][]byte {
	out := make([][]byte, len(entries))
	for i, e := range entries {
		out[i] = e.UserID.Value
	}
	return out
}
