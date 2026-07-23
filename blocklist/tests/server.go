package tests

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"
	"google.golang.org/grpc"

	blocklistpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/blocklist/v1"
	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"

	"github.com/code-payments/flipcash2-server/account"
	"github.com/code-payments/flipcash2-server/auth"
	"github.com/code-payments/flipcash2-server/blocklist"
	"github.com/code-payments/flipcash2-server/model"
	"github.com/code-payments/flipcash2-server/testutil"
)

// RunServerTests runs the shared blocklist.Server test suite against s, using
// accounts to authorize callers and to resolve whether a user being blocked
// exists. teardown is called between tests to reset the blocklist store.
func RunServerTests(t *testing.T, accounts account.Store, s blocklist.Store, teardown func()) {
	for _, tf := range []func(t *testing.T, accounts account.Store, s blocklist.Store){
		testServer_BlockUser_OK,
		testServer_BlockUser_Self,
		testServer_BlockUser_UserNotFound,
		testServer_BlockUser_Idempotent,
		testServer_UnblockUser_OK,
		testServer_UnblockUser_NotBlocked,
		testServer_IsBlocked,
		testServer_GetBlocklist_Empty,
		testServer_GetBlocklist_OrderAndContent,
		testServer_GetBlocklist_Paging,
	} {
		tf(t, accounts, s)
		teardown()
	}
}

type serverEnv struct {
	t        *testing.T
	ctx      context.Context
	client   blocklistpb.BlocklistClient
	accounts account.Store
	store    blocklist.Store

	userID *commonpb.UserId
	keys   model.KeyPair
}

func newServerEnv(t *testing.T, accounts account.Store, s blocklist.Store) *serverEnv {
	ctx := context.Background()
	log := zaptest.NewLogger(t)

	authz := account.NewAuthorizer(log, accounts, auth.NewKeyPairAuthenticator(log))

	userID := model.MustGenerateUserID()
	keys := model.MustGenerateKeyPair()
	// The caller is itself a real account. Binding its key both authorizes it and
	// makes it exist, so the self-block test rejects a self-block even though the
	// account exists rather than falling through to a missing-account path.
	_, err := accounts.Bind(ctx, userID, keys.Proto())
	require.NoError(t, err)

	server := blocklist.NewServer(log, authz, accounts, s)
	cc := testutil.RunGRPCServer(t, log, testutil.WithService(func(s *grpc.Server) {
		blocklistpb.RegisterBlocklistServer(s, server)
	}))

	return &serverEnv{
		t:        t,
		ctx:      ctx,
		client:   blocklistpb.NewBlocklistClient(cc),
		accounts: accounts,
		store:    s,
		userID:   userID,
		keys:     keys,
	}
}

// existingUser generates a user ID and binds a key for it in the account store,
// making it an existing (and thus blockable) account.
func (e *serverEnv) existingUser() *commonpb.UserId {
	userID := model.MustGenerateUserID()
	_, err := e.accounts.Bind(e.ctx, userID, model.MustGenerateKeyPair().Proto())
	require.NoError(e.t, err)
	return userID
}

func (e *serverEnv) blockUser(userID *commonpb.UserId) *blocklistpb.BlockUserResponse {
	req := &blocklistpb.BlockUserRequest{UserId: userID}
	require.NoError(e.t, e.keys.Auth(req, &req.Auth))
	resp, err := e.client.BlockUser(e.ctx, req)
	require.NoError(e.t, err)
	return resp
}

func (e *serverEnv) unblockUser(userID *commonpb.UserId) *blocklistpb.UnblockUserResponse {
	req := &blocklistpb.UnblockUserRequest{UserId: userID}
	require.NoError(e.t, e.keys.Auth(req, &req.Auth))
	resp, err := e.client.UnblockUser(e.ctx, req)
	require.NoError(e.t, err)
	return resp
}

func (e *serverEnv) isBlocked(userID *commonpb.UserId) *blocklistpb.IsBlockedResponse {
	req := &blocklistpb.IsBlockedRequest{UserId: userID}
	require.NoError(e.t, e.keys.Auth(req, &req.Auth))
	resp, err := e.client.IsBlocked(e.ctx, req)
	require.NoError(e.t, err)
	return resp
}

func (e *serverEnv) getBlocklist(opts *commonpb.QueryOptions) *blocklistpb.GetBlocklistResponse {
	req := &blocklistpb.GetBlocklistRequest{QueryOptions: opts}
	require.NoError(e.t, e.keys.Auth(req, &req.Auth))
	resp, err := e.client.GetBlocklist(e.ctx, req)
	require.NoError(e.t, err)
	return resp
}

// seedBlock records a block directly in the store (bypassing the server's
// wall-clock timestamp) so tests can control blocked_at and thus the ordering
// read back through the server.
func (e *serverEnv) seedBlock(blockedID *commonpb.UserId, blockedAt time.Time) {
	added, err := e.store.Block(e.ctx, e.userID, blockedID, blockedAt)
	require.NoError(e.t, err)
	require.True(e.t, added)
}

func testServer_BlockUser_OK(t *testing.T, accounts account.Store, s blocklist.Store) {
	e := newServerEnv(t, accounts, s)

	target := e.existingUser()

	resp := e.blockUser(target)
	require.Equal(t, blocklistpb.BlockUserResponse_OK, resp.Result)

	// The block is reflected by IsBlocked.
	require.True(t, e.isBlocked(target).IsBlocked)
}

func testServer_BlockUser_Self(t *testing.T, accounts account.Store, s blocklist.Store) {
	e := newServerEnv(t, accounts, s)

	resp := e.blockUser(e.userID)
	require.Equal(t, blocklistpb.BlockUserResponse_CANNOT_BLOCK_SELF, resp.Result)

	// Nothing was recorded.
	require.False(t, e.isBlocked(e.userID).IsBlocked)
}

func testServer_BlockUser_UserNotFound(t *testing.T, accounts account.Store, s blocklist.Store) {
	e := newServerEnv(t, accounts, s)

	// A user that has no account (never bound a key) cannot be blocked.
	stranger := model.MustGenerateUserID()
	resp := e.blockUser(stranger)
	require.Equal(t, blocklistpb.BlockUserResponse_USER_NOT_FOUND, resp.Result)

	require.False(t, e.isBlocked(stranger).IsBlocked)
}

func testServer_BlockUser_Idempotent(t *testing.T, accounts account.Store, s blocklist.Store) {
	e := newServerEnv(t, accounts, s)

	target := e.existingUser()

	require.Equal(t, blocklistpb.BlockUserResponse_OK, e.blockUser(target).Result)
	// Blocking an already-blocked user is still OK.
	require.Equal(t, blocklistpb.BlockUserResponse_OK, e.blockUser(target).Result)

	require.True(t, e.isBlocked(target).IsBlocked)

	// Only one entry exists for the pair.
	resp := e.getBlocklist(&commonpb.QueryOptions{})
	require.Len(t, resp.BlockedUsers, 1)
	require.Equal(t, target.Value, resp.BlockedUsers[0].UserId.Value)
}

func testServer_UnblockUser_OK(t *testing.T, accounts account.Store, s blocklist.Store) {
	e := newServerEnv(t, accounts, s)

	target := e.existingUser()
	require.Equal(t, blocklistpb.BlockUserResponse_OK, e.blockUser(target).Result)
	require.True(t, e.isBlocked(target).IsBlocked)

	resp := e.unblockUser(target)
	require.Equal(t, blocklistpb.UnblockUserResponse_OK, resp.Result)
	require.False(t, e.isBlocked(target).IsBlocked)
}

func testServer_UnblockUser_NotBlocked(t *testing.T, accounts account.Store, s blocklist.Store) {
	e := newServerEnv(t, accounts, s)

	// Unblocking a user who was never blocked is a no-op that still returns OK.
	resp := e.unblockUser(model.MustGenerateUserID())
	require.Equal(t, blocklistpb.UnblockUserResponse_OK, resp.Result)
}

func testServer_IsBlocked(t *testing.T, accounts account.Store, s blocklist.Store) {
	e := newServerEnv(t, accounts, s)

	target := e.existingUser()

	// Unblocked → false.
	resp := e.isBlocked(target)
	require.Equal(t, blocklistpb.IsBlockedResponse_OK, resp.Result)
	require.False(t, resp.IsBlocked)

	// Blocked → true.
	require.Equal(t, blocklistpb.BlockUserResponse_OK, e.blockUser(target).Result)
	resp = e.isBlocked(target)
	require.Equal(t, blocklistpb.IsBlockedResponse_OK, resp.Result)
	require.True(t, resp.IsBlocked)
}

func testServer_GetBlocklist_Empty(t *testing.T, accounts account.Store, s blocklist.Store) {
	e := newServerEnv(t, accounts, s)

	resp := e.getBlocklist(&commonpb.QueryOptions{})
	require.Equal(t, blocklistpb.GetBlocklistResponse_OK, resp.Result)
	require.Empty(t, resp.BlockedUsers)
	require.False(t, resp.HasMore)
	require.Nil(t, resp.PagingToken)
}

func testServer_GetBlocklist_OrderAndContent(t *testing.T, accounts account.Store, s blocklist.Store) {
	e := newServerEnv(t, accounts, s)

	// Seeded out of order; the list must return most-recently blocked first.
	older := e.existingUser()
	newer := e.existingUser()
	e.seedBlock(older, at(1))
	e.seedBlock(newer, at(2))

	resp := e.getBlocklist(&commonpb.QueryOptions{})
	require.Equal(t, blocklistpb.GetBlocklistResponse_OK, resp.Result)
	require.False(t, resp.HasMore)
	require.Len(t, resp.BlockedUsers, 2)

	require.Equal(t, newer.Value, resp.BlockedUsers[0].UserId.Value)
	require.Equal(t, older.Value, resp.BlockedUsers[1].UserId.Value)

	// The blocked-at timestamp is carried through.
	require.True(t, resp.BlockedUsers[0].BlockedAt.AsTime().Equal(at(2)))
	require.True(t, resp.BlockedUsers[1].BlockedAt.AsTime().Equal(at(1)))

	// A paging token is minted (it opaquely pins the cursor).
	require.NotNil(t, resp.PagingToken)
}

func testServer_GetBlocklist_Paging(t *testing.T, accounts account.Store, s blocklist.Store) {
	e := newServerEnv(t, accounts, s)

	const total = 5
	want := make([][]byte, total)
	for i := 0; i < total; i++ {
		// Increasing blocked_at, so DESC order is the reverse of insertion order.
		target := e.existingUser()
		e.seedBlock(target, at(int64(i+1)))
		want[total-1-i] = target.Value
	}

	var got [][]byte
	var token *commonpb.PagingToken
	for {
		resp := e.getBlocklist(&commonpb.QueryOptions{PageSize: 2, PagingToken: token})
		require.Equal(t, blocklistpb.GetBlocklistResponse_OK, resp.Result)
		require.LessOrEqual(t, len(resp.BlockedUsers), 2)
		for _, b := range resp.BlockedUsers {
			got = append(got, b.UserId.Value)
		}
		if !resp.HasMore {
			break
		}
		require.NotNil(t, resp.PagingToken)
		token = resp.PagingToken
	}

	require.Equal(t, want, got)
}
