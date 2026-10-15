package tests

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/mr-tron/base58"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"
	"google.golang.org/grpc"

	chatpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/chat/v1"
	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"
	ocp_balancepb "github.com/code-payments/ocp-protobuf-api/generated/go/balance/v1"

	"github.com/code-payments/flipcash2-server/account"
	accountmemory "github.com/code-payments/flipcash2-server/account/memory"
	"github.com/code-payments/flipcash2-server/balance"
	"github.com/code-payments/flipcash2-server/chat"
	"github.com/code-payments/flipcash2-server/model"
)

// RunHoldingsWorkerTests runs the shared chat.HoldingsWorker test suite
// against s; teardown resets it between tests.
func RunHoldingsWorkerTests(t *testing.T, s chat.Store, teardown func()) {
	for _, tf := range []func(t *testing.T, s chat.Store){
		testHoldingsWorker_Refresh,
		testHoldingsWorker_PagesRoster,
		testHoldingsWorker_FailureBacksOff,
		testHoldingsWorker_BelowMinimum,
		testHoldingsWorker_ClaimedElsewhere,
	} {
		tf(t, s)
		teardown()
	}
}

type holdingsWorkerEnv struct {
	t        *testing.T
	ctx      context.Context
	chats    chat.Store
	accounts account.Store
	ledger   *fakeMintLedger
	balances *balance.Client
}

func newHoldingsWorkerEnv(t *testing.T, s chat.Store) *holdingsWorkerEnv {
	log := zaptest.NewLogger(t)
	accounts := accountmemory.NewInMemory()
	ledger := &fakeMintLedger{quarks: make(map[string]map[string]uint64)}
	return &holdingsWorkerEnv{
		t:        t,
		ctx:      context.Background(),
		chats:    s,
		accounts: accounts,
		ledger:   ledger,
		balances: balance.NewClient(log, accounts, ledger),
	}
}

func (e *holdingsWorkerEnv) newWorker(opts ...chat.HoldingsWorkerOption) *chat.HoldingsWorker {
	return e.newWorkerOn(e.chats, opts...)
}

// newWorkerOn is newWorker reading and writing through chats, which may wrap
// the env's store.
func (e *holdingsWorkerEnv) newWorkerOn(chats chat.Store, opts ...chat.HoldingsWorkerOption) *chat.HoldingsWorker {
	return chat.NewHoldingsWorker(zaptest.NewLogger(e.t), chats, e.balances, opts...)
}

// fakeMintLedger is the OCP balance service behind the env's balance client,
// answering GetBalances from a ledger of each owner's quarks per mint and
// honouring the request's mint filter as OCP does: an owner it is asked about
// is in the response, with an entry for each requested mint they hold.
type fakeMintLedger struct {
	mu sync.Mutex

	// quarks is keyed by base58 owner, then base58 mint.
	quarks map[string]map[string]uint64
	// err, when set, fails every call.
	err error
	// calls counts GetBalances calls.
	calls int
}

func (f *fakeMintLedger) set(owner, mint *commonpb.PublicKey, quarks uint64) {
	f.mu.Lock()
	defer f.mu.Unlock()
	byMint, ok := f.quarks[base58.Encode(owner.Value)]
	if !ok {
		byMint = make(map[string]uint64)
		f.quarks[base58.Encode(owner.Value)] = byMint
	}
	byMint[base58.Encode(mint.Value)] = quarks
}

func (f *fakeMintLedger) setErr(err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.err = err
}

func (f *fakeMintLedger) callCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls
}

func (f *fakeMintLedger) GetBalances(_ context.Context, req *ocp_balancepb.GetBalancesRequest, _ ...grpc.CallOption) (*ocp_balancepb.GetBalancesResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	if f.err != nil {
		return nil, f.err
	}

	resp := &ocp_balancepb.GetBalancesResponse{
		Result:          ocp_balancepb.GetBalancesResponse_OK,
		BalancesByOwner: make(map[string]*ocp_balancepb.OwnerBalance),
	}
	for _, owner := range req.Owners {
		ownerKey := base58.Encode(owner.Value)
		ownerBalance := &ocp_balancepb.OwnerBalance{Owner: owner, BalancesByMint: make(map[string]*ocp_balancepb.MintBalance)}
		for _, mint := range req.Mints {
			mintKey := base58.Encode(mint.Value)
			if q := f.quarks[ownerKey][mintKey]; q > 0 {
				ownerBalance.BalancesByMint[mintKey] = &ocp_balancepb.MintBalance{Mint: mint, Quarks: q}
			}
		}
		resp.BalancesByOwner[ownerKey] = ownerBalance
	}
	return resp, nil
}

// holder registers a user with a bound key holding quarks of mint (none when
// zero), and returns them.
func (e *holdingsWorkerEnv) holder(mint *commonpb.PublicKey, quarks uint64) *commonpb.UserId {
	userID := model.MustGenerateUserID()
	keys := model.MustGenerateKeyPair()
	_, err := e.accounts.Bind(e.ctx, userID, keys.Proto())
	require.NoError(e.t, err)
	if quarks > 0 {
		e.ledger.set(keys.Proto(), mint, quarks)
	}
	return userID
}

// holders registers n users each holding quarks of mint, and returns them.
func (e *holdingsWorkerEnv) holders(mint *commonpb.PublicKey, n int, quarks uint64) []*commonpb.UserId {
	out := make([]*commonpb.UserId, n)
	for i := range out {
		out[i] = e.holder(mint, quarks)
	}
	return out
}

// createGatedGroup creates a public group gated on mint with the given
// members, through the store, which enqueues its refresh due at once.
func (e *holdingsWorkerEnv) createGatedGroup(mint *commonpb.PublicKey, members ...*commonpb.UserId) *commonpb.ChatId {
	c := &chat.Chat{
		ID:        chat.MustGenerateGroupChatID(),
		Type:      chatpb.ChatType_GROUP,
		Members:   members,
		Title:     "Gated",
		CreatorID: members[0],
		MinimumListenerBalance: &chat.MinimumBalance{
			Currency:     "usd",
			NativeAmount: 1,
			Mints:        []*commonpb.PublicKey{mint},
		},
		LastActivity: time.Now(),
	}
	require.NoError(e.t, e.chats.PutChat(e.ctx, c))
	return c.ID
}

func (e *holdingsWorkerEnv) holdingsOf(chatID *commonpb.ChatId) (chat.GroupHoldings, bool) {
	got, err := e.chats.GetGroupHoldings(e.ctx, []*commonpb.ChatId{chatID})
	require.NoError(e.t, err)
	h, ok := got[string(chatID.Value)]
	return h, ok
}

// queueEntry returns chatID's refresh entry as due by asOf, failing when it
// is not due by then.
func (e *holdingsWorkerEnv) queueEntry(chatID *commonpb.ChatId, asOf time.Time) chat.HoldingsRefresh {
	e.t.Helper()
	due, err := e.chats.GetDueHoldingsRefreshes(e.ctx, asOf, 0)
	require.NoError(e.t, err)
	for _, r := range due {
		if string(r.ChatID.Value) == string(chatID.Value) {
			return r
		}
	}
	e.t.Fatalf("group not due by %v", asOf)
	return chat.HoldingsRefresh{}
}

func (e *holdingsWorkerEnv) requireNotDue(asOf time.Time) {
	e.t.Helper()
	due, err := e.chats.GetDueHoldingsRefreshes(e.ctx, asOf, 0)
	require.NoError(e.t, err)
	require.Empty(e.t, due)
}

func testHoldingsWorker_Refresh(t *testing.T, s chat.Store) {
	e := newHoldingsWorkerEnv(t, s)
	mint := model.MustGenerateKeyPair().Proto()
	other := model.MustGenerateKeyPair().Proto()

	// Eleven holders, ten of 10 quarks and one of 50; beside them a member
	// holding none, one holding only another mint, and one with no bound key
	// at all, none of whom count.
	members := append(e.holders(mint, 10, 10),
		e.holder(mint, 50),
		e.holder(mint, 0),
		e.holder(other, 1_000),
		model.MustGenerateUserID(),
	)
	chatID := e.createGatedGroup(mint, members...)
	_, ok := e.holdingsOf(chatID)
	require.False(t, ok)

	const interval = time.Hour
	w := e.newWorker(chat.WithHoldingsRefreshInterval(interval))
	before := time.Now()
	processed, err := w.Process(e.ctx)
	require.NoError(t, err)
	require.Equal(t, 1, processed)

	h, ok := e.holdingsOf(chatID)
	require.True(t, ok)
	require.Equal(t, mint.Value, h.Mint.Value)
	require.EqualValues(t, 150, h.Quarks)
	require.EqualValues(t, 11, h.HolderCount)
	require.False(t, h.AsOf.Before(before.Truncate(time.Millisecond)))

	// Rescheduled a refresh interval out, with no failed attempts.
	e.requireNotDue(time.Now().Add(interval - time.Minute))
	require.Zero(t, e.queueEntry(chatID, time.Now().Add(interval+time.Minute)).Attempts)

	// Nothing else is due, so a tick does nothing.
	processed, err = w.Process(e.ctx)
	require.NoError(t, err)
	require.Zero(t, processed)

	// It is ranked for its currency.
	top, err := s.GetTopGroupsByHoldings(e.ctx, mint, 0)
	require.NoError(t, err)
	require.Len(t, top, 1)
	require.Equal(t, chatID.Value, top[0].ChatID.Value)

	// refreshAgain runs the next refresh at once. Snapshots are kept to the
	// millisecond, and one no newer than the stored one is dropped, so the
	// clock must move past the last refresh's first.
	refreshAgain := func() {
		t.Helper()
		time.Sleep(5 * time.Millisecond)
		require.NoError(t, s.RescheduleHoldingsRefresh(e.ctx, chatID, time.Now(), 0))
		processed, err := w.Process(e.ctx)
		require.NoError(t, err)
		require.Equal(t, 1, processed)
	}

	// A later refresh replaces the snapshot.
	_, _, err = s.AddGroupMembers(e.ctx, chatID, []*commonpb.UserId{e.holder(mint, 25)})
	require.NoError(t, err)
	refreshAgain()
	h, _ = e.holdingsOf(chatID)
	require.EqualValues(t, 175, h.Quarks)
	require.EqualValues(t, 12, h.HolderCount)

	// Once fewer than the minimum hold any, the snapshot that records it is
	// stored but not listed: the group leaves the ranking, and stays queued.
	for _, leaver := range members[:3] {
		_, _, err := s.RemoveGroupMember(e.ctx, chatID, leaver, false)
		require.NoError(t, err)
	}
	refreshAgain()
	h, ok = e.holdingsOf(chatID)
	require.True(t, ok)
	require.EqualValues(t, 145, h.Quarks)
	require.EqualValues(t, chat.MinGroupHoldingsHolders-1, h.HolderCount)
	require.False(t, h.Listed())
	top, err = s.GetTopGroupsByHoldings(e.ctx, mint, 0)
	require.NoError(t, err)
	require.Empty(t, top)
	require.Zero(t, e.queueEntry(chatID, time.Now().Add(interval+time.Minute)).Attempts)
}

func testHoldingsWorker_PagesRoster(t *testing.T, s chat.Store) {
	e := newHoldingsWorkerEnv(t, s)
	mint := model.MustGenerateKeyPair().Proto()

	var members []*commonpb.UserId
	for i := range 12 {
		members = append(members, e.holder(mint, uint64(i+1)))
	}
	chatID := e.createGatedGroup(mint, members...)

	processed, err := e.newWorker(chat.WithHoldingsRosterPageSize(5)).Process(e.ctx)
	require.NoError(t, err)
	require.Equal(t, 1, processed)

	h, ok := e.holdingsOf(chatID)
	require.True(t, ok)
	require.EqualValues(t, 78, h.Quarks)
	require.EqualValues(t, 12, h.HolderCount)
	require.Equal(t, 3, e.ledger.callCount())
}

func testHoldingsWorker_FailureBacksOff(t *testing.T, s chat.Store) {
	e := newHoldingsWorkerEnv(t, s)
	mint := model.MustGenerateKeyPair().Proto()
	chatID := e.createGatedGroup(mint, e.holders(mint, chat.MinGroupHoldingsHolders, 1)...)

	const base = 50 * time.Millisecond
	const interval = time.Hour
	w := e.newWorker(chat.WithHoldingsWorkerBackoff(base, interval), chat.WithHoldingsRefreshInterval(interval))

	// A failed refresh writes nothing and is retried under backoff, its
	// attempts counted.
	e.ledger.setErr(errors.New("ocp is down"))
	processed, err := w.Process(e.ctx)
	require.NoError(t, err)
	require.Equal(t, 1, processed)
	_, ok := e.holdingsOf(chatID)
	require.False(t, ok)
	e.requireNotDue(time.Now())
	require.EqualValues(t, 1, e.queueEntry(chatID, time.Now().Add(base)).Attempts)

	// Once due again, a refresh that succeeds clears them.
	e.ledger.setErr(nil)
	time.Sleep(base + 10*time.Millisecond)
	processed, err = w.Process(e.ctx)
	require.NoError(t, err)
	require.Equal(t, 1, processed)
	h, ok := e.holdingsOf(chatID)
	require.True(t, ok)
	require.EqualValues(t, chat.MinGroupHoldingsHolders, h.Quarks)
	require.Zero(t, e.queueEntry(chatID, time.Now().Add(interval+time.Minute)).Attempts)
}

func testHoldingsWorker_BelowMinimum(t *testing.T, s chat.Store) {
	e := newHoldingsWorkerEnv(t, s)
	mint := model.MustGenerateKeyPair().Proto()

	// One holder short of the minimum, however much they hold; and a group
	// whose members hold nothing at all.
	tooFew := e.createGatedGroup(mint, append(e.holders(mint, chat.MinGroupHoldingsHolders-1, 1_000_000), e.holder(mint, 0))...)
	nothing := e.createGatedGroup(mint, e.holders(mint, chat.MinGroupHoldingsHolders+2, 0)...)

	// Both are stored as found, neither is listed, and both succeed: asked
	// about again a refresh interval out, with no failed attempts.
	const interval = time.Hour
	processed, err := e.newWorker(chat.WithHoldingsRefreshInterval(interval)).Process(e.ctx)
	require.NoError(t, err)
	require.Equal(t, 2, processed)
	for chatID, want := range map[*commonpb.ChatId][2]uint64{
		tooFew:  {uint64(chat.MinGroupHoldingsHolders-1) * 1_000_000, chat.MinGroupHoldingsHolders - 1},
		nothing: {0, 0},
	} {
		h, ok := e.holdingsOf(chatID)
		require.True(t, ok)
		require.Equal(t, want[0], h.Quarks)
		require.Equal(t, want[1], h.HolderCount)
		require.False(t, h.Listed())
		require.Zero(t, e.queueEntry(chatID, time.Now().Add(interval+time.Minute)).Attempts)
	}
	top, err := s.GetTopGroupsByHoldings(e.ctx, mint, 0)
	require.NoError(t, err)
	require.Empty(t, top)
}

// claimedAfterListing is a chat.Store whose listing of due refreshes is
// followed, before it returns, by another worker claiming every group it
// lists: the race a worker's own claim loses.
type claimedAfterListing struct {
	chat.Store
}

func (c claimedAfterListing) GetDueHoldingsRefreshes(ctx context.Context, asOf time.Time, limit int) ([]chat.HoldingsRefresh, error) {
	due, err := c.Store.GetDueHoldingsRefreshes(ctx, asOf, limit)
	if err != nil {
		return nil, err
	}
	for _, r := range due {
		if _, err := c.Store.ClaimHoldingsRefresh(ctx, r.ChatID, time.Now(), time.Now().Add(time.Hour)); err != nil {
			return nil, err
		}
	}
	return due, nil
}

func testHoldingsWorker_ClaimedElsewhere(t *testing.T, s chat.Store) {
	e := newHoldingsWorkerEnv(t, s)
	mint := model.MustGenerateKeyPair().Proto()
	chatID := e.createGatedGroup(mint, e.holders(mint, chat.MinGroupHoldingsHolders, 10)...)

	// The listing names the group, but another worker claims it before this
	// one can: this worker takes nothing on, reads nothing and writes
	// nothing, and the other's claim stands.
	processed, err := e.newWorkerOn(claimedAfterListing{Store: s}).Process(e.ctx)
	require.NoError(t, err)
	require.Zero(t, processed)
	_, ok := e.holdingsOf(chatID)
	require.False(t, ok)
	require.Zero(t, e.ledger.callCount())
	e.requireNotDue(time.Now().Add(time.Hour - time.Minute))
}
