package tests

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"
	pollpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/poll/v1"

	ocp_common "github.com/code-payments/ocp-server/ocp/common"

	"github.com/code-payments/flipcash2-server/account"
	accountmemory "github.com/code-payments/flipcash2-server/account/memory"
	"github.com/code-payments/flipcash2-server/balance"
	"github.com/code-payments/flipcash2-server/chat"
	"github.com/code-payments/flipcash2-server/model"
	"github.com/code-payments/flipcash2-server/poll"
)

// RunWorkerTests runs the shared poll.Worker test suite against polls;
// teardown resets it between tests.
func RunWorkerTests(t *testing.T, polls poll.Store, teardown func()) {
	for _, tf := range []func(t *testing.T, polls poll.Store){
		testWorker_CountPoll,
		testWorker_CountPoll_Withheld,
		testWorker_BalancePoll,
		testWorker_BalancePoll_FewVoters,
		testWorker_OnlyDuePolls,
		testWorker_RetriesFailedTally,
	} {
		tf(t, polls)
		teardown()
	}
}

// testWorker_CountPoll pins a COUNT poll's tally: one per voter for each
// option they chose, and the poll closed off the queue.
func testWorker_CountPoll(t *testing.T, polls poll.Store) {
	e := newWorkerEnv(t, polls)

	members := e.newUsers(4)
	chatID := newGroupChatID()
	p := e.putDuePoll(chatID, pollpb.Mode_COUNT, 3, true)

	// The tally reads no membership: a vote was a member's when it was cast,
	// so it counts whether or not they have since left.
	e.vote(p, members[0], 0, 2)
	e.vote(p, members[1], 1)
	e.vote(p, members[2], 0)
	e.vote(p, members[3], 0, 1)

	processed, err := e.worker.Process(e.ctx)
	require.NoError(t, err)
	require.Equal(t, 1, processed)

	e.requireClosed(p, &pollpb.Results{Type: &pollpb.Results_Count{Count: &pollpb.CountResults{
		TotalVoters:  4,
		OptionVoters: []uint64{3, 2, 1},
	}}})

	// Closed, it is off the queue for good.
	processed, err = e.worker.Process(e.ctx)
	require.NoError(t, err)
	require.Zero(t, processed)
}

func testWorker_CountPoll_Withheld(t *testing.T, polls poll.Store) {
	e := newWorkerEnv(t, polls)

	members := e.newUsers(2)
	chatID := newGroupChatID()
	p := e.putDuePoll(chatID, pollpb.Mode_COUNT, 2, false)
	e.vote(p, members[0], 0)
	e.vote(p, members[1], 1)

	// Two voters' totals would say how each voted.
	_, err := e.worker.Process(e.ctx)
	require.NoError(t, err)
	e.requireClosed(p, withheld())

	// So would none at all.
	empty := e.putDuePoll(chatID, pollpb.Mode_COUNT, 2, false)
	_, err = e.worker.Process(e.ctx)
	require.NoError(t, err)
	e.requireClosed(empty, withheld())
}

// testWorker_BalancePoll pins a BALANCE poll's tally: each voter's holdings
// of the mint, in its quarks and valued in the poll's currency, added to every
// option they chose and to the total once, exactly.
func testWorker_BalancePoll(t *testing.T, polls poll.Store) {
	e := newWorkerEnv(t, polls)
	e.ocpBalance.setRate("cad", 2)

	members := e.newUsers(6)
	chatID := newGroupChatID()
	p := e.putDuePoll(chatID, pollpb.Mode_BALANCE, 2, true)

	// Valued in cad at 2: 247, 100, 50, 20, 10.5, and nothing.
	for i, holding := range []struct {
		quarks uint64
		usdf   float64
	}{{1000, 123.5}, {400, 50}, {200, 25}, {80, 10}, {42, 5.25}} {
		e.fund(members[i], holding.quarks, holding.usdf)
	}
	e.vote(p, members[0], 0)
	e.vote(p, members[1], 0, 1)
	e.vote(p, members[2], 1)
	e.vote(p, members[3], 1)
	e.vote(p, members[4], 0)
	e.vote(p, members[5], 1)

	_, err := e.worker.Process(e.ctx)
	require.NoError(t, err)

	// Recorded exactly: only the ranking is ever shown (see
	// poll.ResultsProto).
	amount := func(native float64, quarks uint64) *commonpb.CryptoPaymentAmount {
		return &commonpb.CryptoPaymentAmount{Currency: "cad", NativeAmount: native, Quarks: quarks, Mint: e.mint}
	}
	e.requireClosed(p, &pollpb.Results{Type: &pollpb.Results_Balance{Balance: &pollpb.BalanceResults{
		Total:          amount(427.5, 1722),
		OptionBalances: []*commonpb.CryptoPaymentAmount{amount(357.5, 1442), amount(170, 680)},
	}}})
}

// testWorker_BalancePoll_FewVoters pins that a BALANCE poll's totals are
// recorded however few voters hold any of the mint, since no reader is shown
// them.
func testWorker_BalancePoll_FewVoters(t *testing.T, polls poll.Store) {
	e := newWorkerEnv(t, polls)
	e.ocpBalance.setRate("cad", 1)

	members := e.newUsers(3)
	chatID := newGroupChatID()
	p := e.putDuePoll(chatID, pollpb.Mode_BALANCE, 2, false)
	e.fund(members[0], 25, 10)
	e.vote(p, members[0], 0)
	e.vote(p, members[1], 1)
	e.vote(p, members[2], 1)

	_, err := e.worker.Process(e.ctx)
	require.NoError(t, err)

	amount := func(native float64, quarks uint64) *commonpb.CryptoPaymentAmount {
		return &commonpb.CryptoPaymentAmount{Currency: "cad", NativeAmount: native, Quarks: quarks, Mint: e.mint}
	}
	e.requireClosed(p, &pollpb.Results{Type: &pollpb.Results_Balance{Balance: &pollpb.BalanceResults{
		Total:          amount(10, 25),
		OptionBalances: []*commonpb.CryptoPaymentAmount{amount(10, 25), amount(0, 0)},
	}}})

	// Nobody holding any of the mint is recorded too.
	empty := e.putDuePoll(chatID, pollpb.Mode_BALANCE, 2, false)
	_, err = e.worker.Process(e.ctx)
	require.NoError(t, err)
	e.requireClosed(empty, &pollpb.Results{Type: &pollpb.Results_Balance{Balance: &pollpb.BalanceResults{
		Total:          amount(0, 0),
		OptionBalances: []*commonpb.CryptoPaymentAmount{amount(0, 0), amount(0, 0)},
	}}})
}

// testWorker_OnlyDuePolls pins what the tally takes up: a live poll once
// TallyDelay has passed since its end, and nothing else.
func testWorker_OnlyDuePolls(t *testing.T, polls poll.Store) {
	e := newWorkerEnv(t, polls)
	chatID := newGroupChatID()

	// Ended, but not TallyDelay ago, and not ended at all.
	justEnded := e.putPoll(chatID, pollpb.Mode_COUNT, 2, false, true, time.Now().Add(-time.Second))
	open := e.putPoll(chatID, pollpb.Mode_COUNT, 2, false, true, time.Now().Add(time.Hour))
	// Unsent, which is never tallied.
	unsent := e.putPoll(chatID, pollpb.Mode_COUNT, 2, false, false, time.Now().Add(time.Hour))

	processed, err := e.worker.Process(e.ctx)
	require.NoError(t, err)
	require.Zero(t, processed)
	for _, p := range []*poll.Poll{justEnded, open, unsent} {
		stored, err := e.polls.GetPoll(e.ctx, p.ID)
		require.NoError(t, err)
		require.Equal(t, poll.StatusOpen, stored.Status)
	}
}

// testWorker_RetriesFailedTally pins that a failed tally leaves the poll open
// and comes due again under backoff, until it succeeds.
func testWorker_RetriesFailedTally(t *testing.T, polls poll.Store) {
	e := newWorkerEnv(t, polls)
	e.ocpBalance.setRate("cad", 1)

	members := e.newUsers(5)
	chatID := newGroupChatID()
	p := e.putDuePoll(chatID, pollpb.Mode_BALANCE, 2, false)
	for _, member := range members {
		e.fund(member, 10, 10)
		e.vote(p, member, 0)
	}

	e.ocpBalance.setErr(errors.New("ocp is down"))
	for attempt := uint32(1); attempt <= 3; attempt++ {
		processed, err := e.worker.Process(e.ctx)
		require.NoError(t, err)
		require.Equal(t, 1, processed)

		stored, err := e.polls.GetPoll(e.ctx, p.ID)
		require.NoError(t, err)
		require.Equal(t, poll.StatusOpen, stored.Status)

		due, err := e.polls.GetDueForTally(e.ctx, time.Now().Add(time.Hour), 10)
		require.NoError(t, err)
		require.Len(t, due, 1)
		require.Equal(t, attempt, due[0].Attempts)

		// Wait out the backoff.
		time.Sleep(10 * time.Millisecond)
	}

	e.ocpBalance.setErr(nil)
	processed, err := e.worker.Process(e.ctx)
	require.NoError(t, err)
	require.Equal(t, 1, processed)

	stored, err := e.polls.GetPoll(e.ctx, p.ID)
	require.NoError(t, err)
	require.Equal(t, poll.StatusClosed, stored.Status)
	require.NotNil(t, stored.Results.GetBalance())
}

type workerEnv struct {
	t   *testing.T
	ctx context.Context

	polls      poll.Store
	accounts   account.Store
	ocpBalance *fakeOcpBalance
	worker     *poll.Worker

	// mint is every BALANCE poll's mint.
	mint *commonpb.PublicKey
}

func newWorkerEnv(t *testing.T, polls poll.Store) *workerEnv {
	log := zaptest.NewLogger(t)
	accounts := accountmemory.NewInMemory()
	ocpBalance := &fakeOcpBalance{byOwner: make(map[string]fakeHolding)}
	balances := balance.NewClient(log, accounts, ocpBalance)

	return &workerEnv{
		t:          t,
		ctx:        context.Background(),
		polls:      polls,
		accounts:   accounts,
		ocpBalance: ocpBalance,
		// A failed tally comes due again in a few milliseconds.
		worker: poll.NewWorker(log, polls, balances, poll.WithWorkerBackoff(time.Millisecond, 5*time.Millisecond)),
		mint:   model.MustGenerateKeyPair().Proto(),
	}
}

func (e *workerEnv) newUsers(n int) []*commonpb.UserId {
	users := make([]*commonpb.UserId, n)
	for i := range users {
		users[i] = model.MustGenerateUserID()
	}
	return users
}

// newGroupChatID returns a group chat ID for a poll to belong to. The tally
// reads nothing of the chat, so no chat is stored.
func newGroupChatID() *commonpb.ChatId {
	return chat.MustGenerateGroupChatID()
}

// putDuePoll stores a live poll whose tally is due now.
func (e *workerEnv) putDuePoll(chatID *commonpb.ChatId, mode pollpb.Mode, options uint32, multipleChoice bool) *poll.Poll {
	return e.putPoll(chatID, mode, options, multipleChoice, true, time.Now().Add(-poll.TallyDelay-time.Second))
}

func (e *workerEnv) putPoll(chatID *commonpb.ChatId, mode pollpb.Mode, options uint32, multipleChoice, live bool, endsAt time.Time) *poll.Poll {
	text := &pollpb.PollText{Question: "Which?"}
	for range options {
		text.Options = append(text.Options, &pollpb.Option{Text: "option"})
	}
	definition := &pollpb.PollDefinition{
		OptionCount:           options,
		Mode:                  mode,
		AllowsMultipleChoices: multipleChoice,
		EndsAt:                timestamppb.New(endsAt),
		Text:                  &pollpb.PollDefinition_Plaintext{Plaintext: text},
	}
	if mode == pollpb.Mode_BALANCE {
		definition.BalanceParameters = &pollpb.BalanceParameters{Mint: e.mint, Currency: "cad"}
	}
	p := &poll.Poll{
		ID:         newPollID(),
		ChatID:     chatID,
		CreatorID:  model.MustGenerateUserID(),
		Definition: definition,
		CreatedAt:  endsAt.Add(-time.Hour),
		Live:       live,
		Status:     poll.StatusOpen,
	}
	require.NoError(e.t, e.polls.CreatePoll(e.ctx, p))
	return p
}

func (e *workerEnv) vote(p *poll.Poll, userID *commonpb.UserId, options ...uint32) {
	require.NoError(e.t, e.polls.SetVote(e.ctx, p.ID, userID, options, p.EndsAt().Add(-time.Minute)))
}

// fund binds a key for userID and gives them quarks of the env's mint, worth
// usdf USDF.
func (e *workerEnv) fund(userID *commonpb.UserId, quarks uint64, usdf float64) {
	key := model.MustGenerateKeyPair().Proto()
	_, err := e.accounts.Bind(e.ctx, userID, key)
	require.NoError(e.t, err)
	e.ocpBalance.setHolding(key, quarks, uint64(usdf*float64(ocp_common.CoreMintQuarksPerUnit)))
}

func (e *workerEnv) requireClosed(p *poll.Poll, want *pollpb.Results) {
	e.t.Helper()
	stored, err := e.polls.GetPoll(e.ctx, p.ID)
	require.NoError(e.t, err)
	require.Equal(e.t, poll.StatusClosed, stored.Status)
	require.True(e.t, proto.Equal(want, stored.Results), "results %v, want %v", stored.Results, want)
	require.NoError(e.t, stored.Results.Validate())
}

func withheld() *pollpb.Results {
	return &pollpb.Results{Type: &pollpb.Results_Withheld{Withheld: &pollpb.WithheldResults{}}}
}
