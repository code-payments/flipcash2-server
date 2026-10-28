package tests

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"
	"google.golang.org/protobuf/proto"

	paymentrequestpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/paymentrequest/v1"

	"github.com/code-payments/flipcash2-server/chat"
	"github.com/code-payments/flipcash2-server/paymentrequest"
)

// RunWorkerTests runs the shared paymentrequest.Worker test suite against s;
// teardown resets it between tests.
func RunWorkerTests(t *testing.T, s paymentrequest.Store, teardown func()) {
	for _, tf := range []func(t *testing.T, s paymentrequest.Store){
		testWorker_ExpiresDueRequest,
		testWorker_LeavesFinalRequests,
		testWorker_OnlyDueRequests,
		testWorker_NotYetEnded,
		testWorker_PaymentRacesExpiry,
		testWorker_RetriesFailedExpire,
	} {
		tf(t, s)
		teardown()
	}
}

// testWorker_ExpiresDueRequest pins what the worker stores: a request stored
// open and due is expired at its end, at its version, filed in the resolved
// section and off the queue, and a reader is shown the same state before and
// after.
func testWorker_ExpiresDueRequest(t *testing.T, s paymentrequest.Store) {
	ctx := context.Background()
	worker := newTestWorker(t, s)

	now := time.Now()
	chatID := chat.MustGenerateGroupChatID()
	r, debtors := newRequest(chatID, dueEndsAt(), 2)
	require.NoError(t, s.Create(ctx, r))
	_, _, _, err := s.PayShare(ctx, r.ID, debtors[0], intentPaid(r, debtors[0], now), now)
	require.NoError(t, err)

	before, err := s.Get(ctx, r.ID)
	require.NoError(t, err)
	require.EqualValues(t, 2, before.Version)
	requireIDs(t, []*paymentrequest.PaymentRequest{r}, walk(t, s, chatID, paymentrequest.SectionOpen, 10))

	processed, err := worker.Process(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, processed)

	got, err := s.Get(ctx, r.ID)
	require.NoError(t, err)
	require.Equal(t, paymentrequest.StatusExpired, got.Status)
	require.Equal(t, before.Version, got.Version)
	require.NotNil(t, got.ClosedAt)
	require.True(t, r.EndsAt().Equal(*got.ClosedAt))
	require.NotNil(t, got.Share(debtors[0]).Paid.GetIntent())
	require.True(t, proto.Equal(before.ToProto(now), got.ToProto(now)))

	require.Empty(t, walk(t, s, chatID, paymentrequest.SectionOpen, 10))
	requireIDs(t, []*paymentrequest.PaymentRequest{r}, walk(t, s, chatID, paymentrequest.SectionResolved, 10))

	due, err := s.GetDueForExpiry(ctx, time.Now().Add(time.Hour), 10)
	require.NoError(t, err)
	require.Empty(t, due)

	// Expired, it is off the queue for good.
	processed, err = worker.Process(ctx)
	require.NoError(t, err)
	require.Zero(t, processed)
}

// testWorker_LeavesFinalRequests pins that a request made final before its
// expiry came due, by its last share paid or by a close, is never touched:
// the write that made it final took it off the queue.
func testWorker_LeavesFinalRequests(t *testing.T, s paymentrequest.Store) {
	ctx := context.Background()
	worker := newTestWorker(t, s)

	now := time.Now()
	chatID := chat.MustGenerateGroupChatID()

	completed, debtors := newRequest(chatID, dueEndsAt(), 1)
	require.NoError(t, s.Create(ctx, completed))
	_, changed, _, err := s.PayShare(ctx, completed.ID, debtors[0], intentPaid(completed, debtors[0], now), now)
	require.NoError(t, err)
	require.True(t, changed)

	closed, _ := newRequest(chatID, dueEndsAt(), 1)
	require.NoError(t, s.Create(ctx, closed))
	_, changed, err = s.Close(ctx, closed.ID, closed.EndsAt().Add(-time.Second))
	require.NoError(t, err)
	require.True(t, changed)

	var before []*paymentrequest.PaymentRequest
	for _, r := range []*paymentrequest.PaymentRequest{completed, closed} {
		stored, err := s.Get(ctx, r.ID)
		require.NoError(t, err)
		before = append(before, stored)
	}

	processed, err := worker.Process(ctx)
	require.NoError(t, err)
	require.Zero(t, processed)

	for _, want := range before {
		got, err := s.Get(ctx, want.ID)
		require.NoError(t, err)
		requireStateEqual(t, want, got)
	}
	require.Equal(t, paymentrequest.StatusCompleted, before[0].Status)
	require.Equal(t, paymentrequest.StatusClosed, before[1].Status)
}

// testWorker_OnlyDueRequests pins what the worker takes up: a request stored
// open once ExpireDelay has passed since its end, and nothing sooner.
func testWorker_OnlyDueRequests(t *testing.T, s paymentrequest.Store) {
	ctx := context.Background()
	worker := newTestWorker(t, s)

	chatID := chat.MustGenerateGroupChatID()
	due, _ := newRequest(chatID, dueEndsAt(), 1)
	// Ended, but not ExpireDelay ago, and not ended at all.
	justEnded, _ := newRequest(chatID, time.Now().Add(-time.Second), 1)
	open, _ := newRequest(chatID, time.Now().Add(time.Hour), 1)
	for _, r := range []*paymentrequest.PaymentRequest{due, justEnded, open} {
		require.NoError(t, s.Create(ctx, r))
	}

	processed, err := worker.Process(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, processed)

	stored, err := s.Get(ctx, due.ID)
	require.NoError(t, err)
	require.Equal(t, paymentrequest.StatusExpired, stored.Status)

	for _, r := range []*paymentrequest.PaymentRequest{justEnded, open} {
		stored, err := s.Get(ctx, r.ID)
		require.NoError(t, err)
		require.Equal(t, paymentrequest.StatusOpen, stored.Status)
		require.Nil(t, stored.ClosedAt)
	}

	// Both are still queued, at their own due times.
	queued, err := s.GetDueForExpiry(ctx, time.Now().Add(2*time.Hour), 10)
	require.NoError(t, err)
	require.Len(t, queued, 2)
	require.Equal(t, justEnded.ID.Value, queued[0].ID.Value)
	require.Equal(t, open.ID.Value, queued[1].ID.Value)

	processed, err = worker.Process(ctx)
	require.NoError(t, err)
	require.Zero(t, processed)
}

// testWorker_NotYetEnded pins that a request the worker finds due but not yet
// ended by its own clock (only clock skew between servers gets there) is left
// open and put back on the queue at its due time.
func testWorker_NotYetEnded(t *testing.T, s paymentrequest.Store) {
	ctx := context.Background()
	worker := newTestWorker(t, s)

	r, _ := newRequest(chat.MustGenerateGroupChatID(), time.Now().Add(time.Hour), 1)
	require.NoError(t, s.Create(ctx, r))
	// Due now, an hour before its end.
	require.NoError(t, s.DelayExpiry(ctx, r.ID, time.Now().Add(-time.Second)))

	processed, err := worker.Process(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, processed)

	stored, err := s.Get(ctx, r.ID)
	require.NoError(t, err)
	require.Equal(t, paymentrequest.StatusOpen, stored.Status)
	require.EqualValues(t, 1, stored.Version)
	require.Nil(t, stored.ClosedAt)

	due, err := s.GetDueForExpiry(ctx, paymentrequest.ExpiryDueAt(r).Add(-time.Nanosecond), 10)
	require.NoError(t, err)
	require.Empty(t, due)

	due, err = s.GetDueForExpiry(ctx, paymentrequest.ExpiryDueAt(r), 10)
	require.NoError(t, err)
	require.Len(t, due, 1)
	require.True(t, paymentrequest.ExpiryDueAt(r).Equal(due[0].NextAttemptAt))
}

// testWorker_PaymentRacesExpiry pins that a payment of a request's last share
// racing its expiry is never lost, whichever lands first: the request ends
// completed (the payment landed first) or expired with the share paid (the
// expiry did), at one version past the read either way.
func testWorker_PaymentRacesExpiry(t *testing.T, s paymentrequest.Store) {
	ctx := context.Background()
	chatID := chat.MustGenerateGroupChatID()

	// The payment lands between the worker's read and its write: the request
	// completes, and the expiry is the no-op.
	paidFirst, debtors := newRequest(chatID, dueEndsAt(), 1)
	require.NoError(t, s.Create(ctx, paidFirst))
	paidAt := time.Now()
	// The hook runs on the worker's goroutine, so it asserts rather than
	// requires.
	hooked := &hookStore{Store: s, beforeExpire: func() {
		_, changed, _, err := s.PayShare(ctx, paidFirst.ID, debtors[0], intentPaid(paidFirst, debtors[0], paidAt), paidAt)
		assert.NoError(t, err)
		assert.True(t, changed)
	}}
	processed, err := newTestWorker(t, hooked).Process(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, processed)

	got, err := s.Get(ctx, paidFirst.ID)
	require.NoError(t, err)
	require.Equal(t, paymentrequest.StatusCompleted, got.Status)
	require.EqualValues(t, 2, got.Version)
	require.True(t, paidAt.Equal(*got.ClosedAt))
	require.NotNil(t, got.Share(debtors[0]).Paid.GetIntent())

	// The payment lands after the expiry is stored: it is recorded on the
	// expired request, which stays expired at its end.
	expiredFirst, debtors := newRequest(chatID, dueEndsAt(), 1)
	require.NoError(t, s.Create(ctx, expiredFirst))
	processed, err = newTestWorker(t, s).Process(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, processed)
	_, changed, _, err := s.PayShare(ctx, expiredFirst.ID, debtors[0], intentPaid(expiredFirst, debtors[0], time.Now()), time.Now())
	require.NoError(t, err)
	require.True(t, changed)

	got, err = s.Get(ctx, expiredFirst.ID)
	require.NoError(t, err)
	require.Equal(t, paymentrequest.StatusExpired, got.Status)
	require.EqualValues(t, 2, got.Version)
	require.True(t, expiredFirst.EndsAt().Equal(*got.ClosedAt))
	require.NotNil(t, got.Share(debtors[0]).Paid.GetIntent())

	// Truly concurrent: either outcome, never a lost share.
	const n = 4
	raced := make([]*paymentrequest.PaymentRequest, n)
	racedDebtors := make([]*paymentrequestpb.Share, n)
	for i := range raced {
		r, debtors := newRequest(chatID, dueEndsAt(), 1)
		require.NoError(t, s.Create(ctx, r))
		raced[i] = r
		racedDebtors[i] = r.Share(debtors[0])
	}
	var wg sync.WaitGroup
	wg.Go(func() {
		_, err := newTestWorker(t, s, paymentrequest.WithWorkerMaxConcurrency(n)).Process(ctx)
		assert.NoError(t, err)
	})
	for i, r := range raced {
		wg.Go(func() {
			_, changed, _, err := s.PayShare(ctx, r.ID, racedDebtors[i].UserId, intentPaid(r, racedDebtors[i].UserId, time.Now()), time.Now())
			assert.NoError(t, err)
			assert.True(t, changed)
		})
	}
	wg.Wait()
	require.False(t, t.Failed())

	for i, r := range raced {
		got, err := s.Get(ctx, r.ID)
		require.NoError(t, err)
		require.EqualValues(t, 2, got.Version)
		require.NotNil(t, got.Share(racedDebtors[i].UserId).Paid.GetIntent())
		require.NotNil(t, got.ClosedAt)
		switch got.Status {
		case paymentrequest.StatusCompleted:
		case paymentrequest.StatusExpired:
			require.True(t, r.EndsAt().Equal(*got.ClosedAt))
		default:
			require.Failf(t, "unexpected status", "request %d is %s", i, got.Status)
		}
	}

	// Every one of them is final, filed as resolved and off the queue.
	require.Empty(t, walk(t, s, chatID, paymentrequest.SectionOpen, 10))
	require.Len(t, walk(t, s, chatID, paymentrequest.SectionResolved, 10), n+2)
	due, err := s.GetDueForExpiry(ctx, time.Now().Add(time.Hour), 10)
	require.NoError(t, err)
	require.Empty(t, due)
}

// testWorker_RetriesFailedExpire pins that a failed expiry leaves the request
// stored open and comes due again under backoff, until it succeeds.
func testWorker_RetriesFailedExpire(t *testing.T, s paymentrequest.Store) {
	ctx := context.Background()

	r, _ := newRequest(chat.MustGenerateGroupChatID(), dueEndsAt(), 1)
	require.NoError(t, s.Create(ctx, r))

	const failures = 3
	faulty := &hookStore{Store: s, failExpires: failures}
	worker := newTestWorker(t, faulty)
	for attempt := uint32(1); attempt <= failures; attempt++ {
		processed, err := worker.Process(ctx)
		require.NoError(t, err)
		require.Equal(t, 1, processed)

		stored, err := s.Get(ctx, r.ID)
		require.NoError(t, err)
		require.Equal(t, paymentrequest.StatusOpen, stored.Status)

		due, err := s.GetDueForExpiry(ctx, time.Now().Add(time.Hour), 10)
		require.NoError(t, err)
		require.Len(t, due, 1)
		require.Equal(t, attempt, due[0].Attempts)

		// Wait out the backoff.
		time.Sleep(10 * time.Millisecond)
	}

	processed, err := worker.Process(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, processed)

	stored, err := s.Get(ctx, r.ID)
	require.NoError(t, err)
	require.Equal(t, paymentrequest.StatusExpired, stored.Status)
	require.EqualValues(t, 1, stored.Version)
	require.True(t, r.EndsAt().Equal(*stored.ClosedAt))
}

// newTestWorker returns a worker over s whose failed attempts come due again
// in a few milliseconds.
func newTestWorker(t *testing.T, s paymentrequest.Store, opts ...paymentrequest.WorkerOption) *paymentrequest.Worker {
	opts = append([]paymentrequest.WorkerOption{paymentrequest.WithWorkerBackoff(time.Millisecond, 5*time.Millisecond)}, opts...)
	return paymentrequest.NewWorker(zaptest.NewLogger(t), s, opts...)
}

// dueEndsAt is an end whose expiry is due now.
func dueEndsAt() time.Time {
	return time.Now().Add(-paymentrequest.ExpireDelay - time.Second)
}

// hookStore wraps a store to fail its next failExpires calls to Expire, and
// to run beforeExpire (once) just before the next call it lets through.
type hookStore struct {
	paymentrequest.Store

	mu           sync.Mutex
	failExpires  int
	beforeExpire func()
}

func (h *hookStore) Expire(ctx context.Context, id *paymentrequestpb.PaymentRequestId) (bool, error) {
	h.mu.Lock()
	fail := h.failExpires > 0
	if fail {
		h.failExpires--
	}
	hook := h.beforeExpire
	if !fail {
		h.beforeExpire = nil
	}
	h.mu.Unlock()

	if fail {
		return false, errors.New("injected expire failure")
	}
	if hook != nil {
		hook()
	}
	return h.Store.Expire(ctx, id)
}
