package tests

import (
	"bytes"
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"
	paymentrequestpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/paymentrequest/v1"

	"github.com/code-payments/flipcash2-server/chat"
	"github.com/code-payments/flipcash2-server/model"
	"github.com/code-payments/flipcash2-server/paymentrequest"
)

// RunStoreTests runs the shared paymentrequest.Store test suite against s;
// teardown resets it between tests.
func RunStoreTests(t *testing.T, s paymentrequest.Store, teardown func()) {
	for _, tf := range []func(t *testing.T, s paymentrequest.Store){
		testStore_CreateAndGet,
		testStore_Create_Exists,
		testStore_PaymentIntentIDs,
		testStore_GetChatPage_Open,
		testStore_GetChatPage_Resolved,
		testStore_PayShare_Manual,
		testStore_PayShare_NotOpen,
		testStore_PayShare_Intent,
		testStore_PayShare_Completes,
		testStore_PayShare_ReplacesManual,
		testStore_PayShare_Concurrent,
		testStore_Close,
		testStore_ExpiryQueue,
		testStore_ClaimForExpiry,
		testStore_DelayExpiry,
		testStore_ExpiryQueueStats,
		testStore_Expire,
	} {
		tf(t, s)
		teardown()
	}
}

func testStore_CreateAndGet(t *testing.T, s paymentrequest.Store) {
	ctx := context.Background()

	r, _ := newRequest(chat.MustGenerateGroupChatID(), time.Now().Add(time.Hour), 2)
	require.NoError(t, s.Create(ctx, r))

	got, err := s.Get(ctx, r.ID)
	require.NoError(t, err)
	requireRequestEqual(t, r, got)
	require.Equal(t, paymentrequest.StatusOpen, got.Status)
	require.EqualValues(t, 1, got.Version)
	require.Nil(t, got.ClosedAt)

	// The creator's share is paid from creation, as given; the debtors' are
	// unpaid.
	own := got.CreatorShare()
	require.NotNil(t, own)
	require.NotNil(t, own.Paid.GetCreator())
	require.True(t, r.CreatedAt.Equal(own.Paid.PaidAt.AsTime()))
	require.False(t, got.AllSharesPaid())
	for _, share := range got.Shares() {
		if !proto.Equal(share.UserId, got.Creator) {
			require.Nil(t, share.Paid)
		}
	}

	// Whatever state the request is given in, it is stored open at version
	// one.
	odd, _ := newRequest(chat.MustGenerateGroupChatID(), time.Now().Add(time.Hour), 1)
	odd.Status = paymentrequest.StatusClosed
	odd.Version = 7
	closedAt := time.Now()
	odd.ClosedAt = &closedAt
	require.NoError(t, s.Create(ctx, odd))
	got, err = s.Get(ctx, odd.ID)
	require.NoError(t, err)
	require.Equal(t, paymentrequest.StatusOpen, got.Status)
	require.EqualValues(t, 1, got.Version)
	require.Nil(t, got.ClosedAt)

	// A request that is not one a store may hold is refused.
	invalid, _ := newRequest(chat.MustGenerateGroupChatID(), time.Now().Add(time.Hour), 1)
	invalid.Split = nil
	require.Error(t, s.Create(ctx, invalid))
	_, err = s.Get(ctx, invalid.ID)
	require.ErrorIs(t, err, paymentrequest.ErrNotFound)

	_, err = s.Get(ctx, newID())
	require.ErrorIs(t, err, paymentrequest.ErrNotFound)
}

func testStore_Create_Exists(t *testing.T, s paymentrequest.Store) {
	ctx := context.Background()

	r, _ := newRequest(chat.MustGenerateGroupChatID(), time.Now().Add(time.Hour), 1)
	require.NoError(t, s.Create(ctx, r))

	// A second request under the same ID is refused, and the first stands.
	other, _ := newRequest(chat.MustGenerateGroupChatID(), time.Now().Add(2*time.Hour), 3)
	other.ID = r.ID
	require.ErrorIs(t, s.Create(ctx, other), paymentrequest.ErrExists)

	got, err := s.Get(ctx, r.ID)
	require.NoError(t, err)
	requireRequestEqual(t, r, got)
}

func testStore_GetChatPage_Open(t *testing.T, s paymentrequest.Store) {
	ctx := context.Background()

	now := time.Now().Truncate(time.Millisecond)
	chatID := chat.MustGenerateGroupChatID()

	// Two share an end, so their order is by ID; one has already reached its
	// end but is still stored open.
	tied := now.Add(2 * time.Hour)
	ended, _ := newRequest(chatID, now.Add(-time.Minute), 1)
	soon, _ := newRequest(chatID, now.Add(time.Hour), 1)
	tieA, _ := newRequest(chatID, tied, 1)
	tieB, _ := newRequest(chatID, tied, 1)
	late, _ := newRequest(chatID, now.Add(3*time.Hour), 1)
	for _, r := range []*paymentrequest.PaymentRequest{late, tieB, soon, ended, tieA} {
		require.NoError(t, s.Create(ctx, r))
	}
	if bytes.Compare(tieA.ID.Value, tieB.ID.Value) > 0 {
		tieA, tieB = tieB, tieA
	}

	// Requests in another chat, and resolved ones in this chat, stay out.
	elsewhere, _ := newRequest(chat.MustGenerateGroupChatID(), now.Add(time.Hour), 1)
	require.NoError(t, s.Create(ctx, elsewhere))
	closed, _ := newRequest(chatID, now.Add(time.Hour), 1)
	require.NoError(t, s.Create(ctx, closed))
	_, changed, err := s.Close(ctx, closed.ID, now)
	require.NoError(t, err)
	require.True(t, changed)

	want := []*paymentrequest.PaymentRequest{ended, soon, tieA, tieB, late}
	requireIDs(t, want, walk(t, s, chatID, paymentrequest.SectionOpen, 2))

	whole, err := s.GetChatPage(ctx, chatID, paymentrequest.SectionOpen, nil, 10)
	require.NoError(t, err)
	requireIDs(t, want, whole)

	// The request past its end is still stored open, and read as expired.
	require.Equal(t, paymentrequest.StatusOpen, whole[0].Status)
	require.Equal(t, paymentrequest.StatusExpired, whole[0].StatusAt(now))

	// A cursor resumes strictly after the request it names, even within a tie.
	page, err := s.GetChatPage(ctx, chatID, paymentrequest.SectionOpen, paymentrequest.CursorOf(tieA, paymentrequest.SectionOpen), 10)
	require.NoError(t, err)
	requireIDs(t, []*paymentrequest.PaymentRequest{tieB, late}, page)

	empty, err := s.GetChatPage(ctx, chat.MustGenerateGroupChatID(), paymentrequest.SectionOpen, nil, 10)
	require.NoError(t, err)
	require.Empty(t, empty)
}

func testStore_GetChatPage_Resolved(t *testing.T, s paymentrequest.Store) {
	ctx := context.Background()

	now := time.Now().Truncate(time.Millisecond)
	chatID := chat.MustGenerateGroupChatID()

	// One of each final status, resolved at distinct times, and two closed at
	// the same moment, ordered by ID.
	completed, debtors := newRequest(chatID, now.Add(time.Hour), 1)
	expired, _ := newRequest(chatID, now.Add(-time.Hour), 1)
	closedEarly, _ := newRequest(chatID, now.Add(time.Hour), 1)
	tieA, _ := newRequest(chatID, now.Add(time.Hour), 1)
	tieB, _ := newRequest(chatID, now.Add(time.Hour), 1)
	open, _ := newRequest(chatID, now.Add(time.Hour), 1)
	for _, r := range []*paymentrequest.PaymentRequest{completed, expired, closedEarly, tieA, tieB, open} {
		require.NoError(t, s.Create(ctx, r))
	}
	if bytes.Compare(tieA.ID.Value, tieB.ID.Value) > 0 {
		tieA, tieB = tieB, tieA
	}

	// expired closes at its end (now - 1h), closedEarly at now - 30m, the tie
	// at now - 10m, and completed at now.
	changed, err := s.Expire(ctx, expired.ID)
	require.NoError(t, err)
	require.True(t, changed)
	_, _, err = s.Close(ctx, closedEarly.ID, now.Add(-30*time.Minute))
	require.NoError(t, err)
	for _, r := range []*paymentrequest.PaymentRequest{tieA, tieB} {
		_, _, err = s.Close(ctx, r.ID, now.Add(-10*time.Minute))
		require.NoError(t, err)
	}
	_, _, _, err = s.PayShare(ctx, completed.ID, debtors[0], manualPaid(now), now)
	require.NoError(t, err)

	elsewhere, _ := newRequest(chat.MustGenerateGroupChatID(), now.Add(time.Hour), 1)
	require.NoError(t, s.Create(ctx, elsewhere))
	_, _, err = s.Close(ctx, elsewhere.ID, now)
	require.NoError(t, err)

	want := []*paymentrequest.PaymentRequest{completed, tieB, tieA, closedEarly, expired}
	got := walk(t, s, chatID, paymentrequest.SectionResolved, 2)
	requireIDs(t, want, got)
	require.Equal(t, paymentrequest.StatusCompleted, got[0].Status)
	require.Equal(t, paymentrequest.StatusClosed, got[1].Status)
	require.Equal(t, paymentrequest.StatusExpired, got[4].Status)

	// A cursor resumes strictly after the request it names, even within a tie.
	stored, err := s.Get(ctx, tieB.ID)
	require.NoError(t, err)
	page, err := s.GetChatPage(ctx, chatID, paymentrequest.SectionResolved, paymentrequest.CursorOf(stored, paymentrequest.SectionResolved), 10)
	require.NoError(t, err)
	requireIDs(t, []*paymentrequest.PaymentRequest{tieA, closedEarly, expired}, page)

	// A request moves from the open section to the resolved one when it is
	// made final.
	requireIDs(t, []*paymentrequest.PaymentRequest{open}, walk(t, s, chatID, paymentrequest.SectionOpen, 10))
	_, _, err = s.Close(ctx, open.ID, now.Add(time.Minute))
	require.NoError(t, err)
	require.Empty(t, walk(t, s, chatID, paymentrequest.SectionOpen, 10))
	requireIDs(t, append([]*paymentrequest.PaymentRequest{open}, want...), walk(t, s, chatID, paymentrequest.SectionResolved, 10))
}

func testStore_PayShare_Manual(t *testing.T, s paymentrequest.Store) {
	ctx := context.Background()

	now := time.Now().Truncate(time.Millisecond)
	r, debtors := newRequest(chat.MustGenerateGroupChatID(), now.Add(time.Hour), 2)
	require.NoError(t, s.Create(ctx, r))

	paid := manualPaid(now)
	got, changed, _, err := s.PayShare(ctx, r.ID, debtors[0], paid, now)
	require.NoError(t, err)
	require.True(t, changed)
	require.EqualValues(t, 2, got.Version)
	require.Equal(t, paymentrequest.StatusOpen, got.Status)
	require.True(t, proto.Equal(paid, got.Share(debtors[0]).Paid))
	require.Nil(t, got.Share(debtors[1]).Paid)

	stored, err := s.Get(ctx, r.ID)
	require.NoError(t, err)
	require.EqualValues(t, 2, stored.Version)
	require.True(t, proto.Equal(paid, stored.Share(debtors[0]).Paid))

	// Marking a share already marked is the no-op.
	got, changed, _, err = s.PayShare(ctx, r.ID, debtors[0], manualPaid(now.Add(time.Minute)), now.Add(time.Minute))
	require.NoError(t, err)
	require.False(t, changed)
	require.EqualValues(t, 2, got.Version)
	require.True(t, proto.Equal(paid, got.Share(debtors[0]).Paid))

	// A payment replaces the hand mark, at the next version, and then stands:
	// neither a hand mark nor another payment replaces it.
	payment := intentPaid(r, debtors[0], now.Add(-time.Second))
	got, changed, _, err = s.PayShare(ctx, r.ID, debtors[0], payment, now.Add(time.Minute))
	require.NoError(t, err)
	require.True(t, changed)
	require.EqualValues(t, 3, got.Version)
	require.Equal(t, paymentrequest.StatusOpen, got.Status)
	require.True(t, proto.Equal(payment, got.Share(debtors[0]).Paid))

	for _, again := range []*paymentrequestpb.PaidShare{manualPaid(now.Add(2 * time.Minute)), intentPaid(r, debtors[0], now.Add(2*time.Minute))} {
		got, changed, _, err = s.PayShare(ctx, r.ID, debtors[0], again, now.Add(2*time.Minute))
		require.NoError(t, err)
		require.False(t, changed)
		require.EqualValues(t, 3, got.Version)
		require.True(t, proto.Equal(payment, got.Share(debtors[0]).Paid))
	}

	stored, err = s.Get(ctx, r.ID)
	require.NoError(t, err)
	require.EqualValues(t, 3, stored.Version)
	require.True(t, proto.Equal(payment, stored.Share(debtors[0]).Paid))

	// The creator's own share is paid from creation, so marking it is the
	// no-op too, and no intent can pay it.
	_, changed, _, err = s.PayShare(ctx, r.ID, r.Creator, manualPaid(now), now)
	require.NoError(t, err)
	require.False(t, changed)
	_, _, _, err = s.PayShare(ctx, r.ID, r.Creator, otherIntentPaid(now), now)
	require.ErrorIs(t, err, paymentrequest.ErrWrongIntent)

	// A payment by any intent but the share's is refused, whether the share
	// is paid or not: one no share's, and one another share's.
	for _, wrong := range []struct {
		debtor *commonpb.UserId
		paid   *paymentrequestpb.PaidShare
	}{
		{debtors[0], otherIntentPaid(now)},
		{debtors[1], otherIntentPaid(now)},
		{debtors[1], intentPaid(r, debtors[0], now)},
		{debtors[0], intentPaid(r, debtors[1], now)},
	} {
		_, _, _, err = s.PayShare(ctx, r.ID, wrong.debtor, wrong.paid, now)
		require.ErrorIs(t, err, paymentrequest.ErrWrongIntent)
	}

	_, _, _, err = s.PayShare(ctx, r.ID, model.MustGenerateUserID(), manualPaid(now), now)
	require.ErrorIs(t, err, paymentrequest.ErrNotAShare)

	_, _, _, err = s.PayShare(ctx, newID(), debtors[1], manualPaid(now), now)
	require.ErrorIs(t, err, paymentrequest.ErrNotFound)

	// A share is never paid as the creator's, nor without saying how or when.
	for _, invalid := range []*paymentrequestpb.PaidShare{
		{PaidAt: timestamppb.New(now), How: &paymentrequestpb.PaidShare_Creator_{Creator: &paymentrequestpb.PaidShare_Creator{}}},
		{PaidAt: timestamppb.New(now)},
		{How: &paymentrequestpb.PaidShare_Manual_{Manual: &paymentrequestpb.PaidShare_Manual{}}},
		{PaidAt: timestamppb.New(now), How: &paymentrequestpb.PaidShare_Intent{Intent: &commonpb.IntentId{}}},
		nil,
	} {
		_, _, _, err = s.PayShare(ctx, r.ID, debtors[1], invalid, now)
		require.ErrorIs(t, err, paymentrequest.ErrInvalidPaidShare)
	}

	stored, err = s.Get(ctx, r.ID)
	require.NoError(t, err)
	require.EqualValues(t, 3, stored.Version)
	require.Nil(t, stored.Share(debtors[1]).Paid)
}

func testStore_PayShare_NotOpen(t *testing.T, s paymentrequest.Store) {
	ctx := context.Background()

	now := time.Now().Truncate(time.Millisecond)

	// A closed request takes no share marked by hand.
	closed, debtors := newRequest(chat.MustGenerateGroupChatID(), now.Add(time.Hour), 2)
	require.NoError(t, s.Create(ctx, closed))
	_, _, _, err := s.PayShare(ctx, closed.ID, debtors[0], manualPaid(now), now)
	require.NoError(t, err)
	_, _, err = s.Close(ctx, closed.ID, now)
	require.NoError(t, err)

	_, _, _, err = s.PayShare(ctx, closed.ID, debtors[1], manualPaid(now), now)
	require.ErrorIs(t, err, paymentrequest.ErrNotOpen)

	// A share already paid is still the no-op, whatever the status.
	got, changed, _, err := s.PayShare(ctx, closed.ID, debtors[0], manualPaid(now), now)
	require.NoError(t, err)
	require.False(t, changed)
	require.Equal(t, paymentrequest.StatusClosed, got.Status)
	require.EqualValues(t, 3, got.Version)

	// Nor does a request stored open that has reached its end, from its end
	// on.
	ending, debtors := newRequest(chat.MustGenerateGroupChatID(), now.Add(time.Hour), 1)
	require.NoError(t, s.Create(ctx, ending))
	_, _, _, err = s.PayShare(ctx, ending.ID, debtors[0], manualPaid(ending.EndsAt()), ending.EndsAt())
	require.ErrorIs(t, err, paymentrequest.ErrNotOpen)

	stored, err := s.Get(ctx, ending.ID)
	require.NoError(t, err)
	require.EqualValues(t, 1, stored.Version)
	require.Equal(t, paymentrequest.StatusOpen, stored.Status)
	require.Nil(t, stored.Share(debtors[0]).Paid)

	// Just before its end it does.
	before := ending.EndsAt().Add(-time.Millisecond)
	_, changed, _, err = s.PayShare(ctx, ending.ID, debtors[0], manualPaid(before), before)
	require.NoError(t, err)
	require.True(t, changed)
}

func testStore_PayShare_Intent(t *testing.T, s paymentrequest.Store) {
	ctx := context.Background()

	now := time.Now().Truncate(time.Millisecond)

	// A payment landing after a close is still recorded, and the request stays
	// closed as it was, even when it paid the last share.
	closed, debtors := newRequest(chat.MustGenerateGroupChatID(), now.Add(time.Hour), 1)
	require.NoError(t, s.Create(ctx, closed))
	_, _, err := s.Close(ctx, closed.ID, now)
	require.NoError(t, err)

	paid := intentPaid(closed, debtors[0], now.Add(time.Second))
	got, changed, completed, err := s.PayShare(ctx, closed.ID, debtors[0], paid, now.Add(time.Second))
	require.NoError(t, err)
	require.True(t, changed)
	require.False(t, completed)
	require.Equal(t, paymentrequest.StatusClosed, got.Status)
	require.EqualValues(t, 3, got.Version)
	require.True(t, now.Equal(*got.ClosedAt))
	require.True(t, proto.Equal(paid, got.Share(debtors[0]).Paid))
	require.True(t, got.AllSharesPaid())

	// One landing after the request has reached its end but before the expiry
	// is stored completes it, at the payment's time.
	ended, debtors := newRequest(chat.MustGenerateGroupChatID(), now.Add(-time.Minute), 1)
	require.NoError(t, s.Create(ctx, ended))
	paid = intentPaid(ended, debtors[0], now)
	got, changed, completed, err = s.PayShare(ctx, ended.ID, debtors[0], paid, now)
	require.NoError(t, err)
	require.True(t, changed)
	require.True(t, completed)
	require.Equal(t, paymentrequest.StatusCompleted, got.Status)
	require.EqualValues(t, 2, got.Version)
	require.True(t, now.Equal(*got.ClosedAt))

	// And one landing after the expiry is stored is recorded on the expired
	// request, which stays expired at its end.
	expired, debtors := newRequest(chat.MustGenerateGroupChatID(), now.Add(-time.Minute), 1)
	require.NoError(t, s.Create(ctx, expired))
	changed, err = s.Expire(ctx, expired.ID)
	require.NoError(t, err)
	require.True(t, changed)

	got, changed, completed, err = s.PayShare(ctx, expired.ID, debtors[0], intentPaid(expired, debtors[0], now), now)
	require.NoError(t, err)
	require.True(t, changed)
	require.False(t, completed)
	require.Equal(t, paymentrequest.StatusExpired, got.Status)
	require.EqualValues(t, 2, got.Version)
	require.True(t, expired.EndsAt().Equal(*got.ClosedAt))

	stored, err := s.Get(ctx, expired.ID)
	require.NoError(t, err)
	require.Equal(t, paymentrequest.StatusExpired, stored.Status)
	require.NotNil(t, stored.Share(debtors[0]).Paid.GetIntent())
}

func testStore_PayShare_Completes(t *testing.T, s paymentrequest.Store) {
	ctx := context.Background()

	now := time.Now().Truncate(time.Millisecond)
	r, debtors := newRequest(chat.MustGenerateGroupChatID(), now.Add(time.Hour), 2)
	require.NoError(t, s.Create(ctx, r))

	got, _, completed, err := s.PayShare(ctx, r.ID, debtors[0], intentPaid(r, debtors[0], now), now)
	require.NoError(t, err)
	require.False(t, completed)
	require.Equal(t, paymentrequest.StatusOpen, got.Status)
	require.Nil(t, got.ClosedAt)

	// The last share completes the request, closed when it was paid, and takes
	// it off the expiry queue.
	paidAt := now.Add(time.Minute)
	got, changed, completed, err := s.PayShare(ctx, r.ID, debtors[1], manualPaid(paidAt), now.Add(2*time.Minute))
	require.NoError(t, err)
	require.True(t, changed)
	require.True(t, completed)
	require.Equal(t, paymentrequest.StatusCompleted, got.Status)
	require.EqualValues(t, 3, got.Version)
	require.True(t, paidAt.Equal(*got.ClosedAt))
	require.True(t, got.AllSharesPaid())

	stored, err := s.Get(ctx, r.ID)
	require.NoError(t, err)
	require.Equal(t, paymentrequest.StatusCompleted, stored.Status)
	require.EqualValues(t, 3, stored.Version)
	require.True(t, paidAt.Equal(*stored.ClosedAt))

	due, err := s.GetDueForExpiry(ctx, paymentrequest.ExpiryDueAt(r).Add(time.Hour), 10)
	require.NoError(t, err)
	require.Empty(t, due)

	// A completed request cannot be closed or expired.
	_, changed, err = s.Close(ctx, r.ID, now)
	require.NoError(t, err)
	require.False(t, changed)
	changed, err = s.Expire(ctx, r.ID)
	require.NoError(t, err)
	require.False(t, changed)

	stored, err = s.Get(ctx, r.ID)
	require.NoError(t, err)
	require.Equal(t, paymentrequest.StatusCompleted, stored.Status)
	require.EqualValues(t, 3, stored.Version)
}

// A payment that lands after its share was marked by hand replaces the mark,
// whatever the request's status since, and moves nothing but the share and
// the version: not the status, not ClosedAt, not the request's place in its
// listing.
func testStore_PayShare_ReplacesManual(t *testing.T, s paymentrequest.Store) {
	ctx := context.Background()

	now := time.Now().Truncate(time.Millisecond)

	// The hand mark completed the request.
	chatID := chat.MustGenerateGroupChatID()
	completed, debtors := newRequest(chatID, now.Add(time.Hour), 1)
	require.NoError(t, s.Create(ctx, completed))
	marked, changed, completes, err := s.PayShare(ctx, completed.ID, debtors[0], manualPaid(now), now)
	require.NoError(t, err)
	require.True(t, changed)
	require.True(t, completes)
	require.Equal(t, paymentrequest.StatusCompleted, marked.Status)

	// The request returned is COMPLETED either way, so only completed tells
	// that this payment completed nothing.
	payment := intentPaid(completed, debtors[0], now.Add(-time.Second))
	got, changed, completes, err := s.PayShare(ctx, completed.ID, debtors[0], payment, now.Add(time.Minute))
	require.NoError(t, err)
	require.True(t, changed)
	require.False(t, completes)
	require.Equal(t, paymentrequest.StatusCompleted, got.Status)
	require.Equal(t, marked.Version+1, got.Version)
	require.True(t, marked.ClosedAt.Equal(*got.ClosedAt))
	require.True(t, proto.Equal(payment, got.Share(debtors[0]).Paid))

	stored, err := s.Get(ctx, completed.ID)
	require.NoError(t, err)
	require.Equal(t, paymentrequest.StatusCompleted, stored.Status)
	require.Equal(t, marked.Version+1, stored.Version)
	require.True(t, marked.ClosedAt.Equal(*stored.ClosedAt))
	require.True(t, proto.Equal(payment, stored.Share(debtors[0]).Paid))
	requireIDs(t, []*paymentrequest.PaymentRequest{completed}, walk(t, s, chatID, paymentrequest.SectionResolved, 10))

	// The hand mark was made, then the request closed with a share unpaid.
	closed, debtors := newRequest(chat.MustGenerateGroupChatID(), now.Add(time.Hour), 2)
	require.NoError(t, s.Create(ctx, closed))
	_, _, _, err = s.PayShare(ctx, closed.ID, debtors[0], manualPaid(now), now)
	require.NoError(t, err)
	closedNow, changed, err := s.Close(ctx, closed.ID, now)
	require.NoError(t, err)
	require.True(t, changed)

	payment = intentPaid(closed, debtors[0], now.Add(-time.Second))
	got, changed, completes, err = s.PayShare(ctx, closed.ID, debtors[0], payment, now.Add(time.Minute))
	require.NoError(t, err)
	require.True(t, changed)
	require.False(t, completes)
	require.Equal(t, paymentrequest.StatusClosed, got.Status)
	require.Equal(t, closedNow.Version+1, got.Version)
	require.True(t, closedNow.ClosedAt.Equal(*got.ClosedAt))
	require.True(t, proto.Equal(payment, got.Share(debtors[0]).Paid))
	require.Nil(t, got.Share(debtors[1]).Paid)
}

func testStore_PayShare_Concurrent(t *testing.T, s paymentrequest.Store) {
	ctx := context.Background()

	now := time.Now().Truncate(time.Millisecond)
	const n = 8
	r, debtors := newRequest(chat.MustGenerateGroupChatID(), now.Add(time.Hour), n)
	require.NoError(t, s.Create(ctx, r))

	// Every share paid at once, each twice: each lands exactly once, and each
	// landing advances the version by exactly one.
	var (
		wg          sync.WaitGroup
		mu          sync.Mutex
		changes     int
		completions int
	)
	for _, debtor := range debtors {
		for range 2 {
			wg.Add(1)
			go func() {
				defer wg.Done()
				_, changed, completed, err := s.PayShare(ctx, r.ID, debtor, intentPaid(r, debtor, now), now)
				if !assert.NoError(t, err) {
					return
				}
				assert.False(t, completed && !changed)
				mu.Lock()
				if changed {
					changes++
				}
				if completed {
					completions++
				}
				mu.Unlock()
			}()
		}
	}
	wg.Wait()
	require.Equal(t, n, changes)
	require.Equal(t, 1, completions)

	stored, err := s.Get(ctx, r.ID)
	require.NoError(t, err)
	require.EqualValues(t, 1+n, stored.Version)
	require.Equal(t, paymentrequest.StatusCompleted, stored.Status)
	require.True(t, stored.AllSharesPaid())
}

func testStore_Close(t *testing.T, s paymentrequest.Store) {
	ctx := context.Background()

	now := time.Now().Truncate(time.Millisecond)
	r, debtors := newRequest(chat.MustGenerateGroupChatID(), now.Add(time.Hour), 1)
	require.NoError(t, s.Create(ctx, r))

	got, changed, err := s.Close(ctx, r.ID, now)
	require.NoError(t, err)
	require.True(t, changed)
	require.Equal(t, paymentrequest.StatusClosed, got.Status)
	require.EqualValues(t, 2, got.Version)
	require.True(t, now.Equal(*got.ClosedAt))
	require.Nil(t, got.Share(debtors[0]).Paid)

	stored, err := s.Get(ctx, r.ID)
	require.NoError(t, err)
	requireStateEqual(t, got, stored)

	// Closing again is the no-op, and keeps the first close.
	got, changed, err = s.Close(ctx, r.ID, now.Add(time.Minute))
	require.NoError(t, err)
	require.False(t, changed)
	require.EqualValues(t, 2, got.Version)
	require.True(t, now.Equal(*got.ClosedAt))

	// A request stored open that has reached its end is expired, not closed.
	ending, _ := newRequest(chat.MustGenerateGroupChatID(), now.Add(time.Hour), 1)
	require.NoError(t, s.Create(ctx, ending))
	got, changed, err = s.Close(ctx, ending.ID, ending.EndsAt())
	require.NoError(t, err)
	require.False(t, changed)
	require.Equal(t, paymentrequest.StatusOpen, got.Status)
	require.Equal(t, paymentrequest.StatusExpired, got.StatusAt(ending.EndsAt()))
	require.EqualValues(t, 1, got.Version)
	require.Nil(t, got.ClosedAt)

	stored, err = s.Get(ctx, ending.ID)
	require.NoError(t, err)
	require.Equal(t, paymentrequest.StatusOpen, stored.Status)
	require.EqualValues(t, 1, stored.Version)

	_, _, err = s.Close(ctx, newID(), now)
	require.ErrorIs(t, err, paymentrequest.ErrNotFound)
}

func testStore_ExpiryQueue(t *testing.T, s paymentrequest.Store) {
	ctx := context.Background()

	now := time.Now().Truncate(time.Millisecond)
	chatID := chat.MustGenerateGroupChatID()
	early, _ := newRequest(chatID, now.Add(time.Hour), 1)
	late, _ := newRequest(chatID, now.Add(2*time.Hour), 1)
	closed, _ := newRequest(chatID, now.Add(time.Hour), 1)
	completed, debtors := newRequest(chatID, now.Add(time.Hour), 1)
	expired, _ := newRequest(chatID, now.Add(time.Hour), 1)
	for _, r := range []*paymentrequest.PaymentRequest{late, early, closed, completed, expired} {
		require.NoError(t, s.Create(ctx, r))
	}

	// A request comes due ExpireDelay after it ends, and not before.
	due, err := s.GetDueForExpiry(ctx, paymentrequest.ExpiryDueAt(early).Add(-time.Nanosecond), 10)
	require.NoError(t, err)
	require.Empty(t, due)

	due, err = s.GetDueForExpiry(ctx, paymentrequest.ExpiryDueAt(early), 10)
	require.NoError(t, err)
	require.Len(t, due, 4)
	for _, task := range due {
		require.True(t, paymentrequest.ExpiryDueAt(early).Equal(task.NextAttemptAt))
		require.Zero(t, task.Attempts)
	}

	// Every write that makes a request final dequeues it.
	_, _, err = s.Close(ctx, closed.ID, now)
	require.NoError(t, err)
	_, _, _, err = s.PayShare(ctx, completed.ID, debtors[0], intentPaid(completed, debtors[0], now), now)
	require.NoError(t, err)
	_, err = s.Expire(ctx, expired.ID)
	require.NoError(t, err)

	// Soonest first.
	due, err = s.GetDueForExpiry(ctx, now.Add(3*time.Hour), 10)
	require.NoError(t, err)
	require.Len(t, due, 2)
	require.Equal(t, early.ID.Value, due[0].ID.Value)
	require.Equal(t, late.ID.Value, due[1].ID.Value)
	require.True(t, paymentrequest.ExpiryDueAt(late).Equal(due[1].NextAttemptAt))

	due, err = s.GetDueForExpiry(ctx, now.Add(3*time.Hour), 1)
	require.NoError(t, err)
	require.Len(t, due, 1)
	require.Equal(t, early.ID.Value, due[0].ID.Value)
}

func testStore_ClaimForExpiry(t *testing.T, s paymentrequest.Store) {
	ctx := context.Background()

	now := time.Now().Truncate(time.Millisecond)
	r, _ := newRequest(chat.MustGenerateGroupChatID(), now.Add(time.Hour), 1)
	require.NoError(t, s.Create(ctx, r))

	dueAt := paymentrequest.ExpiryDueAt(r)
	lease := dueAt.Add(2 * time.Minute)

	// A request that is not due yet cannot be claimed.
	claimed, err := s.ClaimForExpiry(ctx, r.ID, dueAt.Add(-time.Nanosecond), lease)
	require.NoError(t, err)
	require.False(t, claimed)

	// One claim as of the due time wins; a second as of the same time loses,
	// since the first pushed the request out to its lease.
	claimed, err = s.ClaimForExpiry(ctx, r.ID, dueAt, lease)
	require.NoError(t, err)
	require.True(t, claimed)
	claimed, err = s.ClaimForExpiry(ctx, r.ID, dueAt, lease)
	require.NoError(t, err)
	require.False(t, claimed)

	// The claimed request is out of the due set until its lease runs out, and
	// a claim counts no attempt.
	due, err := s.GetDueForExpiry(ctx, lease.Add(-time.Nanosecond), 10)
	require.NoError(t, err)
	require.Empty(t, due)

	due, err = s.GetDueForExpiry(ctx, lease, 10)
	require.NoError(t, err)
	require.Len(t, due, 1)
	require.True(t, lease.Equal(due[0].NextAttemptAt))
	require.Zero(t, due[0].Attempts)

	claimed, err = s.ClaimForExpiry(ctx, r.ID, lease, lease.Add(time.Minute))
	require.NoError(t, err)
	require.True(t, claimed)

	// Neither a request that doesn't exist nor a final one can be claimed.
	claimed, err = s.ClaimForExpiry(ctx, newID(), now.Add(24*time.Hour), lease)
	require.NoError(t, err)
	require.False(t, claimed)

	closed, _ := newRequest(chat.MustGenerateGroupChatID(), now.Add(time.Hour), 1)
	require.NoError(t, s.Create(ctx, closed))
	_, _, err = s.Close(ctx, closed.ID, now)
	require.NoError(t, err)
	claimed, err = s.ClaimForExpiry(ctx, closed.ID, now.Add(24*time.Hour), lease)
	require.NoError(t, err)
	require.False(t, claimed)
}

func testStore_DelayExpiry(t *testing.T, s paymentrequest.Store) {
	ctx := context.Background()

	now := time.Now().Truncate(time.Millisecond)
	r, _ := newRequest(chat.MustGenerateGroupChatID(), now.Add(time.Hour), 1)
	require.NoError(t, s.Create(ctx, r))

	retryAt := paymentrequest.ExpiryDueAt(r).Add(10 * time.Minute)
	require.NoError(t, s.DelayExpiry(ctx, r.ID, retryAt))
	require.NoError(t, s.DelayExpiry(ctx, r.ID, retryAt))

	due, err := s.GetDueForExpiry(ctx, retryAt.Add(-time.Nanosecond), 10)
	require.NoError(t, err)
	require.Empty(t, due)

	due, err = s.GetDueForExpiry(ctx, retryAt, 10)
	require.NoError(t, err)
	require.Len(t, due, 1)
	require.True(t, retryAt.Equal(due[0].NextAttemptAt))
	require.EqualValues(t, 2, due[0].Attempts)

	// Delaying a request that is not queued is a no-op: it stays off the
	// queue.
	closed, _ := newRequest(chat.MustGenerateGroupChatID(), now.Add(time.Hour), 1)
	require.NoError(t, s.Create(ctx, closed))
	_, _, err = s.Close(ctx, closed.ID, now)
	require.NoError(t, err)
	require.NoError(t, s.DelayExpiry(ctx, closed.ID, retryAt))
	require.NoError(t, s.DelayExpiry(ctx, newID(), retryAt))

	due, err = s.GetDueForExpiry(ctx, retryAt, 10)
	require.NoError(t, err)
	require.Len(t, due, 1)
	require.Equal(t, r.ID.Value, due[0].ID.Value)
}

func testStore_ExpiryQueueStats(t *testing.T, s paymentrequest.Store) {
	ctx := context.Background()

	now := time.Now().Truncate(time.Millisecond)

	stats, err := s.GetExpiryQueueStats(ctx, now)
	require.NoError(t, err)
	require.Zero(t, stats.DueDepth)
	require.True(t, stats.OldestDueAt.IsZero())

	chatID := chat.MustGenerateGroupChatID()
	sooner, _ := newRequest(chatID, now.Add(time.Hour), 1)
	middle, _ := newRequest(chatID, now.Add(90*time.Minute), 1)
	later, _ := newRequest(chatID, now.Add(2*time.Hour), 1)
	for _, r := range []*paymentrequest.PaymentRequest{sooner, middle, later} {
		require.NoError(t, s.Create(ctx, r))
	}

	// Queued requests not yet due are not backlog.
	stats, err = s.GetExpiryQueueStats(ctx, now)
	require.NoError(t, err)
	require.Zero(t, stats.DueDepth)
	require.True(t, stats.OldestDueAt.IsZero())

	// A request counts from its due time on.
	stats, err = s.GetExpiryQueueStats(ctx, paymentrequest.ExpiryDueAt(sooner))
	require.NoError(t, err)
	require.EqualValues(t, 1, stats.DueDepth)
	require.True(t, paymentrequest.ExpiryDueAt(sooner).Equal(stats.OldestDueAt))

	asOf := now.Add(3 * time.Hour)
	stats, err = s.GetExpiryQueueStats(ctx, asOf)
	require.NoError(t, err)
	require.EqualValues(t, 3, stats.DueDepth)
	require.True(t, paymentrequest.ExpiryDueAt(sooner).Equal(stats.OldestDueAt))

	// A claimed request is not due until its lease ends.
	claimed, err := s.ClaimForExpiry(ctx, sooner.ID, asOf, asOf.Add(time.Minute))
	require.NoError(t, err)
	require.True(t, claimed)

	stats, err = s.GetExpiryQueueStats(ctx, asOf)
	require.NoError(t, err)
	require.EqualValues(t, 2, stats.DueDepth)
	require.True(t, paymentrequest.ExpiryDueAt(middle).Equal(stats.OldestDueAt))

	// Expiring a request dequeues it.
	_, err = s.Expire(ctx, middle.ID)
	require.NoError(t, err)

	stats, err = s.GetExpiryQueueStats(ctx, asOf)
	require.NoError(t, err)
	require.EqualValues(t, 1, stats.DueDepth)
	require.True(t, paymentrequest.ExpiryDueAt(later).Equal(stats.OldestDueAt))
}

func testStore_Expire(t *testing.T, s paymentrequest.Store) {
	ctx := context.Background()

	now := time.Now().Truncate(time.Millisecond)
	r, debtors := newRequest(chat.MustGenerateGroupChatID(), now.Add(-time.Minute), 2)
	require.NoError(t, s.Create(ctx, r))
	_, _, _, err := s.PayShare(ctx, r.ID, debtors[0], intentPaid(r, debtors[0], now), now)
	require.NoError(t, err)

	// Expiry closes the request at its end and moves no version: a reader saw
	// it expired at this version already.
	before, err := s.Get(ctx, r.ID)
	require.NoError(t, err)
	require.Equal(t, paymentrequest.StatusExpired, before.StatusAt(now))

	changed, err := s.Expire(ctx, r.ID)
	require.NoError(t, err)
	require.True(t, changed)

	got, err := s.Get(ctx, r.ID)
	require.NoError(t, err)
	require.Equal(t, paymentrequest.StatusExpired, got.Status)
	require.Equal(t, before.Version, got.Version)
	require.True(t, r.EndsAt().Equal(*got.ClosedAt))
	require.True(t, proto.Equal(before.ToProto(now), got.ToProto(now)))

	due, err := s.GetDueForExpiry(ctx, now.Add(time.Hour), 10)
	require.NoError(t, err)
	require.Empty(t, due)

	// Expiring again is the no-op.
	changed, err = s.Expire(ctx, r.ID)
	require.NoError(t, err)
	require.False(t, changed)

	// So is expiring a request made final otherwise, which keeps its status.
	closed, _ := newRequest(chat.MustGenerateGroupChatID(), now.Add(-time.Minute), 1)
	require.NoError(t, s.Create(ctx, closed))
	_, _, err = s.Close(ctx, closed.ID, closed.EndsAt().Add(-time.Second))
	require.NoError(t, err)
	changed, err = s.Expire(ctx, closed.ID)
	require.NoError(t, err)
	require.False(t, changed)
	got, err = s.Get(ctx, closed.ID)
	require.NoError(t, err)
	require.Equal(t, paymentrequest.StatusClosed, got.Status)
	require.EqualValues(t, 2, got.Version)

	_, err = s.Expire(ctx, newID())
	require.ErrorIs(t, err, paymentrequest.ErrNotFound)
}

// testStore_PaymentIntentIDs pins that a request's payment intent IDs are
// stored as created and never change: not on a payment, a close or an expiry,
// and not in a listing. A request with an ID missing, extra or the wrong
// width is refused.
func testStore_PaymentIntentIDs(t *testing.T, s paymentrequest.Store) {
	ctx := context.Background()

	now := time.Now().Truncate(time.Millisecond)
	chatID := chat.MustGenerateGroupChatID()
	r, debtors := newRequest(chatID, now.Add(-time.Minute), 2)
	require.Len(t, r.PaymentIntentIDs, 2)
	require.Nil(t, r.PaymentIntentID(r.Creator))
	require.NoError(t, s.Create(ctx, r))

	requireIntentIDs := func(got *paymentrequest.PaymentRequest) {
		t.Helper()
		require.Len(t, got.PaymentIntentIDs, len(r.PaymentIntentIDs))
		for _, debtor := range debtors {
			require.Equal(t, r.PaymentIntentID(debtor), got.PaymentIntentID(debtor))
		}
		require.Nil(t, got.PaymentIntentID(got.Creator))
	}

	got, err := s.Get(ctx, r.ID)
	require.NoError(t, err)
	requireIntentIDs(got)

	got, _, _, err = s.PayShare(ctx, r.ID, debtors[0], intentPaid(r, debtors[0], now), now)
	require.NoError(t, err)
	requireIntentIDs(got)

	page, err := s.GetChatPage(ctx, chatID, paymentrequest.SectionOpen, nil, 10)
	require.NoError(t, err)
	require.Len(t, page, 1)
	requireIntentIDs(page[0])

	changed, err := s.Expire(ctx, r.ID)
	require.NoError(t, err)
	require.True(t, changed)
	got, _, _, err = s.PayShare(ctx, r.ID, debtors[1], intentPaid(r, debtors[1], now), now)
	require.NoError(t, err)
	requireIntentIDs(got)

	got, err = s.Get(ctx, r.ID)
	require.NoError(t, err)
	requireIntentIDs(got)

	closable, closableDebtors := newRequest(chatID, now.Add(time.Hour), 1)
	require.NoError(t, s.Create(ctx, closable))
	got, _, err = s.Close(ctx, closable.ID, now)
	require.NoError(t, err)
	require.Equal(t, closable.PaymentIntentID(closableDebtors[0]), got.PaymentIntentID(closableDebtors[0]))
	got, err = s.Get(ctx, closable.ID)
	require.NoError(t, err)
	require.Equal(t, closable.PaymentIntentID(closableDebtors[0]), got.PaymentIntentID(closableDebtors[0]))

	// Every debtor has one of the right width, and nobody else has one.
	for name, mutate := range map[string]func(*paymentrequest.PaymentRequest, []*commonpb.UserId){
		"missing": func(r *paymentrequest.PaymentRequest, debtors []*commonpb.UserId) {
			delete(r.PaymentIntentIDs, string(debtors[0].Value))
		},
		"short": func(r *paymentrequest.PaymentRequest, debtors []*commonpb.UserId) {
			r.PaymentIntentIDs[string(debtors[0].Value)] = make([]byte, paymentrequest.PaymentIntentIDSize-1)
		},
		"the creator's": func(r *paymentrequest.PaymentRequest, _ []*commonpb.UserId) {
			r.PaymentIntentIDs[string(r.Creator.Value)] = make([]byte, paymentrequest.PaymentIntentIDSize)
		},
		"a stranger's": func(r *paymentrequest.PaymentRequest, _ []*commonpb.UserId) {
			r.PaymentIntentIDs[string(model.MustGenerateUserID().Value)] = make([]byte, paymentrequest.PaymentIntentIDSize)
		},
	} {
		bad, badDebtors := newRequest(chatID, now.Add(time.Hour), 1)
		mutate(bad, badDebtors)
		require.Error(t, s.Create(ctx, bad), name)
		_, err := s.Get(ctx, bad.ID)
		require.ErrorIs(t, err, paymentrequest.ErrNotFound, name)
	}
}

// newRequest returns a split in chatID, ending at endsAt, from a fresh creator
// to debtors fresh debtors, with the creator's own share listed first and so
// paid from creation. It returns the debtors in the order their shares are
// listed.
func newRequest(chatID *commonpb.ChatId, endsAt time.Time, debtors int) (*paymentrequest.PaymentRequest, []*commonpb.UserId) {
	creator := model.MustGenerateUserID()
	shares := []*paymentrequestpb.Share{{UserId: creator, NativeAmount: 2.5}}
	debtorIDs := make([]*commonpb.UserId, debtors)
	for i := range debtorIDs {
		debtorIDs[i] = model.MustGenerateUserID()
		shares = append(shares, &paymentrequestpb.Share{UserId: debtorIDs[i], NativeAmount: 2.5 + float64(i)})
	}

	r, err := paymentrequest.NewSplitRequest(
		paymentrequest.MustDeriveID(creator, &paymentrequestpb.IdempotencyKey{Value: model.MustGenerateUserID().Value}),
		creator,
		chatID,
		&paymentrequestpb.Split{
			Text: &paymentrequestpb.Split_Plaintext{Plaintext: &paymentrequestpb.PaymentRequestText{
				Title:       "Dinner",
				Description: "Friday night",
			}},
			Currency: "usd",
			EndsAt:   timestamppb.New(endsAt),
			Shares:   shares,
		},
		time.Now().Truncate(time.Millisecond),
	)
	if err != nil {
		panic(err)
	}
	return r, debtorIDs
}

func newID() *paymentrequestpb.PaymentRequestId {
	return paymentrequest.MustDeriveID(model.MustGenerateUserID(), &paymentrequestpb.IdempotencyKey{Value: model.MustGenerateUserID().Value})
}

func manualPaid(at time.Time) *paymentrequestpb.PaidShare {
	return &paymentrequestpb.PaidShare{
		PaidAt: timestamppb.New(at),
		How:    &paymentrequestpb.PaidShare_Manual_{Manual: &paymentrequestpb.PaidShare_Manual{}},
	}
}

// intentPaid is debtor's share of r paid at at by the intent that share's
// payment must use (see paymentrequest.ValidatePaymentIntent).
func intentPaid(r *paymentrequest.PaymentRequest, debtor *commonpb.UserId, at time.Time) *paymentrequestpb.PaidShare {
	return paidByIntent(r.PaymentIntentID(debtor), at)
}

// otherIntentPaid is a share paid at at by an intent that is no share's.
func otherIntentPaid(at time.Time) *paymentrequestpb.PaidShare {
	return paidByIntent(model.MustGenerateKeyPair().Public(), at)
}

func paidByIntent(intentID []byte, at time.Time) *paymentrequestpb.PaidShare {
	return &paymentrequestpb.PaidShare{
		PaidAt: timestamppb.New(at),
		How:    &paymentrequestpb.PaidShare_Intent{Intent: &commonpb.IntentId{Value: append([]byte(nil), intentID...)}},
	}
}

// walk reads a whole section of chatID in pages of pageSize, checking each
// page is at most that long.
func walk(t *testing.T, s paymentrequest.Store, chatID *commonpb.ChatId, section paymentrequest.Section, pageSize int) []*paymentrequest.PaymentRequest {
	t.Helper()

	var (
		all    []*paymentrequest.PaymentRequest
		cursor *paymentrequest.PageCursor
	)
	for {
		page, err := s.GetChatPage(context.Background(), chatID, section, cursor, pageSize)
		require.NoError(t, err)
		require.LessOrEqual(t, len(page), pageSize)
		if len(page) == 0 {
			return all
		}
		for _, r := range page {
			require.Equal(t, chatID.Value, r.ChatID.Value)
		}
		all = append(all, page...)
		cursor = paymentrequest.CursorOf(page[len(page)-1], section)
	}
}

// requireIDs requires that got is exactly the requests in want, in order.
func requireIDs(t *testing.T, want, got []*paymentrequest.PaymentRequest) {
	t.Helper()

	ids := func(rs []*paymentrequest.PaymentRequest) [][]byte {
		out := make([][]byte, len(rs))
		for i, r := range rs {
			out[i] = r.ID.Value
		}
		return out
	}
	require.Equal(t, ids(want), ids(got))
}

// requireRequestEqual requires that got is the request want describes.
func requireRequestEqual(t *testing.T, want, got *paymentrequest.PaymentRequest) {
	t.Helper()
	require.NotNil(t, got)
	require.Equal(t, want.ID.Value, got.ID.Value)
	require.Equal(t, want.Creator.Value, got.Creator.Value)
	require.Equal(t, want.ChatID.Value, got.ChatID.Value)
	require.True(t, want.CreatedAt.Equal(got.CreatedAt), "created_at %v != %v", want.CreatedAt, got.CreatedAt)
	require.True(t, proto.Equal(want.Split, got.Split))
	requireStateEqual(t, want, got)
}

// requireStateEqual requires that got's stored state is want's.
func requireStateEqual(t *testing.T, want, got *paymentrequest.PaymentRequest) {
	t.Helper()
	require.Equal(t, want.Status, got.Status)
	require.Equal(t, want.Version, got.Version)
	if want.ClosedAt == nil {
		require.Nil(t, got.ClosedAt)
	} else {
		require.NotNil(t, got.ClosedAt)
		require.True(t, want.ClosedAt.Equal(*got.ClosedAt), "closed_at %v != %v", *want.ClosedAt, *got.ClosedAt)
	}
}
