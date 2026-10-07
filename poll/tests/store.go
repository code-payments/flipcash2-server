package tests

import (
	"bytes"
	"context"
	"sort"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"
	pollpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/poll/v1"

	"github.com/code-payments/flipcash2-server/chat"
	"github.com/code-payments/flipcash2-server/model"
	"github.com/code-payments/flipcash2-server/poll"
)

// RunStoreTests runs the shared poll.Store test suite against s; teardown
// resets it between tests.
func RunStoreTests(t *testing.T, s poll.Store, teardown func()) {
	for _, tf := range []func(t *testing.T, s poll.Store){
		testStore_CreateAndGet,
		testStore_CreatePoll_Exists,
		testStore_GetPolls,
		testStore_UnsentPollDiscardedAtEnd,
		testStore_MarkLive,
		testStore_Votes,
		testStore_GetVotesPage,
		testStore_TallyQueue,
		testStore_ClaimForTally,
		testStore_DelayTally,
		testStore_TallyQueueStats,
		testStore_Close,
	} {
		tf(t, s)
		teardown()
	}
}

func testStore_CreateAndGet(t *testing.T, s poll.Store) {
	ctx := context.Background()

	for _, live := range []bool{true, false} {
		p := newStorePoll(live, time.Now().Add(time.Hour))
		require.NoError(t, s.CreatePoll(ctx, p))

		got, err := s.GetPoll(ctx, p.ID)
		require.NoError(t, err)
		requirePollEqual(t, p, got)
		require.Equal(t, poll.StatusOpen, got.Status)
		require.Nil(t, got.Results)
	}

	_, err := s.GetPoll(ctx, newPollID())
	require.ErrorIs(t, err, poll.ErrPollNotFound)
}

func testStore_CreatePoll_Exists(t *testing.T, s poll.Store) {
	ctx := context.Background()

	p := newStorePoll(true, time.Now().Add(time.Hour))
	require.NoError(t, s.CreatePoll(ctx, p))

	// A second poll under the same ID is refused, and the first stands.
	other := newStorePoll(false, time.Now().Add(2*time.Hour))
	other.ID = p.ID
	require.ErrorIs(t, s.CreatePoll(ctx, other), poll.ErrPollExists)

	got, err := s.GetPoll(ctx, p.ID)
	require.NoError(t, err)
	requirePollEqual(t, p, got)
}

func testStore_GetPolls(t *testing.T, s poll.Store) {
	ctx := context.Background()

	a := newStorePoll(true, time.Now().Add(time.Hour))
	b := newStorePoll(false, time.Now().Add(time.Hour))
	require.NoError(t, s.CreatePoll(ctx, a))
	require.NoError(t, s.CreatePoll(ctx, b))

	got, err := s.GetPolls(ctx, []*pollpb.PollId{a.ID, newPollID(), b.ID, a.ID})
	require.NoError(t, err)
	require.Len(t, got, 2)
	requirePollEqual(t, a, got[string(a.ID.Value)])
	requirePollEqual(t, b, got[string(b.ID.Value)])

	got, err = s.GetPolls(ctx, nil)
	require.NoError(t, err)
	require.Empty(t, got)
}

func testStore_UnsentPollDiscardedAtEnd(t *testing.T, s poll.Store) {
	ctx := context.Background()

	// The store takes any end it is given; one already past makes an unsent
	// poll that is discarded from the start.
	unsent := newStorePoll(false, time.Now().Add(-time.Second))
	require.NoError(t, s.CreatePoll(ctx, unsent))

	_, err := s.GetPoll(ctx, unsent.ID)
	require.ErrorIs(t, err, poll.ErrPollNotFound)

	got, err := s.GetPolls(ctx, []*pollpb.PollId{unsent.ID})
	require.NoError(t, err)
	require.Empty(t, got)

	_, err = s.MarkLive(ctx, unsent.ID, time.Now())
	require.ErrorIs(t, err, poll.ErrPollNotFound)

	// A live poll past its end is not discarded: it is waiting to be counted.
	live := newStorePoll(true, time.Now().Add(-time.Second))
	require.NoError(t, s.CreatePoll(ctx, live))

	gotLive, err := s.GetPoll(ctx, live.ID)
	require.NoError(t, err)
	requirePollEqual(t, live, gotLive)
}

func testStore_MarkLive(t *testing.T, s poll.Store) {
	ctx := context.Background()

	p := newStorePoll(false, time.Now().Add(time.Hour))
	require.NoError(t, s.CreatePoll(ctx, p))

	changed, err := s.MarkLive(ctx, p.ID, time.Now())
	require.NoError(t, err)
	require.True(t, changed)

	got, err := s.GetPoll(ctx, p.ID)
	require.NoError(t, err)
	require.True(t, got.Live)

	// Going live again is the no-op.
	changed, err = s.MarkLive(ctx, p.ID, time.Now())
	require.NoError(t, err)
	require.False(t, changed)

	// So is a poll created live.
	live := newStorePoll(true, time.Now().Add(time.Hour))
	require.NoError(t, s.CreatePoll(ctx, live))
	changed, err = s.MarkLive(ctx, live.ID, time.Now())
	require.NoError(t, err)
	require.False(t, changed)

	// An unsent poll cannot go live at or after its end, which is when it is
	// discarded.
	ending := newStorePoll(false, time.Now().Add(time.Hour))
	require.NoError(t, s.CreatePoll(ctx, ending))
	_, err = s.MarkLive(ctx, ending.ID, ending.EndsAt())
	require.ErrorIs(t, err, poll.ErrPollNotFound)

	_, err = s.MarkLive(ctx, newPollID(), time.Now())
	require.ErrorIs(t, err, poll.ErrPollNotFound)
}

func testStore_Votes(t *testing.T, s poll.Store) {
	ctx := context.Background()

	a := newStorePoll(true, time.Now().Add(time.Hour))
	b := newStorePoll(true, time.Now().Add(time.Hour))
	require.NoError(t, s.CreatePoll(ctx, a))
	require.NoError(t, s.CreatePoll(ctx, b))

	voter := model.MustGenerateUserID()
	other := model.MustGenerateUserID()

	got, err := s.GetVotes(ctx, voter, []*pollpb.PollId{a.ID, b.ID})
	require.NoError(t, err)
	require.Empty(t, got)

	// A selection is held in ascending order however it was given.
	require.NoError(t, s.SetVote(ctx, a.ID, voter, []uint32{2, 0}, time.Now()))
	require.NoError(t, s.SetVote(ctx, b.ID, voter, []uint32{1}, time.Now()))
	require.NoError(t, s.SetVote(ctx, a.ID, other, []uint32{1}, time.Now()))

	got, err = s.GetVotes(ctx, voter, []*pollpb.PollId{a.ID, b.ID, newPollID(), a.ID})
	require.NoError(t, err)
	require.Equal(t, map[string][]uint32{
		string(a.ID.Value): {0, 2},
		string(b.ID.Value): {1},
	}, got)

	// A vote is replaced whole.
	require.NoError(t, s.SetVote(ctx, a.ID, voter, []uint32{1}, time.Now()))
	got, err = s.GetVotes(ctx, voter, []*pollpb.PollId{a.ID})
	require.NoError(t, err)
	require.Equal(t, map[string][]uint32{string(a.ID.Value): {1}}, got)

	// An empty selection removes it, and removing a vote twice is harmless.
	require.NoError(t, s.SetVote(ctx, a.ID, voter, nil, time.Now()))
	require.NoError(t, s.SetVote(ctx, a.ID, voter, nil, time.Now()))
	got, err = s.GetVotes(ctx, voter, []*pollpb.PollId{a.ID, b.ID})
	require.NoError(t, err)
	require.Equal(t, map[string][]uint32{string(b.ID.Value): {1}}, got)

	// Another voter's vote is their own.
	got, err = s.GetVotes(ctx, other, []*pollpb.PollId{a.ID, b.ID})
	require.NoError(t, err)
	require.Equal(t, map[string][]uint32{string(a.ID.Value): {1}}, got)
}

func testStore_GetVotesPage(t *testing.T, s poll.Store) {
	ctx := context.Background()

	p := newStorePoll(true, time.Now().Add(time.Hour))
	require.NoError(t, s.CreatePoll(ctx, p))

	// Votes in another poll stay out of this one's pages.
	elsewhere := newStorePoll(true, time.Now().Add(time.Hour))
	require.NoError(t, s.CreatePoll(ctx, elsewhere))
	require.NoError(t, s.SetVote(ctx, elsewhere.ID, model.MustGenerateUserID(), []uint32{0}, time.Now()))

	votedAt := time.Now().Truncate(time.Millisecond)
	voters := make([]*commonpb.UserId, 5)
	for i := range voters {
		voters[i] = model.MustGenerateUserID()
		require.NoError(t, s.SetVote(ctx, p.ID, voters[i], []uint32{uint32(i % 2)}, votedAt))
	}
	sort.Slice(voters, func(i, j int) bool { return bytes.Compare(voters[i].Value, voters[j].Value) < 0 })

	var (
		all   []poll.Vote
		after *commonpb.UserId
	)
	for {
		page, err := s.GetVotesPage(ctx, p.ID, after, 2)
		require.NoError(t, err)
		if len(page) == 0 {
			break
		}
		require.LessOrEqual(t, len(page), 2)
		all = append(all, page...)
		after = page[len(page)-1].UserID
	}

	require.Len(t, all, len(voters))
	for i, vote := range all {
		require.Equal(t, voters[i].Value, vote.UserID.Value)
		require.Len(t, vote.Options, 1)
		require.True(t, votedAt.Equal(vote.VotedAt))
	}

	unbounded, err := s.GetVotesPage(ctx, p.ID, nil, 0)
	require.NoError(t, err)
	require.Len(t, unbounded, len(voters))

	empty, err := s.GetVotesPage(ctx, newPollID(), nil, 10)
	require.NoError(t, err)
	require.Empty(t, empty)
}

func testStore_TallyQueue(t *testing.T, s poll.Store) {
	ctx := context.Background()

	now := time.Now()
	early := newStorePoll(true, now.Add(time.Hour))
	late := newStorePoll(true, now.Add(2*time.Hour))
	unsent := newStorePoll(false, now.Add(time.Hour))
	sent := newStorePoll(false, now.Add(3*time.Hour))
	for _, p := range []*poll.Poll{late, early, unsent, sent} {
		require.NoError(t, s.CreatePoll(ctx, p))
	}
	_, err := s.MarkLive(ctx, sent.ID, now)
	require.NoError(t, err)

	// A poll comes due TallyDelay after it ends, and not before.
	due, err := s.GetDueForTally(ctx, poll.TallyDueAt(early).Add(-time.Nanosecond), 10)
	require.NoError(t, err)
	require.Empty(t, due)

	due, err = s.GetDueForTally(ctx, poll.TallyDueAt(early), 10)
	require.NoError(t, err)
	require.Len(t, due, 1)
	require.Equal(t, early.ID.Value, due[0].ID.Value)
	require.True(t, poll.TallyDueAt(early).Equal(due[0].NextAttemptAt))
	require.Zero(t, due[0].Attempts)

	// Soonest first, unsent polls never, and a poll gone live once it did.
	due, err = s.GetDueForTally(ctx, now.Add(4*time.Hour), 10)
	require.NoError(t, err)
	require.Len(t, due, 3)
	require.Equal(t, early.ID.Value, due[0].ID.Value)
	require.Equal(t, late.ID.Value, due[1].ID.Value)
	require.Equal(t, sent.ID.Value, due[2].ID.Value)
	require.True(t, poll.TallyDueAt(sent).Equal(due[2].NextAttemptAt))

	due, err = s.GetDueForTally(ctx, now.Add(4*time.Hour), 2)
	require.NoError(t, err)
	require.Len(t, due, 2)
	require.Equal(t, early.ID.Value, due[0].ID.Value)
	require.Equal(t, late.ID.Value, due[1].ID.Value)
}

func testStore_ClaimForTally(t *testing.T, s poll.Store) {
	ctx := context.Background()

	now := time.Now()
	p := newStorePoll(true, now.Add(time.Hour))
	require.NoError(t, s.CreatePoll(ctx, p))

	dueAt := poll.TallyDueAt(p)
	lease := dueAt.Add(2 * time.Minute)

	// A poll that is not due yet cannot be claimed.
	claimed, err := s.ClaimForTally(ctx, p.ID, dueAt.Add(-time.Nanosecond), lease)
	require.NoError(t, err)
	require.False(t, claimed)

	// One claim as of the due time wins; a second as of the same time loses,
	// since the first pushed the poll out to its lease.
	claimed, err = s.ClaimForTally(ctx, p.ID, dueAt, lease)
	require.NoError(t, err)
	require.True(t, claimed)
	claimed, err = s.ClaimForTally(ctx, p.ID, dueAt, lease)
	require.NoError(t, err)
	require.False(t, claimed)

	// The claimed poll is out of the due set until its lease runs out, and a
	// claim counts no attempt.
	due, err := s.GetDueForTally(ctx, lease.Add(-time.Nanosecond), 10)
	require.NoError(t, err)
	require.Empty(t, due)

	due, err = s.GetDueForTally(ctx, lease, 10)
	require.NoError(t, err)
	require.Len(t, due, 1)
	require.True(t, lease.Equal(due[0].NextAttemptAt))
	require.Zero(t, due[0].Attempts)

	claimed, err = s.ClaimForTally(ctx, p.ID, lease, lease.Add(time.Minute))
	require.NoError(t, err)
	require.True(t, claimed)

	// Neither a poll that doesn't exist nor an unsent one can be claimed.
	claimed, err = s.ClaimForTally(ctx, newPollID(), now.Add(24*time.Hour), lease)
	require.NoError(t, err)
	require.False(t, claimed)

	unsent := newStorePoll(false, now.Add(time.Hour))
	require.NoError(t, s.CreatePoll(ctx, unsent))
	claimed, err = s.ClaimForTally(ctx, unsent.ID, now.Add(24*time.Hour), lease)
	require.NoError(t, err)
	require.False(t, claimed)
}

func testStore_DelayTally(t *testing.T, s poll.Store) {
	ctx := context.Background()

	now := time.Now()
	p := newStorePoll(true, now.Add(time.Hour))
	require.NoError(t, s.CreatePoll(ctx, p))

	retryAt := poll.TallyDueAt(p).Add(10 * time.Minute)
	require.NoError(t, s.DelayTally(ctx, p.ID, retryAt))
	require.NoError(t, s.DelayTally(ctx, p.ID, retryAt))

	due, err := s.GetDueForTally(ctx, retryAt.Add(-time.Nanosecond), 10)
	require.NoError(t, err)
	require.Empty(t, due)

	due, err = s.GetDueForTally(ctx, retryAt, 10)
	require.NoError(t, err)
	require.Len(t, due, 1)
	require.True(t, retryAt.Equal(due[0].NextAttemptAt))
	require.EqualValues(t, 2, due[0].Attempts)

	// Delaying a poll that is not queued is a no-op: it stays off the queue.
	unsent := newStorePoll(false, now.Add(time.Hour))
	require.NoError(t, s.CreatePoll(ctx, unsent))
	require.NoError(t, s.DelayTally(ctx, unsent.ID, retryAt))
	require.NoError(t, s.DelayTally(ctx, newPollID(), retryAt))

	due, err = s.GetDueForTally(ctx, retryAt, 10)
	require.NoError(t, err)
	require.Len(t, due, 1)
	require.Equal(t, p.ID.Value, due[0].ID.Value)
}

func testStore_TallyQueueStats(t *testing.T, s poll.Store) {
	ctx := context.Background()

	now := time.Now().Truncate(time.Millisecond)

	stats, err := s.GetTallyQueueStats(ctx, now)
	require.NoError(t, err)
	require.Zero(t, stats.DueDepth)
	require.True(t, stats.OldestDueAt.IsZero())

	sooner := newStorePoll(true, now.Add(time.Hour))
	later := newStorePoll(true, now.Add(2*time.Hour))
	unsent := newStorePoll(false, now.Add(time.Hour))
	for _, p := range []*poll.Poll{sooner, later, unsent} {
		require.NoError(t, s.CreatePoll(ctx, p))
	}

	// Queued polls not yet due are not backlog, and an unsent poll is not
	// queued at all.
	stats, err = s.GetTallyQueueStats(ctx, now)
	require.NoError(t, err)
	require.Zero(t, stats.DueDepth)
	require.True(t, stats.OldestDueAt.IsZero())

	// A poll counts from its due time on.
	stats, err = s.GetTallyQueueStats(ctx, poll.TallyDueAt(sooner))
	require.NoError(t, err)
	require.EqualValues(t, 1, stats.DueDepth)
	require.True(t, poll.TallyDueAt(sooner).Equal(stats.OldestDueAt))

	// Going live enqueues a poll, and the most overdue sets the age.
	_, err = s.MarkLive(ctx, unsent.ID, now)
	require.NoError(t, err)

	asOf := now.Add(3 * time.Hour)
	stats, err = s.GetTallyQueueStats(ctx, asOf)
	require.NoError(t, err)
	require.EqualValues(t, 3, stats.DueDepth)
	require.True(t, poll.TallyDueAt(sooner).Equal(stats.OldestDueAt))

	// A claimed poll is not due until its lease ends.
	claimed, err := s.ClaimForTally(ctx, sooner.ID, asOf, asOf.Add(time.Minute))
	require.NoError(t, err)
	require.True(t, claimed)

	stats, err = s.GetTallyQueueStats(ctx, asOf)
	require.NoError(t, err)
	require.EqualValues(t, 2, stats.DueDepth)
	require.True(t, poll.TallyDueAt(unsent).Equal(stats.OldestDueAt))

	// Closing a poll dequeues it.
	_, err = s.Close(ctx, unsent.ID, &pollpb.Results{Type: &pollpb.Results_Withheld{Withheld: &pollpb.WithheldResults{}}})
	require.NoError(t, err)

	stats, err = s.GetTallyQueueStats(ctx, asOf)
	require.NoError(t, err)
	require.EqualValues(t, 1, stats.DueDepth)
	require.True(t, poll.TallyDueAt(later).Equal(stats.OldestDueAt))
}

func testStore_Close(t *testing.T, s poll.Store) {
	ctx := context.Background()

	now := time.Now()
	p := newStorePoll(true, now.Add(time.Hour))
	require.NoError(t, s.CreatePoll(ctx, p))

	results := &pollpb.Results{Type: &pollpb.Results_Count{Count: &pollpb.CountResults{
		TotalVoters:  3,
		OptionVoters: []uint64{2, 1},
	}}}
	changed, err := s.Close(ctx, p.ID, results)
	require.NoError(t, err)
	require.True(t, changed)

	got, err := s.GetPoll(ctx, p.ID)
	require.NoError(t, err)
	require.Equal(t, poll.StatusClosed, got.Status)
	require.True(t, proto.Equal(results, got.Results))

	// A closed poll is off the tally queue for good, and cannot be claimed or
	// delayed back onto it.
	require.NoError(t, s.DelayTally(ctx, p.ID, now))
	due, err := s.GetDueForTally(ctx, now.Add(24*time.Hour), 10)
	require.NoError(t, err)
	require.Empty(t, due)
	claimed, err := s.ClaimForTally(ctx, p.ID, now.Add(24*time.Hour), now.Add(25*time.Hour))
	require.NoError(t, err)
	require.False(t, claimed)

	// Closing again is the no-op, and keeps the first results.
	withheld := &pollpb.Results{Type: &pollpb.Results_Withheld{Withheld: &pollpb.WithheldResults{}}}
	changed, err = s.Close(ctx, p.ID, withheld)
	require.NoError(t, err)
	require.False(t, changed)
	got, err = s.GetPoll(ctx, p.ID)
	require.NoError(t, err)
	require.True(t, proto.Equal(results, got.Results))

	unsent := newStorePoll(false, now.Add(time.Hour))
	require.NoError(t, s.CreatePoll(ctx, unsent))
	_, err = s.Close(ctx, unsent.ID, withheld)
	require.ErrorIs(t, err, poll.ErrPollNotLive)

	_, err = s.Close(ctx, newPollID(), withheld)
	require.ErrorIs(t, err, poll.ErrPollNotFound)
}

// newStorePoll returns a COUNT poll in a fresh group, live or not, ending at
// endsAt.
func newStorePoll(live bool, endsAt time.Time) *poll.Poll {
	return &poll.Poll{
		ID:        newPollID(),
		ChatID:    chat.MustGenerateGroupChatID(),
		CreatorID: model.MustGenerateUserID(),
		Definition: &pollpb.PollDefinition{
			OptionCount: 3,
			Mode:        pollpb.Mode_COUNT,
			EndsAt:      timestamppb.New(endsAt),
			Text: &pollpb.PollDefinition_Plaintext{Plaintext: &pollpb.PollText{
				Question: "Which one?",
				Options:  []*pollpb.Option{{Text: "a"}, {Text: "b"}, {Text: "c"}},
			}},
		},
		CreatedAt: time.Now().Truncate(time.Millisecond),
		Live:      live,
		Status:    poll.StatusOpen,
	}
}

func newPollID() *pollpb.PollId {
	return poll.MustDerivePollID(model.MustGenerateUserID(), &pollpb.IdempotencyKey{Value: model.MustGenerateUserID().Value})
}

// requirePollEqual requires that got is the poll want describes.
func requirePollEqual(t *testing.T, want, got *poll.Poll) {
	t.Helper()
	require.NotNil(t, got)
	require.Equal(t, want.ID.Value, got.ID.Value)
	require.Equal(t, want.ChatID.Value, got.ChatID.Value)
	require.Equal(t, want.CreatorID.Value, got.CreatorID.Value)
	require.True(t, proto.Equal(want.Definition, got.Definition))
	require.True(t, want.CreatedAt.Equal(got.CreatedAt), "created_at %v != %v", want.CreatedAt, got.CreatedAt)
	require.Equal(t, want.Live, got.Live)
	require.Equal(t, want.Status, got.Status)
}
