package poll

import (
	"context"
	"errors"
	"time"

	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"
	pollpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/poll/v1"
)

var (
	// ErrPollNotFound is returned for a poll that does not exist, including an
	// unsent poll that has ended and so been discarded.
	ErrPollNotFound = errors.New("poll not found")

	// ErrPollExists is returned by CreatePoll when a poll with the same ID
	// already exists: an earlier attempt of the same request created it.
	ErrPollExists = errors.New("poll already exists")

	// ErrPollNotLive is returned by Close for a poll that never went live,
	// which is never tallied.
	ErrPollNotLive = errors.New("poll is not live")
)

// Store persists polls and their votes.
//
// A poll is written once with everything its creator chose (see Poll), and
// what changes afterward moves forward only: it goes live (MarkLive), and is
// closed with its results (Close). Both are conditional and idempotent, so a
// retry, or two servers doing the same step, is harmless.
//
// Votes are kept per (poll, voter) and replaced whole, each a single write of
// the voter's own item: the poll's item is never part of a vote, so a burst of
// votes in a large group contends on nothing. Whether a poll still takes a
// vote is the caller's to decide off the poll's end (see Poll.IsOpenAt and
// TallyDelay), not the store's. No version or count of votes is kept anywhere,
// since one that moved with each vote would tell a watcher when votes are
// cast.
//
// Every live poll that has yet to be closed is on a tally queue
// (GetDueForTally, ClaimForTally, DelayTally), due TallyDelay after it ends,
// and leaves it when it is closed: going live enqueues it, and closing it
// dequeues it in the same write.
//
// Reads of polls and votes are strongly consistent: a creator reads their poll
// right after creating it, a voter their vote right after casting it, and the
// tally every vote cast before the poll ended. The tally queue is not, and need
// not be: a claim re-checks the poll it names.
type Store interface {
	// CreatePoll writes a new poll as given, live or not (see Poll.Live), with
	// status open and no results, and a live one enqueued for the tally.
	// ErrPollExists is returned, and nothing is written, when a poll with its
	// ID already exists.
	CreatePoll(ctx context.Context, p *Poll) error

	// GetPoll returns a poll, or ErrPollNotFound.
	GetPoll(ctx context.Context, pollID *pollpb.PollId) (*Poll, error)

	// GetPolls returns each of the given polls that exists, keyed by
	// string(pollID.Value). Polls that do not exist are absent from the map,
	// as are duplicates beyond the first.
	GetPolls(ctx context.Context, pollIDs []*pollpb.PollId) (map[string]*Poll, error)

	// MarkLive makes an unsent poll live (see Poll.Live), enqueuing it for the
	// tally, and reports whether it changed anything: a poll already live is a
	// no-op. ErrPollNotFound is returned for a poll that does not exist, or
	// that ended unsent at or before now and so has been discarded.
	MarkLive(ctx context.Context, pollID *pollpb.PollId, now time.Time) (changed bool, err error)

	// SetVote replaces userID's vote in a poll with options, as cast at
	// votedAt, or removes it when options is empty. Nothing is checked against
	// the poll, which need not even exist: the caller has established that the
	// poll is open and the options are a valid selection in it.
	SetVote(ctx context.Context, pollID *pollpb.PollId, userID *commonpb.UserId, options []uint32, votedAt time.Time) error

	// GetVotes returns userID's vote in each of the given polls they have voted
	// in, as its options in ascending order, keyed by string(pollID.Value).
	// Polls they have not voted in are absent.
	GetVotes(ctx context.Context, userID *commonpb.UserId, pollIDs []*pollpb.PollId) (map[string][]uint32, error)

	// GetVotesPage returns up to limit of a poll's votes in ascending order of
	// voter ID, strictly after the voter after (from the first when nil). An
	// empty page means there are no more.
	GetVotesPage(ctx context.Context, pollID *pollpb.PollId, after *commonpb.UserId, limit int) ([]Vote, error)

	// GetDueForTally returns up to limit polls on the tally queue whose due time
	// is at or before asOf, soonest first. The queue is eventually consistent:
	// a poll just claimed or closed may still be listed, which ClaimForTally
	// settles.
	GetDueForTally(ctx context.Context, asOf time.Time, limit int) ([]*TallyTask, error)

	// GetTallyQueueStats reports the tally's backlog as of asOf: how many
	// queued polls are due, counting at most MaxTallyQueueStatsDepth, and the
	// due time of the most overdue. Polls not yet due are never read, so its
	// cost follows the backlog, not the number of open polls. It is eventually
	// consistent, like GetDueForTally.
	GetTallyQueueStats(ctx context.Context, asOf time.Time) (*TallyQueueStats, error)

	// ClaimForTally pushes a queued poll's due time out to until, provided it
	// is still queued and due as of asOf. It reports whether the claim was
	// performed: false means the poll left the queue (it was closed) or is no
	// longer due (another server claimed or delayed it first).
	//
	// A claim is an efficiency guard, not a lock: a tally is idempotent and
	// safe to run concurrently, and claiming only keeps servers from
	// duplicating its reads. A crashed claimant needs no recovery: the claim
	// expires when its until passes and the poll simply becomes due again.
	ClaimForTally(ctx context.Context, pollID *pollpb.PollId, asOf, until time.Time) (bool, error)

	// DelayTally reschedules a queued poll after a failed attempt: its due
	// time moves to nextAttemptAt and its failed-attempt count increments. It
	// is a no-op on a poll that is no longer queued (a concurrent tally closed
	// it, dequeuing it).
	DelayTally(ctx context.Context, pollID *pollpb.PollId, nextAttemptAt time.Time) error

	// Close records a live poll's results, moves it to closed and dequeues it
	// from the tally queue, and reports whether it changed anything: a poll already
	// closed is a no-op that keeps its results. ErrPollNotFound is returned for
	// a poll that does not exist, and ErrPollNotLive for one that never went
	// live. Whether the poll has ended, and its votes are all in, is the
	// caller's (see TallyDelay).
	Close(ctx context.Context, pollID *pollpb.PollId, results *pollpb.Results) (changed bool, err error)
}
