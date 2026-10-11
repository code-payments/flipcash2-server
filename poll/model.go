// Package poll implements polls in group chats (see pollpb.PollServer).
//
// A poll belongs to the group chat it was created for. It is shown in that
// chat as a message carrying a messagingpb.PollWidget, which names the poll by
// ID alone: the question, options and state come from this package's RPCs.
//
// A poll in a group that is not private is created unsent: only its creator
// sees it, and it takes no votes, until it goes live when first sent in a
// message (see Store.MarkLive), and one that is never sent is discarded when
// it ends. It is sent at most once: the send that makes it live is the only
// one (see PrepareSend). A private group's poll text is encrypted with the
// chat key, and its widget is sent inside encrypted content the server never
// reads, so the server cannot tell when it is sent: it is live from creation.
//
// A poll closes at its PollDefinition.ends_at and at no other time. Votes are
// refused from then on; TallyDelay later, once every vote accepted before the
// end has landed, its votes are counted and its results recorded (see
// Store.Close). Until then nobody sees any totals, and individual votes are
// never shown to anyone but the voter.
package poll

import (
	"crypto/sha256"
	"fmt"
	"slices"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"
	messagingpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/messaging/v1"
	pollpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/poll/v1"

	"github.com/code-payments/flipcash2-server/model"
)

// IDSize is the length, in bytes, of a PollId.
const IDSize = 16

// IdempotencyKeySize is the length, in bytes, of a client's IdempotencyKey.
const IdempotencyKeySize = 16

const (
	// MinDuration and MaxDuration bound how long after its creation a poll may
	// end (see PollDefinition.ends_at).
	MinDuration = 5 * time.Minute
	MaxDuration = 30 * 24 * time.Hour
)

// SendRetryWindow is how long after a poll's creation the send that made it
// live may be retried (see PrepareSend). A retry is safe only while messaging
// still holds the send's client message ID, which is what makes it a replay
// of the original message rather than a second one, so every messaging store
// must keep a client message ID at least this long after its send. The send
// comes after the poll's creation, so a client message ID kept that long is
// kept past the window. It is MaxDuration, so the send can be retried at least
// until the poll ends.
const SendRetryWindow = MaxDuration

// TallyDelay is how long after a poll ends the tally takes it up, and
// VoteWriteTimeout bounds the write of a vote accepted before the end.
//
// A vote is accepted while the server taking it reads the clock before the
// poll's end (see Poll.IsOpenAt), and its write is a single item of the
// voter's own, which nothing about the poll gates (see Store.SetVote). What
// makes the tally see every accepted vote is time: a vote's write is abandoned
// VoteWriteTimeout after it is accepted, so every accepted vote has landed, or
// never will, well before TallyDelay has passed since the end. The margin
// between the two covers clock skew between the server that took a vote and
// the one tallying it. The alternative, conditioning each vote on the poll's
// item in a transaction, would make every concurrent vote in a poll a
// transaction conflict on that one item, which a poll sent to a large group
// would hit at once.
const (
	TallyDelay       = time.Minute
	VoteWriteTimeout = 10 * time.Second
)

// pollIDDomain namespaces the poll ID hash so it can never collide with an ID
// derived for another purpose.
const pollIDDomain = "flipcash:poll"

// MustDerivePollID returns the ID of the poll that a CreatePoll from creatorID
// carrying key creates.
//
// Like a group chat's ID (see chat.MustDeriveGroupChatID), a poll's ID is
// derived from its creator and the request's idempotency key rather than
// minted at random, so the ID is itself the idempotency record: a retry
// derives the same ID, and the store's uniqueness condition turns the
// duplicate into a read of the original. The creator is part of the input so
// no client can derive another user's poll ID. The digest is truncated to
// IDSize bytes and stamped as a version 8 UUID, as a group ID is.
//
// It panics if either input is not its fixed width, which would be a
// programming error: user IDs are UUIDs, and a request's key is checked to
// width before it reaches here.
func MustDerivePollID(creatorID *commonpb.UserId, key *pollpb.IdempotencyKey) *pollpb.PollId {
	if len(creatorID.GetValue()) != model.UserIDSize {
		panic(fmt.Sprintf("user id must be %d bytes, got %d", model.UserIDSize, len(creatorID.GetValue())))
	}
	if len(key.GetValue()) != IdempotencyKeySize {
		panic(fmt.Sprintf("idempotency key must be %d bytes, got %d", IdempotencyKeySize, len(key.GetValue())))
	}

	h := sha256.New()
	h.Write([]byte(pollIDDomain))
	h.Write(creatorID.Value)
	h.Write(key.Value)

	id := h.Sum(nil)[:IDSize]
	id[6] = (id[6] & 0x0f) | 0x80 // version 8
	id[8] = (id[8] & 0x3f) | 0x80 // RFC 9562 variant

	return &pollpb.PollId{Value: id}
}

// Status is a poll's status. It only moves forward: open, then tallying, then
// closed.
//
// Only open and closed are stored. Tallying is what a poll stored open is
// from its end until its results are recorded, decided off the clock (see
// Poll.StatusAt), so no write is needed for a poll to stop taking votes.
type Status uint8

const (
	StatusOpen Status = iota
	StatusTallying
	StatusClosed
)

func (s Status) String() string {
	switch s {
	case StatusOpen:
		return "open"
	case StatusTallying:
		return "tallying"
	case StatusClosed:
		return "closed"
	default:
		return fmt.Sprintf("status(%d)", uint8(s))
	}
}

// ToProto projects the status onto pollpb.PollState_Status.
func (s Status) ToProto() pollpb.PollState_Status {
	switch s {
	case StatusOpen:
		return pollpb.PollState_OPEN
	case StatusTallying:
		return pollpb.PollState_TALLYING
	case StatusClosed:
		return pollpb.PollState_CLOSED
	default:
		return pollpb.PollState_UNKNOWN
	}
}

// Poll is a poll as stored.
//
// Definition is exactly what the creator chose, and never changes. Its text is
// plaintext in a group that is not private and encrypted in a private group
// (see PollDefinition.text); the store holds either as given.
//
// Live reports whether the poll is visible to the chat and takes votes. A poll
// in a private group is created live; any other is created unsent and goes
// live when it is first sent in a message (see Store.MarkLive). An unsent poll
// is discarded once it ends, and no store returns one after that.
//
// ClientMessageID is the client message ID of the send that made the poll
// live, the one message that carries it (see PrepareSend). A poll created
// live, or not yet sent, has none.
//
// Results are set once Status is StatusClosed, and never before. They are the
// results as recorded, which for a BALANCE poll are totals no reader is shown
// (see ResultsProto).
type Poll struct {
	ID              *pollpb.PollId
	ChatID          *commonpb.ChatId
	CreatorID       *commonpb.UserId
	Definition      *pollpb.PollDefinition
	CreatedAt       time.Time
	Live            bool
	ClientMessageID *messagingpb.ClientMessageId
	Status          Status
	Results         *pollpb.Results
}

// EndsAt is when the poll stops taking votes.
func (p *Poll) EndsAt() time.Time {
	return p.Definition.GetEndsAt().AsTime()
}

// IsOpenAt reports whether the poll takes votes at now: it is live, stored
// open, and has not ended (see TallyDelay).
func (p *Poll) IsOpenAt(now time.Time) bool {
	return p.Live && p.Status == StatusOpen && now.Before(p.EndsAt())
}

// IsDiscardedAt reports whether the poll is gone at now: it ended unsent. A
// store may still hold it for a while (a DynamoDB TTL deletes lazily), but
// never returns it.
func (p *Poll) IsDiscardedAt(now time.Time) bool {
	return !p.Live && !now.Before(p.EndsAt())
}

// StatusAt is the status a reader is shown at now: the stored status, except
// that a poll stored open is tallying from its end, since it takes no votes
// from then.
func (p *Poll) StatusAt(now time.Time) Status {
	if p.Status == StatusOpen && !now.Before(p.EndsAt()) {
		return StatusTallying
	}
	return p.Status
}

// ToProto projects the poll onto pollpb.PollMetadata as a viewer sees it at
// now, with selfVote, the viewer's own vote (empty for none), as its
// PollState.self_vote.
func (p *Poll) ToProto(now time.Time, selfVote []uint32) *pollpb.PollMetadata {
	return &pollpb.PollMetadata{
		PollId:     proto.Clone(p.ID).(*pollpb.PollId),
		ChatId:     proto.Clone(p.ChatID).(*commonpb.ChatId),
		Creator:    proto.Clone(p.CreatorID).(*commonpb.UserId),
		Definition: proto.Clone(p.Definition).(*pollpb.PollDefinition),
		CreatedAt:  timestamppb.New(p.CreatedAt),
		State:      p.StateProto(now, selfVote),
	}
}

// StateProto projects the poll's state onto pollpb.PollState as a viewer sees
// it at now (see StatusAt), with selfVote as its self_vote.
//
// closed_at is the poll's end, which is when it stopped accepting votes (see
// IsOpenAt). Results are carried only once the poll is closed, as a reader is
// shown them (see ResultsProto).
func (p *Poll) StateProto(now time.Time, selfVote []uint32) *pollpb.PollState {
	status := p.StatusAt(now)
	state := &pollpb.PollState{
		Status:   status.ToProto(),
		SelfVote: slices.Clone(selfVote),
	}
	if status != StatusOpen {
		state.ClosedAt = timestamppb.New(p.EndsAt())
	}
	if status == StatusClosed && p.Results != nil {
		state.Results = ResultsProto(p.Results)
	}
	return state
}

// ResultsProto projects stored results onto what a reader is shown: a
// BALANCE poll's totals as their ranking alone (see pollpb.RankedResults), and
// every other variant as stored.
//
// The ranking is by quarks, not by value: every holding in a poll is of its
// one mint, so the quark totals order the options exactly as their value
// would, without the float error a sum of values carries (equal balances must
// share a rank) or the rate moving between the batches a large tally is read
// in (see balance.Client.GetMintHoldingsForUsers).
//
// The totals stay stored, so the server keeps what the ranking was made from;
// no reader is ever sent them.
func ResultsProto(results *pollpb.Results) *pollpb.Results {
	balance := results.GetBalance()
	if balance == nil {
		return proto.Clone(results).(*pollpb.Results)
	}

	amounts := make([]uint64, len(balance.OptionBalances))
	for i, option := range balance.OptionBalances {
		amounts[i] = option.GetQuarks()
	}
	return &pollpb.Results{Type: &pollpb.Results_Ranked{Ranked: &pollpb.RankedResults{
		OptionRanks: rank(amounts),
	}}}
}

// rank returns each amount's competition rank, in order: 1 for the largest,
// equal amounts sharing a rank, and the next rank skipping the tied positions
// (30, 20, 20, 10 rank 1, 2, 2, 4).
func rank(amounts []uint64) []uint32 {
	ranks := make([]uint32, len(amounts))
	for i, amount := range amounts {
		ranks[i] = 1
		for _, other := range amounts {
			if other > amount {
				ranks[i]++
			}
		}
	}
	return ranks
}

// Clone returns a deep copy of the poll.
func (p *Poll) Clone() *Poll {
	clone := &Poll{
		ID:         proto.Clone(p.ID).(*pollpb.PollId),
		ChatID:     proto.Clone(p.ChatID).(*commonpb.ChatId),
		CreatorID:  proto.Clone(p.CreatorID).(*commonpb.UserId),
		Definition: proto.Clone(p.Definition).(*pollpb.PollDefinition),
		CreatedAt:  p.CreatedAt,
		Live:       p.Live,
		Status:     p.Status,
	}
	if p.ClientMessageID != nil {
		clone.ClientMessageID = proto.Clone(p.ClientMessageID).(*messagingpb.ClientMessageId)
	}
	if p.Results != nil {
		clone.Results = proto.Clone(p.Results).(*pollpb.Results)
	}
	return clone
}

// Vote is one user's current selection in a poll: the positions of the
// options they chose, in ascending order, never empty (a removed vote is no
// vote at all), and when they last chose it.
type Vote struct {
	UserID  *commonpb.UserId
	Options []uint32
	VotedAt time.Time
}

// NormalizeSelection returns options sorted ascending, the order every store
// holds a selection in, so equal selections compare equal however a client
// ordered them.
func NormalizeSelection(options []uint32) []uint32 {
	sorted := slices.Clone(options)
	slices.Sort(sorted)
	return sorted
}

// TallyTask is a queued unit of tally work: a live poll that has yet to be
// closed, with the retry bookkeeping the tally schedules off of (see
// Store.GetDueForTally).
type TallyTask struct {
	ID *pollpb.PollId

	// Attempts is the number of failed tally attempts recorded so far (via
	// Store.DelayTally). The tally uses it to pace backoff.
	Attempts uint32

	// NextAttemptAt is when the task next becomes due: TallyDelay after the
	// poll ends at first (see TallyDueAt), then whenever a claim or a delay
	// moves it.
	NextAttemptAt time.Time
}

// MaxTallyQueueStatsDepth bounds the walk behind Store.GetTallyQueueStats:
// a backlog deeper than this reports this depth, so the gauges cost one bounded
// read even mid-incident.
const MaxTallyQueueStatsDepth = 1000

// TallyQueueStats is a snapshot of the tally's backlog, backing its gauges
// (see Store.GetTallyQueueStats). Only due polls count: one waiting for its end
// is not work the tally is behind on. A claimed poll is not due until its
// lease ends, and one whose attempt failed not until its retry, so a poll
// stuck retrying shows here only between its retry coming due and a worker
// claiming it; its failures are logged by the worker.
type TallyQueueStats struct {
	// DueDepth is how many queued polls are due, at most
	// MaxTallyQueueStatsDepth.
	DueDepth uint64

	// OldestDueAt is the due time of the most overdue poll, or the zero time
	// when none is due.
	OldestDueAt time.Time
}

// TallyDueAt is when a live poll first comes due for the tally: TallyDelay
// after it ends.
func TallyDueAt(p *Poll) time.Time {
	return p.EndsAt().Add(TallyDelay)
}
