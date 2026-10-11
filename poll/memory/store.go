package memory

import (
	"bytes"
	"context"
	"slices"
	"sort"
	"sync"
	"time"

	"google.golang.org/protobuf/proto"

	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"
	messagingpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/messaging/v1"
	pollpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/poll/v1"

	"github.com/code-payments/flipcash2-server/poll"
)

// queued is a poll's place on the tally queue: present while the poll is live
// and not yet closed.
type queued struct {
	dueAt    time.Time
	attempts uint32
}

type memory struct {
	sync.Mutex

	// polls maps a poll ID to the poll.
	polls map[string]*poll.Poll
	// queue maps a poll ID to its place on the tally queue.
	queue map[string]*queued
	// votes maps a poll ID to its votes, keyed by voter ID.
	votes map[string]map[string]poll.Vote
}

// NewInMemory returns an in-memory poll.Store, for tests.
func NewInMemory() poll.Store {
	m := &memory{}
	m.reset()
	return m
}

func (m *memory) reset() {
	m.Lock()
	defer m.Unlock()

	m.polls = make(map[string]*poll.Poll)
	m.queue = make(map[string]*queued)
	m.votes = make(map[string]map[string]poll.Vote)
}

func (m *memory) CreatePoll(_ context.Context, p *poll.Poll) error {
	m.Lock()
	defer m.Unlock()

	key := string(p.ID.Value)
	if _, ok := m.polls[key]; ok {
		return poll.ErrPollExists
	}
	stored := p.Clone()
	stored.Status = poll.StatusOpen
	stored.Results = nil
	m.polls[key] = stored
	if stored.Live {
		m.queue[key] = &queued{dueAt: poll.TallyDueAt(stored)}
	}
	return nil
}

// get returns the stored poll for pollID unless it does not exist or has been
// discarded. The caller holds the lock.
func (m *memory) get(pollID *pollpb.PollId, now time.Time) (*poll.Poll, bool) {
	p, ok := m.polls[string(pollID.Value)]
	if !ok || p.IsDiscardedAt(now) {
		return nil, false
	}
	return p, true
}

func (m *memory) GetPoll(_ context.Context, pollID *pollpb.PollId) (*poll.Poll, error) {
	m.Lock()
	defer m.Unlock()

	p, ok := m.get(pollID, time.Now())
	if !ok {
		return nil, poll.ErrPollNotFound
	}
	return p.Clone(), nil
}

func (m *memory) GetPolls(_ context.Context, pollIDs []*pollpb.PollId) (map[string]*poll.Poll, error) {
	m.Lock()
	defer m.Unlock()

	now := time.Now()
	out := make(map[string]*poll.Poll)
	for _, id := range pollIDs {
		if p, ok := m.get(id, now); ok {
			out[string(id.Value)] = p.Clone()
		}
	}
	return out, nil
}

func (m *memory) MarkLive(_ context.Context, pollID *pollpb.PollId, clientMessageID *messagingpb.ClientMessageId, now time.Time) (bool, error) {
	m.Lock()
	defer m.Unlock()

	p, ok := m.get(pollID, now)
	if !ok {
		return false, poll.ErrPollNotFound
	}
	if p.Live {
		if p.ClientMessageID != nil && proto.Equal(p.ClientMessageID, clientMessageID) {
			return false, nil
		}
		return false, poll.ErrPollAlreadyLive
	}
	p.Live = true
	p.ClientMessageID = proto.Clone(clientMessageID).(*messagingpb.ClientMessageId)
	m.queue[string(pollID.Value)] = &queued{dueAt: poll.TallyDueAt(p)}
	return true, nil
}

func (m *memory) SetVote(_ context.Context, pollID *pollpb.PollId, userID *commonpb.UserId, options []uint32, votedAt time.Time) error {
	m.Lock()
	defer m.Unlock()

	pollKey := string(pollID.Value)
	if len(options) == 0 {
		delete(m.votes[pollKey], string(userID.Value))
		return nil
	}
	votes, ok := m.votes[pollKey]
	if !ok {
		votes = make(map[string]poll.Vote)
		m.votes[pollKey] = votes
	}
	votes[string(userID.Value)] = poll.Vote{
		UserID:  proto.Clone(userID).(*commonpb.UserId),
		Options: poll.NormalizeSelection(options),
		VotedAt: votedAt,
	}
	return nil
}

func (m *memory) GetVotes(_ context.Context, userID *commonpb.UserId, pollIDs []*pollpb.PollId) (map[string][]uint32, error) {
	m.Lock()
	defer m.Unlock()

	out := make(map[string][]uint32)
	for _, id := range pollIDs {
		if v, ok := m.votes[string(id.Value)][string(userID.Value)]; ok {
			out[string(id.Value)] = slices.Clone(v.Options)
		}
	}
	return out, nil
}

func (m *memory) GetVotesPage(_ context.Context, pollID *pollpb.PollId, after *commonpb.UserId, limit int) ([]poll.Vote, error) {
	m.Lock()
	defer m.Unlock()

	var page []poll.Vote
	for _, v := range m.votes[string(pollID.Value)] {
		if after != nil && bytes.Compare(v.UserID.Value, after.Value) <= 0 {
			continue
		}
		page = append(page, cloneVote(v))
	}
	sort.Slice(page, func(i, j int) bool {
		return bytes.Compare(page[i].UserID.Value, page[j].UserID.Value) < 0
	})
	if limit > 0 && len(page) > limit {
		page = page[:limit]
	}
	return page, nil
}

func (m *memory) GetDueForTally(_ context.Context, asOf time.Time, limit int) ([]*poll.TallyTask, error) {
	m.Lock()
	defer m.Unlock()

	var out []*poll.TallyTask
	for key, q := range m.queue {
		if q.dueAt.After(asOf) {
			continue
		}
		out = append(out, &poll.TallyTask{
			ID:            &pollpb.PollId{Value: []byte(key)},
			Attempts:      q.attempts,
			NextAttemptAt: q.dueAt,
		})
	}
	sort.Slice(out, func(i, j int) bool {
		if !out[i].NextAttemptAt.Equal(out[j].NextAttemptAt) {
			return out[i].NextAttemptAt.Before(out[j].NextAttemptAt)
		}
		return bytes.Compare(out[i].ID.Value, out[j].ID.Value) < 0
	})
	if limit > 0 && len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}

func (m *memory) GetTallyQueueStats(_ context.Context, asOf time.Time) (*poll.TallyQueueStats, error) {
	m.Lock()
	defer m.Unlock()

	stats := &poll.TallyQueueStats{}
	for _, q := range m.queue {
		if q.dueAt.After(asOf) {
			continue
		}
		if stats.DueDepth < poll.MaxTallyQueueStatsDepth {
			stats.DueDepth++
		}
		if stats.OldestDueAt.IsZero() || q.dueAt.Before(stats.OldestDueAt) {
			stats.OldestDueAt = q.dueAt
		}
	}
	return stats, nil
}

func (m *memory) ClaimForTally(_ context.Context, pollID *pollpb.PollId, asOf, until time.Time) (bool, error) {
	m.Lock()
	defer m.Unlock()

	q, ok := m.queue[string(pollID.Value)]
	if !ok || q.dueAt.After(asOf) {
		return false, nil
	}
	q.dueAt = until
	return true, nil
}

func (m *memory) DelayTally(_ context.Context, pollID *pollpb.PollId, nextAttemptAt time.Time) error {
	m.Lock()
	defer m.Unlock()

	q, ok := m.queue[string(pollID.Value)]
	if !ok {
		return nil
	}
	q.dueAt = nextAttemptAt
	q.attempts++
	return nil
}

func (m *memory) Close(_ context.Context, pollID *pollpb.PollId, results *pollpb.Results) (bool, error) {
	m.Lock()
	defer m.Unlock()

	p, ok := m.get(pollID, time.Now())
	if !ok {
		return false, poll.ErrPollNotFound
	}
	if !p.Live {
		return false, poll.ErrPollNotLive
	}
	if p.Status == poll.StatusClosed {
		return false, nil
	}
	p.Status = poll.StatusClosed
	p.Results = proto.Clone(results).(*pollpb.Results)
	delete(m.queue, string(pollID.Value))
	return true, nil
}

func cloneVote(v poll.Vote) poll.Vote {
	return poll.Vote{
		UserID:  proto.Clone(v.UserID).(*commonpb.UserId),
		Options: slices.Clone(v.Options),
		VotedAt: v.VotedAt,
	}
}
