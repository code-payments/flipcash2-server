package memory

import (
	"bytes"
	"context"
	"sort"
	"sync"
	"time"

	"google.golang.org/protobuf/proto"

	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"
	paymentrequestpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/paymentrequest/v1"

	"github.com/code-payments/flipcash2-server/paymentrequest"
)

// queued is a request's place on the expiry queue: present while the request
// is stored open.
type queued struct {
	dueAt    time.Time
	attempts uint32
}

type memory struct {
	sync.Mutex

	// requests maps a request ID to the request.
	requests map[string]*paymentrequest.PaymentRequest
	// queue maps a request ID to its place on the expiry queue.
	queue map[string]*queued
	// payers maps a collection's ID, then a member's user ID, to the intent
	// ID issued to them and, once they have contributed, its Paid.
	payers map[string]map[string]*paymentrequest.PayerIntent
}

// NewInMemory returns an in-memory paymentrequest.Store, for tests.
func NewInMemory() paymentrequest.Store {
	m := &memory{}
	m.reset()
	return m
}

func (m *memory) reset() {
	m.Lock()
	defer m.Unlock()

	m.requests = make(map[string]*paymentrequest.PaymentRequest)
	m.queue = make(map[string]*queued)
	m.payers = make(map[string]map[string]*paymentrequest.PayerIntent)
}

func (m *memory) Create(_ context.Context, r *paymentrequest.PaymentRequest) error {
	if err := r.Validate(); err != nil {
		return err
	}

	m.Lock()
	defer m.Unlock()

	key := string(r.ID.Value)
	if _, ok := m.requests[key]; ok {
		return paymentrequest.ErrExists
	}
	stored := r.Clone()
	stored.Status = paymentrequest.StatusOpen
	stored.Version = 1
	stored.ClosedAt = nil
	stored.PayerCount = 0
	m.requests[key] = stored
	if _, ok := stored.EndsAt(); ok {
		m.queue[key] = &queued{dueAt: paymentrequest.ExpiryDueAt(stored)}
	}
	return nil
}

func (m *memory) Get(_ context.Context, id *paymentrequestpb.PaymentRequestId) (*paymentrequest.PaymentRequest, error) {
	m.Lock()
	defer m.Unlock()

	r, ok := m.requests[string(id.Value)]
	if !ok {
		return nil, paymentrequest.ErrNotFound
	}
	return r.Clone(), nil
}

func (m *memory) GetChatPage(_ context.Context, chatID *commonpb.ChatId, section paymentrequest.Section, cursor *paymentrequest.PageCursor, limit int) ([]*paymentrequest.PaymentRequest, error) {
	m.Lock()
	defer m.Unlock()

	type entry struct {
		at time.Time
		r  *paymentrequest.PaymentRequest
	}
	var entries []entry
	for _, r := range m.requests {
		if !bytes.Equal(r.ChatID.Value, chatID.GetValue()) {
			continue
		}
		if (section == paymentrequest.SectionOpen) == r.Status.IsFinal() {
			continue
		}
		if _, ok := r.EndsAt(); section == paymentrequest.SectionOpen && !ok {
			// Undated: in neither section while open.
			continue
		}
		entries = append(entries, entry{at: paymentrequest.CursorOf(r, section).At, r: r})
	}

	// before reports whether a sorts ahead of b in the section's order.
	before := func(aAt time.Time, aID []byte, bAt time.Time, bID []byte) bool {
		if section == paymentrequest.SectionOpen {
			if !aAt.Equal(bAt) {
				return aAt.Before(bAt)
			}
			return bytes.Compare(aID, bID) < 0
		}
		if !aAt.Equal(bAt) {
			return aAt.After(bAt)
		}
		return bytes.Compare(aID, bID) > 0
	}
	sort.Slice(entries, func(i, j int) bool {
		return before(entries[i].at, entries[i].r.ID.Value, entries[j].at, entries[j].r.ID.Value)
	})

	var page []*paymentrequest.PaymentRequest
	for _, e := range entries {
		if cursor != nil && !before(cursor.At, cursor.ID, e.at, e.r.ID.Value) {
			continue
		}
		if limit > 0 && len(page) == limit {
			break
		}
		page = append(page, e.r.Clone())
	}
	return page, nil
}

func (m *memory) PayShare(_ context.Context, id *paymentrequestpb.PaymentRequestId, userID *commonpb.UserId, paid *paymentrequestpb.Paid, now time.Time) (*paymentrequest.PaymentRequest, bool, bool, error) {
	if err := paymentrequest.ValidatePaidShare(paid); err != nil {
		return nil, false, false, err
	}

	m.Lock()
	defer m.Unlock()

	key := string(id.Value)
	r, ok := m.requests[key]
	if !ok {
		return nil, false, false, paymentrequest.ErrNotFound
	}
	share := r.Share(userID)
	if share == nil {
		return nil, false, false, paymentrequest.ErrNotAShare
	}
	if err := paymentrequest.ValidatePaymentIntent(r, userID, paid); err != nil {
		return nil, false, false, err
	}
	if !paymentrequest.PaidShareSupersedes(share.Paid, paid) {
		return r.Clone(), false, false, nil
	}
	if paid.GetManual() != nil && !r.IsOpenAt(now) {
		return nil, false, false, paymentrequest.ErrNotOpen
	}

	share.Paid = proto.Clone(paid).(*paymentrequestpb.Paid)
	r.Version++
	completed := r.Status == paymentrequest.StatusOpen && r.AllSharesPaid()
	if completed {
		closedAt := paid.PaidAt.AsTime()
		r.Status = paymentrequest.StatusCompleted
		r.ClosedAt = &closedAt
		delete(m.queue, key)
	}
	return r.Clone(), true, completed, nil
}

func (m *memory) Close(_ context.Context, id *paymentrequestpb.PaymentRequestId, now time.Time) (*paymentrequest.PaymentRequest, bool, error) {
	m.Lock()
	defer m.Unlock()

	key := string(id.Value)
	r, ok := m.requests[key]
	if !ok {
		return nil, false, paymentrequest.ErrNotFound
	}
	if !r.IsOpenAt(now) {
		return r.Clone(), false, nil
	}
	r.Status = paymentrequest.StatusClosed
	r.ClosedAt = &now
	r.Version++
	delete(m.queue, key)
	return r.Clone(), true, nil
}

func (m *memory) GetDueForExpiry(_ context.Context, asOf time.Time, limit int) ([]*paymentrequest.ExpiryTask, error) {
	m.Lock()
	defer m.Unlock()

	var out []*paymentrequest.ExpiryTask
	for key, q := range m.queue {
		if q.dueAt.After(asOf) {
			continue
		}
		out = append(out, &paymentrequest.ExpiryTask{
			ID:            &paymentrequestpb.PaymentRequestId{Value: []byte(key)},
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

func (m *memory) GetExpiryQueueStats(_ context.Context, asOf time.Time) (*paymentrequest.ExpiryQueueStats, error) {
	m.Lock()
	defer m.Unlock()

	stats := &paymentrequest.ExpiryQueueStats{}
	for _, q := range m.queue {
		if q.dueAt.After(asOf) {
			continue
		}
		if stats.DueDepth < paymentrequest.MaxExpiryQueueStatsDepth {
			stats.DueDepth++
		}
		if stats.OldestDueAt.IsZero() || q.dueAt.Before(stats.OldestDueAt) {
			stats.OldestDueAt = q.dueAt
		}
	}
	return stats, nil
}

func (m *memory) ClaimForExpiry(_ context.Context, id *paymentrequestpb.PaymentRequestId, asOf, until time.Time) (bool, error) {
	m.Lock()
	defer m.Unlock()

	q, ok := m.queue[string(id.Value)]
	if !ok || q.dueAt.After(asOf) {
		return false, nil
	}
	q.dueAt = until
	return true, nil
}

func (m *memory) DelayExpiry(_ context.Context, id *paymentrequestpb.PaymentRequestId, nextAttemptAt time.Time) error {
	m.Lock()
	defer m.Unlock()

	q, ok := m.queue[string(id.Value)]
	if !ok {
		return nil
	}
	q.dueAt = nextAttemptAt
	q.attempts++
	return nil
}

func (m *memory) Expire(_ context.Context, id *paymentrequestpb.PaymentRequestId) (bool, error) {
	m.Lock()
	defer m.Unlock()

	key := string(id.Value)
	r, ok := m.requests[key]
	if !ok {
		return false, paymentrequest.ErrNotFound
	}
	delete(m.queue, key)
	if r.Status.IsFinal() {
		return false, nil
	}
	endsAt, ok := r.EndsAt()
	if !ok {
		// A request with no ends_at never expires.
		return false, nil
	}
	r.Status = paymentrequest.StatusExpired
	r.ClosedAt = &endsAt
	return true, nil
}

func (m *memory) GetOrCreatePayerIntent(_ context.Context, id *paymentrequestpb.PaymentRequestId, userID *commonpb.UserId, candidate []byte) (*paymentrequest.PayerIntent, error) {
	if err := paymentrequest.ValidatePayerIntentCandidate(candidate); err != nil {
		return nil, err
	}

	m.Lock()
	defer m.Unlock()

	key := string(id.Value)
	r, ok := m.requests[key]
	switch {
	case !ok:
		return nil, paymentrequest.ErrNotFound
	case !r.IsCollection():
		return nil, paymentrequest.ErrNotACollection
	case proto.Equal(r.Creator, userID):
		return nil, paymentrequest.ErrNotAPayer
	}

	payers, ok := m.payers[key]
	if !ok {
		payers = make(map[string]*paymentrequest.PayerIntent)
		m.payers[key] = payers
	}
	record, ok := payers[string(userID.Value)]
	if !ok {
		record = &paymentrequest.PayerIntent{IntentID: append([]byte(nil), candidate...)}
		payers[string(userID.Value)] = record
	}
	return clonePayerIntent(record), nil
}

func (m *memory) GetPayerIntent(_ context.Context, id *paymentrequestpb.PaymentRequestId, userID *commonpb.UserId) (*paymentrequest.PayerIntent, error) {
	m.Lock()
	defer m.Unlock()

	record, ok := m.payers[string(id.Value)][string(userID.GetValue())]
	if !ok {
		return nil, paymentrequest.ErrNotFound
	}
	return clonePayerIntent(record), nil
}

func (m *memory) GetCollectionPayer(_ context.Context, id *paymentrequestpb.PaymentRequestId, userID *commonpb.UserId) (*paymentrequestpb.Payer, error) {
	m.Lock()
	defer m.Unlock()

	r, ok := m.requests[string(id.Value)]
	if !ok {
		return nil, paymentrequest.ErrNotFound
	}
	record, ok := m.payers[string(id.Value)][string(userID.GetValue())]
	if !ok || record.Paid == nil {
		return nil, paymentrequest.ErrNotFound
	}
	return r.ContributionPayer(userID, record.Paid), nil
}

func (m *memory) GetCollectionPayersPage(_ context.Context, id *paymentrequestpb.PaymentRequestId, cursor *paymentrequest.PayerCursor, limit int) ([]*paymentrequestpb.Payer, error) {
	m.Lock()
	defer m.Unlock()

	r, ok := m.requests[string(id.Value)]
	if !ok {
		return nil, nil
	}

	var payers []*paymentrequestpb.Payer
	for userID, record := range m.payers[string(id.Value)] {
		if record.Paid == nil {
			continue
		}
		payer := r.ContributionPayer(&commonpb.UserId{Value: []byte(userID)}, record.Paid)
		if cursor != nil && !paymentrequest.PaidBefore(cursor, paymentrequest.PaidPayerCursor(payer)) {
			continue
		}
		payers = append(payers, payer)
	}
	sort.Slice(payers, func(i, j int) bool {
		return paymentrequest.PaidBefore(paymentrequest.PaidPayerCursor(payers[i]), paymentrequest.PaidPayerCursor(payers[j]))
	})
	if limit > 0 && len(payers) > limit {
		payers = payers[:limit]
	}
	return payers, nil
}

func (m *memory) RecordContribution(_ context.Context, id *paymentrequestpb.PaymentRequestId, userID *commonpb.UserId, paid *paymentrequestpb.Paid, _ time.Time) (*paymentrequest.PaymentRequest, bool, error) {
	if err := paymentrequest.ValidateContribution(paid); err != nil {
		return nil, false, err
	}

	m.Lock()
	defer m.Unlock()

	key := string(id.Value)
	r, ok := m.requests[key]
	switch {
	case !ok:
		return nil, false, paymentrequest.ErrNotFound
	case !r.IsCollection():
		return nil, false, paymentrequest.ErrNotACollection
	}
	record, ok := m.payers[key][string(userID.GetValue())]
	if !ok || !bytes.Equal(record.IntentID, paid.GetIntent().GetValue()) {
		return nil, false, paymentrequest.ErrWrongIntent
	}
	if record.Paid != nil {
		// A member contributes once, with their one intent: a repeat.
		return r.Clone(), false, nil
	}

	record.Paid = proto.Clone(paid).(*paymentrequestpb.Paid)
	r.PayerCount++
	r.Version++
	return r.Clone(), true, nil
}

func clonePayerIntent(record *paymentrequest.PayerIntent) *paymentrequest.PayerIntent {
	clone := &paymentrequest.PayerIntent{IntentID: append([]byte(nil), record.IntentID...)}
	if record.Paid != nil {
		clone.Paid = proto.Clone(record.Paid).(*paymentrequestpb.Paid)
	}
	return clone
}
