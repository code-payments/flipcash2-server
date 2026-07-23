package memory

import (
	"bytes"
	"context"
	"sort"
	"sync"
	"time"

	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"

	"github.com/code-payments/flipcash2-server/blocklist"
)

type memory struct {
	sync.Mutex

	// entries maps an owner (blocker) to their blocklist, keyed by the blocked
	// user ID.
	entries map[string]map[string]*blocklist.BlockedUser
}

// NewInMemory returns an in-memory blocklist.Store, for tests.
func NewInMemory() blocklist.Store {
	return &memory{
		entries: make(map[string]map[string]*blocklist.BlockedUser),
	}
}

func (m *memory) reset() {
	m.Lock()
	defer m.Unlock()

	m.entries = make(map[string]map[string]*blocklist.BlockedUser)
}

func (m *memory) Block(_ context.Context, ownerID, blockedID *commonpb.UserId, blockedAt time.Time) (bool, error) {
	m.Lock()
	defer m.Unlock()

	ownerKey := string(ownerID.Value)
	list, ok := m.entries[ownerKey]
	if !ok {
		list = make(map[string]*blocklist.BlockedUser)
		m.entries[ownerKey] = list
	}

	blockedKey := string(blockedID.Value)
	if _, ok := list[blockedKey]; ok {
		// Already blocked: preserve the existing entry (and its blocked_at).
		return false, nil
	}
	list[blockedKey] = &blocklist.BlockedUser{
		UserID:    &commonpb.UserId{Value: append([]byte(nil), blockedID.Value...)},
		BlockedAt: blockedAt,
	}
	return true, nil
}

func (m *memory) Unblock(_ context.Context, ownerID, blockedID *commonpb.UserId) (bool, error) {
	m.Lock()
	defer m.Unlock()

	list, ok := m.entries[string(ownerID.Value)]
	if !ok {
		return false, nil
	}
	blockedKey := string(blockedID.Value)
	if _, ok := list[blockedKey]; !ok {
		return false, nil
	}
	delete(list, blockedKey)
	return true, nil
}

func (m *memory) IsBlocked(_ context.Context, ownerID, blockedID *commonpb.UserId) (bool, error) {
	m.Lock()
	defer m.Unlock()

	list, ok := m.entries[string(ownerID.Value)]
	if !ok {
		return false, nil
	}
	_, ok = list[string(blockedID.Value)]
	return ok, nil
}

func (m *memory) GetBlockedCount(_ context.Context, ownerID *commonpb.UserId) (int, error) {
	m.Lock()
	defer m.Unlock()

	return len(m.entries[string(ownerID.Value)]), nil
}

func (m *memory) GetBlocked(_ context.Context, ownerID *commonpb.UserId, candidateIDs []*commonpb.UserId) (map[string]bool, error) {
	m.Lock()
	defer m.Unlock()

	list := m.entries[string(ownerID.Value)]
	if len(list) == 0 || len(candidateIDs) == 0 {
		return nil, nil
	}
	blocked := make(map[string]bool)
	for _, c := range candidateIDs {
		if _, ok := list[string(c.Value)]; ok {
			blocked[string(c.Value)] = true
		}
	}
	return blocked, nil
}

func (m *memory) GetBlocklistPage(_ context.Context, ownerID *commonpb.UserId, cursor *blocklist.Cursor, limit int) ([]*blocklist.BlockedUser, error) {
	m.Lock()
	defer m.Unlock()

	var entries []*blocklist.BlockedUser
	for _, e := range m.entries[string(ownerID.Value)] {
		entries = append(entries, e.Clone())
	}

	// Order by (blocked_at, user_id) descending: most recently blocked first.
	sort.Slice(entries, func(i, j int) bool {
		return lessByBlockedAt(entries[j], entries[i])
	})

	// Resume strictly after the cursor. In descending order every entry past the
	// cursor position is strictly below it, so advance to the first such entry.
	start := 0
	if cursor != nil {
		for start < len(entries) && !afterCursorDesc(entries[start], cursor) {
			start++
		}
	}

	end := len(entries)
	if limit > 0 && start+limit < end {
		end = start + limit
	}
	if start >= end {
		return nil, nil
	}
	return entries[start:end], nil
}

// lessByBlockedAt orders entries by blocked_at ascending, breaking ties by user
// ID so the ordering is total and pagination is stable.
func lessByBlockedAt(a, b *blocklist.BlockedUser) bool {
	if !a.BlockedAt.Equal(b.BlockedAt) {
		return a.BlockedAt.Before(b.BlockedAt)
	}
	return bytes.Compare(a.UserID.Value, b.UserID.Value) < 0
}

// afterCursorDesc reports whether e falls strictly after the cursor in the
// list's descending (blocked_at, user_id) order.
func afterCursorDesc(e *blocklist.BlockedUser, cursor *blocklist.Cursor) bool {
	if !e.BlockedAt.Equal(cursor.BlockedAt) {
		return e.BlockedAt.Before(cursor.BlockedAt)
	}
	return bytes.Compare(e.UserID.Value, cursor.UserID.Value) < 0
}
