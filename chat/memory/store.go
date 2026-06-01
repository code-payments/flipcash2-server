package memory

import (
	"bytes"
	"context"
	"sort"
	"sync"
	"time"

	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"

	"github.com/code-payments/flipcash2-server/chat"
	"github.com/code-payments/flipcash2-server/database"
)

type memory struct {
	sync.Mutex

	chats map[string]*chat.Chat // keyed by chat ID
}

// NewInMemory returns an in-memory chat.Store, for tests.
func NewInMemory() chat.Store {
	return &memory{
		chats: make(map[string]*chat.Chat),
	}
}

func (m *memory) reset() {
	m.Lock()
	defer m.Unlock()

	m.chats = make(map[string]*chat.Chat)
}

func (m *memory) PutChat(_ context.Context, c *chat.Chat) error {
	m.Lock()
	defer m.Unlock()

	key := string(c.ID.Value)
	if _, ok := m.chats[key]; ok {
		return chat.ErrChatExists
	}
	m.chats[key] = c.Clone()
	return nil
}

func (m *memory) GetChatByID(_ context.Context, chatID *commonpb.ChatId) (*chat.Chat, error) {
	m.Lock()
	defer m.Unlock()

	c, ok := m.chats[string(chatID.Value)]
	if !ok {
		return nil, chat.ErrChatNotFound
	}
	return c.Clone(), nil
}

func (m *memory) GetDmsForUserByLastActivity(_ context.Context, userID *commonpb.UserId, opts ...database.QueryOption) ([]*chat.Chat, error) {
	q := database.ApplyQueryOptions(opts...)

	m.Lock()
	defer m.Unlock()

	// Collect the chats this user is a member of.
	var chats []*chat.Chat
	for _, c := range m.chats {
		if c.HasMember(userID) {
			chats = append(chats, c.Clone())
		}
	}

	// Order by (last_activity, chat_id) ascending, then reverse for descending.
	sort.Slice(chats, func(i, j int) bool {
		return lessByActivity(chats[i], chats[j])
	})
	if q.Order == commonpb.QueryOptions_DESC {
		reverse(chats)
	}

	// Resolve the paging cursor: the token value is the chat ID of the last
	// chat from the previous page, so resume strictly after it. A token that
	// does not match any chat in the user's list yields an empty page.
	start := 0
	if q.PagingToken != nil {
		found := false
		for idx, c := range chats {
			if bytes.Equal(c.ID.Value, q.PagingToken.Value) {
				start = idx + 1
				found = true
				break
			}
		}
		if !found {
			return nil, nil
		}
	}

	end := len(chats)
	if q.Limit > 0 && start+q.Limit < end {
		end = start + q.Limit
	}
	if start >= end {
		return nil, nil
	}
	return chats[start:end], nil
}

func (m *memory) GetMembers(_ context.Context, chatID *commonpb.ChatId) ([]*commonpb.UserId, error) {
	m.Lock()
	defer m.Unlock()

	c, ok := m.chats[string(chatID.Value)]
	if !ok {
		return nil, chat.ErrChatNotFound
	}
	members := make([]*commonpb.UserId, len(c.Members))
	for i, member := range c.Members {
		members[i] = &commonpb.UserId{Value: append([]byte(nil), member.Value...)}
	}
	return members, nil
}

func (m *memory) IsMember(_ context.Context, chatID *commonpb.ChatId, userID *commonpb.UserId) (bool, error) {
	m.Lock()
	defer m.Unlock()

	c, ok := m.chats[string(chatID.Value)]
	if !ok {
		return false, nil
	}
	return c.HasMember(userID), nil
}

func (m *memory) AdvanceLastActivity(_ context.Context, chatID *commonpb.ChatId, ts time.Time) error {
	m.Lock()
	defer m.Unlock()

	c, ok := m.chats[string(chatID.Value)]
	if !ok {
		return chat.ErrChatNotFound
	}
	if ts.After(c.LastActivity) {
		c.LastActivity = ts
	}
	return nil
}

// lessByActivity orders chats by last_activity ascending, breaking ties by chat
// ID so the ordering is total and pagination is stable.
func lessByActivity(a, b *chat.Chat) bool {
	if !a.LastActivity.Equal(b.LastActivity) {
		return a.LastActivity.Before(b.LastActivity)
	}
	return bytes.Compare(a.ID.Value, b.ID.Value) < 0
}

func reverse(chats []*chat.Chat) {
	for i, j := 0, len(chats)-1; i < j; i, j = i+1, j-1 {
		chats[i], chats[j] = chats[j], chats[i]
	}
}
