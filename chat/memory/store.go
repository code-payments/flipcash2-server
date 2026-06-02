package memory

import (
	"bytes"
	"context"
	"sort"
	"sync"
	"time"

	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"
	messagingpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/messaging/v1"

	"github.com/code-payments/flipcash2-server/chat"
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

func (m *memory) GetDmFeedPage(_ context.Context, userID *commonpb.UserId, snapshot time.Time, cursor *chat.DmFeedCursor, limit int) ([]*chat.Chat, error) {
	m.Lock()
	defer m.Unlock()

	// Collect the user's chats within the snapshot window (last_activity at or
	// before the watermark). A chat that became active after the snapshot has
	// moved above the watermark and is excluded from the read.
	var chats []*chat.Chat
	for _, c := range m.chats {
		if c.HasMember(userID) && !c.LastActivity.After(snapshot) {
			chats = append(chats, c.Clone())
		}
	}

	// Order by (last_activity, chat_id) descending: most recent first.
	sort.Slice(chats, func(i, j int) bool {
		return lessByActivity(chats[j], chats[i])
	})

	// Resume strictly after the cursor. In descending order every chat past the
	// cursor position is strictly below it, so advance to the first such chat.
	start := 0
	if cursor != nil {
		for start < len(chats) && !afterCursorDesc(chats[start], cursor) {
			start++
		}
	}

	end := len(chats)
	if limit > 0 && start+limit < end {
		end = start + limit
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

func (m *memory) AdvanceLastMessage(_ context.Context, chatID *commonpb.ChatId, messageID *messagingpb.MessageId, ts time.Time) (bool, error) {
	m.Lock()
	defer m.Unlock()

	c, ok := m.chats[string(chatID.Value)]
	if !ok {
		return false, chat.ErrChatNotFound
	}
	if ts.After(c.LastActivity) {
		c.LastActivity = ts
		c.LastMessageID = &messagingpb.MessageId{Value: messageID.Value}
		return true, nil
	}
	return false, nil
}

// lessByActivity orders chats by last_activity ascending, breaking ties by chat
// ID so the ordering is total and pagination is stable.
func lessByActivity(a, b *chat.Chat) bool {
	if !a.LastActivity.Equal(b.LastActivity) {
		return a.LastActivity.Before(b.LastActivity)
	}
	return bytes.Compare(a.ID.Value, b.ID.Value) < 0
}

// afterCursorDesc reports whether c falls strictly after the cursor in the
// feed's descending (last_activity, chat_id) order.
func afterCursorDesc(c *chat.Chat, cursor *chat.DmFeedCursor) bool {
	if !c.LastActivity.Equal(cursor.LastActivity) {
		return c.LastActivity.Before(cursor.LastActivity)
	}
	return bytes.Compare(c.ID.Value, cursor.ChatID.Value) < 0
}
