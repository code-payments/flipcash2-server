package memory

import (
	"bytes"
	"context"
	"fmt"
	"sort"
	"sync"
	"time"

	chatpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/chat/v1"
	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"
	messagingpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/messaging/v1"

	"github.com/code-payments/flipcash2-server/chat"
)

type memory struct {
	sync.Mutex

	chats map[string]*chat.Chat // keyed by chat ID

	// groupMembers holds group chat membership, keyed by chat ID then user ID.
	// The value is the member's joined state: a removed member is tombstoned as
	// false, mirroring the persistent stores, rather than deleted.
	groupMembers map[string]map[string]bool
}

// NewInMemory returns an in-memory chat.Store, for tests.
func NewInMemory() chat.Store {
	return &memory{
		chats:        make(map[string]*chat.Chat),
		groupMembers: make(map[string]map[string]bool),
	}
}

func (m *memory) reset() {
	m.Lock()
	defer m.Unlock()

	m.chats = make(map[string]*chat.Chat)
	m.groupMembers = make(map[string]map[string]bool)
}

func (m *memory) PutChat(_ context.Context, c *chat.Chat) error {
	if chat.IsGroupChatID(c.ID) != (c.Type == chatpb.ChatType_GROUP) {
		return fmt.Errorf("chat id length does not match chat type")
	}

	// The distinct member set, which duplicates collapse into. Built before the
	// existence check so a malformed request is rejected on its own terms
	// regardless of whether the chat happens to exist, matching the persistent
	// stores, which validate before they attempt the write.
	var groupMembers map[string]bool
	if chat.IsGroupChatID(c.ID) {
		groupMembers = make(map[string]bool, len(c.Members))
		for _, member := range c.Members {
			groupMembers[string(member.Value)] = true
		}
		if len(groupMembers) == 0 {
			return chat.ErrNoMembers
		}
		if len(groupMembers) > chat.MaxGroupChatCreationMembers {
			return chat.ErrTooManyMembers
		}
	} else if len(c.Members) == 0 {
		return chat.ErrNoMembers
	}

	m.Lock()
	defer m.Unlock()

	key := string(c.ID.Value)
	if _, ok := m.chats[key]; ok {
		return chat.ErrChatExists
	}

	// Group membership lives in its own records; the canonical chat holds no
	// member list. DM membership is stored inline and immutable.
	if chat.IsGroupChatID(c.ID) {
		stored := c.Clone()
		stored.Members = nil
		m.chats[key] = stored
		m.groupMembers[key] = groupMembers
		return nil
	}

	m.chats[key] = c.Clone()
	return nil
}

func (m *memory) AddGroupMembers(_ context.Context, chatID *commonpb.ChatId, userIDs []*commonpb.UserId) error {
	if !chat.IsGroupChatID(chatID) {
		return fmt.Errorf("not a group chat id")
	}

	m.Lock()
	defer m.Unlock()

	key := string(chatID.Value)
	if _, ok := m.chats[key]; !ok {
		return chat.ErrChatNotFound
	}
	members := m.groupMembers[key]
	if members == nil {
		members = make(map[string]bool)
		m.groupMembers[key] = members
	}
	for _, userID := range userIDs {
		members[string(userID.Value)] = true
	}
	return nil
}

func (m *memory) RemoveGroupMember(_ context.Context, chatID *commonpb.ChatId, userID *commonpb.UserId) error {
	if !chat.IsGroupChatID(chatID) {
		return fmt.Errorf("not a group chat id")
	}

	m.Lock()
	defer m.Unlock()

	members, ok := m.groupMembers[string(chatID.Value)]
	if !ok {
		return nil
	}
	if _, ok := members[string(userID.Value)]; ok {
		members[string(userID.Value)] = false
	}
	return nil
}

func (m *memory) GetChatByID(_ context.Context, chatID *commonpb.ChatId) (*chat.Chat, error) {
	m.Lock()
	defer m.Unlock()

	c, ok := m.chats[string(chatID.Value)]
	if !ok {
		return nil, chat.ErrChatNotFound
	}
	// Only the canonical record: a group's membership lives in its own records
	// and is stored with Members nil, so the clone is already member-free.
	return c.Clone(), nil
}

func (m *memory) GetDmFeedPage(_ context.Context, userID *commonpb.UserId, chatType chatpb.ChatType, snapshot time.Time, cursor *chat.DmFeedCursor, limit int) ([]*chat.Chat, error) {
	m.Lock()
	defer m.Unlock()

	// Collect the user's chats of the requested type within the snapshot window
	// (last_activity at or before the watermark). A chat that became active
	// after the snapshot has moved above the watermark and is excluded from the
	// read.
	var chats []*chat.Chat
	for _, c := range m.chats {
		if c.Type == chatType && hasInlineMember(c, userID) && !c.LastActivity.After(snapshot) {
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
	if chat.IsGroupChatID(chatID) {
		return m.joinedGroupMembersLocked(chatID), nil
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

	if _, ok := m.chats[string(chatID.Value)]; !ok {
		return false, nil
	}
	if chat.IsGroupChatID(chatID) {
		return m.groupMembers[string(chatID.Value)][string(userID.Value)], nil
	}
	return hasInlineMember(m.chats[string(chatID.Value)], userID), nil
}

// hasInlineMember reports whether userID is in a chat's inline member list — a
// DM's fixed participants, carried on the canonical record. It is only ever
// consulted on a DM path: a group's members live in groupMembers and its
// canonical record's list is always empty, so a group would answer false here
// for every one of its members.
func hasInlineMember(c *chat.Chat, userID *commonpb.UserId) bool {
	for _, m := range c.Members {
		if bytes.Equal(m.Value, userID.Value) {
			return true
		}
	}
	return false
}

func (m *memory) joinedGroupMembersLocked(chatID *commonpb.ChatId) []*commonpb.UserId {
	members := make([]*commonpb.UserId, 0)
	for userKey, joined := range m.groupMembers[string(chatID.Value)] {
		if joined {
			members = append(members, &commonpb.UserId{Value: []byte(userKey)})
		}
	}
	return members
}

func (m *memory) AdvanceLastMessage(_ context.Context, chatID *commonpb.ChatId, messageID *messagingpb.MessageId, ts time.Time) (bool, []*commonpb.UserId, error) {
	m.Lock()
	defer m.Unlock()

	c, ok := m.chats[string(chatID.Value)]
	if !ok {
		return false, nil, chat.ErrChatNotFound
	}
	// Members are returned regardless of whether the activity advances.
	members := make([]*commonpb.UserId, len(c.Members))
	for i, member := range c.Members {
		members[i] = &commonpb.UserId{Value: append([]byte(nil), member.Value...)}
	}
	if ts.After(c.LastActivity) {
		c.LastActivity = ts
		c.LastMessageID = &messagingpb.MessageId{Value: messageID.Value}
		return true, members, nil
	}
	return false, members, nil
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
