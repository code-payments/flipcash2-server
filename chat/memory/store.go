package memory

import (
	"bytes"
	"context"
	"fmt"
	"sort"
	"sync"
	"time"

	blobpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/blob/v1"
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

	// groupVersions is each group's roster version, keyed by chat ID: the
	// number of membership transitions it has seen. Absent reads as zero.
	groupVersions map[string]uint64

	// memberVersions stamps each membership record with the roster version of
	// the transition that last moved it, keyed by chat ID then user ID, as the
	// persistent stores do. A record written at creation is unstamped and reads
	// as zero.
	memberVersions map[string]map[string]uint64

	// viewerStates holds each user's state per chat, keyed by user ID then
	// chat ID, mirroring the persistent layout (see chat.UserStateStore).
	viewerStates map[string]map[string]*chat.ViewerState

	// mutedCounts is each chat's count of records with a mute recorded, keyed
	// by chat ID, moved as the persistent stores move theirs (see
	// chat.UserStateStore.GetMutedCount). Absent reads as zero.
	mutedCounts map[string]uint64
}

// NewInMemory returns an in-memory chat.Store, for tests. The value also
// implements chat.UserStateStore.
func NewInMemory() chat.Store {
	return &memory{
		chats:          make(map[string]*chat.Chat),
		groupMembers:   make(map[string]map[string]bool),
		groupVersions:  make(map[string]uint64),
		memberVersions: make(map[string]map[string]uint64),
		viewerStates:   make(map[string]map[string]*chat.ViewerState),
		mutedCounts:    make(map[string]uint64),
	}
}

func (m *memory) reset() {
	m.Lock()
	defer m.Unlock()

	m.chats = make(map[string]*chat.Chat)
	m.groupMembers = make(map[string]map[string]bool)
	m.groupVersions = make(map[string]uint64)
	m.memberVersions = make(map[string]map[string]uint64)
	m.viewerStates = make(map[string]map[string]*chat.ViewerState)
	m.mutedCounts = make(map[string]uint64)
}

// stampMemberLocked records the group's current roster version on a member's
// record, after a transition has advanced it.
func (m *memory) stampMemberLocked(chatKey, userKey string) {
	stamps := m.memberVersions[chatKey]
	if stamps == nil {
		stamps = make(map[string]uint64)
		m.memberVersions[chatKey] = stamps
	}
	stamps[userKey] = m.groupVersions[chatKey]
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
	// member list and no roster summary. DM membership is stored inline and
	// immutable, so its summary is fixed here.
	stored := c.Clone()
	if chat.IsGroupChatID(c.ID) {
		stored.Members = nil
		stored.RosterSummary = chat.RosterSummary{}
		m.chats[key] = stored
		m.groupMembers[key] = groupMembers
		return nil
	}

	stored.RosterSummary = chat.RosterSummary{MemberCount: uint64(len(stored.Members))}
	m.chats[key] = stored
	return nil
}

func (m *memory) AddGroupMembers(_ context.Context, chatID *commonpb.ChatId, userIDs []*commonpb.UserId) (bool, chat.RosterSummary, error) {
	if !chat.IsGroupChatID(chatID) {
		return false, chat.RosterSummary{}, fmt.Errorf("not a group chat id")
	}

	m.Lock()
	defer m.Unlock()

	key := string(chatID.Value)
	if _, ok := m.chats[key]; !ok {
		return false, chat.RosterSummary{}, chat.ErrChatNotFound
	}
	members := m.groupMembers[key]
	if members == nil {
		members = make(map[string]bool)
		m.groupMembers[key] = members
	}
	changed := false
	for _, userID := range userIDs {
		if members[string(userID.Value)] {
			continue // Already joined: no transition.
		}
		members[string(userID.Value)] = true
		m.groupVersions[key]++
		m.stampMemberLocked(key, string(userID.Value))
		changed = true
	}
	return changed, m.rosterSummaryLocked(chatID), nil
}

func (m *memory) RemoveGroupMember(_ context.Context, chatID *commonpb.ChatId, userID *commonpb.UserId) (bool, chat.RosterSummary, error) {
	if !chat.IsGroupChatID(chatID) {
		return false, chat.RosterSummary{}, fmt.Errorf("not a group chat id")
	}

	m.Lock()
	defer m.Unlock()

	key := string(chatID.Value)
	if _, ok := m.chats[key]; !ok {
		return false, chat.RosterSummary{}, chat.ErrChatNotFound
	}
	members := m.groupMembers[key]
	if !members[string(userID.Value)] {
		return false, m.rosterSummaryLocked(chatID), nil // Not joined: no transition.
	}
	members[string(userID.Value)] = false
	m.groupVersions[key]++
	m.stampMemberLocked(key, string(userID.Value))
	return true, m.rosterSummaryLocked(chatID), nil
}

func (m *memory) SetGroupPicture(_ context.Context, chatID *commonpb.ChatId, blobID *blobpb.BlobId) error {
	if !chat.IsGroupChatID(chatID) {
		return fmt.Errorf("not a group chat id")
	}

	m.Lock()
	defer m.Unlock()

	c, ok := m.chats[string(chatID.Value)]
	if !ok {
		return chat.ErrChatNotFound
	}
	if blobID == nil {
		c.PictureBlobID = nil
		return nil
	}
	c.PictureBlobID = &blobpb.BlobId{Value: append([]byte(nil), blobID.Value...)}
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
	// and is stored with Members nil and a zero RosterSummary, so the clone is
	// already member-free.
	return c.Clone(), nil
}

func (m *memory) GetGroupRosterSummary(_ context.Context, chatID *commonpb.ChatId) (chat.RosterSummary, error) {
	if !chat.IsGroupChatID(chatID) {
		return chat.RosterSummary{}, fmt.Errorf("not a group chat id")
	}

	m.Lock()
	defer m.Unlock()

	if _, ok := m.chats[string(chatID.Value)]; !ok {
		return chat.RosterSummary{}, chat.ErrChatNotFound
	}
	return m.rosterSummaryLocked(chatID), nil
}

func (m *memory) GetGroupRosterSummaries(_ context.Context, chatIDs []*commonpb.ChatId) (map[string]chat.RosterSummary, error) {
	for _, chatID := range chatIDs {
		if !chat.IsGroupChatID(chatID) {
			return nil, fmt.Errorf("not a group chat id")
		}
	}

	m.Lock()
	defer m.Unlock()

	out := make(map[string]chat.RosterSummary, len(chatIDs))
	for _, chatID := range chatIDs {
		if _, ok := m.chats[string(chatID.Value)]; ok {
			out[string(chatID.Value)] = m.rosterSummaryLocked(chatID)
		}
	}
	return out, nil
}

func (m *memory) GetGroupRules(_ context.Context, chatID *commonpb.ChatId) (*chatpb.Rules, error) {
	if !chat.IsGroupChatID(chatID) {
		return nil, fmt.Errorf("not a group chat id")
	}

	m.Lock()
	defer m.Unlock()

	c, ok := m.chats[string(chatID.Value)]
	if !ok {
		return nil, chat.ErrChatNotFound
	}
	return c.Rules(), nil
}

// rosterSummaryLocked is a group's summary: the joined count from its records
// and the version that its transitions have advanced.
func (m *memory) rosterSummaryLocked(chatID *commonpb.ChatId) chat.RosterSummary {
	return chat.RosterSummary{
		MemberCount: uint64(len(m.joinedGroupMembersLocked(chatID))),
		Version:     m.groupVersions[string(chatID.Value)],
	}
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

func (m *memory) GetGroupMembershipsForUser(_ context.Context, userID *commonpb.UserId) ([]chat.GroupMembership, error) {
	m.Lock()
	defer m.Unlock()

	memberships := make([]chat.GroupMembership, 0)
	for chatKey, members := range m.groupMembers {
		// A departed member is a false entry, kept as the persistent stores
		// keep a tombstone; a user never in the group has no entry.
		joined, ok := members[string(userID.Value)]
		if !ok {
			continue
		}
		memberships = append(memberships, chat.GroupMembership{
			ChatID:  &commonpb.ChatId{Value: []byte(chatKey)},
			Joined:  joined,
			Version: m.memberVersions[chatKey][string(userID.Value)],
		})
	}
	return memberships, nil
}

func (m *memory) GetGroupChatsForUser(_ context.Context, userID *commonpb.UserId) ([]*chat.Chat, error) {
	m.Lock()
	defer m.Unlock()

	chats := make([]*chat.Chat, 0)
	for chatKey, members := range m.groupMembers {
		if members[string(userID.Value)] {
			chats = append(chats, m.chats[chatKey].Clone())
		}
	}
	return chats, nil
}

func (m *memory) GetGroupChatsForUserByIDs(_ context.Context, userID *commonpb.UserId, chatIDs []*commonpb.ChatId) ([]*chat.Chat, error) {
	for _, chatID := range chatIDs {
		if !chat.IsGroupChatID(chatID) {
			return nil, fmt.Errorf("not a group chat id")
		}
	}

	m.Lock()
	defer m.Unlock()

	chats := make([]*chat.Chat, 0, len(chatIDs))
	seen := make(map[string]struct{}, len(chatIDs))
	for _, chatID := range chatIDs {
		key := string(chatID.Value)
		if _, dup := seen[key]; dup {
			continue
		}
		seen[key] = struct{}{}
		// Membership first, then the record: a chat that exists but the user is
		// not joined to is as absent as one that does not exist.
		if !m.groupMembers[key][string(userID.Value)] {
			continue
		}
		if c, ok := m.chats[key]; ok {
			chats = append(chats, c.Clone())
		}
	}
	return chats, nil
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

func (m *memory) SetMute(_ context.Context, chatID *commonpb.ChatId, userID *commonpb.UserId, mute chat.Mute) (chat.ViewerState, bool, error) {
	mute, err := mute.Normalize()
	if err != nil {
		return chat.ViewerState{}, false, err
	}

	m.Lock()
	defer m.Unlock()

	byChat := m.viewerStates[string(userID.Value)]
	if byChat == nil {
		byChat = make(map[string]*chat.ViewerState)
		m.viewerStates[string(userID.Value)] = byChat
	}
	state := byChat[string(chatID.Value)]
	if state == nil {
		state = &chat.ViewerState{}
		byChat[string(chatID.Value)] = state
	}

	// The mute already recorded: the no-op the contract promises.
	if state.Mute != nil && *state.Mute == mute {
		return state.Clone(), false, nil
	}
	// A first mute is counted; a replaced one is not.
	if state.Mute == nil {
		m.mutedCounts[string(chatID.Value)]++
	}
	state.Mute = &mute
	state.Version++
	return state.Clone(), true, nil
}

func (m *memory) ClearMute(_ context.Context, chatID *commonpb.ChatId, userID *commonpb.UserId) (chat.ViewerState, bool, error) {
	m.Lock()
	defer m.Unlock()

	// No record, or a record with no mute: nothing to clear, and nothing is
	// created to say so.
	state := m.viewerStates[string(userID.Value)][string(chatID.Value)]
	if state == nil {
		return chat.ViewerState{}, false, nil
	}
	if state.Mute == nil {
		return state.Clone(), false, nil
	}
	m.mutedCounts[string(chatID.Value)]--
	state.Mute = nil
	state.Version++
	return state.Clone(), true, nil
}

func (m *memory) GetViewerStates(_ context.Context, userID *commonpb.UserId, chatIDs []*commonpb.ChatId) (map[string]chat.ViewerState, error) {
	m.Lock()
	defer m.Unlock()

	out := make(map[string]chat.ViewerState)
	byChat := m.viewerStates[string(userID.Value)]
	for _, chatID := range chatIDs {
		if state, ok := byChat[string(chatID.Value)]; ok {
			out[string(chatID.Value)] = state.Clone()
		}
	}
	return out, nil
}

func (m *memory) GetMutedUsers(_ context.Context, chatID *commonpb.ChatId, now time.Time, limit int) ([]*commonpb.UserId, error) {
	m.Lock()
	defer m.Unlock()

	users := make([]*commonpb.UserId, 0)
	for user, byChat := range m.viewerStates {
		if limit > 0 && len(users) >= limit {
			break
		}
		state := byChat[string(chatID.Value)]
		if state == nil || state.ActiveMute(now) == nil {
			continue
		}
		users = append(users, &commonpb.UserId{Value: []byte(user)})
	}
	return users, nil
}

func (m *memory) GetMutedUsersInOrder(_ context.Context, chatID *commonpb.ChatId, now time.Time, after *commonpb.UserId, limit int) (chat.MutedUsersPage, error) {
	m.Lock()
	defer m.Unlock()

	users := make([]*commonpb.UserId, 0)
	for user, byChat := range m.viewerStates {
		if after != nil && bytes.Compare([]byte(user), after.Value) <= 0 {
			continue
		}
		state := byChat[string(chatID.Value)]
		if state == nil || state.ActiveMute(now) == nil {
			continue
		}
		users = append(users, &commonpb.UserId{Value: []byte(user)})
	}
	sort.Slice(users, func(i, j int) bool { return bytes.Compare(users[i].Value, users[j].Value) < 0 })

	page := chat.MutedUsersPage{Users: users}
	if limit > 0 && len(users) >= limit {
		page.Users = users[:limit]
		page.Next = users[limit-1]
	}
	return page, nil
}

func (m *memory) GetMutedCount(_ context.Context, chatID *commonpb.ChatId) (uint64, error) {
	m.Lock()
	defer m.Unlock()

	return m.mutedCounts[string(chatID.Value)], nil
}
