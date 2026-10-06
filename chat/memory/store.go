package memory

import (
	"bytes"
	"context"
	"fmt"
	"slices"
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

	// groupMembers holds group chat membership records, keyed by chat ID then
	// user ID. A removed member is tombstoned (joined false), mirroring the
	// persistent stores, rather than deleted.
	groupMembers map[string]map[string]*memberRecord

	// groupVersions is each group's roster version, keyed by chat ID: the
	// number of membership transitions it has seen. Absent reads as zero.
	groupVersions map[string]uint64

	// viewerStates holds each user's state per chat, keyed by user ID then
	// chat ID, mirroring the persistent layout (see chat.Store).
	viewerStates map[string]map[string]*chat.ViewerState

	// mutedCounts is each chat's count of records with a mute recorded, keyed
	// by chat ID, moved as the persistent stores move theirs (see
	// chat.Store.GetMutedCount). Absent reads as zero.
	mutedCounts map[string]uint64

	// excludedFromFeed is the set of members each DM excludes from the feed,
	// keyed by chat ID then user ID, recorded at creation as the persistent
	// stores record it (see chat.Store.PutChat). Absent for a DM excluding no
	// one.
	excludedFromFeed map[string]map[string]struct{}

	// lastSent is each group's activity records, keyed by chat ID then user
	// ID: the latest recorded send, at the persistent store's millisecond
	// precision (see chat.Store.RecordSend). Records never expire here; the
	// contract lets a reader see one past chat.ActivityRetention.
	lastSent map[string]map[string]time.Time

	// keyEnvelopes holds each user's key envelope per chat, keyed by user ID
	// then chat ID, mirroring the persistent layout (see chat.Store).
	keyEnvelopes map[string]map[string]chat.KeyEnvelope

	// lobbies holds each user's lobby entries, keyed by user ID then chat
	// ID, as when they entered (see chat.LobbyEntry). The counts the
	// persistent stores keep are computed from it.
	lobbies map[string]map[string]time.Time

	// featured holds each user's featured groups, keyed by user ID, as
	// last set (see chat.Store.SetFeaturedGroups). Absent reads as none at
	// version zero.
	featured map[string]chat.FeaturedGroups

	exclusions chat.FeedExclusions
}

// memberRecord is one group membership record, as the persistent stores keep
// it: the joined state, when the member most recently joined (meaningful
// while joined), and the roster version of the transition that last moved the
// record. A record written at creation is unstamped and reads as version zero.
type memberRecord struct {
	joined   bool
	joinedAt time.Time
	version  uint64
}

// NewInMemory returns an in-memory chat.Store, for tests, creating every DM
// with a user in excludedFromFeed excluding them from the feed (see
// chat.FeedExclusions); nil excludes no one.
func NewInMemory(excludedFromFeed []*commonpb.UserId) chat.Store {
	return &memory{
		exclusions:       chat.NewFeedExclusions(excludedFromFeed),
		excludedFromFeed: make(map[string]map[string]struct{}),
		chats:            make(map[string]*chat.Chat),
		groupMembers:     make(map[string]map[string]*memberRecord),
		groupVersions:    make(map[string]uint64),
		viewerStates:     make(map[string]map[string]*chat.ViewerState),
		mutedCounts:      make(map[string]uint64),
		lastSent:         make(map[string]map[string]time.Time),
		keyEnvelopes:     make(map[string]map[string]chat.KeyEnvelope),
		lobbies:          make(map[string]map[string]time.Time),
		featured:         make(map[string]chat.FeaturedGroups),
	}
}

func (m *memory) reset() {
	m.Lock()
	defer m.Unlock()

	m.chats = make(map[string]*chat.Chat)
	m.groupMembers = make(map[string]map[string]*memberRecord)
	m.groupVersions = make(map[string]uint64)
	m.viewerStates = make(map[string]map[string]*chat.ViewerState)
	m.mutedCounts = make(map[string]uint64)
	m.excludedFromFeed = make(map[string]map[string]struct{})
	m.lastSent = make(map[string]map[string]time.Time)
	m.keyEnvelopes = make(map[string]map[string]chat.KeyEnvelope)
	m.lobbies = make(map[string]map[string]time.Time)
	m.featured = make(map[string]chat.FeaturedGroups)
}

// isJoinedLocked reports whether the user's record on the group has them
// joined; a user with no record, or a tombstoned one, is not.
func (m *memory) isJoinedLocked(chatKey, userKey string) bool {
	r := m.groupMembers[chatKey][userKey]
	return r != nil && r.joined
}

func (m *memory) PutChat(_ context.Context, c *chat.Chat) error {
	if chat.IsGroupChatID(c.ID) != (c.Type == chatpb.ChatType_GROUP) {
		return fmt.Errorf("chat id length does not match chat type")
	}

	// The distinct member set, which duplicates collapse into. Built before the
	// existence check so a malformed request is rejected on its own terms
	// regardless of whether the chat happens to exist, matching the persistent
	// stores, which validate before they attempt the write.
	var groupMembers map[string]*memberRecord
	if chat.IsGroupChatID(c.ID) {
		now := time.Now().UTC()
		groupMembers = make(map[string]*memberRecord, len(c.Members))
		for _, member := range c.Members {
			groupMembers[string(member.Value)] = &memberRecord{joined: true, joinedAt: now}
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
	if excluded := m.exclusions.For(c); len(excluded) > 0 {
		set := make(map[string]struct{}, len(excluded))
		for _, userID := range excluded {
			set[string(userID.Value)] = struct{}{}
		}
		m.excludedFromFeed[key] = set
	}
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
		members = make(map[string]*memberRecord)
		m.groupMembers[key] = members
	}
	changed := false
	for _, userID := range userIDs {
		if m.isJoinedLocked(key, string(userID.Value)) {
			continue // Already joined: no transition.
		}
		// A (re)join is a fresh record: a new join time, stamped with the
		// version the transition advances the roster to.
		m.groupVersions[key]++
		members[string(userID.Value)] = &memberRecord{
			joined:   true,
			joinedAt: time.Now().UTC(),
			version:  m.groupVersions[key],
		}
		changed = true
	}
	return changed, m.rosterSummaryLocked(chatID), nil
}

func (m *memory) RemoveGroupMember(_ context.Context, chatID *commonpb.ChatId, userID *commonpb.UserId, discardKeyEnvelope bool) (bool, chat.RosterSummary, error) {
	if !chat.IsGroupChatID(chatID) {
		return false, chat.RosterSummary{}, fmt.Errorf("not a group chat id")
	}

	m.Lock()
	defer m.Unlock()

	key := string(chatID.Value)
	if _, ok := m.chats[key]; !ok {
		return false, chat.RosterSummary{}, chat.ErrChatNotFound
	}
	if !m.isJoinedLocked(key, string(userID.Value)) {
		return false, m.rosterSummaryLocked(chatID), nil // Not joined: no transition.
	}
	// The tombstone keeps the departure's version, as the persistent stores'
	// do; the join time is meaningless once departed and is dropped with it.
	m.groupVersions[key]++
	m.groupMembers[key][string(userID.Value)] = &memberRecord{version: m.groupVersions[key]}
	if discardKeyEnvelope {
		delete(m.keyEnvelopes[string(userID.Value)], key)
	}
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
		c.ProfilePictureBlobID = nil
		return nil
	}
	c.ProfilePictureBlobID = &blobpb.BlobId{Value: append([]byte(nil), blobID.Value...)}
	return nil
}

func (m *memory) EditGroup(_ context.Context, chatID *commonpb.ChatId, edit chat.GroupEdit) error {
	if !chat.IsGroupChatID(chatID) {
		return fmt.Errorf("not a group chat id")
	}
	if edit.IsEmpty() {
		return fmt.Errorf("edit names nothing")
	}

	m.Lock()
	defer m.Unlock()

	c, ok := m.chats[string(chatID.Value)]
	if !ok {
		return chat.ErrChatNotFound
	}
	if edit.Title != nil {
		c.Title = *edit.Title
	}
	if edit.Description != nil {
		c.Description = *edit.Description
	}
	if edit.ProfilePictureBlobID != nil {
		c.ProfilePictureBlobID = &blobpb.BlobId{Value: append([]byte(nil), edit.ProfilePictureBlobID.Value...)}
	}
	if edit.CoverPictureBlobID != nil {
		c.CoverPictureBlobID = &blobpb.BlobId{Value: append([]byte(nil), edit.CoverPictureBlobID.Value...)}
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

func (m *memory) GetGroupRules(_ context.Context, chatID *commonpb.ChatId) (chat.GroupRules, error) {
	if !chat.IsGroupChatID(chatID) {
		return chat.GroupRules{}, fmt.Errorf("not a group chat id")
	}

	m.Lock()
	defer m.Unlock()

	c, ok := m.chats[string(chatID.Value)]
	if !ok {
		return chat.GroupRules{}, chat.ErrChatNotFound
	}
	return c.Clone().GroupRules(), nil
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
	for key, c := range m.chats {
		// A member the DM excludes from the feed never has it listed (see
		// chat.Store.PutChat).
		if _, ok := m.excludedFromFeed[key][string(userID.Value)]; ok {
			continue
		}
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
		return m.isJoinedLocked(string(chatID.Value), string(userID.Value)), nil
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
		r, ok := members[string(userID.Value)]
		if !ok {
			continue
		}
		memberships = append(memberships, chat.GroupMembership{
			ChatID:  &commonpb.ChatId{Value: []byte(chatKey)},
			Joined:  r.joined,
			Version: r.version,
		})
	}
	return memberships, nil
}

func (m *memory) GetGroupChatsForUser(_ context.Context, userID *commonpb.UserId) ([]*chat.Chat, error) {
	m.Lock()
	defer m.Unlock()

	chats := make([]*chat.Chat, 0)
	for chatKey := range m.groupMembers {
		if m.isJoinedLocked(chatKey, string(userID.Value)) {
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
		if !m.isJoinedLocked(key, string(userID.Value)) {
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
	for _, r := range m.joinedGroupRecordsLocked(chatID) {
		members = append(members, r.UserID)
	}
	return members
}

// joinedGroupRecordsLocked is a group's joined members as GroupMember records,
// in no particular order.
func (m *memory) joinedGroupRecordsLocked(chatID *commonpb.ChatId) []chat.GroupMember {
	members := make([]chat.GroupMember, 0)
	for userKey, r := range m.groupMembers[string(chatID.Value)] {
		if r.joined {
			members = append(members, chat.GroupMember{
				UserID:   &commonpb.UserId{Value: []byte(userKey)},
				JoinedAt: r.joinedAt,
				Version:  r.version,
			})
		}
	}
	return members
}

func (m *memory) GetGroupRoster(_ context.Context, chatID *commonpb.ChatId) (chat.RosterSummary, []chat.GroupMember, error) {
	if !chat.IsGroupChatID(chatID) {
		return chat.RosterSummary{}, nil, fmt.Errorf("not a group chat id")
	}

	m.Lock()
	defer m.Unlock()

	if _, ok := m.chats[string(chatID.Value)]; !ok {
		return chat.RosterSummary{}, nil, chat.ErrChatNotFound
	}
	return m.rosterSummaryLocked(chatID), m.joinedGroupRecordsLocked(chatID), nil
}

func (m *memory) GetGroupRosterPage(_ context.Context, chatID *commonpb.ChatId, after *chat.RosterPosition, limit int) ([]chat.GroupMember, error) {
	if !chat.IsGroupChatID(chatID) {
		return nil, fmt.Errorf("not a group chat id")
	}

	m.Lock()
	defer m.Unlock()

	members := make([]chat.GroupMember, 0)
	for _, r := range m.joinedGroupRecordsLocked(chatID) {
		if after != nil && r.Position().Compare(*after) <= 0 {
			continue
		}
		members = append(members, r)
	}
	chat.SortRoster(members)
	if limit > 0 && len(members) > limit {
		members = members[:limit]
	}
	return members, nil
}

func (m *memory) GetGroupMemberRecords(_ context.Context, userID *commonpb.UserId, chatIDs []*commonpb.ChatId) (map[string]chat.GroupMember, error) {
	for _, chatID := range chatIDs {
		if !chat.IsGroupChatID(chatID) {
			return nil, fmt.Errorf("not a group chat id")
		}
	}

	m.Lock()
	defer m.Unlock()

	out := make(map[string]chat.GroupMember, len(chatIDs))
	for _, chatID := range chatIDs {
		key := string(chatID.Value)
		r := m.groupMembers[key][string(userID.Value)]
		if r == nil || !r.joined {
			continue
		}
		out[key] = chat.GroupMember{
			UserID:   &commonpb.UserId{Value: append([]byte(nil), userID.Value...)},
			JoinedAt: r.joinedAt,
			Version:  r.version,
		}
	}
	return out, nil
}

func (m *memory) GetGroupMembersByID(_ context.Context, chatID *commonpb.ChatId, userIDs []*commonpb.UserId) (map[string]chat.GroupMember, error) {
	if !chat.IsGroupChatID(chatID) {
		return nil, fmt.Errorf("not a group chat id")
	}

	m.Lock()
	defer m.Unlock()

	out := make(map[string]chat.GroupMember)
	for _, userID := range userIDs {
		r := m.groupMembers[string(chatID.Value)][string(userID.Value)]
		if r == nil || !r.joined {
			continue
		}
		out[string(userID.Value)] = chat.GroupMember{
			UserID:   &commonpb.UserId{Value: append([]byte(nil), userID.Value...)},
			JoinedAt: r.joinedAt,
			Version:  r.version,
		}
	}
	return out, nil
}

func (m *memory) GetGroupMembersPage(_ context.Context, chatID *commonpb.ChatId, after *commonpb.UserId, limit int) (chat.MembersPage, error) {
	if !chat.IsGroupChatID(chatID) {
		return chat.MembersPage{}, fmt.Errorf("not a group chat id")
	}

	m.Lock()
	defer m.Unlock()

	// The persistent stores' partitions sort by user-ID bytes, which the
	// contract promises and the mute walk shares.
	users := make([]*commonpb.UserId, 0)
	for _, user := range m.joinedGroupMembersLocked(chatID) {
		if after != nil && bytes.Compare(user.Value, after.Value) <= 0 {
			continue
		}
		users = append(users, user)
	}
	sort.Slice(users, func(i, j int) bool { return bytes.Compare(users[i].Value, users[j].Value) < 0 })

	page := chat.MembersPage{Users: users}
	if limit > 0 && len(users) >= limit {
		page.Users = users[:limit]
		page.Next = users[limit-1]
	}
	return page, nil
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

func (m *memory) GetMutedUsersPage(_ context.Context, chatID *commonpb.ChatId, now time.Time, lo, hi *commonpb.UserId) ([]*commonpb.UserId, error) {
	m.Lock()
	defer m.Unlock()

	users := make([]*commonpb.UserId, 0)
	for user, byChat := range m.viewerStates {
		if lo != nil && bytes.Compare([]byte(user), lo.Value) < 0 {
			continue
		}
		if hi != nil && bytes.Compare([]byte(user), hi.Value) > 0 {
			continue
		}
		state := byChat[string(chatID.Value)]
		if state == nil || state.ActiveMute(now) == nil {
			continue
		}
		users = append(users, &commonpb.UserId{Value: []byte(user)})
	}
	sort.Slice(users, func(i, j int) bool { return bytes.Compare(users[i].Value, users[j].Value) < 0 })
	return users, nil
}

func (m *memory) GetMutedCount(_ context.Context, chatID *commonpb.ChatId) (uint64, error) {
	m.Lock()
	defer m.Unlock()

	return m.mutedCounts[string(chatID.Value)], nil
}

func (m *memory) RecordSend(_ context.Context, chatID *commonpb.ChatId, userID *commonpb.UserId, sentAt time.Time) (bool, error) {
	if !chat.IsGroupChatID(chatID) {
		return false, fmt.Errorf("not a group chat id")
	}
	if sentAt.UnixMilli() <= 0 {
		return false, fmt.Errorf("send time %v is not after the epoch", sentAt)
	}
	sentAt = time.UnixMilli(sentAt.UnixMilli()).UTC()

	m.Lock()
	defer m.Unlock()

	chatKey := string(chatID.Value)
	if last, ok := m.lastSent[chatKey][string(userID.Value)]; ok && last.After(sentAt.Add(-chat.ActivityRecordInterval)) {
		return false, nil
	}
	if m.lastSent[chatKey] == nil {
		m.lastSent[chatKey] = make(map[string]time.Time)
	}
	m.lastSent[chatKey][string(userID.Value)] = sentAt
	return true, nil
}

func (m *memory) GetRecentSenders(_ context.Context, chatID *commonpb.ChatId, limit int) ([]chat.RecentSender, error) {
	if !chat.IsGroupChatID(chatID) {
		return nil, fmt.Errorf("not a group chat id")
	}

	m.Lock()
	defer m.Unlock()

	senders := make([]chat.RecentSender, 0, len(m.lastSent[string(chatID.Value)]))
	for user, last := range m.lastSent[string(chatID.Value)] {
		senders = append(senders, chat.RecentSender{
			UserID:     &commonpb.UserId{Value: []byte(user)},
			LastSentAt: last,
		})
	}
	// Ties are in no particular order by contract; break them by user so
	// this store is at least deterministic.
	sort.Slice(senders, func(i, j int) bool {
		if !senders[i].LastSentAt.Equal(senders[j].LastSentAt) {
			return senders[i].LastSentAt.After(senders[j].LastSentAt)
		}
		return bytes.Compare(senders[i].UserID.Value, senders[j].UserID.Value) > 0
	})
	if limit > 0 && len(senders) > limit {
		senders = senders[:limit]
	}
	return senders, nil
}

func (m *memory) GetLastSentAt(_ context.Context, chatID *commonpb.ChatId, userID *commonpb.UserId) (time.Time, bool, error) {
	if !chat.IsGroupChatID(chatID) {
		return time.Time{}, false, fmt.Errorf("not a group chat id")
	}

	m.Lock()
	defer m.Unlock()

	last, ok := m.lastSent[string(chatID.Value)][string(userID.Value)]
	return last, ok, nil
}

func (m *memory) SetKeyEnvelope(_ context.Context, chatID *commonpb.ChatId, userID *commonpb.UserId, envelope chat.KeyEnvelope) (chat.KeyEnvelope, error) {
	if !chat.IsGroupChatID(chatID) {
		return chat.KeyEnvelope{}, fmt.Errorf("not a group chat id")
	}
	if envelope.WrappedBy == nil {
		return chat.KeyEnvelope{}, fmt.Errorf("key envelope has no wrapper")
	}

	m.Lock()
	defer m.Unlock()

	byChat := m.keyEnvelopes[string(userID.Value)]
	if byChat == nil {
		byChat = make(map[string]chat.KeyEnvelope)
		m.keyEnvelopes[string(userID.Value)] = byChat
	}
	// An envelope the user wrapped themself stands.
	if stored, ok := byChat[string(chatID.Value)]; ok && stored.IsWrappedBy(userID) {
		return stored.Clone(), nil
	}
	byChat[string(chatID.Value)] = envelope.Clone()
	return envelope.Clone(), nil
}

func (m *memory) GetKeyEnvelope(_ context.Context, chatID *commonpb.ChatId, userID *commonpb.UserId) (chat.KeyEnvelope, error) {
	m.Lock()
	defer m.Unlock()

	stored, ok := m.keyEnvelopes[string(userID.Value)][string(chatID.Value)]
	if !ok {
		return chat.KeyEnvelope{}, chat.ErrKeyEnvelopeNotFound
	}
	return stored.Clone(), nil
}

func (m *memory) EnterLobby(_ context.Context, chatID *commonpb.ChatId, userID *commonpb.UserId, limits chat.LobbyLimits) (chat.LobbyEntry, bool, error) {
	if !chat.IsGroupChatID(chatID) {
		return chat.LobbyEntry{}, false, fmt.Errorf("not a group chat id")
	}

	m.Lock()
	defer m.Unlock()

	userKey, chatKey := string(userID.Value), string(chatID.Value)
	// Membership first, then an existing entry, then the lobby's cap before
	// the user's, as the persistent stores judge them.
	if m.isJoinedLocked(chatKey, userKey) {
		return chat.LobbyEntry{}, false, chat.ErrAlreadyMember
	}
	if enteredAt, ok := m.lobbies[userKey][chatKey]; ok {
		return chat.LobbyEntry{UserID: cloneUserID(userID), EnteredAt: enteredAt}, false, nil
	}
	if m.lobbySizeLocked(chatKey) >= limits.LobbySize {
		return chat.LobbyEntry{}, false, chat.ErrLobbyFull
	}
	if len(m.lobbies[userKey]) >= limits.LobbiesPerUser {
		return chat.LobbyEntry{}, false, chat.ErrTooManyLobbies
	}

	byChat := m.lobbies[userKey]
	if byChat == nil {
		byChat = make(map[string]time.Time)
		m.lobbies[userKey] = byChat
	}
	enteredAt := time.Now().UTC()
	byChat[chatKey] = enteredAt
	return chat.LobbyEntry{UserID: cloneUserID(userID), EnteredAt: enteredAt}, true, nil
}

func (m *memory) LeaveLobby(_ context.Context, chatID *commonpb.ChatId, userID *commonpb.UserId) (bool, error) {
	if !chat.IsGroupChatID(chatID) {
		return false, fmt.Errorf("not a group chat id")
	}

	m.Lock()
	defer m.Unlock()

	return m.leaveLobbyLocked(chatID, userID), nil
}

func (m *memory) GetLobbyEntries(_ context.Context, userID *commonpb.UserId, chatIDs []*commonpb.ChatId) (map[string]chat.LobbyEntry, error) {
	m.Lock()
	defer m.Unlock()

	out := make(map[string]chat.LobbyEntry)
	byChat := m.lobbies[string(userID.Value)]
	for _, chatID := range chatIDs {
		if enteredAt, ok := byChat[string(chatID.Value)]; ok {
			out[string(chatID.Value)] = chat.LobbyEntry{UserID: cloneUserID(userID), EnteredAt: enteredAt}
		}
	}
	return out, nil
}

func (m *memory) GetLobbyPage(_ context.Context, chatID *commonpb.ChatId, after *chat.LobbyPosition, limit int) ([]chat.LobbyEntry, error) {
	if !chat.IsGroupChatID(chatID) {
		return nil, fmt.Errorf("not a group chat id")
	}

	m.Lock()
	defer m.Unlock()

	chatKey := string(chatID.Value)
	entries := make([]chat.LobbyEntry, 0)
	for userKey, byChat := range m.lobbies {
		if enteredAt, ok := byChat[chatKey]; ok {
			entries = append(entries, chat.LobbyEntry{UserID: &commonpb.UserId{Value: []byte(userKey)}, EnteredAt: enteredAt})
		}
	}
	slices.SortFunc(entries, func(a, b chat.LobbyEntry) int { return a.Position().Compare(b.Position()) })
	if after != nil {
		entries = slices.DeleteFunc(entries, func(e chat.LobbyEntry) bool { return e.Position().Compare(*after) <= 0 })
	}
	if limit > 0 && len(entries) > limit {
		entries = entries[:limit]
	}
	return entries, nil
}

func (m *memory) AdmitFromLobby(_ context.Context, chatID *commonpb.ChatId, userID *commonpb.UserId, envelope chat.KeyEnvelope) (bool, chat.RosterSummary, error) {
	if !chat.IsGroupChatID(chatID) {
		return false, chat.RosterSummary{}, fmt.Errorf("not a group chat id")
	}
	if envelope.WrappedBy == nil {
		return false, chat.RosterSummary{}, fmt.Errorf("key envelope has no wrapper")
	}

	m.Lock()
	defer m.Unlock()

	userKey, chatKey := string(userID.Value), string(chatID.Value)
	if _, ok := m.chats[chatKey]; !ok {
		return false, chat.RosterSummary{}, chat.ErrChatNotFound
	}
	// A member already is the no-op, judged before the lobby as the
	// persistent stores judge it (see chat.Store.AdmitFromLobby).
	if m.isJoinedLocked(chatKey, userKey) {
		return false, m.rosterSummaryLocked(chatID), nil
	}
	if _, ok := m.lobbies[userKey][chatKey]; !ok {
		return false, chat.RosterSummary{}, chat.ErrNotInLobby
	}

	byChat := m.keyEnvelopes[userKey]
	if byChat == nil {
		byChat = make(map[string]chat.KeyEnvelope)
		m.keyEnvelopes[userKey] = byChat
	}
	byChat[chatKey] = envelope.Clone()

	members := m.groupMembers[chatKey]
	if members == nil {
		members = make(map[string]*memberRecord)
		m.groupMembers[chatKey] = members
	}
	m.groupVersions[chatKey]++
	members[userKey] = &memberRecord{
		joined:   true,
		joinedAt: time.Now().UTC(),
		version:  m.groupVersions[chatKey],
	}
	m.leaveLobbyLocked(chatID, userID)
	return true, m.rosterSummaryLocked(chatID), nil
}

// lobbySizeLocked counts the users waiting in chatKey's lobby.
func (m *memory) lobbySizeLocked(chatKey string) int {
	n := 0
	for _, byChat := range m.lobbies {
		if _, ok := byChat[chatKey]; ok {
			n++
		}
	}
	return n
}

// leaveLobbyLocked removes userID's entry in chatID's lobby, reporting
// whether there was one.
func (m *memory) leaveLobbyLocked(chatID *commonpb.ChatId, userID *commonpb.UserId) bool {
	byChat := m.lobbies[string(userID.Value)]
	if _, ok := byChat[string(chatID.Value)]; !ok {
		return false
	}
	delete(byChat, string(chatID.Value))
	if len(byChat) == 0 {
		delete(m.lobbies, string(userID.Value))
	}
	return true
}

func (m *memory) SetFeaturedGroups(_ context.Context, userID *commonpb.UserId, chatIDs []*commonpb.ChatId) (chat.FeaturedGroups, bool, error) {
	if err := chat.ValidateFeaturedGroups(chatIDs); err != nil {
		return chat.FeaturedGroups{}, false, err
	}

	m.Lock()
	defer m.Unlock()

	current := m.featured[string(userID.Value)]
	if current.Equal(chatIDs) {
		return cloneFeaturedGroups(current), false, nil
	}
	next := cloneFeaturedGroups(chat.FeaturedGroups{ChatIDs: chatIDs, Version: current.Version + 1})
	m.featured[string(userID.Value)] = next
	return cloneFeaturedGroups(next), true, nil
}

func (m *memory) GetFeaturedGroups(_ context.Context, userID *commonpb.UserId) (chat.FeaturedGroups, error) {
	m.Lock()
	defer m.Unlock()

	return cloneFeaturedGroups(m.featured[string(userID.Value)]), nil
}

// cloneFeaturedGroups deep-copies a list, so the stored list and the lists
// handed to or taken from callers share no memory.
func cloneFeaturedGroups(f chat.FeaturedGroups) chat.FeaturedGroups {
	out := chat.FeaturedGroups{Version: f.Version}
	for _, chatID := range f.ChatIDs {
		out.ChatIDs = append(out.ChatIDs, &commonpb.ChatId{Value: append([]byte(nil), chatID.Value...)})
	}
	return out
}

func cloneUserID(userID *commonpb.UserId) *commonpb.UserId {
	return &commonpb.UserId{Value: append([]byte(nil), userID.Value...)}
}
