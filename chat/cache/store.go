package cache

import (
	"context"
	"time"

	"github.com/ReneKroon/ttlcache"

	blobpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/blob/v1"
	chatpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/chat/v1"
	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"
	messagingpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/messaging/v1"

	"github.com/code-payments/flipcash2-server/chat"
)

// Cache wraps a chat.Store, caching what is fixed at a chat's creation and so
// can never go stale: DM membership checks, a DM's member list, and a group's
// participation rules with its creator. A DM's membership never changes, so a confirmed DM
// member, and the DM's member pair, are safe to cache. Group membership is
// mutable — and can be mutated by other processes, which this cache can never
// observe — so group membership checks and member lists always defer to the
// backing store. The rest of the store is passed straight through, viewer
// state and activity reads included: a user's state is theirs to change at any
// time, a group's recent senders change with every send, and both reads are
// already one strongly consistent query, so nothing of either is held.
//
// One thing is held that is not fixed at creation: a lower bound on each
// activity record, so a throttled send costs no write (see RecordSend). It can
// be out of date but never wrong in the direction that matters, because the
// record only moves forward.
type Cache struct {
	db                chat.Store
	memberCache       *ttlcache.Cache
	dmMembersCache    *ttlcache.Cache
	rulesCache        *ttlcache.Cache
	sendActivityCache *ttlcache.Cache
}

func NewInCache(db chat.Store) chat.Store {
	sendActivityCache := ttlcache.NewCache()
	// An entry stops being able to answer a send once its interval is up, so
	// it lives that long from when it was recorded and no longer, however
	// often it is read.
	sendActivityCache.SkipTtlExtensionOnHit(true)
	return &Cache{
		db:                db,
		memberCache:       ttlcache.NewCache(),
		dmMembersCache:    ttlcache.NewCache(),
		rulesCache:        ttlcache.NewCache(),
		sendActivityCache: sendActivityCache,
	}
}

func (c *Cache) PutChat(ctx context.Context, ch *chat.Chat) error {
	return c.db.PutChat(ctx, ch)
}

func (c *Cache) AddGroupMembers(ctx context.Context, chatID *commonpb.ChatId, userIDs []*commonpb.UserId) (bool, chat.RosterSummary, error) {
	return c.db.AddGroupMembers(ctx, chatID, userIDs)
}

func (c *Cache) RemoveGroupMember(ctx context.Context, chatID *commonpb.ChatId, userID *commonpb.UserId) (bool, chat.RosterSummary, error) {
	return c.db.RemoveGroupMember(ctx, chatID, userID)
}

func (c *Cache) SetGroupPicture(ctx context.Context, chatID *commonpb.ChatId, blobID *blobpb.BlobId) error {
	return c.db.SetGroupPicture(ctx, chatID, blobID)
}

func (c *Cache) EditGroup(ctx context.Context, chatID *commonpb.ChatId, edit chat.GroupEdit) error {
	return c.db.EditGroup(ctx, chatID, edit)
}

func (c *Cache) GetChatByID(ctx context.Context, chatID *commonpb.ChatId) (*chat.Chat, error) {
	return c.db.GetChatByID(ctx, chatID)
}

func (c *Cache) GetDmFeedPage(ctx context.Context, userID *commonpb.UserId, chatType chatpb.ChatType, snapshot time.Time, cursor *chat.DmFeedCursor, limit int) ([]*chat.Chat, error) {
	return c.db.GetDmFeedPage(ctx, userID, chatType, snapshot, cursor, limit)
}

// GetMembers is cached for a DM: its member pair is fixed at creation, so a
// list once read is never stale. It is the read behind every DM broadcast (a
// message, a pointer advance, a reaction, typing), which otherwise costs a
// store read per event just to find the peer. A group's roster is mutable, so
// a group's list always defers to the backing store, as IsMember does. Errors
// — including ErrChatNotFound, since the DM may be created later — are not
// cached. The caller gets its own copy, so the cached list is never mutated
// through a result.
func (c *Cache) GetMembers(ctx context.Context, chatID *commonpb.ChatId) ([]*commonpb.UserId, error) {
	if chat.IsGroupChatID(chatID) {
		return c.db.GetMembers(ctx, chatID)
	}

	key := string(chatID.Value)
	if cached, ok := c.dmMembersCache.Get(key); ok {
		return copyUserIDs(cached.([]*commonpb.UserId)), nil
	}

	members, err := c.db.GetMembers(ctx, chatID)
	if err != nil {
		return nil, err
	}
	c.dmMembersCache.Set(key, copyUserIDs(members))
	// The list confirms each member's membership, so the membership cache can
	// be answered from it too.
	for _, member := range members {
		c.memberCache.Set(memberCacheKey(chatID, member), true)
	}
	return copyUserIDs(members), nil
}

// GetGroupMembersPage passes through: a group's roster is mutable and never
// cached here, as for GetMembers.
func (c *Cache) GetGroupMembersPage(ctx context.Context, chatID *commonpb.ChatId, after *commonpb.UserId, limit int) (chat.MembersPage, error) {
	return c.db.GetGroupMembersPage(ctx, chatID, after, limit)
}

// GetGroupRoster, GetGroupRosterPage and GetGroupMemberRecords pass through
// likewise: each is a read of the mutable membership records.
func (c *Cache) GetGroupRoster(ctx context.Context, chatID *commonpb.ChatId) (chat.RosterSummary, []chat.GroupMember, error) {
	return c.db.GetGroupRoster(ctx, chatID)
}

func (c *Cache) GetGroupRosterPage(ctx context.Context, chatID *commonpb.ChatId, after *chat.RosterPosition, limit int) ([]chat.GroupMember, error) {
	return c.db.GetGroupRosterPage(ctx, chatID, after, limit)
}

func (c *Cache) GetGroupMemberRecords(ctx context.Context, userID *commonpb.UserId, chatIDs []*commonpb.ChatId) (map[string]chat.GroupMember, error) {
	return c.db.GetGroupMemberRecords(ctx, userID, chatIDs)
}

func (c *Cache) GetGroupRosterSummary(ctx context.Context, chatID *commonpb.ChatId) (chat.RosterSummary, error) {
	return c.db.GetGroupRosterSummary(ctx, chatID)
}

func (c *Cache) GetGroupRosterSummaries(ctx context.Context, chatIDs []*commonpb.ChatId) (map[string]chat.RosterSummary, error) {
	return c.db.GetGroupRosterSummaries(ctx, chatIDs)
}

// GetGroupRules is cached, including the absence of rules (a nil Rules), so a
// group without any is read once too. Rules and the creator are fixed at
// creation (see chat.Store), so a cached entry is never stale: if either ever
// becomes mutable, this needs invalidation on write. That holds only while
// both are written with the group: a creator backfilled onto a legacy group,
// or IsCreatorOnlySpeaker set on a group that already exists, reaches a
// process that has read the group only when it restarts. A cached entry is
// shared by every caller and must be treated as read-only. Errors — including
// ErrChatNotFound, since the group may be created later — are not cached.
func (c *Cache) GetGroupRules(ctx context.Context, chatID *commonpb.ChatId) (chat.GroupRules, error) {
	key := string(chatID.Value)
	if cached, ok := c.rulesCache.Get(key); ok {
		return cached.(chat.GroupRules), nil
	}

	rules, err := c.db.GetGroupRules(ctx, chatID)
	if err != nil {
		return chat.GroupRules{}, err
	}
	c.rulesCache.Set(key, rules)
	return rules, nil
}

func (c *Cache) IsMember(ctx context.Context, chatID *commonpb.ChatId, userID *commonpb.UserId) (bool, error) {
	// Group membership is mutable, so it is never cached: a stale positive
	// would let a removed member keep reading and sending.
	if chat.IsGroupChatID(chatID) {
		return c.db.IsMember(ctx, chatID, userID)
	}

	key := memberCacheKey(chatID, userID)
	if cached, ok := c.memberCache.Get(key); ok {
		return cached.(bool), nil
	}

	isMember, err := c.db.IsMember(ctx, chatID, userID)
	if err == nil && isMember {
		// Only cache positive results: DM membership is fixed at creation, so a
		// confirmed member stays a member. A negative result is not cached —
		// the chat may not exist yet at check time and could later be created
		// with this user as a member.
		c.memberCache.Set(key, true)
	}
	return isMember, err
}

// Never cached, for the same reason group IsMember checks are not: group
// membership is mutable from other processes.
func (c *Cache) GetGroupMembershipsForUser(ctx context.Context, userID *commonpb.UserId) ([]chat.GroupMembership, error) {
	return c.db.GetGroupMembershipsForUser(ctx, userID)
}

func (c *Cache) GetGroupChatsForUser(ctx context.Context, userID *commonpb.UserId) ([]*chat.Chat, error) {
	return c.db.GetGroupChatsForUser(ctx, userID)
}

func (c *Cache) GetGroupChatsForUserByIDs(ctx context.Context, userID *commonpb.UserId, chatIDs []*commonpb.ChatId) ([]*chat.Chat, error) {
	return c.db.GetGroupChatsForUserByIDs(ctx, userID, chatIDs)
}

func (c *Cache) AdvanceLastMessage(ctx context.Context, chatID *commonpb.ChatId, messageID *messagingpb.MessageId, ts time.Time) (bool, []*commonpb.UserId, error) {
	return c.db.AdvanceLastMessage(ctx, chatID, messageID, ts)
}

func (c *Cache) SetMute(ctx context.Context, chatID *commonpb.ChatId, userID *commonpb.UserId, mute chat.Mute) (chat.ViewerState, bool, error) {
	return c.db.SetMute(ctx, chatID, userID, mute)
}

func (c *Cache) ClearMute(ctx context.Context, chatID *commonpb.ChatId, userID *commonpb.UserId) (chat.ViewerState, bool, error) {
	return c.db.ClearMute(ctx, chatID, userID)
}

func (c *Cache) GetViewerStates(ctx context.Context, userID *commonpb.UserId, chatIDs []*commonpb.ChatId) (map[string]chat.ViewerState, error) {
	return c.db.GetViewerStates(ctx, userID, chatIDs)
}

func (c *Cache) GetMutedUsers(ctx context.Context, chatID *commonpb.ChatId, now time.Time, limit int) ([]*commonpb.UserId, error) {
	return c.db.GetMutedUsers(ctx, chatID, now, limit)
}

func (c *Cache) GetMutedUsersPage(ctx context.Context, chatID *commonpb.ChatId, now time.Time, lo, hi *commonpb.UserId) ([]*commonpb.UserId, error) {
	return c.db.GetMutedUsersPage(ctx, chatID, now, lo, hi)
}

func (c *Cache) GetMutedCount(ctx context.Context, chatID *commonpb.ChatId) (uint64, error) {
	return c.db.GetMutedCount(ctx, chatID)
}

// RecordSend answers a throttled send itself, with no write: the backing
// store is billed for a conditional write whether or not its condition holds,
// so leaving the throttle to it would cost a write per message. Each send this
// process saw recorded is held for ActivityRecordInterval, keyed by (group,
// user), and a send the interval has not yet cleared since it is answered
// false, exactly as the store would answer it: the record only moves forward,
// so it holds at least what was seen recorded here, whatever other processes
// have written since. A send this process has not seen within the interval,
// or one the store refused, goes to the store, so a stale entry can cost a
// write but never skip one. Each process throttles on its own, so a user's
// sends spread across several processes cost up to one write per process per
// interval. A send the store would reject outright (a DM ID, a time before the
// epoch) is never answered here.
func (c *Cache) RecordSend(ctx context.Context, chatID *commonpb.ChatId, userID *commonpb.UserId, sentAt time.Time) (bool, error) {
	sentAtMillis := sentAt.UnixMilli()
	if !chat.IsGroupChatID(chatID) || sentAtMillis <= 0 {
		return c.db.RecordSend(ctx, chatID, userID, sentAt)
	}

	key := sendActivityCacheKey(chatID, userID)
	cached, ok := c.sendActivityCache.Get(key)
	if ok && cached.(int64) > sentAtMillis-chat.ActivityRecordInterval.Milliseconds() {
		return false, nil
	}

	recorded, err := c.db.RecordSend(ctx, chatID, userID, sentAt)
	if err != nil || !recorded {
		return recorded, err
	}
	// A concurrent record may have held a later send; keep whichever bound is
	// higher. The read and write race, but either value is a true bound.
	if cached, ok := c.sendActivityCache.Get(key); !ok || cached.(int64) < sentAtMillis {
		c.sendActivityCache.SetWithTTL(key, sentAtMillis, chat.ActivityRecordInterval)
	}
	return true, nil
}

func (c *Cache) GetRecentSenders(ctx context.Context, chatID *commonpb.ChatId, limit int) ([]chat.RecentSender, error) {
	return c.db.GetRecentSenders(ctx, chatID, limit)
}

// sendActivityCacheKey keys the activity cache by (group, user). Only group
// IDs are held, and they are fixed width (chat.GroupChatIDSize), so
// concatenating the raw bytes is unambiguous.
func sendActivityCacheKey(chatID *commonpb.ChatId, userID *commonpb.UserId) string {
	return string(chatID.Value) + string(userID.Value)
}

// memberCacheKey keys the membership cache by (chat, user). Only DM memberships
// are cached, and DM chat IDs are fixed width (chat.DmChatIDSize), so
// concatenating the raw bytes is unambiguous.
func memberCacheKey(chatID *commonpb.ChatId, userID *commonpb.UserId) string {
	return string(chatID.Value) + string(userID.Value)
}

// copyUserIDs deep-copies a member list, so a cached list and the lists handed
// to callers share no memory.
func copyUserIDs(userIDs []*commonpb.UserId) []*commonpb.UserId {
	out := make([]*commonpb.UserId, len(userIDs))
	for i, id := range userIDs {
		out[i] = &commonpb.UserId{Value: append([]byte(nil), id.Value...)}
	}
	return out
}
