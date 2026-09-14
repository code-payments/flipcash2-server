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
// can never go stale: DM membership checks and a group's participation rules.
// A DM's membership never changes, so a confirmed DM member is safe to cache.
// Group membership is mutable — and can be mutated by other processes, which
// this cache can never observe — so group membership checks always defer to
// the backing store. The rest of the store is passed straight through.
type Cache struct {
	db          chat.Store
	memberCache *ttlcache.Cache
	rulesCache  *ttlcache.Cache
}

func NewInCache(db chat.Store) chat.Store {
	return &Cache{
		db:          db,
		memberCache: ttlcache.NewCache(),
		rulesCache:  ttlcache.NewCache(),
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

func (c *Cache) GetChatByID(ctx context.Context, chatID *commonpb.ChatId) (*chat.Chat, error) {
	return c.db.GetChatByID(ctx, chatID)
}

func (c *Cache) GetDmFeedPage(ctx context.Context, userID *commonpb.UserId, chatType chatpb.ChatType, snapshot time.Time, cursor *chat.DmFeedCursor, limit int) ([]*chat.Chat, error) {
	return c.db.GetDmFeedPage(ctx, userID, chatType, snapshot, cursor, limit)
}

func (c *Cache) GetMembers(ctx context.Context, chatID *commonpb.ChatId) ([]*commonpb.UserId, error) {
	return c.db.GetMembers(ctx, chatID)
}

func (c *Cache) GetGroupRosterSummary(ctx context.Context, chatID *commonpb.ChatId) (chat.RosterSummary, error) {
	return c.db.GetGroupRosterSummary(ctx, chatID)
}

// GetGroupRules is cached, including the absence of rules (a nil result), so a
// group without any is read once too. Rules are fixed at creation (see
// chat.Store), so a cached entry is never stale: if rules ever become mutable,
// this needs invalidation on write. A cached entry is shared by every caller
// and must be treated as read-only. Errors — including ErrChatNotFound, since
// the group may be created later — are not cached.
func (c *Cache) GetGroupRules(ctx context.Context, chatID *commonpb.ChatId) (*chatpb.Rules, error) {
	key := string(chatID.Value)
	if cached, ok := c.rulesCache.Get(key); ok {
		return cached.(*chatpb.Rules), nil
	}

	rules, err := c.db.GetGroupRules(ctx, chatID)
	if err != nil {
		return nil, err
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
func (c *Cache) GetGroupChatIDsForUser(ctx context.Context, userID *commonpb.UserId) ([]*commonpb.ChatId, error) {
	return c.db.GetGroupChatIDsForUser(ctx, userID)
}

func (c *Cache) AdvanceLastMessage(ctx context.Context, chatID *commonpb.ChatId, messageID *messagingpb.MessageId, ts time.Time) (bool, []*commonpb.UserId, error) {
	return c.db.AdvanceLastMessage(ctx, chatID, messageID, ts)
}

// memberCacheKey keys the membership cache by (chat, user). Only DM memberships
// are cached, and DM chat IDs are fixed width (chat.DmChatIDSize), so
// concatenating the raw bytes is unambiguous.
func memberCacheKey(chatID *commonpb.ChatId, userID *commonpb.UserId) string {
	return string(chatID.Value) + string(userID.Value)
}
