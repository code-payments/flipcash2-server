package cache

import (
	"context"
	"time"

	"github.com/ReneKroon/ttlcache"

	chatpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/chat/v1"
	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"
	messagingpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/messaging/v1"

	"github.com/code-payments/flipcash2-server/chat"
)

// Cache wraps a chat.Store, caching membership checks. Membership is fixed at
// chat creation and never changes, so a confirmed member is safe to cache; the
// rest of the store is passed straight through.
type Cache struct {
	db          chat.Store
	memberCache *ttlcache.Cache
}

func NewInCache(db chat.Store) chat.Store {
	return &Cache{
		db:          db,
		memberCache: ttlcache.NewCache(),
	}
}

func (c *Cache) PutChat(ctx context.Context, ch *chat.Chat) error {
	return c.db.PutChat(ctx, ch)
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

func (c *Cache) IsMember(ctx context.Context, chatID *commonpb.ChatId, userID *commonpb.UserId) (bool, error) {
	key := memberCacheKey(chatID, userID)
	if cached, ok := c.memberCache.Get(key); ok {
		return cached.(bool), nil
	}

	isMember, err := c.db.IsMember(ctx, chatID, userID)
	if err == nil && isMember {
		// Only cache positive results: membership is fixed at creation, so a
		// confirmed member stays a member. A negative result is not cached —
		// the chat may not exist yet at check time and could later be created
		// with this user as a member.
		c.memberCache.Set(key, true)
	}
	return isMember, err
}

func (c *Cache) AdvanceLastMessage(ctx context.Context, chatID *commonpb.ChatId, messageID *messagingpb.MessageId, ts time.Time) (bool, []*commonpb.UserId, error) {
	return c.db.AdvanceLastMessage(ctx, chatID, messageID, ts)
}

// memberCacheKey keys the membership cache by (chat, user). Chat IDs are fixed
// width (chat.ChatIDSize), so concatenating the raw bytes is unambiguous.
func memberCacheKey(chatID *commonpb.ChatId, userID *commonpb.UserId) string {
	return string(chatID.Value) + string(userID.Value)
}
