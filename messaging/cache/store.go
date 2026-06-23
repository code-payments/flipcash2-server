package cache

import (
	"context"
	"sync"
	"time"

	"github.com/ReneKroon/ttlcache"

	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"
	messagingpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/messaging/v1"

	"github.com/code-payments/flipcash2-server/database"
	"github.com/code-payments/flipcash2-server/messaging"
)

// Cache wraps a messaging.Store, caching the largest message ID seen per chat to
// short-circuit existence checks. Each chat has its own gapless message ID
// sequence starting at 1, and messages are never removed (a deletion is a
// tombstone, still a real message). So if id <= the largest known ID, every ID up
// to it — including id — exists, and MessageExists can answer true without
// touching the backing store.
//
// The cached value is only ever a lower bound on the true maximum: it is seeded
// from IDs the backing store has confirmed (a PutMessage result, or a positive
// MessageExists), so a true cache answer is always correct even across multiple
// server instances. A miss (id above the cached bound, or no entry yet) falls
// through to the backing store, and a confirmed existing id then raises the bound.
// The rest of the store is passed straight through.
type Cache struct {
	db messaging.Store

	mu      sync.Mutex // guards the read-modify-write of the largest-ID cache
	largest *ttlcache.Cache
}

func NewInCache(db messaging.Store) messaging.Store {
	return &Cache{
		db:      db,
		largest: ttlcache.NewCache(),
	}
}

// largestSeen returns the cached largest known message ID for the chat, and
// whether an entry exists.
func (c *Cache) largestSeen(chatID *commonpb.ChatId) (uint64, bool) {
	if v, ok := c.largest.Get(largestKey(chatID)); ok {
		return v.(uint64), true
	}
	return 0, false
}

// observe raises the cached largest known message ID for the chat to id if id is
// greater (or seeds it when absent). It is monotonic: an id at or below the
// current bound leaves the cache unchanged. The caller must pass an id the backing
// store has confirmed exists.
func (c *Cache) observe(chatID *commonpb.ChatId, id uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if cur, ok := c.largestSeen(chatID); ok && id <= cur {
		return
	}
	c.largest.Set(largestKey(chatID), id)
}

func (c *Cache) PutMessage(
	ctx context.Context,
	chatID *commonpb.ChatId,
	senderID *commonpb.UserId,
	content []*messagingpb.Content,
	ts time.Time,
	clientMessageID *messagingpb.ClientMessageId,
	countsTowardUnread bool,
) (*messaging.Message, bool, error) {
	msg, created, err := c.db.PutMessage(ctx, chatID, senderID, content, ts, clientMessageID, countsTowardUnread)
	if err == nil {
		// The persisted message's ID is a confirmed existing ID for the chat,
		// whether this was a fresh write or an idempotent retry.
		c.observe(chatID, msg.ID.Value)
	}
	return msg, created, err
}

func (c *Cache) GetMessage(ctx context.Context, chatID *commonpb.ChatId, messageID *messagingpb.MessageId) (*messaging.Message, error) {
	msg, err := c.db.GetMessage(ctx, chatID, messageID)
	if err == nil {
		// A returned message is a confirmed existing ID for the chat.
		c.observe(chatID, msg.ID.Value)
	}
	return msg, err
}

func (c *Cache) GetMessages(ctx context.Context, chatID *commonpb.ChatId, opts ...database.QueryOption) ([]*messaging.Message, error) {
	msgs, err := c.db.GetMessages(ctx, chatID, opts...)
	if err == nil {
		// Every returned message is a confirmed existing ID for the chat; the
		// largest in the page raises the bound. The page may be in any order, so
		// take the max rather than assuming a position.
		var max uint64
		for _, msg := range msgs {
			if msg.ID.Value > max {
				max = msg.ID.Value
			}
		}
		if max > 0 {
			c.observe(chatID, max)
		}
	}
	return msgs, err
}

func (c *Cache) GetMessagesByRefs(ctx context.Context, refs []messaging.MessageRef) ([]*messaging.Message, error) {
	msgs, err := c.db.GetMessagesByRefs(ctx, refs)
	if err == nil {
		// Results span multiple chats; raise each chat's bound from the largest of
		// its own returned messages, observing each chat once.
		max := make(map[string]*messaging.Message, len(msgs))
		for _, msg := range msgs {
			if cur, ok := max[string(msg.ChatID.Value)]; !ok || msg.ID.Value > cur.ID.Value {
				max[string(msg.ChatID.Value)] = msg
			}
		}
		for _, msg := range max {
			c.observe(msg.ChatID, msg.ID.Value)
		}
	}
	return msgs, err
}

func (c *Cache) MessageExists(ctx context.Context, chatID *commonpb.ChatId, messageID *messagingpb.MessageId) (bool, error) {
	// Fast path: the message ID is at or below a confirmed existing ID, so it
	// exists by the gapless-sequence invariant — no backing read needed.
	if largest, ok := c.largestSeen(chatID); ok && messageID.Value <= largest {
		return true, nil
	}

	exists, err := c.db.MessageExists(ctx, chatID, messageID)
	if err == nil && exists {
		// A confirmed existing ID raises the bound, serving future checks at or
		// below it from the cache.
		c.observe(chatID, messageID.Value)
	}
	return exists, err
}

func (c *Cache) GetLatestEventSequence(ctx context.Context, chatID *commonpb.ChatId) (uint64, error) {
	return c.db.GetLatestEventSequence(ctx, chatID)
}

func (c *Cache) GetPointers(ctx context.Context, chatID *commonpb.ChatId) ([]*messagingpb.Pointer, error) {
	return c.db.GetPointers(ctx, chatID)
}

func (c *Cache) GetPointersForChats(ctx context.Context, refs []messaging.PointerRef) (map[string][]*messagingpb.Pointer, error) {
	return c.db.GetPointersForChats(ctx, refs)
}

func (c *Cache) AdvancePointer(
	ctx context.Context,
	chatID *commonpb.ChatId,
	userID *commonpb.UserId,
	pointerType messagingpb.Pointer_Type,
	newValue *messagingpb.MessageId,
) (*messagingpb.Pointer, bool, error) {
	return c.db.AdvancePointer(ctx, chatID, userID, pointerType, newValue)
}

func (c *Cache) AddReaction(
	ctx context.Context,
	chatID *commonpb.ChatId,
	messageID *messagingpb.MessageId,
	userID *commonpb.UserId,
	emoji string,
	ts time.Time,
) (*messaging.Reaction, bool, bool, error) {
	return c.db.AddReaction(ctx, chatID, messageID, userID, emoji, ts)
}

func (c *Cache) RemoveReaction(
	ctx context.Context,
	chatID *commonpb.ChatId,
	messageID *messagingpb.MessageId,
	userID *commonpb.UserId,
	emoji string,
) (*messaging.Reaction, bool, error) {
	return c.db.RemoveReaction(ctx, chatID, messageID, userID, emoji)
}

func (c *Cache) GetReactionSummary(
	ctx context.Context,
	chatID *commonpb.ChatId,
	messageID *messagingpb.MessageId,
) ([]*messaging.Reaction, error) {
	return c.db.GetReactionSummary(ctx, chatID, messageID)
}

func (c *Cache) GetReactionSummariesByRefs(
	ctx context.Context,
	chatID *commonpb.ChatId,
	messageIDs []*messagingpb.MessageId,
) ([]*messaging.ReactionSummary, error) {
	return c.db.GetReactionSummariesByRefs(ctx, chatID, messageIDs)
}

func (c *Cache) GetReactionSummaries(
	ctx context.Context,
	chatID *commonpb.ChatId,
	opts ...database.QueryOption,
) ([]*messaging.ReactionSummary, error) {
	return c.db.GetReactionSummaries(ctx, chatID, opts...)
}

func (c *Cache) GetSelfReactions(
	ctx context.Context,
	chatID *commonpb.ChatId,
	userID *commonpb.UserId,
	refs []messaging.ReactionRef,
) ([]messaging.ReactionRef, error) {
	return c.db.GetSelfReactions(ctx, chatID, userID, refs)
}

func (c *Cache) GetReactors(
	ctx context.Context,
	chatID *commonpb.ChatId,
	messageID *messagingpb.MessageId,
	emoji string,
	consistent bool,
	opts ...database.QueryOption,
) ([]*messaging.Reactor, bool, error) {
	return c.db.GetReactors(ctx, chatID, messageID, emoji, consistent, opts...)
}

// largestKey keys the largest-message-ID cache by chat. Chat IDs are fixed width,
// so the raw bytes are an unambiguous key.
func largestKey(chatID *commonpb.ChatId) string {
	return string(chatID.Value)
}
