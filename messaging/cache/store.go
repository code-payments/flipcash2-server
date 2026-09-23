package cache

import (
	"context"
	"strconv"
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
//
// It likewise caches a lower bound on each stored pointer, per (chat, member,
// type), to answer no-op advances without a write. Pointers are monotonic, so a
// value the backing store has confirmed at or below the stored pointer stays so;
// an advance to newValue at or below that bound would fail the store's
// condition, and is answered as not advanced without reaching it. That saves the
// write a failed conditional update still bills, for the repeats clients send
// (an advance re-sent on reconnect, a READ to the sender's own message, which
// the send already advanced). Across server instances the bound only ever lags
// the stored value, so a cached no-op is always correct; a miss falls through.
//
// The rest of the store is passed straight through.
type Cache struct {
	db messaging.Store

	mu      sync.Mutex // guards the read-modify-write of the largest-ID cache
	largest *ttlcache.Cache

	pointerMu sync.Mutex // guards the read-modify-write of the pointer bound cache
	pointers  *ttlcache.Cache
}

// pointerBoundTTL bounds how long an idle pointer bound is held. The pointer
// cache has an entry per member per chat, unlike the per-chat message
// watermark, so it is aged out rather than kept for the process lifetime; an
// expired bound only costs the next no-op its write.
const pointerBoundTTL = time.Hour

func NewInCache(db messaging.Store) messaging.Store {
	pointers := ttlcache.NewCache()
	pointers.SetTTL(pointerBoundTTL)
	return &Cache{
		db:       db,
		largest:  ttlcache.NewCache(),
		pointers: pointers,
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

func (c *Cache) EditMessage(
	ctx context.Context,
	chatID *commonpb.ChatId,
	messageID *messagingpb.MessageId,
	content []*messagingpb.Content,
	editedTs time.Time,
	expectedEventSeq uint64,
) (*messaging.Message, error) {
	msg, err := c.db.EditMessage(ctx, chatID, messageID, content, editedTs, expectedEventSeq)
	// A returned message is a confirmed existing ID for the chat, whether the edit
	// succeeded or it came back as the current state on a conflict.
	if msg != nil {
		c.observe(chatID, msg.ID.Value)
	}
	return msg, err
}

func (c *Cache) DeleteMessage(
	ctx context.Context,
	chatID *commonpb.ChatId,
	messageID *messagingpb.MessageId,
	deletedBy *commonpb.UserId,
	deletedTs time.Time,
	expectedEventSeq uint64,
) (*messaging.Message, error) {
	msg, err := c.db.DeleteMessage(ctx, chatID, messageID, deletedBy, deletedTs, expectedEventSeq)
	// A returned message is a confirmed existing ID for the chat, whether the
	// delete succeeded or it came back as the current state on a conflict.
	if msg != nil {
		c.observe(chatID, msg.ID.Value)
	}
	return msg, err
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

func (c *Cache) GetEventDelta(ctx context.Context, chatID *commonpb.ChatId, afterEventSeq, headEventSeq uint64, limit int) ([]*messaging.Message, uint64, error) {
	msgs, nextCursor, err := c.db.GetEventDelta(ctx, chatID, afterEventSeq, headEventSeq, limit)
	if err == nil {
		// Every returned message is a confirmed existing ID for the chat; the
		// largest raises the bound. The page is ordered by event_sequence, not ID,
		// so take the max rather than assuming a position.
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
	return msgs, nextCursor, err
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

func (c *Cache) GetLatestEventSequencesForChats(ctx context.Context, chatIDs []*commonpb.ChatId) (map[string]uint64, error) {
	return c.db.GetLatestEventSequencesForChats(ctx, chatIDs)
}

func (c *Cache) GetPointers(ctx context.Context, chatID *commonpb.ChatId) ([]*messagingpb.Pointer, error) {
	pointers, err := c.db.GetPointers(ctx, chatID)
	if err == nil {
		// Each stored pointer is a confirmed value for its member and type.
		for _, p := range pointers {
			c.observePointer(chatID, p.UserId, p.Type, p.Value.Value)
		}
	}
	return pointers, err
}

func (c *Cache) GetPointersForChats(ctx context.Context, refs []messaging.PointerRef) (map[string][]*messagingpb.Pointer, error) {
	byChat, err := c.db.GetPointersForChats(ctx, refs)
	if err == nil {
		for chatKey, pointers := range byChat {
			chatID := &commonpb.ChatId{Value: []byte(chatKey)}
			for _, p := range pointers {
				c.observePointer(chatID, p.UserId, p.Type, p.Value.Value)
			}
		}
	}
	return byChat, err
}

func (c *Cache) AdvancePointer(
	ctx context.Context,
	chatID *commonpb.ChatId,
	userID *commonpb.UserId,
	pointerType messagingpb.Pointer_Type,
	newValue *messagingpb.MessageId,
) (*messagingpb.Pointer, bool, error) {
	// Fast path: the stored pointer is known to be at or past newValue, so the
	// store would reject the advance as a no-op.
	if bound, ok := c.pointerBound(chatID, userID, pointerType); ok && newValue.Value <= bound {
		return nil, false, nil
	}

	pointer, advanced, err := c.db.AdvancePointer(ctx, chatID, userID, pointerType, newValue)
	if err == nil {
		// Advanced or not, the stored pointer is now at or past newValue.
		c.observePointer(chatID, userID, pointerType, newValue.Value)
	}
	return pointer, advanced, err
}

// pointerBound returns the cached lower bound on the member's stored pointer of
// the given type, and whether one exists.
func (c *Cache) pointerBound(chatID *commonpb.ChatId, userID *commonpb.UserId, pointerType messagingpb.Pointer_Type) (uint64, bool) {
	if v, ok := c.pointers.Get(pointerCacheKey(chatID, userID, pointerType)); ok {
		return v.(uint64), true
	}
	return 0, false
}

// observePointer raises the cached bound on the member's pointer of the given
// type to value if value is greater (or seeds it when absent). The caller must
// pass a value the backing store has confirmed the stored pointer is at or past.
func (c *Cache) observePointer(chatID *commonpb.ChatId, userID *commonpb.UserId, pointerType messagingpb.Pointer_Type, value uint64) {
	c.pointerMu.Lock()
	defer c.pointerMu.Unlock()

	if cur, ok := c.pointerBound(chatID, userID, pointerType); ok && value <= cur {
		return
	}
	c.pointers.Set(pointerCacheKey(chatID, userID, pointerType), value)
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
	messageIDs []*messagingpb.MessageId,
) ([]messaging.SelfReaction, error) {
	return c.db.GetSelfReactions(ctx, chatID, userID, messageIDs)
}

func (c *Cache) GetReactors(
	ctx context.Context,
	chatID *commonpb.ChatId,
	messageID *messagingpb.MessageId,
	emoji string,
	opts ...database.QueryOption,
) ([]*messaging.Reactor, uint64, bool, error) {
	return c.db.GetReactors(ctx, chatID, messageID, emoji, opts...)
}

// largestKey keys the largest-message-ID cache by chat. Chat IDs are fixed width,
// so the raw bytes are an unambiguous key.
func largestKey(chatID *commonpb.ChatId) string {
	return string(chatID.Value)
}

// pointerCacheKey keys the pointer bound cache by (chat, member, type). User IDs
// are fixed width and the type trails them, so the raw bytes are an unambiguous
// key whichever length of chat ID leads it.
func pointerCacheKey(chatID *commonpb.ChatId, userID *commonpb.UserId, pointerType messagingpb.Pointer_Type) string {
	return string(chatID.Value) + string(userID.Value) + strconv.Itoa(int(pointerType))
}
