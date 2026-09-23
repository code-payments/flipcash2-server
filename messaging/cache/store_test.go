package cache_test

import (
	"context"
	"crypto/rand"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"
	messagingpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/messaging/v1"

	"github.com/code-payments/flipcash2-server/messaging"
	"github.com/code-payments/flipcash2-server/messaging/cache"
	"github.com/code-payments/flipcash2-server/messaging/memory"
	"github.com/code-payments/flipcash2-server/messaging/tests"
)

// TestMessaging_Cache runs the full shared store suite against the cache decorator
// wrapping an in-memory store, proving the decorator preserves every behavior.
func TestMessaging_Cache(t *testing.T) {
	testStore := cache.NewInCache(memory.NewInMemory())
	// Each test in the suite uses freshly generated random chat IDs, so it is
	// isolated without an explicit reset between runs.
	tests.RunStoreTests(t, testStore, func() {})
}

// countingStore is a messaging.Store that counts MessageExists calls and lets the
// test drive its PutMessage/MessageExists results. The embedded nil interface
// makes any other call panic, which is the intent for these focused tests.
type countingStore struct {
	messaging.Store

	mu          sync.Mutex
	existsCalls int
	exists      bool
	nextID      uint64
}

func (s *countingStore) PutMessage(
	_ context.Context,
	chatID *commonpb.ChatId,
	_ *commonpb.UserId,
	_ []*messagingpb.Content,
	_ time.Time,
	_ *messagingpb.ClientMessageId,
	_ bool,
) (*messaging.Message, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.nextID++
	return &messaging.Message{
		ChatID: chatID,
		ID:     &messagingpb.MessageId{Value: s.nextID},
	}, true, nil
}

func (s *countingStore) MessageExists(_ context.Context, _ *commonpb.ChatId, _ *messagingpb.MessageId) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.existsCalls++
	return s.exists, nil
}

func (s *countingStore) existsCallCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.existsCalls
}

// countingMemory is a real in-memory messaging.Store that counts MessageExists
// calls, so a test can assert which checks were served from the cache versus the
// backing store while still exercising real persistence for the getters.
type countingMemory struct {
	messaging.Store

	mu          sync.Mutex
	existsCalls int
}

func newCountingMemory() *countingMemory {
	return &countingMemory{Store: memory.NewInMemory()}
}

func (s *countingMemory) MessageExists(ctx context.Context, chatID *commonpb.ChatId, messageID *messagingpb.MessageId) (bool, error) {
	s.mu.Lock()
	s.existsCalls++
	s.mu.Unlock()
	return s.Store.MessageExists(ctx, chatID, messageID)
}

func (s *countingMemory) resetExistsCount() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.existsCalls = 0
}

func (s *countingMemory) existsCallCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.existsCalls
}

func TestCache_MessageExists_ServedFromLargestSeenAfterPut(t *testing.T) {
	ctx := context.Background()
	backing := &countingStore{}
	c := cache.NewInCache(backing)

	chatID := generateChatID()

	// Write three messages: the largest known ID for the chat becomes 3.
	for range 3 {
		_, _, err := c.PutMessage(ctx, chatID, nil, nil, time.Unix(0, 0), nil, true)
		require.NoError(t, err)
	}

	// Every ID at or below the largest seen exists by the gapless invariant and is
	// answered from the cache, never reaching the backing store.
	for id := uint64(1); id <= 3; id++ {
		exists, err := c.MessageExists(ctx, chatID, &messagingpb.MessageId{Value: id})
		require.NoError(t, err)
		require.True(t, exists)
	}
	require.Equal(t, 0, backing.existsCallCount())
}

func TestCache_MessageExists_MissFallsThroughAndRaisesBound(t *testing.T) {
	ctx := context.Background()
	backing := &countingStore{exists: true}
	c := cache.NewInCache(backing)

	chatID := generateChatID()

	// No entry yet: the check falls through to the backing store.
	exists, err := c.MessageExists(ctx, chatID, &messagingpb.MessageId{Value: 5})
	require.NoError(t, err)
	require.True(t, exists)
	require.Equal(t, 1, backing.existsCallCount())

	// The confirmed existing ID raised the bound to 5, so checks at or below it are
	// now served from the cache.
	for id := uint64(1); id <= 5; id++ {
		exists, err := c.MessageExists(ctx, chatID, &messagingpb.MessageId{Value: id})
		require.NoError(t, err)
		require.True(t, exists)
	}
	require.Equal(t, 1, backing.existsCallCount())

	// A check above the bound still falls through.
	_, err = c.MessageExists(ctx, chatID, &messagingpb.MessageId{Value: 6})
	require.NoError(t, err)
	require.Equal(t, 2, backing.existsCallCount())
}

func TestCache_MessageExists_NegativeNotCached(t *testing.T) {
	ctx := context.Background()
	backing := &countingStore{exists: false}
	c := cache.NewInCache(backing)

	chatID := generateChatID()

	// A negative backing result must not raise the bound; repeats keep re-querying.
	for range 2 {
		exists, err := c.MessageExists(ctx, chatID, &messagingpb.MessageId{Value: 5})
		require.NoError(t, err)
		require.False(t, exists)
	}
	require.Equal(t, 2, backing.existsCallCount())

	// Once the message exists, the next check reflects the new truth.
	backing.mu.Lock()
	backing.exists = true
	backing.mu.Unlock()
	exists, err := c.MessageExists(ctx, chatID, &messagingpb.MessageId{Value: 5})
	require.NoError(t, err)
	require.True(t, exists)
}

func TestCache_MessageExists_KeyedByChat(t *testing.T) {
	ctx := context.Background()
	backing := &countingStore{}
	c := cache.NewInCache(backing)

	chatA := generateChatID()
	chatB := generateChatID()

	// A message in chat A raises only chat A's bound.
	_, _, err := c.PutMessage(ctx, chatA, nil, nil, time.Unix(0, 0), nil, true)
	require.NoError(t, err)

	exists, err := c.MessageExists(ctx, chatA, &messagingpb.MessageId{Value: 1})
	require.NoError(t, err)
	require.True(t, exists)
	require.Equal(t, 0, backing.existsCallCount())

	// Chat B has no cached bound, so its check falls through to the backing store.
	_, err = c.MessageExists(ctx, chatB, &messagingpb.MessageId{Value: 1})
	require.NoError(t, err)
	require.Equal(t, 1, backing.existsCallCount())
}

// TestCache_GettersSeedWatermark verifies the message getters raise the high
// watermark, so a later MessageExists at or below a previously-read ID is served
// from the cache. It exercises the real in-memory backing store.
func TestCache_GettersSeedWatermark(t *testing.T) {
	ctx := context.Background()

	t.Run("GetMessage", func(t *testing.T) {
		backing := newCountingMemory()
		c := cache.NewInCache(backing)
		chatID := generateChatID()

		msg := putMessage(t, ctx, c, chatID)
		// Drop the watermark seeded by PutMessage so the getter is what seeds it.
		backing.resetExistsCount()

		_, err := c.GetMessage(ctx, chatID, msg.ID)
		require.NoError(t, err)

		exists, err := c.MessageExists(ctx, chatID, msg.ID)
		require.NoError(t, err)
		require.True(t, exists)
		require.Equal(t, 0, backing.existsCallCount())
	})

	t.Run("GetMessages", func(t *testing.T) {
		backing := newCountingMemory()
		c := cache.NewInCache(backing)
		chatID := generateChatID()

		putMessage(t, ctx, c, chatID)
		putMessage(t, ctx, c, chatID)
		last := putMessage(t, ctx, c, chatID)

		msgs, err := c.GetMessages(ctx, chatID)
		require.NoError(t, err)
		require.Len(t, msgs, 3)

		// Every ID up to the largest read is now served from the cache.
		for id := uint64(1); id <= last.ID.Value; id++ {
			exists, err := c.MessageExists(ctx, chatID, &messagingpb.MessageId{Value: id})
			require.NoError(t, err)
			require.True(t, exists)
		}
		require.Equal(t, 0, backing.existsCallCount())
	})

	t.Run("GetMessagesByRefs", func(t *testing.T) {
		backing := newCountingMemory()
		c := cache.NewInCache(backing)
		chatA := generateChatID()
		chatB := generateChatID()

		a := putMessage(t, ctx, c, chatA)
		b := putMessage(t, ctx, c, chatB)

		msgs, err := c.GetMessagesByRefs(ctx, []messaging.MessageRef{
			{ChatID: chatA, MessageID: a.ID},
			{ChatID: chatB, MessageID: b.ID},
		})
		require.NoError(t, err)
		require.Len(t, msgs, 2)

		// Each chat's bound was raised independently from its own message.
		for _, tc := range []struct {
			chatID *commonpb.ChatId
			id     *messagingpb.MessageId
		}{{chatA, a.ID}, {chatB, b.ID}} {
			exists, err := c.MessageExists(ctx, tc.chatID, tc.id)
			require.NoError(t, err)
			require.True(t, exists)
		}
		require.Equal(t, 0, backing.existsCallCount())
	})

	t.Run("GetEventDelta", func(t *testing.T) {
		backing := newCountingMemory()
		c := cache.NewInCache(backing)
		chatID := generateChatID()

		// Seed through the backing store directly, so the cache holds no watermark
		// yet — the getter under test is the only thing that can raise it.
		putMessage(t, ctx, backing, chatID)
		putMessage(t, ctx, backing, chatID)
		last := putMessage(t, ctx, backing, chatID)

		msgs, _, err := c.GetEventDelta(ctx, chatID, 0, last.EventSequence, 100)
		require.NoError(t, err)
		require.Len(t, msgs, 3)

		// The page raised the bound to the largest returned ID, so every ID up to it
		// is now served from the cache without touching the backing store.
		for id := uint64(1); id <= last.ID.Value; id++ {
			exists, err := c.MessageExists(ctx, chatID, &messagingpb.MessageId{Value: id})
			require.NoError(t, err)
			require.True(t, exists)
		}
		require.Equal(t, 0, backing.existsCallCount())
	})
}

// countingPointers is a real in-memory messaging.Store that counts
// AdvancePointer calls, so a test can assert which advances were answered from
// the pointer bound cache versus the backing store.
type countingPointers struct {
	messaging.Store

	mu           sync.Mutex
	advanceCalls int
}

func newCountingPointers() *countingPointers {
	return &countingPointers{Store: memory.NewInMemory()}
}

func (s *countingPointers) AdvancePointer(
	ctx context.Context,
	chatID *commonpb.ChatId,
	userID *commonpb.UserId,
	pointerType messagingpb.Pointer_Type,
	newValue *messagingpb.MessageId,
) (*messagingpb.Pointer, bool, error) {
	s.mu.Lock()
	s.advanceCalls++
	s.mu.Unlock()
	return s.Store.AdvancePointer(ctx, chatID, userID, pointerType, newValue)
}

func (s *countingPointers) advanceCallCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.advanceCalls
}

// newPointerChat creates a chat in the backing store to hold pointers: the
// in-memory store keeps no pointers for a chat with no messages.
func newPointerChat(t *testing.T, ctx context.Context, backing messaging.Store) *commonpb.ChatId {
	t.Helper()
	chatID := generateChatID()
	putMessage(t, ctx, backing, chatID)
	return chatID
}

// advance advances a pointer through s and returns whether it moved.
func advance(t *testing.T, ctx context.Context, s messaging.Store, chatID *commonpb.ChatId, userID *commonpb.UserId, pointerType messagingpb.Pointer_Type, value uint64) bool {
	t.Helper()
	pointer, advanced, err := s.AdvancePointer(ctx, chatID, userID, pointerType, &messagingpb.MessageId{Value: value})
	require.NoError(t, err)
	if advanced {
		require.EqualValues(t, value, pointer.Value.Value)
	} else {
		require.Nil(t, pointer)
	}
	return advanced
}

func TestCache_AdvancePointer_NoOpServedFromBoundAfterAdvance(t *testing.T) {
	ctx := context.Background()
	backing := newCountingPointers()
	c := cache.NewInCache(backing)
	chatID, userID := newPointerChat(t, ctx, backing), generateUserID()

	require.True(t, advance(t, ctx, c, chatID, userID, messagingpb.Pointer_READ, 3))
	require.Equal(t, 1, backing.advanceCallCount())

	// The same value and anything below it are no-ops answered from the bound.
	require.False(t, advance(t, ctx, c, chatID, userID, messagingpb.Pointer_READ, 3))
	require.False(t, advance(t, ctx, c, chatID, userID, messagingpb.Pointer_READ, 1))
	require.Equal(t, 1, backing.advanceCallCount())

	// A forward advance reaches the store and raises the bound.
	require.True(t, advance(t, ctx, c, chatID, userID, messagingpb.Pointer_READ, 5))
	require.False(t, advance(t, ctx, c, chatID, userID, messagingpb.Pointer_READ, 4))
	require.Equal(t, 2, backing.advanceCallCount())
}

// TestCache_AdvancePointer_StoreNoOpRaisesBound covers a pointer advanced
// elsewhere (another server instance): the first stale advance reaches the
// store and fails there, and raises the bound so the next one does not.
func TestCache_AdvancePointer_StoreNoOpRaisesBound(t *testing.T) {
	ctx := context.Background()
	backing := newCountingPointers()
	c := cache.NewInCache(backing)
	chatID, userID := newPointerChat(t, ctx, backing), generateUserID()

	require.True(t, advance(t, ctx, backing, chatID, userID, messagingpb.Pointer_READ, 5))

	require.False(t, advance(t, ctx, c, chatID, userID, messagingpb.Pointer_READ, 3))
	require.Equal(t, 2, backing.advanceCallCount())

	require.False(t, advance(t, ctx, c, chatID, userID, messagingpb.Pointer_READ, 3))
	require.Equal(t, 2, backing.advanceCallCount())

	// The bound is 3, not the stored 5: the store's no-op confirms only that
	// the pointer is at or past the value asked for. A value between the two
	// reaches the store, which rejects it.
	require.False(t, advance(t, ctx, c, chatID, userID, messagingpb.Pointer_READ, 4))
	require.Equal(t, 3, backing.advanceCallCount())
}

func TestCache_AdvancePointer_KeyedByChatMemberAndType(t *testing.T) {
	ctx := context.Background()
	backing := newCountingPointers()
	c := cache.NewInCache(backing)
	chatA, chatB := newPointerChat(t, ctx, backing), newPointerChat(t, ctx, backing)
	userA, userB := generateUserID(), generateUserID()

	require.True(t, advance(t, ctx, c, chatA, userA, messagingpb.Pointer_READ, 5))

	// A bound covers only its own (chat, member, type): each of these is a real
	// advance that reaches the store.
	require.True(t, advance(t, ctx, c, chatA, userA, messagingpb.Pointer_DELIVERED, 3))
	require.True(t, advance(t, ctx, c, chatA, userB, messagingpb.Pointer_READ, 3))
	require.True(t, advance(t, ctx, c, chatB, userA, messagingpb.Pointer_READ, 3))
	require.Equal(t, 4, backing.advanceCallCount())
}

func TestCache_AdvancePointer_GettersSeedBound(t *testing.T) {
	ctx := context.Background()

	t.Run("GetPointers", func(t *testing.T) {
		backing := newCountingPointers()
		c := cache.NewInCache(backing)
		chatID, userID := newPointerChat(t, ctx, backing), generateUserID()

		// Seed through the backing store directly, so the getter under test is
		// the only thing that can set the bound.
		require.True(t, advance(t, ctx, backing, chatID, userID, messagingpb.Pointer_READ, 4))
		require.True(t, advance(t, ctx, backing, chatID, userID, messagingpb.Pointer_DELIVERED, 6))

		_, err := c.GetPointers(ctx, chatID)
		require.NoError(t, err)

		require.False(t, advance(t, ctx, c, chatID, userID, messagingpb.Pointer_READ, 4))
		require.False(t, advance(t, ctx, c, chatID, userID, messagingpb.Pointer_DELIVERED, 6))
		require.Equal(t, 2, backing.advanceCallCount())
	})

	t.Run("GetPointersForChats", func(t *testing.T) {
		backing := newCountingPointers()
		c := cache.NewInCache(backing)
		chatA, chatB := newPointerChat(t, ctx, backing), newPointerChat(t, ctx, backing)
		userID := generateUserID()

		require.True(t, advance(t, ctx, backing, chatA, userID, messagingpb.Pointer_READ, 2))
		require.True(t, advance(t, ctx, backing, chatB, userID, messagingpb.Pointer_READ, 7))

		_, err := c.GetPointersForChats(ctx, []messaging.PointerRef{
			{ChatID: chatA, Members: []*commonpb.UserId{userID}},
			{ChatID: chatB, Members: []*commonpb.UserId{userID}},
		})
		require.NoError(t, err)

		// Each chat's bound was seeded from its own pointer.
		require.False(t, advance(t, ctx, c, chatA, userID, messagingpb.Pointer_READ, 2))
		require.False(t, advance(t, ctx, c, chatB, userID, messagingpb.Pointer_READ, 7))
		require.Equal(t, 2, backing.advanceCallCount())
		require.True(t, advance(t, ctx, c, chatA, userID, messagingpb.Pointer_READ, 3))
		require.Equal(t, 3, backing.advanceCallCount())
	})
}

func putMessage(t *testing.T, ctx context.Context, s messaging.Store, chatID *commonpb.ChatId) *messaging.Message {
	t.Helper()
	msg, _, err := s.PutMessage(ctx, chatID, nil, []*messagingpb.Content{textContent("hi")}, time.Unix(0, 0), generateClientID(), true)
	require.NoError(t, err)
	return msg
}

func textContent(s string) *messagingpb.Content {
	return &messagingpb.Content{Type: &messagingpb.Content_Text{Text: &messagingpb.TextContent{Text: s}}}
}

func generateClientID() *messagingpb.ClientMessageId {
	b := make([]byte, messaging.ClientMessageIDSize)
	if _, err := rand.Read(b); err != nil {
		panic(err)
	}
	return &messagingpb.ClientMessageId{Value: b}
}

func generateChatID() *commonpb.ChatId {
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		panic(err)
	}
	return &commonpb.ChatId{Value: b}
}

func generateUserID() *commonpb.UserId {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		panic(err)
	}
	return &commonpb.UserId{Value: b}
}
