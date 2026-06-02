package cache_test

import (
	"context"
	"crypto/rand"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"

	"github.com/code-payments/flipcash2-server/chat"
	"github.com/code-payments/flipcash2-server/chat/cache"
	"github.com/code-payments/flipcash2-server/model"
)

// countingStore is a chat.Store whose IsMember result is configurable and whose
// calls are counted. Only IsMember is exercised; the embedded nil interface
// makes any other call panic, which is the intent for these tests.
type countingStore struct {
	chat.Store

	mu     sync.Mutex
	calls  int
	result bool
}

func (s *countingStore) IsMember(_ context.Context, _ *commonpb.ChatId, _ *commonpb.UserId) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls++
	return s.result, nil
}

func (s *countingStore) callCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.calls
}

func TestCache_IsMember_CachesPositive(t *testing.T) {
	ctx := context.Background()
	backing := &countingStore{result: true}
	c := cache.NewInCache(backing)

	chatID := generateChatID()
	userID := model.MustGenerateUserID()

	for i := 0; i < 3; i++ {
		ok, err := c.IsMember(ctx, chatID, userID)
		require.NoError(t, err)
		require.True(t, ok)
	}

	// Confirmed membership is cached after the first lookup.
	require.Equal(t, 1, backing.callCount())
}

func TestCache_IsMember_DoesNotCacheNegative(t *testing.T) {
	ctx := context.Background()
	backing := &countingStore{result: false}
	c := cache.NewInCache(backing)

	chatID := generateChatID()
	userID := model.MustGenerateUserID()

	// A negative result is re-queried, not cached.
	for i := 0; i < 2; i++ {
		ok, err := c.IsMember(ctx, chatID, userID)
		require.NoError(t, err)
		require.False(t, ok)
	}
	require.Equal(t, 2, backing.callCount())

	// If the chat is later created with this user as a member, the cache must
	// reflect the new truth rather than a stale false.
	backing.result = true
	ok, err := c.IsMember(ctx, chatID, userID)
	require.NoError(t, err)
	require.True(t, ok)
}

func TestCache_IsMember_KeyedByChatAndUser(t *testing.T) {
	ctx := context.Background()
	backing := &countingStore{result: true}
	c := cache.NewInCache(backing)

	chatA := generateChatID()
	chatB := generateChatID()
	userX := model.MustGenerateUserID()
	userY := model.MustGenerateUserID()

	// Each distinct (chat, user) pair is cached independently, so each is a fresh
	// backing lookup the first time.
	_, _ = c.IsMember(ctx, chatA, userX)
	_, _ = c.IsMember(ctx, chatA, userY)
	_, _ = c.IsMember(ctx, chatB, userX)
	require.Equal(t, 3, backing.callCount())

	// Repeats of those pairs are served from the cache.
	_, _ = c.IsMember(ctx, chatA, userX)
	_, _ = c.IsMember(ctx, chatB, userX)
	require.Equal(t, 3, backing.callCount())
}

func generateChatID() *commonpb.ChatId {
	b := make([]byte, chat.ChatIDSize)
	if _, err := rand.Read(b); err != nil {
		panic(err)
	}
	return &commonpb.ChatId{Value: b}
}
