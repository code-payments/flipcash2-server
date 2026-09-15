package cache_test

import (
	"context"
	"crypto/rand"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	chatpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/chat/v1"
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

// countingRulesStore is a chat.Store whose GetGroupRules result is
// configurable and whose calls are counted.
type countingRulesStore struct {
	chat.Store

	mu    sync.Mutex
	calls int
	rules *chatpb.Rules
	err   error
}

func (s *countingRulesStore) GetGroupRules(_ context.Context, _ *commonpb.ChatId) (*chatpb.Rules, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls++
	return s.rules, s.err
}

func (s *countingRulesStore) callCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.calls
}

// countingMembersStore is a chat.Store whose GetMembers result is configurable
// and whose calls are counted. IsMember is served from the configured members
// so the seeding of the membership cache can be observed.
type countingMembersStore struct {
	chat.Store

	mu            sync.Mutex
	calls         int
	isMemberCalls int
	members       []*commonpb.UserId
	err           error
}

func (s *countingMembersStore) GetMembers(_ context.Context, _ *commonpb.ChatId) ([]*commonpb.UserId, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls++
	return s.members, s.err
}

func (s *countingMembersStore) IsMember(_ context.Context, _ *commonpb.ChatId, userID *commonpb.UserId) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.isMemberCalls++
	for _, m := range s.members {
		if string(m.Value) == string(userID.Value) {
			return true, nil
		}
	}
	return false, nil
}

func (s *countingMembersStore) callCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.calls
}

func TestCache_GetMembers_CachesDm(t *testing.T) {
	ctx := context.Background()
	userX := model.MustGenerateUserID()
	userY := model.MustGenerateUserID()
	backing := &countingMembersStore{members: []*commonpb.UserId{userX, userY}}
	c := cache.NewInCache(backing)

	chatID := generateDmChatID()
	for i := 0; i < 3; i++ {
		members, err := c.GetMembers(ctx, chatID)
		require.NoError(t, err)
		require.Len(t, members, 2)
		require.Equal(t, userX.Value, members[0].Value)
		require.Equal(t, userY.Value, members[1].Value)
	}
	require.Equal(t, 1, backing.callCount())

	// A DM's members are fixed at creation, so the cache is what answers even
	// once the backing store would say otherwise.
	backing.members = nil
	members, err := c.GetMembers(ctx, chatID)
	require.NoError(t, err)
	require.Len(t, members, 2)
	require.Equal(t, 1, backing.callCount())

	// The list confirmed both memberships, so IsMember is answered from the
	// cache without a backing lookup.
	for _, u := range []*commonpb.UserId{userX, userY} {
		ok, err := c.IsMember(ctx, chatID, u)
		require.NoError(t, err)
		require.True(t, ok)
	}
	require.Equal(t, 0, backing.isMemberCalls)

	// Each distinct DM is cached independently.
	backing.members = []*commonpb.UserId{userX, userY}
	_, err = c.GetMembers(ctx, generateDmChatID())
	require.NoError(t, err)
	require.Equal(t, 2, backing.callCount())
}

func TestCache_GetMembers_ReturnsCopies(t *testing.T) {
	ctx := context.Background()
	userX := model.MustGenerateUserID()
	userY := model.MustGenerateUserID()
	backing := &countingMembersStore{members: []*commonpb.UserId{userX, userY}}
	c := cache.NewInCache(backing)

	chatID := generateDmChatID()
	first, err := c.GetMembers(ctx, chatID)
	require.NoError(t, err)

	// Mutating a result — the slice or an ID's bytes — must not leak into the
	// cached list or into any later result.
	first[0], first[1] = first[1], first[0]
	first[0].Value[0] ^= 0xff

	second, err := c.GetMembers(ctx, chatID)
	require.NoError(t, err)
	require.Equal(t, userX.Value, second[0].Value)
	require.Equal(t, userY.Value, second[1].Value)
	require.Equal(t, 1, backing.callCount())
}

func TestCache_GetMembers_DoesNotCacheErrors(t *testing.T) {
	ctx := context.Background()
	backing := &countingMembersStore{err: chat.ErrChatNotFound}
	c := cache.NewInCache(backing)

	// A not-found is re-queried: the DM may be created later.
	chatID := generateDmChatID()
	for i := 0; i < 2; i++ {
		_, err := c.GetMembers(ctx, chatID)
		require.ErrorIs(t, err, chat.ErrChatNotFound)
	}
	require.Equal(t, 2, backing.callCount())

	// Once it exists, its members are served and then held.
	backing.err = nil
	backing.members = []*commonpb.UserId{model.MustGenerateUserID(), model.MustGenerateUserID()}
	for i := 0; i < 2; i++ {
		members, err := c.GetMembers(ctx, chatID)
		require.NoError(t, err)
		require.Len(t, members, 2)
	}
	require.Equal(t, 3, backing.callCount())
}

func TestCache_GetMembers_GroupNeverCached(t *testing.T) {
	ctx := context.Background()
	backing := &countingMembersStore{members: []*commonpb.UserId{model.MustGenerateUserID()}}
	c := cache.NewInCache(backing)

	// A group's roster is mutable, so every read goes to the backing store.
	chatID := chat.MustGenerateGroupChatID()
	for i := 0; i < 3; i++ {
		members, err := c.GetMembers(ctx, chatID)
		require.NoError(t, err)
		require.Len(t, members, 1)
	}
	require.Equal(t, 3, backing.callCount())
}

func TestCache_GetGroupRules_Cached(t *testing.T) {
	ctx := context.Background()
	staffOnly := &chatpb.Rules{Listener: []*chatpb.ListenerRules{{Kind: &chatpb.ListenerRules_Staff{Staff: &chatpb.StaffRequirement{}}}}}
	backing := &countingRulesStore{rules: staffOnly}
	c := cache.NewInCache(backing)

	chatID := chat.MustGenerateGroupChatID()
	for i := 0; i < 3; i++ {
		rules, err := c.GetGroupRules(ctx, chatID)
		require.NoError(t, err)
		require.True(t, proto.Equal(staffOnly, rules))
	}
	require.Equal(t, 1, backing.callCount())

	// Rules are fixed at creation, so the cache is what answers even once the
	// backing store would say otherwise.
	backing.rules = nil
	rules, err := c.GetGroupRules(ctx, chatID)
	require.NoError(t, err)
	require.True(t, proto.Equal(staffOnly, rules))
	require.Equal(t, 1, backing.callCount())

	// The absence of rules is cached too: a group without any is read once.
	plainID := chat.MustGenerateGroupChatID()
	for i := 0; i < 2; i++ {
		rules, err := c.GetGroupRules(ctx, plainID)
		require.NoError(t, err)
		require.Nil(t, rules)
	}
	require.Equal(t, 2, backing.callCount())
}

func TestCache_GetGroupRules_DoesNotCacheErrors(t *testing.T) {
	ctx := context.Background()
	backing := &countingRulesStore{err: chat.ErrChatNotFound}
	c := cache.NewInCache(backing)

	// A not-found is re-queried: the group may be created later.
	chatID := chat.MustGenerateGroupChatID()
	for i := 0; i < 2; i++ {
		_, err := c.GetGroupRules(ctx, chatID)
		require.ErrorIs(t, err, chat.ErrChatNotFound)
	}
	require.Equal(t, 2, backing.callCount())

	// Once it exists, its rules are served and then held.
	backing.err = nil
	for i := 0; i < 2; i++ {
		rules, err := c.GetGroupRules(ctx, chatID)
		require.NoError(t, err)
		require.Nil(t, rules)
	}
	require.Equal(t, 3, backing.callCount())
}

func TestCache_IsMember_CachesPositive(t *testing.T) {
	ctx := context.Background()
	backing := &countingStore{result: true}
	c := cache.NewInCache(backing)

	chatID := generateDmChatID()
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

	chatID := generateDmChatID()
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

	chatA := generateDmChatID()
	chatB := generateDmChatID()
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

func generateDmChatID() *commonpb.ChatId {
	b := make([]byte, chat.DmChatIDSize)
	if _, err := rand.Read(b); err != nil {
		panic(err)
	}
	return &commonpb.ChatId{Value: b}
}
