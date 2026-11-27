package cache

import (
	"context"
	"testing"

	"github.com/mr-tron/base58"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"

	"github.com/code-payments/flipcash2-server/account"
	"github.com/code-payments/flipcash2-server/account/memory"
	"github.com/code-payments/flipcash2-server/model"
)

// countingStore records which users each key lookup, and which keys each user
// lookup, asks the DB about.
type countingStore struct {
	account.Store
	asked     [][]*commonpb.UserId
	askedKeys [][]*commonpb.PublicKey
}

func (s *countingStore) GetUserIds(ctx context.Context, pubKeys []*commonpb.PublicKey) (map[string]*commonpb.UserId, error) {
	s.askedKeys = append(s.askedKeys, pubKeys)
	return s.Store.GetUserIds(ctx, pubKeys)
}

func (s *countingStore) GetPubKeys(ctx context.Context, userID *commonpb.UserId) ([]*commonpb.PublicKey, error) {
	s.asked = append(s.asked, []*commonpb.UserId{userID})
	return s.Store.GetPubKeys(ctx, userID)
}

func (s *countingStore) GetPubKeysForUsers(ctx context.Context, userIDs []*commonpb.UserId) (map[string][]*commonpb.PublicKey, error) {
	s.asked = append(s.asked, userIDs)
	return s.Store.GetPubKeysForUsers(ctx, userIDs)
}

func TestCache_PubKeys(t *testing.T) {
	ctx := context.Background()

	db := &countingStore{Store: memory.NewInMemory()}
	c := NewInCache(db)

	user1, key1 := model.MustGenerateUserID(), model.MustGenerateKeyPair().Proto()
	user2, key2 := model.MustGenerateUserID(), model.MustGenerateKeyPair().Proto()
	unbound := model.MustGenerateUserID()
	for _, binding := range []struct {
		user *commonpb.UserId
		key  *commonpb.PublicKey
	}{{user1, key1}, {user2, key2}} {
		_, err := c.Bind(ctx, binding.user, binding.key)
		require.NoError(t, err)
	}

	// A single lookup reads the DB, and a repeat is served from the cache.
	for range 2 {
		pubKeys, err := c.GetPubKeys(ctx, user1)
		require.NoError(t, err)
		require.Len(t, pubKeys, 1)
		require.True(t, proto.Equal(key1, pubKeys[0]))
	}
	require.Len(t, db.asked, 1)

	// A batch asks the DB only about the users not cached.
	res, err := c.GetPubKeysForUsers(ctx, []*commonpb.UserId{user1, user2, unbound})
	require.NoError(t, err)
	require.Len(t, res, 2)
	require.True(t, proto.Equal(key1, res[string(user1.Value)][0]))
	require.True(t, proto.Equal(key2, res[string(user2.Value)][0]))
	require.Len(t, db.asked, 2)
	requireUsers(t, []*commonpb.UserId{user2, unbound}, db.asked[1])

	// A user found unbound is not cached, so a later batch finds their key
	// once they bind; the bound users stay cached.
	key3 := model.MustGenerateKeyPair().Proto()
	_, err = c.Bind(ctx, unbound, key3)
	require.NoError(t, err)

	res, err = c.GetPubKeysForUsers(ctx, []*commonpb.UserId{user1, user2, unbound})
	require.NoError(t, err)
	require.Len(t, res, 3)
	require.True(t, proto.Equal(key3, res[string(unbound.Value)][0]))
	require.Len(t, db.asked, 3)
	requireUsers(t, []*commonpb.UserId{unbound}, db.asked[2])

	// Everyone is cached now: no DB read at all.
	res, err = c.GetPubKeysForUsers(ctx, []*commonpb.UserId{user1, user2, unbound})
	require.NoError(t, err)
	require.Len(t, res, 3)
	require.Len(t, db.asked, 3)

	// Results are copies: mutating one doesn't corrupt the cache.
	res[string(user1.Value)][0].Value = []byte("mutated")
	pubKeys, err := c.GetPubKeys(ctx, user1)
	require.NoError(t, err)
	require.True(t, proto.Equal(key1, pubKeys[0]))
}

func TestCache_UserIds(t *testing.T) {
	ctx := context.Background()

	db := &countingStore{Store: memory.NewInMemory()}
	c := NewInCache(db)

	user1, key1 := model.MustGenerateUserID(), model.MustGenerateKeyPair().Proto()
	user2, key2 := model.MustGenerateUserID(), model.MustGenerateKeyPair().Proto()
	user3, unbound := model.MustGenerateUserID(), model.MustGenerateKeyPair().Proto()
	for _, binding := range []struct {
		user *commonpb.UserId
		key  *commonpb.PublicKey
	}{{user1, key1}, {user2, key2}} {
		_, err := c.Bind(ctx, binding.user, binding.key)
		require.NoError(t, err)
	}

	// An empty batch reads nothing.
	res, err := c.GetUserIds(ctx, nil)
	require.NoError(t, err)
	require.Empty(t, res)
	require.Empty(t, db.askedKeys)

	// A key cached by a single lookup serves a batch, which asks the DB only
	// about the rest.
	_, err = c.GetUserId(ctx, key1)
	require.NoError(t, err)

	res, err = c.GetUserIds(ctx, []*commonpb.PublicKey{key1, key2, unbound})
	require.NoError(t, err)
	require.Len(t, res, 2)
	require.True(t, proto.Equal(user1, res[base58.Encode(key1.Value)]))
	require.True(t, proto.Equal(user2, res[base58.Encode(key2.Value)]))
	require.Len(t, db.askedKeys, 1)
	requireKeys(t, []*commonpb.PublicKey{key2, unbound}, db.askedKeys[0])

	// A key found unbound is not cached, so a later batch finds its user once
	// it is bound; the bound keys stay cached.
	_, err = c.Bind(ctx, user3, unbound)
	require.NoError(t, err)

	res, err = c.GetUserIds(ctx, []*commonpb.PublicKey{key1, key2, unbound})
	require.NoError(t, err)
	require.Len(t, res, 3)
	require.True(t, proto.Equal(user3, res[base58.Encode(unbound.Value)]))
	require.Len(t, db.askedKeys, 2)
	requireKeys(t, []*commonpb.PublicKey{unbound}, db.askedKeys[1])

	// Every key is cached now: no DB read at all, and the single lookup is
	// served from the same cache.
	res, err = c.GetUserIds(ctx, []*commonpb.PublicKey{key1, key2, unbound})
	require.NoError(t, err)
	require.Len(t, res, 3)
	require.Len(t, db.askedKeys, 2)

	// Results are copies: mutating one doesn't corrupt the cache.
	res[base58.Encode(key2.Value)].Value = []byte("mutated")
	userID, err := c.GetUserId(ctx, key2)
	require.NoError(t, err)
	require.True(t, proto.Equal(user2, userID))
}

func requireKeys(t *testing.T, expected, actual []*commonpb.PublicKey) {
	require.Len(t, actual, len(expected))
	for i := range expected {
		require.True(t, proto.Equal(expected[i], actual[i]))
	}
}

func requireUsers(t *testing.T, expected, actual []*commonpb.UserId) {
	require.Len(t, actual, len(expected))
	for i := range expected {
		require.True(t, proto.Equal(expected[i], actual[i]))
	}
}
