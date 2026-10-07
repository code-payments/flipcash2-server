package cache

import (
	"bytes"
	"context"

	"github.com/ReneKroon/ttlcache"
	"google.golang.org/protobuf/proto"

	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"

	"github.com/code-payments/flipcash2-server/account"
)

type Cache struct {
	db                  account.Store
	pubkeyToUserCache   *ttlcache.Cache
	userToPubkeysCache  *ttlcache.Cache
	registeredUserCache *ttlcache.Cache
	staffFlagCache      *ttlcache.Cache
}

func NewInCache(db account.Store) account.Store {
	return &Cache{
		db:                  db,
		pubkeyToUserCache:   ttlcache.NewCache(),
		userToPubkeysCache:  ttlcache.NewCache(),
		registeredUserCache: ttlcache.NewCache(),
		staffFlagCache:      ttlcache.NewCache(),
	}
}

func (c *Cache) Bind(ctx context.Context, userID *commonpb.UserId, pubKey *commonpb.PublicKey) (*commonpb.UserId, error) {
	return c.db.Bind(ctx, userID, pubKey)
}

func (c *Cache) GetUserId(ctx context.Context, pubKey *commonpb.PublicKey) (*commonpb.UserId, error) {
	cached, ok := c.pubkeyToUserCache.Get(string(pubKey.Value))
	if !ok {
		userID, err := c.db.GetUserId(ctx, pubKey)
		if err == nil {
			cloned := proto.Clone(userID).(*commonpb.UserId)
			c.pubkeyToUserCache.Set(string(pubKey.Value), cloned)
		}
		return userID, err
	}
	cloned := proto.Clone(cached.(*commonpb.UserId)).(*commonpb.UserId)
	return cloned, nil
}

func (c *Cache) GetUserIds(ctx context.Context, pubKeys []*commonpb.PublicKey) (map[string]*commonpb.UserId, error) {
	return c.db.GetUserIds(ctx, pubKeys)
}

// GetPubKeys caches positive results only: a user's bound key never changes
// once bound (one per user, never unbound), but a user with none may bind one.
func (c *Cache) GetPubKeys(ctx context.Context, userID *commonpb.UserId) ([]*commonpb.PublicKey, error) {
	if cached, ok := c.userToPubkeysCache.Get(string(userID.Value)); ok {
		return clonePubKeys(cached.([]*commonpb.PublicKey)), nil
	}
	pubKeys, err := c.db.GetPubKeys(ctx, userID)
	if err == nil && len(pubKeys) > 0 {
		c.userToPubkeysCache.Set(string(userID.Value), clonePubKeys(pubKeys))
	}
	return pubKeys, err
}

// GetPubKeysForUsers answers what it can from the cache and reads the rest
// from the DB in one batch, caching positive results only (see GetPubKeys).
func (c *Cache) GetPubKeysForUsers(ctx context.Context, userIDs []*commonpb.UserId) (map[string][]*commonpb.PublicKey, error) {
	res := make(map[string][]*commonpb.PublicKey, len(userIDs))
	var misses []*commonpb.UserId
	for _, userID := range userIDs {
		if cached, ok := c.userToPubkeysCache.Get(string(userID.Value)); ok {
			res[string(userID.Value)] = clonePubKeys(cached.([]*commonpb.PublicKey))
			continue
		}
		misses = append(misses, userID)
	}
	if len(misses) == 0 {
		return res, nil
	}

	fetched, err := c.db.GetPubKeysForUsers(ctx, misses)
	if err != nil {
		return nil, err
	}
	for userKey, pubKeys := range fetched {
		if len(pubKeys) > 0 {
			c.userToPubkeysCache.Set(userKey, clonePubKeys(pubKeys))
		}
		res[userKey] = pubKeys
	}
	return res, nil
}

func clonePubKeys(pubKeys []*commonpb.PublicKey) []*commonpb.PublicKey {
	cloned := make([]*commonpb.PublicKey, len(pubKeys))
	for i, pubKey := range pubKeys {
		cloned[i] = proto.Clone(pubKey).(*commonpb.PublicKey)
	}
	return cloned
}

func (c *Cache) IsAuthorized(ctx context.Context, userID *commonpb.UserId, pubKey *commonpb.PublicKey) (bool, error) {
	linkedUserID, err := c.GetUserId(ctx, pubKey)
	if err != nil {
		return false, err
	}
	return bytes.Equal(linkedUserID.Value, userID.Value), nil
}

func (c *Cache) IsStaff(ctx context.Context, userID *commonpb.UserId) (bool, error) {
	cached, ok := c.staffFlagCache.Get(string(userID.Value))
	if !ok {
		isStaff, err := c.db.IsStaff(ctx, userID)
		if err == nil {
			c.staffFlagCache.Set(string(userID.Value), isStaff)
		}
		return isStaff, err
	}
	return cached.(bool), nil
}

func (c *Cache) IsRegistered(ctx context.Context, userID *commonpb.UserId) (bool, error) {
	cached, ok := c.registeredUserCache.Get(string(userID.Value))
	if !ok {
		isRegistered, err := c.db.IsRegistered(ctx, userID)
		if err == nil && isRegistered {
			// Only cache positive results
			c.registeredUserCache.Set(string(userID.Value), isRegistered)
		}
		return isRegistered, err
	}
	return cached.(bool), nil
}

func (c *Cache) SetRegistrationFlag(ctx context.Context, userID *commonpb.UserId, isRegistered bool) error {
	return c.db.SetRegistrationFlag(ctx, userID, isRegistered)
}
