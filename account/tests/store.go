package tests

import (
	"context"
	"testing"

	"github.com/mr-tron/base58"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"
	"github.com/code-payments/flipcash2-server/account"
	"github.com/code-payments/flipcash2-server/model"
	"github.com/code-payments/flipcash2-server/protoutil"
)

func RunStoreTests(t *testing.T, s account.Store, teardown func()) {
	for _, tf := range []func(t *testing.T, s account.Store){
		testStore_keyManagement,
		testStore_batchGetUserIds,
		testStore_registrationStatus,
	} {
		tf(t, s)
		teardown()
	}
}

func testStore_keyManagement(t *testing.T, s account.Store) {
	ctx := context.Background()

	user := model.MustGenerateUserID()

	keyPair := model.MustGenerateKeyPair().Proto()

	_, err := s.GetUserId(ctx, keyPair)
	require.ErrorIs(t, err, account.ErrNotFound)

	authorized, err := s.IsAuthorized(ctx, user, keyPair)
	require.NoError(t, err)
	require.False(t, authorized)

	actualUser, err := s.Bind(ctx, user, keyPair)
	require.NoError(t, err)
	require.True(t, proto.Equal(user, actualUser))

	actualUser, err = s.GetUserId(ctx, keyPair)
	require.NoError(t, err)
	require.True(t, proto.Equal(user, actualUser))

	actualUser, err = s.Bind(ctx, model.MustGenerateUserID(), keyPair)
	require.NoError(t, err)
	require.True(t, proto.Equal(user, actualUser))

	_, err = s.Bind(ctx, user, model.MustGenerateKeyPair().Proto())
	require.Equal(t, account.ErrManyPublicKeys, err)

	authorized, err = s.IsAuthorized(ctx, user, keyPair)
	require.NoError(t, err)
	require.True(t, authorized)

	authorized, err = s.IsAuthorized(ctx, model.MustGenerateUserID(), keyPair)
	require.NoError(t, err)
	require.False(t, authorized)

	actualKeyPairs, err := s.GetPubKeys(ctx, user)
	require.NoError(t, err)
	require.NoError(t, protoutil.SetEqualError([]*commonpb.PublicKey{keyPair}, actualKeyPairs))

}

func testStore_batchGetUserIds(t *testing.T, s account.Store) {
	ctx := context.Background()

	// Create several users with bound keys
	user1 := model.MustGenerateUserID()
	key1 := model.MustGenerateKeyPair().Proto()
	_, err := s.Bind(ctx, user1, key1)
	require.NoError(t, err)

	user2 := model.MustGenerateUserID()
	key2 := model.MustGenerateKeyPair().Proto()
	_, err = s.Bind(ctx, user2, key2)
	require.NoError(t, err)

	unknownKey := model.MustGenerateKeyPair().Proto()

	// Empty input returns empty map
	res, err := s.GetUserIds(ctx, nil)
	require.NoError(t, err)
	require.Empty(t, res)

	// Batch lookup with a mix of known and unknown keys
	res, err = s.GetUserIds(ctx, []*commonpb.PublicKey{key1, key2, unknownKey})
	require.NoError(t, err)
	require.Len(t, res, 2)
	require.True(t, proto.Equal(user1, res[base58.Encode(key1.Value)]))
	require.True(t, proto.Equal(user2, res[base58.Encode(key2.Value)]))
	_, exists := res[base58.Encode(unknownKey.Value)]
	require.False(t, exists)
}

func testStore_registrationStatus(t *testing.T, s account.Store) {
	ctx := context.Background()

	user := model.MustGenerateUserID()

	isRegistered, err := s.IsRegistered(ctx, user)
	require.Nil(t, err)
	require.False(t, isRegistered)

	require.Equal(t, account.ErrNotFound, s.SetRegistrationFlag(ctx, user, true))

	user, err = s.Bind(ctx, user, model.MustGenerateKeyPair().Proto())
	require.NoError(t, err)

	isRegistered, err = s.IsRegistered(ctx, user)
	require.Nil(t, err)
	require.False(t, isRegistered)

	require.NoError(t, s.SetRegistrationFlag(ctx, user, true))

	isRegistered, err = s.IsRegistered(ctx, user)
	require.Nil(t, err)
	require.True(t, isRegistered)

	require.NoError(t, s.SetRegistrationFlag(ctx, user, false))

	isRegistered, err = s.IsRegistered(ctx, user)
	require.Nil(t, err)
	require.False(t, isRegistered)
}
