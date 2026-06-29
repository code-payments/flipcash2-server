package tests

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"
	pushpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/push/v1"

	"github.com/code-payments/flipcash2-server/push"
)

func RunStoreTests(t *testing.T, s push.Store, teardown func()) {
	for _, tf := range []func(t *testing.T, s push.Store){
		testAddAndGetTokens,
		testUpdateExistingToken,
		testDeleteToken,
		testMultipleUsers,
		testFilterUsersWithTokens,
		testClaimGainPush,
	} {
		tf(t, s)
		teardown()
	}
}

func testAddAndGetTokens(t *testing.T, store push.Store) {
	ctx := context.Background()

	userID := &commonpb.UserId{Value: []byte("user1")}
	appInstallID1 := &commonpb.AppInstallId{Value: "device1"}
	appInstallID2 := &commonpb.AppInstallId{Value: "device2"}

	// Initially no tokens
	tokens, err := store.GetTokens(ctx, userID)
	require.NoError(t, err)
	assert.Empty(t, tokens)

	// Add tokens for two devices
	err = store.AddToken(ctx, userID, appInstallID1, pushpb.TokenType_FCM_APNS, "token1")
	require.NoError(t, err)

	err = store.AddToken(ctx, userID, appInstallID2, pushpb.TokenType_FCM_APNS, "token2")
	require.NoError(t, err)

	// Verify both tokens are retrieved
	tokens, err = store.GetTokens(ctx, userID)
	require.NoError(t, err)
	assert.Len(t, tokens, 2)

	// Verify token contents
	tokenMap := make(map[string]push.Token)
	for _, token := range tokens {
		tokenMap[token.AppInstallID] = token
	}

	assert.Equal(t, "token1", tokenMap[appInstallID1.Value].Token)
	assert.Equal(t, "token2", tokenMap[appInstallID2.Value].Token)
}

func testUpdateExistingToken(t *testing.T, store push.Store) {
	ctx := context.Background()
	userID := &commonpb.UserId{Value: []byte("user1")}
	appInstallID := &commonpb.AppInstallId{Value: "device1"}

	// Add initial token
	err := store.AddToken(ctx, userID, appInstallID, pushpb.TokenType_FCM_APNS, "token1")
	require.NoError(t, err)

	// Update token
	err = store.AddToken(ctx, userID, appInstallID, pushpb.TokenType_FCM_APNS, "token2")
	require.NoError(t, err)

	// Verify updated token
	tokens, err := store.GetTokens(ctx, userID)
	require.NoError(t, err)
	assert.Len(t, tokens, 1)
	assert.Equal(t, "token2", tokens[0].Token)
}

func testDeleteToken(t *testing.T, store push.Store) {
	ctx := context.Background()

	userID := &commonpb.UserId{Value: []byte("user1")}
	appInstallID := &commonpb.AppInstallId{Value: "device1"}

	// Add tokens
	err := store.AddToken(ctx, userID, appInstallID, pushpb.TokenType_FCM_APNS, "token1")
	require.NoError(t, err)
	err = store.AddToken(ctx, userID, appInstallID, pushpb.TokenType_FCM_APNS, "token2")
	require.NoError(t, err)

	// Delete token
	err = store.DeleteToken(ctx, pushpb.TokenType_FCM_APNS, "token1")
	require.NoError(t, err)

	// Verify token is deleted
	tokens, err := store.GetTokens(ctx, userID)
	require.NoError(t, err)
	assert.Len(t, tokens, 1)
	assert.Equal(t, "token2", tokens[0].Token)
}

func testMultipleUsers(t *testing.T, store push.Store) {
	ctx := context.Background()

	user1 := &commonpb.UserId{Value: []byte("user1")}
	user2 := &commonpb.UserId{Value: []byte("user2")}
	appInstallID := &commonpb.AppInstallId{Value: "device1"}

	// Add tokens for both users
	err := store.AddToken(ctx, user1, appInstallID, pushpb.TokenType_FCM_APNS, "token1")
	require.NoError(t, err)

	err = store.AddToken(ctx, user2, appInstallID, pushpb.TokenType_FCM_APNS, "token2")
	require.NoError(t, err)

	// Verify user1's tokens
	tokens1, err := store.GetTokens(ctx, user1)
	require.NoError(t, err)
	assert.Len(t, tokens1, 1)
	assert.Equal(t, "token1", tokens1[0].Token)

	// Verify user2's tokens
	tokens2, err := store.GetTokens(ctx, user2)
	require.NoError(t, err)
	assert.Len(t, tokens2, 1)
	assert.Equal(t, "token2", tokens2[0].Token)

	// Get all users' tokens in a batch
	tokens, err := store.GetTokensBatch(ctx, user1, user2)
	require.NoError(t, err)
	assert.Len(t, tokens, 2)

	// Verify token contents
	tokenMap := make(map[string]push.Token)
	for _, token := range tokens {
		tokenMap[token.Token] = token
	}

	assert.Equal(t, tokens1[0], tokenMap["token1"])
	assert.Equal(t, tokens2[0], tokenMap["token2"])
}

func testFilterUsersWithTokens(t *testing.T, store push.Store) {
	ctx := context.Background()

	user1 := &commonpb.UserId{Value: []byte("user1")}
	user2 := &commonpb.UserId{Value: []byte("user2")}
	user3 := &commonpb.UserId{Value: []byte("user3")}
	appInstallID := &commonpb.AppInstallId{Value: "device1"}

	// No users have tokens yet
	filtered, err := store.FilterUsersWithTokens(ctx, user1, user2, user3)
	require.NoError(t, err)
	assert.Empty(t, filtered)

	// Add tokens for user1 and user3 only
	err = store.AddToken(ctx, user1, appInstallID, pushpb.TokenType_FCM_APNS, "token1")
	require.NoError(t, err)
	err = store.AddToken(ctx, user3, appInstallID, pushpb.TokenType_FCM_APNS, "token3")
	require.NoError(t, err)

	// Filter should return only user1 and user3
	filtered, err = store.FilterUsersWithTokens(ctx, user1, user2, user3)
	require.NoError(t, err)
	assert.Len(t, filtered, 2)

	filteredValues := make(map[string]struct{})
	for _, u := range filtered {
		filteredValues[string(u.Value)] = struct{}{}
	}
	assert.Contains(t, filteredValues, string(user1.Value))
	assert.Contains(t, filteredValues, string(user3.Value))
	assert.NotContains(t, filteredValues, string(user2.Value))

	// Empty input returns nil
	filtered, err = store.FilterUsersWithTokens(ctx)
	require.NoError(t, err)
	assert.Empty(t, filtered)
}

func testClaimGainPush(t *testing.T, store push.Store) {
	ctx := context.Background()

	mint := &commonpb.PublicKey{Value: []byte("mint1")}
	otherMint := &commonpb.PublicKey{Value: []byte("mint2")}
	const bigCooldown = time.Hour

	// No state recorded yet.
	_, err := store.GetCurrencyState(ctx, mint)
	assert.ErrorIs(t, err, push.ErrCurrencyStateNotFound)

	// First observation always grants (no prior push), regardless of cooldown. The
	// returned state reflects the freshly written values.
	granted, state, err := store.ClaimGainPush(ctx, mint, 100, 10, bigCooldown)
	require.NoError(t, err)
	assert.True(t, granted)
	require.NotNil(t, state)
	assert.EqualValues(t, 100, state.AllTimeHighSupply)
	assert.EqualValues(t, 10, state.AllTimeHighSlot)
	require.NotNil(t, state.LastGainPushAt)
	firstPushAt := *state.LastGainPushAt

	// A strictly higher supply within the cooldown is NOT granted and must leave
	// the stored all-time high untouched. The returned state reflects the existing
	// (unchanged) stored values, not the rejected observation.
	granted, state, err = store.ClaimGainPush(ctx, mint, 150, 20, bigCooldown)
	require.NoError(t, err)
	assert.False(t, granted)
	require.NotNil(t, state)
	assert.EqualValues(t, 100, state.AllTimeHighSupply)
	assert.EqualValues(t, 10, state.AllTimeHighSlot)
	require.NotNil(t, state.LastGainPushAt)
	assert.True(t, state.LastGainPushAt.Equal(firstPushAt))

	// Once the cooldown has elapsed (zero cooldown here), a strictly higher supply
	// is granted and advances the stored high.
	granted, state, err = store.ClaimGainPush(ctx, mint, 150, 20, 0)
	require.NoError(t, err)
	assert.True(t, granted)
	require.NotNil(t, state)
	assert.EqualValues(t, 150, state.AllTimeHighSupply)
	assert.EqualValues(t, 20, state.AllTimeHighSlot)

	// Equal supply is not a new high; returned state stays at the stored high.
	granted, state, err = store.ClaimGainPush(ctx, mint, 150, 25, 0)
	require.NoError(t, err)
	assert.False(t, granted)
	require.NotNil(t, state)
	assert.EqualValues(t, 150, state.AllTimeHighSupply)
	assert.EqualValues(t, 20, state.AllTimeHighSlot)

	// Lower supply is not a new high; returned state stays at the stored high.
	granted, state, err = store.ClaimGainPush(ctx, mint, 120, 30, 0)
	require.NoError(t, err)
	assert.False(t, granted)
	require.NotNil(t, state)
	assert.EqualValues(t, 150, state.AllTimeHighSupply)
	assert.EqualValues(t, 20, state.AllTimeHighSlot)

	// The stored high is unchanged after the non-grants.
	state, err = store.GetCurrencyState(ctx, mint)
	require.NoError(t, err)
	assert.EqualValues(t, 150, state.AllTimeHighSupply)
	assert.EqualValues(t, 20, state.AllTimeHighSlot)

	// A different mint is tracked independently.
	granted, state, err = store.ClaimGainPush(ctx, otherMint, 1, 1, bigCooldown)
	require.NoError(t, err)
	assert.True(t, granted)
	require.NotNil(t, state)
	assert.EqualValues(t, 1, state.AllTimeHighSupply)
}
