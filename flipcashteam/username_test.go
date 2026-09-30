package flipcashteam_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/code-payments/flipcash2-server/flipcashteam"
	"github.com/code-payments/flipcash2-server/model"
	"github.com/code-payments/flipcash2-server/profile"
	profile_memory "github.com/code-payments/flipcash2-server/profile/memory"
)

func TestUsername_ReservedAndValid(t *testing.T) {
	// Only AssignUsername can give the handle out: the SetUsername RPC refuses a
	// reserved word, and the store accepts only a valid one.
	require.True(t, profile.IsUsernameReserved(flipcashteam.Username))
	require.NoError(t, profile.ValidateUsername(flipcashteam.Username))
}

func TestAssignUsername(t *testing.T) {
	ctx := context.Background()
	profiles := profile_memory.NewInMemory()
	team := model.MustGenerateUserID()
	other := model.MustGenerateUserID()

	_, err := flipcashteam.GetUserID(ctx, profiles)
	require.ErrorIs(t, err, profile.ErrNotFound)

	require.NoError(t, flipcashteam.AssignUsername(ctx, profiles, team))
	got, err := flipcashteam.GetUserID(ctx, profiles)
	require.NoError(t, err)
	require.Equal(t, team.Value, got.Value)

	// Assigning it again to its holder changes nothing.
	require.NoError(t, flipcashteam.AssignUsername(ctx, profiles, team))

	// Nobody else can take it.
	require.ErrorIs(t, flipcashteam.AssignUsername(ctx, profiles, other), profile.ErrUsernameTaken)
}
