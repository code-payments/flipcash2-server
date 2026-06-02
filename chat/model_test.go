package chat

import (
	"testing"

	"github.com/stretchr/testify/require"

	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"

	"github.com/code-payments/flipcash2-server/model"
)

func TestMustDeriveDmChatID(t *testing.T) {
	a := model.MustGenerateUserID()
	b := model.MustGenerateUserID()

	// Deterministic and order-independent.
	id := MustDeriveDmChatID(a, b)
	require.Equal(t, id.Value, MustDeriveDmChatID(a, b).Value)
	require.Equal(t, id.Value, MustDeriveDmChatID(b, a).Value)
	require.Len(t, id.Value, ChatIDSize)

	// Distinct pairs derive distinct IDs.
	c := model.MustGenerateUserID()
	require.NotEqual(t, id.Value, MustDeriveDmChatID(a, c).Value)

	// A self-DM collapses to a single member and is still derivable.
	self := MustDeriveDmChatID(a, a)
	require.Len(t, self.Value, ChatIDSize)
	require.NotEqual(t, id.Value, self.Value)

	// A malformed (non-UUID) user ID is a programming error.
	require.Panics(t, func() {
		MustDeriveDmChatID(a, &commonpb.UserId{Value: []byte("short")})
	})
}
