package chat

import (
	"testing"

	"github.com/stretchr/testify/require"

	chatpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/chat/v1"
	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"

	ocp_common "github.com/code-payments/ocp-server/ocp/common"

	"github.com/code-payments/flipcash2-server/model"
)

func TestGatedMint(t *testing.T) {
	mint := model.MustGenerateKeyPair().Proto()
	other := model.MustGenerateKeyPair().Proto()
	core := &commonpb.PublicKey{Value: ocp_common.CoreMintAccount.PublicKey().ToBytes()}

	balance := func(mints ...*commonpb.PublicKey) *MinimumBalance {
		return &MinimumBalance{Currency: "usd", NativeAmount: 1, Mints: mints}
	}
	group := func(configure func(*Chat)) *Chat {
		c := &Chat{ID: MustGenerateGroupChatID(), Type: chatpb.ChatType_GROUP}
		configure(c)
		return c
	}

	for _, tc := range []struct {
		name  string
		chat  *Chat
		gated bool
	}{
		{"listener only", group(func(c *Chat) { c.MinimumListenerBalance = balance(mint) }), true},
		{"speaker only", group(func(c *Chat) { c.MinimumSpeakerBalance = balance(mint) }), true},
		{"listener and speaker on one mint", group(func(c *Chat) {
			c.MinimumListenerBalance = balance(mint)
			c.MinimumSpeakerBalance = &MinimumBalance{Currency: "eur", NativeAmount: 5, Mints: []*commonpb.PublicKey{mint}}
		}), true},
		{"no requirements", group(func(c *Chat) {}), false},
		{"staff only", group(func(c *Chat) { c.IsStaffOnly = true }), false},
		{"every mint", group(func(c *Chat) { c.MinimumListenerBalance = balance() }), false},
		{"listener and speaker on different mints", group(func(c *Chat) {
			c.MinimumListenerBalance = balance(mint)
			c.MinimumSpeakerBalance = balance(other)
		}), false},
		{"one names every mint", group(func(c *Chat) {
			c.MinimumListenerBalance = balance(mint)
			c.MinimumSpeakerBalance = balance()
		}), false},
		{"several mints", group(func(c *Chat) { c.MinimumListenerBalance = balance(mint, other) }), false},
		{"core mint", group(func(c *Chat) { c.MinimumListenerBalance = balance(core) }), false},
		{"private", group(func(c *Chat) {
			c.IsPrivate = true
			c.MinimumListenerBalance = balance(mint)
		}), false},
		{"dm", &Chat{
			ID:                     MustDeriveDmChatID(chatpb.ChatType_DM, model.MustGenerateUserID(), model.MustGenerateUserID()),
			Type:                   chatpb.ChatType_DM,
			MinimumListenerBalance: balance(mint),
		}, false},
	} {
		got, ok := GatedMint(tc.chat)
		require.Equal(t, tc.gated, ok, tc.name)
		if tc.gated {
			require.Equal(t, mint.Value, got.Value, tc.name)
		} else {
			require.Nil(t, got, tc.name)
		}
	}
}
