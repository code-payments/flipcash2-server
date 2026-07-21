package chat

import (
	"bytes"
	"crypto/sha256"
	"testing"

	"github.com/stretchr/testify/require"

	chatpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/chat/v1"
	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"

	"github.com/code-payments/flipcash2-server/model"
)

func TestMustDeriveDmChatID(t *testing.T) {
	a := model.MustGenerateUserID()
	b := model.MustGenerateUserID()

	// Deterministic and order-independent.
	id := MustDeriveDmChatID(chatpb.ChatType_CONTACT_DM, a, b)
	require.Equal(t, id.Value, MustDeriveDmChatID(chatpb.ChatType_CONTACT_DM, a, b).Value)
	require.Equal(t, id.Value, MustDeriveDmChatID(chatpb.ChatType_CONTACT_DM, b, a).Value)
	require.Len(t, id.Value, ChatIDSize)

	// Distinct pairs derive distinct IDs.
	c := model.MustGenerateUserID()
	require.NotEqual(t, id.Value, MustDeriveDmChatID(chatpb.ChatType_CONTACT_DM, a, c).Value)

	// Distinct DM types derive distinct IDs for the same pair.
	tipID := MustDeriveDmChatID(chatpb.ChatType_TIP_DM, a, b)
	require.NotEqual(t, id.Value, tipID.Value)
	require.Equal(t, tipID.Value, MustDeriveDmChatID(chatpb.ChatType_TIP_DM, b, a).Value)

	// A self-DM collapses to a single member and is still derivable.
	self := MustDeriveDmChatID(chatpb.ChatType_CONTACT_DM, a, a)
	require.Len(t, self.Value, ChatIDSize)
	require.NotEqual(t, id.Value, self.Value)

	// An unspecified chat type is a programming error.
	require.Panics(t, func() {
		MustDeriveDmChatID(chatpb.ChatType_UNKNOWN, a, b)
	})

	// A malformed (non-UUID) user ID is a programming error.
	require.Panics(t, func() {
		MustDeriveDmChatID(chatpb.ChatType_CONTACT_DM, a, &commonpb.UserId{Value: []byte("short")})
	})
}

func TestDeriveDmChatType(t *testing.T) {
	a := model.MustGenerateUserID()
	b := model.MustGenerateUserID()
	members := []*commonpb.UserId{a, b}

	// Each DM type's canonical ID maps back to that type, regardless of member
	// order.
	for _, chatType := range []chatpb.ChatType{chatpb.ChatType_CONTACT_DM, chatpb.ChatType_TIP_DM} {
		id := MustDeriveDmChatID(chatType, a, b)
		require.Equal(t, chatType, DeriveDmChatType(id, members))
		require.Equal(t, chatType, DeriveDmChatType(id, []*commonpb.UserId{b, a}))
	}

	// A self-DM's single collapsed member still recovers the type.
	selfID := MustDeriveDmChatID(chatpb.ChatType_CONTACT_DM, a, a)
	require.Equal(t, chatpb.ChatType_CONTACT_DM, DeriveDmChatType(selfID, []*commonpb.UserId{a}))

	// Anything that isn't a derivable DM resolves to UNKNOWN rather than
	// erroring: a random ID, mismatched members, too many members, a malformed
	// member, or a malformed chat ID.
	contactID := MustDeriveDmChatID(chatpb.ChatType_CONTACT_DM, a, b)
	require.Equal(t, chatpb.ChatType_UNKNOWN, DeriveDmChatType(&commonpb.ChatId{Value: make([]byte, ChatIDSize)}, members))
	require.Equal(t, chatpb.ChatType_UNKNOWN, DeriveDmChatType(contactID, []*commonpb.UserId{a, model.MustGenerateUserID()}))
	require.Equal(t, chatpb.ChatType_UNKNOWN, DeriveDmChatType(contactID, []*commonpb.UserId{a, b, model.MustGenerateUserID()}))
	require.Equal(t, chatpb.ChatType_UNKNOWN, DeriveDmChatType(contactID, []*commonpb.UserId{a, {Value: []byte("short")}}))
	require.Equal(t, chatpb.ChatType_UNKNOWN, DeriveDmChatType(&commonpb.ChatId{Value: []byte("short")}, members))
	require.Equal(t, chatpb.ChatType_UNKNOWN, DeriveDmChatType(nil, members))
}

// TestMustDeriveDmChatID_Domains pins the exact hash input per DM type. Contact
// DMs must keep the bare legacy domain forever: existing chat IDs are persisted
// and clients derive them independently, so a domain change would orphan every
// contact DM.
func TestMustDeriveDmChatID_Domains(t *testing.T) {
	a := model.MustGenerateUserID()
	b := model.MustGenerateUserID()

	sorted := [][]byte{a.Value, b.Value}
	if bytes.Compare(sorted[0], sorted[1]) > 0 {
		sorted[0], sorted[1] = sorted[1], sorted[0]
	}

	expected := func(domain string) []byte {
		h := sha256.New()
		h.Write([]byte(domain))
		for _, m := range sorted {
			h.Write(m)
		}
		return h.Sum(nil)
	}

	require.Equal(t, expected("flipcash:chat:dm"), MustDeriveDmChatID(chatpb.ChatType_CONTACT_DM, a, b).Value)
	require.Equal(t, expected("flipcash:chat:dm:2"), MustDeriveDmChatID(chatpb.ChatType_TIP_DM, a, b).Value)
}
