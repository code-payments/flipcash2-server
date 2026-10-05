package chat

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	chatpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/chat/v1"
	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"

	"github.com/code-payments/flipcash2-server/model"
)

// TestBlobEncryptedUploadGate: an encrypted upload is admitted for a chat
// that takes encrypted content — a DM, or a private group with its key — and
// only for a caller who may speak in it; every other chat refuses before
// membership is read.
func TestBlobEncryptedUploadGate(t *testing.T) {
	ctx := context.Background()
	f := newAccessFixture(t)
	gate := NewBlobEncryptedUploadGate(NewAccess(f.chats, f.rules))

	can := func(chatID *commonpb.ChatId, userID *commonpb.UserId) bool {
		t.Helper()
		ok, err := gate.CanUploadEncrypted(ctx, chatID, userID)
		require.NoError(t, err)
		return ok
	}

	// A DM: its members, and no one else.
	peer := model.MustGenerateUserID()
	dm := MustDeriveDmChatID(chatpb.ChatType_DM, f.funded, peer)
	f.chats.join(dm, f.funded)
	f.chats.join(dm, peer)
	require.True(t, can(dm, f.funded))
	require.True(t, can(dm, peer))
	require.False(t, can(dm, f.unfunded))

	// A non-member is refused on membership alone: nothing else is read.
	rulesReads, envelopeReads := f.chats.reads, f.chats.envelopeReads
	require.False(t, can(f.gated.ID, f.funded))
	require.Equal(t, rulesReads, f.chats.reads)
	require.Zero(t, f.ocpBalance.asked)

	// A public group takes none, from a member who may speak in it included.
	// Their standing is found in full first — membership, then the rules —
	// so the refusal costs a valuation, which only a client that should not
	// be asking pays.
	f.chats.join(f.gated.ID, f.funded)
	require.False(t, can(f.gated.ID, f.funded))
	require.Equal(t, 1, f.ocpBalance.asked)

	// A private group takes none until its creator stores their envelope, and
	// then takes its members' and no one else's.
	creator := model.MustGenerateUserID()
	private := f.chats.put(&Chat{ID: MustGenerateGroupChatID(), Type: chatpb.ChatType_GROUP, IsPrivate: true, CreatorID: creator})
	f.chats.join(private.ID, creator)
	f.chats.join(private.ID, f.unfunded)
	require.False(t, can(private.ID, creator))
	require.False(t, can(private.ID, f.unfunded))
	f.chats.storeKey(private.ID, creator)
	require.True(t, can(private.ID, creator))
	require.True(t, can(private.ID, f.unfunded))
	envelopeReads = f.chats.envelopeReads
	require.False(t, can(private.ID, f.funded))
	require.Equal(t, envelopeReads, f.chats.envelopeReads)
	require.Equal(t, 1, f.ocpBalance.asked)

	// A chat ID of neither shape, and a group that does not exist.
	require.False(t, can(&commonpb.ChatId{Value: []byte{1, 2, 3}}, f.funded))
	require.False(t, can(MustGenerateGroupChatID(), f.funded))
}
