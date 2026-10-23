package tests

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"
	messagingpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/messaging/v1"

	"github.com/code-payments/flipcash2-server/badge"
	"github.com/code-payments/flipcash2-server/blocklist"
	"github.com/code-payments/flipcash2-server/chat"
	"github.com/code-payments/flipcash2-server/messaging"
	"github.com/code-payments/flipcash2-server/profile"
)

// testServer_SilencedMember pins what a silence takes from a group member,
// public or private: every send, edit and typing notification, refused on the
// very next ask; and what it leaves them: reading, reacting, and deleting
// their own messages. Lifted, they speak again at once.
func testServer_SilencedMember(t *testing.T, badges badge.Store, blocklists blocklist.Store, chats chat.Store, messages messaging.Store, profiles profile.Store) {
	e := newServerEnv(t, badges, blocklists, chats, messages, profiles)
	e.useOpenGroup(chats)
	public := e.chatID
	private := e.putPrivateGroup(chats, e.userB)
	e.storeChatKey(chats, private, e.userA)

	for _, tc := range []struct {
		chatID  *commonpb.ChatId
		content func(byte) []*messagingpb.Content
	}{
		{public, func(b byte) []*messagingpb.Content { return textContent(string('a' + rune(b))) }},
		{private, chatKeyEncryptedContent},
	} {
		chatID := tc.chatID

		// Two messages from B before the silence.
		var sent []*messagingpb.Message
		for i := byte(1); i <= 2; i++ {
			resp, err := e.sendContentToChat(e.keysB, chatID, tc.content(i), generateClientID())
			require.NoError(t, err)
			require.Equal(t, messagingpb.SendMessageResponse_OK, resp.Result)
			sent = append(sent, resp.Message)
		}

		_, _, err := chats.SetSilence(e.ctx, chatID, e.userB, chat.Silence{Forever: true}, time.Now())
		require.NoError(t, err)

		// No send, edit or typing.
		sendResp, err := e.sendContentToChat(e.keysB, chatID, tc.content(3), generateClientID())
		require.NoError(t, err)
		require.Equal(t, messagingpb.SendMessageResponse_DENIED, sendResp.Result)
		editResp, err := e.editMessageInChat(e.keysB, chatID, sent[0].MessageId, tc.content(4), sent[1].EventSequence)
		require.NoError(t, err)
		require.Equal(t, messagingpb.EditMessageResponse_DENIED, editResp.Result)
		typingResp, err := e.notifyIsTypingInChat(e.keysB, chatID, messagingpb.IsTypingNotification_STARTED_TYPING)
		require.NoError(t, err)
		require.Equal(t, messagingpb.NotifyIsTypingResponse_DENIED, typingResp.Result)

		// Reading, reacting and deleting their own message stand.
		getResp, err := e.getMessagesByOptionsInChat(e.keysB, chatID, nil)
		require.NoError(t, err)
		require.Equal(t, messagingpb.GetMessagesResponse_OK, getResp.Result)
		reactResp, err := e.addReactionInChat(e.keysB, chatID, sent[0].MessageId, "👍")
		require.NoError(t, err)
		require.Equal(t, messagingpb.AddReactionResponse_OK, reactResp.Result)
		deleteResp, err := e.deleteMessageInChat(e.keysB, chatID, sent[1].MessageId, sent[1].EventSequence)
		require.NoError(t, err)
		require.Equal(t, messagingpb.DeleteMessageResponse_OK, deleteResp.Result)
		// Not someone else's: the carve-out is for their own messages.
		other, err := e.sendContentToChat(e.keysA, chatID, tc.content(5), generateClientID())
		require.NoError(t, err)
		require.Equal(t, messagingpb.SendMessageResponse_OK, other.Result)
		deleteResp, err = e.deleteMessageInChat(e.keysB, chatID, other.Message.MessageId, other.Message.EventSequence)
		require.NoError(t, err)
		require.Equal(t, messagingpb.DeleteMessageResponse_DENIED, deleteResp.Result)

		// A silence that has lapsed takes nothing.
		_, _, err = chats.SetSilence(e.ctx, chatID, e.userB, chat.Silence{Until: time.Now().Add(-time.Minute).Truncate(time.Second)}, time.Now())
		require.NoError(t, err)
		sendResp, err = e.sendContentToChat(e.keysB, chatID, tc.content(6), generateClientID())
		require.NoError(t, err)
		require.Equal(t, messagingpb.SendMessageResponse_OK, sendResp.Result)

		// Silenced again, then lifted: they speak at once.
		_, _, err = chats.SetSilence(e.ctx, chatID, e.userB, chat.Silence{Forever: true}, time.Now())
		require.NoError(t, err)
		sendResp, err = e.sendContentToChat(e.keysB, chatID, tc.content(7), generateClientID())
		require.NoError(t, err)
		require.Equal(t, messagingpb.SendMessageResponse_DENIED, sendResp.Result)
		_, _, err = chats.ClearSilence(e.ctx, chatID, e.userB)
		require.NoError(t, err)
		sendResp, err = e.sendContentToChat(e.keysB, chatID, tc.content(8), generateClientID())
		require.NoError(t, err)
		require.Equal(t, messagingpb.SendMessageResponse_OK, sendResp.Result)
	}
}

// testServer_BannedNonMember_Reads pins what a banned non-member reads of an
// open public group: no full read, the redacted one anyone has, and the
// reaction overlay a redacted reader sees. Lifted, the full read is back.
func testServer_BannedNonMember_Reads(t *testing.T, badges badge.Store, blocklists blocklist.Store, chats chat.Store, messages messaging.Store, profiles profile.Store) {
	e := newServerEnv(t, badges, blocklists, chats, messages, profiles)
	e.useOpenGroup(chats)
	sent, err := e.send(e.keysA, "hello", generateClientID())
	require.NoError(t, err)
	require.Equal(t, messagingpb.SendMessageResponse_OK, sent.Result)

	stranger, strangerKeys := e.addUser()
	full, err := e.getMessagesByOptionsInChatWithMode(strangerKeys, e.chatID, nil, messagingpb.ViewMode_FULL)
	require.NoError(t, err)
	require.Equal(t, messagingpb.GetMessagesResponse_OK, full.Result)
	require.False(t, full.Messages.Messages[0].Redacted)

	_, err = chats.BanUser(e.ctx, e.chatID, stranger, time.Now(), false)
	require.NoError(t, err)

	full, err = e.getMessagesByOptionsInChatWithMode(strangerKeys, e.chatID, nil, messagingpb.ViewMode_FULL)
	require.NoError(t, err)
	require.Equal(t, messagingpb.GetMessagesResponse_DENIED, full.Result)
	one, err := e.getMessageInChatWithMode(strangerKeys, e.chatID, sent.Message.MessageId, messagingpb.ViewMode_FULL)
	require.NoError(t, err)
	require.Equal(t, messagingpb.GetMessageResponse_DENIED, one.Result)
	for _, mode := range []messagingpb.ViewMode{messagingpb.ViewMode_FULL_OR_REDACTED, messagingpb.ViewMode_REDACTED} {
		redacted, err := e.getMessagesByOptionsInChatWithMode(strangerKeys, e.chatID, nil, mode)
		require.NoError(t, err)
		require.Equal(t, messagingpb.GetMessagesResponse_OK, redacted.Result, mode)
		require.True(t, redacted.Messages.Messages[0].Redacted, mode)
	}
	summary, err := e.getReactionSummaryInChat(strangerKeys, e.chatID, sent.Message.MessageId)
	require.NoError(t, err)
	require.Equal(t, messagingpb.GetReactionSummaryResponse_OK, summary.Result)

	_, _, err = chats.UnbanUser(e.ctx, e.chatID, stranger)
	require.NoError(t, err)
	full, err = e.getMessagesByOptionsInChatWithMode(strangerKeys, e.chatID, nil, messagingpb.ViewMode_FULL)
	require.NoError(t, err)
	require.Equal(t, messagingpb.GetMessagesResponse_OK, full.Result)
	require.False(t, full.Messages.Messages[0].Redacted)
}
