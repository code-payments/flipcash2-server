package flipcashteam

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"

	chatpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/chat/v1"
	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"
	messagingpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/messaging/v1"

	"github.com/code-payments/flipcash2-server/chat"
	"github.com/code-payments/flipcash2-server/messaging"
	"github.com/code-payments/flipcash2-server/model"
)

// ErrNoTeamAccount is returned by SendMessages when the Sender it is given was
// built with no team account (see messaging.NewSender).
var ErrNoTeamAccount = errors.New("no team account configured")

// clientMessageIDNamespace is the UUID namespace SendMessages derives its client
// message IDs under, so they cannot collide with an ID a client or another
// server-authored send picks.
var clientMessageIDNamespace = uuid.MustParse("0fea31a1-c7d6-4084-b481-fde267cf1177")

// SendMessages sends messages from the team account to userID, in the chat
// between them, creating the chat first if it does not exist yet, and returns
// the chat's ID and the messages as persisted, in order. The chat is their
// canonical tip DM, the type DMs open as, so a tip the user sends the team
// lands in the same conversation. Each element of messages is one message's
// content, and they land as one batch (see messaging.Sender.SendBatch): the
// user sees all of them or none, in order, each counting toward their unread
// and earning its own push. At most messaging.MaxMessagesPerPut messages go in
// one call. The team account is the one sender was built with (see
// messaging.NewSender), so the account these messages are sent as is
// the one the Sender treats as the team, and the one the parent configured
// the chat store to exclude from the feed (see chat.FeedExclusions):
// the parent resolves it once, at startup, with GetUserID. It fails with
// ErrNoTeamAccount when sender has none, as it has when the process started
// before the team account was set up; setting it up takes a restart.
//
// It is idempotent on (the chat, idempotencyKey): the messages' client message
// IDs are derived from the pair and each message's position, so a retry with
// the same key and messages returns what the first call sent without sending
// again. The key names what the messages are for (e.g. "welcome"), so later
// messages into the same chat take a key of their own. Idempotency lasts as
// long as the store's markers do (they carry a TTL), so a caller that must
// send exactly once decides whether to call at all, and uses the retry only
// to recover a call whose outcome it did not learn. A retry is matched by
// position, never by content: under the same key, a retry with more messages
// than the first call fails without sending anything, and one with fewer
// returns the first call's leading messages, as a replay, without sending
// anything either.
func SendMessages(
	ctx context.Context,
	chats chat.Store,
	sender *messaging.Sender,
	userID *commonpb.UserId,
	idempotencyKey string,
	messages []*messagingpb.Content,
) (*commonpb.ChatId, []*messagingpb.Message, error) {
	if idempotencyKey == "" {
		return nil, nil, errors.New("idempotency key is required")
	}
	if len(messages) == 0 {
		return nil, nil, errors.New("expected at least one message")
	}
	if len(userID.GetValue()) != model.UserIDSize {
		return nil, nil, fmt.Errorf("user id must be %d bytes", model.UserIDSize)
	}

	teamUserID := sender.TeamAccount()
	if teamUserID == nil {
		return nil, nil, ErrNoTeamAccount
	}
	if bytes.Equal(teamUserID.Value, userID.GetValue()) {
		return nil, nil, errors.New("the team account cannot message itself")
	}

	chatID := chat.MustDeriveDmChatID(chatpb.ChatType_TIP_DM, teamUserID, userID)

	if err := ensureChat(ctx, chats, chatID, teamUserID, userID); err != nil {
		return nil, nil, err
	}

	outgoing := make([]messaging.OutgoingMessage, len(messages))
	for i, content := range messages {
		outgoing[i] = messaging.OutgoingMessage{
			SenderID:           teamUserID,
			Content:            []*messagingpb.Content{content},
			ClientMessageID:    clientMessageID(chatID, idempotencyKey, i),
			CountsTowardUnread: true,
		}
	}
	sent, err := sender.SendBatch(ctx, chatID, outgoing)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to send messages: %w", err)
	}
	return chatID, sent, nil
}

// ensureChat creates the tip DM chatID between the team account and userID
// unless it already exists. An earlier call, a prior attempt of this one, or a
// tip between the two may have created it, so an existing chat is expected,
// not a failure.
//
// The chat store excludes the team account from the feed of every DM it
// creates with it (see chat.FeedExclusions), this one and one a tip the
// user sent the team first created alike, so nothing here asks for it.
//
// The chat is looked up before it is created, rather than created and
// ErrChatExists ignored, because a create that finds the chat already there is
// still a cancelled transaction that spends write capacity on every item it
// names, the team's own inbox row among them, whose partition key every team
// DM shares. Without the lookup every send after a user's first would pay it.
// The lookup reads the chat's own item instead. It may be stale: a chat
// created a moment ago can read as missing, in which case the create finds it
// and reports ErrChatExists, which is handled like the lookup finding it.
func ensureChat(ctx context.Context, chats chat.Store, chatID *commonpb.ChatId, teamUserID, userID *commonpb.UserId) error {
	_, err := chats.GetChatByID(ctx, chatID)
	if err == nil {
		return nil
	}
	if !errors.Is(err, chat.ErrChatNotFound) {
		return fmt.Errorf("failed to get chat: %w", err)
	}

	err = chats.PutChat(ctx, &chat.Chat{
		ID:           chatID,
		Type:         chatpb.ChatType_TIP_DM,
		Members:      []*commonpb.UserId{teamUserID, userID},
		LastActivity: time.Now().UTC(),
	})
	if err != nil && !errors.Is(err, chat.ErrChatExists) {
		return fmt.Errorf("failed to create chat: %w", err)
	}
	return nil
}

// clientMessageID derives the client message ID of the index-th message
// SendMessages sends into chatID under idempotencyKey. The key is length-prefixed
// so no two (key, index) pairs hash the same input.
func clientMessageID(chatID *commonpb.ChatId, idempotencyKey string, index int) *messagingpb.ClientMessageId {
	var data []byte
	data = append(data, chatID.Value...)
	data = binary.BigEndian.AppendUint32(data, uint32(len(idempotencyKey)))
	data = append(data, idempotencyKey...)
	data = binary.BigEndian.AppendUint32(data, uint32(index))

	id := uuid.NewSHA1(clientMessageIDNamespace, data)
	return &messagingpb.ClientMessageId{Value: id[:]}
}
