package messaging

import (
	"context"
	"encoding/binary"
	"errors"
	"time"

	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"
	messagingpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/messaging/v1"

	"github.com/code-payments/flipcash2-server/database"
)

// ErrMessageNotFound indicates that no message exists for the given chat and
// message ID.
var ErrMessageNotFound = errors.New("message not found")

// Store persists chat messages and message-history pointers.
//
// Each chat has its own gapless message ID sequence. Sends are made idempotent
// by a client-generated message ID, so a retried send returns the originally
// persisted message rather than assigning a new ID.
type Store interface {
	// PutMessage assigns the next gapless message ID for the chat and persists
	// the message, returning the persisted message with its assigned ID and
	// unread sequence.
	//
	// It is idempotent on (chatID, clientMessageID): a retried send with the
	// same client message ID returns the originally persisted message without
	// assigning a new ID.
	//
	// countsTowardUnread controls the unread sequence: when true the message's
	// unread_seq is the previous value + 1; when false it carries the previous
	// value forward (e.g. for the sender's own or system messages).
	//
	// senderID may be nil to denote a system message.
	PutMessage(
		ctx context.Context,
		chatID *commonpb.ChatId,
		senderID *commonpb.UserId,
		content []*messagingpb.Content,
		ts time.Time,
		clientMessageID *messagingpb.ClientMessageId,
		countsTowardUnread bool,
	) (*Message, error)

	// GetMessage returns a single message by ID, or ErrMessageNotFound.
	GetMessage(ctx context.Context, chatID *commonpb.ChatId, messageID *messagingpb.MessageId) (*Message, error)

	// GetMessages returns a page of messages for a chat ordered by message ID
	// (ascending by default), paged via the provided query options. The paging
	// token's value is the message ID of the last message from the previous
	// page (see PageTokenFromID). Returns an empty result (no error) when the
	// chat has no messages.
	GetMessages(ctx context.Context, chatID *commonpb.ChatId, opts ...database.QueryOption) ([]*Message, error)

	// GetMessagesByIDs returns the messages with the given IDs that exist,
	// ordered by message ID ascending. IDs without a message are omitted.
	GetMessagesByIDs(ctx context.Context, chatID *commonpb.ChatId, messageIDs []*messagingpb.MessageId) ([]*Message, error)

	// GetPointers returns all delivered/read pointers for a chat. Returns an
	// empty result (no error) when the chat has no pointers.
	GetPointers(ctx context.Context, chatID *commonpb.ChatId) ([]*messagingpb.Pointer, error)

	// AdvancePointer moves a member's pointer of the given type forward to
	// newValue. Pointers are monotonic: a request to move a pointer to a value
	// at or before its current value is a no-op. It returns ErrMessageNotFound
	// if newValue does not reference an existing message in the chat. The bool
	// return reports whether the pointer advanced.
	AdvancePointer(
		ctx context.Context,
		chatID *commonpb.ChatId,
		userID *commonpb.UserId,
		pointerType messagingpb.Pointer_Type,
		newValue *messagingpb.MessageId,
	) (bool, error)
}

// PageTokenFromID encodes a message ID as a paging token. The token is the
// identifier of the last message in a page; the next request resumes strictly
// after it. Shared by the store implementations and callers.
func PageTokenFromID(messageID *messagingpb.MessageId) *commonpb.PagingToken {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, messageID.Value)
	return &commonpb.PagingToken{Value: buf}
}

// IDFromPageToken decodes the message ID from a token produced by
// PageTokenFromID. The ok return is false if the token is nil or malformed.
func IDFromPageToken(token *commonpb.PagingToken) (messageID uint64, ok bool) {
	if token == nil || len(token.Value) != 8 {
		return 0, false
	}
	return binary.BigEndian.Uint64(token.Value), true
}
