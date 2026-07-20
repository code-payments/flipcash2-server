package chat

import (
	"context"
	"errors"
	"time"

	chatpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/chat/v1"
	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"
	messagingpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/messaging/v1"
)

var (
	// ErrChatNotFound indicates that no chat exists for the given chat ID.
	ErrChatNotFound = errors.New("chat not found")

	// ErrChatExists indicates that a chat with the given ID already exists.
	ErrChatExists = errors.New("chat already exists")
)

// DmFeedCursor marks a position within a DM feed snapshot read. The next page
// resumes at the chat immediately after (LastActivity, ChatID) in the feed's
// descending (last_activity, chat_id) order.
type DmFeedCursor struct {
	LastActivity time.Time
	ChatID       *commonpb.ChatId
}

// Store persists chats and their membership.
//
// Membership is fixed at creation time (e.g. the two participants of a DM) and
// is never mutated afterward. The only mutable field is last_activity, which is
// advanced as new activity (typically messages) occurs and is the sort key for
// a user's chat list.
type Store interface {
	// PutChat persists a new chat and its membership. It returns ErrChatExists
	// if a chat with the same ID already exists.
	PutChat(ctx context.Context, chat *Chat) error

	// GetChatByID returns the chat with the given ID, or ErrChatNotFound.
	GetChatByID(ctx context.Context, chatID *commonpb.ChatId) (*Chat, error)

	// GetDmFeedPage returns one page of userID's DM feed for a single chat type,
	// pinned to a snapshot: the DMs of chatType userID is a member of whose
	// last_activity is at or before snapshot, ordered by (last_activity, chat_id)
	// descending (most recent first), at most limit chats (limit <= 0 means
	// unbounded). When cursor is nil the page starts at the most recent chat in
	// the snapshot; otherwise it resumes strictly after cursor. An empty result
	// (no error) is returned when no chats remain.
	//
	// Pinning to a fixed watermark makes a multi-page read internally consistent.
	// last_activity only ever advances to a wall-clock send time, so any chat that
	// becomes active after the snapshot moves strictly above the watermark and
	// leaves the window — it can be neither duplicated onto nor skipped within a
	// later page. Those freshly-active chats are surfaced through the live
	// MetadataUpdate event stream instead (see the Chat service's GetDmChatFeed).
	//
	// It is scoped to a single DM type because each type is its own feed (see
	// GetDmChatFeedRequest.dm_chat_type). Group chats will have a parallel
	// accessor, and the server merges the descending streams into one feed.
	GetDmFeedPage(ctx context.Context, userID *commonpb.UserId, chatType chatpb.ChatType, snapshot time.Time, cursor *DmFeedCursor, limit int) ([]*Chat, error)

	// GetMembers returns the member user IDs of a chat, or ErrChatNotFound.
	GetMembers(ctx context.Context, chatID *commonpb.ChatId) ([]*commonpb.UserId, error)

	// IsMember reports whether userID is a member of chatID. It returns false
	// (no error) when the chat does not exist.
	IsMember(ctx context.Context, chatID *commonpb.ChatId, userID *commonpb.UserId) (bool, error)

	// AdvanceLastMessage records messageID as the chat's most recent message,
	// moving last_activity forward to ts and last_message_id to messageID, and
	// reports whether it advanced. The two fields are two views of the same event
	// (the newest message) and are updated together. If the stored last_activity
	// is already at or after ts, it is a no-op and reports advanced=false. It
	// returns ErrChatNotFound if the chat does not exist.
	//
	// It also returns the chat's members — the set the new activity is fanned out
	// to, which it must load regardless. A caller that goes on to broadcast the
	// same activity can reuse this set instead of issuing a separate GetMembers.
	// Members are returned on both the advanced and no-op paths; they are nil on
	// error (including ErrChatNotFound).
	AdvanceLastMessage(ctx context.Context, chatID *commonpb.ChatId, messageID *messagingpb.MessageId, ts time.Time) (advanced bool, members []*commonpb.UserId, err error)
}
