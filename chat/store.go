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

	// ErrTooManyMembers indicates that a group chat was created with more
	// initial members than MaxGroupChatCreationMembers allows.
	ErrTooManyMembers = errors.New("too many initial members")

	// ErrNoMembers indicates that a chat was created with an empty member set.
	// A chat nobody belongs to is unreachable: no one can read it, send to it,
	// or be added to it, since every such path gates on membership.
	ErrNoMembers = errors.New("chat must have at least one member")
)

// MaxGroupChatCreationMembers is the largest initial member set a group chat
// may be created with; a larger set is rejected with ErrTooManyMembers and the
// caller grows the group with AddGroupMembers instead.
//
// Creation is all-or-nothing, so the bound is what a single atomic write can
// cover: a DynamoDB transaction caps at 100 items, and creation spends one on
// the canonical record. The value is held well under that ceiling so the
// membership records and the canonical record always commit together — there is
// no partial-creation state for a caller to reconcile.
const MaxGroupChatCreationMembers = 50

// DmFeedCursor marks a position within a DM feed snapshot read. The next page
// resumes at the chat immediately after (LastActivity, ChatID) in the feed's
// descending (last_activity, chat_id) order.
type DmFeedCursor struct {
	LastActivity time.Time
	ChatID       *commonpb.ChatId
}

// Store persists chats and their membership.
//
// DM membership is fixed at creation time (the two participants) and is never
// mutated afterward. Group chat membership is mutable via AddGroupMembers and
// RemoveGroupMember. last_activity is advanced as new activity (typically
// messages) occurs and is the sort key for a user's chat list.
//
// A chat ID's length discriminates its family (see DmChatIDSize and
// GroupChatIDSize): implementations dispatch on it, and each method rejects or
// misses IDs of the wrong family for its semantics.
type Store interface {
	// PutChat persists a new chat and its membership. It returns ErrChatExists
	// if a chat with the same ID already exists, ErrNoMembers if the member set
	// is empty, and an error when the chat ID's length does not match the chat's
	// type family.
	//
	// For a group chat (16-byte ID, type GROUP), Members become group membership
	// records, equivalent to AddGroupMembers, and are written atomically with the
	// canonical record: creation either fully succeeds or leaves nothing behind.
	// Duplicate members collapse. A set larger than MaxGroupChatCreationMembers
	// is rejected with ErrTooManyMembers rather than written non-atomically; a
	// caller wanting a larger group creates it at the cap and grows it with
	// AddGroupMembers.
	PutChat(ctx context.Context, chat *Chat) error

	// AddGroupMembers adds users as joined members of a group chat. It is
	// idempotent: adding an already-joined member is a no-op that preserves
	// their original join time, and re-adding a departed member rejoins them
	// fresh. It returns ErrChatNotFound if the chat does not exist, and an
	// error if chatID is not a group chat ID.
	AddGroupMembers(ctx context.Context, chatID *commonpb.ChatId, userIDs []*commonpb.UserId) error

	// RemoveGroupMember ends a user's membership in a group chat. Departure is
	// a tombstone, not a deletion: the user stops being a member (IsMember
	// false, excluded from GetMembers) but the record of their former
	// membership is kept, and they can be re-added later. Removing a non-member
	// or unknown user is a no-op. It returns an error if chatID is not a group
	// chat ID.
	RemoveGroupMember(ctx context.Context, chatID *commonpb.ChatId, userID *commonpb.UserId) error

	// GetChatByID returns the canonical record for the chat with the given ID,
	// or ErrChatNotFound. It reads only that record: Members carries a DM's
	// inline participants and is always empty for a group chat, whose mutable
	// membership lives in its own records. A caller that needs a group's members
	// reads them explicitly via GetMembers, so the cost of enumerating a large
	// group is never paid implicitly by a metadata read.
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

	// GetMembers returns the member user IDs of a chat, or ErrChatNotFound. For
	// a DM this is the canonical inline member list; for a group chat it is the
	// currently joined members.
	GetMembers(ctx context.Context, chatID *commonpb.ChatId) ([]*commonpb.UserId, error)

	// IsMember reports whether userID is a member of chatID. It returns false
	// (no error) when the chat does not exist, or when a group member has been
	// removed.
	IsMember(ctx context.Context, chatID *commonpb.ChatId, userID *commonpb.UserId) (bool, error)

	// AdvanceLastMessage records messageID as the chat's most recent message,
	// moving last_activity forward to ts and last_message_id to messageID, and
	// reports whether it advanced. The two fields are two views of the same event
	// (the newest message) and are updated together. If the stored last_activity
	// is already at or after ts, it is a no-op and reports advanced=false. It
	// returns ErrChatNotFound if the chat does not exist.
	//
	// For a DM it also returns the chat's members — the set the new activity is
	// fanned out to, which rides on the canonical record it must load regardless.
	// A caller that goes on to broadcast the same activity can reuse this set
	// instead of issuing a separate GetMembers. Members are returned on both the
	// advanced and no-op paths; they are nil on error (including
	// ErrChatNotFound). A group chat's membership lives in its own records, so
	// members is empty and the caller reads GetMembers itself.
	AdvanceLastMessage(ctx context.Context, chatID *commonpb.ChatId, messageID *messagingpb.MessageId, ts time.Time) (advanced bool, members []*commonpb.UserId, err error)
}
