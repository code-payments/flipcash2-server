package chat

import (
	"context"
	"errors"
	"time"

	blobpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/blob/v1"
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
// cover: a DynamoDB transaction caps at 100 items, and creation spends two on
// the group's own records (the canonical record and its member count). The value
// is held well under that ceiling so the membership records and the group's
// records always commit together — there is no partial-creation state for a
// caller to reconcile.
const MaxGroupChatCreationMembers = 50

// GroupMembership is one of a user's group membership records: the chat,
// whether the record currently has them joined or departed, and the roster
// version the record was last moved at — the version of the user's own last
// transition on that group (see RosterSummary), or zero for a membership
// written at the group's creation, which no transition has touched. Only the
// user's own transitions move their record, so the version is a per-user
// watermark on the group: a transition of theirs at or below it is one the
// record already reflects, whichever way it went. A departed record carries
// the version of the departure for exactly that reason — without it, a stale
// copy of the join it superseded would be indistinguishable from news.
type GroupMembership struct {
	ChatID  *commonpb.ChatId
	Joined  bool
	Version uint64
}

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
	// AddGroupMembers. RosterSummary is derived from Members and ignored.
	PutChat(ctx context.Context, chat *Chat) error

	// AddGroupMembers adds users as joined members of a group chat. It is
	// idempotent: adding an already-joined member is a no-op that preserves
	// their original join time, and re-adding a departed member rejoins them
	// fresh. Each member that actually joins is one membership transition,
	// moving the group's RosterSummary atomically with their record. It
	// reports whether any member actually joined, and the summary as of the
	// last write — which may already reflect a concurrent transition by
	// another writer, and so is what a caller should publish as current. It
	// returns ErrChatNotFound if the chat does not exist, and an error if chatID
	// is not a group chat ID.
	AddGroupMembers(ctx context.Context, chatID *commonpb.ChatId, userIDs []*commonpb.UserId) (changed bool, roster RosterSummary, err error)

	// RemoveGroupMember ends a user's membership in a group chat. Departure is
	// a tombstone, not a deletion: the user stops being a member (IsMember
	// false, excluded from GetMembers) but the record of their former
	// membership is kept, and they can be re-added later. Removing a non-member
	// or unknown user is a no-op. A departure that actually happens is one
	// membership transition, moving the group's RosterSummary atomically with
	// the record; changed and roster are as for AddGroupMembers. It returns
	// ErrChatNotFound if the chat does not exist, and an error if chatID is not
	// a group chat ID.
	RemoveGroupMember(ctx context.Context, chatID *commonpb.ChatId, userID *commonpb.UserId) (changed bool, roster RosterSummary, err error)

	// SetGroupPicture sets a group chat's picture to the blob holding its
	// ORIGINAL rendition, replacing any picture already set; a nil blobID clears
	// it. It touches only the canonical record and performs no validation of the
	// blob or granting of read access — that is the blob domain's job, done
	// before this is called (see blob.Integration.SetAsChatPicture). It returns
	// ErrChatNotFound if the chat does not exist, and an error if chatID is not
	// a group chat ID.
	SetGroupPicture(ctx context.Context, chatID *commonpb.ChatId, blobID *blobpb.BlobId) error

	// GetChatByID returns the canonical record for the chat with the given ID,
	// or ErrChatNotFound. It reads only that record: Members carries a DM's
	// inline participants and is always empty for a group chat, whose mutable
	// membership lives in its own records. A caller that needs a group's members
	// reads them explicitly via GetMembers, so the cost of enumerating a large
	// group is never paid implicitly by a metadata read. Likewise RosterSummary
	// is a DM's inline summary and zero for a group, whose summary is its own
	// read (GetGroupRosterSummary).
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

	// GetGroupRosterSummary returns a group chat's RosterSummary, maintained
	// alongside its membership records rather than computed by enumerating
	// them — so a group's size and version are known without paying for its
	// member list, and stay known once GetMembers returns only a subset.
	//
	// A caller that hands both the summary and a member list to a client reads
	// the summary first: any transition that lands during the enumeration is
	// then above the version handed out, so the client sees it as stale and
	// refetches. Read the other way round, a client could hold a version newer
	// than its list. It returns ErrChatNotFound if the chat does not exist, and
	// an error if chatID is not a group chat ID.
	GetGroupRosterSummary(ctx context.Context, chatID *commonpb.ChatId) (RosterSummary, error)

	// GetGroupRosterSummaries is the cross-chat batch counterpart to
	// GetGroupRosterSummary: the summary of each given group chat, keyed by
	// string(chatID.Value), read as a batch rather than one read per chat. A
	// chat that does not exist is absent from the map rather than reported —
	// which callers passing chats they hold never hit. Duplicate IDs collapse.
	// It returns an error if any ID is not a group chat ID, and an empty map
	// (no error) when chatIDs is empty.
	GetGroupRosterSummaries(ctx context.Context, chatIDs []*commonpb.ChatId) (map[string]RosterSummary, error)

	// GetGroupRules returns a group chat's participation rules (see
	// Chat.Rules), or nil when it has none. It reads only what the rules are
	// projected from, never the full canonical record — and rules are fixed at
	// creation, so an implementation is free to cache them indefinitely. It
	// returns ErrChatNotFound if the chat does not exist, and an error if chatID
	// is not a group chat ID.
	GetGroupRules(ctx context.Context, chatID *commonpb.ChatId) (*chatpb.Rules, error)

	// IsMember reports whether userID is a member of chatID. It returns false
	// (no error) when the chat does not exist, or when a group member has been
	// removed.
	IsMember(ctx context.Context, chatID *commonpb.ChatId, userID *commonpb.UserId) (bool, error)

	// GetGroupMembershipsForUser returns every group chat userID has a
	// membership record on — joined or departed — in no particular order, each
	// with its state and the version of the user's last transition there (see
	// GroupMembership). A user with no records gets an empty result, not an
	// error. It is the inverse of GetMembers over the membership records alone
	// — no canonical chat metadata is read or returned; a caller that needs it
	// follows up with GetChatByID, and one that wants only current memberships
	// filters on Joined (see GetGroupChatsForUser).
	GetGroupMembershipsForUser(ctx context.Context, userID *commonpb.UserId) ([]GroupMembership, error)

	// GetGroupChatsForUser returns the canonical record of every group chat
	// userID is currently a joined member of, in no particular order. It is
	// GetGroupMembershipsForUser, less the departed records, followed by the
	// canonical read of each ID, and returns records exactly as GetChatByID
	// does: Members empty and RosterSummary zero. A user with no group
	// memberships gets an empty result, not an error.
	//
	// It is the group feed's source: with no per-member activity index, a user's
	// groups are ordered by reading every one of them and sorting — so this is
	// paid once per feed snapshot, and the order is carried forward from there
	// (see Server.GetGroupChatFeed).
	GetGroupChatsForUser(ctx context.Context, userID *commonpb.UserId) ([]*Chat, error)

	// GetGroupChatsForUserByIDs is GetGroupChatsForUser restricted to chatIDs:
	// the canonical record of each given group chat that exists and that userID
	// is currently a joined member of, in no particular order. IDs the user is
	// not a member of (never, or no longer) and IDs of chats that do not exist
	// are omitted rather than reported; duplicate IDs collapse. It returns an
	// error if any ID is not a group chat ID, and an empty result (no error) when
	// chatIDs is empty.
	//
	// Membership is checked here, per ID, against the membership records — the
	// IDs are a caller's hint of what to read, never its authority to read it.
	// The group feed resumes from IDs a client echoed back in a paging token,
	// and this is what keeps a tampered token, or one that outlived the caller's
	// membership, from surfacing a chat they cannot see.
	GetGroupChatsForUserByIDs(ctx context.Context, userID *commonpb.UserId, chatIDs []*commonpb.ChatId) ([]*Chat, error)

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
