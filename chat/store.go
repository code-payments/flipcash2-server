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

	// ErrLobbyFull indicates that a private group's lobby holds as many users
	// as the limits allow (see LobbyLimits.LobbySize).
	ErrLobbyFull = errors.New("lobby full")

	// ErrTooManyLobbies indicates that a user is waiting in as many lobbies
	// as the limits allow (see LobbyLimits.LobbiesPerUser).
	ErrTooManyLobbies = errors.New("too many lobbies")

	// ErrNotInLobby indicates that a user is not waiting in the lobby they
	// were to be admitted from.
	ErrNotInLobby = errors.New("not in lobby")

	// ErrAlreadyMember indicates that a user is a member of the group whose
	// lobby they were to enter.
	ErrAlreadyMember = errors.New("already a member")

	// ErrKeyEnvelopeNotFound indicates that a user has no key envelope stored
	// for a chat.
	ErrKeyEnvelopeNotFound = errors.New("key envelope not found")
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

// Store persists chats, their membership, and each user's own state on a chat.
//
// DM membership is fixed at creation time (the two participants) and is never
// mutated afterward. Group chat membership is mutable via AddGroupMembers and
// RemoveGroupMember. last_activity is advanced as new activity (typically
// messages) occurs and is the sort key for a user's chat list.
//
// A chat ID's length discriminates its family (see DmChatIDSize and
// GroupChatIDSize): implementations dispatch on it, and each method rejects or
// misses IDs of the wrong family for its semantics — except the viewer-state
// methods, which accept both and do not care which.
//
// Viewer state (see ViewerState) is one record per (user, chat), written only
// by that user's own actions and read back only to them — plus the chat-scoped
// reads GetMutedUsers, GetMutedUsersPage and GetMutedCount, which serve the
// push fan-out. Records are sparse: nothing is written when a chat is created
// or joined, a record appears on the user's first write, and nothing removes
// it — a cleared mute leaves the record and its version behind. The record
// knows nothing of membership: a departure clears a mute only because the
// leave handler calls ClearMute (see Server.LeaveChat), and whether the user
// may act on the chat is likewise the caller's gate. Every write is
// conditional — a single update, or one transaction where a store keeps an
// aggregate alongside the record — and moves Version by exactly one on a real
// change and not at all on a no-op, so a retried or duplicated request is
// harmless.
//
// A key envelope (see KeyEnvelope) is likewise one record per (user, chat),
// for a private group: the group's chat key as that user can open it. It is
// written against the IDs alone, like viewer state, and knows nothing of the
// group or its roster: that the chat is a private group, and that the user is
// a member, are the caller's gates. The one tie to the roster is a departure,
// which removes the envelope in the same write when the caller asks it to
// (see RemoveGroupMember). A store of one is one conditional write of the one
// record, so a retried or duplicated request is harmless.
//
// A lobby entry (see LobbyEntry) is one record per (user, private group) too,
// kept with two counts, the lobby's size and the user's lobbies, that move in
// the same write as the entry (see EnterLobby). Like the rest, it is written
// against the IDs alone; that the chat is a private group with its key, that
// the user is not a member, and who may admit them are the caller's gates. An
// admission (AdmitFromLobby) is where the lobby meets the roster: the entry
// leaves, the key envelope lands and the membership transitions in one
// write, so a user is never admitted without a key or left waiting after.
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
	//
	// A store built with users excluded from the feed (see FeedExclusions)
	// creates every DM with one of them excluding them from it: their copy of the chat records their membership
	// (so IsMember answers for them as for anyone) and nothing else, with no
	// activity to order a feed by, which AdvanceLastMessage never moves and
	// GetDmFeedPage never returns. The exclusion is decided at creation and
	// kept with the chat, internal to the store and not on the Chat it
	// returns: a DM keeps it however the store is later configured, which is
	// what makes an advance of it safe from any process.
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
	//
	// With discardKeyEnvelope, a departure that actually happens also removes
	// the user's key envelope for the chat (see KeyEnvelope), atomically with
	// it: the user stops being a member and stops holding an envelope
	// together, or neither. A no-op removes nothing, the envelope included.
	// Whether a departure takes the envelope is the caller's to say, since
	// the store knows neither that a group is private nor who created it (see
	// Server.LeaveChat).
	RemoveGroupMember(ctx context.Context, chatID *commonpb.ChatId, userID *commonpb.UserId, discardKeyEnvelope bool) (changed bool, roster RosterSummary, err error)

	// SetGroupPicture sets a group chat's profile picture to the blob holding its
	// ORIGINAL rendition, replacing any picture already set; a nil blobID clears
	// it. It touches only the canonical record and performs no validation of the
	// blob or granting of read access — that is the blob domain's job, done
	// before this is called (see blob.Integration.SetAsChatMedia). It returns
	// ErrChatNotFound if the chat does not exist, and an error if chatID is not
	// a group chat ID.
	SetGroupPicture(ctx context.Context, chatID *commonpb.ChatId, blobID *blobpb.BlobId) error

	// EditGroup applies edit to a group chat's canonical record (see
	// GroupEdit): each field the edit names is set (an empty description
	// clears it), and every other field is left as it is, in one write. Like
	// SetGroupPicture it touches only the canonical record and validates
	// nothing — the title and description are validated and moderated, and the
	// pictures attached, before this is called (see Server.EditChat). It returns
	// ErrChatNotFound if the chat does not exist, and an error if chatID is
	// not a group chat ID or the edit names nothing.
	EditGroup(ctx context.Context, chatID *commonpb.ChatId, edit GroupEdit) error

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
	// pinned to a snapshot: the DMs of chatType userID is a member of (never
	// one that excludes them from the feed, see PutChat) whose last_activity
	// is at or before snapshot, ordered by (last_activity, chat_id) descending
	// (most recent first), at most limit chats (limit <= 0 means unbounded).
	// When cursor is nil the page starts at the most recent chat in the
	// snapshot; otherwise it resumes strictly after cursor. An empty result
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

	// GetGroupMembersPage is GetMembers as a walk over a group's roster: the
	// currently joined members in ascending user-ID (byte) order, strictly
	// after the after cursor (nil starts at the beginning), at most limit of
	// them (limit <= 0 means unbounded). The order is the one
	// GetMutedUsersPage returns a chat's mutes in, so a fan-out can read the
	// mutes between a roster page's first and last user and never hold either
	// whole; that is what bounds a push to a large group by its page size
	// rather than its membership. A page's cursor is the last user it
	// carries whenever the limit was reached, whether or not any member
	// follows, so the walk may end on an empty, final page.
	//
	// It reads the membership records alone, never the canonical record: a
	// group that does not exist is an empty page, not ErrChatNotFound, and a
	// caller that needs the distinction reads GetChatByID first — which the
	// walkers do anyway, for the metadata a push carries. Like GetMembers, it
	// may lag a transition by a moment. It returns an error if chatID is not a
	// group chat ID.
	GetGroupMembersPage(ctx context.Context, chatID *commonpb.ChatId, after *commonpb.UserId, limit int) (MembersPage, error)

	// GetGroupRoster returns a group's RosterSummary and every joined member
	// (see GroupMember), in no particular order, from one strongly consistent
	// read: the members are exactly the roster at the summary's version, and
	// the summary's count is their number — an implementation whose read is
	// not a snapshot verifies that against the rows and re-reads, a bounded
	// number of times, when a transition landed mid-read, so the promise holds
	// short of a roster churning faster than it can be read. It is the read
	// behind a small group's roster page, which promises a client exactly
	// that (see Server.GetRoster); it costs the whole roster, so a caller
	// sizes the group first (GetGroupRosterSummary) and pages a large one with
	// GetGroupRosterPage instead. It returns ErrChatNotFound if the chat does
	// not exist, and an error if chatID is not a group chat ID.
	GetGroupRoster(ctx context.Context, chatID *commonpb.ChatId) (RosterSummary, []GroupMember, error)

	// GetGroupRosterPage is one page of a group's joined members in roster
	// order (see RosterPosition): most recently joined first, strictly after
	// the after position (nil starts at the newest member), at most limit of
	// them (limit <= 0 means unbounded). A position need not name a current
	// member: a cursor whose member has since left resumes just as well. The
	// read may lag a transition by a moment — it is served from an index of
	// the joined members in join order rather than the records themselves, so
	// a member who just joined may be missing and one who just left may still
	// appear — which is what a large group's page tolerates (see
	// Server.GetRoster). It reads the membership records alone, never the
	// canonical record, so a group that does not exist is an empty page. It
	// returns an error if chatID is not a group chat ID.
	GetGroupRosterPage(ctx context.Context, chatID *commonpb.ChatId, after *RosterPosition, limit int) ([]GroupMember, error)

	// GetGroupMemberRecords returns userID's joined membership record (see
	// GroupMember) on each of chatIDs they are currently a member of, keyed by
	// string(chatID.Value); a chat they are not joined to — never, or no
	// longer — or that does not exist is absent rather than reported, and
	// duplicate IDs collapse. The read is strongly consistent, as IsMember is
	// and for the same reason: it is asked about a chat the user just joined
	// or created, to announce the record the join wrote (see
	// RosterUpdate.MemberJoined). It returns an error if any ID is not a group
	// chat ID, and an empty map (no error) when chatIDs is empty.
	GetGroupMemberRecords(ctx context.Context, userID *commonpb.UserId, chatIDs []*commonpb.ChatId) (map[string]GroupMember, error)

	// GetGroupMembersByID returns the joined membership record of each of
	// userIDs in the group chatID, keyed by string(userID.Value): the
	// transpose of GetGroupMemberRecords, one group and many users. A user
	// who is not joined — never, or no longer — is absent rather than
	// reported, a group that does not exist reads as no members, and
	// duplicate IDs collapse. The read is eventually consistent, at half the
	// cost of a strong one: it may trail a join or departure by a moment, so
	// a user who has just left can still be returned, which its one reader
	// tolerates (see Server.SampleChatters). It is not for a gate. It returns
	// an error if chatID is not a group chat ID, and an empty map (no error)
	// when userIDs is empty.
	//
	// TODO: revisit the consistency if a reader needs a strongly consistent
	// answer, e.g. by having the caller choose it.
	GetGroupMembersByID(ctx context.Context, chatID *commonpb.ChatId, userIDs []*commonpb.UserId) (map[string]GroupMember, error)

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

	// GetGroupRules returns a group chat's participation rules, its recorded
	// creator and whether it is private (see GroupRules), with a nil Rules
	// when it has none. It reads only what they are projected from, never the
	// full canonical record — and all are fixed at creation, so an
	// implementation is free to cache them indefinitely. It returns ErrChatNotFound if the chat does not exist,
	// and an error if chatID is not a group chat ID.
	GetGroupRules(ctx context.Context, chatID *commonpb.ChatId) (GroupRules, error)

	// IsMember reports whether userID is a member of chatID. It returns false
	// (no error) when the chat does not exist, or when a group member has been
	// removed. The read is strongly consistent: it reflects every join, leave
	// and creation that completed before it. It is the authority behind every
	// access gate (see Access), and the first thing a user does after joining
	// or creating a chat is act in it, so a read that could lag the write
	// would deny exactly that action.
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

	// GetGroupChatsByID returns the canonical record of each given group chat
	// that exists, keyed by string(chatID.Value), whoever is asking: it checks
	// no membership, so a caller passes only IDs whose records it may show.
	// IDs of chats that do not exist are absent; duplicate IDs collapse. The
	// read is eventually consistent, at half the cost of a strong one: a group
	// created moments ago may be absent, and a record may trail an edit by a
	// moment, which its readers tolerate (see Server.SetFeaturedGroups and
	// Server.GetFeaturedGroups). Like GetChatByID it reads only the records: a
	// group's Members and RosterSummary are empty. It returns an error if any
	// ID is not a group chat ID, and an empty result (no error) when chatIDs is
	// empty.
	//
	// TODO: revisit the consistency if another reader comes to use it,
	// especially one that needs a strongly consistent answer, e.g. by having
	// the caller choose it.
	GetGroupChatsByID(ctx context.Context, chatIDs []*commonpb.ChatId) (map[string]*Chat, error)

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
	//
	// A DM keeps a copy of its activity per member, which is what orders that
	// member's DM feed (see GetDmFeedPage); an advance moves every such copy
	// with the canonical record. A member the DM excludes from the feed (see
	// PutChat) has no copy to move and is left out.
	AdvanceLastMessage(ctx context.Context, chatID *commonpb.ChatId, messageID *messagingpb.MessageId, ts time.Time) (advanced bool, members []*commonpb.UserId, err error)

	// SetMute records mute as userID's mute on chatID, replacing any mute
	// already set, and returns the state after the write. changed reports
	// whether anything moved: recording the mute already recorded (after
	// Mute.Normalize) is a no-op that returns the current state at its
	// current version. It returns ErrMuteUntilOutOfRange for a timed mute
	// outside the recordable range (see Mute).
	SetMute(ctx context.Context, chatID *commonpb.ChatId, userID *commonpb.UserId, mute Mute) (state ViewerState, changed bool, err error)

	// ClearMute removes userID's mute on chatID, lapsed or not, and returns
	// the state after the write. Clearing when no mute is recorded is a no-op
	// that returns the current state — the zero state when the user has no
	// record, which the no-op does not create.
	ClearMute(ctx context.Context, chatID *commonpb.ChatId, userID *commonpb.UserId) (state ViewerState, changed bool, err error)

	// GetViewerStates returns userID's state on each of chatIDs that they have
	// a record on, keyed by string(chatID.Value); a chat with no record is
	// absent rather than reported, and duplicate IDs collapse. The read is
	// strongly consistent: it reflects every write that completed before it,
	// so the user who just muted sees the mute. An empty chatIDs is an empty
	// result, not an error.
	GetViewerStates(ctx context.Context, userID *commonpb.UserId, chatIDs []*commonpb.ChatId) (map[string]ViewerState, error)

	// GetMutedUsers returns the users whose mute on chatID is active at now
	// (see Mute.Active), at most limit of them when limit is positive; which
	// users are omitted when the limit truncates is unspecified, and the
	// order is arbitrary. It reads only the recorded mutes, so it is the
	// cheaper read for a chat few have muted; see GetMutedCount for choosing.
	// The read may lag a write by a moment — it is the fan-out's read, where
	// a mute that lands during a send is tolerated, and the flag it feeds is
	// best-effort by contract — and a caller that needs a user's own current
	// state reads GetViewerStates instead.
	GetMutedUsers(ctx context.Context, chatID *commonpb.ChatId, now time.Time, limit int) ([]*commonpb.UserId, error)

	// GetMutedUsersPage is GetMutedUsers bounded to a key range: the users
	// whose mute on chatID is active at now and whose ID lies in [lo, hi]
	// (byte order, inclusive; a nil bound is open on that side), in ascending
	// user-ID order. The order is the one GetGroupMembersPage walks a group's
	// roster in, so a fan-out that holds one page of the roster asks for the
	// mutes between that page's first and last user and never holds either
	// whole; that is the read for a chat most have muted. It reads every
	// record the chat's users hold in the range, muted or not — including
	// records of users who have since left, which a caller intersects with
	// the roster — and consistency is as for GetMutedUsers.
	GetMutedUsersPage(ctx context.Context, chatID *commonpb.ChatId, now time.Time, lo, hi *commonpb.UserId) ([]*commonpb.UserId, error)

	// GetMutedCount returns how many users have a mute recorded on chatID:
	// moved with every mute first recorded or cleared, never by a replaced
	// mute or a timed one lapsing, so it bounds the active mutes from above.
	// It is what a fan-out compares against the roster size to choose
	// between GetMutedUsers and GetMutedUsersPage, and is read for that
	// alone: like them, it may lag a write by a moment, which a choice of
	// shape tolerates. A chat nobody has muted reads as zero.
	GetMutedCount(ctx context.Context, chatID *commonpb.ChatId) (uint64, error)

	// RecordSend records that userID sent in the group chatID at sentAt (see
	// the activity record, RecentSender), moving its activity score with it
	// (see NextActivityScore), and reports whether it did. Each recorded send
	// moves the score exactly once, however many writers record sends for the
	// same user at once. It does not record when the record already holds a
	// send later than sentAt − ActivityRecordInterval, which is what
	// throttles a burst and keeps a delayed or retried send from moving the
	// record backwards, so any number of retries is safe. It records against
	// the chat ID alone, reading neither the canonical record nor the roster:
	// the caller is the send path, which has already gated both. A caching
	// decorator may answer a throttled send itself, with no store request
	// (see cache.Cache). It returns an error if chatID is not a group chat
	// ID, or if sentAt is not after the Unix epoch.
	RecordSend(ctx context.Context, chatID *commonpb.ChatId, userID *commonpb.UserId, sentAt time.Time) (recorded bool, err error)

	// GetRecentSenders returns the group's activity records, with their
	// activity scores, most recently sent first, at most limit of them
	// (limit <= 0 means unbounded), from an eventually consistent read, so a
	// send recorded a moment ago may be missing or at its previous time: its
	// readers rank and show people, which a refetch corrects. Ties come back
	// in no particular order. Records of users who have since left are
	// included (see the activity record), and a record past
	// ActivityRetention may be. A group with no records, or that does not
	// exist, is an empty result. It returns an error if chatID is not a group
	// chat ID.
	GetRecentSenders(ctx context.Context, chatID *commonpb.ChatId, limit int) ([]RecentSender, error)

	// GetActiveSenders returns the group's activity records most active
	// first, by activity score (see NextActivityScore), at most limit of them
	// (limit <= 0 means unbounded), from an eventually consistent read, as
	// GetRecentSenders. Ties come back in no particular order. A record
	// written before scores existed and never since backfilled is not
	// returned. Records of users who have since left are included, and a
	// record past ActivityRetention may be. A group with no records, or that
	// does not exist, is an empty result. It returns an error if chatID is
	// not a group chat ID.
	GetActiveSenders(ctx context.Context, chatID *commonpb.ChatId, limit int) ([]RecentSender, error)

	// GetLastSentAt returns userID's activity record in the group chatID:
	// when their latest recorded send was, and whether they have a record at
	// all. It is the point read of what GetRecentSenders ranges over, for a
	// user it may not reach, and eventually consistent like it. A record past
	// ActivityRetention may be returned. A user with no record, or a group
	// that does not exist, is ok == false. It returns an error if chatID is
	// not a group chat ID.
	GetLastSentAt(ctx context.Context, chatID *commonpb.ChatId, userID *commonpb.UserId) (lastSentAt time.Time, ok bool, err error)

	// SetKeyEnvelope stores envelope as userID's key envelope for the group
	// chatID, and returns the envelope that stands after the call. An
	// envelope the user wrapped themself (KeyEnvelope.IsWrappedBy userID) is
	// never replaced: when one is stored, nothing is written and it is
	// returned, whatever the call carried. Otherwise the call's envelope is
	// stored, over none or over one someone else wrapped for the user, and
	// returned. So the first envelope a user stores for themself stands, a
	// repeat of the stored envelope is the no-op it looks like, and a caller
	// learns whether its envelope is the one stored by comparing it with the
	// one returned (KeyEnvelope.Equal). The check and the write are one
	// conditional write, so two concurrent calls agree on which envelope
	// stands. It returns an error if chatID is not a group chat ID, or if
	// envelope.WrappedBy is nil.
	SetKeyEnvelope(ctx context.Context, chatID *commonpb.ChatId, userID *commonpb.UserId, envelope KeyEnvelope) (KeyEnvelope, error)

	// GetKeyEnvelope returns userID's key envelope for chatID, or
	// ErrKeyEnvelopeNotFound when they have none. The read is strongly
	// consistent: it reflects every write that completed before it, so a
	// user who just stored an envelope, or was just given one, reads it.
	GetKeyEnvelope(ctx context.Context, chatID *commonpb.ChatId, userID *commonpb.UserId) (KeyEnvelope, error)

	// EnterLobby records userID as waiting in the group chatID's lobby, as
	// of now, and returns their entry. A member of the group is refused as
	// ErrAlreadyMember, judged in the write that would record the entry
	// against the membership record as of that write, so an entry can never
	// land for a user admitted meanwhile (see AdmitFromLobby, which removes
	// the entry in the write that joins them): the lobby and the roster
	// never hold the same user. A user already waiting is a no-op that
	// returns the entry as it stands, with changed false; the entry's time
	// is the first entry's. Both limits are enforced in the same write: a
	// lobby at LobbySize is ErrLobbyFull and a user in LobbiesPerUser
	// lobbies ErrTooManyLobbies, judged against the counts as of the write,
	// with nothing recorded. Membership is judged before an existing entry,
	// and the lobby's cap before the user's. It returns an error if chatID
	// is not a group chat ID. It does not check that the chat exists or that
	// it is a private group: those are the caller's.
	EnterLobby(ctx context.Context, chatID *commonpb.ChatId, userID *commonpb.UserId, limits LobbyLimits) (entry LobbyEntry, changed bool, err error)

	// LeaveLobby removes userID from chatID's lobby, reporting whether they
	// were in it. A user not waiting is a no-op. It returns an error if
	// chatID is not a group chat ID.
	LeaveLobby(ctx context.Context, chatID *commonpb.ChatId, userID *commonpb.UserId) (changed bool, err error)

	// GetLobbyEntries returns userID's entry in each of the given lobbies
	// they are waiting in, keyed by string(chatID.Value); lobbies they are
	// not in are absent. The read is strongly consistent, like a key
	// envelope's: it is what a user is told of their own waiting.
	GetLobbyEntries(ctx context.Context, userID *commonpb.UserId, chatIDs []*commonpb.ChatId) (map[string]LobbyEntry, error)

	// GetLobbyPage returns up to limit of chatID's lobby in lobby order (see
	// LobbyPosition), strictly after the given position, or from the start
	// when it is nil. A limit of zero or less means no limit. The read is
	// eventually consistent: an entry just made may be absent and one just
	// removed present, which the proto allows of this read alone. A group
	// with no lobby, or that does not exist, is an empty result. It returns
	// an error if chatID is not a group chat ID.
	GetLobbyPage(ctx context.Context, chatID *commonpb.ChatId, after *LobbyPosition, limit int) ([]LobbyEntry, error)

	// AdmitFromLobby admits userID, waiting in chatID's lobby, to the group:
	// in one write it stores envelope as their key envelope (replacing any
	// they hold, since the one the creator wraps now is the one that opens
	// the chat), joins them as a member (one membership transition, as
	// AddGroupMembers makes it) and removes their lobby entry with its
	// counts. It returns the roster summary as of the write, as
	// AddGroupMembers does, and changed false when the user was a member
	// already (nothing is written, their entry included). It returns
	// ErrNotInLobby when the user is not waiting, with nothing written;
	// ErrChatNotFound if the chat does not exist; and an error if chatID is
	// not a group chat ID or envelope.WrappedBy is nil.
	AdmitFromLobby(ctx context.Context, chatID *commonpb.ChatId, userID *commonpb.UserId, envelope KeyEnvelope) (changed bool, roster RosterSummary, err error)

	// SetFeaturedGroups replaces userID's featured groups with chatIDs, in
	// that order, and returns the list that stands after the call. A list
	// equal to the stored one is a no-op that writes nothing and returns it
	// with changed false; otherwise the whole list is replaced in one write
	// that moves the version by one, so concurrent replaces each land whole,
	// in some order, and a reader never sees a mix. An empty list clears the
	// featured groups. It returns an error unless chatIDs passes
	// ValidateFeaturedGroups. It does not check that the groups exist or are
	// public: those need the groups' records, and are the caller's.
	SetFeaturedGroups(ctx context.Context, userID *commonpb.UserId, chatIDs []*commonpb.ChatId) (featured FeaturedGroups, changed bool, err error)

	// GetFeaturedGroups returns userID's featured groups in their order. The
	// read is eventually consistent, at half the cost of a strong one: it may
	// trail a replace by a moment, returning the list as it was before, but
	// never a mix of two lists. Its readers display the list, which a refetch
	// corrects; SetFeaturedGroups decides against a strongly consistent read
	// of its own. A user who has never set any reads as no groups at version
	// zero.
	GetFeaturedGroups(ctx context.Context, userID *commonpb.UserId) (FeaturedGroups, error)
}
