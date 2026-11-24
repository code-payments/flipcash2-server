package chat

import (
	"fmt"
	"time"

	chatpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/chat/v1"
	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"
)

// A user's archive of a chat (see chatpb.ArchiveState) is a setting of their
// own on it, recorded on their ViewerState as ArchivedAt, beside their mute
// and the creator's restrictions, and sharing the record's one Version: a
// real archive or unarchive moves it by exactly one, and archiving a chat
// already archived, or unarchiving one that is not, is a no-op that moves
// nothing and writes nothing (see Store.SetArchived). It is independent of
// the mute: an archive's writes never touch a mute or the muted count, and a
// mute's never touch an archive, so either can be set, replaced or cleared
// under the other.
//
// The archive decides the Folder each listing of the user's chats files the
// chat in: every chat a user is a member of is in exactly one of their
// folders, FolderArchived while they hold an archive of it and FolderInbox
// otherwise, and the DM feed and the group listings each read one folder
// (see Store.GetDmFeedPage, Store.GetGroupChatsForUser and
// Store.GetGroupChatsForUserByIDs). A store files the chat in the same write
// that records or removes the archive, so the record and the folder never
// disagree: an archived DM is listed in the archived folder by its
// last_activity, which every advance keeps moving there as in the inbox, and
// an archived group in the user's archived groups. Nothing that happens in
// the chat moves it back; only an unarchive does, while the user is a
// member.
//
// Only a member holds an archive. Archiving is refused with ErrNotMember for
// a user who is not one, judged in the write that would record it, against
// the DM's members or the user's group membership as of that write, so an
// archive never lands for a user who left meanwhile. A user whose copy of
// the chat is in no folder at all — one the store excludes from the feed
// (see FeedExclusions), in production the team account — cannot archive it
// either, and is refused with ErrExcludedFromFeed: there is no folder to
// move it to. A membership's archive lasts only as long as the membership: a
// group departure, a leave or a ban, removes it in the same write that
// records the departure, moving Version by one beside whatever else the
// write moves (see Store.RemoveGroupMember and Store.BanUser), and files the
// departed record in the inbox, so a user who returns to a group starts in
// their inbox. Nothing reports that an archive ended with a departure, and
// nothing is announced for it: a departure always ends the archive, so the
// user's devices learn of it from their own RosterUpdate.MemberLeft, and a
// user who returns is given their state afresh. A DM has no departure, so a
// DM's archive lasts until it is unarchived.
//
// Who has archived a chat is read two ways, for what an archive keeps quiet
// for its user: a page of a group's roster says which of its members have
// archived it (see MembersPage.Archived), and a user's archives across all
// their chats are one strongly consistent read with the version of each
// (see Store.GetArchivedChats). The store applies no quiet itself; it
// records the archive, files the chat by it and answers those reads.

// Folder is which of a user's two lists of their chats a chat is filed in
// (see chatpb.ChatFolder): FolderArchived while they hold an archive of it,
// FolderInbox otherwise. The zero value is the inbox, as the proto's is.
type Folder int

const (
	FolderInbox Folder = iota
	FolderArchived
)

// FolderFromProto converts a chatpb.ChatFolder, or returns an error for a
// value the store has no folder for, which request validation keeps from
// reaching it.
func FolderFromProto(f chatpb.ChatFolder) (Folder, error) {
	switch f {
	case chatpb.ChatFolder_INBOX:
		return FolderInbox, nil
	case chatpb.ChatFolder_ARCHIVED:
		return FolderArchived, nil
	default:
		return FolderInbox, fmt.Errorf("unknown chat folder %d", f)
	}
}

// ToProto converts the folder to its chatpb.ChatFolder.
func (f Folder) ToProto() chatpb.ChatFolder {
	if f == FolderArchived {
		return chatpb.ChatFolder_ARCHIVED
	}
	return chatpb.ChatFolder_INBOX
}

// IsArchived reports whether f is the archived folder.
func (f Folder) IsArchived() bool {
	return f == FolderArchived
}

// String names the folder for logs and test messages.
func (f Folder) String() string {
	if f == FolderArchived {
		return "archived"
	}
	return "inbox"
}

// ArchivedChat is one chat a user has archived, as Store.GetArchivedChats
// lists it: the chat, when they archived it, and the version of their
// ViewerState there as of the read, the version GetViewerStates would have
// returned with it.
type ArchivedChat struct {
	ChatID     *commonpb.ChatId
	ArchivedAt time.Time
	Version    uint64
}

// ValidateArchiveChat returns an error unless c can be archived through a
// store: a chat whose ID's family matches its type (see IsGroupChatID), as
// every stored chat's does. Membership and exclusion are judged by the store
// (see Store.SetArchived).
func ValidateArchiveChat(c *Chat) error {
	if c == nil || c.ID == nil {
		return fmt.Errorf("no chat")
	}
	if IsGroupChatID(c.ID) != (c.Type == chatpb.ChatType_GROUP) {
		return fmt.Errorf("chat id length does not match chat type")
	}
	if !IsGroupChatID(c.ID) && len(c.ID.Value) != DmChatIDSize {
		return fmt.Errorf("not a chat id")
	}
	return nil
}
