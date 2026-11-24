package chat

import (
	"context"
	"errors"
	"fmt"
	"time"

	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	chatpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/chat/v1"
	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"

	"github.com/code-payments/flipcash2-server/model"
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
// Who has archived a chat is read for what an archive keeps quiet for its
// user, every push from the chat (see messaging's push fan-out and the
// lobby's pushes, lobby.go): a page of a group's roster says which of its
// members have archived it (see MembersPage.Archived), a few users' states
// on one chat say it of each of them (see Store.GetViewerStatesForUsers), as
// a user's own state on a chat does (see Store.GetViewerStates), and a
// user's archives across all their chats are one strongly consistent read
// with the version of each (see Store.GetArchivedChats). The store applies
// no quiet itself; it records the archive, files the chat by it and answers
// those reads.

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

// ArchiveChat records the caller's archive of a chat, a DM or a group, which
// files it in their archived folder of the feeds (see GetDmChatFeed and
// GetGroupChatFeed).
//
// Only a member may archive, and the gate is MuteChat's (see
// memberSettingGate): NOT_FOUND for a chat that does not exist, DENIED for a
// non-member, membership alone and never the rules. The store judges
// membership again in the write (see Store.SetArchived), so a departure that
// lands between the gate and the write is refused there with ErrNotMember,
// and answered DENIED, as a non-member's call is. So is the one member whose
// copy of the chat is in no folder, refused with ErrExcludedFromFeed: in
// production the team account, which has no client to call with.
//
// The write is the store's own no-op-aware update: archiving a chat already
// archived is answered with the current state, its original archive time
// kept, and publishes nothing, and a real archive reaches the caller's other
// devices as a MetadataUpdate.ViewerStateChanged on their user topic (see
// publishViewerStateChanged), with the state the response carries. An
// archive changes nothing anyone else sees, so nothing is published on the
// chat topic. Nothing in the chat ends it: only UnarchiveChat does while the
// caller is a member, and a departure ends it with the membership, in the
// store's write of the departure, with nothing announced for it (see
// archive.go).
func (s *Server) ArchiveChat(ctx context.Context, req *chatpb.ArchiveChatRequest) (*chatpb.ArchiveChatResponse, error) {
	userID, err := s.authz.Authorize(ctx, req, &req.Auth)
	if err != nil {
		return nil, err
	}

	log := s.log.With(
		zap.String("user_id", model.UserIDString(userID)),
		zap.String("chat_id", model.ChatIDString(req.ChatId)),
	)

	_, gate, state, permissions, err := s.writeArchive(ctx, log, req.ChatId, userID, true)
	if err != nil {
		return nil, err
	}
	switch gate {
	case memberGateNotFound:
		return &chatpb.ArchiveChatResponse{Result: chatpb.ArchiveChatResponse_NOT_FOUND}, nil
	case memberGateDenied:
		return &chatpb.ArchiveChatResponse{Result: chatpb.ArchiveChatResponse_DENIED}, nil
	}
	return &chatpb.ArchiveChatResponse{
		Result:      chatpb.ArchiveChatResponse_OK,
		ViewerState: state.ToProto(permissions),
	}, nil
}

// UnarchiveChat removes the caller's archive of a chat, filing it back in
// their inbox. It is gated, written and published exactly as ArchiveChat is;
// unarchiving a chat that is not archived is the no-op that returns the
// current state and publishes nothing.
//
// Nothing missed while the chat was archived is replayed. The response
// carries the chat's Metadata instead, on every OK, a no-op included, so the
// client can show the chat again and catch up from it: the chat as GetChat
// returns it to a member under ViewMode FULL (see chatAsSeenBy), hydrated the
// same way, as the member the gate found the caller to be, from the record
// the gate read. Its viewer_state is the state the write returned, the one
// the response's viewer_state carries: hydrate's read of the state is
// strongly consistent, so it would read the same state but for a write of
// another of the caller's devices landing in between, and substituting the
// written one keeps the response to the one version the proto promises. A
// later write of theirs reaches them on the stream as its own
// ViewerStateChanged.
//
// A hydration that fails after the write is an Internal error, since the
// proto has no OK without the chat. The write has landed, and its
// ViewerStateChanged was published before the hydration, so the caller's
// other devices are told of it either way, and the client's retry is the
// no-op, which returns the state and the chat.
func (s *Server) UnarchiveChat(ctx context.Context, req *chatpb.UnarchiveChatRequest) (*chatpb.UnarchiveChatResponse, error) {
	userID, err := s.authz.Authorize(ctx, req, &req.Auth)
	if err != nil {
		return nil, err
	}

	log := s.log.With(
		zap.String("user_id", model.UserIDString(userID)),
		zap.String("chat_id", model.ChatIDString(req.ChatId)),
	)

	c, gate, state, permissions, err := s.writeArchive(ctx, log, req.ChatId, userID, false)
	if err != nil {
		return nil, err
	}
	switch gate {
	case memberGateNotFound:
		return &chatpb.UnarchiveChatResponse{Result: chatpb.UnarchiveChatResponse_NOT_FOUND}, nil
	case memberGateDenied:
		return &chatpb.UnarchiveChatResponse{Result: chatpb.UnarchiveChatResponse_DENIED}, nil
	}

	metadata, err := s.hydrator.hydrate(ctx, userID, memberListenerStanding, ReadingFull, fullDetail, []*Chat{c})
	if err != nil {
		log.With(zap.Error(err)).Warn("Failure hydrating chat metadata")
		return nil, status.Error(codes.Internal, "")
	}
	md := metadata[0]
	md.ViewerState = state.ToProto(permissions)

	return &chatpb.UnarchiveChatResponse{
		Result:      chatpb.UnarchiveChatResponse_OK,
		ViewerState: state.ToProto(permissions),
		Chat:        md,
	}, nil
}

// writeArchive is what ArchiveChat and UnarchiveChat share: the member gate
// (see memberSettingGate), the store's write of archived (see
// Store.SetArchived), and the publish of a real change. It returns the chat
// the gate read, the gate's outcome, and on memberGateOK the caller's state
// after the write with their permissions, a member's, since the gate and the
// write both established membership. A refusal by the write is reported in
// the gate's vocabulary: ErrChatNotFound as memberGateNotFound, and
// ErrNotMember (a departure racing the call) and ErrExcludedFromFeed (a user
// with no folder to move the chat to) as memberGateDenied. A gRPC error is
// returned only for a failed read or write.
func (s *Server) writeArchive(ctx context.Context, log *zap.Logger, chatID *commonpb.ChatId, userID *commonpb.UserId, archived bool) (*Chat, memberGate, ViewerState, Permissions, error) {
	c, gate, err := s.memberSettingGate(ctx, log, chatID, userID)
	if err != nil || gate != memberGateOK {
		return nil, gate, ViewerState{}, Permissions{}, err
	}

	state, changed, err := s.chats.SetArchived(ctx, c, userID, archived, time.Now())
	switch {
	case errors.Is(err, ErrChatNotFound):
		return nil, memberGateNotFound, ViewerState{}, Permissions{}, nil
	case errors.Is(err, ErrNotMember), errors.Is(err, ErrExcludedFromFeed):
		return nil, memberGateDenied, ViewerState{}, Permissions{}, nil
	case err != nil:
		log.With(zap.Error(err), zap.Bool("archived", archived)).Warn("Failure recording archive")
		return nil, 0, ViewerState{}, Permissions{}, status.Error(codes.Internal, "")
	}

	permissions := c.PermissionsFor(userID, true)
	if changed {
		s.publishViewerStateChanged(userID, chatID, state, permissions)
	}
	return c, memberGateOK, state, permissions, nil
}
