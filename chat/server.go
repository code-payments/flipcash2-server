package chat

import (
	"bytes"
	"context"
	"encoding/base64"
	"errors"
	"fmt"

	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	blobpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/blob/v1"
	chatpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/chat/v1"
	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"
	eventpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/event/v1"
	messagingpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/messaging/v1"
	profilepb "github.com/code-payments/flipcash2-protobuf-api/generated/go/profile/v1"

	"github.com/code-payments/flipcash2-server/account"
	"github.com/code-payments/flipcash2-server/auth"
	"github.com/code-payments/flipcash2-server/model"
	"github.com/code-payments/flipcash2-server/moderation"
	"github.com/code-payments/flipcash2-server/redact"
)

// MessageRef identifies a chat's message to hydrate. The feed builds one ref per
// chat (its last message) to batch the lookup across the page.
type MessageRef struct {
	ChatID    *commonpb.ChatId
	MessageID *messagingpb.MessageId
}

// PointerRef names a chat and the members whose pointers to hydrate. The feed
// builds one ref per chat — a DM's members, or the viewer alone in a group
// (see hydrate) — to batch the pointer lookup across the page.
type PointerRef struct {
	ChatID  *commonpb.ChatId
	Members []*commonpb.UserId
}

// MessagingReader is the read slice of the messaging domain the Chat service
// needs to hydrate feed metadata. It is declared here (consumer side) so the
// chat package need not import messaging, keeping the messaging→chat dependency
// one-way; the messaging package supplies the concrete adapter.
type MessagingReader interface {
	// LastMessages returns the message for each ref that exists, keyed by
	// string(chatID.Value). Refs without a message are absent from the map.
	LastMessages(ctx context.Context, refs []MessageRef) (map[string]*messagingpb.Message, error)

	// Pointers returns the delivered/read pointers for the members named in each
	// ref, keyed by string(chatID.Value). Chats with no matching pointers are
	// absent from the map.
	Pointers(ctx context.Context, refs []PointerRef) (map[string][]*messagingpb.Pointer, error)

	// LatestEventSequences returns the head event sequence of each given chat,
	// keyed by string(chatID.Value). Chats at head 0 (no messages) are absent from
	// the map, so a missing key means 0.
	LatestEventSequences(ctx context.Context, chatIDs []*commonpb.ChatId) (map[string]uint64, error)
}

// ProfileReader is the read slice of the profile domain the Chat service needs
// to hydrate member profiles and to find a user by handle. Like
// MessagingReader it is declared here (consumer side) so the chat package need
// not import profile; the profile package supplies the concrete adapter.
type ProfileReader interface {
	// GetPhoneNumbers returns the linked phone number for each of the given
	// users that has one, keyed by string(userID.Value). Users without a linked
	// phone number are absent from the map.
	GetPhoneNumbers(ctx context.Context, userIDs []*commonpb.UserId) (map[string]*commonpb.PhoneNumber, error)

	// GetLimitedPublicProfilesForRow returns the row profile of each of the given
	// users the profile domain knows, keyed by string(userID.Value). Unknown
	// users are absent from the map. Every field a member row shows — display
	// name, username, profile picture, join timestamp, Flipcard customization —
	// comes back in this one call, and nothing a row does not show.
	//
	// A member who has set neither a name nor a picture still gets an entry,
	// carrying just the join timestamp. Every chat member is a user the profile
	// domain knows, so hydration treats a missing entry as an error rather than
	// standing in a profile of its own.
	//
	// Profile pictures come back with their blob metadata already resolved —
	// including a short-lived download URL — so a client can render member avatars
	// without a follow-up call. The URL expires; to re-mint one the client calls
	// GetBlobs with an AccessContext naming that user's profile, which authorizes
	// it because a profile picture is public.
	//
	// There is one proto per user, so a caller that fills in per-member fields
	// must copy before mutating: the same user can be a member of several chats.
	GetLimitedPublicProfilesForRow(ctx context.Context, userIDs []*commonpb.UserId) (map[string]*profilepb.UserProfile, error)

	// GetUserIDByUsername returns the user currently holding the given
	// handle, and ok false when nobody holds it.
	GetUserIDByUsername(ctx context.Context, username string) (userID *commonpb.UserId, ok bool, err error)
}

// BlocklistReader is the read slice of the blocklist domain the Chat service
// needs to compute per-viewer hidden state and to keep users the viewer
// blocked out of mention suggestions. Like the other readers it is
// declared here (consumer side) so the chat package need not import blocklist;
// the blocklist package supplies the concrete adapter.
type BlocklistReader interface {
	// GetBlocked returns which of candidateIDs the owner has blocked, as a set
	// keyed by string(userID.Value). Candidates the owner has not blocked are
	// absent from the map.
	GetBlocked(ctx context.Context, ownerID *commonpb.UserId, candidateIDs []*commonpb.UserId) (map[string]bool, error)
}

// Media is the slice of the blob domain the Chat service needs: hydrating group
// pictures on read, and attaching a picture to a group on write. Like the
// readers it is declared here (consumer side) so the service can be tested
// against a canned implementation; blob.Integration satisfies it directly.
type Media interface {
	// ResolveRenditions returns each original's full rendition set — the
	// ORIGINAL plus every derived rendition, each with a freshly minted,
	// short-lived download URL — keyed by string(BlobId.Value). Originals that
	// are unknown or not yet servable are absent from the map. It performs no
	// authorization: the caller passes only ids it has already established the
	// reader may see.
	ResolveRenditions(ctx context.Context, ids []*blobpb.BlobId) (map[string][]*blobpb.Rendition, error)

	// SetAsChatMedia attaches the blob holding a picture's ORIGINAL to chatID
	// as its profile picture or cover picture — the two are one surface to the
	// blob domain: it verifies that ownerID owns the blob and that it is a
	// READY image original, then grants read access to it on the surfaces the
	// picture is shown from. It is idempotent. It returns one of
	// blob.ErrBlobNotFound, blob.ErrBlobNotReady, blob.ErrBlobRejected, or
	// blob.ErrBlobInvalid when the blob cannot back a picture, having granted
	// nothing; any other error is a failure to attach.
	//
	// It touches only blob-domain state, so it may be called for a chat that
	// does not exist yet — which is how a group is created with its picture in
	// place, rather than briefly without one.
	SetAsChatMedia(ctx context.Context, ownerID *commonpb.UserId, chatID *commonpb.ChatId, blobID *blobpb.BlobId) error
}

// UserEventPublisher is the write slice of the event domain the Chat service
// needs to notify one user's streams — the user-keyed event bus. Like the
// readers it is declared here (consumer side) because the event package
// imports chat for stream registration, so chat cannot import it back; the
// event package's Bus satisfies it directly.
type UserEventPublisher interface {
	OnEvent(userID *commonpb.UserId, e *eventpb.Event)
}

// ChatEventPublisher is the write slice of the event domain the Chat service
// needs to notify every stream subscribed to a group chat's topic — the
// chat-keyed event bus. See UserEventPublisher for why it is declared here.
type ChatEventPublisher interface {
	OnEvent(chatID *commonpb.ChatId, e *eventpb.ChatEvent)
}

type Server struct {
	log *zap.Logger

	authz auth.Authorizer

	accounts  account.Store
	blocklist BlocklistReader
	chats     Store
	media     Media
	messaging MessagingReader
	moderator moderation.Client
	profiles  ProfileReader

	// teamUserID is the Flipcash team account (see flipcashteam), whose DMs are
	// never end-to-end encrypted (see useE2ee). The Never speaker rule they
	// carry is the RuleEvaluator's, built with the same account (see
	// RuleEvaluator.RulesOf). Nil when there is none.
	teamUserID *commonpb.UserId

	rules  *RuleEvaluator
	access *Access

	userEventBus UserEventPublisher
	chatEventBus ChatEventPublisher

	// disableGetRoster turns the GetRoster RPC off when set: every call is
	// refused with UNAVAILABLE before anything is read (see roster.go). It is
	// an operator's switch on the one read that walks a whole roster, so the
	// read can be withdrawn without a deploy if it proves too costly.
	disableGetRoster bool

	// maxGroupFeedChats is the most group chats a user's feed may hold (see
	// feed.go). It is the package constant of the same name in production;
	// tests lower it to exercise the refusal without thousands of groups.
	maxGroupFeedChats int

	// rosterWholeReadCap is the largest group whose roster GetRoster reads
	// whole (see roster.go). It is the package constant of the same name in
	// production; tests lower it to exercise the paged shape without
	// thousands of members.
	rosterWholeReadCap int

	// lobbyLimits caps a private group's lobby and a user's lobbies (see
	// lobby.go). DefaultLobbyLimits in production; tests lower them with
	// WithLobbyLimits to exercise the refusals without thousands of users.
	lobbyLimits LobbyLimits

	chatpb.UnimplementedChatServer
}

// ServerOption configures a Server at construction beyond its required
// dependencies.
type ServerOption func(*Server)

// WithLobbyLimits overrides DefaultLobbyLimits (see LobbyLimits).
func WithLobbyLimits(limits LobbyLimits) ServerOption {
	return func(s *Server) {
		s.lobbyLimits = limits
	}
}

func NewServer(
	log *zap.Logger,

	authz auth.Authorizer,

	accounts account.Store,
	blocklist BlocklistReader,
	chats Store,
	media Media,
	messaging MessagingReader,
	moderator moderation.Client,
	profiles ProfileReader,

	access *Access,

	userEventBus UserEventPublisher,
	chatEventBus ChatEventPublisher,

	teamUserID *commonpb.UserId,

	disableGetRoster bool,

	opts ...ServerOption,
) *Server {
	s := &Server{
		log: log,

		authz: authz,

		accounts:  accounts,
		blocklist: blocklist,
		chats:     chats,
		media:     media,
		messaging: messaging,
		moderator: moderator,
		profiles:  profiles,

		rules:  access.Rules(),
		access: access,

		userEventBus: userEventBus,
		chatEventBus: chatEventBus,

		teamUserID: teamUserID,

		disableGetRoster: disableGetRoster,

		maxGroupFeedChats:  maxGroupFeedChats,
		rosterWholeReadCap: rosterWholeReadCap,
		lobbyLimits:        DefaultLobbyLimits,
	}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

// GetChat returns one chat's metadata as the caller may see it.
//
// A DM is its two members' alone: anyone else is DENIED. A group's record is
// returned to every registered user, member or not — its title, picture,
// rules and roster summary are what a user weighs before joining, and what a
// client renders for a group it was pointed at (see Access for the rules). What
// the caller's standing decides, combined with the view mode they asked for
// (see ListenerStanding.Reading), is how much of the group comes with it:
//
//   - A member sees everything, as before: the record, themselves as the
//     hydrated member with their pointers, and the group's messaging state —
//     its last message and head event sequence.
//   - A non-member who satisfies the group's listener rules sees the record
//     and its messaging state, so a group they may read previews like one they
//     are in. They are not on the roster, so no member is hydrated: an empty
//     Members is how the metadata says the viewer is not a member.
//   - A non-member who does not satisfy the rules sees the record alone under
//     FULL, the mode a client that does not know redaction asks for. Under
//     FULL_OR_REDACTED they see the messaging state redacted: the last message
//     as a placeholder (see redact.Message) and the head event sequence, so a
//     client can show that the group is alive and how much, without what was
//     said. The rules are carried in every case so the client can show what
//     would admit them.
//   - Under REDACTED anyone who may read the group at all — a member too —
//     sees its last message redacted, and the rules are not evaluated for a
//     non-member (see Access.ListenerStanding).
//
// A private group (see Chat.IsPrivate) falls out of the same rules. Its record
// is returned to any registered user, with is_private set, so that one who is
// not a member can see what they would ask to join. It carries no rules, so a
// non-member's standing is none under every mode and they see the record
// alone: its messaging state is its members'. What a non-member does get is
// in_lobby, whether they are waiting in its lobby (see lobby.go). A private
// group whose key has not been stored is returned like any other.
//
// The group's pictures and description are returned in every case: they are
// part of the record, as the title is, and are what identify a group — a
// group's pictures are readable by anyone. Its download URLs are resolved here without a blob ACL
// check on that basis. The blob domain itself still resolves a chat-scoped
// grant against membership, so a non-member's GetBlobs on the same picture, or
// on media in a message they previewed, is denied; that is accepted, since a
// non-member's read access is short-lived by nature and the URLs hydrated here
// and on the messages are what a previewing client renders from.
//
// Auth is optional. A request without it asks for the chat's public view (see
// getPublicChat).
func (s *Server) GetChat(ctx context.Context, req *chatpb.GetChatRequest) (*chatpb.GetChatResponse, error) {
	if req.Auth == nil {
		return s.getPublicChat(ctx, req)
	}

	userID, err := s.authz.Authorize(ctx, req, &req.Auth)
	if err != nil {
		return nil, err
	}

	log := s.log.With(zap.String("user_id", model.UserIDString(userID)))

	// The canonical record first: its absence is the only thing that
	// distinguishes NOT_FOUND from DENIED, since a membership check reports a
	// missing chat as a plain non-membership.
	c, err := s.chats.GetChatByID(ctx, req.ChatId)
	switch {
	case errors.Is(err, ErrChatNotFound):
		return &chatpb.GetChatResponse{Result: chatpb.GetChatResponse_NOT_FOUND}, nil
	case err != nil:
		log.With(zap.Error(err)).Warn("Failure getting chat")
		return nil, status.Error(codes.Internal, "")
	}

	// The caller's standing is decided off the record where it can be — a DM's
	// members are on it — and otherwise by a keyed membership check rather
	// than a scan of the member list, so a group's membership is never
	// enumerated on behalf of a caller who turns out not to be a member — and,
	// for a non-member of a group, the listener rules.
	standing, err := s.access.ListenerStandingWithChat(ctx, c, userID, req.GetViewMode())
	if err != nil {
		log.With(zap.Error(err)).Warn("Failure determining chat standing")
		return nil, status.Error(codes.Internal, "")
	}
	if !standing.IsMember && !IsGroupChatID(c.ID) {
		return &chatpb.GetChatResponse{Result: chatpb.GetChatResponse_DENIED}, nil
	}

	metadata, err := s.hydrate(ctx, userID, standing, standing.Reading(req.GetViewMode()), fullDetail, []*Chat{c})
	if err != nil {
		log.With(zap.Error(err)).Warn("Failure hydrating chat metadata")
		return nil, status.Error(codes.Internal, "")
	}

	return &chatpb.GetChatResponse{
		Result:   chatpb.GetChatResponse_OK,
		Metadata: metadata[0],
	}, nil
}

// getPublicChat is GetChat for an unauthenticated caller: the chat's public
// view, which is what a registered non-member previewing the group gets under
// REDACTED (see Access.PublicListenerStanding) — the record, and its redacted
// messaging state, for any public group — with none of the
// per-viewer fields, since there is no viewer: no hydrated member, no
// is_hidden, no viewer_state. It is REDACTED or nothing: any other mode is
// DENIED, as is a DM, before anything is read, so an anonymous caller cannot
// learn whether a DM exists. A private group has no public view either and is
// DENIED off its record: its title, description and pictures are for
// registered users.
func (s *Server) getPublicChat(ctx context.Context, req *chatpb.GetChatRequest) (*chatpb.GetChatResponse, error) {
	if req.GetViewMode() != messagingpb.ViewMode_REDACTED || !IsGroupChatID(req.ChatId) {
		return &chatpb.GetChatResponse{Result: chatpb.GetChatResponse_DENIED}, nil
	}

	log := s.log.With(zap.String("chat_id", base64.StdEncoding.EncodeToString(req.ChatId.Value)))

	c, err := s.chats.GetChatByID(ctx, req.ChatId)
	switch {
	case errors.Is(err, ErrChatNotFound):
		return &chatpb.GetChatResponse{Result: chatpb.GetChatResponse_NOT_FOUND}, nil
	case err != nil:
		log.With(zap.Error(err)).Warn("Failure getting chat")
		return nil, status.Error(codes.Internal, "")
	}

	if c.IsPrivate {
		return &chatpb.GetChatResponse{Result: chatpb.GetChatResponse_DENIED}, nil
	}

	standing := s.access.PublicListenerStanding(c)
	metadata, err := s.hydrate(ctx, nil, standing, standing.Reading(req.GetViewMode()), fullDetail, []*Chat{c})
	if err != nil {
		log.With(zap.Error(err)).Warn("Failure hydrating chat metadata")
		return nil, status.Error(codes.Internal, "")
	}

	return &chatpb.GetChatResponse{
		Result:   chatpb.GetChatResponse_OK,
		Metadata: metadata[0],
	}, nil
}

// metadataDetail is how much of a chat's record hydrate returns.
type metadataDetail int

const (
	// fullDetail is the whole record, for a caller showing one chat — GetChat,
	// a creation, an edit, a join, a lobby — or carrying it to a client that
	// may.
	fullDetail metadataDetail = iota

	// listDetail is the record as a list view shows it: everything but the
	// group's cover picture, which only its profile view shows. A feed page
	// is many chats a client renders as rows, and the cover's renditions cost
	// the server a resolve and a download URL per group for a field no row
	// renders. The description stays: a row may show it, and it is short text
	// read with the record, costing nothing to carry. The proto allows a feed
	// to omit the cover (see chat.v1.Metadata.cover_picture), and tells the
	// client to fetch it with GetChat and never to let a feed result clear
	// what it holds.
	listDetail
)

// hydrate builds the proto metadata for a set of chats as a viewer of the given
// standing sees them, batching the reads across the whole set: every chat's
// last message in one call, every group's roster summary in one call, every
// hydrated member's pointers in one call, every chat's head event sequence in
// one call, every member's display name in one call, and every DM member's
// phone number in one call. The calls are independent and run concurrently, so
// a page costs the slowest of them rather than their sum.
//
// The standing is the viewer's towards every chat in the set (see ListenerStanding),
// and the reading is what the viewer's read of every chat in the set is
// answered with (see ListenerStanding.Reading). Together they decide what is hydrated
// at all: what they withhold is never read, not read and dropped. Most
// callers hydrate chats the viewer is a member of — a feed built from their
// memberships, a join or creation that just landed — and pass memberListenerStanding
// and ReadingFull. GetChat hydrates a group for whoever asks, and passes what
// Access found under the mode the client asked for:
//
//   - A non-member has no hydrated member in a group: they are not on the
//     roster, and a pointer stored for them — a former member's — is not
//     theirs to see. A DM's members are its record's, whoever the viewer is.
//   - A viewer whose reading is denied gets no messaging state: no last
//     message, no head event sequence, no pointers. The record's own fields —
//     title, description, pictures, rules, roster summary, last activity —
//     are hydrated for anyone the caller admits to the record at all.
//   - A viewer whose reading is redacted gets the messaging state with the
//     last message redacted (see redact.Message), after its media is
//     resolved, so the placeholder carries the blurhash a client renders. The
//     read fails rather than returning a message that cannot be redacted.
//
// A DM's members are its two participants, carried on the canonical record. A
// group's roster lives in its own records and is not enumerated here: the
// metadata carries the viewer as its only member, and the group's roster
// summary — read as a batch across the set — says how large the roster really
// is. So the cost of a group in the set is fixed, whatever its size, and every
// caller passes chats straight from the store with no membership read of its
// own. The roster itself is a separate read (see GetRoster), and it is where
// a member's join time and version are read: the viewer's own entry here
// carries neither (see Member.joined_at), since nothing a client does with
// its own entry needs them, and reading them would cost a membership read
// per group on the page.
//
// Display names are populated for members of every chat: they are the public
// identifier a member is known by within the chat. Phone numbers are populated
// only for members of DM chats, so each party can resolve the other to a
// contact. Group chats deliberately do not expose member phone numbers.
//
// Pointers are populated for every hydrated member: both parties of a DM, and
// the viewer in a group. The viewer's own READ pointer is what lets a client
// compute its unread count. Other group members' pointers are stored but never
// surfaced: they are not broadcast in real time (see
// messaging.Server.AdvancePointer), and hydrating them would cost two keyed
// reads per member, growing with the roster.
//
// is_hidden is per-viewer: a DM is hidden from viewerID when the DM's peer (the
// member who is not the viewer) is on the viewer's blocklist. Every DM peer
// across the set is resolved against the viewer's blocklist in one batched read.
//
// use_e2ee is a DM's alone and, while E2EE is rolling out, a staff DM's alone:
// it is set exactly when every member of the DM is a staff user (see useE2ee),
// which is decided per read from the account store's staff flag — never
// stored on the chat — so a DM starts carrying it the moment its second
// member becomes staff, and a group never carries it. Every DM member across
// the set is resolved once, concurrently with the other reads.
//
// The viewer is nil for an unauthenticated read (see getPublicChat): its
// standing is never a member's and it is never shown a DM, so no per-viewer
// state is read on its behalf.
//
// in_lobby is per-viewer and a non-member's alone: whether the viewer is
// waiting in a private group's lobby (see lobby.go), read for every private
// group in the set when the viewer is not a member, one strongly consistent
// read across the set, and never for a member, who is past waiting, or for a
// group that is not private, which has no lobby. A nil viewer waits nowhere.
//
// viewer_state is per-viewer too, and a member's alone: it is what the chat
// holds about the viewer (see ViewerState) plus what they may do in it, and
// a non-member may do nothing and has no standing to see what the record
// still holds — a mute a departure failed to clear stays on the record, out
// of sight, until they rejoin. So it is read only for a member, one read
// across the set, and every member gets one: the record exactly, a lapsed
// mute included, or the zero record when they have written none, with their
// permissions computed off the record (see Chat.PermissionsFor and
// ViewerState.ToProto). A non-member gets none.
//
// A group's profile picture and cover picture are each stored as the blob
// holding its ORIGINAL; every picture across the set is expanded to its full
// rendition set, each with a short-lived download URL, in one batched read —
// so a client renders the group's avatar and banner without a follow-up
// GetBlobs. That is done without an ACL check here: a chat's pictures are
// granted to the chat and its public profile when they are set (see
// blob.Integration.SetAsChatMedia), and a non-member is shown them as part of
// the record that identifies the group (see GetChat). A picture whose
// original no longer resolves is left with its stored ORIGINAL for the client
// to treat as unavailable, rather than failing the whole read.
//
// The detail is how much of the record is returned (see metadataDetail): a
// feed asks for listDetail, and its chats carry no cover picture — its
// renditions are never resolved, not resolved and dropped — while every other
// caller asks for fullDetail.
func (s *Server) hydrate(ctx context.Context, viewerID *commonpb.UserId, standing ListenerStanding, reading Reading, detail metadataDetail, chats []*Chat) ([]*chatpb.Metadata, error) {
	var msgRefs []MessageRef
	var seqChatIDs []*commonpb.ChatId
	var pointerRefs []PointerRef
	var groupChatIDs []*commonpb.ChatId
	uniqueUserIDs := make(map[string]*commonpb.UserId)
	uniquePrivateProfileUserIds := make(map[string]*commonpb.UserId)
	dmPeerByChat := make(map[string]*commonpb.UserId)
	uniquePeerIDs := make(map[string]*commonpb.UserId)
	uniquePictureBlobIDs := make(map[string]*blobpb.BlobId)
	uniqueDmMemberIDs := make(map[string]*commonpb.UserId)
	var lobbyChatIDs []*commonpb.ChatId
	hydratedMembers := make([][]*commonpb.UserId, len(chats))
	for i, c := range chats {
		if c.IsPrivate && !standing.IsMember && viewerID != nil {
			lobbyChatIDs = append(lobbyChatIDs, c.ID)
		}
		// The members to hydrate: a DM's participants, or the viewer alone in a
		// group they are a member of (see above).
		members := c.Members
		if IsGroupChatID(c.ID) {
			members = nil
			if standing.IsMember {
				members = []*commonpb.UserId{viewerID}
			}
			groupChatIDs = append(groupChatIDs, c.ID)
		}
		hydratedMembers[i] = members

		// Messaging state is a reader's alone (see above).
		if reading != ReadingDenied {
			if len(members) > 0 {
				pointerRefs = append(pointerRefs, PointerRef{ChatID: c.ID, Members: members})
			}
			if c.LastMessageID != nil {
				msgRefs = append(msgRefs, MessageRef{ChatID: c.ID, MessageID: c.LastMessageID})
				// A chat's head is 0 unless it has at least one message, which is
				// exactly when it has a last message ID. Skip the rest: their head is
				// the proto default 0.
				seqChatIDs = append(seqChatIDs, c.ID)
			}
		}
		if c.ProfilePictureBlobID != nil {
			uniquePictureBlobIDs[string(c.ProfilePictureBlobID.Value)] = c.ProfilePictureBlobID
		}
		if c.CoverPictureBlobID != nil && detail == fullDetail {
			uniquePictureBlobIDs[string(c.CoverPictureBlobID.Value)] = c.CoverPictureBlobID
		}
		for _, m := range members {
			uniqueUserIDs[string(m.Value)] = m
			if c.Type == chatpb.ChatType_CONTACT_DM {
				uniquePrivateProfileUserIds[string(m.Value)] = m
			}
		}
		if IsDmChatType(c.Type) {
			for _, m := range c.Members {
				uniqueDmMemberIDs[string(m.Value)] = m
			}
			for _, m := range c.Members {
				if !bytes.Equal(m.Value, viewerID.GetValue()) {
					dmPeerByChat[string(c.ID.Value)] = m
					uniquePeerIDs[string(m.Value)] = m
					break
				}
			}
		}
	}
	userIDs := make([]*commonpb.UserId, 0, len(uniqueUserIDs))
	for _, u := range uniqueUserIDs {
		userIDs = append(userIDs, u)
	}
	privateProfileUserIDs := make([]*commonpb.UserId, 0, len(uniquePrivateProfileUserIds))
	for _, u := range uniquePrivateProfileUserIds {
		privateProfileUserIDs = append(privateProfileUserIDs, u)
	}
	peerIDs := make([]*commonpb.UserId, 0, len(uniquePeerIDs))
	for _, u := range uniquePeerIDs {
		peerIDs = append(peerIDs, u)
	}
	pictureBlobIDs := make([]*blobpb.BlobId, 0, len(uniquePictureBlobIDs))
	for _, id := range uniquePictureBlobIDs {
		pictureBlobIDs = append(pictureBlobIDs, id)
	}
	dmMemberIDs := make([]*commonpb.UserId, 0, len(uniqueDmMemberIDs))
	for _, u := range uniqueDmMemberIDs {
		dmMemberIDs = append(dmMemberIDs, u)
	}

	// The reads are independent of one another, so they run concurrently: the
	// page waits for the slowest rather than the sum. Each goroutine writes only
	// its own result, and Wait is the barrier before any is read. The first
	// failure cancels the rest through the group's context.
	var (
		lastMessages           map[string]*messagingpb.Message
		rosterSummaries        map[string]RosterSummary
		pointers               map[string][]*messagingpb.Pointer
		latestEventSeqs        map[string]uint64
		phoneNumbersByUserId   map[string]*commonpb.PhoneNumber
		publicProfilesByUserId map[string]*profilepb.UserProfile
		blockedPeers           map[string]bool
		pictureRenditions      map[string][]*blobpb.Rendition
		viewerStates           map[string]ViewerState
		staffByUserId          map[string]bool
		lobbyEntries           map[string]LobbyEntry
	)
	chatIDs := make([]*commonpb.ChatId, len(chats))
	for i, c := range chats {
		chatIDs[i] = c.ID
	}
	// A read with nothing to ask for is skipped, not made: a viewer's standing
	// may have withheld a whole class of state from the set.
	g, gctx := errgroup.WithContext(ctx)
	if len(msgRefs) > 0 {
		g.Go(func() (err error) {
			lastMessages, err = s.messaging.LastMessages(gctx, msgRefs)
			return err
		})
	}
	g.Go(func() (err error) {
		rosterSummaries, err = s.chats.GetGroupRosterSummaries(gctx, groupChatIDs)
		return err
	})
	if len(pointerRefs) > 0 {
		g.Go(func() (err error) {
			pointers, err = s.messaging.Pointers(gctx, pointerRefs)
			return err
		})
	}
	if len(seqChatIDs) > 0 {
		g.Go(func() (err error) {
			latestEventSeqs, err = s.messaging.LatestEventSequences(gctx, seqChatIDs)
			return err
		})
	}
	g.Go(func() (err error) {
		phoneNumbersByUserId, err = s.profiles.GetPhoneNumbers(gctx, privateProfileUserIDs)
		return err
	})
	g.Go(func() (err error) {
		publicProfilesByUserId, err = s.profiles.GetLimitedPublicProfilesForRow(gctx, userIDs)
		return err
	})
	if len(peerIDs) > 0 {
		g.Go(func() (err error) {
			blockedPeers, err = s.blocklist.GetBlocked(gctx, viewerID, peerIDs)
			return err
		})
	}
	if standing.IsMember {
		g.Go(func() (err error) {
			viewerStates, err = s.chats.GetViewerStates(gctx, viewerID, chatIDs)
			return err
		})
	}
	if len(pictureBlobIDs) > 0 {
		g.Go(func() (err error) {
			pictureRenditions, err = s.media.ResolveRenditions(gctx, pictureBlobIDs)
			return err
		})
	}
	if len(lobbyChatIDs) > 0 {
		g.Go(func() (err error) {
			lobbyEntries, err = s.chats.GetLobbyEntries(gctx, viewerID, lobbyChatIDs)
			return err
		})
	}
	if len(dmMemberIDs) > 0 {
		g.Go(func() (err error) {
			staffByUserId, err = s.staffFlags(gctx, dmMemberIDs)
			return err
		})
	}
	if err := g.Wait(); err != nil {
		return nil, err
	}

	metadata := make([]*chatpb.Metadata, len(chats))
	for i, c := range chats {
		key := string(c.ID.Value)
		md := c.ToProto()
		if standing.IsMember {
			md.ViewerState = viewerStates[key].ToProto(c.PermissionsFor(viewerID, true))
		}
		if IsGroupChatID(c.ID) {
			// ToProto projects the canonical record, which carries neither a
			// group's members nor its summary. The members are the hydrated ones
			// — the viewer, or no one (see above). Every group in the set exists,
			// but the batch read is eventually consistent, so a group written
			// moments ago may be absent and read as a zero summary, and one moved
			// moments ago may read one transition behind. A caller that has
			// just written the group or its roster holds the authoritative
			// summary and should overwrite this one with it.
			md.Members = make([]*chatpb.Member, 0, len(hydratedMembers[i]))
			for _, m := range hydratedMembers[i] {
				md.Members = append(md.Members, &chatpb.Member{UserId: &commonpb.UserId{Value: append([]byte(nil), m.Value...)}})
			}
			md.RosterSummary = rosterSummaries[key].ToProto()
		}
		md.LastMessage = lastMessages[key]
		if reading == ReadingRedacted && md.LastMessage != nil {
			redacted, err := redact.Message(c.ID, md.LastMessage)
			if err != nil {
				return nil, fmt.Errorf("redacting last message of chat %s: %w", base64.StdEncoding.EncodeToString(c.ID.Value), err)
			}
			md.LastMessage = redacted
		}
		md.LatestEventSequence = latestEventSeqs[key]
		if peer, ok := dmPeerByChat[key]; ok {
			md.IsHidden = blockedPeers[string(peer.Value)]
		}
		if _, ok := lobbyEntries[key]; ok {
			md.InLobby = true
		}
		if IsDmChatType(c.Type) {
			md.UseE2Ee = useE2ee(c, staffByUserId, s.teamUserID)
			md.Rules = s.rules.RulesOf(c)
		}
		// ToProto seeds each picture with its stored ORIGINAL; swap in the full
		// resolved set when the original is servable, else leave that seed.
		if c.ProfilePictureBlobID != nil {
			if renditions, ok := pictureRenditions[string(c.ProfilePictureBlobID.Value)]; ok {
				md.ProfilePicture.Renditions = renditions
			}
		}
		switch detail {
		case fullDetail:
			if c.CoverPictureBlobID != nil {
				if renditions, ok := pictureRenditions[string(c.CoverPictureBlobID.Value)]; ok {
					md.CoverPicture.Renditions = renditions
				}
			}
		case listDetail:
			md.CoverPicture = nil
		}
		assignPointers(md.Members, pointers[key])
		for _, m := range md.Members {
			if err := assignProfile(m, md.Type, publicProfilesByUserId, phoneNumbersByUserId); err != nil {
				return nil, err
			}
		}
		metadata[i] = md
	}
	return metadata, nil
}

// assignProfile fills a member's profile from a batch of public profiles and,
// for a contact DM's member, their phone number. Every chat member is a user
// the profile domain knows, so a missing profile is a data integrity problem
// rather than something to paper over with an invented one.
//
// The batch returns one proto per user, but each member row is filled in per
// chat, so every row gets its own copy. Sharing one would let a contact DM's
// phone number show up on that same user's row in a chat of another type.
// Today's callers pass a single chat or a single-type feed page, so the copy
// is what keeps that true of any future caller.
func assignProfile(m *chatpb.Member, chatType chatpb.ChatType, publicProfiles map[string]*profilepb.UserProfile, phoneNumbers map[string]*commonpb.PhoneNumber) error {
	publicProfile, ok := publicProfiles[string(m.UserId.Value)]
	if !ok {
		return fmt.Errorf("no public profile for chat member %s", base64.StdEncoding.EncodeToString(m.UserId.Value))
	}
	profile := proto.Clone(publicProfile).(*profilepb.UserProfile)
	if chatType == chatpb.ChatType_CONTACT_DM {
		profile.PhoneNumber = phoneNumbers[string(m.UserId.Value)]
	}
	m.UserProfile = profile
	return nil
}

// assignPointers distributes a chat's pointers onto the matching member entries
// by user ID. SENT pointers are never shared with the chat, so they are dropped
// defensively; each member is left with its DELIVERED and/or READ pointers.
func assignPointers(members []*chatpb.Member, pointers []*messagingpb.Pointer) {
	if len(pointers) == 0 {
		return
	}
	byUser := make(map[string][]*messagingpb.Pointer, len(members))
	for _, p := range pointers {
		if p.Type == messagingpb.Pointer_SENT {
			continue
		}
		byUser[string(p.UserId.Value)] = append(byUser[string(p.UserId.Value)], p)
	}
	for _, m := range members {
		m.Pointers = byUser[string(m.UserId.Value)]
	}
}

// useE2ee reports whether a DM's messages are to be end-to-end encrypted (see
// chat.v1.Metadata.use_e2ee): today, exactly when every member of the DM is a
// staff user and none is the Flipcash team account, so the transitional rollout
// reaches staff DMs and no one else. The team account is left out even when it
// is staff: its DMs are opened and written to by the server (see flipcashteam),
// which holds no keys for it. staffByUserId is the staff flag per member, as
// staffFlags returns it; a member it does not name is not staff. teamUserID is
// nil when there is no team account.
func useE2ee(c *Chat, staffByUserId map[string]bool, teamUserID *commonpb.UserId) bool {
	if !IsDmChatType(c.Type) || len(c.Members) == 0 {
		return false
	}
	for _, m := range c.Members {
		if !staffByUserId[string(m.Value)] {
			return false
		}
		if teamUserID != nil && bytes.Equal(m.Value, teamUserID.Value) {
			return false
		}
	}
	return true
}

// staffFlagConcurrency bounds how many staff-flag reads staffFlags has in
// flight at once for one set.
const staffFlagConcurrency = 8

// staffFlags resolves the account store's staff flag for each user, keyed by
// user ID. The store answers one user at a time (behind a cache in
// production), so the reads run concurrently under staffFlagConcurrency and
// the first failure fails the set.
func (s *Server) staffFlags(ctx context.Context, userIDs []*commonpb.UserId) (map[string]bool, error) {
	flags := make([]bool, len(userIDs))
	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(staffFlagConcurrency)
	for i, userID := range userIDs {
		g.Go(func() (err error) {
			flags[i], err = s.accounts.IsStaff(gctx, userID)
			return err
		})
	}
	if err := g.Wait(); err != nil {
		return nil, err
	}
	out := make(map[string]bool, len(userIDs))
	for i, userID := range userIDs {
		out[string(userID.Value)] = flags[i]
	}
	return out, nil
}
