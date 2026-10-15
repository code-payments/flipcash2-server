package chat

import (
	"context"
	"encoding/base64"
	"errors"

	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

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

// SystemMessenger is the write slice of the messaging domain the Chat service
// needs to put its own messages in a chat's log. Like MessagingReader it is
// declared here (consumer side) because messaging imports chat;
// messaging.Sender satisfies it directly.
type SystemMessenger interface {
	// SendUserJoined writes the system message telling a group's members that
	// userID joined it: admitted by admittedBy, or on their own when it is nil.
	// rosterVersion is the roster version after the join, which names it: a
	// repeat of one join writes nothing.
	SendUserJoined(ctx context.Context, chatID *commonpb.ChatId, userID, admittedBy *commonpb.UserId, rosterVersion uint64) error
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

	// systemMessages writes the join messages of a private group (see
	// sendUserJoined). Nil when there is none, which writes nothing.
	systemMessages SystemMessenger

	rules  *RuleEvaluator
	access *Access

	// hydrator builds every chatpb.Metadata the service returns (see
	// Hydrator), shared with the other servers that show chats.
	hydrator *Hydrator

	userEventBus UserEventPublisher
	chatEventBus ChatEventPublisher

	// disablePublicGroupRoster turns GetRoster off for public groups when
	// set: a call naming one is DENIED before membership is read, members
	// included (see roster.go). It is an operator's switch on the one read that
	// walks a whole roster, so the read can be withdrawn from the groups
	// anyone may join without a deploy if it proves too costly. DMs and
	// private groups are served whatever it says.
	disablePublicGroupRoster bool

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
	systemMessages SystemMessenger,

	access *Access,
	hydrator *Hydrator,

	userEventBus UserEventPublisher,
	chatEventBus ChatEventPublisher,

	disablePublicGroupRoster bool,

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

		systemMessages: systemMessages,

		rules:    access.Rules(),
		access:   access,
		hydrator: hydrator,

		userEventBus: userEventBus,
		chatEventBus: chatEventBus,

		disablePublicGroupRoster: disablePublicGroupRoster,

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
// in_lobby, whether they are waiting in its lobby (see lobby.go). Its public
// view is that same record, for a caller with no auth at all (see
// getPublicChat). A private group whose key has not been stored is returned
// like any other.
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

	metadata, err := s.hydrator.hydrate(ctx, userID, standing, standing.Reading(req.GetViewMode()), fullDetail, []*Chat{c})
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
// learn whether a DM exists. A private group's public view is its record
// alone — title, description, pictures, roster summary, is_private — which is
// what a registered non-member gets of it too: the public standing admits no
// one to its messaging state under any mode (see
// Access.PublicListenerStanding), and there is no viewer to be in its lobby.
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

	standing := s.access.PublicListenerStanding(c)
	metadata, err := s.hydrator.hydrate(ctx, nil, standing, standing.Reading(req.GetViewMode()), fullDetail, []*Chat{c})
	if err != nil {
		log.With(zap.Error(err)).Warn("Failure hydrating chat metadata")
		return nil, status.Error(codes.Internal, "")
	}

	return &chatpb.GetChatResponse{
		Result:   chatpb.GetChatResponse_OK,
		Metadata: metadata[0],
	}, nil
}
