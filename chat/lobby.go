package chat

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"time"

	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	chatpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/chat/v1"
	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"
	eventpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/event/v1"
	profilepb "github.com/code-payments/flipcash2-protobuf-api/generated/go/profile/v1"

	"github.com/code-payments/flipcash2-server/model"
)

// The lobby: how a user is admitted to a private group (see Chat.IsPrivate and
// LobbyEntry). A user who wants in enters the group's lobby and waits; the
// group's creator, who alone sees the lobby, admits them with the chat key
// wrapped for them, or denies them. JoinChat is DENIED for a private group,
// so this is the only way in for anyone but the creator.
//
// Every RPC here is gated on the canonical record first, since its absence is
// the only thing that tells NOT_FOUND from DENIED, then on the chat being a
// private group. The creator's RPCs (GetLobbyMembers, AdmitLobbyMember,
// DenyLobbyMember) then require the caller to be the recorded creator and a
// current member: a creator who has left may rejoin and resume, but may not
// run the lobby from outside. A lobby exists only once the group has its key
// (see Access.SpeakerStanding): entering a keyless group's lobby is DENIED, as
// is admitting anyone to one, so no one waits for a group that cannot take
// them and no one is admitted without a key to be wrapped for them. The
// creator is DENIED their own lobby: they rejoin with JoinChat.
//
// An admission is one write (see Store.AdmitFromLobby): the envelope the
// creator wrapped, the membership transition and the entry's removal commit
// together, so an admitted member always has an envelope and a waiting user is
// never both waiting and admitted. An entry is likewise refused in the write
// that would make it for a user who is a member by then (see
// Store.EnterLobby), so a retried EnterLobby racing the admission cannot
// leave a member with an entry. It is announced like any join, as a
// RosterUpdate.MemberJoined to the members and to the admitted user, who
// learns of it that way and fetches their envelope (see Server.GetKeyEnvelope).
//
// The lobby's own changes reach the creator alone, as LobbyUpdates on their
// user topic (see publishLobbyUpdate): a MemberEntered carrying the waiting
// user as a LobbyMember, and a MemberLeft for every way out, a withdrawal, a
// denial or an admission. They are sent to the recorded creator whether or
// not they are a member at the time — a creator who has left is still the
// one person the lobby is for, and may rejoin to act on what they heard — and
// are best-effort, outside the event log, so a creator reconciles by reading
// the lobby. A waiting user is told nothing of a denial; they read in_lobby
// off GetChat (see hydrate) to learn they are no longer waiting.
//
// Caps (see LobbyLimits) are the store's to enforce in the write that would
// exceed them, so they hold under concurrent entries; the server only names
// them (lobbyLimits, DefaultLobbyLimits in production).

const (
	// maxGetLobbyMembersPageSize bounds a single GetLobbyMembers page and is
	// its default.
	maxGetLobbyMembersPageSize = 100

	lobbyTokenVersion = 1

	// lobbyTokenLen is the fixed size of a lobby paging token: the version
	// byte, the chat ID, the entry time and the user ID (see encodeLobbyToken).
	lobbyTokenLen = 1 + GroupChatIDSize + 8 + model.UserIDSize
)

func (s *Server) EnterLobby(ctx context.Context, req *chatpb.EnterLobbyRequest) (*chatpb.EnterLobbyResponse, error) {
	userID, err := s.authz.Authorize(ctx, req, &req.Auth)
	if err != nil {
		return nil, err
	}

	log := s.log.With(
		zap.String("user_id", model.UserIDString(userID)),
		zap.String("chat_id", model.ChatIDString(req.ChatId)),
	)

	c, result, err := s.lobbyChat(ctx, log, req.ChatId)
	if err != nil {
		return nil, err
	}
	switch result {
	case lobbyChatNotFound:
		return &chatpb.EnterLobbyResponse{Result: chatpb.EnterLobbyResponse_NOT_FOUND}, nil
	case lobbyChatDenied:
		return &chatpb.EnterLobbyResponse{Result: chatpb.EnterLobbyResponse_DENIED}, nil
	}
	if c.IsCreator(userID) {
		return &chatpb.EnterLobbyResponse{Result: chatpb.EnterLobbyResponse_DENIED}, nil
	}

	// A member is answered here, cheaply and before the key is asked about;
	// the store's write decides it again atomically (see Store.EnterLobby),
	// so one admitted between this read and that write is refused there and
	// never gains an entry.
	isMember, err := s.access.IsMemberWithChat(ctx, c, userID)
	if err != nil {
		log.With(zap.Error(err)).Warn("Failure checking chat membership")
		return nil, status.Error(codes.Internal, "")
	}
	if isMember {
		return &chatpb.EnterLobbyResponse{Result: chatpb.EnterLobbyResponse_ALREADY_MEMBER}, nil
	}

	// A keyless group has no lobby (see above). Decided off the record's
	// rules, as the speaker gate decides it.
	keyed, err := s.access.hasKey(ctx, c.ID, c.GroupRules())
	if err != nil {
		log.With(zap.Error(err)).Warn("Failure checking chat key")
		return nil, status.Error(codes.Internal, "")
	}
	if !keyed {
		return &chatpb.EnterLobbyResponse{Result: chatpb.EnterLobbyResponse_DENIED}, nil
	}

	entry, changed, err := s.chats.EnterLobby(ctx, c.ID, userID, s.lobbyLimits)
	switch {
	case errors.Is(err, ErrAlreadyMember):
		return &chatpb.EnterLobbyResponse{Result: chatpb.EnterLobbyResponse_ALREADY_MEMBER}, nil
	case errors.Is(err, ErrLobbyFull):
		return &chatpb.EnterLobbyResponse{Result: chatpb.EnterLobbyResponse_LOBBY_FULL}, nil
	case errors.Is(err, ErrTooManyLobbies):
		return &chatpb.EnterLobbyResponse{Result: chatpb.EnterLobbyResponse_TOO_MANY_LOBBIES}, nil
	case err != nil:
		log.With(zap.Error(err)).Warn("Failure entering lobby")
		return nil, status.Error(codes.Internal, "")
	}

	// The chat as a waiting non-member sees it: the record alone (see
	// hydrate), with in_lobby set off the entry this call holds rather than a
	// read that would only repeat it.
	metadata, err := s.hydrate(ctx, userID, ListenerStanding{}, ReadingDenied, fullDetail, []*Chat{c})
	if err != nil {
		// The entry has landed; only the read back failed. A retry is the
		// no-op path and returns the lobby then.
		log.With(zap.Error(err)).Warn("Failure hydrating chat metadata")
		return nil, status.Error(codes.Internal, "")
	}
	md := metadata[0]
	md.InLobby = true

	if changed {
		members, err := s.hydrateLobbyMembers(ctx, []LobbyEntry{entry})
		if err != nil {
			// The entry has landed and the response stands; the creator
			// reconciles by reading the lobby, as for any missed update.
			log.With(zap.Error(err)).Warn("Failure hydrating lobby member for update")
		} else {
			s.publishLobbyUpdate(c, &chatpb.LobbyUpdate{Kind: &chatpb.LobbyUpdate_MemberEntered_{
				MemberEntered: &chatpb.LobbyUpdate_MemberEntered{Member: members[0]},
			}})
		}
	}

	return &chatpb.EnterLobbyResponse{
		Result: chatpb.EnterLobbyResponse_OK,
		Lobby: &chatpb.Lobby{
			Chat:      md,
			EnteredAt: timestamppb.New(entry.EnteredAt),
		},
	}, nil
}

func (s *Server) LeaveLobby(ctx context.Context, req *chatpb.LeaveLobbyRequest) (*chatpb.LeaveLobbyResponse, error) {
	userID, err := s.authz.Authorize(ctx, req, &req.Auth)
	if err != nil {
		return nil, err
	}

	log := s.log.With(
		zap.String("user_id", model.UserIDString(userID)),
		zap.String("chat_id", model.ChatIDString(req.ChatId)),
	)

	c, result, err := s.lobbyChat(ctx, log, req.ChatId)
	if err != nil {
		return nil, err
	}
	switch result {
	case lobbyChatNotFound:
		return &chatpb.LeaveLobbyResponse{Result: chatpb.LeaveLobbyResponse_NOT_FOUND}, nil
	case lobbyChatDenied:
		return &chatpb.LeaveLobbyResponse{Result: chatpb.LeaveLobbyResponse_DENIED}, nil
	}

	// Whether the group has its key is not asked: a user may always withdraw
	// from wherever they are waiting.
	changed, err := s.chats.LeaveLobby(ctx, c.ID, userID)
	if err != nil {
		log.With(zap.Error(err)).Warn("Failure leaving lobby")
		return nil, status.Error(codes.Internal, "")
	}
	if changed {
		s.publishLobbyLeft(c, userID)
	}
	return &chatpb.LeaveLobbyResponse{Result: chatpb.LeaveLobbyResponse_OK}, nil
}

func (s *Server) GetLobbyMembers(ctx context.Context, req *chatpb.GetLobbyMembersRequest) (*chatpb.GetLobbyMembersResponse, error) {
	userID, err := s.authz.Authorize(ctx, req, &req.Auth)
	if err != nil {
		return nil, err
	}

	log := s.log.With(
		zap.String("user_id", model.UserIDString(userID)),
		zap.String("chat_id", model.ChatIDString(req.ChatId)),
	)

	limit := maxGetLobbyMembersPageSize
	if pageSize := req.GetQueryOptions().GetPageSize(); pageSize > 0 && int(pageSize) < limit {
		limit = int(pageSize)
	}

	// A token names the chat it was minted for, and is refused elsewhere (see
	// GetRoster for the same rule).
	var after *LobbyPosition
	if token := req.GetQueryOptions().GetPagingToken(); token != nil {
		pos, ok := decodeLobbyToken(token, req.ChatId)
		if !ok {
			return nil, status.Error(codes.InvalidArgument, "invalid paging token")
		}
		after = &pos
	}

	c, result, err := s.lobbyCreatorChat(ctx, log, req.ChatId, userID)
	if err != nil {
		return nil, err
	}
	switch result {
	case lobbyChatNotFound:
		return &chatpb.GetLobbyMembersResponse{Result: chatpb.GetLobbyMembersResponse_NOT_FOUND}, nil
	case lobbyChatDenied:
		return &chatpb.GetLobbyMembersResponse{Result: chatpb.GetLobbyMembersResponse_DENIED}, nil
	}

	// One more than the page, for an exact has_more, as the roster's paged
	// shape reads it.
	entries, err := s.chats.GetLobbyPage(ctx, c.ID, after, limit+1)
	if err != nil {
		log.With(zap.Error(err)).Warn("Failure reading lobby page")
		return nil, status.Error(codes.Internal, "")
	}
	hasMore := len(entries) > limit
	if hasMore {
		entries = entries[:limit]
	}

	members, err := s.hydrateLobbyMembers(ctx, entries)
	if err != nil {
		log.With(zap.Error(err)).Warn("Failure hydrating lobby page")
		return nil, status.Error(codes.Internal, "")
	}

	resp := &chatpb.GetLobbyMembersResponse{
		Result:  chatpb.GetLobbyMembersResponse_OK,
		Members: members,
		HasMore: hasMore,
	}
	if hasMore {
		resp.PagingToken = encodeLobbyToken(c.ID, entries[len(entries)-1].Position())
	}
	return resp, nil
}

func (s *Server) AdmitLobbyMember(ctx context.Context, req *chatpb.AdmitLobbyMemberRequest) (*chatpb.AdmitLobbyMemberResponse, error) {
	userID, err := s.authz.Authorize(ctx, req, &req.Auth)
	if err != nil {
		return nil, err
	}

	log := s.log.With(
		zap.String("user_id", model.UserIDString(userID)),
		zap.String("chat_id", model.ChatIDString(req.ChatId)),
		zap.String("admitted_user_id", model.UserIDString(req.UserId)),
	)

	c, result, err := s.lobbyCreatorChat(ctx, log, req.ChatId, userID)
	if err != nil {
		return nil, err
	}
	switch result {
	case lobbyChatNotFound:
		return &chatpb.AdmitLobbyMemberResponse{Result: chatpb.AdmitLobbyMemberResponse_NOT_FOUND}, nil
	case lobbyChatDenied:
		return &chatpb.AdmitLobbyMemberResponse{Result: chatpb.AdmitLobbyMemberResponse_DENIED}, nil
	}

	// Nobody is admitted to a group before its creator holds its key (see
	// above). The creator is the caller by now, so this is their own envelope.
	keyed, err := s.access.hasKey(ctx, c.ID, c.GroupRules())
	if err != nil {
		log.With(zap.Error(err)).Warn("Failure checking chat key")
		return nil, status.Error(codes.Internal, "")
	}
	if !keyed {
		return &chatpb.AdmitLobbyMemberResponse{Result: chatpb.AdmitLobbyMemberResponse_DENIED}, nil
	}

	// A member already is the no-op the proto promises, their envelope left
	// as it is: decided here, before the write, since the write would replace
	// it. The creator admitting themself is that case too.
	isMember, err := s.chats.IsMember(ctx, c.ID, req.UserId)
	if err != nil {
		log.With(zap.Error(err)).Warn("Failure checking admitted user's membership")
		return nil, status.Error(codes.Internal, "")
	}
	if isMember {
		return &chatpb.AdmitLobbyMemberResponse{Result: chatpb.AdmitLobbyMemberResponse_OK}, nil
	}

	envelope := KeyEnvelopeFromProto(req.KeyEnvelope, userID)
	changed, roster, err := s.chats.AdmitFromLobby(ctx, c.ID, req.UserId, envelope)
	switch {
	case errors.Is(err, ErrNotInLobby):
		return &chatpb.AdmitLobbyMemberResponse{Result: chatpb.AdmitLobbyMemberResponse_NOT_IN_LOBBY}, nil
	case errors.Is(err, ErrChatNotFound):
		return &chatpb.AdmitLobbyMemberResponse{Result: chatpb.AdmitLobbyMemberResponse_NOT_FOUND}, nil
	case err != nil:
		log.With(zap.Error(err)).Warn("Failure admitting lobby member")
		return nil, status.Error(codes.Internal, "")
	}
	if !changed {
		// Admitted by a concurrent call between the membership check and the
		// write: theirs announced it.
		return &chatpb.AdmitLobbyMemberResponse{Result: chatpb.AdmitLobbyMemberResponse_OK}, nil
	}

	// Announced as any join is (see JoinChat): the admitted user's own entry
	// as hydrated for them, with the metadata to their own devices so they
	// can insert the chat, and the record the write produced.
	metadata, err := s.hydrate(ctx, req.UserId, memberListenerStanding, ReadingFull, fullDetail, []*Chat{c})
	if err != nil {
		// The admission has landed; only the read back failed. The admitted
		// user is told nothing, and reads the chat on their next open; the
		// creator's retry is the no-op path.
		log.With(zap.Error(err)).Warn("Failure hydrating chat metadata for admitted user")
		return nil, status.Error(codes.Internal, "")
	}
	md := metadata[0]
	rosterSummary := roster.ToProto()
	md.RosterSummary = rosterSummary
	member, err := s.announcedMember(ctx, log, c.ID, req.UserId, md.Members[0].UserProfile, roster)
	if err != nil {
		return nil, err
	}
	toMembers := &chatpb.RosterUpdate{
		Kind:          &chatpb.RosterUpdate_MemberJoined_{MemberJoined: &chatpb.RosterUpdate_MemberJoined{Member: member}},
		RosterSummary: rosterSummary,
	}
	toJoiner := &chatpb.RosterUpdate{
		Kind: &chatpb.RosterUpdate_MemberJoined_{MemberJoined: &chatpb.RosterUpdate_MemberJoined{
			Member:   member,
			Metadata: md,
		}},
		RosterSummary: rosterSummary,
	}
	s.publishRosterUpdate(c.ID, req.UserId, toMembers, toJoiner)
	s.publishLobbyLeft(c, req.UserId)

	return &chatpb.AdmitLobbyMemberResponse{Result: chatpb.AdmitLobbyMemberResponse_OK}, nil
}

func (s *Server) DenyLobbyMember(ctx context.Context, req *chatpb.DenyLobbyMemberRequest) (*chatpb.DenyLobbyMemberResponse, error) {
	userID, err := s.authz.Authorize(ctx, req, &req.Auth)
	if err != nil {
		return nil, err
	}

	log := s.log.With(
		zap.String("user_id", model.UserIDString(userID)),
		zap.String("chat_id", model.ChatIDString(req.ChatId)),
		zap.String("denied_user_id", model.UserIDString(req.UserId)),
	)

	c, result, err := s.lobbyCreatorChat(ctx, log, req.ChatId, userID)
	if err != nil {
		return nil, err
	}
	switch result {
	case lobbyChatNotFound:
		return &chatpb.DenyLobbyMemberResponse{Result: chatpb.DenyLobbyMemberResponse_NOT_FOUND}, nil
	case lobbyChatDenied:
		return &chatpb.DenyLobbyMemberResponse{Result: chatpb.DenyLobbyMemberResponse_DENIED}, nil
	}

	changed, err := s.chats.LeaveLobby(ctx, c.ID, req.UserId)
	if err != nil {
		log.With(zap.Error(err)).Warn("Failure removing lobby member")
		return nil, status.Error(codes.Internal, "")
	}
	if changed {
		s.publishLobbyLeft(c, req.UserId)
	}
	return &chatpb.DenyLobbyMemberResponse{Result: chatpb.DenyLobbyMemberResponse_OK}, nil
}

// lobbyChatResult is the shared gate's verdict on a lobby RPC's chat.
type lobbyChatResult uint8

const (
	lobbyChatOK lobbyChatResult = iota
	lobbyChatNotFound
	lobbyChatDenied
)

// lobbyChat is the gate every lobby RPC starts with: the chat is a group
// (DENIED before any read for a DM ID), its record exists (NOT_FOUND), and
// it is a private group (DENIED). On OK the record is returned for the
// caller's further gates. A gRPC error is returned only for a failed read.
func (s *Server) lobbyChat(ctx context.Context, log *zap.Logger, chatID *commonpb.ChatId) (*Chat, lobbyChatResult, error) {
	if !IsGroupChatID(chatID) {
		return nil, lobbyChatDenied, nil
	}
	c, err := s.chats.GetChatByID(ctx, chatID)
	switch {
	case errors.Is(err, ErrChatNotFound):
		return nil, lobbyChatNotFound, nil
	case err != nil:
		log.With(zap.Error(err)).Warn("Failure getting chat")
		return nil, 0, status.Error(codes.Internal, "")
	}
	if !c.IsPrivate {
		return nil, lobbyChatDenied, nil
	}
	return c, lobbyChatOK, nil
}

// lobbyCreatorChat is lobbyChat and then the creator's gate: the caller is
// the recorded creator and a current member (DENIED otherwise). Membership is
// the store's, strongly consistent, as every gate reads it.
func (s *Server) lobbyCreatorChat(ctx context.Context, log *zap.Logger, chatID *commonpb.ChatId, userID *commonpb.UserId) (*Chat, lobbyChatResult, error) {
	c, result, err := s.lobbyChat(ctx, log, chatID)
	if err != nil || result != lobbyChatOK {
		return nil, result, err
	}
	if !c.IsCreator(userID) {
		return nil, lobbyChatDenied, nil
	}
	isMember, err := s.access.IsMemberWithChat(ctx, c, userID)
	if err != nil {
		log.With(zap.Error(err)).Warn("Failure checking chat membership")
		return nil, 0, status.Error(codes.Internal, "")
	}
	if !isMember {
		return nil, lobbyChatDenied, nil
	}
	return c, lobbyChatOK, nil
}

// hydrateLobbyMembers fills a page of entries into LobbyMembers: each user's
// public profile and the public key they registered with, which is what the
// creator wraps the chat key for. Every waiting user is a registered user the
// profile and account domains know, so a missing profile or key is a data
// integrity problem, not something to stand in for. The two reads run
// concurrently, the profiles batched, the keys one per user (the account
// store reads one user at a time, and a page is at most a hundred).
func (s *Server) hydrateLobbyMembers(ctx context.Context, entries []LobbyEntry) ([]*chatpb.LobbyMember, error) {
	out := make([]*chatpb.LobbyMember, 0, len(entries))
	if len(entries) == 0 {
		return out, nil
	}
	userIDs := make([]*commonpb.UserId, len(entries))
	for i, e := range entries {
		userIDs[i] = e.UserID
	}

	var publicProfiles map[string]*profilepb.UserProfile
	publicKeys := make([]*commonpb.PublicKey, len(entries))
	g, gctx := errgroup.WithContext(ctx)
	g.Go(func() (err error) {
		publicProfiles, err = s.profiles.GetLimitedPublicProfilesForRow(gctx, userIDs)
		return err
	})
	for i, userID := range userIDs {
		g.Go(func() error {
			keys, err := s.accounts.GetPubKeys(gctx, userID)
			if err != nil {
				return err
			}
			if len(keys) == 0 {
				return fmt.Errorf("lobby member %s has no public key", model.UserIDString(userID))
			}
			publicKeys[i] = keys[0]
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return nil, err
	}

	for i, e := range entries {
		publicProfile, ok := publicProfiles[string(e.UserID.Value)]
		if !ok {
			return nil, fmt.Errorf("lobby member %s has no profile", model.UserIDString(e.UserID))
		}
		profile := proto.Clone(publicProfile).(*profilepb.UserProfile)
		profile.UserId = &commonpb.UserId{Value: append([]byte(nil), e.UserID.Value...)}
		out = append(out, &chatpb.LobbyMember{
			UserProfile: profile,
			PublicKey:   publicKeys[i],
			EnteredAt:   timestamppb.New(e.EnteredAt),
		})
	}
	return out, nil
}

// publishLobbyLeft announces that userID is no longer in c's lobby, whichever
// way they left (see publishLobbyUpdate).
func (s *Server) publishLobbyLeft(c *Chat, userID *commonpb.UserId) {
	s.publishLobbyUpdate(c, &chatpb.LobbyUpdate{Kind: &chatpb.LobbyUpdate_MemberLeft_{
		MemberLeft: &chatpb.LobbyUpdate_MemberLeft{UserId: userID},
	}})
}

// publishLobbyUpdate sends one change to c's lobby to its creator's devices,
// on the creator's user topic alone: the lobby is theirs to see, so it never
// rides the chat topic, where the members would hear it. A group with no
// recorded creator has no one to tell. It is best-effort and non-blocking, as
// every publish here is, and never fails the RPC whose change it announces.
func (s *Server) publishLobbyUpdate(c *Chat, update *chatpb.LobbyUpdate) {
	if c.CreatorID == nil {
		return
	}
	s.userEventBus.OnEvent(c.CreatorID, &eventpb.Event{
		Id: model.MustGenerateEventID(),
		Ts: timestamppb.Now(),
		Type: &eventpb.Event_ChatUpdate{ChatUpdate: &eventpb.ChatUpdate{
			Chat:         c.ID,
			LobbyUpdates: &chatpb.LobbyUpdateBatch{LobbyUpdates: []*chatpb.LobbyUpdate{update}},
		}},
	})
}

// encodeLobbyToken serializes a page's resume position, bound to the chat it
// pages: the version byte, the chat ID, the entry time as big-endian
// nanoseconds, and the user ID (see encodeRosterToken for the same shape).
func encodeLobbyToken(chatID *commonpb.ChatId, pos LobbyPosition) *commonpb.PagingToken {
	buf := make([]byte, lobbyTokenLen)
	buf[0] = lobbyTokenVersion
	copy(buf[1:1+GroupChatIDSize], chatID.Value)
	binary.BigEndian.PutUint64(buf[1+GroupChatIDSize:], uint64(pos.EnteredAt.UnixNano()))
	copy(buf[1+GroupChatIDSize+8:], pos.UserID.Value)
	return &commonpb.PagingToken{Value: buf}
}

// decodeLobbyToken parses a lobby token minted for chatID; a malformed token,
// or one bound to another chat, is refused.
func decodeLobbyToken(token *commonpb.PagingToken, chatID *commonpb.ChatId) (LobbyPosition, bool) {
	b := token.GetValue()
	if len(b) != lobbyTokenLen || b[0] != lobbyTokenVersion {
		return LobbyPosition{}, false
	}
	if !bytes.Equal(b[1:1+GroupChatIDSize], chatID.GetValue()) {
		return LobbyPosition{}, false
	}
	return LobbyPosition{
		EnteredAt: time.Unix(0, int64(binary.BigEndian.Uint64(b[1+GroupChatIDSize:]))).UTC(),
		UserID:    &commonpb.UserId{Value: append([]byte(nil), b[1+GroupChatIDSize+8:]...)},
	}, true
}
