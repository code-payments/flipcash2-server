package chat

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"time"

	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	chatpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/chat/v1"
	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"
	profilepb "github.com/code-payments/flipcash2-protobuf-api/generated/go/profile/v1"

	"github.com/code-payments/flipcash2-server/model"
)

// Moderation: a group's creator restricting what a user may do in it. There
// are two restrictions, both recorded on the user's ViewerState in the group
// (see Store.SetSilence and Store.BanUser) and both enforced by Access, where
// every gate reads them:
//
//   - A silence, until a time or indefinitely, takes away speaking: a send,
//     an edit, a typing notification, a mention suggestion, a poll or payment
//     request, a group payment, an encrypted upload — everything gated on
//     Access.SpeakerStanding. The silenced user still reads, reacts, votes,
//     boosts and pays their share of a request, none of which asks the
//     speaker gate, and may still delete their own messages (see
//     Access.CanDelete).
//   - A ban removes the user from the roster, as a departure like any other,
//     and from a private group's lobby, in the one write that records it;
//     refuses them a join, a lobby entry and an admission until it is lifted
//     (see ErrBanned); and takes away a full non-member read of a public
//     group, leaving them the redacted one anyone has (see
//     Access.publicGroupNonMember).
//
// Public and private groups alike; a DM has no creator and is never
// moderated. Only the recorded creator, while a member, moderates (see
// Chat.PermissionsFor and ViewerState.Permissions.moderation, whose two
// flags gate their own RPCs: can_silence SilenceUser and UnsilenceUser,
// can_ban BanUser and UnbanUser, and either GetRestrictedUsers): every RPC
// here is gated as EditChat is — a DM ID DENIED before any read, the record
// for NOT_FOUND, creatorship off the record and only then membership, so a
// caller who could never moderate costs no membership read — and a departed
// creator moderates nothing until they rejoin. The creator cannot be
// restricted (CANNOT_RESTRICT_CREATOR), so lifting a restriction from them is
// the no-op it always is. The target must be a well-formed user ID, exactly
// model.UserIDSize bytes, or the request is InvalidArgument before anything
// is read (see validTarget): the proto admits any length, and the listing
// and its paging token hold user IDs at that size. That the ID names a
// registered user is not checked, since the proto has no result for one
// that does not: a restriction on an ID nobody holds records a row that
// gates no one, and the listing leaves out (see hydrateRestrictedUsers). A
// restriction may be set on anyone else, member or
// not — a creator can ban someone before they ever join — and survives
// leaving and rejoining. Lifting a ban does not readmit: the user joins, or
// enters the lobby, again like anyone.
//
// Each write is the store's no-op-aware one, so a repeated request is OK with
// nothing published. What is published:
//
//   - A real silence or unsilence reaches the target's own devices as a
//     MetadataUpdate.ViewerStateChanged on their user topic, and only while
//     they are a member: a non-member is shown no ViewerState (see hydrate),
//     and a user who joins later reads the silence off their metadata. No one
//     else is told — a silence is between the creator and the user.
//   - A ban that removed a member is announced exactly as a departure (see
//     publishRosterUpdate): a RosterUpdate.MemberLeft to the banned user's
//     devices, which does not say why (they learn of the ban from a BANNED
//     result when they try to return), and MembershipChanged, naming no one,
//     to the other members. Their mute is cleared as a leave clears one (see
//     clearMuteOnLeave). A ban that removed a lobby entry tells the creator,
//     as a denial does (see publishLobbyLeft). The banned user gets no push
//     and the log no system message: a ban, like a leave, writes nothing into
//     the conversation.
//   - An unban publishes nothing: the user is not a member and has nothing to
//     update.
//
// GetRestrictedUsers is the creator's listing of whom they have restricted,
// read from an index that may trail a write by a moment (see
// Store.GetRestrictedUsersPage).

const (
	// maxGetRestrictedUsersPageSize bounds a single GetRestrictedUsers page
	// and is its default.
	maxGetRestrictedUsersPageSize = 100

	// maxRestrictedUsersReads bounds the store reads one GetRestrictedUsers
	// page makes: lapsed silences are recorded restrictions the listing
	// leaves out, so a page reads on past them, but no further than this
	// many reads, ending early with has_more set and a token past what it
	// skipped.
	maxRestrictedUsersReads = 4

	restrictionTokenVersion = 1

	// restrictionTokenLen is the fixed size of a GetRestrictedUsers paging
	// token: the version byte, the chat ID, the restriction time and the
	// user ID (see encodeRestrictionToken).
	restrictionTokenLen = 1 + GroupChatIDSize + 8 + model.UserIDSize
)

// SilenceUser records a silence on a user in a group (see above), until a
// time or indefinitely, replacing any silence already set. A timed silence
// must end in the future as it will be recorded (see Silence.Normalize), as
// MuteChat requires of a mute, and is refused as malformed, with
// InvalidArgument, before anything is read.
func (s *Server) SilenceUser(ctx context.Context, req *chatpb.SilenceUserRequest) (*chatpb.SilenceUserResponse, error) {
	userID, err := s.authz.Authorize(ctx, req, &req.Auth)
	if err != nil {
		return nil, err
	}

	log := s.moderationLog(userID, req.ChatId, req.UserId)

	if err := validTarget(req.UserId); err != nil {
		return nil, err
	}

	silence, err := SilenceFromProto(req.Silence).Normalize()
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	if !silence.Forever && !silence.Until.After(time.Now()) {
		return nil, status.Error(codes.InvalidArgument, "silence must end in the future")
	}

	c, result, err := s.moderatedChat(ctx, log, req.ChatId, userID, canSilence)
	if err != nil {
		return nil, err
	}
	switch result {
	case moderationNotFound:
		return &chatpb.SilenceUserResponse{Result: chatpb.SilenceUserResponse_NOT_FOUND}, nil
	case moderationDenied:
		return &chatpb.SilenceUserResponse{Result: chatpb.SilenceUserResponse_DENIED}, nil
	}
	if c.IsCreator(req.UserId) {
		return &chatpb.SilenceUserResponse{Result: chatpb.SilenceUserResponse_CANNOT_RESTRICT_CREATOR}, nil
	}

	state, changed, err := s.chats.SetSilence(ctx, c.ID, req.UserId, silence, time.Now())
	if err != nil {
		if errors.Is(err, ErrSilenceUntilOutOfRange) {
			return nil, status.Error(codes.InvalidArgument, err.Error())
		}
		log.With(zap.Error(err)).Warn("Failure recording silence")
		return nil, status.Error(codes.Internal, "")
	}
	if changed {
		s.publishRestrictionChanged(ctx, log, c, req.UserId, state)
	}
	return &chatpb.SilenceUserResponse{Result: chatpb.SilenceUserResponse_OK}, nil
}

// UnsilenceUser clears a user's silence in a group, lapsed or not. It is
// gated and published as SilenceUser is; a user who is not silenced, the
// creator among them, is the no-op that publishes nothing.
func (s *Server) UnsilenceUser(ctx context.Context, req *chatpb.UnsilenceUserRequest) (*chatpb.UnsilenceUserResponse, error) {
	userID, err := s.authz.Authorize(ctx, req, &req.Auth)
	if err != nil {
		return nil, err
	}

	log := s.moderationLog(userID, req.ChatId, req.UserId)

	if err := validTarget(req.UserId); err != nil {
		return nil, err
	}

	c, result, err := s.moderatedChat(ctx, log, req.ChatId, userID, canSilence)
	if err != nil {
		return nil, err
	}
	switch result {
	case moderationNotFound:
		return &chatpb.UnsilenceUserResponse{Result: chatpb.UnsilenceUserResponse_NOT_FOUND}, nil
	case moderationDenied:
		return &chatpb.UnsilenceUserResponse{Result: chatpb.UnsilenceUserResponse_DENIED}, nil
	}
	if c.IsCreator(req.UserId) {
		// The creator is never silenced, so there is nothing to clear.
		return &chatpb.UnsilenceUserResponse{Result: chatpb.UnsilenceUserResponse_OK}, nil
	}

	state, changed, err := s.chats.ClearSilence(ctx, c.ID, req.UserId)
	if err != nil {
		log.With(zap.Error(err)).Warn("Failure clearing silence")
		return nil, status.Error(codes.Internal, "")
	}
	if changed {
		s.publishRestrictionChanged(ctx, log, c, req.UserId, state)
	}
	return &chatpb.UnsilenceUserResponse{Result: chatpb.UnsilenceUserResponse_OK}, nil
}

// BanUser bans a user from a group (see above): one store write records the
// ban, removes them from the roster if they are a member — taking their key
// envelope in a private group, as their own departure would (see
// Server.LeaveChat; the creator, whose envelope is kept, cannot be banned) —
// and removes their lobby entry if they are waiting, and the store reports
// which it did, so each is announced exactly once, by the call that did it.
//
// The banned user's mute is cleared on every OK ban, whether or not this call
// removed them, as LeaveChat clears one on every OK leave: a repeated ban is
// the retry that repairs a clear the first one failed, and a user banned
// while not a member has no mute in the common case, a no-op that costs one
// read.
func (s *Server) BanUser(ctx context.Context, req *chatpb.BanUserRequest) (*chatpb.BanUserResponse, error) {
	userID, err := s.authz.Authorize(ctx, req, &req.Auth)
	if err != nil {
		return nil, err
	}

	log := s.moderationLog(userID, req.ChatId, req.UserId)

	if err := validTarget(req.UserId); err != nil {
		return nil, err
	}

	c, result, err := s.moderatedChat(ctx, log, req.ChatId, userID, canBan)
	if err != nil {
		return nil, err
	}
	switch result {
	case moderationNotFound:
		return &chatpb.BanUserResponse{Result: chatpb.BanUserResponse_NOT_FOUND}, nil
	case moderationDenied:
		return &chatpb.BanUserResponse{Result: chatpb.BanUserResponse_DENIED}, nil
	}
	if c.IsCreator(req.UserId) {
		return &chatpb.BanUserResponse{Result: chatpb.BanUserResponse_CANNOT_RESTRICT_CREATOR}, nil
	}

	// The target is not the creator by now, so in a private group their
	// envelope goes with them, by LeaveChat's rule.
	banned, err := s.chats.BanUser(ctx, c.ID, req.UserId, time.Now(), c.IsPrivate)
	switch {
	case errors.Is(err, ErrChatNotFound):
		return &chatpb.BanUserResponse{Result: chatpb.BanUserResponse_NOT_FOUND}, nil
	case err != nil:
		log.With(zap.Error(err)).Warn("Failure banning user")
		return nil, status.Error(codes.Internal, "")
	}

	if banned.Departed {
		update := &chatpb.RosterUpdate{
			Kind: &chatpb.RosterUpdate_MemberLeft_{MemberLeft: &chatpb.RosterUpdate_MemberLeft{
				UserId: req.UserId,
			}},
			RosterSummary: banned.Roster.ToProto(),
		}
		s.publishRosterUpdate(c.ID, req.UserId, update, true)
	}
	if banned.LeftLobby {
		s.publishLobbyLeft(c, req.UserId)
	}
	s.clearMuteOnLeave(ctx, log, c.ID, req.UserId)

	return &chatpb.BanUserResponse{Result: chatpb.BanUserResponse_OK}, nil
}

// UnbanUser lifts a user's ban from a group. It is gated as BanUser is,
// returns no one to the group or its lobby, and publishes nothing; a user who
// is not banned, the creator among them, is the no-op.
func (s *Server) UnbanUser(ctx context.Context, req *chatpb.UnbanUserRequest) (*chatpb.UnbanUserResponse, error) {
	userID, err := s.authz.Authorize(ctx, req, &req.Auth)
	if err != nil {
		return nil, err
	}

	log := s.moderationLog(userID, req.ChatId, req.UserId)

	if err := validTarget(req.UserId); err != nil {
		return nil, err
	}

	c, result, err := s.moderatedChat(ctx, log, req.ChatId, userID, canBan)
	if err != nil {
		return nil, err
	}
	switch result {
	case moderationNotFound:
		return &chatpb.UnbanUserResponse{Result: chatpb.UnbanUserResponse_NOT_FOUND}, nil
	case moderationDenied:
		return &chatpb.UnbanUserResponse{Result: chatpb.UnbanUserResponse_DENIED}, nil
	}
	if c.IsCreator(req.UserId) {
		// The creator is never banned, so there is nothing to lift.
		return &chatpb.UnbanUserResponse{Result: chatpb.UnbanUserResponse_OK}, nil
	}

	if _, _, err := s.chats.UnbanUser(ctx, c.ID, req.UserId); err != nil {
		log.With(zap.Error(err)).Warn("Failure lifting ban")
		return nil, status.Error(codes.Internal, "")
	}
	return &chatpb.UnbanUserResponse{Result: chatpb.UnbanUserResponse_OK}, nil
}

// GetRestrictedUsers pages the users the group's creator has restricted, most
// recently restricted first (see RestrictionPosition), each with their public
// profile, their silence while it is in force, and when they were banned. A
// user whose only restriction is a lapsed silence is left out, as the proto
// promises: the store lists it (see Store.GetRestrictedUsersPage), so the
// page reads on past such users until it fills, at most
// maxRestrictedUsersReads reads, each one more than the page still needs for
// an exact has_more, as GetLobbyMembers reads. A page that runs out of reads
// first ends with has_more set and a token after the last user it read,
// shown or not, so the next page never reads them again; it may then carry
// fewer users than asked for, none at all in the extreme, which the proto's
// paging contract allows. Which silences have lapsed is judged once, at the
// page's start. The token is bound to the chat, as every paging token here
// is, and refused elsewhere.
func (s *Server) GetRestrictedUsers(ctx context.Context, req *chatpb.GetRestrictedUsersRequest) (*chatpb.GetRestrictedUsersResponse, error) {
	userID, err := s.authz.Authorize(ctx, req, &req.Auth)
	if err != nil {
		return nil, err
	}

	log := s.log.With(
		zap.String("user_id", model.UserIDString(userID)),
		zap.String("chat_id", model.ChatIDString(req.ChatId)),
	)

	limit := maxGetRestrictedUsersPageSize
	if pageSize := req.GetQueryOptions().GetPageSize(); pageSize > 0 && int(pageSize) < limit {
		limit = int(pageSize)
	}

	var after *RestrictionPosition
	if token := req.GetQueryOptions().GetPagingToken(); token != nil {
		pos, ok := decodeRestrictionToken(token, req.ChatId)
		if !ok {
			return nil, status.Error(codes.InvalidArgument, "invalid paging token")
		}
		after = &pos
	}

	c, result, err := s.moderatedChat(ctx, log, req.ChatId, userID, ModerationPermissions.Any)
	if err != nil {
		return nil, err
	}
	switch result {
	case moderationNotFound:
		return &chatpb.GetRestrictedUsersResponse{Result: chatpb.GetRestrictedUsersResponse_NOT_FOUND}, nil
	case moderationDenied:
		return &chatpb.GetRestrictedUsersResponse{Result: chatpb.GetRestrictedUsersResponse_DENIED}, nil
	}

	now := time.Now()
	var (
		shown    []RestrictedUser
		lastRead *RestrictionPosition
		hasMore  bool
	)
	for reads := 0; ; reads++ {
		want := limit + 1 - len(shown)
		page, err := s.chats.GetRestrictedUsersPage(ctx, c.ID, after, want)
		if err != nil {
			log.With(zap.Error(err)).Warn("Failure reading restricted users")
			return nil, status.Error(codes.Internal, "")
		}
		for _, u := range page {
			if u.State.IsBanned() || u.State.ActiveSilence(now) != nil {
				shown = append(shown, u)
			}
		}
		if len(page) < want {
			// The listing is exhausted.
			break
		}
		pos := page[len(page)-1].Position()
		after, lastRead = &pos, &pos
		if len(shown) > limit {
			hasMore = true
			break
		}
		if reads+1 == maxRestrictedUsersReads {
			// Out of reads with the listing not exhausted: more may follow,
			// resumed after the last user read.
			hasMore = true
			break
		}
	}

	var resume RestrictionPosition
	if len(shown) > limit {
		shown = shown[:limit]
		resume = shown[limit-1].Position()
	} else if hasMore {
		resume = *lastRead
	}

	users, err := s.hydrateRestrictedUsers(ctx, log, shown, now)
	if err != nil {
		log.With(zap.Error(err)).Warn("Failure hydrating restricted users")
		return nil, status.Error(codes.Internal, "")
	}

	resp := &chatpb.GetRestrictedUsersResponse{
		Result:  chatpb.GetRestrictedUsersResponse_OK,
		Users:   users,
		HasMore: hasMore,
	}
	if hasMore {
		resp.PagingToken = encodeRestrictionToken(c.ID, resume)
	}
	return resp, nil
}

// validTarget refuses, with InvalidArgument, a target that is not a
// well-formed user ID (see above).
func validTarget(targetID *commonpb.UserId) error {
	if len(targetID.GetValue()) != model.UserIDSize {
		return status.Error(codes.InvalidArgument, "invalid user id")
	}
	return nil
}

// moderationResult is the shared gate's verdict on a moderation RPC's chat.
type moderationResult uint8

const (
	moderationOK moderationResult = iota
	moderationNotFound
	moderationDenied
)

// canSilence is the moderation permission SilenceUser and UnsilenceUser
// require.
func canSilence(m ModerationPermissions) bool { return m.CanSilence }

// canBan is the moderation permission BanUser and UnbanUser require.
func canBan(m ModerationPermissions) bool { return m.CanBan }

// moderatedChat is the gate every moderation RPC starts with (see above): the
// chat is a group (DENIED before any read for a DM ID), its record exists
// (NOT_FOUND), and the caller holds the moderation permission the RPC
// requires, allowed — today the recorded creator alone, decided off the
// record before membership is read, and only while a current member, read
// strongly consistent as every gate reads it (DENIED otherwise). On OK the
// record is returned for the caller's further gates. A gRPC error is returned
// only for a failed read.
func (s *Server) moderatedChat(ctx context.Context, log *zap.Logger, chatID *commonpb.ChatId, userID *commonpb.UserId, allowed func(ModerationPermissions) bool) (*Chat, moderationResult, error) {
	if !IsGroupChatID(chatID) {
		return nil, moderationDenied, nil
	}
	c, err := s.chats.GetChatByID(ctx, chatID)
	switch {
	case errors.Is(err, ErrChatNotFound):
		return nil, moderationNotFound, nil
	case err != nil:
		log.With(zap.Error(err)).Warn("Failure getting chat")
		return nil, 0, status.Error(codes.Internal, "")
	}
	if c.Type != chatpb.ChatType_GROUP || !c.IsCreator(userID) {
		return nil, moderationDenied, nil
	}
	isMember, err := s.access.IsMemberWithChat(ctx, c, userID)
	if err != nil {
		log.With(zap.Error(err)).Warn("Failure checking chat membership")
		return nil, 0, status.Error(codes.Internal, "")
	}
	if !allowed(c.PermissionsFor(userID, isMember).Moderation) {
		return nil, moderationDenied, nil
	}
	return c, moderationOK, nil
}

// publishRestrictionChanged announces a real change to targetID's silence in
// c to their own devices (see publishViewerStateChanged), with the
// permissions of a member, which is the only time it is announced: a
// non-member is shown no ViewerState, so their membership is read first,
// strongly consistent. The change has landed by then, so a failed read is
// logged and nothing is published, as for any best-effort publish; the
// target converges from the next Metadata they read.
func (s *Server) publishRestrictionChanged(ctx context.Context, log *zap.Logger, c *Chat, targetID *commonpb.UserId, state ViewerState) {
	isMember, err := s.access.IsMemberWithChat(ctx, c, targetID)
	if err != nil {
		log.With(zap.Error(err)).Warn("Failure checking restricted user's membership")
		return
	}
	if !isMember {
		return
	}
	s.publishViewerStateChanged(targetID, c.ID, state, c.PermissionsFor(targetID, true))
}

// hydrateRestrictedUsers fills a page of restricted users into their proto
// form: each user's public profile, in one batch, and their restrictions as
// of now — the silence only while in force, since the listing leaves a lapsed
// one out (see GetRestrictedUsers), and the ban, with when it was made. A
// user the profile domain does not know is left out and logged, not failed as
// a member is on
// the roster (see hydrateRoster): a restriction may name an ID nobody holds,
// or someone who has since gone, and one such row must not stop the creator
// paging past it. The page may then carry fewer users than it read, which
// the paging contract allows.
func (s *Server) hydrateRestrictedUsers(ctx context.Context, log *zap.Logger, users []RestrictedUser, now time.Time) ([]*chatpb.RestrictedUser, error) {
	out := make([]*chatpb.RestrictedUser, 0, len(users))
	if len(users) == 0 {
		return out, nil
	}
	userIDs := make([]*commonpb.UserId, len(users))
	for i, u := range users {
		userIDs[i] = u.UserID
	}
	publicProfiles, err := s.profiles.GetLimitedPublicProfilesForRow(ctx, userIDs)
	if err != nil {
		return nil, err
	}
	for _, u := range users {
		publicProfile, ok := publicProfiles[string(u.UserID.Value)]
		if !ok {
			log.With(zap.String("restricted_user_id", model.UserIDString(u.UserID))).Warn("Restricted user has no profile")
			continue
		}
		// One proto per user is shared across callers (see
		// ProfileReader.GetLimitedPublicProfilesForRow), so the response
		// carries a copy.
		restricted := &chatpb.RestrictedUser{
			UserId:      &commonpb.UserId{Value: append([]byte(nil), u.UserID.Value...)},
			UserProfile: proto.Clone(publicProfile).(*profilepb.UserProfile),
		}
		if silence := u.State.ActiveSilence(now); silence != nil {
			restricted.Silence = silence.ToProto()
		}
		if u.State.BannedAt != nil {
			restricted.Ban = &chatpb.BanState{BannedAt: timestamppb.New(*u.State.BannedAt)}
		}
		out = append(out, restricted)
	}
	return out, nil
}

// moderationLog is the logger a moderation RPC logs with: the caller, the
// chat and the user acted on.
func (s *Server) moderationLog(userID *commonpb.UserId, chatID *commonpb.ChatId, targetID *commonpb.UserId) *zap.Logger {
	return s.log.With(
		zap.String("user_id", model.UserIDString(userID)),
		zap.String("chat_id", model.ChatIDString(chatID)),
		zap.String("target_user_id", model.UserIDString(targetID)),
	)
}

// encodeRestrictionToken serializes a GetRestrictedUsers page's resume
// position, bound to the chat it pages: the version byte, the chat ID, the
// restriction time as big-endian nanoseconds, and the user ID (see
// encodeLobbyToken for the same shape).
func encodeRestrictionToken(chatID *commonpb.ChatId, pos RestrictionPosition) *commonpb.PagingToken {
	buf := make([]byte, restrictionTokenLen)
	buf[0] = restrictionTokenVersion
	copy(buf[1:1+GroupChatIDSize], chatID.Value)
	binary.BigEndian.PutUint64(buf[1+GroupChatIDSize:], uint64(pos.RestrictedAt.UnixNano()))
	copy(buf[1+GroupChatIDSize+8:], pos.UserID.Value)
	return &commonpb.PagingToken{Value: buf}
}

// decodeRestrictionToken parses a GetRestrictedUsers token minted for chatID;
// a malformed token, or one bound to another chat, is refused.
func decodeRestrictionToken(token *commonpb.PagingToken, chatID *commonpb.ChatId) (RestrictionPosition, bool) {
	b := token.GetValue()
	if len(b) != restrictionTokenLen || b[0] != restrictionTokenVersion {
		return RestrictionPosition{}, false
	}
	if !bytes.Equal(b[1:1+GroupChatIDSize], chatID.GetValue()) {
		return RestrictionPosition{}, false
	}
	return RestrictionPosition{
		RestrictedAt: time.Unix(0, int64(binary.BigEndian.Uint64(b[1+GroupChatIDSize:]))).UTC(),
		UserID:       &commonpb.UserId{Value: append([]byte(nil), b[1+GroupChatIDSize+8:]...)},
	}, true
}
