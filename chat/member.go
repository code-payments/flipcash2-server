package chat

import (
	"context"
	"errors"
	"time"

	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	chatpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/chat/v1"
	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"
	eventpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/event/v1"
	profilepb "github.com/code-payments/flipcash2-protobuf-api/generated/go/profile/v1"

	"github.com/code-payments/flipcash2-server/model"
)

// Self-service group membership: a user joining or leaving a group's roster.
//
// Only a group's roster is open to this. A DM's membership is fixed at creation
// (its two participants derive its ID), so joining or leaving one is denied
// whoever asks. A group's roster is the membership records, and each RPC is one
// transition on them — or none: both are idempotent, so a retry after a lost
// response lands on the state the first call left.
//
// Joining is where a group's listener rules are enforced (see RuleEvaluator):
// a user who does not satisfy them is refused, and never becomes a member.
// Membership is checked first, so the rules are never evaluated on behalf of a
// current member re-joining — a member's reads gate on membership alone (see
// Access), and so does a no-op join, public group or private, whatever the
// request carries: an invite a member passes is not checked, even one revoked
// or to another chat, no use of it is counted, and their envelope stays as it
// is. Leaving has no rule to satisfy.
//
// A private group (see Chat.IsPrivate) is joined here only with an invite
// (see Invite and invite.go) or by its creator. Anyone else is refused as
// DENIED: their way in is its lobby, where its creator admits them (see
// lobby.go). The creator needs no one's approval and rejoins a private group
// they left; an invite they pass is ignored, and no use of it counted. No
// rule is evaluated for either: a private group is not governed by rules and
// has no RuleSet to evaluate (see RuleSet), and being its creator, or holding
// a token for one of its ACTIVE invites, is what admits them. A join with an
// invite is the store's one write (see Store.JoinWithInvite): membership, the
// invite's use, the invite's copy of the chat key as the joiner's envelope,
// and their lobby entry gone, together or not at all.
//
// Only a public group's rules or a private group's invite are checked, never
// both: an invite passed to a public group is INVITE_NOT_FOUND, its rules
// not evaluated, since no invite is ever made to one.
//
// A public group without listener rules is open: the empty set is satisfied
// by everyone, so anyone may join it, as anyone may read it (see Access).
//
// A user the group's creator has banned (see Store.BanUser) is refused as
// BANNED, public or private group, until unbanned. The ban is judged by the
// store in the write that would join them (see ErrBanned), so a ban racing a
// join never leaves a banned member; it is not read ahead of the rules, so a
// banned user who also fails the rules is told RULES_NOT_SATISFIED, the
// answer they would get after an unban too, and one holding an invite that
// admits no one is told why the invite does not (see Store.JoinWithInvite).
//
// Each transition that actually happens is broadcast as a RosterUpdate to the
// chat's members and to the affected user (see publishRosterUpdate): the user
// learns what happened to them, and the other members only that the roster
// moved, with no one named. A join or departure that is a no-op broadcasts
// nothing: the roster did not move, and a client applying updates by version
// would drop it anyway.
//
// A join of a private group also writes a UserJoined system message into the
// group's log (see sendUserJoined), which does name the joiner: everyone
// there is a member, and members see the roster anyway. A join with an invite
// is announced exactly as an admission is (see Server.AdmitLobbyMember and
// announceJoin), the message naming the invite's creator as who invited
// them, and a LobbyUpdate to the creator when the join took the joiner out of
// its lobby, but nobody is pushed: the joiner acted themself, and the
// creator is not pushed for each use of a link they handed out.

// userJoinedMessageTimeout bounds writing a join's system message (see
// sendUserJoined).
const userJoinedMessageTimeout = 5 * time.Second

func (s *Server) JoinChat(ctx context.Context, req *chatpb.JoinChatRequest) (*chatpb.JoinChatResponse, error) {
	userID, err := s.authz.Authorize(ctx, req, &req.Auth)
	if err != nil {
		return nil, err
	}

	log := s.log.With(zap.String("user_id", model.UserIDString(userID)))

	if !IsGroupChatID(req.ChatId) {
		return &chatpb.JoinChatResponse{Result: chatpb.JoinChatResponse_DENIED}, nil
	}

	c, err := s.chats.GetChatByID(ctx, req.ChatId)
	switch {
	case errors.Is(err, ErrChatNotFound):
		return &chatpb.JoinChatResponse{Result: chatpb.JoinChatResponse_NOT_FOUND}, nil
	case err != nil:
		log.With(zap.Error(err)).Warn("Failure getting chat")
		return nil, status.Error(codes.Internal, "")
	}
	if c.Type != chatpb.ChatType_GROUP {
		return &chatpb.JoinChatResponse{Result: chatpb.JoinChatResponse_DENIED}, nil
	}

	// A current member re-joining is a no-op answered on membership alone,
	// before the rules or any invite: a member whose balance has since dipped
	// under the group's requirement is still a member, and a retry after a
	// lost response must say so rather than refuse them. A retry of a join
	// with an invite is that too, whatever has become of the invite since.
	isMember, err := s.chats.IsMember(ctx, req.ChatId, userID)
	if err != nil {
		log.With(zap.Error(err)).Warn("Failure checking chat membership")
		return nil, status.Error(codes.Internal, "")
	}

	if !isMember {
		switch {
		case c.IsPrivate && c.IsCreator(userID):
			// The creator rejoins on their own, needing no approval, and an
			// invite they pass is ignored (see above).
		case c.IsPrivate:
			if req.Invite == nil {
				return &chatpb.JoinChatResponse{Result: chatpb.JoinChatResponse_DENIED}, nil
			}
			return s.joinWithInvite(ctx, c, userID, req.Invite)
		case req.Invite != nil:
			// No invite is made to a public group (see above).
			return &chatpb.JoinChatResponse{Result: chatpb.JoinChatResponse_INVITE_NOT_FOUND}, nil
		default:
			// The rules come from the canonical record already in hand
			// rather than a second read through the evaluator: the record is
			// what they are projected from.
			if ruleSet, ruled := c.ChatRules().RuleSet(); ruled {
				canListen, err := s.rules.CanListenWithRules(ctx, ruleSet, userID)
				if err != nil {
					log.With(zap.Error(err)).Warn("Failure evaluating chat rules")
					return nil, status.Error(codes.Internal, "")
				}
				if !canListen {
					return &chatpb.JoinChatResponse{Result: chatpb.JoinChatResponse_RULES_NOT_SATISFIED}, nil
				}
			}
		}
	}

	// A member of a private group other than its creator is answered without
	// the write: AddGroupMembers takes no invite, so had they left between
	// the membership check and it, it would let them back in with neither an
	// invite nor an envelope.
	if isMember && c.IsPrivate && !c.IsCreator(userID) {
		roster, err := s.chats.GetGroupRosterSummary(ctx, c.ID)
		if err != nil {
			log.With(zap.Error(err)).Warn("Failure getting group roster summary")
			return nil, status.Error(codes.Internal, "")
		}
		md, err := s.joinedMetadata(ctx, log, c, userID, roster)
		if err != nil {
			return nil, err
		}
		return &chatpb.JoinChatResponse{Result: chatpb.JoinChatResponse_OK, Chat: md}, nil
	}

	// Idempotent against a concurrent join from another of the user's devices:
	// both may pass the membership check above, but only the one whose write
	// is the transition sees changed, and only it broadcasts.
	changed, roster, err := s.chats.AddGroupMembers(ctx, req.ChatId, []*commonpb.UserId{userID})
	switch {
	case errors.Is(err, ErrBanned):
		return &chatpb.JoinChatResponse{Result: chatpb.JoinChatResponse_BANNED}, nil
	case err != nil:
		log.With(zap.Error(err)).Warn("Failure adding group member")
		return nil, status.Error(codes.Internal, "")
	}

	var md *chatpb.Metadata
	if changed {
		// Only a private group's creator gets a real join this far in one,
		// and they rejoin on their own.
		md, err = s.announceJoin(ctx, log, c, userID, JoinedDirectly(), roster)
	} else {
		md, err = s.joinedMetadata(ctx, log, c, userID, roster)
	}
	if err != nil {
		return nil, err
	}

	return &chatpb.JoinChatResponse{
		Result: chatpb.JoinChatResponse_OK,
		Chat:   md,
	}, nil
}

// joinWithInvite is JoinChat for a non-member of the private group c, other
// than its creator, who passed an invite: the store's one write (see
// Store.JoinWithInvite) with the invite named by the token's ID, its refusals
// mapped onto JoinChat's results, and a real join announced as an admission
// is, naming the invite's creator.
func (s *Server) joinWithInvite(ctx context.Context, c *Chat, userID *commonpb.UserId, params *chatpb.JoinChatRequest_InviteParameters) (*chatpb.JoinChatResponse, error) {
	inviteID := MustDeriveInviteID(params.InviteToken)
	log := s.inviteLog(userID, c.ID, inviteID)

	result, err := s.chats.JoinWithInvite(ctx, c.ID, userID, inviteID, params.SecretEnvelope, time.Now())
	switch {
	case errors.Is(err, ErrInviteNotFound):
		return &chatpb.JoinChatResponse{Result: chatpb.JoinChatResponse_INVITE_NOT_FOUND}, nil
	case errors.Is(err, ErrInviteExhausted):
		return &chatpb.JoinChatResponse{Result: chatpb.JoinChatResponse_INVITE_EXHAUSTED}, nil
	case errors.Is(err, ErrInviteExpired):
		return &chatpb.JoinChatResponse{Result: chatpb.JoinChatResponse_INVITE_EXPIRED}, nil
	case errors.Is(err, ErrInviteRevoked):
		return &chatpb.JoinChatResponse{Result: chatpb.JoinChatResponse_INVITE_REVOKED}, nil
	case errors.Is(err, ErrBanned):
		return &chatpb.JoinChatResponse{Result: chatpb.JoinChatResponse_BANNED}, nil
	case errors.Is(err, ErrChatNotFound):
		return &chatpb.JoinChatResponse{Result: chatpb.JoinChatResponse_NOT_FOUND}, nil
	case err != nil:
		log.With(zap.Error(err)).Warn("Failure joining with invite")
		return nil, status.Error(codes.Internal, "")
	}

	if !result.Changed {
		// A member by now: another of their devices joined between the
		// membership check and the write, and its call announced it.
		md, err := s.joinedMetadata(ctx, log, c, userID, result.Roster)
		if err != nil {
			return nil, err
		}
		return &chatpb.JoinChatResponse{Result: chatpb.JoinChatResponse_OK, Chat: md}, nil
	}

	// The creator is told the lobby shrank before the reads announceJoin
	// makes: unlike an admission, they are not the caller, so were a read to
	// fail they would never learn it, the joiner's retry being the no-op.
	if result.LeftLobby {
		s.publishLobbyLeft(c, userID)
	}
	md, err := s.announceJoin(ctx, log, c, userID, InvitedBy(result.Invite.CreatedBy), result.Roster)
	if err != nil {
		return nil, err
	}
	return &chatpb.JoinChatResponse{Result: chatpb.JoinChatResponse_OK, Chat: md}, nil
}

// announceJoin announces userID's real join of c, by method, at the roster
// the join's write returned: the UserJoined message (see sendUserJoined),
// then the joiner's metadata (see joinedMetadata), then the roster update
// (see publishRosterUpdate), and returns that metadata. Every way of joining
// a group announces through it, JoinChat's (with or without an invite) and
// AdmitLobbyMember's; what else a way announces (a lobby update, a push) is
// its own.
//
// The joiner's own devices learn of the join with their member entry, as
// hydrate built it, plus the join time and version of the record the join
// wrote (see announcedMember), which hydrate does not read, and the full
// metadata, so they can insert the chat without a refetch. The rest of the
// chat is told only that the roster moved.
//
// A failed read is an Internal error with nothing published to the roster:
// the join has landed, and a retry is the no-op path, which announces
// nothing, so a client that sees the error reads the chat on its next open.
func (s *Server) announceJoin(ctx context.Context, log *zap.Logger, c *Chat, userID *commonpb.UserId, method JoinMethod, roster RosterSummary) (*chatpb.Metadata, error) {
	s.sendUserJoined(ctx, log, c, userID, method, roster)

	md, err := s.joinedMetadata(ctx, log, c, userID, roster)
	if err != nil {
		return nil, err
	}
	member, err := s.announcedMember(ctx, log, c.ID, userID, md.Members[0].UserProfile, roster)
	if err != nil {
		return nil, err
	}
	toJoiner := &chatpb.RosterUpdate{
		Kind: &chatpb.RosterUpdate_MemberJoined_{MemberJoined: &chatpb.RosterUpdate_MemberJoined{
			Member:   member,
			Metadata: md,
		}},
		RosterSummary: md.RosterSummary,
	}
	s.publishRosterUpdate(c.ID, userID, toJoiner, true)
	return md, nil
}

// joinedMetadata is c as its member userID sees it right after a join, or a
// no-op one: hydrated in full, with the roster summary the join's write
// returned. That write read the roster strongly consistent, as it stands
// after the join; hydrate's batch read is not, and may lag it by the very
// transition the call made. The response and the update it announces carry
// the same summary, so a client sees one version, not two. A failed hydrate
// is an Internal error (see announceJoin).
func (s *Server) joinedMetadata(ctx context.Context, log *zap.Logger, c *Chat, userID *commonpb.UserId, roster RosterSummary) (*chatpb.Metadata, error) {
	metadata, err := s.hydrator.hydrate(ctx, userID, memberListenerStanding, ReadingFull, fullDetail, []*Chat{c})
	if err != nil {
		log.With(zap.Error(err)).Warn("Failure hydrating chat metadata")
		return nil, status.Error(codes.Internal, "")
	}
	md := metadata[0]
	md.RosterSummary = roster.ToProto()
	return md, nil
}

func (s *Server) LeaveChat(ctx context.Context, req *chatpb.LeaveChatRequest) (*chatpb.LeaveChatResponse, error) {
	userID, err := s.authz.Authorize(ctx, req, &req.Auth)
	if err != nil {
		return nil, err
	}

	log := s.log.With(zap.String("user_id", model.UserIDString(userID)))

	// The ID's length is the type check: a group ID names a group or nothing,
	// so there is no canonical record to read here. Unlike a join, a departure
	// has no rules to evaluate and no metadata to return.
	if !IsGroupChatID(req.ChatId) {
		return &chatpb.LeaveChatResponse{Result: chatpb.LeaveChatResponse_DENIED}, nil
	}

	// A departure from a private group takes the caller's key envelope with
	// it, as the proto promises: a user who returns enters the lobby and is
	// given a new one. The creator's is kept, since no one else could give
	// them another, and it is what marks the group as having a key (see
	// KeyEnvelope). The envelope is removed in the write that records the
	// departure, so the two cannot disagree, which means deciding it before
	// that write. Whether the group is private, and who created it, come from
	// the rules read: it is the one a store caches, so a leave pays nothing
	// for it in steady state.
	rules, err := s.chats.GetGroupRules(ctx, req.ChatId)
	switch {
	case errors.Is(err, ErrChatNotFound):
		return &chatpb.LeaveChatResponse{Result: chatpb.LeaveChatResponse_NOT_FOUND}, nil
	case err != nil:
		log.With(zap.Error(err)).Warn("Failure getting chat rules")
		return nil, status.Error(codes.Internal, "")
	}
	discardKeyEnvelope := rules.IsPrivate && !rules.isCreator(userID)

	// Leaving a group the caller is not in — never joined, or already gone —
	// is a no-op that already holds: the caller asked not to be a member, and
	// they are not. The store answers it without a transition.
	changed, roster, err := s.chats.RemoveGroupMember(ctx, req.ChatId, userID, discardKeyEnvelope)
	switch {
	case errors.Is(err, ErrChatNotFound):
		return &chatpb.LeaveChatResponse{Result: chatpb.LeaveChatResponse_NOT_FOUND}, nil
	case err != nil:
		log.With(zap.Error(err)).Warn("Failure removing group member")
		return nil, status.Error(codes.Internal, "")
	}

	if changed {
		update := &chatpb.RosterUpdate{
			Kind: &chatpb.RosterUpdate_MemberLeft_{MemberLeft: &chatpb.RosterUpdate_MemberLeft{
				UserId: userID,
			}},
			RosterSummary: roster.ToProto(),
		}
		s.publishRosterUpdate(req.ChatId, userID, update, true)
	}

	// A departure clears the caller's mute (see clearMuteOnLeave). It runs
	// whether or not the roster moved: a repeated leave is the retry that
	// repairs a clear the first one failed.
	s.clearMuteOnLeave(ctx, log, req.ChatId, userID)

	return &chatpb.LeaveChatResponse{Result: chatpb.LeaveChatResponse_OK}, nil
}

// announcedMember is the member entry a join announces to the joiner's own
// devices (see RosterUpdate.MemberJoined): the joiner's profile as hydrated,
// and the join time and version stamp of the record the join just wrote, read
// back strongly consistent as one point read — the one carrier of a member's
// record besides the roster page, and the one the client merges pages
// against. A record that is not joined after all — a departure from another
// of the user's devices landing between the join and this read — is announced
// at the roster version the join produced, with no join time; the departure's
// own update follows and supersedes it. A failed read is an Internal error:
// the join has landed, and a retry is the idempotent path, which announces
// nothing — the same standing as a failed hydrate.
func (s *Server) announcedMember(ctx context.Context, log *zap.Logger, chatID *commonpb.ChatId, userID *commonpb.UserId, profile *profilepb.UserProfile, roster RosterSummary) (*chatpb.Member, error) {
	records, err := s.chats.GetGroupMemberRecords(ctx, userID, []*commonpb.ChatId{chatID})
	if err != nil {
		log.With(zap.Error(err)).Warn("Failure reading membership record")
		return nil, status.Error(codes.Internal, "")
	}
	member := &chatpb.Member{
		UserId:      userID,
		UserProfile: profile,
		Version:     roster.Version,
	}
	if record, ok := records[string(chatID.Value)]; ok {
		member.JoinedAt = timestamppb.New(record.JoinedAt)
		member.Version = record.Version
	}
	return member, nil
}

// clearMuteOnLeave clears the caller's mute on a chat they have just left, as
// the proto promises: a mute is a member's setting on a chat they are in, and
// a user who returns later starts unmuted. It is best effort, after the
// departure has landed: the roster is the record of the leave, and a mute
// left behind costs the returning member one unmute (they see it on the
// chat's viewer_state once they rejoin; a non-member is shown no state),
// while failing the RPC would tell them they had not left. A failure
// is logged and the response is unchanged. A clear that moves the state is
// published to the caller's other devices exactly as UnmuteChat publishes
// one; a departed member has no mute to clear in the common case, and that
// no-op costs one read and publishes nothing. The state is published with no
// permissions: the caller has just left, and a non-member may do nothing.
func (s *Server) clearMuteOnLeave(ctx context.Context, log *zap.Logger, chatID *commonpb.ChatId, userID *commonpb.UserId) {
	state, changed, err := s.chats.ClearMute(ctx, chatID, userID)
	if err != nil {
		log.With(zap.Error(err), zap.String("chat_id", model.ChatIDString(chatID))).Warn("Failure clearing mute on leave")
		return
	}
	if changed {
		s.publishViewerStateChanged(userID, chatID, state, Permissions{})
	}
}

// sendUserJoined writes the UserJoined system message for userID's join of c
// (see SystemMessenger), joined by method, at the roster version the join
// produced. Only a private group gets one today: a public group may be very
// large and anyone may join it, so its log would fill with joins.
//
// It is called once per real join, after the write and before anything else
// is read, so a failed read-back cannot lose it. It is best effort: the join
// has landed, so a failure is logged and never fails the RPC, and since a
// retry of the RPC is the no-op path, a message lost here stays lost.
func (s *Server) sendUserJoined(ctx context.Context, log *zap.Logger, c *Chat, userID *commonpb.UserId, method JoinMethod, roster RosterSummary) {
	if !c.IsPrivate || s.systemMessages == nil {
		return
	}

	// The join has landed, so the caller going away must not abort the
	// message; the timeout bounds the write, and the send detaches its own
	// side effects.
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), userJoinedMessageTimeout)
	defer cancel()
	if err := s.systemMessages.SendUserJoined(ctx, c.ID, userID, method, roster.Version); err != nil {
		log.With(zap.Error(err)).Warn("Failure sending user joined message")
	}
}

// publishRosterUpdate broadcasts one roster transition on chatID whose subject
// is the user who joined or left: toSubject to the subject's own devices and,
// when tellMembers is set, a MembershipChanged carrying the same roster
// summary to every other member. It is best-effort and non-blocking — the bus
// hands events to its handlers on their own goroutines — and never fails the
// RPC whose transition it announces.
//
// The other members are never told who joined or left: a membership is not
// announced to the chat, so the update they get names no one and only keeps
// their roster summary current (member_count and version), with no gap in
// the versions they hold. The subject is always told, since their own
// devices act on it.
//
// The two audiences are reached over different topics because a stream's
// group subscriptions follow its user's membership, and it is the subject's
// own copy that moves them (see event.Server.followMembership): a joiner's
// open streams are not on the chat's topic until their MemberJoined arrives
// on their user topic, and a leaver's come off it when their MemberLeft does.
// So the chat topic carries the members' update with the subject excluded —
// which also keeps a leaver whose stream is still on the topic from hearing
// it twice — and the subject's user topic carries toSubject, reaching every
// device they have open whatever the topic knows. Both carry the same roster
// summary, so either audience converges on the same version.
//
// tellMembers is false when there is no one else to tell — a group's
// creation, where the subject is its only member — and the chat topic is
// left silent.
func (s *Server) publishRosterUpdate(chatID *commonpb.ChatId, subject *commonpb.UserId, toSubject *chatpb.RosterUpdate, tellMembers bool) {
	newEvent := func(update *chatpb.RosterUpdate) *eventpb.Event {
		return &eventpb.Event{
			Id: model.MustGenerateEventID(),
			Ts: timestamppb.Now(),
			Type: &eventpb.Event_ChatUpdate{ChatUpdate: &eventpb.ChatUpdate{
				Chat:          chatID,
				RosterUpdates: &chatpb.RosterUpdateBatch{RosterUpdates: []*chatpb.RosterUpdate{update}},
			}},
		}
	}

	if tellMembers {
		toMembers := &chatpb.RosterUpdate{
			Kind:          &chatpb.RosterUpdate_MembershipChanged_{MembershipChanged: &chatpb.RosterUpdate_MembershipChanged{}},
			RosterSummary: toSubject.RosterSummary,
		}
		s.chatEventBus.OnEvent(chatID, &eventpb.ChatEvent{
			ChatId:         chatID,
			Event:          newEvent(toMembers),
			ExcludeUserIds: []*commonpb.UserId{subject},
		})
	}
	s.userEventBus.OnEvent(subject, newEvent(toSubject))
}
