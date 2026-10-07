package chat

import (
	"context"
	"errors"

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
// Access), and so does a no-op join. Leaving has no rule to satisfy.
//
// A private group (see Chat.IsPrivate) is not joined here: its members are
// admitted by its creator from its lobby (see lobby.go), so JoinChat refuses
// everyone as DENIED. Everyone but the creator, that is, who needs no one's
// approval and rejoins a private group they left. No rule is evaluated for
// them: a private group is not governed by rules and has no RuleSet to
// evaluate (see RuleSet), and being its creator is what admits them.
//
// A public group without listener rules is open: the empty set is satisfied
// by everyone, so anyone may join it, as anyone may read it (see Access).
//
// Each transition that actually happens is broadcast as a RosterUpdate to the
// chat's members and to the affected user (see publishRosterUpdate): the user
// learns what happened to them, and the other members only that the roster
// moved, with no one named. A join or departure that is a no-op broadcasts
// nothing: the roster did not move, and a client applying updates by version
// would drop it anyway.

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
	if c.IsPrivate && !c.IsCreator(userID) {
		return &chatpb.JoinChatResponse{Result: chatpb.JoinChatResponse_DENIED}, nil
	}

	// A current member re-joining is a no-op answered on membership alone,
	// before the rules: a member whose balance has since dipped under the
	// group's requirement is still a member, and a retry after a lost response
	// must say so rather than refuse them.
	isMember, err := s.chats.IsMember(ctx, req.ChatId, userID)
	if err != nil {
		log.With(zap.Error(err)).Warn("Failure checking chat membership")
		return nil, status.Error(codes.Internal, "")
	}

	// The rules come from the canonical record already in hand rather than a
	// second read through the evaluator: the record is what they are projected
	// from. A private group has none to evaluate, and its caller is its
	// creator by now.
	if ruleSet, ruled := c.GroupRules().RuleSet(); !isMember && ruled {
		canListen, err := s.rules.CanListenWithRules(ctx, ruleSet, userID)
		if err != nil {
			log.With(zap.Error(err)).Warn("Failure evaluating chat rules")
			return nil, status.Error(codes.Internal, "")
		}
		if !canListen {
			return &chatpb.JoinChatResponse{Result: chatpb.JoinChatResponse_RULES_NOT_SATISFIED}, nil
		}
	}

	// Idempotent against a concurrent join from another of the user's devices:
	// both may pass the membership check above, but only the one whose write
	// is the transition sees changed, and only it broadcasts.
	changed, roster, err := s.chats.AddGroupMembers(ctx, req.ChatId, []*commonpb.UserId{userID})
	if err != nil {
		log.With(zap.Error(err)).Warn("Failure adding group member")
		return nil, status.Error(codes.Internal, "")
	}

	metadata, err := s.hydrate(ctx, userID, memberListenerStanding, ReadingFull, fullDetail, []*Chat{c})
	if err != nil {
		// The join has landed; only the read back failed. A retry is the
		// idempotent path above and returns the metadata then.
		log.With(zap.Error(err)).Warn("Failure hydrating chat metadata")
		return nil, status.Error(codes.Internal, "")
	}
	md := metadata[0]

	// The write returned the roster as it stands after the join, read strongly
	// consistent; hydrate's batch read is not, and may lag it by the very
	// transition this call made. The response and the update it announces
	// carry the same summary, so a client sees one version, not two.
	rosterSummary := roster.ToProto()
	md.RosterSummary = rosterSummary

	if changed {
		// The joiner's own devices learn of the join with their member entry,
		// as hydrate built it, plus the join time and version of the record
		// this call wrote (see announcedMember), which hydrate does not read,
		// and the full metadata, so they can insert the chat without a
		// refetch. The rest of the chat is told only that the roster moved.
		member, err := s.announcedMember(ctx, log, req.ChatId, userID, md.Members[0].UserProfile, roster)
		if err != nil {
			return nil, err
		}
		toJoiner := &chatpb.RosterUpdate{
			Kind: &chatpb.RosterUpdate_MemberJoined_{MemberJoined: &chatpb.RosterUpdate_MemberJoined{
				Member:   member,
				Metadata: md,
			}},
			RosterSummary: rosterSummary,
		}
		s.publishRosterUpdate(req.ChatId, userID, toJoiner, true)
	}

	return &chatpb.JoinChatResponse{
		Result: chatpb.JoinChatResponse_OK,
		Chat:   md,
	}, nil
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
