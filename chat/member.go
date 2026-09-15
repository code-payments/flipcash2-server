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
// current member re-joining — reads gate on membership alone, and so does a
// no-op join. Leaving has no rule to satisfy.
//
// Each transition that actually happens is broadcast as a RosterUpdate to the
// chat's members and to the affected user (see publishRosterUpdate). A join or
// departure that is a no-op broadcasts nothing: the roster did not move, and a
// client applying updates by version would drop it anyway.
//
// Both RPCs can be gated to staff users (see requireStaff), which is how they
// are held back from the wider user base until they are ready for it. The gate
// is applied before the chat is looked up, so a non-staff caller learns
// nothing — not even whether the chat exists.

func (s *Server) JoinChat(ctx context.Context, req *chatpb.JoinChatRequest) (*chatpb.JoinChatResponse, error) {
	userID, err := s.authz.Authorize(ctx, req, &req.Auth)
	if err != nil {
		return nil, err
	}

	log := s.log.With(zap.String("user_id", model.UserIDString(userID)))

	allowed, err := s.requireStaffForGroupManagementRPC(ctx, log, userID)
	if err != nil {
		return nil, err
	}
	if !allowed {
		return &chatpb.JoinChatResponse{Result: chatpb.JoinChatResponse_DENIED}, nil
	}

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
	// before the rules: a member whose balance has since dipped under the
	// group's requirement is still a member, and a retry after a lost response
	// must say so rather than refuse them.
	isMember, err := s.chats.IsMember(ctx, req.ChatId, userID)
	if err != nil {
		log.With(zap.Error(err)).Warn("Failure checking chat membership")
		return nil, status.Error(codes.Internal, "")
	}

	if !isMember {
		canListen, err := s.rules.CanListen(ctx, req.ChatId, userID)
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

	metadata, err := s.hydrate(ctx, userID, []*Chat{c})
	if err != nil {
		// The join has landed; only the read back failed. A retry is the
		// idempotent path above and returns the metadata then.
		log.With(zap.Error(err)).Warn("Failure hydrating chat metadata")
		return nil, status.Error(codes.Internal, "")
	}
	md := metadata[0]

	if changed {
		// The joiner's own member entry, as hydrate built it, is what the rest of
		// the chat learns about them — less their pointers, which are never
		// surfaced to other members (see hydrate).
		member := &chatpb.Member{
			UserId:      userID,
			UserProfile: md.Members[0].UserProfile,
		}
		rosterSummary := roster.ToProto()
		toMembers := &chatpb.RosterUpdate{
			Kind:          &chatpb.RosterUpdate_MemberJoined_{MemberJoined: &chatpb.RosterUpdate_MemberJoined{Member: member}},
			RosterSummary: rosterSummary,
		}
		// The joiner's other devices get the full metadata too, so they can
		// insert the chat without a refetch.
		toJoiner := &chatpb.RosterUpdate{
			Kind: &chatpb.RosterUpdate_MemberJoined_{MemberJoined: &chatpb.RosterUpdate_MemberJoined{
				Member:   member,
				Metadata: md,
			}},
			RosterSummary: rosterSummary,
		}
		s.publishRosterUpdate(req.ChatId, userID, toMembers, toJoiner)
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

	allowed, err := s.requireStaffForGroupManagementRPC(ctx, log, userID)
	if err != nil {
		return nil, err
	}
	if !allowed {
		return &chatpb.LeaveChatResponse{Result: chatpb.LeaveChatResponse_DENIED}, nil
	}

	if !IsGroupChatID(req.ChatId) {
		return &chatpb.LeaveChatResponse{Result: chatpb.LeaveChatResponse_DENIED}, nil
	}

	c, err := s.chats.GetChatByID(ctx, req.ChatId)
	switch {
	case errors.Is(err, ErrChatNotFound):
		return &chatpb.LeaveChatResponse{Result: chatpb.LeaveChatResponse_NOT_FOUND}, nil
	case err != nil:
		log.With(zap.Error(err)).Warn("Failure getting chat")
		return nil, status.Error(codes.Internal, "")
	}
	if c.Type != chatpb.ChatType_GROUP {
		return &chatpb.LeaveChatResponse{Result: chatpb.LeaveChatResponse_DENIED}, nil
	}

	// Leaving a group the caller is not in — never joined, or already gone —
	// is a no-op that already holds: the caller asked not to be a member, and
	// they are not. The store answers it without a transition.
	changed, roster, err := s.chats.RemoveGroupMember(ctx, req.ChatId, userID)
	if err != nil {
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
		s.publishRosterUpdate(req.ChatId, userID, update, update)
	}

	return &chatpb.LeaveChatResponse{Result: chatpb.LeaveChatResponse_OK}, nil
}

// requireStaff reports whether userID passes the staff gate on the membership
// RPCs: everyone does when the server is not configured to require staff, and
// only staff users otherwise. A staff flag that cannot be read is a gRPC
// Internal error, never a pass: the gate is a restriction, and a restriction
// the server cannot evaluate admits no one.
func (s *Server) requireStaffForGroupManagementRPC(ctx context.Context, log *zap.Logger, userID *commonpb.UserId) (bool, error) {
	if !s.requireStaffForGroupManagement {
		return true, nil
	}
	isStaff, err := s.accounts.IsStaff(ctx, userID)
	if err != nil {
		log.With(zap.Error(err)).Warn("Failure getting user staff status")
		return false, status.Error(codes.Internal, "")
	}
	return isStaff, nil
}

// publishRosterUpdate broadcasts one roster transition on chatID whose subject
// is the user who joined or left: toMembers to every other member, and
// toSubject to the subject's own devices. It is best-effort and non-blocking —
// the bus hands events to its handlers on their own goroutines — and never
// fails the RPC whose transition it announces.
//
// The two audiences are reached over different topics because a stream's
// group subscriptions are a snapshot as of its open (see event.Server): a
// joiner's open streams are not yet on the chat's topic, and a leaver's may or
// may not be, depending on whether they were a member when the stream opened.
// So the chat topic carries toMembers with the subject excluded — which also
// keeps a leaver whose stream is still on the topic from hearing it twice —
// and the subject's user topic carries toSubject, reaching every device they
// have open whatever the topic knows. The two updates may differ in payload
// (a join carries the chat's metadata to the joiner alone); they carry the
// same roster summary, so either audience converges on the same version.
//
// A nil toMembers means there is no one else to tell — a group's creation,
// where the subject is its only member — and the chat topic is left silent.
func (s *Server) publishRosterUpdate(chatID *commonpb.ChatId, subject *commonpb.UserId, toMembers, toSubject *chatpb.RosterUpdate) {
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

	if toMembers != nil {
		s.chatEventBus.OnEvent(chatID, &eventpb.ChatEvent{
			ChatId:         chatID,
			Event:          newEvent(toMembers),
			ExcludeUserIds: []*commonpb.UserId{subject},
		})
	}
	s.userEventBus.OnEvent(subject, newEvent(toSubject))
}
