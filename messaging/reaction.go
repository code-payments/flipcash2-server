package messaging

import (
	"bytes"
	"context"
	"errors"
	"time"

	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"
	eventpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/event/v1"
	messagingpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/messaging/v1"

	"github.com/code-payments/flipcash2-server/chat"
	"github.com/code-payments/flipcash2-server/database"
	"github.com/code-payments/flipcash2-server/model"
)

func (s *Server) AddReaction(ctx context.Context, req *messagingpb.AddReactionRequest) (*messagingpb.AddReactionResponse, error) {
	userID, err := s.authz.Authorize(ctx, req, &req.Auth)
	if err != nil {
		return nil, err
	}

	log := s.log.With(zap.String("user_id", model.UserIDString(userID)))

	if err := ValidateEmoji(req.Emoji.GetValue()); err != nil {
		return &messagingpb.AddReactionResponse{Result: messagingpb.AddReactionResponse_DENIED}, nil
	}

	if member, err := s.isMember(ctx, log, req.ChatId, userID); err != nil {
		return nil, err
	} else if !member {
		return &messagingpb.AddReactionResponse{Result: messagingpb.AddReactionResponse_DENIED}, nil
	}

	// The target must exist in this chat and be reactable. Checked after
	// membership so non-members can't probe which message IDs exist.
	msg, err := s.messages.GetMessage(ctx, req.ChatId, req.MessageId)
	switch {
	case errors.Is(err, ErrMessageNotFound):
		return &messagingpb.AddReactionResponse{Result: messagingpb.AddReactionResponse_MESSAGE_NOT_FOUND}, nil
	case err != nil:
		log.With(zap.Error(err)).Warn("Failure getting message to react to")
		return nil, status.Error(codes.Internal, "")
	}
	if !msg.IsReactable() {
		return &messagingpb.AddReactionResponse{Result: messagingpb.AddReactionResponse_CANNOT_REACT}, nil
	}

	now := time.Now().UTC()
	reaction, created, tooManyTypes, err := s.messages.AddReaction(ctx, req.ChatId, req.MessageId, userID, req.Emoji.Value, now)
	if err != nil {
		log.With(zap.Error(err)).Warn("Failure adding reaction")
		return nil, status.Error(codes.Internal, "")
	}
	if tooManyTypes {
		return &messagingpb.AddReactionResponse{Result: messagingpb.AddReactionResponse_TOO_MANY_REACTION_TYPES}, nil
	}

	reaction.ReactedBySelf = true

	if created {
		publishChatUpdate(ctx, log, s.sender.badges, s.sender.chats, s.sender.profiles, s.sender.blocklists, s.sender.ocpData, s.sender.pusher, s.sender.eventBus, req.ChatId, &eventpb.ChatUpdate{
			ReactionUpdates: &messagingpb.ReactionUpdateBatch{
				ReactionUpdates: []*messagingpb.ReactionUpdate{
					{
						MessageId: req.MessageId,
						Emoji:     req.Emoji,
						Actor:     userID,
						Action:    messagingpb.ReactionUpdate_ADDED,
						Count:     reaction.Count,
						Sequence:  reaction.Sequence,
						ReactedTs: timestamppb.New(now),
					},
				},
			},
		}, nil, nil)
	}

	return &messagingpb.AddReactionResponse{
		Result:   messagingpb.AddReactionResponse_OK,
		Reaction: reaction.ToProto(),
	}, nil
}

func (s *Server) RemoveReaction(ctx context.Context, req *messagingpb.RemoveReactionRequest) (*messagingpb.RemoveReactionResponse, error) {
	userID, err := s.authz.Authorize(ctx, req, &req.Auth)
	if err != nil {
		return nil, err
	}

	log := s.log.With(zap.String("user_id", model.UserIDString(userID)))

	if err := ValidateEmoji(req.Emoji.GetValue()); err != nil {
		return &messagingpb.RemoveReactionResponse{Result: messagingpb.RemoveReactionResponse_DENIED}, nil
	}

	if member, err := s.isMember(ctx, log, req.ChatId, userID); err != nil {
		return nil, err
	} else if !member {
		return &messagingpb.RemoveReactionResponse{Result: messagingpb.RemoveReactionResponse_DENIED}, nil
	}

	if exists, err := s.messageExists(ctx, log, req.ChatId, req.MessageId); err != nil {
		return nil, err
	} else if !exists {
		return &messagingpb.RemoveReactionResponse{Result: messagingpb.RemoveReactionResponse_MESSAGE_NOT_FOUND}, nil
	}

	reaction, removed, err := s.messages.RemoveReaction(ctx, req.ChatId, req.MessageId, userID, req.Emoji.Value)
	if err != nil {
		log.With(zap.Error(err)).Warn("Failure removing reaction")
		return nil, status.Error(codes.Internal, "")
	}

	if removed {
		publishChatUpdate(ctx, log, s.sender.badges, s.sender.chats, s.sender.profiles, s.sender.blocklists, s.sender.ocpData, s.sender.pusher, s.sender.eventBus, req.ChatId, &eventpb.ChatUpdate{
			ReactionUpdates: &messagingpb.ReactionUpdateBatch{
				ReactionUpdates: []*messagingpb.ReactionUpdate{
					{
						MessageId: req.MessageId,
						Emoji:     req.Emoji,
						Actor:     userID,
						Action:    messagingpb.ReactionUpdate_REMOVED,
						Count:     reaction.Count,
						Sequence:  reaction.Sequence,
					},
				},
			},
		}, nil, nil)
	}

	resp := &messagingpb.RemoveReactionResponse{Result: messagingpb.RemoveReactionResponse_OK}
	// The aggregate is surfaced even at Count 0 (the last reactor left); it is nil
	// only on a pure no-op remove of an emoji with no aggregate at all.
	if reaction != nil {
		resp.Reaction = reaction.ToProto()
	}
	return resp, nil
}

func (s *Server) GetReactionSummary(ctx context.Context, req *messagingpb.GetReactionSummaryRequest) (*messagingpb.GetReactionSummaryResponse, error) {
	userID, err := s.authz.Authorize(ctx, req, &req.Auth)
	if err != nil {
		return nil, err
	}

	log := s.log.With(zap.String("user_id", model.UserIDString(userID)))

	if member, err := s.isMember(ctx, log, req.ChatId, userID); err != nil {
		return nil, err
	} else if !member {
		return &messagingpb.GetReactionSummaryResponse{Result: messagingpb.GetReactionSummaryResponse_DENIED}, nil
	}

	if exists, err := s.messageExists(ctx, log, req.ChatId, req.MessageId); err != nil {
		return nil, err
	} else if !exists {
		return &messagingpb.GetReactionSummaryResponse{Result: messagingpb.GetReactionSummaryResponse_MESSAGE_NOT_FOUND}, nil
	}

	reactions, err := s.messages.GetReactionSummary(ctx, req.ChatId, req.MessageId)
	if err != nil {
		log.With(zap.Error(err)).Warn("Failure getting reaction summary")
		return nil, status.Error(codes.Internal, "")
	}

	summary := &ReactionSummary{MessageID: req.MessageId, Reactions: reactions}
	if err := s.applySelfReactions(ctx, req.ChatId, userID, []*ReactionSummary{summary}); err != nil {
		log.With(zap.Error(err)).Warn("Failure resolving self reactions")
		return nil, status.Error(codes.Internal, "")
	}

	return &messagingpb.GetReactionSummaryResponse{
		Result:  messagingpb.GetReactionSummaryResponse_OK,
		Summary: summary.ToProto(),
	}, nil
}

func (s *Server) GetReactionSummaries(ctx context.Context, req *messagingpb.GetReactionSummariesRequest) (*messagingpb.GetReactionSummariesResponse, error) {
	userID, err := s.authz.Authorize(ctx, req, &req.Auth)
	if err != nil {
		return nil, err
	}

	log := s.log.With(zap.String("user_id", model.UserIDString(userID)))

	if member, err := s.isMember(ctx, log, req.ChatId, userID); err != nil {
		return nil, err
	} else if !member {
		return &messagingpb.GetReactionSummariesResponse{Result: messagingpb.GetReactionSummariesResponse_DENIED}, nil
	}

	var summaries []*ReactionSummary
	if batch := req.GetMessageIds(); batch != nil {
		summaries, err = s.messages.GetReactionSummariesByRefs(ctx, req.ChatId, batch.MessageIds)
	} else {
		opts := database.FromProtoQueryOptions(req.GetOptions())
		summaries, err = s.messages.GetReactionSummaries(ctx, req.ChatId, opts...)
	}
	if err != nil {
		log.With(zap.Error(err)).Warn("Failure getting reaction summaries")
		return nil, status.Error(codes.Internal, "")
	}

	if err := s.applySelfReactions(ctx, req.ChatId, userID, summaries); err != nil {
		log.With(zap.Error(err)).Warn("Failure resolving self reactions")
		return nil, status.Error(codes.Internal, "")
	}

	protos := make([]*messagingpb.ReactionSummary, len(summaries))
	for i, summary := range summaries {
		protos[i] = summary.ToProto()
	}
	return &messagingpb.GetReactionSummariesResponse{
		Result:    messagingpb.GetReactionSummariesResponse_OK,
		Summaries: protos,
	}, nil
}

func (s *Server) GetReactors(ctx context.Context, req *messagingpb.GetReactorsRequest) (*messagingpb.GetReactorsResponse, error) {
	userID, err := s.authz.Authorize(ctx, req, &req.Auth)
	if err != nil {
		return nil, err
	}

	log := s.log.With(zap.String("user_id", model.UserIDString(userID)))

	if err := ValidateEmoji(req.Emoji.GetValue()); err != nil {
		return &messagingpb.GetReactorsResponse{Result: messagingpb.GetReactorsResponse_DENIED}, nil
	}

	if member, err := s.isMember(ctx, log, req.ChatId, userID); err != nil {
		return nil, err
	} else if !member {
		return &messagingpb.GetReactorsResponse{Result: messagingpb.GetReactorsResponse_DENIED}, nil
	}

	if exists, err := s.messageExists(ctx, log, req.ChatId, req.MessageId); err != nil {
		return nil, err
	} else if !exists {
		return &messagingpb.GetReactorsResponse{Result: messagingpb.GetReactorsResponse_MESSAGE_NOT_FOUND}, nil
	}

	// A DM's reactor set is at most two, and the drill-down is often opened right
	// after reacting, so read those consistently rather than off the eventually
	// consistent index: the cost is trivial and the reader is guaranteed to see the
	// reaction they just added.
	//
	// A group's reactor set is unbounded, and the consistent read has no way to
	// order by recency without reading the whole set — the base table keys reactors
	// by user, not by reaction time — so it pays O(reactors) on every page. Groups
	// page the recency index instead, which reads only the page it returns. Both
	// paths return the same order and honor the same cursor, so this changes
	// consistency alone; a group member who just reacted may briefly not see
	// themselves at the top, which the AddReaction response already told them.
	consistent := !chat.IsGroupChatID(req.ChatId)
	opts := database.FromProtoQueryOptions(req.GetOptions())
	reactors, hasMore, err := s.messages.GetReactors(ctx, req.ChatId, req.MessageId, req.Emoji.Value, consistent, opts...)
	if err != nil {
		log.With(zap.Error(err)).Warn("Failure getting reactors")
		return nil, status.Error(codes.Internal, "")
	}

	protos := make([]*messagingpb.Reactor, len(reactors))
	for i, r := range reactors {
		protos[i] = r.ToProto()
	}
	resp := &messagingpb.GetReactorsResponse{
		Result:   messagingpb.GetReactorsResponse_OK,
		Reactors: protos,
		HasMore:  hasMore,
	}
	// The client echoes the cursor back in options.paging_token for the next page.
	if hasMore && len(reactors) > 0 {
		resp.PagingToken = ReactorPageToken(reactors[len(reactors)-1])
	}
	return resp, nil
}

// applySelfReactions sets ReactedBySelf on the given summaries' aggregates for
// the viewer, choosing its strategy by chat type.
//
// A DM answers from the sample already in hand: it has at most two members, so an
// emoji's reactor set can never outgrow the surfaced sample (MaxSampleReactors)
// and the sample lists every reactor. Membership in it is an exact answer that
// costs no store read at all.
//
// A group's reactor set is unbounded, so the sample is only its most-recent
// window: a viewer who reacted early drops out of it once enough others pile on,
// and scanning it would silently report a reaction of their own as absent. Groups
// therefore resolve the question against the authoritative per-reactor records,
// addressed by exact key and batched across every (message, emoji) in the page —
// one round trip for the whole read, not one per aggregate.
func (s *Server) applySelfReactions(ctx context.Context, chatID *commonpb.ChatId, userID *commonpb.UserId, summaries []*ReactionSummary) error {
	if !chat.IsGroupChatID(chatID) {
		overlaySelfReactions(userID, summaries)
		return nil
	}

	// Every aggregate in the page is a candidate; a summary carrying none (a
	// message with no reactions) contributes nothing to ask about.
	var refs []ReactionRef
	for _, summary := range summaries {
		for _, reaction := range summary.Reactions {
			refs = append(refs, ReactionRef{MessageID: summary.MessageID, Emoji: reaction.Emoji})
		}
	}
	if len(refs) == 0 {
		return nil
	}

	present, err := s.messages.GetSelfReactions(ctx, chatID, userID, refs)
	if err != nil {
		return err
	}
	if len(present) == 0 {
		return nil
	}

	reacted := make(map[selfReactionKey]struct{}, len(present))
	for _, ref := range present {
		reacted[selfReactionKey{messageID: ref.MessageID.Value, emoji: ref.Emoji}] = struct{}{}
	}
	for _, summary := range summaries {
		for _, reaction := range summary.Reactions {
			key := selfReactionKey{messageID: summary.MessageID.Value, emoji: reaction.Emoji}
			if _, ok := reacted[key]; ok {
				reaction.ReactedBySelf = true
			}
		}
	}
	return nil
}

// selfReactionKey identifies one (message, emoji) aggregate within a chat, for
// matching the store's answer back onto the summaries it was derived from. The
// message ID is a per-chat sequence number, so the pair is unambiguous without
// encoding either field.
type selfReactionKey struct {
	messageID uint64
	emoji     string
}

// overlaySelfReactions sets ReactedBySelf on the given summaries' aggregates for
// userID by scanning each aggregate's surfaced sample. It is exact only where the
// sample is guaranteed to hold every reactor — DMs — and applySelfReactions is
// what enforces that; see there for why a group must not use it.
func overlaySelfReactions(userID *commonpb.UserId, summaries []*ReactionSummary) {
	for _, summary := range summaries {
		for _, reaction := range summary.Reactions {
			for _, reactor := range reaction.SampleReactors {
				if bytes.Equal(reactor.UserID.Value, userID.Value) {
					reaction.ReactedBySelf = true
					break
				}
			}
		}
	}
}
