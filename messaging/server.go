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

	"github.com/code-payments/flipcash2-server/auth"
	"github.com/code-payments/flipcash2-server/chat"
	"github.com/code-payments/flipcash2-server/database"
	"github.com/code-payments/flipcash2-server/model"
)

type Server struct {
	log *zap.Logger

	authz auth.Authorizer

	chats    chat.Store
	messages Store

	sender *Sender

	messagingpb.UnimplementedMessagingServer
}

func NewServer(
	log *zap.Logger,
	authz auth.Authorizer,
	chats chat.Store,
	messages Store,
	sender *Sender,
) *Server {
	return &Server{
		log:      log,
		authz:    authz,
		chats:    chats,
		messages: messages,
		sender:   sender,
	}
}

func (s *Server) GetMessage(ctx context.Context, req *messagingpb.GetMessageRequest) (*messagingpb.GetMessageResponse, error) {
	userID, err := s.authz.Authorize(ctx, req, &req.Auth)
	if err != nil {
		return nil, err
	}

	log := s.log.With(zap.String("user_id", model.UserIDString(userID)))

	if member, err := s.isMember(ctx, log, req.ChatId, userID); err != nil {
		return nil, err
	} else if !member {
		return &messagingpb.GetMessageResponse{Result: messagingpb.GetMessageResponse_DENIED}, nil
	}

	msg, err := s.messages.GetMessage(ctx, req.ChatId, req.MessageId)
	switch {
	case errors.Is(err, ErrMessageNotFound):
		return &messagingpb.GetMessageResponse{Result: messagingpb.GetMessageResponse_NOT_FOUND}, nil
	case err != nil:
		log.With(zap.Error(err)).Warn("Failure getting message")
		return nil, status.Error(codes.Internal, "")
	}

	return &messagingpb.GetMessageResponse{
		Result:  messagingpb.GetMessageResponse_OK,
		Message: msg.ToProto(),
	}, nil
}

func (s *Server) GetMessages(ctx context.Context, req *messagingpb.GetMessagesRequest) (*messagingpb.GetMessagesResponse, error) {
	userID, err := s.authz.Authorize(ctx, req, &req.Auth)
	if err != nil {
		return nil, err
	}

	log := s.log.With(zap.String("user_id", model.UserIDString(userID)))

	if member, err := s.isMember(ctx, log, req.ChatId, userID); err != nil {
		return nil, err
	} else if !member {
		return &messagingpb.GetMessagesResponse{Result: messagingpb.GetMessagesResponse_DENIED}, nil
	}

	var msgs []*Message
	if batch := req.GetMessageIds(); batch != nil {
		refs := make([]MessageRef, len(batch.MessageIds))
		for i, id := range batch.MessageIds {
			refs[i] = MessageRef{ChatID: req.ChatId, MessageID: id}
		}
		msgs, err = s.messages.GetMessagesByRefs(ctx, refs)
	} else {
		opts := database.FromProtoQueryOptions(req.GetOptions())
		msgs, err = s.messages.GetMessages(ctx, req.ChatId, opts...)
	}
	if err != nil {
		log.With(zap.Error(err)).Warn("Failure getting messages")
		return nil, status.Error(codes.Internal, "")
	}

	if len(msgs) == 0 {
		return &messagingpb.GetMessagesResponse{Result: messagingpb.GetMessagesResponse_NOT_FOUND}, nil
	}
	protos := make([]*messagingpb.Message, len(msgs))
	for i, m := range msgs {
		protos[i] = m.ToProto()
	}
	return &messagingpb.GetMessagesResponse{
		Result:   messagingpb.GetMessagesResponse_OK,
		Messages: &messagingpb.MessageBatch{Messages: protos},
	}, nil
}

func (s *Server) SendMessage(ctx context.Context, req *messagingpb.SendMessageRequest) (*messagingpb.SendMessageResponse, error) {
	userID, err := s.authz.Authorize(ctx, req, &req.Auth)
	if err != nil {
		return nil, err
	}

	log := s.log.With(zap.String("user_id", model.UserIDString(userID)))

	var repliedMessageID *messagingpb.MessageId
	switch content := req.Content[0].Type.(type) {
	case *messagingpb.Content_Text:
	case *messagingpb.Content_Reply:
		switch content.Reply.Content[0].Type.(type) {
		case *messagingpb.Content_Text:
		default:
			return &messagingpb.SendMessageResponse{Result: messagingpb.SendMessageResponse_DENIED}, nil
		}
		repliedMessageID = content.Reply.RepliedMessageId
	default:
		return &messagingpb.SendMessageResponse{Result: messagingpb.SendMessageResponse_DENIED}, nil
	}

	if member, err := s.isMember(ctx, log, req.ChatId, userID); err != nil {
		return nil, err
	} else if !member {
		return &messagingpb.SendMessageResponse{Result: messagingpb.SendMessageResponse_DENIED}, nil
	}

	// The replied-to message must exist in this chat and be repliable. Checked
	// after membership so non-members can't probe which message IDs exist.
	if repliedMessageID != nil {
		repliedMessage, err := s.messages.GetMessage(ctx, req.ChatId, repliedMessageID)
		switch {
		case errors.Is(err, ErrMessageNotFound):
			return &messagingpb.SendMessageResponse{Result: messagingpb.SendMessageResponse_DENIED}, nil
		case err != nil:
			log.With(zap.Error(err)).Warn("Failure getting replied-to message")
			return nil, status.Error(codes.Internal, "")
		}
		if !repliedMessage.IsReplyable() {
			return &messagingpb.SendMessageResponse{Result: messagingpb.SendMessageResponse_DENIED}, nil
		}
	}

	msg, err := s.sender.Send(ctx, req.ChatId, userID, req.Content, req.ClientMessageId, true)
	if err != nil {
		return nil, err
	}

	return &messagingpb.SendMessageResponse{
		Result:  messagingpb.SendMessageResponse_OK,
		Message: msg.ToProto(),
	}, nil
}

func (s *Server) AdvancePointer(ctx context.Context, req *messagingpb.AdvancePointerRequest) (*messagingpb.AdvancePointerResponse, error) {
	userID, err := s.authz.Authorize(ctx, req, &req.Auth)
	if err != nil {
		return nil, err
	}

	log := s.log.With(zap.String("user_id", model.UserIDString(userID)))

	if member, err := s.isMember(ctx, log, req.ChatId, userID); err != nil {
		return nil, err
	} else if !member {
		return &messagingpb.AdvancePointerResponse{Result: messagingpb.AdvancePointerResponse_DENIED}, nil
	}

	if exists, err := s.messageExists(ctx, log, req.ChatId, req.NewValue); err != nil {
		return nil, err
	} else if !exists {
		return &messagingpb.AdvancePointerResponse{Result: messagingpb.AdvancePointerResponse_MESSAGE_NOT_FOUND}, nil
	}

	pointer, advanced, err := s.messages.AdvancePointer(ctx, req.ChatId, userID, req.PointerType, req.NewValue)
	if err != nil {
		log.With(zap.Error(err)).Warn("Failure advancing pointer")
		return nil, status.Error(codes.Internal, "")
	}

	if advanced {
		publishChatUpdate(ctx, log, s.sender.badges, s.sender.chats, s.sender.profiles, s.sender.ocpData, s.sender.pusher, s.sender.eventBus, req.ChatId, &eventpb.ChatUpdate{
			PointerUpdates: &messagingpb.PointerBatch{Pointers: []*messagingpb.Pointer{pointer}},
		}, nil, nil)
	}

	return &messagingpb.AdvancePointerResponse{Result: messagingpb.AdvancePointerResponse_OK}, nil
}

func (s *Server) NotifyIsTyping(ctx context.Context, req *messagingpb.NotifyIsTypingRequest) (*messagingpb.NotifyIsTypingResponse, error) {
	userID, err := s.authz.Authorize(ctx, req, &req.Auth)
	if err != nil {
		return nil, err
	}

	log := s.log.With(zap.String("user_id", model.UserIDString(userID)))

	if member, err := s.isMember(ctx, log, req.ChatId, userID); err != nil {
		return nil, err
	} else if !member {
		return &messagingpb.NotifyIsTypingResponse{Result: messagingpb.NotifyIsTypingResponse_DENIED}, nil
	}

	// Typing notifications are transient and only meaningful to other members.
	publishChatUpdate(ctx, log, s.sender.badges, s.sender.chats, s.sender.profiles, s.sender.ocpData, s.sender.pusher, s.sender.eventBus, req.ChatId, &eventpb.ChatUpdate{
		IsTypingNotifications: &messagingpb.IsTypingNotificationBatch{
			IsTypingNotifications: []*messagingpb.IsTypingNotification{{
				UserId: userID,
				State:  req.State,
			}},
		},
	}, userID, nil)

	return &messagingpb.NotifyIsTypingResponse{Result: messagingpb.NotifyIsTypingResponse_OK}, nil
}

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
		publishChatUpdate(ctx, log, s.sender.badges, s.sender.chats, s.sender.profiles, s.sender.ocpData, s.sender.pusher, s.sender.eventBus, req.ChatId, &eventpb.ChatUpdate{
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
		publishChatUpdate(ctx, log, s.sender.badges, s.sender.chats, s.sender.profiles, s.sender.ocpData, s.sender.pusher, s.sender.eventBus, req.ChatId, &eventpb.ChatUpdate{
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
	overlaySelfReactions(userID, []*ReactionSummary{summary})

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

	overlaySelfReactions(userID, summaries)

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

	// Reactor sets are small in DMs and the drill-down is often opened right after
	// reacting, so read consistently rather than off the eventually consistent index.
	//
	// todo: Revisit when we introduce groups
	opts := database.FromProtoQueryOptions(req.GetOptions())
	reactors, hasMore, err := s.messages.GetReactors(ctx, req.ChatId, req.MessageId, req.Emoji.Value, true, opts...)
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

func (s *Server) isMember(ctx context.Context, log *zap.Logger, chatID *commonpb.ChatId, userID *commonpb.UserId) (bool, error) {
	isMember, err := s.chats.IsMember(ctx, chatID, userID)
	if err != nil {
		log.With(zap.Error(err)).Warn("Failure checking chat membership")
		return false, status.Error(codes.Internal, "")
	}
	return isMember, nil
}

func (s *Server) messageExists(ctx context.Context, log *zap.Logger, chatID *commonpb.ChatId, messageID *messagingpb.MessageId) (bool, error) {
	exists, err := s.messages.MessageExists(ctx, chatID, messageID)
	if err != nil {
		log.With(zap.Error(err)).Warn("Failure checking message existence")
		return false, status.Error(codes.Internal, "")
	}
	return exists, nil
}

// overlaySelfReactions sets ReactedBySelf on the given summaries' aggregates for
// userID. Chats are DMs — at most two members — so every emoji's reactor set fits
// within the surfaced sample (MaxSampleReactors). The sample therefore lists every
// reactor, making userID's membership in it an exact answer that needs no extra
// store read.
//
// todo: Revisit when we introduce groups
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
