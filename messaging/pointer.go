package messaging

import (
	"context"

	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	eventpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/event/v1"
	messagingpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/messaging/v1"

	"github.com/code-payments/flipcash2-server/chat"
	"github.com/code-payments/flipcash2-server/model"
)

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

	// A group's pointer advances are stored but never broadcast: every member's
	// advance would otherwise fan out to every other member, so one message read
	// by N members would cost N² deliveries. Group clients read pointers on
	// hydration (GetChat) instead. DMs keep real-time pointers, where the fan-out
	// is one peer.
	if advanced && !chat.IsGroupChatID(req.ChatId) {
		publishChatUpdate(ctx, log, s.sender.badges, s.sender.chats, s.sender.profiles, s.sender.blocklists, s.sender.ocpData, s.sender.pusher, s.sender.userEventBus, s.sender.chatEventBus, req.ChatId, &eventpb.ChatUpdate{
			PointerUpdates: &messagingpb.PointerBatch{Pointers: []*messagingpb.Pointer{pointer}},
		}, nil, nil)
	}

	return &messagingpb.AdvancePointerResponse{Result: messagingpb.AdvancePointerResponse_OK}, nil
}
