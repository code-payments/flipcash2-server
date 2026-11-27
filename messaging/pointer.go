package messaging

import (
	"context"
	"time"

	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"
	eventpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/event/v1"
	messagingpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/messaging/v1"

	"github.com/code-payments/flipcash2-server/chat"
	"github.com/code-payments/flipcash2-server/model"
)

// NewPointer builds a stored pointer as the proto every read and event
// carries: userID's pointer of type t at value, last advanced at ts. Every
// store builds the pointers it returns through it, so no read path drops a
// field.
//
// unreadSeq is the unread_seq of the message at value as recorded with the
// pointer (see Store.AdvancePointer), or nil when none is recorded for the
// pointer's current value. It is carried only on a READ pointer and dropped
// on any other, as messagingpb.Pointer.unread_seq is never set on a
// DELIVERED pointer.
func NewPointer(t messagingpb.Pointer_Type, userID *commonpb.UserId, value uint64, ts time.Time, unreadSeq *uint64) *messagingpb.Pointer {
	pointer := &messagingpb.Pointer{
		Type:   t,
		UserId: &commonpb.UserId{Value: append([]byte(nil), userID.Value...)},
		Value:  &messagingpb.MessageId{Value: value},
		Ts:     timestamppb.New(ts),
	}
	if t == messagingpb.Pointer_READ && unreadSeq != nil {
		pointer.UnreadSeq = &messagingpb.UnreadSequence{Value: *unreadSeq}
	}
	return pointer
}

func (s *Server) AdvancePointer(ctx context.Context, req *messagingpb.AdvancePointerRequest) (*messagingpb.AdvancePointerResponse, error) {
	userID, err := s.authz.Authorize(ctx, req, &req.Auth)
	if err != nil {
		return nil, err
	}

	log := s.log.With(zap.String("user_id", model.UserIDString(userID)))

	// A pointer is a per-member item in the chat's partition, so advancing one
	// is a member's alone (see isMember): a non-member who may read the chat
	// leaves no record in it.
	if allowed, err := s.isMember(ctx, log, req.ChatId, userID); err != nil {
		return nil, err
	} else if !allowed {
		return &messagingpb.AdvancePointerResponse{Result: messagingpb.AdvancePointerResponse_DENIED}, nil
	}

	// The target must be a message in the chat. A READ advance records the
	// target's unread_seq with the pointer (see Store.AdvancePointer), so its
	// check is the one read that also returns that value. It always reaches
	// storage, where the cache's existence bound (see MessageExists) could
	// often answer a DELIVERED check without it, and it runs before the
	// cache's pointer bound can answer a no-op advance, so a READ advance that
	// moves nothing pays it too: a READ re-sent on reconnect, or one to the
	// caller's own message, which their send already advanced to. Before
	// unread_seq was recorded those cost no storage at all. The read projects
	// only what it needs, which saves bytes, not capacity: DynamoDB bills a
	// read by the whole item. That is the price of a READ pointer clients can
	// compute an unread count from alone; skipping it for a no-op would need
	// the RPC to consult the pointer bound before reading.
	var unreadSeq uint64
	if req.PointerType == messagingpb.Pointer_READ {
		seq, exists, err := s.messages.GetMessageUnreadSeq(ctx, req.ChatId, req.NewValue)
		if err != nil {
			log.With(zap.Error(err)).Warn("Failure reading message unread sequence")
			return nil, status.Error(codes.Internal, "")
		}
		if !exists {
			return &messagingpb.AdvancePointerResponse{Result: messagingpb.AdvancePointerResponse_MESSAGE_NOT_FOUND}, nil
		}
		unreadSeq = seq
	} else if exists, err := s.messageExists(ctx, log, req.ChatId, req.NewValue); err != nil {
		return nil, err
	} else if !exists {
		return &messagingpb.AdvancePointerResponse{Result: messagingpb.AdvancePointerResponse_MESSAGE_NOT_FOUND}, nil
	}

	pointer, advanced, err := s.messages.AdvancePointer(ctx, req.ChatId, userID, req.PointerType, req.NewValue, unreadSeq)
	if err != nil {
		log.With(zap.Error(err)).Warn("Failure advancing pointer")
		return nil, status.Error(codes.Internal, "")
	}

	// A group's pointer advances are stored but never broadcast: every member's
	// advance would otherwise fan out to every other member, so one message read
	// by N members would cost N² deliveries. Group clients read pointers on
	// hydration (GetChat) instead. DMs keep real-time pointers, where the fan-out
	// is one peer. The pointer published is the one the store returned, so a
	// READ pointer's update carries the unread_seq it recorded.
	if advanced && !chat.IsGroupChatID(req.ChatId) {
		s.sender.publishChatUpdate(ctx, log, req.ChatId, &eventpb.ChatUpdate{
			PointerUpdates: &messagingpb.PointerBatch{Pointers: []*messagingpb.Pointer{pointer}},
		}, nil, nil)
	}

	return &messagingpb.AdvancePointerResponse{Result: messagingpb.AdvancePointerResponse_OK}, nil
}
