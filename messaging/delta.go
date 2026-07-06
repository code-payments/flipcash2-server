package messaging

import (
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	messagingpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/messaging/v1"

	"github.com/code-payments/flipcash2-server/model"
)

const (
	// deltaPageSize bounds each GetDelta batch. MessageBatch caps at 100 messages,
	// so the delta is streamed in pages of at most this many.
	deltaPageSize = 100

	// maxDeltaEvents bounds how large a GetDelta catch-up may be: the number of
	// events between the client's cursor and the head. When the gap exceeds it,
	// streaming the delta would cost more than a fresh history load, so the server
	// returns RESET_REQUIRED and the client re-syncs chat history via GetMessages
	// before resuming the event stream.
	maxDeltaEvents uint64 = 1000
)

func (s *Server) GetDelta(req *messagingpb.GetDeltaRequest, stream messagingpb.Messaging_GetDeltaServer) error {
	ctx := stream.Context()

	userID, err := s.authz.Authorize(ctx, req, &req.Auth)
	if err != nil {
		return err
	}

	log := s.log.With(zap.String("user_id", model.UserIDString(userID)))

	if member, err := s.isMember(ctx, log, req.ChatId, userID); err != nil {
		return err
	} else if !member {
		// A terminal DENIED is a single response that ends the stream.
		return stream.Send(&messagingpb.GetDeltaResponse{Result: messagingpb.GetDeltaResponse_DENIED})
	}

	// Capture the head once, at stream open: the delta converges to this target
	// even as the chat advances under live traffic. Anything past it is delivered
	// by the live event stream, not this bounded catch-up.
	head, err := s.messages.GetLatestEventSequence(ctx, req.ChatId)
	if err != nil {
		log.With(zap.Error(err)).Warn("Failure getting chat head event sequence")
		return status.Error(codes.Internal, "")
	}

	// Already current: nothing changed past the client's cursor. Report the head
	// and leave messages and checkpoint_sequence unset, so the cursor is unchanged.
	if req.AfterSequence >= head {
		return stream.Send(&messagingpb.GetDeltaResponse{
			Result:         messagingpb.GetDeltaResponse_OK,
			LatestSequence: head,
		})
	}

	// Oversized catch-up: beyond maxDeltaEvents the gap is too large to stream as a
	// delta, so tell the client to discard its cursor and re-sync history from
	// GetMessages. head > AfterSequence here (already-current handled above), so
	// the subtraction can't underflow. LatestSequence carries the head as the
	// resume floor: after reloading history the client sets its cursor here, and the
	// (small) gap accumulated during the reload is caught by its next GetDelta —
	// without it the client would have to derive the head from the full history,
	// which is fragile (post-divergence the head is a tombstone's event_sequence,
	// not the newest message).
	if head-req.AfterSequence > maxDeltaEvents {
		return stream.Send(&messagingpb.GetDeltaResponse{
			Result:         messagingpb.GetDeltaResponse_RESET_REQUIRED,
			LatestSequence: head,
		})
	}

	// Walk the event log in (after, head] in pages. Each page returns the current
	// state of the messages whose events it covers, with superseded events already
	// dropped by the store; the checkpoint advances over every event scanned (the
	// store's nextCursor), so a fully superseded page still makes progress. The log
	// is gapless and read consistently, so this converges on head without skipping.
	cursor := req.AfterSequence
	for cursor < head {
		msgs, nextCursor, err := s.messages.GetEventDelta(ctx, req.ChatId, cursor, head, deltaPageSize)
		if err != nil {
			log.With(zap.Error(err)).Warn("Failure reading delta page")
			return status.Error(codes.Internal, "")
		}
		// No forward progress should be impossible on a gapless log while cursor <
		// head; guard against re-reading the same page forever (the live stream
		// delivers anything past head).
		if nextCursor <= cursor {
			break
		}

		resp := &messagingpb.GetDeltaResponse{
			Result:             messagingpb.GetDeltaResponse_OK,
			LatestSequence:     head,
			CheckpointSequence: nextCursor,
		}
		// A page may carry no surviving messages (all superseded) yet still advance
		// the checkpoint, so only attach a batch when there's something to apply.
		if len(msgs) > 0 {
			batch := make([]*messagingpb.Message, len(msgs))
			for i, m := range msgs {
				batch[i] = m.ToProto()
			}
			if err := hydrateMedia(ctx, s.media, batch); err != nil {
				log.With(zap.Error(err)).Warn("Failure resolving media metadata")
			}
			resp.Messages = &messagingpb.MessageBatch{Messages: batch}
		}
		if err := stream.Send(resp); err != nil {
			return err
		}
		cursor = nextCursor
	}

	return nil
}
