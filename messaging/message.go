package messaging

import (
	"bytes"
	"context"
	"errors"
	"time"

	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	eventpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/event/v1"
	messagingpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/messaging/v1"

	"github.com/code-payments/flipcash2-server/database"
	"github.com/code-payments/flipcash2-server/model"
)

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

	proto := msg.ToProto()
	if err := hydrateMedia(ctx, s.media, []*messagingpb.Message{proto}); err != nil {
		log.With(zap.Error(err)).Warn("Failure resolving media metadata")
	}

	return &messagingpb.GetMessageResponse{
		Result:  messagingpb.GetMessageResponse_OK,
		Message: proto,
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
	if err := hydrateMedia(ctx, s.media, protos); err != nil {
		log.With(zap.Error(err)).Warn("Failure resolving media metadata")
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

	repliedMessageID, ok := clientAllowedContent(req.Content)
	if !ok {
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

	// Share any media into the chat — validate the sender owns each blob and it is
	// a READY original, then grant the chat read access — before the message is
	// persisted and broadcast, so the grants are durable before any recipient can
	// resolve the blobs.
	if denied, err := s.shareMessageMedia(ctx, log, userID, req.ChatId, req.Content); err != nil {
		return nil, err
	} else if denied {
		return &messagingpb.SendMessageResponse{Result: messagingpb.SendMessageResponse_DENIED}, nil
	}

	msgProto, err := s.sender.Send(ctx, req.ChatId, userID, req.Content, req.ClientMessageId, true)
	if err != nil {
		return nil, err
	}

	return &messagingpb.SendMessageResponse{
		Result:  messagingpb.SendMessageResponse_OK,
		Message: msgProto,
	}, nil
}

func (s *Server) EditMessage(ctx context.Context, req *messagingpb.EditMessageRequest) (*messagingpb.EditMessageResponse, error) {
	userID, err := s.authz.Authorize(ctx, req, &req.Auth)
	if err != nil {
		return nil, err
	}

	log := s.log.With(zap.String("user_id", model.UserIDString(userID)))

	// The replacement content is held to the same whitelist as a send.
	repliedMessageID, ok := clientAllowedContent(req.Content)
	if !ok {
		return &messagingpb.EditMessageResponse{Result: messagingpb.EditMessageResponse_DENIED}, nil
	}

	if member, err := s.isMember(ctx, log, req.ChatId, userID); err != nil {
		return nil, err
	} else if !member {
		return &messagingpb.EditMessageResponse{Result: messagingpb.EditMessageResponse_DENIED}, nil
	}

	// The target must exist in this chat. Checked after membership so non-members
	// can't probe which message IDs exist.
	msg, err := s.messages.GetMessage(ctx, req.ChatId, req.MessageId)
	switch {
	case errors.Is(err, ErrMessageNotFound):
		return &messagingpb.EditMessageResponse{Result: messagingpb.EditMessageResponse_MESSAGE_NOT_FOUND}, nil
	case err != nil:
		log.With(zap.Error(err)).Warn("Failure getting message to edit")
		return nil, status.Error(codes.Internal, "")
	}

	// Only the original sender may edit their own message; a system message (no
	// sender) is never user-editable.
	if msg.SenderID == nil || !bytes.Equal(msg.SenderID.Value, userID.Value) {
		return &messagingpb.EditMessageResponse{Result: messagingpb.EditMessageResponse_DENIED}, nil
	}

	// Only user-authored conversational content is editable; a cash payment, a
	// system message, or an already-deleted tombstone is not (see IsEditable).
	if !msg.IsEditable() {
		return &messagingpb.EditMessageResponse{Result: messagingpb.EditMessageResponse_CANNOT_EDIT}, nil
	}

	// When the new content is a reply, the replied-to message must exist in this
	// chat and be repliable — the same rule a send is held to.
	if repliedMessageID != nil {
		repliedMessage, err := s.messages.GetMessage(ctx, req.ChatId, repliedMessageID)
		switch {
		case errors.Is(err, ErrMessageNotFound):
			return &messagingpb.EditMessageResponse{Result: messagingpb.EditMessageResponse_DENIED}, nil
		case err != nil:
			log.With(zap.Error(err)).Warn("Failure getting replied-to message")
			return nil, status.Error(codes.Internal, "")
		}
		if !repliedMessage.IsReplyable() {
			return &messagingpb.EditMessageResponse{Result: messagingpb.EditMessageResponse_DENIED}, nil
		}
	}

	// New media on an edit is shared into the chat just like a send, before the
	// edit is persisted and broadcast.
	if denied, err := s.shareMessageMedia(ctx, log, userID, req.ChatId, req.Content); err != nil {
		return nil, err
	} else if denied {
		return &messagingpb.EditMessageResponse{Result: messagingpb.EditMessageResponse_DENIED}, nil
	}

	now := time.Now().UTC()
	updated, err := s.messages.EditMessage(ctx, req.ChatId, req.MessageId, req.Content, now, req.ExpectedEventSequence)
	switch {
	case errors.Is(err, ErrEventSequenceConflict):
		// The optimistic guard rejected a stale expectation: a concurrent edit or
		// delete advanced the message past the version the caller edited against.
		// Unlike a delete an edit has no idempotent end-state to absorb, so this is
		// always a genuine conflict; surface the current state for the client to
		// reconcile rather than retrying blindly.
		return &messagingpb.EditMessageResponse{
			Result:  messagingpb.EditMessageResponse_CONFLICT,
			Message: updated.ToProto(),
		}, nil
	case errors.Is(err, ErrMessageNotFound):
		return &messagingpb.EditMessageResponse{Result: messagingpb.EditMessageResponse_MESSAGE_NOT_FOUND}, nil
	case err != nil:
		log.With(zap.Error(err)).Warn("Failure editing message")
		return nil, status.Error(codes.Internal, "")
	}

	// The edit rides only the event log: no new_messages (so no push, and no spurious
	// "new message" on pre-event-log clients) and no unread/pointer change. Members
	// apply the edit live via the message_edited event, or pick it up on their next
	// history load.
	updatedProto := updated.ToProto()
	// Resolve media metadata onto the edited message before it is broadcast and
	// returned. Best-effort: the edit is already committed, so a resolution failure
	// just leaves Blob unset for the client to re-fetch rather than failing the edit.
	if err := hydrateMedia(ctx, s.media, []*messagingpb.Message{updatedProto}); err != nil {
		log.With(zap.Error(err)).Warn("Failure resolving media metadata for edit")
	}
	publishChatUpdate(ctx, log, s.sender.badges, s.sender.chats, s.sender.profiles, s.sender.ocpData, s.sender.pusher, s.sender.eventBus, req.ChatId, &eventpb.ChatUpdate{
		Events: &messagingpb.EventBatch{Events: []*messagingpb.Event{NewMessageEditedEvent(updatedProto)}},
	}, nil, nil)

	return &messagingpb.EditMessageResponse{
		Result:  messagingpb.EditMessageResponse_OK,
		Message: updatedProto,
	}, nil
}

func (s *Server) DeleteMessage(ctx context.Context, req *messagingpb.DeleteMessageRequest) (*messagingpb.DeleteMessageResponse, error) {
	userID, err := s.authz.Authorize(ctx, req, &req.Auth)
	if err != nil {
		return nil, err
	}

	log := s.log.With(zap.String("user_id", model.UserIDString(userID)))

	if member, err := s.isMember(ctx, log, req.ChatId, userID); err != nil {
		return nil, err
	} else if !member {
		return &messagingpb.DeleteMessageResponse{Result: messagingpb.DeleteMessageResponse_DENIED}, nil
	}

	// The target must exist in this chat. Checked after membership so non-members
	// can't probe which message IDs exist.
	msg, err := s.messages.GetMessage(ctx, req.ChatId, req.MessageId)
	switch {
	case errors.Is(err, ErrMessageNotFound):
		return &messagingpb.DeleteMessageResponse{Result: messagingpb.DeleteMessageResponse_MESSAGE_NOT_FOUND}, nil
	case err != nil:
		log.With(zap.Error(err)).Warn("Failure getting message to delete")
		return nil, status.Error(codes.Internal, "")
	}

	// Only the original sender may delete their own message; a system message (no
	// sender) is never user-deletable.
	if msg.SenderID == nil || !bytes.Equal(msg.SenderID.Value, userID.Value) {
		return &messagingpb.DeleteMessageResponse{Result: messagingpb.DeleteMessageResponse_DENIED}, nil
	}

	// Deleting an already-deleted message is an idempotent no-op: the desired end
	// state (tombstoned) already holds, so return OK with the current tombstone
	// without advancing the event log or re-broadcasting. This makes a retried or
	// double-tapped delete — whose expected_event_sequence is now stale — succeed
	// rather than fail. A tombstone is also non-deletable, so this precedes the
	// IsDeletable check below.
	if msg.IsDeleted() {
		return &messagingpb.DeleteMessageResponse{
			Result:  messagingpb.DeleteMessageResponse_OK,
			Message: msg.ToProto(),
		}, nil
	}

	if !msg.IsDeletable() {
		return &messagingpb.DeleteMessageResponse{Result: messagingpb.DeleteMessageResponse_CANNOT_DELETE}, nil
	}

	now := time.Now().UTC()
	updated, err := s.messages.DeleteMessage(ctx, req.ChatId, req.MessageId, userID, now, req.ExpectedEventSequence)
	switch {
	case errors.Is(err, ErrEventSequenceConflict):
		// The optimistic guard rejected a stale expectation. If the message is
		// already tombstoned, the delete's intent is already satisfied, so treat it
		// as the same idempotent no-op as above — this also covers the window where
		// the eventually-consistent pre-read missed a just-completed delete.
		// Otherwise it's a genuine conflict (e.g. a concurrent edit): surface the
		// current state so the client can reconcile rather than retrying blindly.
		if updated.IsDeleted() {
			return &messagingpb.DeleteMessageResponse{
				Result:  messagingpb.DeleteMessageResponse_OK,
				Message: updated.ToProto(),
			}, nil
		}
		return &messagingpb.DeleteMessageResponse{
			Result:  messagingpb.DeleteMessageResponse_CONFLICT,
			Message: updated.ToProto(),
		}, nil
	case errors.Is(err, ErrMessageNotFound):
		return &messagingpb.DeleteMessageResponse{Result: messagingpb.DeleteMessageResponse_MESSAGE_NOT_FOUND}, nil
	case err != nil:
		log.With(zap.Error(err)).Warn("Failure deleting message")
		return nil, status.Error(codes.Internal, "")
	}

	// The tombstone rides only the event log: no new_messages (so no push, and no
	// spurious "new message" on pre-event-log clients) and no unread/pointer change.
	// Members apply the deletion live via the message_deleted event, or pick it up
	// on their next history load.
	updatedProto := updated.ToProto()
	publishChatUpdate(ctx, log, s.sender.badges, s.sender.chats, s.sender.profiles, s.sender.ocpData, s.sender.pusher, s.sender.eventBus, req.ChatId, &eventpb.ChatUpdate{
		Events: &messagingpb.EventBatch{Events: []*messagingpb.Event{NewMessageDeletedEvent(updatedProto)}},
	}, nil, nil)

	return &messagingpb.DeleteMessageResponse{
		Result:  messagingpb.DeleteMessageResponse_OK,
		Message: updatedProto,
	}, nil
}

// clientAllowedContent reports whether content is a message body a client may
// author via SendMessage or EditMessage, and extracts the replied-to message ID
// when it is a reply. The permitted set is a whitelist — currently a text or media
// message, or a reply whose own body is text or media — so it excludes
// server-injected content (e.g. cash payment messages) and any content type added
// later until it is explicitly allowed. repliedMessageID is non-nil only for a
// valid reply, signaling the caller to verify the replied-to message exists and is
// repliable.
func clientAllowedContent(content []*messagingpb.Content) (repliedMessageID *messagingpb.MessageId, ok bool) {
	if len(content) != 1 {
		return nil, false
	}
	switch c := content[0].Type.(type) {
	case *messagingpb.Content_Text:
		return nil, true
	case *messagingpb.Content_Media:
		return nil, validClientMedia(c.Media)
	case *messagingpb.Content_Reply:
		if len(c.Reply.Content) != 1 {
			return nil, false
		}
		if !validReplyBody(c.Reply.Content[0]) {
			return nil, false
		}
		return c.Reply.RepliedMessageId, true
	default:
		return nil, false
	}
}

// validReplyBody reports whether a reply's body is content a client may author:
// text or well-formed media.
func validReplyBody(content *messagingpb.Content) bool {
	switch c := content.Type.(type) {
	case *messagingpb.Content_Text:
		return true
	case *messagingpb.Content_Media:
		return validClientMedia(c.Media)
	default:
		return false
	}
}
