package redact

import (
	"fmt"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	chatpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/chat/v1"
	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"
	eventpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/event/v1"
	messagingpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/messaging/v1"
)

// ChatUpdate returns the redacted copy of a chat update: the same update with
// every message it carries replaced by its redaction (see Message) and
// everything else as it was. The input is never modified, so one update can
// be shared by pointer between a full stream and a redacted one, as the event
// delivery path shares every event.
//
// The mode a viewer reads a chat under decides only the messages (see
// eventpb.StreamEventsRequest.ChatParams), and an update carries them in
// three places: the mutations of its event-log events, the last_message of a
// metadata refresh, and the last_message of the metadata a MemberJoined hands
// its recipient. Pointer, typing, reaction and roster overlays pass through
// as they are — they describe the conversation's movement, which a redacted
// view shows, not its words. Like Message, the copy is built field by field
// so that every field carried is an allowlist decision: a field added to
// ChatUpdate later, or a new kind of mutation, metadata update or roster
// update, is refused with an error until one is made for it, rather than
// passed through to a viewer who may not read it.
//
// The chat ID seeds every placeholder (see Message), so a message redacted on
// the stream renders exactly as the same message redacted by GetMessages or
// GetDelta, and a client applying stream events to a redacted store sees one
// shape throughout.
func ChatUpdate(chatID *commonpb.ChatId, update *eventpb.ChatUpdate) (*eventpb.ChatUpdate, error) {
	out := &eventpb.ChatUpdate{
		Chat: cloneOrNil(update.GetChat()),
	}
	if update.GetPointerUpdates() != nil {
		out.PointerUpdates = proto.Clone(update.GetPointerUpdates()).(*messagingpb.PointerBatch)
	}
	if update.GetIsTypingNotifications() != nil {
		out.IsTypingNotifications = proto.Clone(update.GetIsTypingNotifications()).(*messagingpb.IsTypingNotificationBatch)
	}
	if update.GetReactionUpdates() != nil {
		out.ReactionUpdates = proto.Clone(update.GetReactionUpdates()).(*messagingpb.ReactionUpdateBatch)
	}
	for _, md := range update.GetMetadataUpdates() {
		redacted, err := metadataUpdate(chatID, md)
		if err != nil {
			return nil, err
		}
		out.MetadataUpdates = append(out.MetadataUpdates, redacted)
	}
	if update.GetEvents() != nil {
		out.Events = &messagingpb.EventBatch{}
		for _, e := range update.GetEvents().GetEvents() {
			redacted, err := event(chatID, e)
			if err != nil {
				return nil, err
			}
			out.Events.Events = append(out.Events.Events, redacted)
		}
	}
	if update.GetRosterUpdates() != nil {
		out.RosterUpdates = &chatpb.RosterUpdateBatch{}
		for _, r := range update.GetRosterUpdates().GetRosterUpdates() {
			redacted, err := rosterUpdate(chatID, r)
			if err != nil {
				return nil, err
			}
			out.RosterUpdates.RosterUpdates = append(out.RosterUpdates.RosterUpdates, redacted)
		}
	}
	return out, nil
}

// event redacts one event-log event: its position and time are kept, and each
// mutation's message is redacted whichever kind it is — a sent, edited or
// deleted message is a message the viewer may not read. A mutation of a kind
// this version does not know is an error.
func event(chatID *commonpb.ChatId, e *messagingpb.Event) (*messagingpb.Event, error) {
	out := &messagingpb.Event{
		Sequence: e.GetSequence(),
		Count:    e.GetCount(),
	}
	if e.GetTs() != nil {
		out.Ts = proto.Clone(e.GetTs()).(*timestamppb.Timestamp)
	}
	for _, m := range e.GetMutations() {
		var redacted messagingpb.Mutation
		switch kind := m.GetType().(type) {
		case *messagingpb.Mutation_MessageSent:
			msg, err := Message(chatID, kind.MessageSent)
			if err != nil {
				return nil, err
			}
			redacted.Type = &messagingpb.Mutation_MessageSent{MessageSent: msg}
		case *messagingpb.Mutation_MessageEdited:
			msg, err := Message(chatID, kind.MessageEdited)
			if err != nil {
				return nil, err
			}
			redacted.Type = &messagingpb.Mutation_MessageEdited{MessageEdited: msg}
		case *messagingpb.Mutation_MessageDeleted:
			msg, err := Message(chatID, kind.MessageDeleted)
			if err != nil {
				return nil, err
			}
			redacted.Type = &messagingpb.Mutation_MessageDeleted{MessageDeleted: msg}
		default:
			return nil, fmt.Errorf("redact: unsupported mutation %T", kind)
		}
		out.Mutations = append(out.Mutations, &redacted)
	}
	return out, nil
}

// metadataUpdate redacts one metadata update: a full refresh carries the
// chat's metadata, whose last_message is a message (see metadata); a
// last-activity change carries only a time. A kind this version does not know
// is an error.
func metadataUpdate(chatID *commonpb.ChatId, md *chatpb.MetadataUpdate) (*chatpb.MetadataUpdate, error) {
	switch kind := md.GetKind().(type) {
	case *chatpb.MetadataUpdate_FullRefresh_:
		redacted, err := metadata(chatID, kind.FullRefresh.GetMetadata())
		if err != nil {
			return nil, err
		}
		return &chatpb.MetadataUpdate{Kind: &chatpb.MetadataUpdate_FullRefresh_{
			FullRefresh: &chatpb.MetadataUpdate_FullRefresh{Metadata: redacted},
		}}, nil
	case *chatpb.MetadataUpdate_LastActivityChanged_:
		return proto.Clone(md).(*chatpb.MetadataUpdate), nil
	default:
		return nil, fmt.Errorf("redact: unsupported metadata update %T", kind)
	}
}

// rosterUpdate redacts one roster update: a join may carry the chat's
// metadata for its recipient, whose last_message is a message (see metadata);
// a departure carries none. A kind this version does not know is an error.
func rosterUpdate(chatID *commonpb.ChatId, r *chatpb.RosterUpdate) (*chatpb.RosterUpdate, error) {
	switch kind := r.GetKind().(type) {
	case *chatpb.RosterUpdate_MemberJoined_:
		out := proto.Clone(r).(*chatpb.RosterUpdate)
		joined := out.GetMemberJoined()
		if joined.GetMetadata() != nil {
			redacted, err := metadata(chatID, joined.GetMetadata())
			if err != nil {
				return nil, err
			}
			joined.Metadata = redacted
		}
		return out, nil
	case *chatpb.RosterUpdate_MemberLeft_:
		return proto.Clone(r).(*chatpb.RosterUpdate), nil
	default:
		return nil, fmt.Errorf("redact: unsupported roster update %T", kind)
	}
}

// metadata redacts a chat's metadata as chat.v1.GetChat does for a redacted
// viewer: the record as it is, with last_message — the one message it
// carries — redacted. A nil metadata is nil.
func metadata(chatID *commonpb.ChatId, md *chatpb.Metadata) (*chatpb.Metadata, error) {
	if md == nil {
		return nil, nil
	}
	out := proto.Clone(md).(*chatpb.Metadata)
	if out.GetLastMessage() != nil {
		redacted, err := Message(chatID, out.GetLastMessage())
		if err != nil {
			return nil, err
		}
		out.LastMessage = redacted
	}
	return out, nil
}

func cloneOrNil(chatID *commonpb.ChatId) *commonpb.ChatId {
	if chatID == nil {
		return nil
	}
	return proto.Clone(chatID).(*commonpb.ChatId)
}
