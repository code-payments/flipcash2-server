package redact

import (
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"
	messagingpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/messaging/v1"
)

// Message returns the redacted copy of a message: the same message — its
// identity, sender, timestamps, sequence numbers — with each content item
// replaced by its placeholder (see Content) and Message.redacted set. The
// input is never modified, and a redacted copy is its own redaction.
//
// Like Content, the copy is built field by field so every field carried is
// an allowlist decision and a field added to the proto later is dropped
// until one is made for it:
//
//   - message_id, ts, unread_seq, last_edited_ts, event_sequence: kept.
//     They say that a message exists, when, and how the chat has moved,
//     which is exactly what a redacted view shows. event_sequence still
//     describes the underlying message: a redacted copy is not a version of
//     it (see Message.redacted).
//   - sender_id: kept. Who spoke is the same kind of fact as who deleted a
//     message (see Content on DeletedContent), and a redacted view is a
//     conversation's rhythm — who is talking, how much — with the words
//     removed.
//   - content: each item is its placeholder. An item Content refuses fails
//     the whole message, so nothing unknown is passed through.
//   - reactions: not carried, as no message read carries them. Reactions are
//     an overlay the reaction RPCs and the event stream serve, to a redacted
//     reader as to a full one (see messaging.Server's overlayStanding): who
//     reacted, with what, on which message is the conversation's movement,
//     not its words.
//
// The message is expected to have had its media resolved already, so the
// placeholder keeps each rendition's dimensions and blurhash (see Content on
// media); redacting a stored message that carries only its ORIGINAL blob ID
// leaves the viewer nothing to render but the placeholder's frame.
func Message(chatID *commonpb.ChatId, msg *messagingpb.Message) (*messagingpb.Message, error) {
	seed := messageSeed(chatID, msg.GetMessageId())
	out := &messagingpb.Message{
		MessageId:     proto.Clone(msg.GetMessageId()).(*messagingpb.MessageId),
		Ts:            proto.Clone(msg.GetTs()).(*timestamppb.Timestamp),
		UnreadSeq:     msg.GetUnreadSeq(),
		EventSequence: msg.GetEventSequence(),
		Redacted:      true,
	}
	if msg.GetSenderId() != nil {
		out.SenderId = proto.Clone(msg.GetSenderId()).(*commonpb.UserId)
	}
	if msg.GetLastEditedTs() != nil {
		out.LastEditedTs = proto.Clone(msg.GetLastEditedTs()).(*timestamppb.Timestamp)
	}
	for _, c := range msg.GetContent() {
		redacted, err := content(seed, c)
		if err != nil {
			return nil, err
		}
		out.Content = append(out.Content, redacted)
	}
	return out, nil
}
