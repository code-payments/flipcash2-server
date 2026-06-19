package messaging

import (
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"
	messagingpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/messaging/v1"
)

// ClientMessageIDSize is the length, in bytes, of a client message ID.
const ClientMessageIDSize = 16

// Message is a stored chat message.
//
// ID and UnreadSeq are server-assigned by the store at PutMessage time. ID is a
// per-chat gapless sequence number that is the message's canonical identity,
// sort key, and pagination cursor. UnreadSeq is a separate per-chat running
// count of unread-eligible messages (see messagingpb.Message for the full
// semantics).
type Message struct {
	ChatID    *commonpb.ChatId
	ID        *messagingpb.MessageId
	SenderID  *commonpb.UserId // nil for system messages
	Content   []*messagingpb.Content
	Timestamp time.Time
	UnreadSeq uint64
}

// Clone returns a deep copy of the message.
func (m *Message) Clone() *Message {
	content := make([]*messagingpb.Content, len(m.Content))
	for i, c := range m.Content {
		content[i] = proto.Clone(c).(*messagingpb.Content)
	}
	var senderID *commonpb.UserId
	if m.SenderID != nil {
		senderID = &commonpb.UserId{Value: append([]byte(nil), m.SenderID.Value...)}
	}
	return &Message{
		ChatID:    &commonpb.ChatId{Value: append([]byte(nil), m.ChatID.Value...)},
		ID:        &messagingpb.MessageId{Value: m.ID.Value},
		SenderID:  senderID,
		Content:   content,
		Timestamp: m.Timestamp,
		UnreadSeq: m.UnreadSeq,
	}
}

// ToProto projects the stored message onto a messagingpb.Message.
func (m *Message) ToProto() *messagingpb.Message {
	content := make([]*messagingpb.Content, len(m.Content))
	for i, c := range m.Content {
		content[i] = proto.Clone(c).(*messagingpb.Content)
	}
	out := &messagingpb.Message{
		MessageId:     &messagingpb.MessageId{Value: m.ID.Value},
		Content:       content,
		Ts:            timestamppb.New(m.Timestamp),
		UnreadSeq:     m.UnreadSeq,
		EventSequence: m.ID.Value,
	}
	if m.SenderID != nil {
		out.SenderId = &commonpb.UserId{Value: append([]byte(nil), m.SenderID.Value...)}
	}
	return out
}
