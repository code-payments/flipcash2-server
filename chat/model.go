package chat

import (
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	chatpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/chat/v1"
	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"
	messagingpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/messaging/v1"
)

// ChatIDSize is the length, in bytes, of a chat ID.
const ChatIDSize = 32

// Chat is the stored metadata for a chat.
//
// It deliberately holds only the state owned by the chat domain: the chat's
// identity, type, immutable membership, and the last-activity timestamp used to
// order a user's chat list. The richer fields of chatpb.Metadata — member
// profiles, per-member message pointers, and the last message — live in other
// domains (profile, messaging) and are hydrated by the server layer.
type Chat struct {
	ID            *commonpb.ChatId
	Type          chatpb.Metadata_ChatType
	Members       []*commonpb.UserId
	LastActivity  time.Time
	LastMessageID *messagingpb.MessageId
}

// Clone returns a deep copy of the chat.
func (c *Chat) Clone() *Chat {
	members := make([]*commonpb.UserId, len(c.Members))
	for i, m := range c.Members {
		members[i] = &commonpb.UserId{Value: append([]byte(nil), m.Value...)}
	}
	var lastMessageID *messagingpb.MessageId
	if c.LastMessageID != nil {
		lastMessageID = &messagingpb.MessageId{Value: c.LastMessageID.Value}
	}
	return &Chat{
		ID:            &commonpb.ChatId{Value: append([]byte(nil), c.ID.Value...)},
		Type:          c.Type,
		Members:       members,
		LastActivity:  c.LastActivity,
		LastMessageID: lastMessageID,
	}
}

// HasMember reports whether userID is a member of the chat.
func (c *Chat) HasMember(userID *commonpb.UserId) bool {
	for _, m := range c.Members {
		if string(m.Value) == string(userID.Value) {
			return true
		}
	}
	return false
}

// ToProto projects the stored chat onto a chatpb.Metadata. Only the fields
// owned by the chat domain are populated: chat_id, type, last_activity, and a
// Member entry per member with just user_id set. The caller is responsible for
// hydrating member profiles, pointers, and the last message.
func (c *Chat) ToProto() *chatpb.Metadata {
	members := make([]*chatpb.Member, len(c.Members))
	for i, m := range c.Members {
		members[i] = &chatpb.Member{
			UserId: &commonpb.UserId{Value: append([]byte(nil), m.Value...)},
		}
	}
	return &chatpb.Metadata{
		ChatId:       &commonpb.ChatId{Value: append([]byte(nil), c.ID.Value...)},
		Type:         c.Type,
		Members:      members,
		LastActivity: timestamppb.New(c.LastActivity),
	}
}
