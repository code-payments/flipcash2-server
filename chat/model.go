package chat

import (
	"bytes"
	"crypto/sha256"
	"fmt"
	"sort"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	chatpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/chat/v1"
	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"
	messagingpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/messaging/v1"

	"github.com/code-payments/flipcash2-server/model"
)

// ChatIDSize is the length, in bytes, of a chat ID.
const ChatIDSize = 32

// dmChatIDDomain namespaces the DM chat ID hash so it can never collide with an
// ID derived for another purpose, even if that purpose hashes the same members.
//
// Contact DMs hash under this bare domain; every other DM type appends its
// ChatType number (e.g. "flipcash:chat:dm:2" for tip DMs), so the same pair of
// users derives a distinct chat per DM type.
const dmChatIDDomain = "flipcash:chat:dm"

// MustDeriveDmChatID returns the deterministic chat ID for a DM of the given
// type between two users.
//
// The ID is derived purely from the DM type and the participants, so it is
// stable across calls and independent of who initiates the chat:
// MustDeriveDmChatID(t, a, b) always equals MustDeriveDmChatID(t, b, a). This
// lets either user open the canonical DM without a prior lookup, and makes
// creation idempotent.
//
// Derivation hashes the byte-sorted, de-duplicated set of user IDs (a DM with
// oneself collapses to a single member) under a domain-separation prefix that
// encodes the DM type. Contact DMs use the bare prefix because they predate
// typed derivation, and their chat IDs must not change; the domains cannot
// alias each other because member sets are fixed-width, so the two encodings
// never produce equal-length hash inputs. Since the input is a sorted set,
// member ordering and duplicates do not affect the result. The SHA-256 digest
// is ChatIDSize bytes wide by construction.
//
// It panics on an unspecified chat type, or if either user ID is not the
// expected fixed width, which would be a programming error: all user IDs in
// the system are UUIDs. Fixed-width members also make the sorted concatenation
// unambiguous without length prefixing.
func MustDeriveDmChatID(chatType chatpb.ChatType, a, b *commonpb.UserId) *commonpb.ChatId {
	domain := dmChatIDDomain
	switch chatType {
	case chatpb.ChatType_CONTACT_DM:
		// Bare legacy domain: contact DM IDs predate typed derivation.
	case chatpb.ChatType_TIP_DM:
		// Every other DM chat type appends its enum value to the domain
		domain = fmt.Sprintf("%s:%d", dmChatIDDomain, chatType)
	default:
		panic("unsupported chat type")
	}

	for _, u := range []*commonpb.UserId{a, b} {
		if len(u.Value) != model.UserIDSize {
			panic(fmt.Sprintf("user id must be %d bytes, got %d", model.UserIDSize, len(u.Value)))
		}
	}

	// Sorted set of the participants' raw ID bytes: sort, then drop the
	// duplicate so a self-DM hashes a single member.
	members := [][]byte{a.Value, b.Value}
	sort.Slice(members, func(i, j int) bool {
		return bytes.Compare(members[i], members[j]) < 0
	})
	if bytes.Equal(members[0], members[1]) {
		members = members[:1]
	}

	h := sha256.New()
	h.Write([]byte(domain))
	for _, m := range members {
		h.Write(m)
	}

	return &commonpb.ChatId{Value: h.Sum(nil)}
}

// Chat is the stored metadata for a chat.
//
// It deliberately holds only the state owned by the chat domain: the chat's
// identity, type, immutable membership, and the last-activity timestamp used to
// order a user's chat list. The richer fields of chatpb.Metadata — member
// profiles, per-member message pointers, and the last message — live in other
// domains (profile, messaging) and are hydrated by the server layer.
type Chat struct {
	ID            *commonpb.ChatId
	Type          chatpb.ChatType
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
