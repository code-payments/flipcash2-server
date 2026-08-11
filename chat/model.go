package chat

import (
	"bytes"
	"crypto/sha256"
	"fmt"
	"slices"
	"sort"
	"time"

	"github.com/google/uuid"
	"google.golang.org/protobuf/types/known/timestamppb"

	chatpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/chat/v1"
	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"
	messagingpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/messaging/v1"

	"github.com/code-payments/flipcash2-server/model"
)

// DmChatIDSize is the length, in bytes, of a DM chat ID: a SHA-256 digest over
// the DM's type and members (see MustDeriveDmChatID).
const DmChatIDSize = 32

// GroupChatIDSize is the length, in bytes, of a group chat ID: a
// server-generated UUID. Group membership is mutable, so a group's ID cannot be
// member-derived; it is an opaque random value minted at creation.
//
// The two sizes never overlap, so a chat ID's length is its type family's
// discriminator: 32 bytes is a DM, 16 bytes is a group. Every DM path must
// reject 16-byte IDs and every group path must reject 32-byte IDs — that
// enforcement is what keeps the discriminator sound (an ID can never be claimed
// as both a derived DM and a group).
const GroupChatIDSize = 16

// IsGroupChatID reports whether chatID is a group chat ID, by length (see
// GroupChatIDSize).
func IsGroupChatID(chatID *commonpb.ChatId) bool {
	return len(chatID.GetValue()) == GroupChatIDSize
}

// MustGenerateGroupChatID mints the ID for a new group chat: a random UUID.
// Group chat IDs are always generated server-side — a client-supplied ID is
// never trusted as a chat's identity.
func MustGenerateGroupChatID() *commonpb.ChatId {
	id, err := uuid.NewRandom()
	if err != nil {
		panic(fmt.Sprintf("failed to generate group chat id: %v", err))
	}
	return &commonpb.ChatId{Value: id[:]}
}

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
// is DmChatIDSize bytes wide by construction.
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

// dmChatTypes are the DM chat types with a canonical member-derived ID.
var dmChatTypes = []chatpb.ChatType{
	chatpb.ChatType_CONTACT_DM,
	chatpb.ChatType_TIP_DM,
}

// IsDmChatType reports whether chatType is a direct-message chat type — one
// with two participants and a canonical, member-derived ID — as opposed to a
// group or unknown chat.
func IsDmChatType(chatType chatpb.ChatType) bool {
	return slices.Contains(dmChatTypes, chatType)
}

// DeriveDmChatType reports which DM type's canonical derivation over the
// members produces chatID, letting callers that already hold a chat's members
// recover its type without a store read. It returns UNKNOWN when no DM type
// matches — including any malformed input — so callers must treat UNKNOWN as
// "not a derivable DM", not an error.
//
// This works because every DM's ID commits to its type via the derivation
// domain. A future chat type whose ID is not member-derived (e.g. group chats)
// will return UNKNOWN here and needs its own discriminator.
func DeriveDmChatType(chatID *commonpb.ChatId, members []*commonpb.UserId) chatpb.ChatType {
	if len(chatID.GetValue()) != DmChatIDSize {
		return chatpb.ChatType_UNKNOWN
	}

	var a, b *commonpb.UserId
	switch len(members) {
	case 1:
		a, b = members[0], members[0]
	case 2:
		a, b = members[0], members[1]
	default:
		return chatpb.ChatType_UNKNOWN
	}
	for _, u := range members {
		if len(u.GetValue()) != model.UserIDSize {
			return chatpb.ChatType_UNKNOWN
		}
	}

	for _, chatType := range dmChatTypes {
		if bytes.Equal(MustDeriveDmChatID(chatType, a, b).Value, chatID.Value) {
			return chatType
		}
	}
	return chatpb.ChatType_UNKNOWN
}

// Chat is the stored metadata for a chat.
//
// It deliberately holds only the state owned by the chat domain: the chat's
// identity, type, membership, title, and the last-activity timestamp used to
// order a user's chat list. The richer fields of chatpb.Metadata — member
// profiles, per-member message pointers, and the last message — live in other
// domains (profile, messaging) and are hydrated by the server layer.
//
// Members is the full, immutable member set for a DM, and is always empty for
// a group chat: group membership is mutable and lives in its own store records,
// which no path that reads the canonical record touches. A caller that needs a
// group's members reads them explicitly via Store.GetMembers. Title is
// group-only and empty for DMs.
type Chat struct {
	ID            *commonpb.ChatId
	Type          chatpb.ChatType
	Members       []*commonpb.UserId
	Title         string
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
		Title:         c.Title,
		LastActivity:  c.LastActivity,
		LastMessageID: lastMessageID,
	}
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
		Title:        c.Title,
		LastActivity: timestamppb.New(c.LastActivity),
	}
}
