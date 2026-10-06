package chat

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"fmt"
	"slices"
	"sort"
	"time"

	"github.com/google/uuid"
	"google.golang.org/protobuf/types/known/timestamppb"

	blobpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/blob/v1"
	chatpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/chat/v1"
	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"
	messagingpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/messaging/v1"

	"github.com/code-payments/flipcash2-server/model"
)

// DmChatIDSize is the length, in bytes, of a DM chat ID: a SHA-256 digest over
// the DM's type and members (see MustDeriveDmChatID).
const DmChatIDSize = 32

// GroupChatIDSize is the length, in bytes, of a group chat ID: the leading
// bytes of a SHA-256 digest over the creator and the idempotency key of the
// request that created it (see MustDeriveGroupChatID). Group membership is
// mutable, so a group's ID cannot be member-derived; it is an opaque value
// fixed at creation, and nothing parses it as anything else.
//
// The two sizes never overlap, so a chat ID's length is its type family's
// discriminator: 32 bytes is a DM, 16 bytes is a group. Every DM path must
// reject 16-byte IDs and every group path must reject 32-byte IDs — that
// enforcement is what keeps the discriminator sound (an ID can never be claimed
// as both a derived DM and a group).
const GroupChatIDSize = 16

// IdempotencyKeySize is the length, in bytes, of a client's IdempotencyKey: a
// UUID's worth of nonce, which is what a client typically mints for one.
const IdempotencyKeySize = 16

// IsGroupChatID reports whether chatID is a group chat ID, by length (see
// GroupChatIDSize).
func IsGroupChatID(chatID *commonpb.ChatId) bool {
	return len(chatID.GetValue()) == GroupChatIDSize
}

// groupChatIDDomain namespaces the group chat ID hash so it can never collide
// with an ID derived for another purpose. It is distinct from every DM domain
// (see dmChatIDDomain), and the two families differ in width regardless.
const groupChatIDDomain = "flipcash:chat:group"

// MustDeriveGroupChatID returns the ID of the group chat that a StartChat from
// creatorID carrying key creates.
//
// A group's ID is derived from its creator and the request's idempotency key
// rather than minted at random, so that the ID is itself the idempotency
// record: a retried request derives the same ID, and the store's uniqueness
// condition on creation turns the duplicate into a read of the original (see
// Server.StartChat). Nothing else is stored, and the mapping never expires.
//
// The creator is part of the input so that no client can derive another
// user's chat ID: the key is a nonce the client chooses, and two users who
// choose the same one derive two distinct groups. A client can predict the ID
// of its own group, which is harmless — group IDs are not secrets (see
// Server.GetChat). Group chat IDs remain server-derived: a client-supplied ID
// is never trusted as a chat's identity.
//
// The digest is truncated to GroupChatIDSize bytes, which is what makes the
// result a group ID by length, and then stamped as a version 8 UUID (RFC 9562
// section 5.8, the custom version, which the spec offers precisely for a
// truncated hash like this one). Group IDs are opaque and nothing parses them,
// but every group ID minted before this derivation was a random UUID, and the
// stamp keeps that shape true of all of them, at a cost of six bits nobody
// will miss. It panics if either input is not its fixed width, which would be
// a programming error: all user IDs in the system are UUIDs, and a request's
// key is checked to width before it reaches here. Fixed-width inputs also make
// the concatenation unambiguous without length prefixing.
func MustDeriveGroupChatID(creatorID *commonpb.UserId, key *chatpb.IdempotencyKey) *commonpb.ChatId {
	if len(creatorID.GetValue()) != model.UserIDSize {
		panic(fmt.Sprintf("user id must be %d bytes, got %d", model.UserIDSize, len(creatorID.GetValue())))
	}
	if len(key.GetValue()) != IdempotencyKeySize {
		panic(fmt.Sprintf("idempotency key must be %d bytes, got %d", IdempotencyKeySize, len(key.GetValue())))
	}

	h := sha256.New()
	h.Write([]byte(groupChatIDDomain))
	h.Write(creatorID.Value)
	h.Write(key.Value)

	id := h.Sum(nil)[:GroupChatIDSize]
	id[6] = (id[6] & 0x0f) | 0x80 // version 8
	id[8] = (id[8] & 0x3f) | 0x80 // RFC 9562 variant

	return &commonpb.ChatId{Value: id}
}

// MustGenerateGroupChatID mints a random group chat ID. StartChat does not use
// it — a group created by a client request derives its ID from the request
// (see MustDeriveGroupChatID) — so it is for a group that has no request behind
// it, which today means tests. Group chat IDs are always produced server-side
// either way; a client-supplied ID is never trusted as a chat's identity.
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
// ChatType number (e.g. "flipcash:chat:dm:2" for DM), so the same pair of
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
	case chatpb.ChatType_DM:
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
	chatpb.ChatType_DM,
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
// group's members reads them explicitly via Store.GetMembers. Title,
// IsStaffOnly, IsCreatorOnlySpeaker, IsPrivate, CreatorID, Description,
// ProfilePictureBlobID and CoverPictureBlobID are group-only and zero for DMs.
//
// RosterSummary describes the member list without containing it. Like Members,
// it is complete for a DM on any read and left zero for a group by the
// canonical record's reads: a group's summary is maintained alongside its
// membership records (see Store.GetGroupRosterSummary), filled in by the caller
// alongside its Members. It is derived state, ignored on PutChat.
//
// IsStaffOnly marks a group whose membership is restricted to staff users, and
// MinimumListenerBalance a group whose members must hold a balance (nil when
// the group asks for none). Both are stored state set at creation, surfaced to
// clients as listener rules (see Rules) and enforced by the messaging service
// through a RuleEvaluator on sends. Reads, of chat metadata and of messages
// alike, gate on membership alone, so a member a rule excludes can still see
// the chat and what it requires of them; enforcing rules on membership changes
// is the job of whatever path mutates membership.
//
// IsCreatorOnlySpeaker marks a group in which only its creator may speak. It
// is stored state, surfaced to clients as a CreatorRequirement speaker rule
// (see Rules) and enforced on sends like any other rule. No RPC sets it —
// StartChat refuses speaker rules (see RulesFromProto) — so a group carries
// it only when its record was written with it. A group that carries it but
// has no recorded creator admits no one to speak.
//
// IsPrivate marks a private group (see chatpb.Metadata.is_private): one whose
// creator admits each member, and whose messages are end-to-end encrypted
// with a chat key the server never holds. It is fixed at creation, like the
// rules, and read with them (see GroupRules). A private group carries no
// rules: StartChat writes none. No non-member is admitted to it in any form
// (see Access), no rule admits anyone to it (see RuleEvaluator), and
// JoinChat refuses everyone but its creator; each is decided on this flag,
// not on the absence of rules. Its
// title, description and pictures are plaintext on the record like any
// group's, and are shown to any registered user.
//
// A private group's chat key reaches the server only as its members' key
// envelopes (see KeyEnvelope), and nothing happens in one until its creator
// has stored theirs: a keyless group's members are refused every send, and
// a keyed group's members send encrypted content under the group's scheme
// and nothing else (see Access.SpeakerStanding and
// messaging.Server.SendMessage). Its other members are admitted by its
// creator from its lobby (see LobbyEntry and Server.EnterLobby), with the
// chat key wrapped for each by the creator.
//
// CreatorID is the user who created the group, or nil when unknown (a DM has
// none, and so does any group written before the field existed). It is fixed
// at creation and records provenance only: creating a group does not by itself
// make the creator a member, and whether they are is answered by the membership
// records, never by this field.
//
// Description is the group's free-form description, written by its creator,
// or empty when it has none. It is stored exactly as written (see
// ValidateDescription for what may be written).
//
// ProfilePictureBlobID is the blob holding the ORIGINAL rendition of the
// group's profile picture — its avatar — or nil when the group has none, and
// CoverPictureBlobID likewise for its cover picture, the banner behind its
// profile view. The chat domain stores only those handles: the full rendition
// sets (and their download URLs) are resolved from blob storage by the server
// layer on read, exactly as a user's pictures are. Read access is a
// blob-domain grant to the chat and to its public profile, made when a picture
// is set (see blob.Integration.SetAsChatMedia), so the record here carries no
// authorization of its own.
type Chat struct {
	ID                     *commonpb.ChatId
	Type                   chatpb.ChatType
	Members                []*commonpb.UserId
	RosterSummary          RosterSummary
	Title                  string
	IsStaffOnly            bool
	MinimumListenerBalance *MinimumBalance
	IsCreatorOnlySpeaker   bool
	IsPrivate              bool
	CreatorID              *commonpb.UserId
	Description            string
	ProfilePictureBlobID   *blobpb.BlobId
	CoverPictureBlobID     *blobpb.BlobId
	LastActivity           time.Time
	LastMessageID          *messagingpb.MessageId
}

// MinimumBalance is a balance a user must hold to satisfy a chat rule: at least
// NativeAmount of Currency (an ISO 4217 alpha-3 code, lowercase) worth of
// tokens, valued at the time the rule is evaluated. Mints restricts which mints
// the balance may be held in; empty means any mint counts. The proto it
// projects onto allows at most one mint today, and the model mirrors the
// proto's list so that lifting the cap is not a storage change.
//
// It is stored as given: what the amount and mints mean is the proto's contract
// (see chatpb.MinimumBalanceRequirement), and validating a requirement is the
// job of the boundary that accepts one, not the record.
type MinimumBalance struct {
	Currency     string
	NativeAmount float64
	Mints        []*commonpb.PublicKey
}

// Clone returns a deep copy of the requirement.
func (m *MinimumBalance) Clone() *MinimumBalance {
	mints := make([]*commonpb.PublicKey, len(m.Mints))
	for i, mint := range m.Mints {
		mints[i] = &commonpb.PublicKey{Value: append([]byte(nil), mint.Value...)}
	}
	return &MinimumBalance{
		Currency:     m.Currency,
		NativeAmount: m.NativeAmount,
		Mints:        mints,
	}
}

// ToProto projects the requirement onto a chatpb.MinimumBalanceRequirement.
func (m *MinimumBalance) ToProto() *chatpb.MinimumBalanceRequirement {
	return &chatpb.MinimumBalanceRequirement{
		Amount: &commonpb.FiatPaymentAmount{
			Currency:     m.Currency,
			NativeAmount: m.NativeAmount,
		},
		Mints: m.Clone().Mints,
	}
}

// RosterSummary summarizes a chat's roster — its member list — without
// enumerating it: how many members there are, and a version that moves
// whenever the membership records do.
//
// A DM's roster is fixed at creation, so its summary is the inline member
// count at version zero, forever. A group's is maintained by the store: every
// membership transition — a join, a departure, and in future any change to
// what a membership record holds about its member — moves Version by exactly
// one, and MemberCount by the transition's effect on the joined set. Idempotent
// no-ops (re-adding a joined member, removing a departed one) move neither.
//
// Version is state, not a sequence of deltas: a client compares it against the
// value it last saw and refetches members when they differ, and on a stream
// applies the greater value and drops the rest, so delivery order does not
// matter. It says nothing about member profiles, which live in their own domain
// and are hydrated afresh onto every response that carries them.
type RosterSummary struct {
	MemberCount uint64
	Version     uint64
}

// ToProto projects the summary onto a chatpb.RosterSummary.
func (r RosterSummary) ToProto() *chatpb.RosterSummary {
	return &chatpb.RosterSummary{
		MemberCount: r.MemberCount,
		Version:     r.Version,
	}
}

// HasMember reports whether userID is among the record's inline Members. It is
// a DM's membership check — a DM's members are fixed at creation and carried
// on its canonical record, so a caller holding the record has the answer in
// hand — and says nothing about a group, whose record carries no members
// (see Store.GetChatByID): for a group it is always false.
func (c *Chat) HasMember(userID *commonpb.UserId) bool {
	for _, m := range c.Members {
		if bytes.Equal(m.Value, userID.Value) {
			return true
		}
	}
	return false
}

// IsCreator reports whether userID created the chat (see CreatorID). It is
// false for a DM, and for a group whose creator was never recorded.
func (c *Chat) IsCreator(userID *commonpb.UserId) bool {
	return c.CreatorID != nil && bytes.Equal(c.CreatorID.Value, userID.Value)
}

// PermissionsFor returns what userID may do in the chat (see Permissions),
// given whether they are a member — which the caller has established, since
// a group's membership is not on its record. A non-member may do nothing. A
// member may edit the chat if and only if they created it: a DM has no
// creator, so a DM is never editable, and a group's creator who has left it
// edits nothing until they rejoin.
func (c *Chat) PermissionsFor(userID *commonpb.UserId, isMember bool) Permissions {
	return Permissions{CanEdit: isMember && c.IsCreator(userID)}
}

// GroupEdit is a change to a group's editable record fields (see
// Store.EditGroup): the title, the description, and the blobs holding the
// ORIGINALs of its profile picture and cover picture. A nil field is left as
// it is, so an edit names only what it changes and two edits of different
// fields never overwrite each other. The description is cleared by naming the
// empty one; the title and pictures cannot be cleared through an edit — a
// profile picture is removed with Store.SetGroupPicture — and an edit that
// names nothing is invalid.
type GroupEdit struct {
	Title                *string
	Description          *string
	ProfilePictureBlobID *blobpb.BlobId
	CoverPictureBlobID   *blobpb.BlobId
}

// IsEmpty reports whether the edit names nothing.
func (e GroupEdit) IsEmpty() bool {
	return e.Title == nil && e.Description == nil && e.ProfilePictureBlobID == nil && e.CoverPictureBlobID == nil
}

// Clone returns a deep copy of the chat.
func (c *Chat) Clone() *Chat {
	members := make([]*commonpb.UserId, len(c.Members))
	for i, m := range c.Members {
		members[i] = &commonpb.UserId{Value: append([]byte(nil), m.Value...)}
	}
	var minimumListenerBalance *MinimumBalance
	if c.MinimumListenerBalance != nil {
		minimumListenerBalance = c.MinimumListenerBalance.Clone()
	}
	var creatorID *commonpb.UserId
	if c.CreatorID != nil {
		creatorID = &commonpb.UserId{Value: append([]byte(nil), c.CreatorID.Value...)}
	}
	var profilePictureBlobID, coverPictureBlobID *blobpb.BlobId
	if c.ProfilePictureBlobID != nil {
		profilePictureBlobID = &blobpb.BlobId{Value: append([]byte(nil), c.ProfilePictureBlobID.Value...)}
	}
	if c.CoverPictureBlobID != nil {
		coverPictureBlobID = &blobpb.BlobId{Value: append([]byte(nil), c.CoverPictureBlobID.Value...)}
	}
	var lastMessageID *messagingpb.MessageId
	if c.LastMessageID != nil {
		lastMessageID = &messagingpb.MessageId{Value: c.LastMessageID.Value}
	}
	return &Chat{
		ID:                     &commonpb.ChatId{Value: append([]byte(nil), c.ID.Value...)},
		Type:                   c.Type,
		Members:                members,
		RosterSummary:          c.RosterSummary,
		Title:                  c.Title,
		IsStaffOnly:            c.IsStaffOnly,
		MinimumListenerBalance: minimumListenerBalance,
		IsCreatorOnlySpeaker:   c.IsCreatorOnlySpeaker,
		IsPrivate:              c.IsPrivate,
		CreatorID:              creatorID,
		Description:            c.Description,
		ProfilePictureBlobID:   profilePictureBlobID,
		CoverPictureBlobID:     coverPictureBlobID,
		LastActivity:           c.LastActivity,
		LastMessageID:          lastMessageID,
	}
}

// ToProto projects the stored chat onto a chatpb.Metadata. Only the fields
// owned by the chat domain are populated: chat_id, type, title, description,
// last_activity, roster_summary, rules, is_private, creator (a group's, when
// recorded), a Member entry per member with just user_id set, and — for a
// group with a profile picture or cover picture — each picture carrying only
// its ORIGINAL rendition's blob id. The caller is responsible for hydrating
// member profiles, pointers, the last message, and each picture's resolved
// rendition set.
func (c *Chat) ToProto() *chatpb.Metadata {
	members := make([]*chatpb.Member, len(c.Members))
	for i, m := range c.Members {
		members[i] = &chatpb.Member{
			UserId: &commonpb.UserId{Value: append([]byte(nil), m.Value...)},
		}
	}
	md := &chatpb.Metadata{
		ChatId:        &commonpb.ChatId{Value: append([]byte(nil), c.ID.Value...)},
		Type:          c.Type,
		Members:       members,
		RosterSummary: c.RosterSummary.ToProto(),
		Title:         c.Title,
		Description:   c.Description,
		Rules:         c.Rules(),
		IsPrivate:     c.IsPrivate,
		LastActivity:  timestamppb.New(c.LastActivity),
	}
	if c.CreatorID != nil {
		md.Creator = &commonpb.UserId{Value: append([]byte(nil), c.CreatorID.Value...)}
	}
	if c.ProfilePictureBlobID != nil {
		md.ProfilePicture = originalMedia(c.ProfilePictureBlobID)
	}
	if c.CoverPictureBlobID != nil {
		md.CoverPicture = originalMedia(c.CoverPictureBlobID)
	}
	return md
}

// originalMedia is a picture as the record knows it: its ORIGINAL rendition's
// blob id, with nothing resolved.
func originalMedia(blobID *blobpb.BlobId) *blobpb.Media {
	return &blobpb.Media{
		Renditions: []*blobpb.Rendition{{
			Role:   blobpb.Rendition_ORIGINAL,
			BlobId: &blobpb.BlobId{Value: append([]byte(nil), blobID.Value...)},
		}},
	}
}

// KeyEnvelope is one user's key envelope for a private group (see
// chatpb.KeyEnvelope and Chat.IsPrivate): the group's chat key, encrypted so
// that only that user can open it. The server stores an envelope and hands it
// back to the user it is for. It cannot open one and never holds the chat
// key, so nothing in an envelope is checked beyond its shape, which is the
// boundary's job (proto validation), not the record's.
//
// WrappedBy is who stored the envelope, as the server authenticated them: the
// user it is for, or the group's creator admitting them. It is the server's
// record, never a client's claim, and it is what a client is told to open the
// envelope against (GetKeyEnvelopeResponse.wrapped_by). It also decides
// whether the envelope can be replaced: one a user wrapped for themself
// stands (see Store.SetKeyEnvelope).
//
// A private group has a key exactly when its creator has an envelope stored.
// Nothing else records it: the creator's envelope is the only one that can
// be the group's first, it is always one they wrapped themself, and it is
// never deleted, so its presence is one-way. Every other member's envelope is
// deleted with their departure (see Server.LeaveChat).
type KeyEnvelope struct {
	Scheme     chatpb.KeyEnvelope_Scheme
	Nonce      []byte
	Ciphertext []byte
	WrappedBy  *commonpb.UserId
}

// KeyEnvelopeFromProto returns the envelope pb describes, recorded as stored
// by wrappedBy.
func KeyEnvelopeFromProto(pb *chatpb.KeyEnvelope, wrappedBy *commonpb.UserId) KeyEnvelope {
	return KeyEnvelope{
		Scheme:     pb.GetScheme(),
		Nonce:      pb.GetNonce(),
		Ciphertext: pb.GetCiphertext(),
		WrappedBy:  wrappedBy,
	}.Clone()
}

// ToProto projects the envelope onto a chatpb.KeyEnvelope. Who wrapped it is
// not part of the proto envelope; a response carries it beside the envelope.
func (e KeyEnvelope) ToProto() *chatpb.KeyEnvelope {
	return &chatpb.KeyEnvelope{
		Scheme:     e.Scheme,
		Nonce:      bytes.Clone(e.Nonce),
		Ciphertext: bytes.Clone(e.Ciphertext),
	}
}

// Equal reports whether the two envelopes are the same envelope: the same
// bytes under the same scheme, stored by the same user.
func (e KeyEnvelope) Equal(other KeyEnvelope) bool {
	return e.Scheme == other.Scheme &&
		bytes.Equal(e.Nonce, other.Nonce) &&
		bytes.Equal(e.Ciphertext, other.Ciphertext) &&
		bytes.Equal(e.WrappedBy.GetValue(), other.WrappedBy.GetValue())
}

// IsWrappedBy reports whether userID stored the envelope.
func (e KeyEnvelope) IsWrappedBy(userID *commonpb.UserId) bool {
	return e.WrappedBy != nil && bytes.Equal(e.WrappedBy.Value, userID.GetValue())
}

// Clone returns a deep copy of the envelope.
func (e KeyEnvelope) Clone() KeyEnvelope {
	clone := KeyEnvelope{
		Scheme:     e.Scheme,
		Nonce:      bytes.Clone(e.Nonce),
		Ciphertext: bytes.Clone(e.Ciphertext),
	}
	if e.WrappedBy != nil {
		clone.WrappedBy = &commonpb.UserId{Value: bytes.Clone(e.WrappedBy.Value)}
	}
	return clone
}

// ErrMuteUntilOutOfRange indicates that a timed mute ends outside the range a
// store can record (see Mute.Until).
var ErrMuteUntilOutOfRange = errors.New("mute until is out of range")

// MaxMuteUntil bounds a timed mute: Mute.Until must be strictly before it.
// Stores encode an indefinite mute as this instant, so a timed mute reaching
// it would read back as indefinite; rejecting the boundary keeps the two
// distinguishable. No real mute ends in the year 9999, so nothing is lost.
var MaxMuteUntil = time.Date(9999, 12, 31, 23, 59, 59, 0, time.UTC)

// Mute is a mute a viewer set on a chat: until an instant, or until they lift
// it themselves (Forever). While a mute is active the viewer still receives
// the chat's pushes, flagged so the client suppresses the notification (see
// push.v1.ChatMetadata.muted); it never affects message delivery.
//
// Until is meaningful only when Forever is false, and is recorded at second
// precision: a store truncates it on write and returns the truncated value.
// It must lie in [Unix epoch, MaxMuteUntil), else the write is rejected with
// ErrMuteUntilOutOfRange. A timed mute past its Until has lapsed: it is still
// recorded, and returned as stored, until replaced or cleared — nothing
// sweeps it, and nothing is published when it lapses — so a reader decides
// with Active, never from presence alone.
type Mute struct {
	Forever bool
	Until   time.Time
}

// Active reports whether the mute is in force at now.
func (m Mute) Active(now time.Time) bool {
	return m.Forever || now.Before(m.Until)
}

// ToProto projects the mute onto a chatpb.MuteState.
func (m Mute) ToProto() *chatpb.MuteState {
	if m.Forever {
		return &chatpb.MuteState{Duration: &chatpb.MuteState_Forever_{Forever: &chatpb.MuteState_Forever{}}}
	}
	return &chatpb.MuteState{Duration: &chatpb.MuteState_Until{Until: timestamppb.New(m.Until)}}
}

// MuteFromProto is the inverse of Mute.ToProto. A MuteState carrying neither
// duration — which validation rejects before a request reaches here — reads
// as a timed mute at the zero time, which no store records (see Mute).
func MuteFromProto(m *chatpb.MuteState) Mute {
	switch d := m.GetDuration().(type) {
	case *chatpb.MuteState_Forever_:
		return Mute{Forever: true}
	case *chatpb.MuteState_Until:
		return Mute{Until: d.Until.AsTime()}
	default:
		return Mute{}
	}
}

// Normalize returns the mute as a store records it — Until truncated to the
// second, in UTC, and zeroed when Forever — or ErrMuteUntilOutOfRange when
// it cannot be recorded. Every store applies it on write, so equality of two
// normalized mutes is what "the mute already recorded" means.
func (m Mute) Normalize() (Mute, error) {
	if m.Forever {
		return Mute{Forever: true}, nil
	}
	until := m.Until.Truncate(time.Second)
	if until.Unix() < 0 || !until.Before(MaxMuteUntil) {
		return Mute{}, ErrMuteUntilOutOfRange
	}
	return Mute{Until: until.UTC()}, nil
}

// ViewerState is what a chat holds about one user, independent of whether
// they are a member: state the user set for themselves (today, a mute) and a
// version over all of it. It is private to that user and never shared with
// other members, and shown to the user only while they are a member (see
// Server.hydrate). The record outlives membership — the version never resets
// — but what it holds may not: a mute is cleared, best effort, when the user
// leaves the chat (see Server.LeaveChat), so a user who returns to a group
// starts unmuted unless that clear failed.
//
// Mute is the recorded mute, or nil when none is set; see Mute for what a
// recorded mute may still mean. Version is state, not a delta: every real
// change — a mute set, replaced or cleared — moves it by exactly one and an
// idempotent no-op leaves it alone, so a client keeps the greater of two
// versions and drops the rest, whatever order they arrived in. It is not the
// roster version (see RosterSummary), which nothing here ever moves.
//
// What a client is shown alongside the record is the chat's Permissions for
// the user (see ToProto), computed from the chat's record and the user's
// membership at the time of the read rather than stored, and not covered by
// Version: a membership transition changes what the user may do (see
// Chat.PermissionsFor) without moving the record, so the same version may
// carry different permissions before and after one. That is accepted for
// now — a transition of the user's own is what changes them, and it reaches
// the user's devices as a RosterUpdate carrying fresh metadata — rather than
// moving the version on every transition of a user who may hold no record.
type ViewerState struct {
	Mute    *Mute
	Version uint64
}

// Permissions is what a user may do in a chat beyond what membership alone
// allows every member: today, whether they may edit its record (see
// Server.EditChat). It is computed from the chat's record and the user's
// membership (see Chat.PermissionsFor), never stored, and shown to the user
// alone on their ViewerState — a client shows an affordance if and only if
// its flag is set, so the flags are the server's word on what an RPC will
// admit, and every gate must agree with them.
type Permissions struct {
	CanEdit bool
}

// ToProto projects the permissions onto a chatpb.ViewerState_Permissions.
func (p Permissions) ToProto() *chatpb.ViewerState_Permissions {
	return &chatpb.ViewerState_Permissions{CanEdit: p.CanEdit}
}

// MembersPage is one page of a group's joined members in user-ID order (see
// Store.GetGroupMembersPage): the users, and the cursor to resume after, nil
// once the walk is complete. Its first and last user bound the key range a
// caller hands Store.GetMutedUsersPage for the same page.
type MembersPage struct {
	Users []*commonpb.UserId
	Next  *commonpb.UserId
}

// GroupMember is one joined member of a group as its membership record holds
// them: who, when they most recently joined, and the roster version that
// placed them there — the version the group's RosterSummary moved to on their
// join, or zero for a member written at the group's creation, which no
// transition has touched (see GroupMembership for the same stamp read the
// other way round). A member who left and rejoined carries the rejoin's time
// and version. It is what a roster read hands a client per member, and what
// Member.joined_at and Member.version are projected from.
type GroupMember struct {
	UserID   *commonpb.UserId
	JoinedAt time.Time
	Version  uint64
}

// Position is the member's place in roster order (see RosterPosition).
func (m GroupMember) Position() RosterPosition {
	return RosterPosition{JoinedAt: m.JoinedAt, UserID: m.UserID}
}

// RosterPosition is a place in a group's roster order: most recently joined
// first, ties broken by user ID descending, so the order is total and a page
// can resume strictly after the last member it carried. Store.GetGroupRosterPage
// walks in this order; a caller that reads a roster whole sorts it with
// SortRoster. Ties in join time are nanosecond coincidences within one group,
// so the tie-break exists for totality rather than for anything a client sees.
//
// A DM's participants have no join time (see Member.joined_at): a DM's
// position is the zero time and the user ID alone, so a DM's roster is in
// user-ID order under the same rule.
type RosterPosition struct {
	JoinedAt time.Time
	UserID   *commonpb.UserId
}

// Compare orders positions in roster order: negative when p comes before o
// (joined later, or the same instant with the greater user ID), positive when
// after, zero when they name the same member.
func (p RosterPosition) Compare(o RosterPosition) int {
	if c := o.JoinedAt.Compare(p.JoinedAt); c != 0 {
		return c
	}
	return bytes.Compare(o.UserID.Value, p.UserID.Value)
}

// SortRoster sorts members into roster order (see RosterPosition).
func SortRoster(members []GroupMember) {
	slices.SortFunc(members, func(a, b GroupMember) int { return a.Position().Compare(b.Position()) })
}

// ActiveMute returns the recorded mute when it is in force at now, else nil.
func (v ViewerState) ActiveMute(now time.Time) *Mute {
	if v.Mute == nil || !v.Mute.Active(now) {
		return nil
	}
	return v.Mute
}

// ToProto projects the state onto a chatpb.ViewerState exactly as recorded:
// the version, under settings the mute if one is recorded, lapsed or not,
// and the user's permissions in the chat as the caller computed them (see
// Permissions). A version names one exact state, and every carrier of that
// version — a response, a hydrated Metadata, a ViewerStateChanged — must
// agree on it, which they could not if the projection depended on the clock
// it was made at. Whether a timed mute is still in force is the reader's call
// against its own clock (see Mute.Active); the server makes that call only
// where it acts on it, in the push fan-out. The permissions are always
// carried, empty for a user who may do nothing, so a carrier never leaves a
// client to guess whether they were withheld or are none.
func (v ViewerState) ToProto(permissions Permissions) *chatpb.ViewerState {
	out := &chatpb.ViewerState{
		Settings:    &chatpb.ViewerState_Settings{},
		Permissions: permissions.ToProto(),
		Version:     v.Version,
	}
	if v.Mute != nil {
		out.Settings.Mute = v.Mute.ToProto()
	}
	return out
}

// Clone returns a deep copy of the state.
func (v ViewerState) Clone() ViewerState {
	out := ViewerState{Version: v.Version}
	if v.Mute != nil {
		mute := *v.Mute
		out.Mute = &mute
	}
	return out
}

// A group's activity record is what the chat remembers of each user's recent
// sends in it, one record per (group, user): today only when they last sent
// (see RecentSender), which orders a group's recent senders for mention
// suggestions, most recent first. It is a fact about the message log, not
// about membership: a record outlives its user's departure, and nothing reads
// the roster to answer it, because a user sees the senders in a chat's log
// without knowing who still belongs to it. Everyone it names was a member
// when they sent, since sending requires it.
//
// It is derived and best effort. A send is recorded after it lands and
// outside its write, so a failed record costs a suggestion and never a
// message, and the next send repairs it. Records are throttled: a send
// within ActivityRecordInterval of the last one recorded is not recorded, so
// a burst costs one write and a recorded time may trail the latest send by up
// to the interval. Records expire ActivityRetention after the last recorded
// send; expiry is garbage collection, not semantics, so a reader may still
// see a record past it.
//
// Beside recency, each record carries an activity score, a frequency-weighted
// ordering of the same sends (see NextActivityScore), which is why it is an
// activity record and not a send time alone. The score is maintained with
// every recorded send, and orders a group's chatter sample (see
// Store.GetActiveSenders).
const (
	// ActivityRecordInterval is the least time between two recorded sends by
	// one user in one group.
	ActivityRecordInterval = time.Minute

	// ActivityRetention is how long a group's activity record is kept after
	// its last recorded send. Old records cost nothing to a read, which takes
	// only the most recent, and are exactly the suggestions a group revived
	// after a long quiet needs, so this is long; it is finite so the records
	// of users who stopped using the app, deleted accounts included, age out
	// on their own.
	ActivityRetention = 365 * 24 * time.Hour
)

// RecentSender is one user's activity record in a group, as the reads of a
// group's senders return it (see Store.GetRecentSenders and
// Store.GetActiveSenders): who, when their latest recorded send was, and the
// record's activity score (see EffectiveActivityScore), both at millisecond
// precision.
type RecentSender struct {
	UserID        *commonpb.UserId
	LastSentAt    time.Time
	ActivityScore time.Time
}

// A private group's lobby (see Chat.IsPrivate) is where a user waits to be
// admitted to it by its creator: they enter it (Server.EnterLobby), and leave
// it when they withdraw, are denied, or are admitted (Server.AdmitLobbyMember,
// which stores the chat key wrapped for them and joins them in one write). A
// lobby is the creator's alone to see; the waiting users are not shown to
// each other or to the members. It is recorded as one entry per (user,
// private group), written against the IDs alone like a key envelope, and is
// not membership: a waiting user has no standing in the chat beyond the
// record any registered user sees, with Metadata.in_lobby set for them.
//
// Both a lobby and a user's waiting are capped (see LobbyLimits), so a store
// counts both and refuses an entry past either cap in the write that would
// have made it. A lobby is read two ways: a user's own entries, strongly
// consistent, for in_lobby and the entry EnterLobby returns, and one day the
// listing of every lobby a user waits in; and a group's lobby paged
// earliest-entered first (see LobbyPosition) off an index that trails writes
// briefly, as the proto allows, since the creator reads it to act on it and
// reconciles against the LobbyUpdates they receive.

// LobbyEntry is one user's place in a private group's lobby: who, and when
// they entered. It is what a lobby page carries per user and what
// LobbyMember.entered_at and Lobby.entered_at are projected from.
type LobbyEntry struct {
	UserID    *commonpb.UserId
	EnteredAt time.Time
}

// Position is the entry's place in its lobby's order.
func (e LobbyEntry) Position() LobbyPosition {
	return LobbyPosition{EnteredAt: e.EnteredAt, UserID: e.UserID}
}

// LobbyPosition is a place in a lobby's order: earliest entered first, ties
// broken by user ID ascending, so the order is total and a page can resume
// strictly after the last entry it carried. Ties in entry time are
// nanosecond coincidences within one lobby, so the tie-break exists for
// totality rather than for anything a client sees.
type LobbyPosition struct {
	EnteredAt time.Time
	UserID    *commonpb.UserId
}

// Compare orders two positions: negative when p precedes o.
func (p LobbyPosition) Compare(o LobbyPosition) int {
	if c := p.EnteredAt.Compare(o.EnteredAt); c != 0 {
		return c
	}
	return bytes.Compare(p.UserID.GetValue(), o.UserID.GetValue())
}

// LobbyLimits caps a lobby's size and how many lobbies one user may wait
// in, enforced by Store.EnterLobby in the write that records the entry. The
// server's are DefaultLobbyLimits; tests lower them.
type LobbyLimits struct {
	// LobbySize is the most users one group's lobby holds.
	LobbySize int
	// LobbiesPerUser is the most lobbies one user waits in at once.
	LobbiesPerUser int
}

// DefaultLobbyLimits are the caps in production, deliberately modest to
// start: a lobby of a hundred is as many as a creator admits in a sitting,
// and a user waiting in a hundred groups is not waiting to join them. Either
// is raised here when it binds.
var DefaultLobbyLimits = LobbyLimits{LobbySize: 100, LobbiesPerUser: 100}
