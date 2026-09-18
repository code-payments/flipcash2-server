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
// group's members reads them explicitly via Store.GetMembers. Title,
// IsStaffOnly, CreatorID and PictureBlobID are group-only and zero for DMs.
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
// CreatorID is the user who created the group, or nil when unknown (a DM has
// none, and so does any group written before the field existed). It is fixed
// at creation and records provenance only: creating a group does not by itself
// make the creator a member, and whether they are is answered by the membership
// records, never by this field.
//
// PictureBlobID is the blob holding the ORIGINAL rendition of the group's
// picture, or nil when the group has none. The chat domain stores only that
// handle: the full rendition set (and its download URLs) is resolved from blob
// storage by the server layer on read, exactly as a profile picture is. Read
// access is a blob-domain grant to the chat's members, made when the picture
// is set (see blob.Integration.SetAsChatPicture), so the record here carries
// no authorization of its own.
type Chat struct {
	ID                     *commonpb.ChatId
	Type                   chatpb.ChatType
	Members                []*commonpb.UserId
	RosterSummary          RosterSummary
	Title                  string
	IsStaffOnly            bool
	MinimumListenerBalance *MinimumBalance
	CreatorID              *commonpb.UserId
	PictureBlobID          *blobpb.BlobId
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
	var pictureBlobID *blobpb.BlobId
	if c.PictureBlobID != nil {
		pictureBlobID = &blobpb.BlobId{Value: append([]byte(nil), c.PictureBlobID.Value...)}
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
		CreatorID:              creatorID,
		PictureBlobID:          pictureBlobID,
		LastActivity:           c.LastActivity,
		LastMessageID:          lastMessageID,
	}
}

// ToProto projects the stored chat onto a chatpb.Metadata. Only the fields
// owned by the chat domain are populated: chat_id, type, title, last_activity,
// roster_summary, rules, a Member entry per member with just user_id set, and —
// for a group with a picture — a picture carrying only its ORIGINAL rendition's
// blob id. The caller is responsible for hydrating member profiles, pointers,
// the last message, and the picture's resolved rendition set.
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
		Rules:         c.Rules(),
		LastActivity:  timestamppb.New(c.LastActivity),
	}
	if c.PictureBlobID != nil {
		md.Picture = &blobpb.Media{
			Renditions: []*blobpb.Rendition{{
				Role:   blobpb.Rendition_ORIGINAL,
				BlobId: &blobpb.BlobId{Value: append([]byte(nil), c.PictureBlobID.Value...)},
			}},
		}
	}
	return md
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
// other members. The record outlives membership — the version never resets
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
type ViewerState struct {
	Mute    *Mute
	Version uint64
}

// MutedUsersPage is one page of a chat's muted users in user-ID order (see
// UserStateStore.GetMutedUsersInOrder): the users, and the cursor to resume
// after, nil once the walk is complete.
type MutedUsersPage struct {
	Users []*commonpb.UserId
	Next  *commonpb.UserId
}

// ActiveMute returns the recorded mute when it is in force at now, else nil.
func (v ViewerState) ActiveMute(now time.Time) *Mute {
	if v.Mute == nil || !v.Mute.Active(now) {
		return nil
	}
	return v.Mute
}

// ToProto projects the state onto a chatpb.ViewerState exactly as recorded:
// the version, and under settings the mute if one is recorded, lapsed or
// not. A version names one exact state, and every carrier of that version —
// a response, a hydrated Metadata, a ViewerStateChanged — must agree on it,
// which they could not if the projection depended on the clock it was made
// at. Whether a timed mute is still in force is the reader's call against
// its own clock (see Mute.Active); the server makes that call only where it
// acts on it, in the push fan-out.
func (v ViewerState) ToProto() *chatpb.ViewerState {
	out := &chatpb.ViewerState{
		Settings: &chatpb.ViewerState_Settings{},
		Version:  v.Version,
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
