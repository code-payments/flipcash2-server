package messaging

import (
	"bytes"
	"math"
	"sort"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"
	messagingpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/messaging/v1"
	"github.com/code-payments/flipcash2-server/account"
)

// ClientMessageIDSize is the length, in bytes, of a client message ID.
const ClientMessageIDSize = 16

// Reaction-aggregate bounds
const (
	// MaxReactionTypesPerMessage caps how many distinct emoji may react to a
	// single message. The (N+1)th distinct emoji is rejected with
	// TOO_MANY_REACTION_TYPES; a re-add of an already-present emoji is unaffected.
	//
	// The cap bounds what one message can cost every reader: a page of
	// summaries carries every active emoji of every message in it, with a
	// sample each, and the viewer's self overlay resolves one key per (message,
	// emoji). Twenty is what the largest chat products render under a message
	// and keeps a full page of capped messages well inside a gRPC response.
	// Lowering it later is safe: the cap blocks activating a new emoji and
	// never touches what a message already has.
	MaxReactionTypesPerMessage = 20

	// MaxSampleReactors bounds the sample of reactors surfaced inline on an
	// EmojiReaction (e.g. for rendering a few avatars). The sample is the head
	// of the reactor list in list order (see ReactorLess and
	// SampleFromReactors): the top boosters by amount, then the most recent
	// plain reactors, so a viewer sees who put the most behind the emoji and
	// who reacted most recently. The full reactor list is fetched on demand
	// via GetReactors.
	MaxSampleReactors = 8

	// MaxStoredSampleReactors bounds how many reactor entries a store retains for a
	// reaction's sample. Once the retained set is full, a new entry evicts the
	// least by ReactorLess — a plain reactor while there is one, the oldest
	// first, so a booster is never evicted for a plain reactor and the set
	// keeps the list's head. A store may lean on that: while an emoji has no
	// more boosters than this, the retained set holds every one of them, and
	// the DynamoDB store serves the boosted head of GetReactors from it
	// rather than from an index (checked against a stored booster count, not
	// assumed). It is twice the surfaced size so that reactors
	// leaving (the sample is not backfilled on removal) rarely depletes the
	// retained set below MaxSampleReactors; reads still surface only the first
	// MaxSampleReactors in list order.
	MaxStoredSampleReactors = 2 * MaxSampleReactors
)

// Message is a stored chat message.
//
// ID, UnreadSeq, and EventSequence are server-assigned by the store at PutMessage
// time. ID is a per-chat gapless sequence number that is the message's canonical
// identity, sort key, and pagination cursor. UnreadSeq is a separate per-chat
// running count of unread-eligible messages. EventSequence is the per-chat
// event-log sequence at which the message reached its current state; while every
// event is a new message it equals ID, and it diverges once edits and deletes
// advance the event log without minting an ID (see messagingpb.Message for the
// full semantics).
type Message struct {
	ChatID        *commonpb.ChatId
	ID            *messagingpb.MessageId
	SenderID      *commonpb.UserId // nil for system messages
	Content       []*messagingpb.Content
	Timestamp     time.Time
	UnreadSeq     uint64
	EventSequence uint64
	LastEditedTs  time.Time // zero until the message is edited; a delete leaves it untouched
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
		ChatID:        &commonpb.ChatId{Value: append([]byte(nil), m.ChatID.Value...)},
		ID:            &messagingpb.MessageId{Value: m.ID.Value},
		SenderID:      senderID,
		Content:       content,
		Timestamp:     m.Timestamp,
		UnreadSeq:     m.UnreadSeq,
		EventSequence: m.EventSequence,
		LastEditedTs:  m.LastEditedTs,
	}
}

// IsReplyable reports whether this message may be the target of a reply. Only
// user-facing messages are replyable; this is a whitelist so that content types
// added later (and non-conversational ones like system messages) are treated as
// non-replyable until explicitly allowed. A widget is server-authored but
// conversational, so it is replyable. Deleted messages remain replyable — the
// tombstone is still a real message in the thread.
func (m *Message) IsReplyable() bool {
	if len(m.Content) == 0 {
		return false
	}
	switch m.Content[0].Type.(type) {
	case *messagingpb.Content_Text,
		*messagingpb.Content_Cash,
		*messagingpb.Content_Media,
		*messagingpb.Content_Reply,
		*messagingpb.Content_Encrypted,
		*messagingpb.Content_Widget,
		*messagingpb.Content_Deleted:
		return true
	default:
		return false
	}
}

// IsReactable reports whether this message may be the target of an emoji
// reaction. Like IsReplyable this is a whitelist, so content types added later
// (and non-conversational ones like system messages) are non-reactable until
// explicitly allowed. A widget is reactable, as it is replyable. A Deleted
// tombstone remains reactable — it is still a real message in the thread.
func (m *Message) IsReactable() bool {
	if len(m.Content) == 0 {
		return false
	}
	switch m.Content[0].Type.(type) {
	case *messagingpb.Content_Text,
		*messagingpb.Content_Cash,
		*messagingpb.Content_Media,
		*messagingpb.Content_Reply,
		*messagingpb.Content_Encrypted,
		*messagingpb.Content_Widget,
		*messagingpb.Content_Deleted:
		return true
	default:
		return false
	}
}

// IsDeletable reports whether this message may be tombstoned via DeleteMessage.
// Like IsReplyable this is a whitelist of user-authored conversational content,
// so content types added later (and non-conversational ones like system messages)
// are non-deletable until explicitly allowed. Cash payment messages are excluded:
// the payment is a settled record and the tombstone path is for ordinary chat
// content. A Deleted tombstone is itself non-deletable; the DeleteMessage RPC
// short-circuits an already-deleted message as an idempotent no-op (see
// IsDeleted) before this check, so it never reaches here.
func (m *Message) IsDeletable() bool {
	if len(m.Content) == 0 {
		return false
	}
	if time.Since(m.Timestamp) > account.DefaultMessageDeleteWindow {
		return false
	}
	switch c := m.Content[0].Type.(type) {
	case *messagingpb.Content_Text,
		*messagingpb.Content_Media,
		*messagingpb.Content_Reply,
		*messagingpb.Content_Encrypted:
		return true
	case *messagingpb.Content_Widget:
		// A poll or a payment request is sent by its creator, so its message
		// is theirs to delete, which leaves the poll or the request as it is.
		// Every other widget is the server's.
		return c.Widget.GetPoll() != nil || c.Widget.GetPaymentRequest() != nil
	default:
		return false
	}
}

// IsDeleted reports whether this message has already been tombstoned — its
// content replaced with a single DeletedContent. A delete targeting an
// already-deleted message is an idempotent no-op (see the DeleteMessage RPC).
func (m *Message) IsDeleted() bool {
	return len(m.Content) > 0 && m.Content[0].GetDeleted() != nil
}

// IsEditable reports whether this message's content may be replaced via
// EditMessage. Like IsDeletable this is a whitelist of user-authored
// conversational content, so content types added later (and non-conversational
// ones like system messages) are non-editable until explicitly allowed. Cash
// payment messages are excluded, as is a Deleted tombstone — both are terminal
// records, not editable chat content; the DeleteMessage tombstone falls through to
// the default here, so editing an already-deleted message is rejected with
// CANNOT_EDIT.
func (m *Message) IsEditable() bool {
	if len(m.Content) == 0 {
		return false
	}
	if time.Since(m.Timestamp) > account.DefaultMessageEditWindow {
		return false
	}
	switch m.Content[0].Type.(type) {
	case *messagingpb.Content_Text,
		*messagingpb.Content_Media,
		*messagingpb.Content_Reply,
		*messagingpb.Content_Encrypted:
		return true
	default:
		return false
	}
}

// Reactor is a single user's reaction to a message, with the time they reacted
// and the money they have put behind it.
//
// Version is the emoji aggregate's version at which this reaction was added —
// the transition that created it (see Reaction.Version). Within one (message,
// emoji) it is unique, so with BoostUsdMicros it makes the reactor list's order
// total (see ReactorLess): boosted reactors first, by what they paid, then
// the rest in reaction order, which is descending Version by definition and
// involves no wall clock. ReactedTs is a display value only; a store assigns
// it from the server clock at the add and never orders by it.
//
// BoostUsdMicros is the total the user has paid to boost this emoji on this
// message, in millionths of a US dollar, summed from each payment's market
// value when it was made (see boost.go). It is 0 for a plain reaction and
// only ever grows: a boosted reaction is permanent (a store refuses to
// remove it, see Store.RemoveReaction), so the total never has to come
// down. A boost advances Version like any transition but does not re-stamp
// the reactor, whose Version stays the add's; it is the total that moves
// them in the order.
type Reactor struct {
	UserID         *commonpb.UserId
	ReactedTs      time.Time
	Version        uint64
	BoostUsdMicros uint64
}

// ToProto projects the reactor onto a messagingpb.Reactor.
func (r *Reactor) ToProto() *messagingpb.Reactor {
	return &messagingpb.Reactor{
		UserId:    &commonpb.UserId{Value: append([]byte(nil), r.UserID.Value...)},
		ReactedTs: timestamppb.New(r.ReactedTs),
		Version:   r.Version,
		BoostUsd:  UsdFromMicros(r.BoostUsdMicros),
	}
}

// Reaction is the aggregate state of a single emoji on a message: how many users
// reacted with it, how much money they have put behind it, a monotonic version
// that advances on every change to it, and a bounded sample of reactors (the
// first in list order, see SampleFromReactors). Self is per-viewer and set by
// the read path for the requesting user; the rest of the aggregate is
// shareable.
//
// Version is state, not a sequence of deltas, in the sense of chat.RosterSummary:
// every real transition on the emoji — a reactor added, removed or boosted —
// moves it by exactly one, an idempotent no-op leaves it alone, and a client
// keeps the greater value. It is per emoji, so a client watermarks each
// (message, emoji) independently. A store keeps it across the emoji emptying
// and being re-added, so a re-add can never look stale.
//
// BoostUsdMicros is the total paid to boost the emoji on the message, in
// millionths of a US dollar: the sum of every reactor's BoostUsdMicros, kept on
// the aggregate so a summary read costs no more than it does today (see
// Store.AddBoost). It only grows, since a boosted reaction cannot be removed,
// and it is the primary key of the summary's order (see ReactionLess). A store
// keeps it, like Version, across the emoji emptying — though a boosted emoji
// never empties, every boosted reactor being pinned.
//
// Self is the viewer's own Reactor entry — the version at which their current
// reaction was added and when — nil when they do not react with the emoji. It
// is their entry in the emoji's reactor order whether or not they still sit in
// the sample — where they rank among the reactors, which ReactionUpdate for the
// emoji was theirs — so a client can render itself among the reactors without a
// second read. Its presence answers "did I react"; its version is not the
// watermark for that toggle: a summary is a snapshot at Version, and every
// transition of the viewer's at or below it is already folded into Self, so
// Version is the value to gate live updates by.
//
// In an AddReaction result Self.Version equals Version. In a summary read the
// two come from separate strongly consistent reads, aggregate first, so
// Self.Version is normally at most Version but can exceed it by the viewer's
// own add landing between the two reads; then Self is the newer truth and the
// aggregate is a transition behind, which the add's ReactionUpdate (or the
// next refresh) reconciles. The rows behind the overlay are written in the
// add's own transaction, so the two never disagree about a completed
// transition. It projects onto EmojiReaction.self_reactor.
type Reaction struct {
	Emoji          string
	Count          uint64
	Version        uint64
	BoostUsdMicros uint64
	Self           *Reactor
	SampleReactors []*Reactor
}

// ToProto projects the aggregate onto a messagingpb.EmojiReaction.
func (r *Reaction) ToProto() *messagingpb.EmojiReaction {
	sample := make([]*messagingpb.Reactor, len(r.SampleReactors))
	for i, reactor := range r.SampleReactors {
		sample[i] = reactor.ToProto()
	}
	out := &messagingpb.EmojiReaction{
		Emoji:          &messagingpb.Emoji{Value: r.Emoji},
		Count:          r.Count,
		SampleReactors: sample,
		Version:        r.Version,
		BoostUsd:       UsdFromMicros(r.BoostUsdMicros),
	}
	if r.Self != nil {
		out.SelfReactor = r.Self.ToProto()
	}
	return out
}

// Boost is one payment made to boost a reaction, as a store records it: the
// permanent record behind the totals on Reaction and Reactor, which are the
// sums of these. A store keeps every boost (see Store.AddBoost); nothing reads
// them back yet, but the record carries what a history would show.
//
// Money is kept two ways. Amount is the payment as made, in its mint's quarks
// and the fiat it was entered in, exact. UsdMicros is its market value in
// millionths of a US dollar when it was made, the one unit every boost shares
// whatever its mint, so it is what the totals are summed and ordered on.
//
// IntentID identifies the payment and is the record's idempotency key: a
// store writes a boost once per intent, so the task that records a payment
// (not built yet; see task.Executor for the cash-message tasks it will sit
// beside) can be delivered more than once without crediting it twice. Version is the emoji aggregate's version the boost produced (the
// Reaction.Version after it), unique among the emoji's boosts, so it orders a
// history the way Reactor.Version orders reactors.
type Boost struct {
	MessageID *messagingpb.MessageId
	Emoji     string
	UserID    *commonpb.UserId
	Amount    *commonpb.CryptoPaymentAmount
	UsdMicros uint64
	IntentID  *commonpb.IntentId
	Ts        time.Time
	Version   uint64
}

// UsdFromMicros projects a total kept in millionths of a US dollar onto the
// dollar amount the wire carries (see messagingpb.EmojiReaction.boost_usd).
// Totals are summed and ordered as integers and only rendered as a float here,
// so two backends never disagree about an order by a rounding error.
func UsdFromMicros(micros uint64) float64 {
	return float64(micros) / 1e6
}

// MicrosFromUsd is the store's unit for a payment's market value: its dollar
// value in millionths, rounded to the nearest whole micro. Anything below
// zero, or not a number, is nothing.
func MicrosFromUsd(usd float64) uint64 {
	if usd <= 0 || math.IsNaN(usd) || math.IsInf(usd, 0) {
		return 0
	}
	return uint64(math.Round(usd * 1e6))
}

// ReactionSummary pairs a message with its non-empty reaction aggregates, the
// unit returned by the batch reaction-summary reads. It projects onto a
// messagingpb.ReactionSummary.
type ReactionSummary struct {
	MessageID *messagingpb.MessageId
	Reactions []*Reaction
}

// ToProto projects onto a messagingpb.ReactionSummary, with the aggregates in
// wire order (see ReactionLess): most money first, then most reacted, ties by
// emoji. The stores each return a deterministic order of their own (by emoji),
// but the order a client sees is decided here, at the one projection every
// summary read goes through, so it never depends on the backend. The model's
// slice is left as it was.
func (s *ReactionSummary) ToProto() *messagingpb.ReactionSummary {
	ordered := make([]*Reaction, len(s.Reactions))
	copy(ordered, s.Reactions)
	sort.SliceStable(ordered, func(i, j int) bool { return ReactionLess(ordered[i], ordered[j]) })
	reactions := make([]*messagingpb.EmojiReaction, len(ordered))
	for i, r := range ordered {
		reactions[i] = r.ToProto()
	}
	return &messagingpb.ReactionSummary{
		MessageId: &messagingpb.MessageId{Value: s.MessageID.Value},
		Reactions: reactions,
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
		EventSequence: m.EventSequence,
	}
	if m.SenderID != nil {
		out.SenderId = &commonpb.UserId{Value: append([]byte(nil), m.SenderID.Value...)}
	}
	if !m.LastEditedTs.IsZero() {
		out.LastEditedTs = timestamppb.New(m.LastEditedTs)
	}
	return out
}

// EventType is the kind of mutation an event-log entry records, mirroring the
// messagingpb.Mutation oneof. It is stored on each event row so the log can be read
// or filtered by what happened (e.g. deletions only) without joining to the
// message. The zero value is EventTypeMessageSent, so a create is the default.
type EventType uint8

const (
	EventTypeMessageSent    EventType = iota // a message was created (a send)
	EventTypeMessageEdited                   // a message's content was edited
	EventTypeMessageDeleted                  // a message was tombstoned (a delete)
)

// NewMessageSentEvent builds the event-log entry for a freshly sent message: a
// single Event carrying one message_sent mutation. While every event is a new
// message, the event's sequence is the message's event_sequence (which equals
// its message ID) and its count is 1 — a send consumes exactly one event-log
// point. msg is referenced, not copied; callers pass a proto they own.
func NewMessageSentEvent(msg *messagingpb.Message) *messagingpb.Event {
	return &messagingpb.Event{
		Sequence: msg.EventSequence,
		Count:    1,
		Ts:       msg.Ts,
		Mutations: []*messagingpb.Mutation{{
			Type: &messagingpb.Mutation_MessageSent{MessageSent: msg},
		}},
	}
}

// NewMessageDeletedEvent builds the event-log entry for a tombstoned message: a
// single Event carrying one message_deleted mutation. The event's sequence is the
// message's (now advanced) event_sequence and its count is 1 — a delete consumes
// exactly one event-log point, advancing the event-log head without minting a
// message ID. The mutation carries the materialized tombstone (content replaced
// with DeletedContent). msg is referenced, not copied; callers pass a proto they
// own.
func NewMessageDeletedEvent(msg *messagingpb.Message) *messagingpb.Event {
	return &messagingpb.Event{
		Sequence: msg.EventSequence,
		Count:    1,
		Ts:       msg.Content[0].GetDeleted().DeletedTs,
		Mutations: []*messagingpb.Mutation{{
			Type: &messagingpb.Mutation_MessageDeleted{MessageDeleted: msg},
		}},
	}
}

// NewMessageEditedEvent builds the event-log entry for an edited message: a single
// Event carrying one message_edited mutation. The event's sequence is the message's
// (now advanced) event_sequence and its count is 1 — an edit consumes exactly one
// event-log point, advancing the event-log head without minting a message ID. The
// mutation carries the materialized message (content replaced, last_edited_ts set),
// and the event's ts is that edit time. msg is referenced, not copied; callers pass
// a proto they own.
func NewMessageEditedEvent(msg *messagingpb.Message) *messagingpb.Event {
	return &messagingpb.Event{
		Sequence: msg.EventSequence,
		Count:    1,
		Ts:       msg.LastEditedTs,
		Mutations: []*messagingpb.Mutation{{
			Type: &messagingpb.Mutation_MessageEdited{MessageEdited: msg},
		}},
	}
}

// SampleFromReactors orders reactors in list order (see ReactorLess) and
// returns the first MaxSampleReactors — the deterministic sample surfaced on a
// reaction aggregate even when a store retains up to MaxStoredSampleReactors:
// the top boosters, then the most recent plain reactors. The ordering matches
// GetReactors, and a store's retained set is evicted by the same order from
// the bottom (the least-boosted, then oldest, entry goes first), so the sample
// always holds the list's head. It mutates the given slice's order; callers
// pass a slice they own.
func SampleFromReactors(reactors []*Reactor) []*Reactor {
	sort.Slice(reactors, func(i, j int) bool { return ReactorLess(reactors[i], reactors[j]) })
	if len(reactors) > MaxSampleReactors {
		reactors = reactors[:MaxSampleReactors]
	}
	return reactors
}

// ReactorLess is the order of an emoji's reactors: most boosted first
// (descending BoostUsdMicros, so every boosted reactor precedes every plain
// one), then most-recent-first (descending Version, the order in which they
// reacted). Versions are unique within an emoji, so the tie-break on ascending
// user ID only ever decides between reactors of different emoji (or malformed
// input) and exists to keep the order total. Shared by the stores, the sample
// and the paging cursor so every reactor list agrees. A boost moves its
// reactor up this order without changing their Version, so a client holding
// a list re-slots a BOOSTED actor by their new total.
func ReactorLess(a, b *Reactor) bool {
	if a.BoostUsdMicros != b.BoostUsdMicros {
		return a.BoostUsdMicros > b.BoostUsdMicros
	}
	if a.Version != b.Version {
		return a.Version > b.Version
	}
	return bytes.Compare(a.UserID.Value, b.UserID.Value) < 0
}

// ReactionLess is the wire order of a message's emoji aggregates: most money
// first (descending BoostUsdMicros), then most reacted (descending Count),
// ties broken by the emoji itself (ascending byte order of its UTF-8 encoding,
// which is code point order). Emoji are unique within a message, so the
// tie-break makes the order total: two summaries of the same state always list
// the same sequence, whatever store or read produced them, and no clock or
// version takes part. Versions are deliberately not a key — they are per
// emoji, so they say nothing about one emoji against another. Counts and
// totals move as users react and boost, so a client that wants a row to hold
// still between refreshes must keep its own order; this is the server's
// contract for a fresh render.
func ReactionLess(a, b *Reaction) bool {
	if a.BoostUsdMicros != b.BoostUsdMicros {
		return a.BoostUsdMicros > b.BoostUsdMicros
	}
	if a.Count != b.Count {
		return a.Count > b.Count
	}
	return a.Emoji < b.Emoji
}
