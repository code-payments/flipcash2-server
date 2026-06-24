package messaging

import (
	"bytes"
	"sort"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"
	messagingpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/messaging/v1"
)

// ClientMessageIDSize is the length, in bytes, of a client message ID.
const ClientMessageIDSize = 16

// Reaction-aggregate bounds
const (
	// MaxReactionTypesPerMessage caps how many distinct emoji may react to a
	// single message. The (N+1)th distinct emoji is rejected with
	// TOO_MANY_REACTION_TYPES; a re-add of an already-present emoji is unaffected.
	MaxReactionTypesPerMessage = 100

	// MaxSampleReactors bounds the sample of reactors surfaced inline on an
	// EmojiReaction (e.g. for rendering a few avatars). The sample is the most
	// recent reactors by reaction time (see SampleFromReactors), so a viewer sees
	// who reacted most recently. The full reactor list is fetched on demand via
	// GetReactors.
	MaxSampleReactors = 8

	// MaxStoredSampleReactors bounds how many reactor entries a store retains for a
	// reaction's sample. Once the retained set is full, a new reactor evicts the
	// least-recent entry, keeping it the most-recent reactors. It is twice the
	// surfaced size so that reactors leaving (the sample is not backfilled on
	// removal) rarely depletes the retained set below MaxSampleReactors; reads still
	// surface only the most-recent MaxSampleReactors.
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
	}
}

// IsReplyable reports whether this message may be the target of a reply. Only
// user-facing messages are replyable; this is a whitelist so that content types
// added later (and non-conversational ones like system messages) are treated as
// non-replyable until explicitly allowed. Deleted messages remain replyable —
// the tombstone is still a real message in the thread.
func (m *Message) IsReplyable() bool {
	if len(m.Content) == 0 {
		return false
	}
	switch m.Content[0].Type.(type) {
	case *messagingpb.Content_Text,
		*messagingpb.Content_Cash,
		*messagingpb.Content_Media,
		*messagingpb.Content_Reply,
		*messagingpb.Content_Deleted:
		return true
	default:
		return false
	}
}

// IsReactable reports whether this message may be the target of an emoji
// reaction. Like IsReplyable this is a whitelist, so content types added later
// (and non-conversational ones like system messages) are non-reactable until
// explicitly allowed. A Deleted tombstone remains reactable — it is still a real
// message in the thread.
func (m *Message) IsReactable() bool {
	if len(m.Content) == 0 {
		return false
	}
	switch m.Content[0].Type.(type) {
	case *messagingpb.Content_Text,
		*messagingpb.Content_Cash,
		*messagingpb.Content_Media,
		*messagingpb.Content_Reply,
		*messagingpb.Content_Deleted:
		return true
	default:
		return false
	}
}

// Reactor is a single user's reaction to a message, with the time they reacted.
type Reactor struct {
	UserID    *commonpb.UserId
	ReactedTs time.Time
}

// ToProto projects the reactor onto a messagingpb.Reactor.
func (r *Reactor) ToProto() *messagingpb.Reactor {
	return &messagingpb.Reactor{
		UserId:    &commonpb.UserId{Value: append([]byte(nil), r.UserID.Value...)},
		ReactedTs: timestamppb.New(r.ReactedTs),
	}
}

// Reaction is the aggregate state of a single emoji on a message: how many users
// reacted with it, a monotonic version that advances on every change to it, and a
// bounded sample of reactors (the most recent by reaction time, see
// SampleFromReactors). ReactedBySelf is per-viewer and set by the read path for
// the requesting user; the rest of the aggregate is shareable.
type Reaction struct {
	Emoji          string
	Count          uint64
	Sequence       uint64
	ReactedBySelf  bool
	SampleReactors []*Reactor
}

// ToProto projects the aggregate onto a messagingpb.EmojiReaction.
func (r *Reaction) ToProto() *messagingpb.EmojiReaction {
	sample := make([]*messagingpb.Reactor, len(r.SampleReactors))
	for i, reactor := range r.SampleReactors {
		sample[i] = reactor.ToProto()
	}
	return &messagingpb.EmojiReaction{
		Emoji:          &messagingpb.Emoji{Value: r.Emoji},
		Count:          r.Count,
		ReactedBySelf:  r.ReactedBySelf,
		SampleReactors: sample,
		Sequence:       r.Sequence,
	}
}

// ReactionSummary pairs a message with its non-empty reaction aggregates, the
// unit returned by the batch reaction-summary reads. It projects onto a
// messagingpb.ReactionSummary.
type ReactionSummary struct {
	MessageID *messagingpb.MessageId
	Reactions []*Reaction
}

// ToProto projects onto a messagingpb.ReactionSummary.
func (s *ReactionSummary) ToProto() *messagingpb.ReactionSummary {
	reactions := make([]*messagingpb.EmojiReaction, len(s.Reactions))
	for i, r := range s.Reactions {
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

// SampleFromReactors orders reactors by descending reaction time (ties broken by
// ascending user ID, for a total and stable order) and returns the first
// MaxSampleReactors — the deterministic, most-recent sample surfaced on a reaction
// aggregate even when a store retains up to MaxStoredSampleReactors. The ordering
// matches the most-recent-first order of GetReactors. It mutates the given slice's
// order; callers pass a slice they own.
func SampleFromReactors(reactors []*Reactor) []*Reactor {
	sort.Slice(reactors, func(i, j int) bool {
		if !reactors[i].ReactedTs.Equal(reactors[j].ReactedTs) {
			return reactors[i].ReactedTs.After(reactors[j].ReactedTs)
		}
		return bytes.Compare(reactors[i].UserID.Value, reactors[j].UserID.Value) < 0
	})
	if len(reactors) > MaxSampleReactors {
		reactors = reactors[:MaxSampleReactors]
	}
	return reactors
}
