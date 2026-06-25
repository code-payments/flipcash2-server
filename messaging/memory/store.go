package memory

import (
	"bytes"
	"context"
	"sort"
	"strconv"
	"sync"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"
	messagingpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/messaging/v1"

	"github.com/code-payments/flipcash2-server/database"
	"github.com/code-payments/flipcash2-server/messaging"
)

// eventLogEntry is a thin event-log record: which message changed, how, and when.
// The message body is not stored; GetEventDelta joins to the current message state.
type eventLogEntry struct {
	eventSeq  uint64
	messageID uint64
	eventType messaging.EventType
	ts        time.Time
}

type chatState struct {
	lastSeq      uint64
	lastUnread   uint64
	lastEventSeq uint64 // event-log head; tracked independently of lastSeq so edits/deletes can advance it without minting a message ID
	messages     map[uint64]*messaging.Message
	events       []eventLogEntry                    // append-only event log: one descriptor per event, ascending by event_sequence
	byClient     map[string]uint64                  // client message ID -> seq
	pointers     map[string]*messagingpb.Pointer    // pointerKey -> pointer
	reactions    map[uint64]map[string]*reactionAgg // message seq -> emoji -> aggregate
}

// reactionAgg is a single emoji's aggregate on a message. The entry is retained
// even after its last reactor leaves (reactors empty) so sequence stays monotonic
// across an emoji being removed and re-added; an empty aggregate is treated as
// inactive (absent from summaries, not counted toward the per-message type cap).
//
// sample is the bounded subset of reactors retained for the surfaced sample,
// capped at messaging.MaxStoredSampleReactors (a new reactor evicts the
// least-recent entry once full) and never backfilled on removal — it mirrors the
// DynamoDB store's sample map so both back ends behave identically.
type reactionAgg struct {
	sequence uint64
	reactors map[string]time.Time // string(userID.Value) -> reacted timestamp
	sample   map[string]time.Time // bounded subset of reactors, string(userID.Value) -> ts
}

func newChatState() *chatState {
	return &chatState{
		messages:  make(map[uint64]*messaging.Message),
		byClient:  make(map[string]uint64),
		pointers:  make(map[string]*messagingpb.Pointer),
		reactions: make(map[uint64]map[string]*reactionAgg),
	}
}

type memory struct {
	sync.Mutex

	chats map[string]*chatState // keyed by chat ID
}

// NewInMemory returns an in-memory messaging.Store, for tests.
func NewInMemory() messaging.Store {
	return &memory{
		chats: make(map[string]*chatState),
	}
}

func (m *memory) reset() {
	m.Lock()
	defer m.Unlock()

	m.chats = make(map[string]*chatState)
}

func (m *memory) PutMessage(
	_ context.Context,
	chatID *commonpb.ChatId,
	senderID *commonpb.UserId,
	content []*messagingpb.Content,
	ts time.Time,
	clientMessageID *messagingpb.ClientMessageId,
	countsTowardUnread bool,
) (*messaging.Message, bool, error) {
	m.Lock()
	defer m.Unlock()

	cs := m.chats[string(chatID.Value)]
	if cs == nil {
		cs = newChatState()
		m.chats[string(chatID.Value)] = cs
	}

	// Idempotency: a retried send with the same client message ID returns the
	// originally persisted message.
	if seq, ok := cs.byClient[string(clientMessageID.Value)]; ok {
		return cs.messages[seq].Clone(), false, nil
	}

	seq := cs.lastSeq + 1
	unreadSeq := cs.lastUnread
	if countsTowardUnread {
		unreadSeq++
	}
	// The event-log head is tracked independently. While every event is a new
	// message it advances in lockstep with the message ID (so eventSeq == seq);
	// once a delete advances the head without minting an ID the two diverge, and a
	// later send takes the next head, which is then greater than its own ID.
	eventSeq := cs.lastEventSeq + 1

	clonedContent := make([]*messagingpb.Content, len(content))
	for i, c := range content {
		clonedContent[i] = proto.Clone(c).(*messagingpb.Content)
	}
	msg := &messaging.Message{
		ChatID:        &commonpb.ChatId{Value: append([]byte(nil), chatID.Value...)},
		ID:            &messagingpb.MessageId{Value: seq},
		SenderID:      senderID,
		Content:       clonedContent,
		Timestamp:     ts,
		UnreadSeq:     unreadSeq,
		EventSequence: eventSeq,
	}

	cs.messages[seq] = msg.Clone()
	// Append a thin descriptor of the send to the event log (the event-ordered read
	// source; see GetEventDelta). Every event is a new message here, so its event_seq
	// is the message's seq; edits and deletes will append further events without
	// minting a message ID.
	cs.events = append(cs.events, eventLogEntry{
		eventSeq:  seq,
		messageID: seq,
		eventType: messaging.EventTypeMessageSent,
		ts:        ts,
	})
	cs.byClient[string(clientMessageID.Value)] = seq
	cs.lastSeq = seq
	cs.lastUnread = unreadSeq
	cs.lastEventSeq = eventSeq

	return msg.Clone(), true, nil
}

func (m *memory) GetLatestEventSequence(_ context.Context, chatID *commonpb.ChatId) (uint64, error) {
	m.Lock()
	defer m.Unlock()

	cs := m.chats[string(chatID.Value)]
	if cs == nil {
		return 0, nil
	}
	return cs.lastEventSeq, nil
}

func (m *memory) GetLatestEventSequencesForChats(_ context.Context, chatIDs []*commonpb.ChatId) (map[string]uint64, error) {
	m.Lock()
	defer m.Unlock()

	out := make(map[string]uint64)
	for _, chatID := range chatIDs {
		cs := m.chats[string(chatID.Value)]
		if cs == nil || cs.lastEventSeq == 0 {
			continue
		}
		out[string(chatID.Value)] = cs.lastEventSeq
	}
	return out, nil
}

func (m *memory) GetMessage(_ context.Context, chatID *commonpb.ChatId, messageID *messagingpb.MessageId) (*messaging.Message, error) {
	m.Lock()
	defer m.Unlock()

	cs := m.chats[string(chatID.Value)]
	if cs == nil {
		return nil, messaging.ErrMessageNotFound
	}
	msg, ok := cs.messages[messageID.Value]
	if !ok {
		return nil, messaging.ErrMessageNotFound
	}
	return msg.Clone(), nil
}

func (m *memory) EditMessage(
	_ context.Context,
	chatID *commonpb.ChatId,
	messageID *messagingpb.MessageId,
	content []*messagingpb.Content,
	editedTs time.Time,
	expectedEventSeq uint64,
) (*messaging.Message, error) {
	m.Lock()
	defer m.Unlock()

	cs := m.chats[string(chatID.Value)]
	if cs == nil {
		return nil, messaging.ErrMessageNotFound
	}
	msg, ok := cs.messages[messageID.Value]
	if !ok {
		return nil, messaging.ErrMessageNotFound
	}

	// Optimistic guard: reject a mutation based on a stale version, returning the
	// current state rather than clobbering it.
	if msg.EventSequence != expectedEventSeq {
		return msg.Clone(), messaging.ErrEventSequenceConflict
	}

	// Advance the event-log head without minting a message ID, replace the content
	// and stamp the edit time, re-stamp the message to the new head (its
	// current-state token), and append an edit event to the log so the catch-up read
	// (GetEventDelta) surfaces it.
	cs.lastEventSeq++
	clonedContent := make([]*messagingpb.Content, len(content))
	for i, c := range content {
		clonedContent[i] = proto.Clone(c).(*messagingpb.Content)
	}
	msg.Content = clonedContent
	msg.LastEditedTs = editedTs
	msg.EventSequence = cs.lastEventSeq
	cs.events = append(cs.events, eventLogEntry{
		eventSeq:  cs.lastEventSeq,
		messageID: msg.ID.Value,
		eventType: messaging.EventTypeMessageEdited,
		ts:        editedTs,
	})

	return msg.Clone(), nil
}

func (m *memory) DeleteMessage(
	_ context.Context,
	chatID *commonpb.ChatId,
	messageID *messagingpb.MessageId,
	deletedBy *commonpb.UserId,
	deletedTs time.Time,
	expectedEventSeq uint64,
) (*messaging.Message, error) {
	m.Lock()
	defer m.Unlock()

	cs := m.chats[string(chatID.Value)]
	if cs == nil {
		return nil, messaging.ErrMessageNotFound
	}
	msg, ok := cs.messages[messageID.Value]
	if !ok {
		return nil, messaging.ErrMessageNotFound
	}

	// Optimistic guard: reject a mutation based on a stale version, returning the
	// current state rather than clobbering it.
	if msg.EventSequence != expectedEventSeq {
		return msg.Clone(), messaging.ErrEventSequenceConflict
	}

	// Advance the event-log head without minting a message ID, re-stamp the
	// tombstone to it (its current-state token), and append a delete event to the
	// log so the catch-up read (GetEventDelta) surfaces it.
	cs.lastEventSeq++
	deleted := &messagingpb.DeletedContent{DeletedTs: timestamppb.New(deletedTs)}
	if deletedBy != nil {
		deleted.DeletedBy = &commonpb.UserId{Value: append([]byte(nil), deletedBy.Value...)}
	}
	msg.Content = []*messagingpb.Content{{Type: &messagingpb.Content_Deleted{Deleted: deleted}}}
	msg.EventSequence = cs.lastEventSeq
	cs.events = append(cs.events, eventLogEntry{
		eventSeq:  cs.lastEventSeq,
		messageID: msg.ID.Value,
		eventType: messaging.EventTypeMessageDeleted,
		ts:        deletedTs,
	})

	return msg.Clone(), nil
}

func (m *memory) MessageExists(_ context.Context, chatID *commonpb.ChatId, messageID *messagingpb.MessageId) (bool, error) {
	m.Lock()
	defer m.Unlock()

	cs := m.chats[string(chatID.Value)]
	if cs == nil {
		return false, nil
	}
	_, ok := cs.messages[messageID.Value]
	return ok, nil
}

func (m *memory) GetMessages(_ context.Context, chatID *commonpb.ChatId, opts ...database.QueryOption) ([]*messaging.Message, error) {
	q := database.ApplyQueryOptions(opts...)

	m.Lock()
	defer m.Unlock()

	cs := m.chats[string(chatID.Value)]
	if cs == nil {
		return nil, nil
	}

	ordered := make([]*messaging.Message, 0, len(cs.messages))
	for _, msg := range cs.messages {
		ordered = append(ordered, msg.Clone())
	}
	sort.Slice(ordered, func(i, j int) bool {
		return ordered[i].ID.Value < ordered[j].ID.Value
	})
	if q.Order == commonpb.QueryOptions_DESC {
		for i, j := 0, len(ordered)-1; i < j; i, j = i+1, j-1 {
			ordered[i], ordered[j] = ordered[j], ordered[i]
		}
	}

	// Resume strictly after the cursor message ID, in the requested order.
	if cursor, ok := messaging.IDFromPageToken(q.PagingToken); ok {
		filtered := ordered[:0]
		for _, msg := range ordered {
			if q.Order == commonpb.QueryOptions_DESC {
				if msg.ID.Value < cursor {
					filtered = append(filtered, msg)
				}
			} else if msg.ID.Value > cursor {
				filtered = append(filtered, msg)
			}
		}
		ordered = filtered
	}

	if q.Limit > 0 && len(ordered) > q.Limit {
		ordered = ordered[:q.Limit]
	}
	return ordered, nil
}

func (m *memory) GetEventDelta(_ context.Context, chatID *commonpb.ChatId, afterEventSeq, headEventSeq uint64, limit int) ([]*messaging.Message, uint64, error) {
	if limit <= 0 {
		limit = database.DefaultQueryOptions().Limit
	}
	if afterEventSeq >= headEventSeq {
		return nil, afterEventSeq, nil
	}

	m.Lock()
	defer m.Unlock()

	cs := m.chats[string(chatID.Value)]
	if cs == nil {
		return nil, afterEventSeq, nil
	}

	// The event log is append-only and ascending by event_sequence. Scan up to limit
	// events in (after, head], joining each to the message's current state and
	// dropping a superseded event (the message's current event_sequence is past this
	// event, so a newer event carries the up-to-date state). nextCursor advances over
	// every event scanned, survivor or not.
	nextCursor := afterEventSeq
	var msgs []*messaging.Message
	scanned := 0
	for _, ev := range cs.events {
		if ev.eventSeq <= afterEventSeq {
			continue
		}
		if ev.eventSeq > headEventSeq {
			break
		}
		nextCursor = ev.eventSeq
		if cur, ok := cs.messages[ev.messageID]; ok && cur.EventSequence <= ev.eventSeq {
			msgs = append(msgs, cur.Clone())
		}
		if scanned++; scanned == limit {
			break
		}
	}
	return msgs, nextCursor, nil
}

func (m *memory) GetMessagesByRefs(_ context.Context, refs []messaging.MessageRef) ([]*messaging.Message, error) {
	m.Lock()
	defer m.Unlock()

	type dedupKey struct {
		chat string
		id   uint64
	}
	seen := make(map[dedupKey]struct{}, len(refs))
	var out []*messaging.Message
	for _, ref := range refs {
		k := dedupKey{chat: string(ref.ChatID.Value), id: ref.MessageID.Value}
		if _, dup := seen[k]; dup {
			continue
		}
		seen[k] = struct{}{}
		cs := m.chats[k.chat]
		if cs == nil {
			continue
		}
		if msg, ok := cs.messages[ref.MessageID.Value]; ok {
			out = append(out, msg.Clone())
		}
	}
	// Order by (chatID, message ID): deterministic, and ascending by ID within a
	// single chat to match the single-chat batch contract.
	sort.Slice(out, func(i, j int) bool {
		if c := bytes.Compare(out[i].ChatID.Value, out[j].ChatID.Value); c != 0 {
			return c < 0
		}
		return out[i].ID.Value < out[j].ID.Value
	})
	return out, nil
}

func (m *memory) GetPointersForChats(_ context.Context, refs []messaging.PointerRef) (map[string][]*messagingpb.Pointer, error) {
	m.Lock()
	defer m.Unlock()

	// Mirror the DynamoDB store: return each named member's stored pointers,
	// addressed by exact key rather than returning the whole chat.
	out := make(map[string][]*messagingpb.Pointer)
	seen := make(map[string]struct{})
	for _, ref := range refs {
		chatKey := string(ref.ChatID.Value)
		cs := m.chats[chatKey]
		if cs == nil {
			continue
		}
		for _, member := range ref.Members {
			dedup := chatKey + "\x00" + string(member.Value)
			if _, dup := seen[dedup]; dup {
				continue
			}
			seen[dedup] = struct{}{}
			for _, t := range messaging.StoredPointerTypes {
				if p, ok := cs.pointers[pointerKey(t, member)]; ok {
					out[chatKey] = append(out[chatKey], proto.Clone(p).(*messagingpb.Pointer))
				}
			}
		}
	}
	return out, nil
}

func (m *memory) GetPointers(_ context.Context, chatID *commonpb.ChatId) ([]*messagingpb.Pointer, error) {
	m.Lock()
	defer m.Unlock()

	cs := m.chats[string(chatID.Value)]
	if cs == nil {
		return nil, nil
	}
	out := make([]*messagingpb.Pointer, 0, len(cs.pointers))
	for _, p := range cs.pointers {
		out = append(out, proto.Clone(p).(*messagingpb.Pointer))
	}
	return out, nil
}

func (m *memory) AdvancePointer(
	_ context.Context,
	chatID *commonpb.ChatId,
	userID *commonpb.UserId,
	pointerType messagingpb.Pointer_Type,
	newValue *messagingpb.MessageId,
) (*messagingpb.Pointer, bool, error) {
	m.Lock()
	defer m.Unlock()

	cs := m.chats[string(chatID.Value)]
	if cs == nil {
		// newValue's existence is the caller's responsibility; a missing chat is
		// treated as not-advanced rather than a panic.
		return nil, false, nil
	}

	key := pointerKey(pointerType, userID)
	if cur, ok := cs.pointers[key]; ok && newValue.Value <= cur.Value.Value {
		return cur, false, nil
	}
	pointer := &messagingpb.Pointer{
		Type:   pointerType,
		UserId: &commonpb.UserId{Value: append([]byte(nil), userID.Value...)},
		Value:  &messagingpb.MessageId{Value: newValue.Value},
		Ts:     timestamppb.New(time.Now()),
	}
	cs.pointers[key] = pointer
	return pointer, true, nil
}

func pointerKey(pointerType messagingpb.Pointer_Type, userID *commonpb.UserId) string {
	return strconv.Itoa(int(pointerType)) + "#" + string(userID.Value)
}

func (m *memory) AddReaction(
	_ context.Context,
	chatID *commonpb.ChatId,
	messageID *messagingpb.MessageId,
	userID *commonpb.UserId,
	emoji string,
	ts time.Time,
) (*messaging.Reaction, bool, bool, error) {
	m.Lock()
	defer m.Unlock()

	cs := m.chats[string(chatID.Value)]
	if cs == nil {
		cs = newChatState()
		m.chats[string(chatID.Value)] = cs
	}

	byEmoji := cs.reactions[messageID.Value]
	if byEmoji == nil {
		byEmoji = make(map[string]*reactionAgg)
		cs.reactions[messageID.Value] = byEmoji
	}

	userKey := string(userID.Value)
	agg := byEmoji[emoji]

	// Idempotent: the user already reacted with this emoji.
	if agg != nil {
		if _, ok := agg.reactors[userKey]; ok {
			return buildReaction(emoji, agg), false, false, nil
		}
	}

	// Activating a (new or previously-emptied) emoji on this message must respect
	// the per-message distinct-type cap; re-adding never trips it.
	if agg == nil || len(agg.reactors) == 0 {
		if countActiveReactions(byEmoji) >= messaging.MaxReactionTypesPerMessage {
			return nil, false, true, nil
		}
	}

	if agg == nil {
		agg = &reactionAgg{reactors: make(map[string]time.Time), sample: make(map[string]time.Time)}
		byEmoji[emoji] = agg
	}
	agg.sequence++
	agg.reactors[userKey] = ts
	// Maintain the recent sample: insert this reactor and, if that pushes the stored
	// set over its cap, evict the least-recent entry. This keeps the sample the
	// most-recent MaxStoredSampleReactors (reads surface the most-recent
	// MaxSampleReactors of it); it is not backfilled on removal.
	agg.sample[userKey] = ts
	if len(agg.sample) > messaging.MaxStoredSampleReactors {
		evictLeastRecentSample(agg.sample)
	}

	return buildReaction(emoji, agg), true, false, nil
}

func (m *memory) RemoveReaction(
	_ context.Context,
	chatID *commonpb.ChatId,
	messageID *messagingpb.MessageId,
	userID *commonpb.UserId,
	emoji string,
) (*messaging.Reaction, bool, error) {
	m.Lock()
	defer m.Unlock()

	cs := m.chats[string(chatID.Value)]
	if cs == nil {
		return nil, false, nil
	}
	agg := cs.reactions[messageID.Value][emoji]
	userKey := string(userID.Value)

	// No aggregate at all: a pure no-op with nothing to report.
	if agg == nil {
		return nil, false, nil
	}
	// Idempotent: the user didn't react. Reflect the current state.
	if _, ok := agg.reactors[userKey]; !ok {
		return buildReaction(emoji, agg), false, nil
	}

	delete(agg.reactors, userKey)
	delete(agg.sample, userKey) // no backfill: the sample may shrink below the surfaced size
	agg.sequence++

	// The aggregate is retained (preserving sequence) even when no reactors remain;
	// buildReaction reports Count 0 and the advanced sequence.
	return buildReaction(emoji, agg), true, nil
}

func (m *memory) GetReactionSummary(
	_ context.Context,
	chatID *commonpb.ChatId,
	messageID *messagingpb.MessageId,
) ([]*messaging.Reaction, error) {
	m.Lock()
	defer m.Unlock()

	cs := m.chats[string(chatID.Value)]
	if cs == nil {
		return nil, nil
	}
	return summarize(cs.reactions[messageID.Value]), nil
}

func (m *memory) GetReactionSummariesByRefs(
	_ context.Context,
	chatID *commonpb.ChatId,
	messageIDs []*messagingpb.MessageId,
) ([]*messaging.ReactionSummary, error) {
	m.Lock()
	defer m.Unlock()

	cs := m.chats[string(chatID.Value)]

	seen := make(map[uint64]struct{}, len(messageIDs))
	out := make([]*messaging.ReactionSummary, 0, len(messageIDs))
	for _, id := range messageIDs {
		if _, dup := seen[id.Value]; dup {
			continue
		}
		seen[id.Value] = struct{}{}
		// Every requested message is echoed; one with no reactions (or unknown)
		// comes back with an empty summary rather than being omitted.
		var reactions []*messaging.Reaction
		if cs != nil {
			reactions = summarize(cs.reactions[id.Value])
		}
		out = append(out, &messaging.ReactionSummary{
			MessageID: &messagingpb.MessageId{Value: id.Value},
			Reactions: reactions,
		})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].MessageID.Value < out[j].MessageID.Value })
	return out, nil
}

func (m *memory) GetReactionSummaries(
	_ context.Context,
	chatID *commonpb.ChatId,
	opts ...database.QueryOption,
) ([]*messaging.ReactionSummary, error) {
	q := database.ApplyQueryOptions(opts...)

	m.Lock()
	defer m.Unlock()

	cs := m.chats[string(chatID.Value)]
	if cs == nil {
		return nil, nil
	}

	// Page over the chat's messages (not just reacted ones), so a message with no
	// reactions is returned with an empty summary rather than skipped.
	seqs := make([]uint64, 0, len(cs.messages))
	for seq := range cs.messages {
		seqs = append(seqs, seq)
	}
	sort.Slice(seqs, func(i, j int) bool { return seqs[i] < seqs[j] })
	if q.Order == commonpb.QueryOptions_DESC {
		for i, j := 0, len(seqs)-1; i < j; i, j = i+1, j-1 {
			seqs[i], seqs[j] = seqs[j], seqs[i]
		}
	}

	// Resume strictly after the cursor message ID, in the requested order.
	if cursor, ok := messaging.IDFromPageToken(q.PagingToken); ok {
		filtered := seqs[:0]
		for _, seq := range seqs {
			if q.Order == commonpb.QueryOptions_DESC {
				if seq < cursor {
					filtered = append(filtered, seq)
				}
			} else if seq > cursor {
				filtered = append(filtered, seq)
			}
		}
		seqs = filtered
	}

	if q.Limit > 0 && len(seqs) > q.Limit {
		seqs = seqs[:q.Limit]
	}

	out := make([]*messaging.ReactionSummary, 0, len(seqs))
	for _, seq := range seqs {
		out = append(out, &messaging.ReactionSummary{
			MessageID: &messagingpb.MessageId{Value: seq},
			Reactions: summarize(cs.reactions[seq]),
		})
	}
	return out, nil
}

func (m *memory) GetSelfReactions(
	_ context.Context,
	chatID *commonpb.ChatId,
	userID *commonpb.UserId,
	refs []messaging.ReactionRef,
) ([]messaging.ReactionRef, error) {
	m.Lock()
	defer m.Unlock()

	cs := m.chats[string(chatID.Value)]
	if cs == nil {
		return nil, nil
	}

	userKey := string(userID.Value)
	var present []messaging.ReactionRef
	for _, ref := range refs {
		agg := cs.reactions[ref.MessageID.Value][ref.Emoji]
		if agg == nil {
			continue
		}
		if _, ok := agg.reactors[userKey]; ok {
			present = append(present, ref)
		}
	}
	return present, nil
}

func (m *memory) GetReactors(
	_ context.Context,
	chatID *commonpb.ChatId,
	messageID *messagingpb.MessageId,
	emoji string,
	_ bool, // always consistent; the flag only matters for eventually consistent backends
	opts ...database.QueryOption,
) ([]*messaging.Reactor, bool, error) {
	q := database.ApplyQueryOptions(opts...)

	m.Lock()
	defer m.Unlock()

	cs := m.chats[string(chatID.Value)]
	if cs == nil {
		return nil, false, nil
	}
	agg := cs.reactions[messageID.Value][emoji]
	if agg == nil || len(agg.reactors) == 0 {
		return nil, false, nil
	}

	reactors := reactorsOf(agg)

	// Resume strictly after the cursor reactor, in most-recent-first order.
	if ts, userID, ok := messaging.ReactorFromPageToken(q.PagingToken); ok {
		cursor := &messaging.Reactor{UserID: userID, ReactedTs: ts}
		filtered := reactors[:0]
		for _, r := range reactors {
			if reactorLess(cursor, r) {
				filtered = append(filtered, r)
			}
		}
		reactors = filtered
	}

	limit := q.Limit
	if limit <= 0 {
		limit = len(reactors)
	}
	hasMore := len(reactors) > limit
	if hasMore {
		reactors = reactors[:limit]
	}
	return reactors, hasMore, nil
}

// buildReaction projects an in-memory aggregate onto a messaging.Reaction. The
// surfaced sample is the most-recent MaxSampleReactors of the retained sample set
// (see messaging.SampleFromReactors). The per-viewer ReactedBySelf is left false
// for the server to overlay.
func buildReaction(emoji string, agg *reactionAgg) *messaging.Reaction {
	sample := make([]*messaging.Reactor, 0, len(agg.sample))
	for userKey, ts := range agg.sample {
		sample = append(sample, &messaging.Reactor{
			UserID:    &commonpb.UserId{Value: []byte(userKey)},
			ReactedTs: ts,
		})
	}
	return &messaging.Reaction{
		Emoji:          emoji,
		Count:          uint64(len(agg.reactors)),
		Sequence:       agg.sequence,
		SampleReactors: messaging.SampleFromReactors(sample),
	}
}

// evictLeastRecentSample removes the single least-recent entry from a sample map
// (earliest ts; ties broken by larger user key), restoring it to
// MaxStoredSampleReactors after an over-cap insert. The tie-break matches
// messaging.SampleFromReactors, whose user-ID order on the raw-byte keys is the
// same as the lexicographic order used here.
func evictLeastRecentSample(sample map[string]time.Time) {
	var evictKey string
	var evictTs time.Time
	first := true
	for k, t := range sample {
		if first || t.Before(evictTs) || (t.Equal(evictTs) && k > evictKey) {
			evictKey, evictTs, first = k, t, false
		}
	}
	if !first {
		delete(sample, evictKey)
	}
}

// summarize returns the active emoji aggregates for one message, ordered by
// emoji for determinism. ReactedBySelf is left false for the caller to overlay.
func summarize(byEmoji map[string]*reactionAgg) []*messaging.Reaction {
	var emojis []string
	for emoji, agg := range byEmoji {
		if len(agg.reactors) > 0 {
			emojis = append(emojis, emoji)
		}
	}
	sort.Strings(emojis)
	out := make([]*messaging.Reaction, 0, len(emojis))
	for _, emoji := range emojis {
		out = append(out, buildReaction(emoji, byEmoji[emoji]))
	}
	return out
}

// reactorsOf returns an aggregate's reactors most-recent-first (ties broken by
// user ID), the canonical order for samples and GetReactors paging.
func reactorsOf(agg *reactionAgg) []*messaging.Reactor {
	out := make([]*messaging.Reactor, 0, len(agg.reactors))
	for userKey, ts := range agg.reactors {
		out = append(out, &messaging.Reactor{
			UserID:    &commonpb.UserId{Value: []byte(userKey)},
			ReactedTs: ts,
		})
	}
	sort.Slice(out, func(i, j int) bool { return reactorLess(out[i], out[j]) })
	return out
}

// reactorLess orders reactors most-recent-first, breaking ties by ascending user
// ID so the ordering is total and stable for pagination.
func reactorLess(a, b *messaging.Reactor) bool {
	if !a.ReactedTs.Equal(b.ReactedTs) {
		return a.ReactedTs.After(b.ReactedTs)
	}
	return bytes.Compare(a.UserID.Value, b.UserID.Value) < 0
}

func countActiveReactions(byEmoji map[string]*reactionAgg) int {
	n := 0
	for _, agg := range byEmoji {
		if len(agg.reactors) > 0 {
			n++
		}
	}
	return n
}
