package memory

import (
	"bytes"
	"context"
	"sort"
	"strconv"
	"sync"
	"time"

	"google.golang.org/protobuf/proto"

	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"
	messagingpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/messaging/v1"

	"github.com/code-payments/flipcash2-server/database"
	"github.com/code-payments/flipcash2-server/messaging"
)

type chatState struct {
	lastSeq    uint64
	lastUnread uint64
	messages   map[uint64]*messaging.Message
	byClient   map[string]uint64               // client message ID -> seq
	pointers   map[string]*messagingpb.Pointer // pointerKey -> pointer
}

func newChatState() *chatState {
	return &chatState{
		messages: make(map[uint64]*messaging.Message),
		byClient: make(map[string]uint64),
		pointers: make(map[string]*messagingpb.Pointer),
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
) (*messaging.Message, error) {
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
		return cs.messages[seq].Clone(), nil
	}

	seq := cs.lastSeq + 1
	unreadSeq := cs.lastUnread
	if countsTowardUnread {
		unreadSeq++
	}

	clonedContent := make([]*messagingpb.Content, len(content))
	for i, c := range content {
		clonedContent[i] = proto.Clone(c).(*messagingpb.Content)
	}
	msg := &messaging.Message{
		ChatID:    &commonpb.ChatId{Value: append([]byte(nil), chatID.Value...)},
		ID:        &messagingpb.MessageId{Value: seq},
		SenderID:  senderID,
		Content:   clonedContent,
		Timestamp: ts,
		UnreadSeq: unreadSeq,
	}

	cs.messages[seq] = msg.Clone()
	cs.byClient[string(clientMessageID.Value)] = seq
	cs.lastSeq = seq
	cs.lastUnread = unreadSeq

	return msg.Clone(), nil
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
) (bool, error) {
	m.Lock()
	defer m.Unlock()

	cs := m.chats[string(chatID.Value)]
	if cs == nil {
		return false, messaging.ErrMessageNotFound
	}
	if _, ok := cs.messages[newValue.Value]; !ok {
		return false, messaging.ErrMessageNotFound
	}

	key := pointerKey(pointerType, userID)
	if cur, ok := cs.pointers[key]; ok && newValue.Value <= cur.Value.Value {
		return false, nil
	}
	cs.pointers[key] = &messagingpb.Pointer{
		Type:   pointerType,
		UserId: &commonpb.UserId{Value: append([]byte(nil), userID.Value...)},
		Value:  &messagingpb.MessageId{Value: newValue.Value},
	}
	return true, nil
}

func pointerKey(pointerType messagingpb.Pointer_Type, userID *commonpb.UserId) string {
	return strconv.Itoa(int(pointerType)) + "#" + string(userID.Value)
}
