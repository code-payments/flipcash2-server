package tests

import (
	"context"
	"crypto/rand"
	"sort"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"
	messagingpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/messaging/v1"

	"github.com/code-payments/flipcash2-server/database"
	"github.com/code-payments/flipcash2-server/messaging"
	"github.com/code-payments/flipcash2-server/model"
)

// RunStoreTests runs the shared messaging.Store test suite against s. teardown
// is called between tests to reset the store.
func RunStoreTests(t *testing.T, s messaging.Store, teardown func()) {
	for _, tf := range []func(t *testing.T, s messaging.Store){
		testStore_PutMessage_AssignsGaplessIDs,
		testStore_PutMessage_Idempotent,
		testStore_PutMessage_PerChatIsolation,
		testStore_PutMessage_ConcurrentDistinct,
		testStore_PutMessage_ConcurrentIdempotent,
		testStore_PutMessage_UnreadSeq,
		testStore_PutMessage_SystemMessage,
		testStore_GetMessage_NotFound,
		testStore_GetMessages_OrderAndPaging,
		testStore_GetMessagesByRefs,
		testStore_Pointers,
		testStore_GetPointersForChats,
		testStore_AdvancePointer_MessageNotFound,
	} {
		tf(t, s)
		teardown()
	}
}

func testStore_PutMessage_AssignsGaplessIDs(t *testing.T, s messaging.Store) {
	ctx := context.Background()
	chatID := generateChatID()
	sender := model.MustGenerateUserID()

	for i := uint64(1); i <= 5; i++ {
		msg, err := s.PutMessage(ctx, chatID, sender, textContent("m"), at(int64(i)), generateClientID(), true)
		require.NoError(t, err)
		require.Equal(t, i, msg.ID.Value)
		require.True(t, msg.Timestamp.Equal(at(int64(i))))
	}
}

func testStore_PutMessage_Idempotent(t *testing.T, s messaging.Store) {
	ctx := context.Background()
	chatID := generateChatID()
	sender := model.MustGenerateUserID()
	clientID := generateClientID()

	first, err := s.PutMessage(ctx, chatID, sender, textContent("hello"), at(1), clientID, true)
	require.NoError(t, err)

	// Replaying the same client message ID returns the original message,
	// without advancing the sequence.
	again, err := s.PutMessage(ctx, chatID, sender, textContent("hello"), at(2), clientID, true)
	require.NoError(t, err)
	require.Equal(t, first.ID.Value, again.ID.Value)
	require.True(t, again.Timestamp.Equal(first.Timestamp))

	// A different client message ID advances to the next ID.
	next, err := s.PutMessage(ctx, chatID, sender, textContent("world"), at(3), generateClientID(), true)
	require.NoError(t, err)
	require.Equal(t, first.ID.Value+1, next.ID.Value)
}

func testStore_PutMessage_PerChatIsolation(t *testing.T, s messaging.Store) {
	ctx := context.Background()
	chatA := generateChatID()
	chatB := generateChatID()
	sender := model.MustGenerateUserID()
	clientID := generateClientID() // deliberately reused across chats

	// Each chat owns an independent ID sequence, so the first message in each
	// chat is ID 1 even though they share a client message ID.
	a, err := s.PutMessage(ctx, chatA, sender, textContent("a"), at(1), clientID, true)
	require.NoError(t, err)
	require.Equal(t, uint64(1), a.ID.Value)

	// The same client message ID in a different chat is NOT a replay; it gets
	// its own ID 1 and its own content. Idempotency is scoped per chat.
	b, err := s.PutMessage(ctx, chatB, sender, textContent("b"), at(1), clientID, true)
	require.NoError(t, err)
	require.Equal(t, uint64(1), b.ID.Value)

	gotA, err := s.GetMessage(ctx, chatA, a.ID)
	require.NoError(t, err)
	require.Equal(t, "a", messageText(gotA))
	gotB, err := s.GetMessage(ctx, chatB, b.ID)
	require.NoError(t, err)
	require.Equal(t, "b", messageText(gotB))

	// Replaying the client message ID within chatA still returns the original
	// message and content, not the new payload.
	replay, err := s.PutMessage(ctx, chatA, sender, textContent("a2"), at(2), clientID, true)
	require.NoError(t, err)
	require.Equal(t, a.ID.Value, replay.ID.Value)
	require.Equal(t, "a", messageText(replay))
}

func testStore_PutMessage_ConcurrentDistinct(t *testing.T, s messaging.Store) {
	ctx := context.Background()
	chatID := generateChatID()
	sender := model.MustGenerateUserID()

	// Concurrent sends with distinct client message IDs must each receive a
	// distinct, gapless ID. This exercises the store's contention handling (a
	// mutex for memory, an optimistic-lock transaction with retries for DynamoDB).
	const n = 25
	var wg sync.WaitGroup
	got := make([]uint64, n)
	errs := make([]error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			msg, err := s.PutMessage(ctx, chatID, sender, textContent("m"), at(int64(i+1)), generateClientID(), true)
			if err != nil {
				errs[i] = err
				return
			}
			got[i] = msg.ID.Value
		}(i)
	}
	wg.Wait()

	for _, err := range errs {
		require.NoError(t, err)
	}

	// The IDs returned to callers are exactly the gapless set 1..n.
	sort.Slice(got, func(a, b int) bool { return got[a] < got[b] })
	want := make([]uint64, n)
	for i := 0; i < n; i++ {
		want[i] = uint64(i + 1)
	}
	require.Equal(t, want, got)

	// The persisted IDs match the IDs handed back: exactly n messages, 1..n.
	all, err := s.GetMessages(ctx, chatID, database.WithAscending())
	require.NoError(t, err)
	require.Equal(t, want, messageIDs(all))
	require.Equal(t, uint64(n), all[len(all)-1].UnreadSeq) // all counted toward unread
}

func testStore_PutMessage_ConcurrentIdempotent(t *testing.T, s messaging.Store) {
	ctx := context.Background()
	chatID := generateChatID()
	sender := model.MustGenerateUserID()
	clientID := generateClientID() // shared by every goroutine

	// Concurrent sends that share a client message ID must collapse to a single
	// message: idempotency holds even when the racing sends interleave.
	const n = 25
	var wg sync.WaitGroup
	got := make([]uint64, n)
	errs := make([]error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			msg, err := s.PutMessage(ctx, chatID, sender, textContent("dup"), at(1), clientID, true)
			if err != nil {
				errs[i] = err
				return
			}
			got[i] = msg.ID.Value
		}(i)
	}
	wg.Wait()

	for _, err := range errs {
		require.NoError(t, err)
	}

	// Every send returned the same message ID, and it is the chat's first (1).
	for i := 0; i < n; i++ {
		require.Equal(t, uint64(1), got[i])
	}

	// The sequence advanced exactly once, and the persisted message carries the
	// ID that was returned to callers.
	all, err := s.GetMessages(ctx, chatID, database.WithAscending())
	require.NoError(t, err)
	require.Equal(t, []uint64{1}, messageIDs(all))
	require.Equal(t, got[0], all[0].ID.Value)
}

func testStore_PutMessage_UnreadSeq(t *testing.T, s messaging.Store) {
	ctx := context.Background()
	chatID := generateChatID()
	sender := model.MustGenerateUserID()

	// counts, counts, not-counts, counts → unread_seq: 1, 2, 2, 3.
	m1, err := s.PutMessage(ctx, chatID, sender, textContent("a"), at(1), generateClientID(), true)
	require.NoError(t, err)
	require.Equal(t, uint64(1), m1.UnreadSeq)

	m2, err := s.PutMessage(ctx, chatID, sender, textContent("b"), at(2), generateClientID(), true)
	require.NoError(t, err)
	require.Equal(t, uint64(2), m2.UnreadSeq)

	m3, err := s.PutMessage(ctx, chatID, sender, textContent("c"), at(3), generateClientID(), false)
	require.NoError(t, err)
	require.Equal(t, uint64(2), m3.UnreadSeq)
	require.Equal(t, m2.ID.Value+1, m3.ID.Value) // ID still advances

	m4, err := s.PutMessage(ctx, chatID, sender, textContent("d"), at(4), generateClientID(), true)
	require.NoError(t, err)
	require.Equal(t, uint64(3), m4.UnreadSeq)
}

func testStore_PutMessage_SystemMessage(t *testing.T, s messaging.Store) {
	ctx := context.Background()
	chatID := generateChatID()

	msg, err := s.PutMessage(ctx, chatID, nil, textContent("system"), at(1), generateClientID(), false)
	require.NoError(t, err)
	require.Nil(t, msg.SenderID)

	got, err := s.GetMessage(ctx, chatID, msg.ID)
	require.NoError(t, err)
	require.Nil(t, got.SenderID)
}

func testStore_GetMessage_NotFound(t *testing.T, s messaging.Store) {
	ctx := context.Background()

	// Unknown chat.
	_, err := s.GetMessage(ctx, generateChatID(), &messagingpb.MessageId{Value: 1})
	require.ErrorIs(t, err, messaging.ErrMessageNotFound)

	// Known chat, unknown message.
	chatID := generateChatID()
	_, err = s.PutMessage(ctx, chatID, model.MustGenerateUserID(), textContent("a"), at(1), generateClientID(), true)
	require.NoError(t, err)
	_, err = s.GetMessage(ctx, chatID, &messagingpb.MessageId{Value: 999})
	require.ErrorIs(t, err, messaging.ErrMessageNotFound)
}

func testStore_GetMessages_OrderAndPaging(t *testing.T, s messaging.Store) {
	ctx := context.Background()
	chatID := generateChatID()
	sender := model.MustGenerateUserID()

	for i := 1; i <= 5; i++ {
		_, err := s.PutMessage(ctx, chatID, sender, textContent("m"), at(int64(i)), generateClientID(), true)
		require.NoError(t, err)
	}

	// Ascending, full set.
	asc, err := s.GetMessages(ctx, chatID, database.WithAscending())
	require.NoError(t, err)
	require.Equal(t, []uint64{1, 2, 3, 4, 5}, messageIDs(asc))

	// Descending, full set.
	desc, err := s.GetMessages(ctx, chatID, database.WithDescending())
	require.NoError(t, err)
	require.Equal(t, []uint64{5, 4, 3, 2, 1}, messageIDs(desc))

	// Ascending, page 1 (limit 2) then page 2 via token.
	page1, err := s.GetMessages(ctx, chatID, database.WithAscending(), database.WithLimit(2))
	require.NoError(t, err)
	require.Equal(t, []uint64{1, 2}, messageIDs(page1))

	token := messaging.PageTokenFromID(page1[len(page1)-1].ID)
	page2, err := s.GetMessages(ctx, chatID, database.WithAscending(), database.WithLimit(2), database.WithPagingToken(token))
	require.NoError(t, err)
	require.Equal(t, []uint64{3, 4}, messageIDs(page2))

	// Descending paging.
	dpage1, err := s.GetMessages(ctx, chatID, database.WithDescending(), database.WithLimit(2))
	require.NoError(t, err)
	require.Equal(t, []uint64{5, 4}, messageIDs(dpage1))
	dtoken := messaging.PageTokenFromID(dpage1[len(dpage1)-1].ID)
	dpage2, err := s.GetMessages(ctx, chatID, database.WithDescending(), database.WithLimit(2), database.WithPagingToken(dtoken))
	require.NoError(t, err)
	require.Equal(t, []uint64{3, 2}, messageIDs(dpage2))

	// Unknown chat → empty.
	empty, err := s.GetMessages(ctx, generateChatID(), database.WithAscending())
	require.NoError(t, err)
	require.Empty(t, empty)
}

func testStore_GetMessagesByRefs(t *testing.T, s messaging.Store) {
	ctx := context.Background()
	chatA := generateChatID()
	chatB := generateChatID()
	sender := model.MustGenerateUserID()

	for i := 1; i <= 5; i++ {
		_, err := s.PutMessage(ctx, chatA, sender, textContent("a"), at(int64(i)), generateClientID(), true)
		require.NoError(t, err)
	}
	for i := 1; i <= 3; i++ {
		_, err := s.PutMessage(ctx, chatB, sender, textContent("b"), at(int64(i)), generateClientID(), true)
		require.NoError(t, err)
	}

	// Single chat: existing only, deduped, ascending by ID.
	got, err := s.GetMessagesByRefs(ctx, refs(chatA, 4, 2, 99, 2))
	require.NoError(t, err)
	require.Equal(t, []uint64{2, 4}, messageIDs(got))

	// Cross-chat batch: one message from each of two chats comes back in one
	// call, each carrying its owning chat ID.
	mixed, err := s.GetMessagesByRefs(ctx, []messaging.MessageRef{
		{ChatID: chatA, MessageID: &messagingpb.MessageId{Value: 5}},
		{ChatID: chatB, MessageID: &messagingpb.MessageId{Value: 2}},
		{ChatID: chatB, MessageID: &messagingpb.MessageId{Value: 99}}, // missing → omitted
	})
	require.NoError(t, err)
	require.Len(t, mixed, 2)
	byChat := make(map[string]*messaging.Message)
	for _, m := range mixed {
		byChat[string(m.ChatID.Value)] = m
	}
	require.Equal(t, uint64(5), byChat[string(chatA.Value)].ID.Value)
	require.Equal(t, uint64(2), byChat[string(chatB.Value)].ID.Value)

	empty, err := s.GetMessagesByRefs(ctx, refs(chatA, 100, 200))
	require.NoError(t, err)
	require.Empty(t, empty)

	none, err := s.GetMessagesByRefs(ctx, nil)
	require.NoError(t, err)
	require.Empty(t, none)
}

func testStore_GetPointersForChats(t *testing.T, s messaging.Store) {
	ctx := context.Background()
	chatA := generateChatID()
	chatB := generateChatID()
	chatC := generateChatID() // no pointers
	userA := model.MustGenerateUserID()
	userB := model.MustGenerateUserID()

	for _, c := range []*commonpb.ChatId{chatA, chatB, chatC} {
		_, err := s.PutMessage(ctx, c, userA, textContent("m"), at(1), generateClientID(), true)
		require.NoError(t, err)
	}

	_, err := s.AdvancePointer(ctx, chatA, userA, messagingpb.Pointer_READ, &messagingpb.MessageId{Value: 1})
	require.NoError(t, err)
	_, err = s.AdvancePointer(ctx, chatB, userB, messagingpb.Pointer_DELIVERED, &messagingpb.MessageId{Value: 1})
	require.NoError(t, err)

	// Batch across chats: A and B return the named members' pointers; C (no
	// pointers) is absent from the map.
	members := []*commonpb.UserId{userA, userB}
	got, err := s.GetPointersForChats(ctx, []messaging.PointerRef{
		{ChatID: chatA, Members: members},
		{ChatID: chatB, Members: members},
		{ChatID: chatC, Members: members},
	})
	require.NoError(t, err)
	require.Len(t, got, 2)
	require.Len(t, got[string(chatA.Value)], 1)
	require.Equal(t, messagingpb.Pointer_READ, got[string(chatA.Value)][0].Type)
	require.Len(t, got[string(chatB.Value)], 1)
	require.Equal(t, messagingpb.Pointer_DELIVERED, got[string(chatB.Value)][0].Type)
	_, ok := got[string(chatC.Value)]
	require.False(t, ok)

	empty, err := s.GetPointersForChats(ctx, nil)
	require.NoError(t, err)
	require.Empty(t, empty)
}

func testStore_Pointers(t *testing.T, s messaging.Store) {
	ctx := context.Background()
	chatID := generateChatID()
	userA := model.MustGenerateUserID()
	userB := model.MustGenerateUserID()

	for i := 1; i <= 5; i++ {
		_, err := s.PutMessage(ctx, chatID, userA, textContent("m"), at(int64(i)), generateClientID(), true)
		require.NoError(t, err)
	}

	// No pointers initially.
	pointers, err := s.GetPointers(ctx, chatID)
	require.NoError(t, err)
	require.Empty(t, pointers)

	// Advance userB's DELIVERED to 3.
	advanced, err := s.AdvancePointer(ctx, chatID, userB, messagingpb.Pointer_DELIVERED, &messagingpb.MessageId{Value: 3})
	require.NoError(t, err)
	require.True(t, advanced)

	// Moving backward is a no-op.
	advanced, err = s.AdvancePointer(ctx, chatID, userB, messagingpb.Pointer_DELIVERED, &messagingpb.MessageId{Value: 2})
	require.NoError(t, err)
	require.False(t, advanced)

	// Moving to the same value is a no-op.
	advanced, err = s.AdvancePointer(ctx, chatID, userB, messagingpb.Pointer_DELIVERED, &messagingpb.MessageId{Value: 3})
	require.NoError(t, err)
	require.False(t, advanced)

	// Forward advances.
	advanced, err = s.AdvancePointer(ctx, chatID, userB, messagingpb.Pointer_DELIVERED, &messagingpb.MessageId{Value: 5})
	require.NoError(t, err)
	require.True(t, advanced)

	// READ is a distinct pointer type; advance it for userB and a pointer for userA.
	advanced, err = s.AdvancePointer(ctx, chatID, userB, messagingpb.Pointer_READ, &messagingpb.MessageId{Value: 4})
	require.NoError(t, err)
	require.True(t, advanced)
	advanced, err = s.AdvancePointer(ctx, chatID, userA, messagingpb.Pointer_READ, &messagingpb.MessageId{Value: 5})
	require.NoError(t, err)
	require.True(t, advanced)

	pointers, err = s.GetPointers(ctx, chatID)
	require.NoError(t, err)
	require.Len(t, pointers, 3)
	require.ElementsMatch(t,
		[]string{
			pointerKey(messagingpb.Pointer_DELIVERED, userB, 5),
			pointerKey(messagingpb.Pointer_READ, userB, 4),
			pointerKey(messagingpb.Pointer_READ, userA, 5),
		},
		[]string{
			pointerKeyOf(pointers[0]),
			pointerKeyOf(pointers[1]),
			pointerKeyOf(pointers[2]),
		},
	)
}

func testStore_AdvancePointer_MessageNotFound(t *testing.T, s messaging.Store) {
	ctx := context.Background()
	user := model.MustGenerateUserID()

	// Unknown chat (no messages) → ErrMessageNotFound.
	_, err := s.AdvancePointer(ctx, generateChatID(), user, messagingpb.Pointer_READ, &messagingpb.MessageId{Value: 1})
	require.ErrorIs(t, err, messaging.ErrMessageNotFound)

	// Known chat, pointer past the last message → ErrMessageNotFound.
	chatID := generateChatID()
	_, err = s.PutMessage(ctx, chatID, user, textContent("a"), at(1), generateClientID(), true)
	require.NoError(t, err)
	_, err = s.AdvancePointer(ctx, chatID, user, messagingpb.Pointer_READ, &messagingpb.MessageId{Value: 2})
	require.ErrorIs(t, err, messaging.ErrMessageNotFound)
}

func textContent(text string) []*messagingpb.Content {
	return []*messagingpb.Content{{
		Type: &messagingpb.Content_Text{
			Text: &messagingpb.TextContent{Text: text},
		},
	}}
}

func messageText(m *messaging.Message) string {
	return m.Content[0].GetText().Text
}

func ids(vals ...uint64) []*messagingpb.MessageId {
	out := make([]*messagingpb.MessageId, len(vals))
	for i, v := range vals {
		out[i] = &messagingpb.MessageId{Value: v}
	}
	return out
}

func refs(chatID *commonpb.ChatId, vals ...uint64) []messaging.MessageRef {
	out := make([]messaging.MessageRef, len(vals))
	for i, v := range vals {
		out[i] = messaging.MessageRef{ChatID: chatID, MessageID: &messagingpb.MessageId{Value: v}}
	}
	return out
}

func messageIDs(msgs []*messaging.Message) []uint64 {
	out := make([]uint64, len(msgs))
	for i, m := range msgs {
		out[i] = m.ID.Value
	}
	return out
}

func pointerKey(t messagingpb.Pointer_Type, userID *commonpb.UserId, value uint64) string {
	return t.String() + "#" + string(userID.Value) + "#" + strconv.FormatUint(value, 10)
}

func pointerKeyOf(p *messagingpb.Pointer) string {
	return pointerKey(p.Type, p.UserId, p.Value.Value)
}

// at returns a deterministic timestamp offset by the given number of seconds
// from a fixed epoch, in UTC.
func at(seconds int64) time.Time {
	return time.Unix(1_700_000_000+seconds, 0).UTC()
}

func generateChatID() *commonpb.ChatId {
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		panic(err)
	}
	return &commonpb.ChatId{Value: b}
}

func generateClientID() *messagingpb.ClientMessageId {
	b := make([]byte, messaging.ClientMessageIDSize)
	if _, err := rand.Read(b); err != nil {
		panic(err)
	}
	return &messagingpb.ClientMessageId{Value: b}
}
