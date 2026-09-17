package tests

import (
	"context"
	"crypto/rand"
	"fmt"
	"sort"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	blobpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/blob/v1"
	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"
	messagingpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/messaging/v1"

	"github.com/code-payments/flipcash2-server/chat"
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
		testStore_MessageExists,
		testStore_GetLatestEventSequence,
		testStore_GetLatestEventSequencesForChats,
		testStore_GetMessagesPaging,
		testStore_GetMessagesByRefs,
		testStore_GetEventDelta,
		testStore_EditMessage,
		testStore_DeleteMessage,
		testStore_Pointers,
		testStore_GetPointersForChats,
		testStore_AdvancePointer_NoExistenceCheck,
		testStore_Reactions_AddRemove,
		testStore_Reactions_SelfReactions,
		testStore_Reactions_SummariesByRefs,
		testStore_Reactions_SummariesPaging,
		testStore_Reactions_SampleCap,
		testStore_Reactions_TypeCap,
		testStore_Reactions_TypeCapConcurrent,
		testStore_Reactions_GetReactors,
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
		msg, created, err := s.PutMessage(ctx, chatID, sender, textContent("m"), at(int64(i)), generateClientID(), true)
		require.NoError(t, err)
		require.True(t, created)
		require.Equal(t, i, msg.ID.Value)
		require.Equal(t, msg.ID.Value, msg.EventSequence)
		require.True(t, msg.Timestamp.Equal(at(int64(i))))
	}
}

func testStore_PutMessage_Idempotent(t *testing.T, s messaging.Store) {
	ctx := context.Background()
	chatID := generateChatID()
	sender := model.MustGenerateUserID()
	clientID := generateClientID()

	first, created, err := s.PutMessage(ctx, chatID, sender, textContent("hello"), at(1), clientID, true)
	require.NoError(t, err)
	require.True(t, created)

	// Replaying the same client message ID returns the original message,
	// without advancing the sequence, and reports that nothing was created so
	// callers know to skip one-time side effects (e.g. pushes).
	again, created, err := s.PutMessage(ctx, chatID, sender, textContent("hello"), at(2), clientID, true)
	require.NoError(t, err)
	require.False(t, created)
	require.Equal(t, first.ID.Value, again.ID.Value)
	require.True(t, again.Timestamp.Equal(first.Timestamp))

	// A different client message ID advances to the next ID.
	next, created, err := s.PutMessage(ctx, chatID, sender, textContent("world"), at(3), generateClientID(), true)
	require.NoError(t, err)
	require.True(t, created)
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
	a, created, err := s.PutMessage(ctx, chatA, sender, textContent("a"), at(1), clientID, true)
	require.NoError(t, err)
	require.True(t, created)
	require.Equal(t, uint64(1), a.ID.Value)

	// The same client message ID in a different chat is NOT a replay; it gets
	// its own ID 1 and its own content. Idempotency is scoped per chat.
	b, created, err := s.PutMessage(ctx, chatB, sender, textContent("b"), at(1), clientID, true)
	require.NoError(t, err)
	require.True(t, created)
	require.Equal(t, uint64(1), b.ID.Value)

	gotA, err := s.GetMessage(ctx, chatA, a.ID)
	require.NoError(t, err)
	require.Equal(t, "a", messageText(gotA))
	gotB, err := s.GetMessage(ctx, chatB, b.ID)
	require.NoError(t, err)
	require.Equal(t, "b", messageText(gotB))

	// Replaying the client message ID within chatA still returns the original
	// message and content, not the new payload.
	replay, created, err := s.PutMessage(ctx, chatA, sender, textContent("a2"), at(2), clientID, true)
	require.NoError(t, err)
	require.False(t, created)
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
			msg, _, err := s.PutMessage(ctx, chatID, sender, textContent("m"), at(int64(i+1)), generateClientID(), true)
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
	createds := make([]bool, n)
	errs := make([]error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			msg, created, err := s.PutMessage(ctx, chatID, sender, textContent("dup"), at(1), clientID, true)
			if err != nil {
				errs[i] = err
				return
			}
			got[i] = msg.ID.Value
			createds[i] = created
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

	// Exactly one of the racing sends created the message; the rest were
	// replays, so only one would trigger side effects like pushes.
	var createdCount int
	for _, created := range createds {
		if created {
			createdCount++
		}
	}
	require.Equal(t, 1, createdCount)

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
	m1, _, err := s.PutMessage(ctx, chatID, sender, textContent("a"), at(1), generateClientID(), true)
	require.NoError(t, err)
	require.Equal(t, uint64(1), m1.UnreadSeq)

	m2, _, err := s.PutMessage(ctx, chatID, sender, textContent("b"), at(2), generateClientID(), true)
	require.NoError(t, err)
	require.Equal(t, uint64(2), m2.UnreadSeq)

	m3, _, err := s.PutMessage(ctx, chatID, sender, textContent("c"), at(3), generateClientID(), false)
	require.NoError(t, err)
	require.Equal(t, uint64(2), m3.UnreadSeq)
	require.Equal(t, m2.ID.Value+1, m3.ID.Value) // ID still advances

	m4, _, err := s.PutMessage(ctx, chatID, sender, textContent("d"), at(4), generateClientID(), true)
	require.NoError(t, err)
	require.Equal(t, uint64(3), m4.UnreadSeq)
}

func testStore_PutMessage_SystemMessage(t *testing.T, s messaging.Store) {
	ctx := context.Background()
	chatID := generateChatID()

	msg, _, err := s.PutMessage(ctx, chatID, nil, textContent("system"), at(1), generateClientID(), false)
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
	_, _, err = s.PutMessage(ctx, chatID, model.MustGenerateUserID(), textContent("a"), at(1), generateClientID(), true)
	require.NoError(t, err)
	_, err = s.GetMessage(ctx, chatID, &messagingpb.MessageId{Value: 999})
	require.ErrorIs(t, err, messaging.ErrMessageNotFound)
}

func testStore_MessageExists(t *testing.T, s messaging.Store) {
	ctx := context.Background()
	chatID := generateChatID()

	// Unknown chat → false, no error.
	exists, err := s.MessageExists(ctx, generateChatID(), &messagingpb.MessageId{Value: 1})
	require.NoError(t, err)
	require.False(t, exists)

	msg, _, err := s.PutMessage(ctx, chatID, model.MustGenerateUserID(), textContent("a"), at(1), generateClientID(), true)
	require.NoError(t, err)

	// Existing message → true.
	exists, err = s.MessageExists(ctx, chatID, msg.ID)
	require.NoError(t, err)
	require.True(t, exists)

	// Known chat, unknown message → false.
	exists, err = s.MessageExists(ctx, chatID, &messagingpb.MessageId{Value: 999})
	require.NoError(t, err)
	require.False(t, exists)
}

func testStore_GetLatestEventSequence(t *testing.T, s messaging.Store) {
	ctx := context.Background()
	chatID := generateChatID()
	sender := model.MustGenerateUserID()

	// An unknown chat has no messages, so its head is 0.
	head, err := s.GetLatestEventSequence(ctx, chatID)
	require.NoError(t, err)
	require.Zero(t, head)

	// Every event is a new message, so the head advances in lockstep with the
	// message ID on each send.
	for i := uint64(1); i <= 3; i++ {
		msg, _, err := s.PutMessage(ctx, chatID, sender, textContent("m"), at(int64(i)), generateClientID(), true)
		require.NoError(t, err)

		got, err := s.GetLatestEventSequence(ctx, chatID)
		require.NoError(t, err)
		require.Equal(t, msg.ID.Value, got)
		require.Equal(t, i, got)
	}

	// An idempotent replay assigns no new ID, so the head does not advance.
	clientID := generateClientID()
	_, created, err := s.PutMessage(ctx, chatID, sender, textContent("dup"), at(10), clientID, true)
	require.NoError(t, err)
	require.True(t, created)
	before, err := s.GetLatestEventSequence(ctx, chatID)
	require.NoError(t, err)

	_, created, err = s.PutMessage(ctx, chatID, sender, textContent("dup"), at(11), clientID, true)
	require.NoError(t, err)
	require.False(t, created)
	after, err := s.GetLatestEventSequence(ctx, chatID)
	require.NoError(t, err)
	require.Equal(t, before, after)
}

func testStore_GetLatestEventSequencesForChats(t *testing.T, s messaging.Store) {
	ctx := context.Background()
	sender := model.MustGenerateUserID()

	// Two chats with messages (distinct heads) and one with none.
	chatA := generateChatID()
	for i := 1; i <= 3; i++ {
		_, _, err := s.PutMessage(ctx, chatA, sender, textContent("m"), at(int64(i)), generateClientID(), true)
		require.NoError(t, err)
	}
	chatB := generateChatID()
	_, _, err := s.PutMessage(ctx, chatB, sender, textContent("m"), at(1), generateClientID(), true)
	require.NoError(t, err)
	empty := generateChatID()

	// Empty input yields an empty map, not an error.
	out, err := s.GetLatestEventSequencesForChats(ctx, nil)
	require.NoError(t, err)
	require.Empty(t, out)

	// Each chat's head is keyed by its raw chat ID; a duplicate request collapses
	// and a chat with no messages (head 0) is omitted so a missing key means 0.
	out, err = s.GetLatestEventSequencesForChats(ctx, []*commonpb.ChatId{chatA, chatB, empty, chatA})
	require.NoError(t, err)
	require.Len(t, out, 2)
	require.Equal(t, uint64(3), out[string(chatA.Value)])
	require.Equal(t, uint64(1), out[string(chatB.Value)])
	_, ok := out[string(empty.Value)]
	require.False(t, ok)

	// Each head matches the single-chat accessor.
	for _, chatID := range []*commonpb.ChatId{chatA, chatB} {
		want, err := s.GetLatestEventSequence(ctx, chatID)
		require.NoError(t, err)
		require.Equal(t, want, out[string(chatID.Value)])
	}
}

func testStore_GetMessagesPaging(t *testing.T, s messaging.Store) {
	ctx := context.Background()
	chatID := generateChatID()
	sender := model.MustGenerateUserID()

	for i := 1; i <= 5; i++ {
		_, _, err := s.PutMessage(ctx, chatID, sender, textContent("m"), at(int64(i)), generateClientID(), true)
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
		_, _, err := s.PutMessage(ctx, chatA, sender, textContent("a"), at(int64(i)), generateClientID(), true)
		require.NoError(t, err)
	}
	for i := 1; i <= 3; i++ {
		_, _, err := s.PutMessage(ctx, chatB, sender, textContent("b"), at(int64(i)), generateClientID(), true)
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

func testStore_GetEventDelta(t *testing.T, s messaging.Store) {
	ctx := context.Background()
	chatID := generateChatID()
	sender := model.MustGenerateUserID()

	// Empty chat: nothing in range; nextCursor is the unchanged cursor.
	empty, next, err := s.GetEventDelta(ctx, chatID, 0, 0, 100)
	require.NoError(t, err)
	require.Empty(t, empty)
	require.Equal(t, uint64(0), next)

	// Seed 5 messages; event_seq == seq == 1..5, so the head is 5.
	for i := uint64(1); i <= 5; i++ {
		_, _, err := s.PutMessage(ctx, chatID, sender, textContent("m"), at(int64(i)), generateClientID(), true)
		require.NoError(t, err)
	}
	head, err := s.GetLatestEventSequence(ctx, chatID)
	require.NoError(t, err)
	require.Equal(t, uint64(5), head)

	// From the beginning: every message at its current state, ascending by
	// event_sequence (which equals its message ID while every event is a send). The
	// cursor advances to the head.
	all, next, err := s.GetEventDelta(ctx, chatID, 0, head, 100)
	require.NoError(t, err)
	require.Equal(t, []uint64{1, 2, 3, 4, 5}, messageIDs(all))
	for _, m := range all {
		require.Equal(t, m.ID.Value, m.EventSequence)
	}
	require.Equal(t, uint64(5), next)

	// From a mid cursor: only events past it.
	tail, next, err := s.GetEventDelta(ctx, chatID, 2, head, 100)
	require.NoError(t, err)
	require.Equal(t, []uint64{3, 4, 5}, messageIDs(tail))
	require.Equal(t, uint64(5), next)

	// Limit bounds how many events are scanned; nextCursor is the last of them, so
	// the next page resumes right after.
	page, next, err := s.GetEventDelta(ctx, chatID, 0, head, 2)
	require.NoError(t, err)
	require.Equal(t, []uint64{1, 2}, messageIDs(page))
	require.Equal(t, uint64(2), next)

	// A cursor at or past the head returns nothing and doesn't move.
	none, next, err := s.GetEventDelta(ctx, chatID, 5, head, 100)
	require.NoError(t, err)
	require.Empty(t, none)
	require.Equal(t, uint64(5), next)

	// limit <= 0 falls back to the default page size (all 5 fit here).
	def, next, err := s.GetEventDelta(ctx, chatID, 0, head, 0)
	require.NoError(t, err)
	require.Equal(t, []uint64{1, 2, 3, 4, 5}, messageIDs(def))
	require.Equal(t, uint64(5), next)
}

func testStore_EditMessage(t *testing.T, s messaging.Store) {
	ctx := context.Background()
	chatID := generateChatID()
	sender := model.MustGenerateUserID()

	// Editing in an unknown chat is a not-found.
	_, err := s.EditMessage(ctx, chatID, &messagingpb.MessageId{Value: 1}, textContent("x"), at(1), 1)
	require.ErrorIs(t, err, messaging.ErrMessageNotFound)

	// Seed 3 messages: IDs and event_seq are 1..3, the head is 3.
	for i := uint64(1); i <= 3; i++ {
		_, _, err := s.PutMessage(ctx, chatID, sender, textContent("m"), at(int64(i)), generateClientID(), true)
		require.NoError(t, err)
	}

	// Unknown message in a known chat is a not-found.
	_, err = s.EditMessage(ctx, chatID, &messagingpb.MessageId{Value: 99}, textContent("x"), at(9), 1)
	require.ErrorIs(t, err, messaging.ErrMessageNotFound)

	// A stale expected event_sequence is a conflict: it returns the current,
	// unmodified state and advances nothing.
	current, err := s.EditMessage(ctx, chatID, &messagingpb.MessageId{Value: 2}, textContent("edited"), at(9), 999)
	require.ErrorIs(t, err, messaging.ErrEventSequenceConflict)
	require.Equal(t, uint64(2), current.ID.Value)
	require.Equal(t, uint64(2), current.EventSequence)
	require.Equal(t, "m", messageText(current)) // still the original text
	require.True(t, current.LastEditedTs.IsZero())
	head, err := s.GetLatestEventSequence(ctx, chatID)
	require.NoError(t, err)
	require.Equal(t, uint64(3), head)

	// Edit message 2 with the matching expected event_sequence.
	editedTs := at(100)
	edited, err := s.EditMessage(ctx, chatID, &messagingpb.MessageId{Value: 2}, textContent("edited"), editedTs, 2)
	require.NoError(t, err)
	require.Equal(t, uint64(2), edited.ID.Value)      // message ID unchanged — the sequence stays gapless
	require.Equal(t, uint64(4), edited.EventSequence) // re-stamped to the new event-log head
	require.Equal(t, "edited", messageText(edited))   // content replaced
	require.True(t, editedTs.Equal(edited.LastEditedTs))

	// The event-log head advanced by one; the message-ID head did NOT (no ID minted).
	head, err = s.GetLatestEventSequence(ctx, chatID)
	require.NoError(t, err)
	require.Equal(t, uint64(4), head)

	// GetMessage now returns the edited content at its advanced event_sequence.
	got, err := s.GetMessage(ctx, chatID, &messagingpb.MessageId{Value: 2})
	require.NoError(t, err)
	require.Equal(t, uint64(4), got.EventSequence)
	require.Equal(t, "edited", messageText(got))
	require.True(t, editedTs.Equal(got.LastEditedTs))

	// The event-log delta returns each message once at its latest event: the
	// untouched 1 and 3, then the edited message re-stamped to the head (4). Message
	// 2's create event is superseded by its edit event and dropped.
	delta, _, err := s.GetEventDelta(ctx, chatID, 0, head, 100)
	require.NoError(t, err)
	require.Equal(t, []uint64{1, 3, 2}, messageIDs(delta))

	// A second edit of the same message must supply its NOW-current event_sequence
	// (4); the original expectation is stale. It advances the head again to 5.
	edited2, err := s.EditMessage(ctx, chatID, &messagingpb.MessageId{Value: 2}, textContent("edited again"), at(150), 4)
	require.NoError(t, err)
	require.Equal(t, uint64(2), edited2.ID.Value)
	require.Equal(t, uint64(5), edited2.EventSequence)
	require.Equal(t, "edited again", messageText(edited2))

	// A later send proves the divergence: it takes the next message ID (gapless: 4)
	// but the next event-log head (6), so its event_sequence now exceeds its ID.
	next, _, err := s.PutMessage(ctx, chatID, sender, textContent("after"), at(200), generateClientID(), true)
	require.NoError(t, err)
	require.Equal(t, uint64(4), next.ID.Value)
	require.Equal(t, uint64(6), next.EventSequence)
}

func testStore_DeleteMessage(t *testing.T, s messaging.Store) {
	ctx := context.Background()
	chatID := generateChatID()
	sender := model.MustGenerateUserID()

	// Deleting in an unknown chat is a not-found.
	_, err := s.DeleteMessage(ctx, chatID, &messagingpb.MessageId{Value: 1}, sender, at(1), 1)
	require.ErrorIs(t, err, messaging.ErrMessageNotFound)

	// Seed 3 messages: IDs and event_seq are 1..3, the head is 3.
	for i := uint64(1); i <= 3; i++ {
		_, _, err := s.PutMessage(ctx, chatID, sender, textContent("m"), at(int64(i)), generateClientID(), true)
		require.NoError(t, err)
	}

	// Unknown message in a known chat is a not-found.
	_, err = s.DeleteMessage(ctx, chatID, &messagingpb.MessageId{Value: 99}, sender, at(9), 1)
	require.ErrorIs(t, err, messaging.ErrMessageNotFound)

	// A stale expected event_sequence is a conflict: it returns the current,
	// unmodified state and advances nothing.
	current, err := s.DeleteMessage(ctx, chatID, &messagingpb.MessageId{Value: 2}, sender, at(9), 999)
	require.ErrorIs(t, err, messaging.ErrEventSequenceConflict)
	require.Equal(t, uint64(2), current.ID.Value)
	require.Equal(t, uint64(2), current.EventSequence)
	require.Equal(t, "m", messageText(current)) // still text, not tombstoned
	head, err := s.GetLatestEventSequence(ctx, chatID)
	require.NoError(t, err)
	require.Equal(t, uint64(3), head)

	// Delete message 2 with the matching expected event_sequence.
	deletedTs := at(100)
	tombstone, err := s.DeleteMessage(ctx, chatID, &messagingpb.MessageId{Value: 2}, sender, deletedTs, 2)
	require.NoError(t, err)
	require.Equal(t, uint64(2), tombstone.ID.Value)      // message ID unchanged — the sequence stays gapless
	require.Equal(t, uint64(4), tombstone.EventSequence) // re-stamped to the new event-log head
	requireDeleted(t, tombstone, sender, deletedTs)

	// The event-log head advanced by one; the message-ID head did NOT (no ID minted).
	head, err = s.GetLatestEventSequence(ctx, chatID)
	require.NoError(t, err)
	require.Equal(t, uint64(4), head)

	// GetMessage now returns the tombstone at its advanced event_sequence.
	got, err := s.GetMessage(ctx, chatID, &messagingpb.MessageId{Value: 2})
	require.NoError(t, err)
	require.Equal(t, uint64(4), got.EventSequence)
	requireDeleted(t, got, sender, deletedTs)

	// The event-log delta returns each message once at its latest event: the
	// untouched 1 and 3, then the tombstone re-stamped to the head (4). Message 2's
	// create event is superseded by its delete event and dropped.
	delta, _, err := s.GetEventDelta(ctx, chatID, 0, head, 100)
	require.NoError(t, err)
	require.Equal(t, []uint64{1, 3, 2}, messageIDs(delta))

	// A later send proves the divergence: it takes the next message ID (gapless: 4)
	// but the next event-log head (5), so its event_sequence now exceeds its ID.
	next, _, err := s.PutMessage(ctx, chatID, sender, textContent("after"), at(200), generateClientID(), true)
	require.NoError(t, err)
	require.Equal(t, uint64(4), next.ID.Value)
	require.Equal(t, uint64(5), next.EventSequence)
}

// requireDeleted asserts a message is a tombstone: its content is a single
// DeletedContent carrying the expected deleter and deletion timestamp.
func requireDeleted(t *testing.T, m *messaging.Message, deletedBy *commonpb.UserId, deletedTs time.Time) {
	t.Helper()
	require.Len(t, m.Content, 1)
	deleted := m.Content[0].GetDeleted()
	require.NotNil(t, deleted)
	require.Equal(t, deletedBy.Value, deleted.DeletedBy.GetValue())
	require.True(t, deletedTs.Equal(deleted.DeletedTs.AsTime()))
}

// chatShapes are the chat ID shapes the pointer tests run against: a store may
// lay pointers out differently per chat type (the DynamoDB store keeps a DM's
// on one item and a group's on one item per member), and the Store contract
// must hold for both.
var chatShapes = []struct {
	name       string
	generateID func() *commonpb.ChatId
}{
	{name: "dm", generateID: generateChatID},
	{name: "group", generateID: chat.MustGenerateGroupChatID},
}

func testStore_GetPointersForChats(t *testing.T, s messaging.Store) {
	ctx := context.Background()
	userA := model.MustGenerateUserID()
	userB := model.MustGenerateUserID()

	// Mixed shapes in one batch: A and C are DMs, B and D are groups.
	chatA := generateChatID()
	chatB := chat.MustGenerateGroupChatID()
	chatC := generateChatID()               // no pointers
	chatD := chat.MustGenerateGroupChatID() // no pointers

	for _, c := range []*commonpb.ChatId{chatA, chatB, chatC, chatD} {
		_, _, err := s.PutMessage(ctx, c, userA, textContent("m"), at(1), generateClientID(), true)
		require.NoError(t, err)
	}

	// userA holds both types in chat A, and userB one; userB holds one in chat B.
	_, _, err := s.AdvancePointer(ctx, chatA, userA, messagingpb.Pointer_DELIVERED, &messagingpb.MessageId{Value: 1})
	require.NoError(t, err)
	_, _, err = s.AdvancePointer(ctx, chatA, userA, messagingpb.Pointer_READ, &messagingpb.MessageId{Value: 1})
	require.NoError(t, err)
	_, _, err = s.AdvancePointer(ctx, chatA, userB, messagingpb.Pointer_READ, &messagingpb.MessageId{Value: 1})
	require.NoError(t, err)
	_, _, err = s.AdvancePointer(ctx, chatB, userB, messagingpb.Pointer_DELIVERED, &messagingpb.MessageId{Value: 1})
	require.NoError(t, err)

	// Batch across chats: A and B return the named members' pointers — every
	// type a member holds — and C and D (no pointers) are absent from the map.
	members := []*commonpb.UserId{userA, userB}
	got, err := s.GetPointersForChats(ctx, []messaging.PointerRef{
		{ChatID: chatA, Members: members},
		{ChatID: chatB, Members: members},
		{ChatID: chatC, Members: members},
		{ChatID: chatD, Members: members},
	})
	require.NoError(t, err)
	require.Len(t, got, 2)
	require.ElementsMatch(t,
		[]string{
			pointerKey(messagingpb.Pointer_DELIVERED, userA, 1),
			pointerKey(messagingpb.Pointer_READ, userA, 1),
			pointerKey(messagingpb.Pointer_READ, userB, 1),
		},
		pointerKeysOf(got[string(chatA.Value)]),
	)
	require.Equal(t,
		[]string{pointerKey(messagingpb.Pointer_DELIVERED, userB, 1)},
		pointerKeysOf(got[string(chatB.Value)]),
	)
	_, ok := got[string(chatC.Value)]
	require.False(t, ok)
	_, ok = got[string(chatD.Value)]
	require.False(t, ok)

	// Only the named members come back, whatever else the chat holds: a ref
	// naming one DM member returns that member's pointers alone.
	got, err = s.GetPointersForChats(ctx, []messaging.PointerRef{
		{ChatID: chatA, Members: []*commonpb.UserId{userB}},
	})
	require.NoError(t, err)
	require.Equal(t,
		[]string{pointerKey(messagingpb.Pointer_READ, userB, 1)},
		pointerKeysOf(got[string(chatA.Value)]),
	)

	// Repeated refs collapse: a chat named twice, and a member named twice,
	// each return their pointers once.
	got, err = s.GetPointersForChats(ctx, []messaging.PointerRef{
		{ChatID: chatA, Members: []*commonpb.UserId{userA, userA}},
		{ChatID: chatA, Members: []*commonpb.UserId{userB}},
		{ChatID: chatB, Members: []*commonpb.UserId{userB}},
		{ChatID: chatB, Members: []*commonpb.UserId{userB}},
	})
	require.NoError(t, err)
	require.Len(t, got[string(chatA.Value)], 3)
	require.Len(t, got[string(chatB.Value)], 1)

	empty, err := s.GetPointersForChats(ctx, nil)
	require.NoError(t, err)
	require.Empty(t, empty)
}

func testStore_Pointers(t *testing.T, s messaging.Store) {
	for _, shape := range chatShapes {
		t.Run(shape.name, func(t *testing.T) {
			testStore_Pointers_Shape(t, s, shape.generateID())
		})
	}
}

func testStore_Pointers_Shape(t *testing.T, s messaging.Store, chatID *commonpb.ChatId) {
	ctx := context.Background()
	userA := model.MustGenerateUserID()
	userB := model.MustGenerateUserID()

	for i := 1; i <= 5; i++ {
		_, _, err := s.PutMessage(ctx, chatID, userA, textContent("m"), at(int64(i)), generateClientID(), true)
		require.NoError(t, err)
	}

	// No pointers initially.
	pointers, err := s.GetPointers(ctx, chatID)
	require.NoError(t, err)
	require.Empty(t, pointers)

	// Advance userB's DELIVERED to 3. The advanced pointer is returned, carrying
	// the value it was moved to and its last-advanced timestamp.
	pointer, advanced, err := s.AdvancePointer(ctx, chatID, userB, messagingpb.Pointer_DELIVERED, &messagingpb.MessageId{Value: 3})
	require.NoError(t, err)
	require.True(t, advanced)
	require.EqualValues(t, 3, pointer.Value.Value)
	require.NotNil(t, pointer.Ts)
	advancedTs := pointer.Ts.AsTime()

	// Moving backward is a no-op, but the current pointer (still at 3) is
	// returned — with the timestamp of the advance that put it there, not the
	// no-op's.
	pointer, advanced, err = s.AdvancePointer(ctx, chatID, userB, messagingpb.Pointer_DELIVERED, &messagingpb.MessageId{Value: 2})
	require.NoError(t, err)
	require.False(t, advanced)
	require.EqualValues(t, 3, pointer.Value.Value)
	require.True(t, pointer.Ts.AsTime().Equal(advancedTs))

	// Moving to the same value is a no-op.
	pointer, advanced, err = s.AdvancePointer(ctx, chatID, userB, messagingpb.Pointer_DELIVERED, &messagingpb.MessageId{Value: 3})
	require.NoError(t, err)
	require.False(t, advanced)
	require.EqualValues(t, 3, pointer.Value.Value)
	require.True(t, pointer.Ts.AsTime().Equal(advancedTs))

	// Forward advances.
	pointer, advanced, err = s.AdvancePointer(ctx, chatID, userB, messagingpb.Pointer_DELIVERED, &messagingpb.MessageId{Value: 5})
	require.NoError(t, err)
	require.True(t, advanced)
	require.EqualValues(t, 5, pointer.Value.Value)

	// READ is a distinct pointer type; advance it for userB and a pointer for userA.
	_, advanced, err = s.AdvancePointer(ctx, chatID, userB, messagingpb.Pointer_READ, &messagingpb.MessageId{Value: 4})
	require.NoError(t, err)
	require.True(t, advanced)
	_, advanced, err = s.AdvancePointer(ctx, chatID, userA, messagingpb.Pointer_READ, &messagingpb.MessageId{Value: 5})
	require.NoError(t, err)
	require.True(t, advanced)

	pointers, err = s.GetPointers(ctx, chatID)
	require.NoError(t, err)
	require.ElementsMatch(t,
		[]string{
			pointerKey(messagingpb.Pointer_DELIVERED, userB, 5),
			pointerKey(messagingpb.Pointer_READ, userB, 4),
			pointerKey(messagingpb.Pointer_READ, userA, 5),
		},
		pointerKeysOf(pointers),
	)

	// Members' pointers are independent: a no-op for one member leaves the
	// other's untouched, and an advance for one member moves only their own.
	_, advanced, err = s.AdvancePointer(ctx, chatID, userA, messagingpb.Pointer_READ, &messagingpb.MessageId{Value: 1})
	require.NoError(t, err)
	require.False(t, advanced)
	_, advanced, err = s.AdvancePointer(ctx, chatID, userB, messagingpb.Pointer_READ, &messagingpb.MessageId{Value: 5})
	require.NoError(t, err)
	require.True(t, advanced)

	pointers, err = s.GetPointers(ctx, chatID)
	require.NoError(t, err)
	require.ElementsMatch(t,
		[]string{
			pointerKey(messagingpb.Pointer_DELIVERED, userB, 5),
			pointerKey(messagingpb.Pointer_READ, userB, 5),
			pointerKey(messagingpb.Pointer_READ, userA, 5),
		},
		pointerKeysOf(pointers),
	)
}

func testStore_AdvancePointer_NoExistenceCheck(t *testing.T, s messaging.Store) {
	for _, shape := range chatShapes {
		t.Run(shape.name, func(t *testing.T) {
			testStore_AdvancePointer_NoExistenceCheck_Shape(t, s, shape.generateID())
		})
	}
}

func testStore_AdvancePointer_NoExistenceCheck_Shape(t *testing.T, s messaging.Store, chatID *commonpb.ChatId) {
	ctx := context.Background()
	user := model.MustGenerateUserID()

	for i := 1; i <= 3; i++ {
		_, _, err := s.PutMessage(ctx, chatID, user, textContent("m"), at(int64(i)), generateClientID(), true)
		require.NoError(t, err)
	}

	// Forward advances. The advanced pointer is returned, carrying its
	// last-advanced timestamp.
	pointer, advanced, err := s.AdvancePointer(ctx, chatID, user, messagingpb.Pointer_READ, &messagingpb.MessageId{Value: 2})
	require.NoError(t, err)
	require.True(t, advanced)
	require.EqualValues(t, 2, pointer.Value.Value)
	require.NotNil(t, pointer.Ts)

	// Backward is a no-op, but the current pointer (still at 2) is returned.
	pointer, advanced, err = s.AdvancePointer(ctx, chatID, user, messagingpb.Pointer_READ, &messagingpb.MessageId{Value: 1})
	require.NoError(t, err)
	require.False(t, advanced)
	require.EqualValues(t, 2, pointer.Value.Value)

	pointers, err := s.GetPointers(ctx, chatID)
	require.NoError(t, err)
	require.Len(t, pointers, 1)
	require.Equal(t, uint64(2), pointers[0].Value.Value)
}

func testStore_Reactions_AddRemove(t *testing.T, s messaging.Store) {
	ctx := context.Background()
	// A group, so the self-reaction read below is in scope (it is group-only).
	chatID := generateGroupChatID()
	userA := model.MustGenerateUserID()
	userB := model.MustGenerateUserID()
	userC := model.MustGenerateUserID()

	msg, _, err := s.PutMessage(ctx, chatID, userA, textContent("react to me"), at(1), generateClientID(), true)
	require.NoError(t, err)
	msgID := msg.ID
	const emoji = "👍"

	// First reactor: count 1, sequence 1, reacted-by-self, sampled.
	r, created, tooMany, err := s.AddReaction(ctx, chatID, msgID, userA, emoji, at(1))
	require.NoError(t, err)
	require.True(t, created)
	require.False(t, tooMany)
	require.Equal(t, uint64(1), r.Count)
	require.Equal(t, uint64(1), r.Version)
	// The add's result is the reactor's own view: reacted, at the version the
	// add produced and when.
	require.True(t, r.ReactedBySelf)
	require.Equal(t, uint64(1), r.ReactedBySelfVersion)
	require.Equal(t, at(1), r.ReactedBySelfTs)
	require.Len(t, r.SampleReactors, 1)
	require.Equal(t, userA.Value, r.SampleReactors[0].UserID.Value)

	// Re-adding the same emoji is an idempotent no-op: nothing advances, and the
	// self view is the original add's, not the retry's time.
	again, created, _, err := s.AddReaction(ctx, chatID, msgID, userA, emoji, at(2))
	require.NoError(t, err)
	require.False(t, created)
	require.Equal(t, uint64(1), again.Count)
	require.Equal(t, uint64(1), again.Version)
	require.True(t, again.ReactedBySelf)
	require.Equal(t, uint64(1), again.ReactedBySelfVersion)
	require.Equal(t, at(1), again.ReactedBySelfTs)

	// Second reactor: count 2, sequence advances, sample ordered most-recent-first.
	r, created, _, err = s.AddReaction(ctx, chatID, msgID, userB, emoji, at(3))
	require.NoError(t, err)
	require.True(t, created)
	require.Equal(t, uint64(2), r.Count)
	require.Equal(t, uint64(2), r.Version)
	require.Len(t, r.SampleReactors, 2)
	require.Equal(t, userB.Value, r.SampleReactors[0].UserID.Value) // most recent first
	require.Equal(t, userA.Value, r.SampleReactors[1].UserID.Value)
	// Each sampled reactor carries the version that added them.
	require.Equal(t, uint64(2), r.SampleReactors[0].Version)
	require.Equal(t, uint64(1), r.SampleReactors[1].Version)
	require.True(t, at(3).Equal(r.SampleReactors[0].ReactedTs))

	// Summary reports the emoji with the shared aggregate; ReactedBySelf is left
	// false for the server to overlay.
	summary, err := s.GetReactionSummary(ctx, chatID, msgID)
	require.NoError(t, err)
	require.Len(t, summary, 1)
	require.Equal(t, emoji, summary[0].Emoji)
	require.Equal(t, uint64(2), summary[0].Count)
	require.False(t, summary[0].ReactedBySelf)

	// Self-reaction lookup is per-user, and reports the version that added it
	// and when.
	present, err := s.GetSelfReactions(ctx, chatID, userA, []*messagingpb.MessageId{msgID})
	require.NoError(t, err)
	require.Len(t, present, 1)
	require.Equal(t, msgID.Value, present[0].MessageID.Value)
	require.Equal(t, emoji, present[0].Emoji)
	require.Equal(t, uint64(1), present[0].Version)
	require.Equal(t, at(1), present[0].ReactedTs)
	present, err = s.GetSelfReactions(ctx, chatID, userC, []*messagingpb.MessageId{msgID})
	require.NoError(t, err)
	require.Empty(t, present)

	// Remove A: count drops to 1, sequence advances.
	r, removed, err := s.RemoveReaction(ctx, chatID, msgID, userA, emoji)
	require.NoError(t, err)
	require.True(t, removed)
	require.Equal(t, uint64(1), r.Count)
	require.Equal(t, uint64(3), r.Version)
	require.Len(t, r.SampleReactors, 1)
	require.Equal(t, userB.Value, r.SampleReactors[0].UserID.Value)

	// Removing A again is an idempotent no-op; B's reaction still stands.
	r, removed, err = s.RemoveReaction(ctx, chatID, msgID, userA, emoji)
	require.NoError(t, err)
	require.False(t, removed)
	require.Equal(t, uint64(1), r.Count)
	require.Equal(t, uint64(3), r.Version)

	// Remove the last reactor: the aggregate reports count 0 but keeps advancing
	// its sequence, and the emoji drops out of the summary.
	r, removed, err = s.RemoveReaction(ctx, chatID, msgID, userB, emoji)
	require.NoError(t, err)
	require.True(t, removed)
	require.NotNil(t, r)
	require.Equal(t, uint64(0), r.Count)
	require.Equal(t, uint64(4), r.Version)
	require.Empty(t, r.SampleReactors)

	summary, err = s.GetReactionSummary(ctx, chatID, msgID)
	require.NoError(t, err)
	require.Empty(t, summary)

	// Re-adding resurrects the emoji but its sequence continues monotonically, so
	// a client never mistakes the new state for stale.
	r, created, _, err = s.AddReaction(ctx, chatID, msgID, userA, emoji, at(10))
	require.NoError(t, err)
	require.True(t, created)
	require.Equal(t, uint64(1), r.Count)
	require.Equal(t, uint64(5), r.Version)
	require.Len(t, r.SampleReactors, 1)
	require.Equal(t, userA.Value, r.SampleReactors[0].UserID.Value)
}

// testStore_Reactions_SelfReactions pins the viewer-addressed overlay read: it
// reports exactly the viewer's current reactions on exactly the requested
// messages of a group, each at the version that added it, across a set of IDs
// wide enough to span more than one query window — and refuses a DM, whose
// overlay is the caller's to answer from the sample.
func testStore_Reactions_SelfReactions(t *testing.T, s messaging.Store) {
	ctx := context.Background()
	chatID := generateGroupChatID()
	viewer := model.MustGenerateUserID()
	other := model.MustGenerateUserID()

	// Messages 1..70: far enough apart that IDs 1 and 70 fall in separate runs of
	// the store's range read (see the DynamoDB store's maxSummaryRefGap).
	var ids []*messagingpb.MessageId
	for i := 1; i <= 70; i++ {
		msg, _, err := s.PutMessage(ctx, chatID, other, textContent("m"), at(int64(i)), generateClientID(), true)
		require.NoError(t, err)
		ids = append(ids, msg.ID)
	}
	// Every add gets its own time, so the overlay's timestamps are checkable
	// per reaction rather than all one value.
	add := func(user *commonpb.UserId, id *messagingpb.MessageId, emoji string, ts time.Time) *messaging.Reaction {
		r, created, tooMany, err := s.AddReaction(ctx, chatID, id, user, emoji, ts)
		require.NoError(t, err)
		require.True(t, created)
		require.False(t, tooMany)
		return r
	}

	// Message 1: viewer holds two emoji, other holds one of them (added first,
	// so the viewer's 👍 is version 2 while their ❤️ is version 1).
	add(other, ids[0], "👍", at(100))
	add(viewer, ids[0], "❤️", at(101))
	add(viewer, ids[0], "👍", at(102))
	// Message 2: only the other user reacted.
	add(other, ids[1], "🔥", at(103))
	// Message 3: the viewer reacted, then removed — nothing to report.
	add(viewer, ids[2], "🔥", at(104))
	_, removed, err := s.RemoveReaction(ctx, chatID, ids[2], viewer, "🔥")
	require.NoError(t, err)
	require.True(t, removed)
	// Message 4: the viewer removed and re-added, so their reaction sits at the
	// re-add's version (3) and time, not the original's (1).
	add(viewer, ids[3], "🎉", at(105))
	_, removed, err = s.RemoveReaction(ctx, chatID, ids[3], viewer, "🎉")
	require.NoError(t, err)
	require.True(t, removed)
	readd := add(viewer, ids[3], "🎉", at(106))
	require.Equal(t, uint64(3), readd.Version)
	require.Equal(t, at(106), readd.ReactedBySelfTs)
	// Message 70: viewer reacted, in the far window.
	add(viewer, ids[69], "👍", at(107))
	// Message 5: viewer reacted, but it won't be asked about.
	add(viewer, ids[4], "👍", at(108))

	type key struct {
		seq   uint64
		emoji string
	}
	type entry struct {
		version uint64
		ts      time.Time
	}
	collect := func(present []messaging.SelfReaction) map[key]entry {
		got := make(map[key]entry, len(present))
		for _, self := range present {
			got[key{self.MessageID.Value, self.Emoji}] = entry{self.Version, self.ReactedTs}
		}
		return got
	}
	// Ask about 1..4 and 70 (plus a duplicate and an unknown ID): the answer is
	// exactly the viewer's live reactions on those, not message 5's, not the
	// removed one, and not the other user's — each at the version and time of
	// the add that holds.
	present, err := s.GetSelfReactions(ctx, chatID, viewer, []*messagingpb.MessageId{ids[69], ids[0], ids[1], ids[2], ids[3], ids[0], {Value: 999}})
	require.NoError(t, err)
	require.Equal(t, map[key]entry{
		{ids[0].Value, "❤️"}: {1, at(101)},
		{ids[0].Value, "👍"}:  {2, at(102)},
		{ids[3].Value, "🎉"}:  {3, at(106)},
		{ids[69].Value, "👍"}: {1, at(107)},
	}, collect(present))

	// The other user's view of the same messages is theirs alone.
	present, err = s.GetSelfReactions(ctx, chatID, other, []*messagingpb.MessageId{ids[0], ids[1], ids[3]})
	require.NoError(t, err)
	require.Equal(t, map[key]entry{
		{ids[0].Value, "👍"}: {1, at(100)},
		{ids[1].Value, "🔥"}: {1, at(103)},
	}, collect(present))

	// Nothing asked, nothing answered.
	present, err = s.GetSelfReactions(ctx, chatID, viewer, nil)
	require.NoError(t, err)
	require.Empty(t, present)

	// A DM is refused outright, reacted or not: the store keeps no per-viewer
	// rows for one, so an empty answer would be a lie.
	dmID := generateChatID()
	msg, _, err := s.PutMessage(ctx, dmID, other, textContent("m"), at(1), generateClientID(), true)
	require.NoError(t, err)
	r, created, tooMany, err := s.AddReaction(ctx, dmID, msg.ID, viewer, "👍", at(2))
	require.NoError(t, err)
	require.True(t, created)
	require.False(t, tooMany)
	require.True(t, r.ReactedBySelf)
	_, err = s.GetSelfReactions(ctx, dmID, viewer, []*messagingpb.MessageId{msg.ID})
	require.ErrorIs(t, err, messaging.ErrSelfReactionsGroupOnly)
	// The DM's add and remove are otherwise whole: the reactor is listed, and
	// the removal lands.
	reactors, _, _, err := s.GetReactors(ctx, dmID, msg.ID, "👍")
	require.NoError(t, err)
	require.Len(t, reactors, 1)
	_, removed, err = s.RemoveReaction(ctx, dmID, msg.ID, viewer, "👍")
	require.NoError(t, err)
	require.True(t, removed)
}

func testStore_Reactions_SummariesByRefs(t *testing.T, s messaging.Store) {
	ctx := context.Background()
	chatID := generateChatID()
	user := model.MustGenerateUserID()

	// Three messages (IDs 1..3); only 1 and 3 get reactions, msg1 with two emoji.
	var ids []*messagingpb.MessageId
	for i := 1; i <= 3; i++ {
		msg, _, err := s.PutMessage(ctx, chatID, user, textContent("m"), at(int64(i)), generateClientID(), true)
		require.NoError(t, err)
		ids = append(ids, msg.ID)
	}
	_, _, _, err := s.AddReaction(ctx, chatID, ids[0], user, "👍", at(1))
	require.NoError(t, err)
	_, _, _, err = s.AddReaction(ctx, chatID, ids[0], user, "❤️", at(2))
	require.NoError(t, err)
	_, _, _, err = s.AddReaction(ctx, chatID, ids[2], user, "❤️", at(3))
	require.NoError(t, err)

	// Batch get by refs echoes every requested message, ordered by ID: messages
	// with reactions carry their full emoji set together, while the reactionless
	// message (ids[1]) and the unknown ID (999) come back with empty summaries
	// rather than being omitted. Duplicate refs collapse.
	byRefs, err := s.GetReactionSummariesByRefs(ctx, chatID, []*messagingpb.MessageId{ids[2], ids[0], ids[1], ids[0], {Value: 999}})
	require.NoError(t, err)
	require.Len(t, byRefs, 4)
	require.Equal(t, ids[0].Value, byRefs[0].MessageID.Value)
	require.Len(t, byRefs[0].Reactions, 2)
	require.Equal(t, ids[1].Value, byRefs[1].MessageID.Value)
	require.Empty(t, byRefs[1].Reactions)
	require.Equal(t, ids[2].Value, byRefs[2].MessageID.Value)
	require.Len(t, byRefs[2].Reactions, 1)
	require.Equal(t, "❤️", byRefs[2].Reactions[0].Emoji)
	require.Equal(t, uint64(999), byRefs[3].MessageID.Value)
	require.Empty(t, byRefs[3].Reactions)

	// Empty input → empty result, no error.
	empty, err := s.GetReactionSummariesByRefs(ctx, chatID, nil)
	require.NoError(t, err)
	require.Empty(t, empty)
}

func testStore_Reactions_SummariesPaging(t *testing.T, s messaging.Store) {
	ctx := context.Background()
	chatID := generateChatID()
	userA := model.MustGenerateUserID()
	userB := model.MustGenerateUserID()

	// Five messages (IDs 1..5). Reactions land on 1, 2, and 4; 3 and 5 stay
	// reactionless so the page is exercised across interior and trailing gaps.
	var ids []*messagingpb.MessageId
	for i := 1; i <= 5; i++ {
		msg, _, err := s.PutMessage(ctx, chatID, userA, textContent("m"), at(int64(i)), generateClientID(), true)
		require.NoError(t, err)
		ids = append(ids, msg.ID)
	}
	// msg1 carries two distinct emoji; msg2 and msg4 one each.
	_, _, _, err := s.AddReaction(ctx, chatID, ids[0], userA, "👍", at(1))
	require.NoError(t, err)
	_, _, _, err = s.AddReaction(ctx, chatID, ids[0], userB, "❤️", at(2))
	require.NoError(t, err)
	_, _, _, err = s.AddReaction(ctx, chatID, ids[1], userA, "👍", at(3))
	require.NoError(t, err)
	_, _, _, err = s.AddReaction(ctx, chatID, ids[3], userA, "🎉", at(4))
	require.NoError(t, err)

	// reactionCounts maps each summary to its number of distinct emoji, so the
	// per-message aggregate can be asserted alongside the ordering.
	reactionCounts := func(summaries []*messaging.ReactionSummary) []int {
		out := make([]int, len(summaries))
		for i, summary := range summaries {
			out[i] = len(summary.Reactions)
		}
		return out
	}

	// Ascending, full set: every message is present in ID order — including the
	// reactionless 3 and 5, returned with empty summaries rather than skipped.
	asc, err := s.GetReactionSummaries(ctx, chatID, database.WithAscending())
	require.NoError(t, err)
	require.Equal(t, []uint64{1, 2, 3, 4, 5}, summaryIDs(asc))
	require.Equal(t, []int{2, 1, 0, 1, 0}, reactionCounts(asc))

	// Descending, full set: reverse ID order, gaps still present.
	desc, err := s.GetReactionSummaries(ctx, chatID, database.WithDescending())
	require.NoError(t, err)
	require.Equal(t, []uint64{5, 4, 3, 2, 1}, summaryIDs(desc))
	require.Equal(t, []int{0, 1, 0, 1, 2}, reactionCounts(desc))

	// Ascending paging: the limit counts messages, not reactions, so msg1's two
	// emoji come back together as one summary. Pages are [1,2], [3,4], [5].
	apage1, err := s.GetReactionSummaries(ctx, chatID, database.WithAscending(), database.WithLimit(2))
	require.NoError(t, err)
	require.Equal(t, []uint64{1, 2}, summaryIDs(apage1))
	require.Equal(t, []int{2, 1}, reactionCounts(apage1))

	// Page 2 resumes strictly after the cursor: the reactionless msg3 leads with
	// an empty summary, ahead of msg4's reaction.
	atoken := messaging.PageTokenFromID(apage1[len(apage1)-1].MessageID)
	apage2, err := s.GetReactionSummaries(ctx, chatID, database.WithAscending(), database.WithLimit(2), database.WithPagingToken(atoken))
	require.NoError(t, err)
	require.Equal(t, []uint64{3, 4}, summaryIDs(apage2))
	require.Equal(t, []int{0, 1}, reactionCounts(apage2))

	// The final page is short (one message left) and trails with the reactionless
	// msg5.
	atoken2 := messaging.PageTokenFromID(apage2[len(apage2)-1].MessageID)
	apage3, err := s.GetReactionSummaries(ctx, chatID, database.WithAscending(), database.WithLimit(2), database.WithPagingToken(atoken2))
	require.NoError(t, err)
	require.Equal(t, []uint64{5}, summaryIDs(apage3))
	require.Equal(t, []int{0}, reactionCounts(apage3))

	// Paging past the end yields an empty page (no error).
	atoken3 := messaging.PageTokenFromID(apage3[len(apage3)-1].MessageID)
	apage4, err := s.GetReactionSummaries(ctx, chatID, database.WithAscending(), database.WithLimit(2), database.WithPagingToken(atoken3))
	require.NoError(t, err)
	require.Empty(t, apage4)

	// Descending paging mirrors ascending: pages are [5,4], [3,2], [1].
	dpage1, err := s.GetReactionSummaries(ctx, chatID, database.WithDescending(), database.WithLimit(2))
	require.NoError(t, err)
	require.Equal(t, []uint64{5, 4}, summaryIDs(dpage1))
	require.Equal(t, []int{0, 1}, reactionCounts(dpage1))

	dtoken := messaging.PageTokenFromID(dpage1[len(dpage1)-1].MessageID)
	dpage2, err := s.GetReactionSummaries(ctx, chatID, database.WithDescending(), database.WithLimit(2), database.WithPagingToken(dtoken))
	require.NoError(t, err)
	require.Equal(t, []uint64{3, 2}, summaryIDs(dpage2))
	require.Equal(t, []int{0, 1}, reactionCounts(dpage2))

	dtoken2 := messaging.PageTokenFromID(dpage2[len(dpage2)-1].MessageID)
	dpage3, err := s.GetReactionSummaries(ctx, chatID, database.WithDescending(), database.WithLimit(2), database.WithPagingToken(dtoken2))
	require.NoError(t, err)
	require.Equal(t, []uint64{1}, summaryIDs(dpage3))
	require.Equal(t, []int{2}, reactionCounts(dpage3))

	// An unknown chat has no messages, hence no summaries.
	empty, err := s.GetReactionSummaries(ctx, generateChatID(), database.WithAscending())
	require.NoError(t, err)
	require.Empty(t, empty)
}

func testStore_Reactions_SampleCap(t *testing.T, s messaging.Store) {
	ctx := context.Background()
	chatID := generateChatID()

	msg, _, err := s.PutMessage(ctx, chatID, model.MustGenerateUserID(), textContent("popular"), at(1), generateClientID(), true)
	require.NoError(t, err)
	msgID := msg.ID
	const emoji = "👍"

	// React with more users than the stored sample holds, at increasing times so
	// recency order is well-defined.
	total := messaging.MaxStoredSampleReactors + messaging.MaxSampleReactors
	users := make([]*commonpb.UserId, total)
	var last *messaging.Reaction
	for i := 0; i < total; i++ {
		users[i] = model.MustGenerateUserID()
		last, _, _, err = s.AddReaction(ctx, chatID, msgID, users[i], emoji, at(int64(i+1)))
		require.NoError(t, err)
	}

	// The surfaced sample is capped at MaxSampleReactors and holds the most-recent
	// reactors (by timestamp), in descending order — even though more were stored.
	require.Equal(t, uint64(total), last.Count)
	require.Len(t, last.SampleReactors, messaging.MaxSampleReactors)
	for i := 0; i < messaging.MaxSampleReactors; i++ {
		require.Equal(t, users[total-1-i].Value, last.SampleReactors[i].UserID.Value)
	}

	// Remove every reactor retained in the stored sample (the most-recent
	// MaxStoredSampleReactors). With no backfill, the surfaced sample empties even
	// though earlier reactors remain — the count stays accurate.
	for i := total - messaging.MaxStoredSampleReactors; i < total; i++ {
		_, removed, err := s.RemoveReaction(ctx, chatID, msgID, users[i], emoji)
		require.NoError(t, err)
		require.True(t, removed)
	}

	summary, err := s.GetReactionSummary(ctx, chatID, msgID)
	require.NoError(t, err)
	require.Len(t, summary, 1)
	require.Equal(t, uint64(messaging.MaxSampleReactors), summary[0].Count)
	require.Empty(t, summary[0].SampleReactors)
}

func testStore_Reactions_TypeCap(t *testing.T, s messaging.Store) {
	ctx := context.Background()
	chatID := generateChatID()
	userA := model.MustGenerateUserID()
	userB := model.MustGenerateUserID()

	msg, _, err := s.PutMessage(ctx, chatID, userA, textContent("popular"), at(1), generateClientID(), true)
	require.NoError(t, err)
	msgID := msg.ID

	// Fill the message to its distinct-emoji cap. (The store treats the emoji as
	// an opaque key; real-emoji validity is enforced one layer up.)
	for i := 0; i < messaging.MaxReactionTypesPerMessage; i++ {
		_, created, tooMany, err := s.AddReaction(ctx, chatID, msgID, userA, fmt.Sprintf("e-%d", i), at(int64(i)))
		require.NoError(t, err)
		require.True(t, created)
		require.False(t, tooMany)
	}

	// One more distinct emoji is rejected.
	r, created, tooMany, err := s.AddReaction(ctx, chatID, msgID, userA, "e-over", at(1))
	require.NoError(t, err)
	require.False(t, created)
	require.True(t, tooMany)
	require.Nil(t, r)

	// Re-adding an already-present emoji (different user) never trips the cap.
	_, created, tooMany, err = s.AddReaction(ctx, chatID, msgID, userB, "e-0", at(1))
	require.NoError(t, err)
	require.True(t, created)
	require.False(t, tooMany)

	// Emptying an emoji frees a slot for a new distinct one.
	_, _, err = s.RemoveReaction(ctx, chatID, msgID, userA, "e-1")
	require.NoError(t, err)
	r, created, tooMany, err = s.AddReaction(ctx, chatID, msgID, userA, "e-over", at(1))
	require.NoError(t, err)
	require.True(t, created)
	require.False(t, tooMany)
	require.NotNil(t, r)
}

// testStore_Reactions_TypeCapConcurrent pins that the type cap is exact under
// contention, not a best-effort check: activations racing for the last slots
// on a message must admit exactly as many as remain and refuse the rest, with
// no error and no overshoot. Every activation compare-and-sets the message's
// type count in its own transaction, so the losers re-decide against the count
// that won.
func testStore_Reactions_TypeCapConcurrent(t *testing.T, s messaging.Store) {
	ctx := context.Background()
	chatID := generateChatID()
	user := model.MustGenerateUserID()

	msg, _, err := s.PutMessage(ctx, chatID, user, textContent("popular"), at(1), generateClientID(), true)
	require.NoError(t, err)
	msgID := msg.ID

	// Fill to ten below the cap sequentially, then race twenty distinct emoji
	// for the ten remaining slots.
	const remaining = 10
	const contenders = 2 * remaining
	prefill := messaging.MaxReactionTypesPerMessage - remaining
	for i := 0; i < prefill; i++ {
		_, created, tooMany, err := s.AddReaction(ctx, chatID, msgID, user, fmt.Sprintf("pre-%d", i), at(1))
		require.NoError(t, err)
		require.True(t, created)
		require.False(t, tooMany)
	}

	var wg sync.WaitGroup
	results := make([]struct {
		created, tooMany bool
		err              error
	}, contenders)
	for i := 0; i < contenders; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, created, tooMany, err := s.AddReaction(ctx, chatID, msgID, user, fmt.Sprintf("race-%d", i), at(2))
			results[i].created, results[i].tooMany, results[i].err = created, tooMany, err
		}(i)
	}
	wg.Wait()

	var admitted, refused int
	for _, r := range results {
		require.NoError(t, r.err)
		switch {
		case r.created && !r.tooMany:
			admitted++
		case !r.created && r.tooMany:
			refused++
		default:
			t.Fatalf("activation neither admitted nor refused: created=%v tooMany=%v", r.created, r.tooMany)
		}
	}
	require.Equal(t, remaining, admitted)
	require.Equal(t, contenders-remaining, refused)

	// The message sits exactly at the cap, and one more distinct emoji is refused.
	summary, err := s.GetReactionSummary(ctx, chatID, msgID)
	require.NoError(t, err)
	require.Len(t, summary, messaging.MaxReactionTypesPerMessage)
	_, created, tooMany, err := s.AddReaction(ctx, chatID, msgID, user, "one-more", at(3))
	require.NoError(t, err)
	require.False(t, created)
	require.True(t, tooMany)
}

func testStore_Reactions_GetReactors(t *testing.T, s messaging.Store) {
	ctx := context.Background()
	chatID := generateChatID()
	sender := model.MustGenerateUserID()

	msg, _, err := s.PutMessage(ctx, chatID, sender, textContent("react"), at(1), generateClientID(), true)
	require.NoError(t, err)
	msgID := msg.ID
	const emoji = "👍"

	// Before anyone reacts there is no aggregate: an empty page at version 0.
	none, version, hasMore, err := s.GetReactors(ctx, chatID, msgID, emoji)
	require.NoError(t, err)
	require.Empty(t, none)
	require.Zero(t, version)
	require.False(t, hasMore)

	// Five reactors, added in order u0..u4 but stamped with DECREASING timestamps:
	// the list is ordered by reaction order (the version that added each
	// reactor), never by the display timestamp, so recency order is u4..u0.
	users := make([]*commonpb.UserId, 5)
	for i := range users {
		users[i] = model.MustGenerateUserID()
		_, _, _, err := s.AddReaction(ctx, chatID, msgID, users[i], emoji, at(int64(10-i)))
		require.NoError(t, err)
	}

	reactorIDs := func(reactors []*messaging.Reactor) [][]byte {
		out := make([][]byte, len(reactors))
		for i, r := range reactors {
			out[i] = r.UserID.Value
		}
		return out
	}
	reactorVersions := func(reactors []*messaging.Reactor) []uint64 {
		out := make([]uint64, len(reactors))
		for i, r := range reactors {
			out[i] = r.Version
		}
		return out
	}

	// Full list, most-recent-first, each reactor carrying the version that added
	// them and its display timestamp; the aggregate version is the last add.
	all, version, hasMore, err := s.GetReactors(ctx, chatID, msgID, emoji)
	require.NoError(t, err)
	require.False(t, hasMore)
	require.Equal(t, uint64(5), version)
	require.Equal(t, [][]byte{users[4].Value, users[3].Value, users[2].Value, users[1].Value, users[0].Value}, reactorIDs(all))
	require.Equal(t, []uint64{5, 4, 3, 2, 1}, reactorVersions(all))
	require.True(t, at(6).Equal(all[0].ReactedTs))

	// Page through two at a time, carrying the cursor forward.
	page1, _, hasMore, err := s.GetReactors(ctx, chatID, msgID, emoji, database.WithLimit(2))
	require.NoError(t, err)
	require.True(t, hasMore)
	require.Equal(t, [][]byte{users[4].Value, users[3].Value}, reactorIDs(page1))

	page2, _, hasMore, err := s.GetReactors(ctx, chatID, msgID, emoji, database.WithLimit(2), database.WithPagingToken(messaging.ReactorPageToken(page1[len(page1)-1])))
	require.NoError(t, err)
	require.True(t, hasMore)
	require.Equal(t, [][]byte{users[2].Value, users[1].Value}, reactorIDs(page2))

	page3, _, hasMore, err := s.GetReactors(ctx, chatID, msgID, emoji, database.WithLimit(2), database.WithPagingToken(messaging.ReactorPageToken(page2[len(page2)-1])))
	require.NoError(t, err)
	require.False(t, hasMore)
	require.Equal(t, [][]byte{users[0].Value}, reactorIDs(page3))

	// Paging past the last reactor is an empty page.
	page4, _, hasMore, err := s.GetReactors(ctx, chatID, msgID, emoji, database.WithLimit(2), database.WithPagingToken(messaging.ReactorPageToken(page3[len(page3)-1])))
	require.NoError(t, err)
	require.False(t, hasMore)
	require.Empty(t, page4)

	// A page whose limit exactly equals the available count must report
	// hasMore false — there is no next page.
	exact, _, hasMore, err := s.GetReactors(ctx, chatID, msgID, emoji, database.WithLimit(5))
	require.NoError(t, err)
	require.False(t, hasMore)
	require.Len(t, exact, 5)

	// A removal advances the aggregate version without adding a row, and a re-add
	// lands at the top under a fresh version: the list is reaction order, so a
	// returning reactor is the most recent.
	_, removed, err := s.RemoveReaction(ctx, chatID, msgID, users[2], emoji)
	require.NoError(t, err)
	require.True(t, removed)
	after, version, _, err := s.GetReactors(ctx, chatID, msgID, emoji)
	require.NoError(t, err)
	require.Equal(t, uint64(6), version)
	require.Equal(t, [][]byte{users[4].Value, users[3].Value, users[1].Value, users[0].Value}, reactorIDs(after))
	_, _, _, err = s.AddReaction(ctx, chatID, msgID, users[2], emoji, at(1))
	require.NoError(t, err)
	readded, version, _, err := s.GetReactors(ctx, chatID, msgID, emoji)
	require.NoError(t, err)
	require.Equal(t, uint64(7), version)
	require.Equal(t, [][]byte{users[2].Value, users[4].Value, users[3].Value, users[1].Value, users[0].Value}, reactorIDs(readded))
	require.Equal(t, uint64(7), readded[0].Version)

	// Unknown emoji on the message: empty, no error.
	none, version, hasMore, err = s.GetReactors(ctx, chatID, msgID, "🚀")
	require.NoError(t, err)
	require.Empty(t, none)
	require.Zero(t, version)
	require.False(t, hasMore)

	// Every reactor leaving keeps the version (the aggregate is retained) while
	// the list empties.
	for _, u := range users {
		_, _, err := s.RemoveReaction(ctx, chatID, msgID, u, emoji)
		require.NoError(t, err)
	}
	none, version, hasMore, err = s.GetReactors(ctx, chatID, msgID, emoji)
	require.NoError(t, err)
	require.Empty(t, none)
	require.Equal(t, uint64(12), version)
	require.False(t, hasMore)
}

func textContent(text string) []*messagingpb.Content {
	return []*messagingpb.Content{{
		Type: &messagingpb.Content_Text{
			Text: &messagingpb.TextContent{Text: text},
		},
	}}
}

func systemContent(text string) []*messagingpb.Content {
	return []*messagingpb.Content{{
		Type: &messagingpb.Content_System{
			System: &messagingpb.SystemContent{FallbackText: text},
		},
	}}
}

func replyContent(repliedMessageID uint64, text string) []*messagingpb.Content {
	return []*messagingpb.Content{{
		Type: &messagingpb.Content_Reply{
			Reply: &messagingpb.ReplyContent{
				RepliedMessageId: &messagingpb.MessageId{Value: repliedMessageID},
				Content:          textContent(text),
			},
		},
	}}
}

// mediaContent builds a one-item media message referencing blobID as its ORIGINAL
// rendition — the shape a client sends.
func mediaContent(blobID *blobpb.BlobId) []*messagingpb.Content {
	return []*messagingpb.Content{{
		Type: &messagingpb.Content_Media{Media: &messagingpb.MediaContent{
			Items: []*blobpb.Media{{
				Renditions: []*blobpb.Rendition{{
					Role:   blobpb.Rendition_ORIGINAL,
					BlobId: blobID,
				}},
			}},
		}},
	}}
}

// replyMediaContent builds a reply to repliedMessageID whose body is a media
// message referencing blobID.
func replyMediaContent(repliedMessageID uint64, blobID *blobpb.BlobId) []*messagingpb.Content {
	return []*messagingpb.Content{{
		Type: &messagingpb.Content_Reply{
			Reply: &messagingpb.ReplyContent{
				RepliedMessageId: &messagingpb.MessageId{Value: repliedMessageID},
				Content:          mediaContent(blobID),
			},
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

func summaryIDs(summaries []*messaging.ReactionSummary) []uint64 {
	out := make([]uint64, len(summaries))
	for i, s := range summaries {
		out[i] = s.MessageID.Value
	}
	return out
}

func pointerKey(t messagingpb.Pointer_Type, userID *commonpb.UserId, value uint64) string {
	return t.String() + "#" + string(userID.Value) + "#" + strconv.FormatUint(value, 10)
}

func pointerKeyOf(p *messagingpb.Pointer) string {
	return pointerKey(p.Type, p.UserId, p.Value.Value)
}

func pointerKeysOf(pointers []*messagingpb.Pointer) []string {
	out := make([]string, len(pointers))
	for i, p := range pointers {
		out[i] = pointerKeyOf(p)
	}
	return out
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

// generateGroupChatID returns a random group-width chat ID (see
// chat.GroupChatIDSize; the store discriminates chat type by length alone).
func generateGroupChatID() *commonpb.ChatId {
	b := make([]byte, chat.GroupChatIDSize)
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
