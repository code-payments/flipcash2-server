package tests

import (
	"context"
	"crypto/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	chatpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/chat/v1"
	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"
	messagingpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/messaging/v1"

	"github.com/code-payments/flipcash2-server/chat"
	"github.com/code-payments/flipcash2-server/model"
)

// RunStoreTests runs the shared chat.Store test suite against s. teardown is
// called between tests to reset the store.
func RunStoreTests(t *testing.T, s chat.Store, teardown func()) {
	for _, tf := range []func(t *testing.T, s chat.Store){
		testStore_PutAndGet,
		testStore_PutChat_Duplicate,
		testStore_GetChatByID_NotFound,
		testStore_Members,
		testStore_IsMember,
		testStore_AdvanceLastMessage,
		testStore_GetDmFeedPage_Order,
		testStore_GetDmFeedPage_Watermark,
		testStore_GetDmFeedPage_Paging,
		testStore_GetDmFeedPage_SnapshotPinned,
		testStore_GetDmFeedPage_Empty,
	} {
		tf(t, s)
		teardown()
	}
}

func testStore_PutAndGet(t *testing.T, s chat.Store) {
	ctx := context.Background()

	userA := model.MustGenerateUserID()
	userB := model.MustGenerateUserID()
	c := &chat.Chat{
		ID:           generateChatID(),
		Type:         chatpb.Metadata_DM,
		Members:      []*commonpb.UserId{userA, userB},
		LastActivity: at(100),
	}
	require.NoError(t, s.PutChat(ctx, c))

	got, err := s.GetChatByID(ctx, c.ID)
	require.NoError(t, err)
	require.Equal(t, c.ID.Value, got.ID.Value)
	require.Equal(t, chatpb.Metadata_DM, got.Type)
	require.True(t, got.LastActivity.Equal(at(100)))
	require.ElementsMatch(t, userIDValues(c.Members), userIDValues(got.Members))
}

func testStore_PutChat_Duplicate(t *testing.T, s chat.Store) {
	ctx := context.Background()

	c := &chat.Chat{
		ID:           generateChatID(),
		Type:         chatpb.Metadata_DM,
		Members:      []*commonpb.UserId{model.MustGenerateUserID(), model.MustGenerateUserID()},
		LastActivity: at(1),
	}
	require.NoError(t, s.PutChat(ctx, c))
	require.ErrorIs(t, s.PutChat(ctx, c), chat.ErrChatExists)
}

func testStore_GetChatByID_NotFound(t *testing.T, s chat.Store) {
	ctx := context.Background()

	_, err := s.GetChatByID(ctx, generateChatID())
	require.ErrorIs(t, err, chat.ErrChatNotFound)

	_, err = s.GetMembers(ctx, generateChatID())
	require.ErrorIs(t, err, chat.ErrChatNotFound)
}

func testStore_Members(t *testing.T, s chat.Store) {
	ctx := context.Background()

	userA := model.MustGenerateUserID()
	userB := model.MustGenerateUserID()
	c := &chat.Chat{
		ID:           generateChatID(),
		Type:         chatpb.Metadata_DM,
		Members:      []*commonpb.UserId{userA, userB},
		LastActivity: at(5),
	}
	require.NoError(t, s.PutChat(ctx, c))

	members, err := s.GetMembers(ctx, c.ID)
	require.NoError(t, err)
	require.ElementsMatch(t, userIDValues([]*commonpb.UserId{userA, userB}), userIDValues(members))
}

func testStore_IsMember(t *testing.T, s chat.Store) {
	ctx := context.Background()

	userA := model.MustGenerateUserID()
	userB := model.MustGenerateUserID()
	stranger := model.MustGenerateUserID()
	c := &chat.Chat{
		ID:           generateChatID(),
		Type:         chatpb.Metadata_DM,
		Members:      []*commonpb.UserId{userA, userB},
		LastActivity: at(5),
	}
	require.NoError(t, s.PutChat(ctx, c))

	ok, err := s.IsMember(ctx, c.ID, userA)
	require.NoError(t, err)
	require.True(t, ok)

	ok, err = s.IsMember(ctx, c.ID, stranger)
	require.NoError(t, err)
	require.False(t, ok)

	// Unknown chat → false, no error.
	ok, err = s.IsMember(ctx, generateChatID(), userA)
	require.NoError(t, err)
	require.False(t, ok)
}

func testStore_AdvanceLastMessage(t *testing.T, s chat.Store) {
	ctx := context.Background()

	member := model.MustGenerateUserID()
	c := &chat.Chat{
		ID:           generateChatID(),
		Type:         chatpb.Metadata_DM,
		Members:      []*commonpb.UserId{member},
		LastActivity: at(100),
	}
	require.NoError(t, s.PutChat(ctx, c))

	// A new chat has no last message.
	got, err := s.GetChatByID(ctx, c.ID)
	require.NoError(t, err)
	require.Nil(t, got.LastMessageID)

	// Forward moves both last_activity and last_message_id, reports advanced, and
	// returns the chat members for the caller to reuse.
	advanced, members, err := s.AdvanceLastMessage(ctx, c.ID, &messagingpb.MessageId{Value: 5}, at(200))
	require.NoError(t, err)
	require.True(t, advanced)
	require.Equal(t, [][]byte{member.Value}, userIDValues(members))
	got, err = s.GetChatByID(ctx, c.ID)
	require.NoError(t, err)
	require.True(t, got.LastActivity.Equal(at(200)))
	require.NotNil(t, got.LastMessageID)
	require.Equal(t, uint64(5), got.LastMessageID.Value)

	// Backward is a no-op and reports not advanced; neither field changes. Members
	// are still returned on the no-op path.
	advanced, members, err = s.AdvanceLastMessage(ctx, c.ID, &messagingpb.MessageId{Value: 3}, at(150))
	require.NoError(t, err)
	require.False(t, advanced)
	require.Equal(t, [][]byte{member.Value}, userIDValues(members))
	got, err = s.GetChatByID(ctx, c.ID)
	require.NoError(t, err)
	require.True(t, got.LastActivity.Equal(at(200)))
	require.Equal(t, uint64(5), got.LastMessageID.Value)

	// Unknown chat → ErrChatNotFound, with nil members.
	_, members, err = s.AdvanceLastMessage(ctx, generateChatID(), &messagingpb.MessageId{Value: 1}, at(1))
	require.ErrorIs(t, err, chat.ErrChatNotFound)
	require.Nil(t, members)
}

func testStore_GetDmFeedPage_Order(t *testing.T, s chat.Store) {
	ctx := context.Background()

	user := model.MustGenerateUserID()
	other := model.MustGenerateUserID()

	// Three chats the user is in, plus one they are not.
	c1 := putChat(t, s, user, other, at(100))
	c2 := putChat(t, s, user, other, at(300))
	c3 := putChat(t, s, user, other, at(200))
	_ = putChat(t, s, model.MustGenerateUserID(), model.MustGenerateUserID(), at(999))

	// A watermark above every chat includes them all, most recent first.
	got, err := s.GetDmFeedPage(ctx, user, at(1000), nil, 0)
	require.NoError(t, err)
	require.Equal(t, [][]byte{c2.ID.Value, c3.ID.Value, c1.ID.Value}, chatIDValues(got))
}

func testStore_GetDmFeedPage_Watermark(t *testing.T, s chat.Store) {
	ctx := context.Background()

	user := model.MustGenerateUserID()
	other := model.MustGenerateUserID()
	c1 := putChat(t, s, user, other, at(100))
	_ = putChat(t, s, user, other, at(300)) // Above the watermark; excluded.
	c3 := putChat(t, s, user, other, at(200))

	// A watermark of 250 pins out the chat last active at 300.
	got, err := s.GetDmFeedPage(ctx, user, at(250), nil, 0)
	require.NoError(t, err)
	require.Equal(t, [][]byte{c3.ID.Value, c1.ID.Value}, chatIDValues(got))
}

func testStore_GetDmFeedPage_Paging(t *testing.T, s chat.Store) {
	ctx := context.Background()

	user := model.MustGenerateUserID()
	other := model.MustGenerateUserID()
	c1 := putChat(t, s, user, other, at(100))
	c2 := putChat(t, s, user, other, at(300))
	c3 := putChat(t, s, user, other, at(200))

	snapshot := at(1000)

	// Page 1: most recent, limit 2 → [c2, c3].
	page1, err := s.GetDmFeedPage(ctx, user, snapshot, nil, 2)
	require.NoError(t, err)
	require.Equal(t, [][]byte{c2.ID.Value, c3.ID.Value}, chatIDValues(page1))

	// Page 2: resume after the last chat of page 1 (c3) → [c1].
	page2, err := s.GetDmFeedPage(ctx, user, snapshot, cursorOf(page1[len(page1)-1]), 2)
	require.NoError(t, err)
	require.Equal(t, [][]byte{c1.ID.Value}, chatIDValues(page2))

	// Resuming after the final chat yields an empty page.
	page3, err := s.GetDmFeedPage(ctx, user, snapshot, cursorOf(c1), 2)
	require.NoError(t, err)
	require.Empty(t, page3)
}

// testStore_GetDmFeedPage_SnapshotPinned verifies that a chat which becomes
// active after the snapshot leaves the pinned window and is not paginated, so
// the multi-page read stays internally consistent.
func testStore_GetDmFeedPage_SnapshotPinned(t *testing.T, s chat.Store) {
	ctx := context.Background()

	user := model.MustGenerateUserID()
	other := model.MustGenerateUserID()
	c1 := putChat(t, s, user, other, at(100))
	c2 := putChat(t, s, user, other, at(200))
	c3 := putChat(t, s, user, other, at(300))

	snapshot := at(350) // All three are within the window.

	// Page 1: the most recent chat.
	page1, err := s.GetDmFeedPage(ctx, user, snapshot, nil, 1)
	require.NoError(t, err)
	require.Equal(t, [][]byte{c3.ID.Value}, chatIDValues(page1))

	// c1, not yet paged, becomes active after the snapshot, moving above the
	// watermark.
	advanced, _, err := s.AdvanceLastMessage(ctx, c1.ID, &messagingpb.MessageId{Value: 1}, at(999))
	require.NoError(t, err)
	require.True(t, advanced)

	// Page 2 sees only c2: c1 has left the snapshot window, so it is neither
	// duplicated nor reordered into the read. Its freshness is the stream's job.
	page2, err := s.GetDmFeedPage(ctx, user, snapshot, cursorOf(page1[len(page1)-1]), 10)
	require.NoError(t, err)
	require.Equal(t, [][]byte{c2.ID.Value}, chatIDValues(page2))
}

func testStore_GetDmFeedPage_Empty(t *testing.T, s chat.Store) {
	ctx := context.Background()

	got, err := s.GetDmFeedPage(ctx, model.MustGenerateUserID(), at(1000), nil, 0)
	require.NoError(t, err)
	require.Empty(t, got)
}

func cursorOf(c *chat.Chat) *chat.DmFeedCursor {
	return &chat.DmFeedCursor{LastActivity: c.LastActivity, ChatID: c.ID}
}

func putChat(t *testing.T, s chat.Store, a, b *commonpb.UserId, lastActivity time.Time) *chat.Chat {
	c := &chat.Chat{
		ID:           generateChatID(),
		Type:         chatpb.Metadata_DM,
		Members:      []*commonpb.UserId{a, b},
		LastActivity: lastActivity,
	}
	require.NoError(t, s.PutChat(context.Background(), c))
	return c
}

// at returns a deterministic timestamp offset by the given number of seconds
// from a fixed epoch, in UTC.
func at(seconds int64) time.Time {
	return time.Unix(1_700_000_000+seconds, 0).UTC()
}

func generateChatID() *commonpb.ChatId {
	b := make([]byte, chat.ChatIDSize)
	if _, err := rand.Read(b); err != nil {
		panic(err)
	}
	return &commonpb.ChatId{Value: b}
}

func chatIDValues(chats []*chat.Chat) [][]byte {
	out := make([][]byte, len(chats))
	for i, c := range chats {
		out[i] = c.ID.Value
	}
	return out
}

func userIDValues(ids []*commonpb.UserId) [][]byte {
	out := make([][]byte, len(ids))
	for i, id := range ids {
		out[i] = id.Value
	}
	return out
}
