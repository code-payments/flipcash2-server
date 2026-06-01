package tests

import (
	"context"
	"crypto/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	chatpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/chat/v1"
	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"

	"github.com/code-payments/flipcash2-server/chat"
	"github.com/code-payments/flipcash2-server/database"
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
		testStore_AdvanceLastActivity,
		testStore_GetDmsForUserByLastActivity_Order,
		testStore_GetDmsForUserByLastActivity_Paging,
		testStore_GetDmsForUserByLastActivity_Empty,
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

func testStore_AdvanceLastActivity(t *testing.T, s chat.Store) {
	ctx := context.Background()

	c := &chat.Chat{
		ID:           generateChatID(),
		Type:         chatpb.Metadata_DM,
		Members:      []*commonpb.UserId{model.MustGenerateUserID()},
		LastActivity: at(100),
	}
	require.NoError(t, s.PutChat(ctx, c))

	// Forward moves and reports advanced.
	advanced, err := s.AdvanceLastActivity(ctx, c.ID, at(200))
	require.NoError(t, err)
	require.True(t, advanced)
	got, err := s.GetChatByID(ctx, c.ID)
	require.NoError(t, err)
	require.True(t, got.LastActivity.Equal(at(200)))

	// Backward is a no-op and reports not advanced.
	advanced, err = s.AdvanceLastActivity(ctx, c.ID, at(150))
	require.NoError(t, err)
	require.False(t, advanced)
	got, err = s.GetChatByID(ctx, c.ID)
	require.NoError(t, err)
	require.True(t, got.LastActivity.Equal(at(200)))

	// Unknown chat → ErrChatNotFound.
	_, err = s.AdvanceLastActivity(ctx, generateChatID(), at(1))
	require.ErrorIs(t, err, chat.ErrChatNotFound)
}

func testStore_GetDmsForUserByLastActivity_Order(t *testing.T, s chat.Store) {
	ctx := context.Background()

	user := model.MustGenerateUserID()
	other := model.MustGenerateUserID()

	// Three chats the user is in, plus one they are not.
	c1 := putChat(t, s, user, other, at(100))
	c2 := putChat(t, s, user, other, at(300))
	c3 := putChat(t, s, user, other, at(200))
	_ = putChat(t, s, model.MustGenerateUserID(), model.MustGenerateUserID(), at(999))

	// Descending by last_activity (most recent first).
	got, err := s.GetDmsForUserByLastActivity(ctx, user, database.WithDescending())
	require.NoError(t, err)
	require.Equal(t, [][]byte{c2.ID.Value, c3.ID.Value, c1.ID.Value}, chatIDValues(got))

	// Ascending.
	got, err = s.GetDmsForUserByLastActivity(ctx, user, database.WithAscending())
	require.NoError(t, err)
	require.Equal(t, [][]byte{c1.ID.Value, c3.ID.Value, c2.ID.Value}, chatIDValues(got))
}

func testStore_GetDmsForUserByLastActivity_Paging(t *testing.T, s chat.Store) {
	ctx := context.Background()

	user := model.MustGenerateUserID()
	other := model.MustGenerateUserID()
	c1 := putChat(t, s, user, other, at(100))
	c2 := putChat(t, s, user, other, at(300))
	c3 := putChat(t, s, user, other, at(200))

	// Page 1: most recent, limit 2 → [c2, c3].
	page1, err := s.GetDmsForUserByLastActivity(ctx, user, database.WithDescending(), database.WithLimit(2))
	require.NoError(t, err)
	require.Equal(t, [][]byte{c2.ID.Value, c3.ID.Value}, chatIDValues(page1))

	// Page 2: resume after the last chat of page 1 (c3) → [c1].
	token := &commonpb.PagingToken{Value: page1[len(page1)-1].ID.Value}
	page2, err := s.GetDmsForUserByLastActivity(ctx, user, database.WithDescending(), database.WithLimit(2), database.WithPagingToken(token))
	require.NoError(t, err)
	require.Equal(t, [][]byte{c1.ID.Value}, chatIDValues(page2))

	// Resuming after the final chat yields an empty page.
	token = &commonpb.PagingToken{Value: c1.ID.Value}
	page3, err := s.GetDmsForUserByLastActivity(ctx, user, database.WithDescending(), database.WithPagingToken(token))
	require.NoError(t, err)
	require.Empty(t, page3)
}

func testStore_GetDmsForUserByLastActivity_Empty(t *testing.T, s chat.Store) {
	ctx := context.Background()

	got, err := s.GetDmsForUserByLastActivity(ctx, model.MustGenerateUserID(), database.WithDescending())
	require.NoError(t, err)
	require.Empty(t, got)
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
