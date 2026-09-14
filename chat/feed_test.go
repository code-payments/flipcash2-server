package chat

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	chatpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/chat/v1"
	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"

	"github.com/code-payments/flipcash2-server/model"
)

func TestGroupFeedToken_RoundTrip(t *testing.T) {
	window := []*commonpb.ChatId{MustGenerateGroupChatID(), MustGenerateGroupChatID()}
	in := &groupFeedToken{
		Snapshot: time.Unix(1_700_000_000, 123).UTC(),
		Window:   window,
	}
	out, ok := decodeGroupFeedToken(encodeGroupFeedToken(in))
	require.True(t, ok)
	require.True(t, out.Snapshot.Equal(in.Snapshot))
	require.Len(t, out.Window, len(window))
	for i := range window {
		require.Equal(t, window[i].Value, out.Window[i].Value)
	}

	// An empty window round-trips to an empty — not nil — window: a decoded
	// token always resumes, never recomputes.
	out, ok = decodeGroupFeedToken(encodeGroupFeedToken(&groupFeedToken{Snapshot: time.Now().UTC()}))
	require.True(t, ok)
	require.NotNil(t, out.Window)
	require.Empty(t, out.Window)
}

func TestGroupFeedToken_RejectsMalformed(t *testing.T) {
	valid := encodeGroupFeedToken(&groupFeedToken{
		Snapshot: time.Now().UTC(),
		Window:   []*commonpb.ChatId{MustGenerateGroupChatID()},
	}).Value

	mutate := func(fn func(b []byte) []byte) *commonpb.PagingToken {
		return &commonpb.PagingToken{Value: fn(append([]byte(nil), valid...))}
	}

	for name, token := range map[string]*commonpb.PagingToken{
		"nil":           nil,
		"too short":     mutate(func(b []byte) []byte { return b[:groupFeedTokenHeaderLen-1] }),
		"wrong version": mutate(func(b []byte) []byte { b[0] = groupFeedTokenVersion + 1; return b }),
		"partial id":    mutate(func(b []byte) []byte { return b[:len(b)-1] }),
		"repeated id":   mutate(func(b []byte) []byte { return append(b, b[len(b)-GroupChatIDSize:]...) }),
		"over the bound": mutate(func(b []byte) []byte {
			return append(b, bytes.Repeat([]byte{0}, maxGroupFeedChats*GroupChatIDSize)...)
		}),
	} {
		_, ok := decodeGroupFeedToken(token)
		require.False(t, ok, name)
	}

	_, ok := decodeGroupFeedToken(&commonpb.PagingToken{Value: valid})
	require.True(t, ok)
}

// fakeFeedStore is the slice of Store the page algorithm reads: a fixed set of
// groups and, per user, which of them they are joined to. Every other method
// panics through the nil embedded Store.
type fakeFeedStore struct {
	Store

	chats  map[string]*Chat
	joined map[string]bool // keyed by chat ID

	fullReads int // GetGroupChatsForUser calls
}

func (f *fakeFeedStore) GetGroupChatsForUser(context.Context, *commonpb.UserId) ([]*Chat, error) {
	f.fullReads++
	var out []*Chat
	for id, c := range f.chats {
		if f.joined[id] {
			out = append(out, c.Clone())
		}
	}
	return out, nil
}

func (f *fakeFeedStore) GetGroupChatsForUserByIDs(_ context.Context, _ *commonpb.UserId, chatIDs []*commonpb.ChatId) ([]*Chat, error) {
	var out []*Chat
	for _, id := range chatIDs {
		if c, ok := f.chats[string(id.Value)]; ok && f.joined[string(id.Value)] {
			out = append(out, c.Clone())
		}
	}
	return out, nil
}

// newFeedFixture returns a server over n groups with last activity 1..n
// seconds after a fixed epoch, so feed order is n, n-1, ..., 1, and the groups
// indexed the same way (groups[i] is active at i+1 seconds).
func newFeedFixture(t *testing.T, n, cap int) (*Server, *fakeFeedStore, []*Chat) {
	t.Helper()
	store := &fakeFeedStore{chats: make(map[string]*Chat), joined: make(map[string]bool)}
	groups := make([]*Chat, n)
	for i := range groups {
		c := &Chat{
			ID:           MustGenerateGroupChatID(),
			Type:         chatpb.ChatType_GROUP,
			LastActivity: time.Unix(1_700_000_000+int64(i+1), 0).UTC(),
		}
		groups[i] = c
		store.chats[string(c.ID.Value)] = c
		store.joined[string(c.ID.Value)] = true
	}
	return &Server{chats: store, maxGroupFeedChats: cap}, store, groups
}

func chatIDsOf(chats []*Chat) [][]byte {
	out := make([][]byte, len(chats))
	for i, c := range chats {
		out[i] = c.ID.Value
	}
	return out
}

func rawIDs(ids []*commonpb.ChatId) [][]byte {
	out := make([][]byte, len(ids))
	for i, id := range ids {
		out[i] = id.Value
	}
	return out
}

// TestGroupFeedPage_Walk pages a feed to exhaustion, pinning that the order is
// computed exactly once and every later page is served from the window.
func TestGroupFeedPage_Walk(t *testing.T) {
	ctx := context.Background()
	userID := model.MustGenerateUserID()
	s, store, g := newFeedFixture(t, 5, 1000)
	snapshot := time.Unix(1_700_000_100, 0).UTC()

	// Page 1 computes the order, emits [5, 4] and windows [3, 2, 1].
	page, next, err := s.groupFeedPage(ctx, userID, &groupFeedToken{Snapshot: snapshot}, 2)
	require.NoError(t, err)
	require.Equal(t, chatIDsOf([]*Chat{g[4], g[3]}), chatIDsOf(page))
	require.Equal(t, chatIDsOf([]*Chat{g[2], g[1], g[0]}), rawIDs(next.Window))
	require.Equal(t, 1, store.fullReads)

	// Page 2 is served from the window.
	page, next, err = s.groupFeedPage(ctx, userID, next, 2)
	require.NoError(t, err)
	require.Equal(t, chatIDsOf([]*Chat{g[2], g[1]}), chatIDsOf(page))
	require.Equal(t, chatIDsOf([]*Chat{g[0]}), rawIDs(next.Window))
	require.Equal(t, 1, store.fullReads)

	// Page 3 drains it.
	page, next, err = s.groupFeedPage(ctx, userID, next, 2)
	require.NoError(t, err)
	require.Equal(t, chatIDsOf([]*Chat{g[0]}), chatIDsOf(page))
	require.Empty(t, next.Window)
	require.Equal(t, 1, store.fullReads)

	// A further page from a spent token is empty and reads nothing.
	page, next, err = s.groupFeedPage(ctx, userID, next, 2)
	require.NoError(t, err)
	require.Empty(t, page)
	require.Empty(t, next.Window)
	require.Equal(t, 1, store.fullReads)
}

// TestGroupFeedPage_FitsInOnePage pins the common case: a feed no larger than
// the page is one read, one page, and an empty window.
func TestGroupFeedPage_FitsInOnePage(t *testing.T) {
	ctx := context.Background()
	s, store, g := newFeedFixture(t, 3, 1000)

	page, next, err := s.groupFeedPage(ctx, model.MustGenerateUserID(), &groupFeedToken{Snapshot: time.Now().UTC()}, 100)
	require.NoError(t, err)
	require.Equal(t, chatIDsOf([]*Chat{g[2], g[1], g[0]}), chatIDsOf(page))
	require.Empty(t, next.Window)
	require.Equal(t, 1, store.fullReads)
}

// TestGroupFeedPage_DropsDisqualified pins that a window ID which no longer
// qualifies is dropped, and the page filled from what the window still holds.
func TestGroupFeedPage_DropsDisqualified(t *testing.T) {
	ctx := context.Background()
	userID := model.MustGenerateUserID()
	s, store, g := newFeedFixture(t, 5, 1000)
	snapshot := time.Unix(1_700_000_100, 0).UTC()

	page, next, err := s.groupFeedPage(ctx, userID, &groupFeedToken{Snapshot: snapshot}, 1)
	require.NoError(t, err)
	require.Equal(t, chatIDsOf([]*Chat{g[4]}), chatIDsOf(page))
	require.Len(t, next.Window, 4)

	// The user leaves one windowed group, and another goes active after the
	// snapshot.
	store.joined[string(g[3].ID.Value)] = false
	g[2].LastActivity = snapshot.Add(time.Second)

	// Of the three the page reaches for, two are dropped; the page is filled
	// from the rest of the window rather than left short.
	page, next, err = s.groupFeedPage(ctx, userID, next, 3)
	require.NoError(t, err)
	require.Equal(t, chatIDsOf([]*Chat{g[1], g[0]}), chatIDsOf(page))
	require.Empty(t, next.Window)
	require.Equal(t, 1, store.fullReads)
}

// TestGroupFeedPage_TooLarge pins that a feed past the cap is refused on the
// first page rather than served in part, and that the cap counts the whole
// feed within the snapshot, not what is left after the first page.
func TestGroupFeedPage_TooLarge(t *testing.T) {
	ctx := context.Background()
	userID := model.MustGenerateUserID()
	s, _, g := newFeedFixture(t, 4, 3)
	snapshot := time.Unix(1_700_000_100, 0).UTC()

	_, _, err := s.groupFeedPage(ctx, userID, &groupFeedToken{Snapshot: snapshot}, 100)
	require.ErrorIs(t, err, ErrGroupFeedTooLarge)

	// A group above the snapshot is outside the feed, and so outside the cap.
	page, next, err := s.groupFeedPage(ctx, userID, &groupFeedToken{Snapshot: g[2].LastActivity}, 100)
	require.NoError(t, err)
	require.Equal(t, chatIDsOf([]*Chat{g[2], g[1], g[0]}), chatIDsOf(page))
	require.Empty(t, next.Window)
}
