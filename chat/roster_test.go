package chat

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"

	"github.com/code-payments/flipcash2-server/model"
)

func TestRosterToken_RoundTrip(t *testing.T) {
	chatID := MustGenerateGroupChatID()
	in := RosterPosition{
		JoinedAt: time.Unix(1_700_000_000, 123).UTC(),
		UserID:   model.MustGenerateUserID(),
	}
	out, ok := decodeRosterToken(encodeRosterToken(chatID, in), chatID)
	require.True(t, ok)
	require.True(t, out.JoinedAt.Equal(in.JoinedAt))
	require.Equal(t, in.UserID.Value, out.UserID.Value)

	// A DM participant's position has no join time, which survives the trip
	// as exactly that: the zero time, not the epoch.
	dmID := &commonpb.ChatId{Value: bytes.Repeat([]byte{7}, DmChatIDSize)}
	in = RosterPosition{UserID: model.MustGenerateUserID()}
	out, ok = decodeRosterToken(encodeRosterToken(dmID, in), dmID)
	require.True(t, ok)
	require.True(t, out.JoinedAt.IsZero())
	require.Equal(t, in.UserID.Value, out.UserID.Value)
}

func TestRosterToken_BoundToChat(t *testing.T) {
	chatID := MustGenerateGroupChatID()
	token := encodeRosterToken(chatID, RosterPosition{JoinedAt: time.Now(), UserID: model.MustGenerateUserID()})

	_, ok := decodeRosterToken(token, MustGenerateGroupChatID())
	require.False(t, ok)

	// A DM whose leading bytes match the group's is another chat too.
	dmID := &commonpb.ChatId{Value: append(append([]byte(nil), chatID.Value...), bytes.Repeat([]byte{1}, DmChatIDSize-GroupChatIDSize)...)}
	_, ok = decodeRosterToken(encodeRosterToken(dmID, RosterPosition{UserID: model.MustGenerateUserID()}), chatID)
	require.True(t, ok, "a fixed-width token binds a DM by its leading bytes, which is as much as it carries")
	_, ok = decodeRosterToken(encodeRosterToken(dmID, RosterPosition{UserID: model.MustGenerateUserID()}), &commonpb.ChatId{Value: bytes.Repeat([]byte{9}, DmChatIDSize)})
	require.False(t, ok)
}

func TestRosterToken_RejectsMalformed(t *testing.T) {
	chatID := MustGenerateGroupChatID()
	valid := encodeRosterToken(chatID, RosterPosition{JoinedAt: time.Now(), UserID: model.MustGenerateUserID()}).Value

	for name, value := range map[string][]byte{
		"empty":         nil,
		"truncated":     valid[:len(valid)-1],
		"extended":      append(append([]byte(nil), valid...), 0),
		"other version": append([]byte{rosterTokenVersion + 1}, valid[1:]...),
	} {
		_, ok := decodeRosterToken(&commonpb.PagingToken{Value: value}, chatID)
		require.False(t, ok, name)
	}
}

// TestRosterPage_ShapesAgree pins that the two shapes of a roster page (see
// roster.go) are one contract: walked from the top under either, and
// switched between mid-walk, they emit the same pages, the same has_more,
// and resume from the same cursor.
func TestRosterPage_ShapesAgree(t *testing.T) {
	ctx := context.Background()
	const n = 7
	store := newRosterFixture(n)
	c := &Chat{ID: store.chatID}

	whole := &Server{chats: store, rosterWholeReadCap: n}
	paged := &Server{chats: store, rosterWholeReadCap: 0}

	walk := func(s *Server, limit int) (pages [][][]byte, summaries []RosterSummary) {
		var after *RosterPosition
		for {
			summary, members, hasMore, err := s.rosterPage(ctx, c, after, limit)
			require.NoError(t, err)
			require.LessOrEqual(t, len(members), limit)
			ids := make([][]byte, len(members))
			for i, m := range members {
				ids[i] = m.UserID.Value
			}
			pages = append(pages, ids)
			summaries = append(summaries, summary)
			if !hasMore {
				return pages, summaries
			}
			require.NotEmpty(t, members, "a page with more after it is never empty")
			pos := members[len(members)-1].Position()
			after = &pos
		}
	}

	for _, limit := range []int{1, 3, n, n + 1} {
		wholePages, wholeSummaries := walk(whole, limit)
		pagedPages, pagedSummaries := walk(paged, limit)
		require.Equal(t, wholePages, pagedPages, "limit %d", limit)
		require.Equal(t, wholeSummaries, pagedSummaries, "limit %d", limit)

		var flat [][]byte
		for _, p := range wholePages {
			flat = append(flat, p...)
		}
		require.Equal(t, store.newestFirst(), flat, "limit %d", limit)
		require.Equal(t, (n+limit-1)/limit, len(wholePages), "limit %d", limit)
	}

	// Whole reads pay the roster per page; the paged shape pays the page.
	require.Positive(t, store.wholeReads)
	require.Positive(t, store.pageReads)

	// Switching shape mid-walk — the group crossing the cap between pages —
	// resumes cleanly: the first page under one, the rest under the other.
	_, first, hasMore, err := whole.rosterPage(ctx, c, nil, 3)
	require.NoError(t, err)
	require.True(t, hasMore)
	pos := first[2].Position()
	_, rest, hasMore, err := paged.rosterPage(ctx, c, &pos, n)
	require.NoError(t, err)
	require.False(t, hasMore)
	require.Len(t, rest, n-3)
	require.Equal(t, store.newestFirst()[3], rest[0].UserID.Value)
}

// rosterFixtureStore is the slice of Store a roster page reads: one group's
// summary and members. Every other method panics through the nil embedded
// Store.
type rosterFixtureStore struct {
	Store

	chatID  *commonpb.ChatId
	members []GroupMember // in roster order

	wholeReads, pageReads int
}

// newRosterFixture is a group of n members joined a second apart, the
// founder first, so roster order is the reverse of join index.
func newRosterFixture(n int) *rosterFixtureStore {
	f := &rosterFixtureStore{chatID: MustGenerateGroupChatID()}
	for i := range n {
		f.members = append(f.members, GroupMember{
			UserID:   model.MustGenerateUserID(),
			JoinedAt: time.Unix(1_700_000_000+int64(i), 0).UTC(),
			Version:  uint64(i),
		})
	}
	SortRoster(f.members)
	return f
}

func (f *rosterFixtureStore) newestFirst() [][]byte {
	out := make([][]byte, len(f.members))
	for i, m := range f.members {
		out[i] = m.UserID.Value
	}
	return out
}

func (f *rosterFixtureStore) summary() RosterSummary {
	return RosterSummary{MemberCount: uint64(len(f.members)), Version: uint64(len(f.members) - 1)}
}

func (f *rosterFixtureStore) GetGroupRosterSummary(context.Context, *commonpb.ChatId) (RosterSummary, error) {
	return f.summary(), nil
}

func (f *rosterFixtureStore) GetGroupRoster(context.Context, *commonpb.ChatId) (RosterSummary, []GroupMember, error) {
	f.wholeReads++
	// Shuffled relative to roster order, as a partition walk returns them.
	out := make([]GroupMember, 0, len(f.members))
	for i := len(f.members) - 1; i >= 0; i-- {
		out = append(out, f.members[i])
	}
	return f.summary(), out, nil
}

func (f *rosterFixtureStore) GetGroupRosterPage(_ context.Context, _ *commonpb.ChatId, after *RosterPosition, limit int) ([]GroupMember, error) {
	f.pageReads++
	out := make([]GroupMember, 0)
	for _, m := range f.members {
		if after != nil && m.Position().Compare(*after) <= 0 {
			continue
		}
		out = append(out, m)
		if limit > 0 && len(out) == limit {
			break
		}
	}
	return out, nil
}
