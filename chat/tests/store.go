package tests

import (
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	blobpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/blob/v1"
	chatpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/chat/v1"
	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"
	messagingpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/messaging/v1"

	"github.com/code-payments/flipcash2-server/chat"
	"github.com/code-payments/flipcash2-server/model"
)

// RunStoreTests runs the shared chat.Store test suite against s, a store
// excluding no one from the feed, and stores newStore builds excluding the
// users given (see chat.FeedExclusions). teardown is called between tests to
// reset the store; the stores newStore builds may share s's storage, and every
// test using them names chats and users of its own.
func RunStoreTests(t *testing.T, s chat.Store, newStore func(excludedFromFeed []*commonpb.UserId) chat.Store, teardown func()) {
	for _, tf := range []func(t *testing.T, s chat.Store){
		testStore_PutAndGet,
		testStore_PutChat_Duplicate,
		testStore_PutChat_TypeIDMismatch,
		testStore_PutChat_NoMembers,
		testStore_GetChatByID_NotFound,
		testStore_Members,
		testStore_IsMember,
		testStore_AdvanceLastMessage,
		func(t *testing.T, _ chat.Store) { testStore_FeedExclusions(t, newStore) },
		testStore_GroupChat_PutAndGet,
		testStore_GroupChat_StaffOnly,
		testStore_GroupChat_MinimumListenerBalance,
		testStore_GroupChat_Rules,
		testStore_GroupChat_Creator,
		testStore_GroupChat_Picture,
		testStore_GroupChat_Edit,
		testStore_GroupChat_Membership,
		testStore_GroupChat_MembersPage,
		testStore_GroupChat_MembersPage_MuteOrder,
		testStore_GroupChat_Roster,
		testStore_GroupChat_RosterPage,
		testStore_GroupChat_MemberRecords,
		testStore_GroupChat_RosterSummary,
		testStore_GroupChat_ConcurrentTransitions,
		testStore_GroupChat_MembershipsForUser,
		testStore_GroupChat_ChatsForUser,
		testStore_GroupChat_ChatsForUserByIDs,
		testStore_GroupChat_CreationCap,
		testStore_GroupChat_DuplicateMembers,
		testStore_GroupChat_AddMembersErrors,
		testStore_GroupChat_AdvanceLastMessage,
		testStore_GroupChat_NotInDmFeed,
		testStore_GetDmFeedPage_Order,
		testStore_GetDmFeedPage_Watermark,
		testStore_GetDmFeedPage_Paging,
		testStore_GetDmFeedPage_SnapshotPinned,
		testStore_GetDmFeedPage_Empty,
		testStore_GetDmFeedPage_TypeScoped,
		testStore_UserState_Empty,
		testStore_UserState_SetMute_Until,
		testStore_UserState_SetMute_Forever,
		testStore_UserState_SetMute_Idempotent,
		testStore_UserState_SetMute_Replace,
		testStore_UserState_ClearMute,
		testStore_UserState_OutOfRange,
		testStore_UserState_GetViewerStates_Batch,
		testStore_UserState_GetViewerStates_Bounded,
		testStore_UserState_GetMutedUsers,
		testStore_UserState_GetMutedUsersPage_Bounds,
		testStore_UserState_MutedCount,
		testStore_Activity_RecentSenders,
		testStore_Activity_Throttle,
		testStore_Activity_Precision,
		testStore_Activity_Scope,
		testStore_Activity_Concurrent,
		testStore_KeyEnvelope_SetAndGet,
		testStore_KeyEnvelope_OwnWrapStands,
		testStore_KeyEnvelope_DiscardedOnLeave,
		testStore_KeyEnvelope_Concurrent,
		testStore_Lobby_EnterAndLeave,
		testStore_Lobby_Limits,
		testStore_Lobby_Page,
		testStore_Lobby_Admit,
		testStore_Lobby_Concurrent,
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
		ID:           generateDmChatID(),
		Type:         chatpb.ChatType_CONTACT_DM,
		Members:      []*commonpb.UserId{userA, userB},
		LastActivity: at(100),
	}
	require.NoError(t, s.PutChat(ctx, c))

	got, err := s.GetChatByID(ctx, c.ID)
	require.NoError(t, err)
	require.Equal(t, c.ID.Value, got.ID.Value)
	require.Equal(t, chatpb.ChatType_CONTACT_DM, got.Type)
	require.True(t, got.LastActivity.Equal(at(100)))
	require.ElementsMatch(t, userIDValues(c.Members), userIDValues(got.Members))
	require.Equal(t, chat.RosterSummary{MemberCount: 2}, got.RosterSummary)

	// The feed read carries the summary too.
	feed, err := s.GetDmFeedPage(ctx, userA, chatpb.ChatType_CONTACT_DM, at(1000), nil, 0)
	require.NoError(t, err)
	require.Len(t, feed, 1)
	require.Equal(t, chat.RosterSummary{MemberCount: 2}, feed[0].RosterSummary)
}

func testStore_PutChat_Duplicate(t *testing.T, s chat.Store) {
	ctx := context.Background()

	c := &chat.Chat{
		ID:           generateDmChatID(),
		Type:         chatpb.ChatType_CONTACT_DM,
		Members:      []*commonpb.UserId{model.MustGenerateUserID(), model.MustGenerateUserID()},
		LastActivity: at(1),
	}
	require.NoError(t, s.PutChat(ctx, c))
	require.ErrorIs(t, s.PutChat(ctx, c), chat.ErrChatExists)
}

func testStore_GetChatByID_NotFound(t *testing.T, s chat.Store) {
	ctx := context.Background()

	_, err := s.GetChatByID(ctx, generateDmChatID())
	require.ErrorIs(t, err, chat.ErrChatNotFound)

	_, err = s.GetMembers(ctx, generateDmChatID())
	require.ErrorIs(t, err, chat.ErrChatNotFound)
}

func testStore_Members(t *testing.T, s chat.Store) {
	ctx := context.Background()

	userA := model.MustGenerateUserID()
	userB := model.MustGenerateUserID()
	c := &chat.Chat{
		ID:           generateDmChatID(),
		Type:         chatpb.ChatType_CONTACT_DM,
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
		ID:           generateDmChatID(),
		Type:         chatpb.ChatType_CONTACT_DM,
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
	ok, err = s.IsMember(ctx, generateDmChatID(), userA)
	require.NoError(t, err)
	require.False(t, ok)
}

func testStore_AdvanceLastMessage(t *testing.T, s chat.Store) {
	ctx := context.Background()

	member := model.MustGenerateUserID()
	c := &chat.Chat{
		ID:           generateDmChatID(),
		Type:         chatpb.ChatType_CONTACT_DM,
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
	_, members, err = s.AdvanceLastMessage(ctx, generateDmChatID(), &messagingpb.MessageId{Value: 1}, at(1))
	require.ErrorIs(t, err, chat.ErrChatNotFound)
	require.Nil(t, members)
}

// testStore_FeedExclusions runs against stores newStore builds, since what it
// tests is how a store is built.
func testStore_FeedExclusions(t *testing.T, newStore func(excludedFromFeed []*commonpb.UserId) chat.Store) {
	ctx := context.Background()

	// Two accounts excluded, one of them named twice: a user named more than
	// once is excluded once.
	team := model.MustGenerateUserID()
	support := model.MustGenerateUserID()
	s := newStore([]*commonpb.UserId{team, team, support})

	// feed is the member's own DM feed in s, whole.
	feed := func(s chat.Store, member *commonpb.UserId) []*chat.Chat {
		t.Helper()
		page, err := s.GetDmFeedPage(ctx, member, chatpb.ChatType_DM, at(1000), nil, 0)
		require.NoError(t, err)
		return page
	}

	// A DM with the configured user excludes them from the feed: they are a
	// member whose feed does not list it, while the other member's does.
	user := model.MustGenerateUserID()
	c := &chat.Chat{
		ID:           generateDmChatID(),
		Type:         chatpb.ChatType_DM,
		Members:      []*commonpb.UserId{user, team},
		LastActivity: at(100),
	}
	require.NoError(t, s.PutChat(ctx, c))
	require.Empty(t, feed(s, team))
	require.Len(t, feed(s, user), 1)
	isMember, err := s.IsMember(ctx, c.ID, team)
	require.NoError(t, err)
	require.True(t, isMember)

	// Every advance moves the record and the other member's feed, and still
	// lists nothing for the excluded member.
	for i, ts := range []time.Time{at(200), at(300)} {
		advanced, members, err := s.AdvanceLastMessage(ctx, c.ID, &messagingpb.MessageId{Value: uint64(i + 1)}, ts)
		require.NoError(t, err)
		require.True(t, advanced)
		require.Len(t, members, 2)

		got, err := s.GetChatByID(ctx, c.ID)
		require.NoError(t, err)
		require.True(t, got.LastActivity.Equal(ts))
		userFeed := feed(s, user)
		require.Len(t, userFeed, 1)
		require.True(t, userFeed[0].LastActivity.Equal(ts))
		require.Equal(t, uint64(i+1), userFeed[0].LastMessageID.GetValue())
		require.Empty(t, feed(s, team))
	}

	// A DM between two excluded users excludes both: neither feed lists it,
	// both are members, and an advance still moves the record.
	both := &chat.Chat{
		ID:           generateDmChatID(),
		Type:         chatpb.ChatType_DM,
		Members:      []*commonpb.UserId{team, support},
		LastActivity: at(100),
	}
	require.NoError(t, s.PutChat(ctx, both))
	require.Empty(t, feed(s, team))
	require.Empty(t, feed(s, support))
	for _, member := range []*commonpb.UserId{team, support} {
		isMember, err := s.IsMember(ctx, both.ID, member)
		require.NoError(t, err)
		require.True(t, isMember)
	}
	advanced, _, err := s.AdvanceLastMessage(ctx, both.ID, &messagingpb.MessageId{Value: 1}, at(200))
	require.NoError(t, err)
	require.True(t, advanced)
	got, err := s.GetChatByID(ctx, both.ID)
	require.NoError(t, err)
	require.True(t, got.LastActivity.Equal(at(200)))
	require.Empty(t, feed(s, team))
	require.Empty(t, feed(s, support))

	// A DM without them excludes no one.
	a, b := model.MustGenerateUserID(), model.MustGenerateUserID()
	plain := &chat.Chat{
		ID:           generateDmChatID(),
		Type:         chatpb.ChatType_DM,
		Members:      []*commonpb.UserId{a, b},
		LastActivity: at(100),
	}
	require.NoError(t, s.PutChat(ctx, plain))
	require.Len(t, feed(s, a), 1)
	require.Len(t, feed(s, b), 1)

	// A group they are created in is created as for anyone: a group keeps no
	// per-member feed copies to exclude them from.
	group := &chat.Chat{
		ID:           chat.MustGenerateGroupChatID(),
		Type:         chatpb.ChatType_GROUP,
		Members:      []*commonpb.UserId{a, team},
		LastActivity: at(100),
	}
	require.NoError(t, s.PutChat(ctx, group))
	isMember, err = s.IsMember(ctx, group.ID, team)
	require.NoError(t, err)
	require.True(t, isMember)

	// A nil user ID, what a parent with no team account configured passes,
	// excludes no one.
	none := newStore([]*commonpb.UserId{nil})
	other := model.MustGenerateUserID()
	listed := &chat.Chat{
		ID:           generateDmChatID(),
		Type:         chatpb.ChatType_DM,
		Members:      []*commonpb.UserId{other, team},
		LastActivity: at(100),
	}
	require.NoError(t, none.PutChat(ctx, listed))
	require.Len(t, feed(none, other), 1)
	teamFeed := feed(none, team)
	require.Len(t, teamFeed, 1)
	require.Equal(t, listed.ID.Value, teamFeed[0].ID.Value)
}

func testStore_PutChat_TypeIDMismatch(t *testing.T, s chat.Store) {
	ctx := context.Background()

	// A chat ID's length is its type family's discriminator, so a group type
	// with a DM-sized ID must be rejected...
	err := s.PutChat(ctx, &chat.Chat{
		ID:           generateDmChatID(),
		Type:         chatpb.ChatType_GROUP,
		Members:      []*commonpb.UserId{model.MustGenerateUserID()},
		LastActivity: at(1),
	})
	require.Error(t, err)
	require.NotErrorIs(t, err, chat.ErrChatExists)

	// ...and so must a DM type with a group-sized ID.
	err = s.PutChat(ctx, &chat.Chat{
		ID:           chat.MustGenerateGroupChatID(),
		Type:         chatpb.ChatType_CONTACT_DM,
		Members:      []*commonpb.UserId{model.MustGenerateUserID(), model.MustGenerateUserID()},
		LastActivity: at(1),
	})
	require.Error(t, err)
	require.NotErrorIs(t, err, chat.ErrChatExists)
}

func testStore_GroupChat_PutAndGet(t *testing.T, s chat.Store) {
	ctx := context.Background()

	members := []*commonpb.UserId{
		model.MustGenerateUserID(),
		model.MustGenerateUserID(),
		model.MustGenerateUserID(),
	}
	c := putGroupChat(t, s, "Weekend Trip", at(100), members...)

	// The metadata read returns the canonical record alone: a group's membership
	// lives in its own records and is never joined into a metadata read.
	got, err := s.GetChatByID(ctx, c.ID)
	require.NoError(t, err)
	require.Equal(t, c.ID.Value, got.ID.Value)
	require.Equal(t, chatpb.ChatType_GROUP, got.Type)
	require.Equal(t, "Weekend Trip", got.Title)
	require.True(t, got.LastActivity.Equal(at(100)))
	require.Empty(t, got.Members)

	// Membership is reached explicitly, and PutChat's initial set is what it
	// converges to.
	gotMembers, err := s.GetMembers(ctx, c.ID)
	require.NoError(t, err)
	require.ElementsMatch(t, userIDValues(members), userIDValues(gotMembers))

	require.ErrorIs(t, s.PutChat(ctx, c), chat.ErrChatExists)
}

func testStore_GroupChat_StaffOnly(t *testing.T, s chat.Store) {
	ctx := context.Background()

	// A group is not staff-only unless it was created as such.
	plain := putGroupChat(t, s, "Weekend Trip", at(100), model.MustGenerateUserID())
	got, err := s.GetChatByID(ctx, plain.ID)
	require.NoError(t, err)
	require.False(t, got.IsStaffOnly)

	staff := &chat.Chat{
		ID:           chat.MustGenerateGroupChatID(),
		Type:         chatpb.ChatType_GROUP,
		Members:      []*commonpb.UserId{model.MustGenerateUserID()},
		Title:        "Staff",
		IsStaffOnly:  true,
		LastActivity: at(100),
	}
	require.NoError(t, s.PutChat(ctx, staff))

	got, err = s.GetChatByID(ctx, staff.ID)
	require.NoError(t, err)
	require.True(t, got.IsStaffOnly)
	require.Equal(t, "Staff", got.Title)

	// The flag is part of the canonical record and survives the updates that
	// touch it.
	advanced, _, err := s.AdvanceLastMessage(ctx, staff.ID, &messagingpb.MessageId{Value: 1}, at(200))
	require.NoError(t, err)
	require.True(t, advanced)

	got, err = s.GetChatByID(ctx, staff.ID)
	require.NoError(t, err)
	require.True(t, got.IsStaffOnly)
	require.True(t, got.LastActivity.Equal(at(200)))
}

func testStore_GroupChat_MinimumListenerBalance(t *testing.T, s chat.Store) {
	ctx := context.Background()

	// A group has no minimum listener balance unless it was created with one.
	plain := putGroupChat(t, s, "Weekend Trip", at(100), model.MustGenerateUserID())
	got, err := s.GetChatByID(ctx, plain.ID)
	require.NoError(t, err)
	require.Nil(t, got.MinimumListenerBalance)

	// The requirement round-trips as stored: currency, an amount that is not a
	// whole number, and the mint it must be held in.
	usdfMint := &commonpb.PublicKey{Value: randomBytes(32)}
	gated := &chat.Chat{
		ID:      chat.MustGenerateGroupChatID(),
		Type:    chatpb.ChatType_GROUP,
		Members: []*commonpb.UserId{model.MustGenerateUserID()},
		Title:   "Whales",
		MinimumListenerBalance: &chat.MinimumBalance{
			Currency:     "usd",
			NativeAmount: 1234.56,
			Mints:        []*commonpb.PublicKey{usdfMint},
		},
		LastActivity: at(100),
	}
	require.NoError(t, s.PutChat(ctx, gated))

	got, err = s.GetChatByID(ctx, gated.ID)
	require.NoError(t, err)
	require.Equal(t, gated.MinimumListenerBalance, got.MinimumListenerBalance)
	require.False(t, got.IsStaffOnly)
	require.Equal(t, "Whales", got.Title)

	// The stored record is independent of the one read.
	got.MinimumListenerBalance.Mints[0].Value[0]++
	again, err := s.GetChatByID(ctx, gated.ID)
	require.NoError(t, err)
	require.Equal(t, usdfMint.Value, again.MinimumListenerBalance.Mints[0].Value)

	// No mints reads back as no mints — the encoding of "any mint" — and a
	// fractional amount with no short decimal form still round-trips exactly.
	anyMint := &chat.Chat{
		ID:                     chat.MustGenerateGroupChatID(),
		Type:                   chatpb.ChatType_GROUP,
		Members:                []*commonpb.UserId{model.MustGenerateUserID()},
		MinimumListenerBalance: &chat.MinimumBalance{Currency: "eur", NativeAmount: 0.1 + 0.2},
		LastActivity:           at(100),
	}
	require.NoError(t, s.PutChat(ctx, anyMint))

	got, err = s.GetChatByID(ctx, anyMint.ID)
	require.NoError(t, err)
	require.Equal(t, "eur", got.MinimumListenerBalance.Currency)
	require.Equal(t, 0.1+0.2, got.MinimumListenerBalance.NativeAmount)
	require.Empty(t, got.MinimumListenerBalance.Mints)

	// The requirement is part of the canonical record and survives the updates
	// that touch it.
	advanced, _, err := s.AdvanceLastMessage(ctx, gated.ID, &messagingpb.MessageId{Value: 1}, at(200))
	require.NoError(t, err)
	require.True(t, advanced)
	require.NoError(t, s.SetGroupPicture(ctx, gated.ID, &blobpb.BlobId{Value: randomBytes(16)}))

	got, err = s.GetChatByID(ctx, gated.ID)
	require.NoError(t, err)
	require.Equal(t, gated.MinimumListenerBalance, got.MinimumListenerBalance)
	require.True(t, got.LastActivity.Equal(at(200)))

	// A DM never carries one, whatever its record says.
	dm := &chat.Chat{
		ID:                     generateDmChatID(),
		Type:                   chatpb.ChatType_CONTACT_DM,
		Members:                []*commonpb.UserId{model.MustGenerateUserID(), model.MustGenerateUserID()},
		MinimumListenerBalance: gated.MinimumListenerBalance,
		LastActivity:           at(100),
	}
	require.NoError(t, s.PutChat(ctx, dm))
	got, err = s.GetChatByID(ctx, dm.ID)
	require.NoError(t, err)
	require.Nil(t, got.Rules())
}

func testStore_GroupChat_Rules(t *testing.T, s chat.Store) {
	ctx := context.Background()

	// A group without a requirement has no rules — nil, not an empty set —
	// and one written without a creator reads back with none.
	plain := putGroupChat(t, s, "Weekend Trip", at(100), model.MustGenerateUserID())
	got, err := s.GetGroupRules(ctx, plain.ID)
	require.NoError(t, err)
	require.Nil(t, got.Rules)
	require.Nil(t, got.CreatorID)

	// A staff-only group's rules are the projection of its record.
	staff := &chat.Chat{
		ID:           chat.MustGenerateGroupChatID(),
		Type:         chatpb.ChatType_GROUP,
		Members:      []*commonpb.UserId{model.MustGenerateUserID()},
		Title:        "Staff",
		IsStaffOnly:  true,
		LastActivity: at(100),
	}
	require.NoError(t, s.PutChat(ctx, staff))

	got, err = s.GetGroupRules(ctx, staff.ID)
	require.NoError(t, err)
	rules := got.Rules
	require.True(t, proto.Equal(staff.Rules(), rules))
	require.Len(t, rules.GetListener(), 1)
	require.NotNil(t, rules.GetListener()[0].GetStaff())
	require.Empty(t, rules.GetSpeaker())

	// So are a balance-gated group's, and a group with both requirements
	// projects both, staff first.
	usdfMint := &commonpb.PublicKey{Value: randomBytes(32)}
	gated := &chat.Chat{
		ID:      chat.MustGenerateGroupChatID(),
		Type:    chatpb.ChatType_GROUP,
		Members: []*commonpb.UserId{model.MustGenerateUserID()},
		Title:   "Whales",
		MinimumListenerBalance: &chat.MinimumBalance{
			Currency:     "usd",
			NativeAmount: 1000,
			Mints:        []*commonpb.PublicKey{usdfMint},
		},
		LastActivity: at(100),
	}
	require.NoError(t, s.PutChat(ctx, gated))

	got, err = s.GetGroupRules(ctx, gated.ID)
	require.NoError(t, err)
	rules = got.Rules
	require.True(t, proto.Equal(gated.Rules(), rules))
	require.Len(t, rules.GetListener(), 1)
	require.True(t, proto.Equal(gated.MinimumListenerBalance.ToProto(), rules.GetListener()[0].GetMinimumBalance()))
	require.Empty(t, rules.GetSpeaker())

	both := gated.Clone()
	both.ID = chat.MustGenerateGroupChatID()
	both.Members = []*commonpb.UserId{model.MustGenerateUserID()}
	both.IsStaffOnly = true
	require.NoError(t, s.PutChat(ctx, both))

	got, err = s.GetGroupRules(ctx, both.ID)
	require.NoError(t, err)
	rules = got.Rules
	require.True(t, proto.Equal(both.Rules(), rules))
	require.Len(t, rules.GetListener(), 2)
	require.NotNil(t, rules.GetListener()[0].GetStaff())
	require.NotNil(t, rules.GetListener()[1].GetMinimumBalance())
	require.Empty(t, rules.GetSpeaker())

	// A creator-only group carries a creator speaker rule beside its listener
	// rules. The rule names no one: the creator is read beside it.
	creator := model.MustGenerateUserID()
	creatorOnly := gated.Clone()
	creatorOnly.ID = chat.MustGenerateGroupChatID()
	creatorOnly.Members = []*commonpb.UserId{creator}
	creatorOnly.IsCreatorOnlySpeaker = true
	creatorOnly.CreatorID = creator
	require.NoError(t, s.PutChat(ctx, creatorOnly))

	got, err = s.GetGroupRules(ctx, creatorOnly.ID)
	require.NoError(t, err)
	rules = got.Rules
	require.Equal(t, creator.Value, got.CreatorID.GetValue())
	require.True(t, proto.Equal(creatorOnly.Rules(), rules))
	require.Len(t, rules.GetListener(), 1)
	require.NotNil(t, rules.GetListener()[0].GetMinimumBalance())
	require.Len(t, rules.GetSpeaker(), 1)
	require.NotNil(t, rules.GetSpeaker()[0].GetCreator())

	// The flag is part of the canonical record.
	record, err := s.GetChatByID(ctx, creatorOnly.ID)
	require.NoError(t, err)
	require.True(t, record.IsCreatorOnlySpeaker)
	require.Equal(t, creator.Value, record.CreatorID.GetValue())
	require.True(t, proto.Equal(creatorOnly.Rules(), record.Rules()))

	// A private group carries no rules, and reads back as private beside
	// them and on its canonical record. Every other group reads as not.
	private := &chat.Chat{
		ID:           chat.MustGenerateGroupChatID(),
		Type:         chatpb.ChatType_GROUP,
		Members:      []*commonpb.UserId{creator},
		Title:        "Private",
		IsPrivate:    true,
		CreatorID:    creator,
		LastActivity: at(100),
	}
	require.NoError(t, s.PutChat(ctx, private))
	got, err = s.GetGroupRules(ctx, private.ID)
	require.NoError(t, err)
	require.True(t, got.IsPrivate)
	require.Nil(t, got.Rules)
	require.Equal(t, creator.Value, got.CreatorID.GetValue())
	record, err = s.GetChatByID(ctx, private.ID)
	require.NoError(t, err)
	require.True(t, record.IsPrivate)
	got, err = s.GetGroupRules(ctx, creatorOnly.ID)
	require.NoError(t, err)
	require.False(t, got.IsPrivate)
	record, err = s.GetChatByID(ctx, creatorOnly.ID)
	require.NoError(t, err)
	require.False(t, record.IsPrivate)

	// The creator is read for any group that recorded one, whatever its rules.
	withCreator := &chat.Chat{
		ID:           chat.MustGenerateGroupChatID(),
		Type:         chatpb.ChatType_GROUP,
		Members:      []*commonpb.UserId{creator},
		Title:        "Open",
		CreatorID:    creator,
		LastActivity: at(100),
	}
	require.NoError(t, s.PutChat(ctx, withCreator))
	got, err = s.GetGroupRules(ctx, withCreator.ID)
	require.NoError(t, err)
	require.Nil(t, got.Rules)
	require.Equal(t, creator.Value, got.CreatorID.GetValue())

	// An unknown group is not found; a DM ID is not a group.
	_, err = s.GetGroupRules(ctx, chat.MustGenerateGroupChatID())
	require.ErrorIs(t, err, chat.ErrChatNotFound)

	dm := putDmChat(t, s, model.MustGenerateUserID(), model.MustGenerateUserID(), at(100))
	_, err = s.GetGroupRules(ctx, dm.ID)
	require.Error(t, err)
}

func testStore_GroupChat_Creator(t *testing.T, s chat.Store) {
	ctx := context.Background()

	// A group written without a creator reads back with none.
	plain := putGroupChat(t, s, "Weekend Trip", at(100), model.MustGenerateUserID())
	got, err := s.GetChatByID(ctx, plain.ID)
	require.NoError(t, err)
	require.Nil(t, got.CreatorID)

	// The creator is recorded at creation, independently of membership: here
	// the creator is not among the initial members, and the record still names
	// them without them appearing in the roster.
	creator := model.MustGenerateUserID()
	member := model.MustGenerateUserID()
	created := &chat.Chat{
		ID:           chat.MustGenerateGroupChatID(),
		Type:         chatpb.ChatType_GROUP,
		Members:      []*commonpb.UserId{member},
		Title:        "Created",
		CreatorID:    creator,
		LastActivity: at(100),
	}
	require.NoError(t, s.PutChat(ctx, created))

	got, err = s.GetChatByID(ctx, created.ID)
	require.NoError(t, err)
	require.NotNil(t, got.CreatorID)
	require.Equal(t, creator.Value, got.CreatorID.Value)
	members, err := s.GetMembers(ctx, created.ID)
	require.NoError(t, err)
	require.Equal(t, [][]byte{member.Value}, userIDValues(members))

	// The creator is part of the canonical record and survives the updates
	// that touch it.
	advanced, _, err := s.AdvanceLastMessage(ctx, created.ID, &messagingpb.MessageId{Value: 1}, at(200))
	require.NoError(t, err)
	require.True(t, advanced)
	require.NoError(t, s.SetGroupPicture(ctx, created.ID, &blobpb.BlobId{Value: []byte("picture-blob-creator")}))

	got, err = s.GetChatByID(ctx, created.ID)
	require.NoError(t, err)
	require.Equal(t, creator.Value, got.CreatorID.Value)
	require.True(t, got.LastActivity.Equal(at(200)))

	// A DM has no creator.
	dm := putDmChat(t, s, model.MustGenerateUserID(), model.MustGenerateUserID(), at(100))
	got, err = s.GetChatByID(ctx, dm.ID)
	require.NoError(t, err)
	require.Nil(t, got.CreatorID)
}

func testStore_GroupChat_Picture(t *testing.T, s chat.Store) {
	ctx := context.Background()

	// A group has no picture unless one was set.
	plain := putGroupChat(t, s, "No Picture", at(100), model.MustGenerateUserID())
	got, err := s.GetChatByID(ctx, plain.ID)
	require.NoError(t, err)
	require.Nil(t, got.PictureBlobID)

	// A picture given at creation is on the canonical record.
	original := &blobpb.BlobId{Value: []byte("picture-blob-0001")}
	withPicture := &chat.Chat{
		ID:            chat.MustGenerateGroupChatID(),
		Type:          chatpb.ChatType_GROUP,
		Members:       []*commonpb.UserId{model.MustGenerateUserID()},
		Title:         "With Picture",
		PictureBlobID: original,
		LastActivity:  at(100),
	}
	require.NoError(t, s.PutChat(ctx, withPicture))
	got, err = s.GetChatByID(ctx, withPicture.ID)
	require.NoError(t, err)
	require.NotNil(t, got.PictureBlobID)
	require.Equal(t, original.Value, got.PictureBlobID.Value)

	// Setting a picture on a group that had none, and replacing one that did.
	first := &blobpb.BlobId{Value: []byte("picture-blob-0002")}
	require.NoError(t, s.SetGroupPicture(ctx, plain.ID, first))
	got, err = s.GetChatByID(ctx, plain.ID)
	require.NoError(t, err)
	require.Equal(t, first.Value, got.PictureBlobID.Value)

	second := &blobpb.BlobId{Value: []byte("picture-blob-0003")}
	require.NoError(t, s.SetGroupPicture(ctx, plain.ID, second))
	got, err = s.GetChatByID(ctx, plain.ID)
	require.NoError(t, err)
	require.Equal(t, second.Value, got.PictureBlobID.Value)

	// The picture is part of the canonical record and survives the updates that
	// touch it; the rest of the record survives the picture update.
	advanced, _, err := s.AdvanceLastMessage(ctx, plain.ID, &messagingpb.MessageId{Value: 1}, at(200))
	require.NoError(t, err)
	require.True(t, advanced)
	got, err = s.GetChatByID(ctx, plain.ID)
	require.NoError(t, err)
	require.Equal(t, second.Value, got.PictureBlobID.Value)
	require.Equal(t, "No Picture", got.Title)
	require.True(t, got.LastActivity.Equal(at(200)))

	// A nil picture clears it.
	require.NoError(t, s.SetGroupPicture(ctx, plain.ID, nil))
	got, err = s.GetChatByID(ctx, plain.ID)
	require.NoError(t, err)
	require.Nil(t, got.PictureBlobID)
	require.Equal(t, "No Picture", got.Title)

	// Setting a picture on a group that does not exist is refused, and must not
	// leave a phantom record behind.
	unknown := chat.MustGenerateGroupChatID()
	require.ErrorIs(t, s.SetGroupPicture(ctx, unknown, first), chat.ErrChatNotFound)
	_, err = s.GetChatByID(ctx, unknown)
	require.ErrorIs(t, err, chat.ErrChatNotFound)

	// Pictures are group-only: a DM chat ID is rejected outright.
	dm := putDmChat(t, s, model.MustGenerateUserID(), model.MustGenerateUserID(), at(1))
	require.Error(t, s.SetGroupPicture(ctx, dm.ID, first))
	got, err = s.GetChatByID(ctx, dm.ID)
	require.NoError(t, err)
	require.Nil(t, got.PictureBlobID)
}

func testStore_GroupChat_Edit(t *testing.T, s chat.Store) {
	ctx := context.Background()

	creator := model.MustGenerateUserID()
	original := &blobpb.BlobId{Value: []byte("picture-blob-0001")}
	group := &chat.Chat{
		ID:                     chat.MustGenerateGroupChatID(),
		Type:                   chatpb.ChatType_GROUP,
		Members:                []*commonpb.UserId{creator},
		Title:                  "Before",
		IsStaffOnly:            true,
		MinimumListenerBalance: &chat.MinimumBalance{Currency: "usd", NativeAmount: 5},
		CreatorID:              creator,
		PictureBlobID:          original,
		LastActivity:           at(100),
	}
	require.NoError(t, s.PutChat(ctx, group))

	// The title alone: the picture and every other field are untouched.
	title := "After"
	require.NoError(t, s.EditGroup(ctx, group.ID, chat.GroupEdit{Title: &title}))
	got, err := s.GetChatByID(ctx, group.ID)
	require.NoError(t, err)
	require.Equal(t, "After", got.Title)
	require.Equal(t, original.Value, got.PictureBlobID.GetValue())
	require.True(t, got.IsStaffOnly)
	require.Equal(t, "usd", got.MinimumListenerBalance.Currency)
	require.Equal(t, float64(5), got.MinimumListenerBalance.NativeAmount)
	require.Equal(t, creator.Value, got.CreatorID.GetValue())
	require.True(t, got.LastActivity.Equal(at(100)))

	// The picture alone: the title stays edited.
	replacement := &blobpb.BlobId{Value: []byte("picture-blob-0002")}
	require.NoError(t, s.EditGroup(ctx, group.ID, chat.GroupEdit{PictureBlobID: replacement}))
	got, err = s.GetChatByID(ctx, group.ID)
	require.NoError(t, err)
	require.Equal(t, "After", got.Title)
	require.Equal(t, replacement.Value, got.PictureBlobID.GetValue())

	// Both at once, on a group that had no picture.
	plain := putGroupChat(t, s, "Plain", at(100), creator)
	both := "Plain, Edited"
	require.NoError(t, s.EditGroup(ctx, plain.ID, chat.GroupEdit{Title: &both, PictureBlobID: original}))
	got, err = s.GetChatByID(ctx, plain.ID)
	require.NoError(t, err)
	require.Equal(t, "Plain, Edited", got.Title)
	require.Equal(t, original.Value, got.PictureBlobID.GetValue())

	// The edit survives the updates that touch the rest of the record.
	advanced, _, err := s.AdvanceLastMessage(ctx, plain.ID, &messagingpb.MessageId{Value: 1}, at(200))
	require.NoError(t, err)
	require.True(t, advanced)
	got, err = s.GetChatByID(ctx, plain.ID)
	require.NoError(t, err)
	require.Equal(t, "Plain, Edited", got.Title)
	require.Equal(t, original.Value, got.PictureBlobID.GetValue())

	// An edit that names nothing is refused.
	require.Error(t, s.EditGroup(ctx, plain.ID, chat.GroupEdit{}))

	// A group that does not exist is refused, and no phantom is left behind.
	unknown := chat.MustGenerateGroupChatID()
	require.ErrorIs(t, s.EditGroup(ctx, unknown, chat.GroupEdit{Title: &title}), chat.ErrChatNotFound)
	_, err = s.GetChatByID(ctx, unknown)
	require.ErrorIs(t, err, chat.ErrChatNotFound)

	// A DM is rejected outright and left as it was.
	dm := putDmChat(t, s, model.MustGenerateUserID(), model.MustGenerateUserID(), at(1))
	require.Error(t, s.EditGroup(ctx, dm.ID, chat.GroupEdit{Title: &title}))
	got, err = s.GetChatByID(ctx, dm.ID)
	require.NoError(t, err)
	require.Empty(t, got.Title)
	require.Nil(t, got.PictureBlobID)
}

func testStore_GroupChat_Membership(t *testing.T, s chat.Store) {
	ctx := context.Background()

	userA := model.MustGenerateUserID()
	userB := model.MustGenerateUserID()
	userC := model.MustGenerateUserID()
	c := putGroupChat(t, s, "Group", at(5), userA, userB)

	ok, err := s.IsMember(ctx, c.ID, userA)
	require.NoError(t, err)
	require.True(t, ok)
	ok, err = s.IsMember(ctx, c.ID, userC)
	require.NoError(t, err)
	require.False(t, ok)

	members, err := s.GetMembers(ctx, c.ID)
	require.NoError(t, err)
	require.ElementsMatch(t, userIDValues([]*commonpb.UserId{userA, userB}), userIDValues(members))

	// Adding is idempotent for an existing member and joins new ones.
	addGroupMembers(t, s, c.ID, userB, userC)
	members, err = s.GetMembers(ctx, c.ID)
	require.NoError(t, err)
	require.ElementsMatch(t, userIDValues([]*commonpb.UserId{userA, userB, userC}), userIDValues(members))

	// Departure: no longer a member, excluded from the member set.
	removeGroupMember(t, s, c.ID, userB)
	ok, err = s.IsMember(ctx, c.ID, userB)
	require.NoError(t, err)
	require.False(t, ok)
	members, err = s.GetMembers(ctx, c.ID)
	require.NoError(t, err)
	require.ElementsMatch(t, userIDValues([]*commonpb.UserId{userA, userC}), userIDValues(members))

	// Removing an already-departed member or a stranger is a no-op.
	removeGroupMember(t, s, c.ID, userB)
	removeGroupMember(t, s, c.ID, model.MustGenerateUserID())

	// A departed member can rejoin.
	addGroupMembers(t, s, c.ID, userB)
	ok, err = s.IsMember(ctx, c.ID, userB)
	require.NoError(t, err)
	require.True(t, ok)
	members, err = s.GetMembers(ctx, c.ID)
	require.NoError(t, err)
	require.ElementsMatch(t, userIDValues([]*commonpb.UserId{userA, userB, userC}), userIDValues(members))
}

// testStore_GroupChat_RosterSummary pins the maintained summary against the
// membership it describes: the count and version move exactly when a
// membership transition happens — the version by one, the count by the
// transition's effect — and not on the idempotent no-ops (re-adding a joined
// member, removing a departed one or a stranger) that leave membership
// unchanged. Each write reports whether it transitioned and the summary it
// produced, which is what a subsequent read returns.
func testStore_GroupChat_RosterSummary(t *testing.T, s chat.Store) {
	ctx := context.Background()

	userA := model.MustGenerateUserID()
	userB := model.MustGenerateUserID()
	userC := model.MustGenerateUserID()

	// Creation seeds the count from the distinct initial set, at version zero.
	c := putGroupChat(t, s, "Summarized", at(5), userA, userB, userA)
	requireRosterSummary(t, s, c.ID, 2, 0)

	// The canonical read carries no group summary, just as it carries no
	// members.
	got, err := s.GetChatByID(ctx, c.ID)
	require.NoError(t, err)
	require.Zero(t, got.RosterSummary)

	// Adding transitions only the member who actually joins: one bump for two
	// requested. The write reports the summary it produced.
	changed, roster := addGroupMembers(t, s, c.ID, userB, userC)
	require.True(t, changed)
	require.Equal(t, chat.RosterSummary{MemberCount: 3, Version: 1}, roster)
	requireRosterSummary(t, s, c.ID, 3, 1)

	// A no-op add reports no change and the summary as it stands.
	changed, roster = addGroupMembers(t, s, c.ID, userC)
	require.False(t, changed)
	require.Equal(t, chat.RosterSummary{MemberCount: 3, Version: 1}, roster)
	requireRosterSummary(t, s, c.ID, 3, 1)

	// Departure moves the count down and the version up: the version counts
	// transitions, not members. Repeating it, or removing a stranger, is a
	// no-op.
	changed, roster = removeGroupMember(t, s, c.ID, userB)
	require.True(t, changed)
	require.Equal(t, chat.RosterSummary{MemberCount: 2, Version: 2}, roster)
	changed, _ = removeGroupMember(t, s, c.ID, userB)
	require.False(t, changed)
	changed, _ = removeGroupMember(t, s, c.ID, model.MustGenerateUserID())
	require.False(t, changed)
	requireRosterSummary(t, s, c.ID, 2, 2)

	// A rejoin is a transition like any other. The count is back where it was;
	// the version says otherwise.
	changed, roster = addGroupMembers(t, s, c.ID, userB)
	require.True(t, changed)
	require.Equal(t, chat.RosterSummary{MemberCount: 3, Version: 3}, roster)

	// Everyone leaving is a count of zero, not an error.
	for _, u := range []*commonpb.UserId{userA, userB, userC} {
		removeGroupMember(t, s, c.ID, u)
	}
	requireRosterSummary(t, s, c.ID, 0, 6)

	// The count always agrees with the enumeration.
	members, err := s.GetMembers(ctx, c.ID)
	require.NoError(t, err)
	require.Empty(t, members)

	// An unknown group has no summary, as opposed to a zero one...
	_, err = s.GetGroupRosterSummary(ctx, chat.MustGenerateGroupChatID())
	require.ErrorIs(t, err, chat.ErrChatNotFound)

	// ...and a DM's summary is its inline member list at version zero, never a
	// group read.
	dm := putDmChat(t, s, userA, userB, at(1))
	got, err = s.GetChatByID(ctx, dm.ID)
	require.NoError(t, err)
	require.Equal(t, chat.RosterSummary{MemberCount: 2}, got.RosterSummary)
	_, err = s.GetGroupRosterSummary(ctx, dm.ID)
	require.Error(t, err)

	// The batch read agrees with the single read for every group it is given,
	// collapses a repeated ID, and leaves out an unknown group rather than
	// failing on it.
	other := putGroupChat(t, s, "Other", at(6), userA, userB, userC)
	summaries, err := s.GetGroupRosterSummaries(ctx, []*commonpb.ChatId{c.ID, other.ID, c.ID, chat.MustGenerateGroupChatID()})
	require.NoError(t, err)
	require.Equal(t, map[string]chat.RosterSummary{
		string(c.ID.Value):     {MemberCount: 0, Version: 6},
		string(other.ID.Value): {MemberCount: 3, Version: 0},
	}, summaries)

	// No IDs is an empty result; a DM ID is an error.
	summaries, err = s.GetGroupRosterSummaries(ctx, nil)
	require.NoError(t, err)
	require.Empty(t, summaries)
	_, err = s.GetGroupRosterSummaries(ctx, []*commonpb.ChatId{c.ID, dm.ID})
	require.Error(t, err)
}

// testStore_GroupChat_ConcurrentTransitions pins the summary's exactness under
// contention: concurrent joins to one group each land as their own
// transition, so the final count and version equal the number of joins, and
// the summaries the writes report are the distinct versions 1..n — no two
// writers can observe having produced the same version.
func testStore_GroupChat_ConcurrentTransitions(t *testing.T, s chat.Store) {
	ctx := context.Background()

	c := putGroupChat(t, s, "Contended", at(5), model.MustGenerateUserID())

	const joins = 6
	rosters := make([]chat.RosterSummary, joins)
	var wg sync.WaitGroup
	for i := range joins {
		wg.Go(func() {
			changed, roster, err := s.AddGroupMembers(ctx, c.ID, []*commonpb.UserId{model.MustGenerateUserID()})
			assert.NoError(t, err)
			assert.True(t, changed)
			rosters[i] = roster
		})
	}
	wg.Wait()

	requireRosterSummary(t, s, c.ID, 1+joins, joins)
	versions := make([]uint64, joins)
	for i, r := range rosters {
		versions[i] = r.Version
		require.EqualValues(t, 1+r.Version, r.MemberCount, "count tracks version one-for-one under pure joins")
	}
	slices.Sort(versions)
	for i, v := range versions {
		require.EqualValues(t, i+1, v)
	}
}

func requireRosterSummary(t *testing.T, s chat.Store, chatID *commonpb.ChatId, wantCount, wantVersion uint64) {
	t.Helper()
	roster, err := s.GetGroupRosterSummary(context.Background(), chatID)
	require.NoError(t, err)
	require.Equal(t, chat.RosterSummary{MemberCount: wantCount, Version: wantVersion}, roster)
}

func addGroupMembers(t *testing.T, s chat.Store, chatID *commonpb.ChatId, userIDs ...*commonpb.UserId) (bool, chat.RosterSummary) {
	t.Helper()
	changed, roster, err := s.AddGroupMembers(context.Background(), chatID, userIDs)
	require.NoError(t, err)
	return changed, roster
}

func removeGroupMember(t *testing.T, s chat.Store, chatID *commonpb.ChatId, userID *commonpb.UserId) (bool, chat.RosterSummary) {
	t.Helper()
	changed, roster, err := s.RemoveGroupMember(context.Background(), chatID, userID, false)
	require.NoError(t, err)
	return changed, roster
}

// testStore_GroupChat_MembershipsForUser pins the inverse membership read:
// every group the user has a record on, joined or departed, tracking joins,
// departures, and rejoins, with DMs never included — each at the version of
// the user's own last transition there, zero for a membership from creation.
// A departure does not drop the record; it flips it to departed at the
// departure's version.
func testStore_GroupChat_MembershipsForUser(t *testing.T, s chat.Store) {
	ctx := context.Background()

	userA := model.MustGenerateUserID()
	userB := model.MustGenerateUserID()

	type record struct {
		joined  bool
		version uint64
	}
	// byChat keys each membership's state and version by its chat ID.
	byChat := func(memberships []chat.GroupMembership) map[string]record {
		out := make(map[string]record, len(memberships))
		for _, m := range memberships {
			out[string(m.ChatID.Value)] = record{joined: m.Joined, version: m.Version}
		}
		return out
	}

	// No memberships is an empty result, not an error.
	memberships, err := s.GetGroupMembershipsForUser(ctx, userA)
	require.NoError(t, err)
	require.Empty(t, memberships)

	groupAB := putGroupChat(t, s, "Both", at(1), userA, userB)
	groupA := putGroupChat(t, s, "Only A", at(2), userA)
	groupB := putGroupChat(t, s, "Only B", at(3), userB)

	// A DM must never surface as a group membership.
	putDmChat(t, s, userA, userB, at(4))

	// Memberships from creation are at version zero: no transition has
	// touched them.
	memberships, err = s.GetGroupMembershipsForUser(ctx, userA)
	require.NoError(t, err)
	require.Equal(t, map[string]record{
		string(groupAB.ID.Value): {joined: true, version: 0},
		string(groupA.ID.Value):  {joined: true, version: 0},
	}, byChat(memberships))

	// Departure keeps the record, departed, at the departure's version — the
	// group's first transition.
	removeGroupMember(t, s, groupAB.ID, userA)
	memberships, err = s.GetGroupMembershipsForUser(ctx, userA)
	require.NoError(t, err)
	require.Equal(t, map[string]record{
		string(groupAB.ID.Value): {joined: false, version: 1},
		string(groupA.ID.Value):  {joined: true, version: 0},
	}, byChat(memberships))

	// Rejoining flips it back, stamped with the rejoin's version — the group's
	// second transition. Joining another user's group adds it at that group's
	// first. A transition by someone else does not move userA's stamp.
	addGroupMembers(t, s, groupAB.ID, userA)
	addGroupMembers(t, s, groupB.ID, userA)
	addGroupMembers(t, s, groupAB.ID, model.MustGenerateUserID())
	memberships, err = s.GetGroupMembershipsForUser(ctx, userA)
	require.NoError(t, err)
	require.Equal(t, map[string]record{
		string(groupAB.ID.Value): {joined: true, version: 2},
		string(groupA.ID.Value):  {joined: true, version: 0},
		string(groupB.ID.Value):  {joined: true, version: 1},
	}, byChat(memberships))

	// A user never in a group has no record on it: leaving groupB again
	// leaves a departed record, but groupA, which userB never joined, is
	// absent from userB's slice rather than departed.
	removeGroupMember(t, s, groupB.ID, userA)
	memberships, err = s.GetGroupMembershipsForUser(ctx, userA)
	require.NoError(t, err)
	require.Equal(t, record{joined: false, version: 2}, byChat(memberships)[string(groupB.ID.Value)])

	// userB's view was never disturbed by userA's churn.
	memberships, err = s.GetGroupMembershipsForUser(ctx, userB)
	require.NoError(t, err)
	require.Equal(t, map[string]record{
		string(groupAB.ID.Value): {joined: true, version: 0},
		string(groupB.ID.Value):  {joined: true, version: 0},
	}, byChat(memberships))
}

// testStore_GroupChat_ChatsForUser pins the group feed's source read: the
// canonical record of exactly the groups the user is currently joined to,
// tracking departures, with DMs never included.
func testStore_GroupChat_ChatsForUser(t *testing.T, s chat.Store) {
	ctx := context.Background()

	userA := model.MustGenerateUserID()
	userB := model.MustGenerateUserID()

	// No memberships is an empty result, not an error.
	chats, err := s.GetGroupChatsForUser(ctx, userA)
	require.NoError(t, err)
	require.Empty(t, chats)

	groupAB := putGroupChat(t, s, "Both", at(1), userA, userB)
	groupA := putGroupChat(t, s, "Only A", at(2), userA)
	_ = putGroupChat(t, s, "Only B", at(3), userB)
	putDmChat(t, s, userA, userB, at(4))

	chats, err = s.GetGroupChatsForUser(ctx, userA)
	require.NoError(t, err)
	require.ElementsMatch(t, chatIDValues([]*chat.Chat{groupAB, groupA}), chatIDValues(chats))

	// Each is the canonical record as GetChatByID returns it: the group's own
	// fields, with membership left to its own records.
	for _, c := range chats {
		want := groupA
		if string(c.ID.Value) == string(groupAB.ID.Value) {
			want = groupAB
		}
		require.Equal(t, chatpb.ChatType_GROUP, c.Type)
		require.Equal(t, want.Title, c.Title)
		require.True(t, c.LastActivity.Equal(want.LastActivity))
		require.Empty(t, c.Members)
		require.Equal(t, chat.RosterSummary{}, c.RosterSummary)
	}

	// Departure excludes the group.
	removeGroupMember(t, s, groupAB.ID, userA)
	chats, err = s.GetGroupChatsForUser(ctx, userA)
	require.NoError(t, err)
	require.Equal(t, chatIDValues([]*chat.Chat{groupA}), chatIDValues(chats))
}

// testStore_GroupChat_ChatsForUserByIDs pins the keyed variant the group feed
// resumes from: the given IDs are a hint of what to read, and membership is
// re-checked on each.
func testStore_GroupChat_ChatsForUserByIDs(t *testing.T, s chat.Store) {
	ctx := context.Background()

	userA := model.MustGenerateUserID()
	userB := model.MustGenerateUserID()

	groupAB := putGroupChat(t, s, "Both", at(1), userA, userB)
	groupA := putGroupChat(t, s, "Only A", at(2), userA)
	groupB := putGroupChat(t, s, "Only B", at(3), userB)
	dm := putDmChat(t, s, userA, userB, at(4))

	// No IDs is an empty result, not an error.
	chats, err := s.GetGroupChatsForUserByIDs(ctx, userA, nil)
	require.NoError(t, err)
	require.Empty(t, chats)

	// A group the user is not in and a group that does not exist are omitted,
	// not reported; a repeated ID collapses.
	chats, err = s.GetGroupChatsForUserByIDs(ctx, userA, []*commonpb.ChatId{
		groupAB.ID, groupB.ID, chat.MustGenerateGroupChatID(), groupAB.ID, groupA.ID,
	})
	require.NoError(t, err)
	require.ElementsMatch(t, chatIDValues([]*chat.Chat{groupAB, groupA}), chatIDValues(chats))
	for _, c := range chats {
		require.Equal(t, chatpb.ChatType_GROUP, c.Type)
		require.Empty(t, c.Members)
		require.Equal(t, chat.RosterSummary{}, c.RosterSummary)
	}

	// A tombstoned membership is not a membership: the ID is dropped once the
	// user has left, even though the chat still exists.
	removeGroupMember(t, s, groupAB.ID, userA)
	chats, err = s.GetGroupChatsForUserByIDs(ctx, userA, []*commonpb.ChatId{groupAB.ID, groupA.ID})
	require.NoError(t, err)
	require.Equal(t, chatIDValues([]*chat.Chat{groupA}), chatIDValues(chats))

	// A DM ID is the wrong family: an error, not an omission.
	_, err = s.GetGroupChatsForUserByIDs(ctx, userA, []*commonpb.ChatId{groupA.ID, dm.ID})
	require.Error(t, err)
}

// testStore_PutChat_NoMembers pins that a memberless chat of either family is
// refused. Nothing could ever reach such a chat: every read, send, and
// membership mutation gates on membership, so it would be an unreachable record
// that only a direct store read could observe.
func testStore_PutChat_NoMembers(t *testing.T, s chat.Store) {
	ctx := context.Background()

	group := &chat.Chat{
		ID:           chat.MustGenerateGroupChatID(),
		Type:         chatpb.ChatType_GROUP,
		Title:        "Nobody",
		LastActivity: at(1),
	}
	require.ErrorIs(t, s.PutChat(ctx, group), chat.ErrNoMembers)
	_, err := s.GetChatByID(ctx, group.ID)
	require.ErrorIs(t, err, chat.ErrChatNotFound)

	dm := &chat.Chat{
		ID:           generateDmChatID(),
		Type:         chatpb.ChatType_CONTACT_DM,
		LastActivity: at(1),
	}
	require.ErrorIs(t, s.PutChat(ctx, dm), chat.ErrNoMembers)
	_, err = s.GetChatByID(ctx, dm.ID)
	require.ErrorIs(t, err, chat.ErrChatNotFound)
}

// testStore_GroupChat_MembersPage walks a group's roster in pages: ascending
// user-ID order, departed members skipped, a cursor that resumes mid-way, and
// a limit that lands exactly on the last member, so the final page carries a
// cursor that yields an empty, final page. The union of the pages is what
// GetMembers returns whole.
func testStore_GroupChat_MembersPage(t *testing.T, s chat.Store) {
	ctx := context.Background()

	users := make([]*commonpb.UserId, 5)
	for i := range users {
		users[i] = model.MustGenerateUserID()
	}
	slices.SortFunc(users, func(a, b *commonpb.UserId) int { return bytes.Compare(a.Value, b.Value) })
	c := putGroupChat(t, s, "Walked", at(10), users...)

	// A departed member sits between the others in key order and must be
	// stepped over, not counted against the limit.
	changed, _, err := s.RemoveGroupMember(ctx, c.ID, users[2], false)
	require.NoError(t, err)
	require.True(t, changed)
	joined := []*commonpb.UserId{users[0], users[1], users[3], users[4]}

	// Unbounded: the whole roster, in order.
	page, err := s.GetGroupMembersPage(ctx, c.ID, nil, 0)
	require.NoError(t, err)
	assert.Equal(t, userIDValues(joined), userIDValues(page.Users))
	assert.Nil(t, page.Next)

	whole, err := s.GetMembers(ctx, c.ID)
	require.NoError(t, err)
	assert.ElementsMatch(t, userIDValues(joined), userIDValues(whole))

	// Paged by three: the first page is the first three joined members, its
	// cursor the third; the second page is the rest.
	page, err = s.GetGroupMembersPage(ctx, c.ID, nil, 3)
	require.NoError(t, err)
	assert.Equal(t, userIDValues(joined[:3]), userIDValues(page.Users))
	require.NotNil(t, page.Next)
	assert.Equal(t, joined[2].Value, page.Next.Value)

	page, err = s.GetGroupMembersPage(ctx, c.ID, page.Next, 3)
	require.NoError(t, err)
	assert.Equal(t, userIDValues(joined[3:]), userIDValues(page.Users))
	assert.Nil(t, page.Next)

	// A cursor that is itself a departed member resumes just as well.
	page, err = s.GetGroupMembersPage(ctx, c.ID, users[2], 0)
	require.NoError(t, err)
	assert.Equal(t, userIDValues(joined[2:]), userIDValues(page.Users))

	// A limit that lands exactly on the last member still hands out a cursor;
	// resuming from it is an empty, final page.
	page, err = s.GetGroupMembersPage(ctx, c.ID, joined[1], 2)
	require.NoError(t, err)
	assert.Equal(t, userIDValues(joined[2:]), userIDValues(page.Users))
	require.NotNil(t, page.Next)
	assert.Equal(t, joined[3].Value, page.Next.Value)

	page, err = s.GetGroupMembersPage(ctx, c.ID, page.Next, 2)
	require.NoError(t, err)
	assert.Empty(t, page.Users)
	assert.Nil(t, page.Next)

	// A group that does not exist is an empty walk, not an error: the read
	// never consults the canonical record.
	page, err = s.GetGroupMembersPage(ctx, chat.MustGenerateGroupChatID(), nil, 0)
	require.NoError(t, err)
	assert.Empty(t, page.Users)
	assert.Nil(t, page.Next)

	// A DM has no roster to walk.
	dm := putDmChat(t, s, model.MustGenerateUserID(), model.MustGenerateUserID(), at(1))
	_, err = s.GetGroupMembersPage(ctx, dm.ID, nil, 0)
	require.Error(t, err)
}

// testStore_GroupChat_MembersPage_MuteOrder pins that the roster walk and the
// mute read agree on order: every user is muted, the roster is walked in
// pages of two, and the mutes between each page's first and last user are
// exactly that page, so a fan-out's per-page mute read covers its page and
// nothing else.
func testStore_GroupChat_MembersPage_MuteOrder(t *testing.T, s chat.Store) {
	ctx := context.Background()

	users := make([]*commonpb.UserId, 5)
	for i := range users {
		users[i] = model.MustGenerateUserID()
	}
	c := putGroupChat(t, s, "In Step", at(10), users...)
	for _, user := range users {
		_, _, err := s.SetMute(ctx, c.ID, user, chat.Mute{Forever: true})
		require.NoError(t, err)
	}

	var after *commonpb.UserId
	var seen int
	for {
		roster, err := s.GetGroupMembersPage(ctx, c.ID, after, 2)
		require.NoError(t, err)
		if len(roster.Users) > 0 {
			muted, err := s.GetMutedUsersPage(ctx, c.ID, at(0), roster.Users[0], roster.Users[len(roster.Users)-1])
			require.NoError(t, err)
			assert.Equal(t, userIDValues(roster.Users), userIDValues(muted))
		}
		seen += len(roster.Users)
		if roster.Next == nil {
			break
		}
		after = roster.Next
	}
	assert.Equal(t, len(users), seen)
}

// testStore_GroupChat_Roster pins the whole-roster read against the records
// it enumerates: every joined member with the join time and version stamp of
// the transition that placed them — creation's members at version zero, a
// joiner at the version their join moved the summary to, a rejoiner at the
// rejoin's — alongside a summary whose count is their number.
func testStore_GroupChat_Roster(t *testing.T, s chat.Store) {
	ctx := context.Background()

	userA := model.MustGenerateUserID()
	userB := model.MustGenerateUserID()
	userC := model.MustGenerateUserID()
	before := time.Now().UTC()
	c := putGroupChat(t, s, "Enumerated", at(5), userA, userB)

	summary, members, err := s.GetGroupRoster(ctx, c.ID)
	require.NoError(t, err)
	require.Equal(t, chat.RosterSummary{MemberCount: 2, Version: 0}, summary)
	byUser := rosterByUser(members)
	require.Len(t, byUser, 2)
	for _, u := range []*commonpb.UserId{userA, userB} {
		m, ok := byUser[string(u.Value)]
		require.True(t, ok)
		require.Zero(t, m.Version)
		require.False(t, m.JoinedAt.Before(before), "a creation member's join time is the creation's")
	}

	// A join is stamped with the version it produced and a join time after
	// the founders'.
	settle()
	_, roster := addGroupMembers(t, s, c.ID, userC)
	summary, members, err = s.GetGroupRoster(ctx, c.ID)
	require.NoError(t, err)
	require.Equal(t, roster, summary)
	require.Equal(t, chat.RosterSummary{MemberCount: 3, Version: 1}, summary)
	byUser = rosterByUser(members)
	require.Len(t, byUser, 3)
	require.EqualValues(t, 1, byUser[string(userC.Value)].Version)
	require.True(t, byUser[string(userC.Value)].JoinedAt.After(byUser[string(userA.Value)].JoinedAt))

	// A departed member is not on the roster; the count says so.
	removeGroupMember(t, s, c.ID, userA)
	summary, members, err = s.GetGroupRoster(ctx, c.ID)
	require.NoError(t, err)
	require.Equal(t, chat.RosterSummary{MemberCount: 2, Version: 2}, summary)
	require.ElementsMatch(t, userIDValues([]*commonpb.UserId{userB, userC}), userIDValues(rosterUserIDs(members)))

	// A rejoin is a fresh record: the rejoin's version and a new join time.
	settle()
	addGroupMembers(t, s, c.ID, userA)
	summary, members, err = s.GetGroupRoster(ctx, c.ID)
	require.NoError(t, err)
	require.Equal(t, chat.RosterSummary{MemberCount: 3, Version: 3}, summary)
	byUser = rosterByUser(members)
	require.EqualValues(t, 3, byUser[string(userA.Value)].Version)
	require.True(t, byUser[string(userA.Value)].JoinedAt.After(byUser[string(userC.Value)].JoinedAt))

	// An unknown group is not found, as opposed to empty; a DM has no roster
	// records to read.
	_, _, err = s.GetGroupRoster(ctx, chat.MustGenerateGroupChatID())
	require.ErrorIs(t, err, chat.ErrChatNotFound)
	dm := putDmChat(t, s, userA, userB, at(1))
	_, _, err = s.GetGroupRoster(ctx, dm.ID)
	require.Error(t, err)
}

// testStore_GroupChat_RosterPage pins the paged read's order and cursor:
// most recently joined first, resuming strictly after a position — one that
// names a departed member included — with departed members absent and a
// rejoiner back at the head.
func testStore_GroupChat_RosterPage(t *testing.T, s chat.Store) {
	ctx := context.Background()

	// Five members joined one after another, so join order is known: the
	// founder first, then each joiner.
	users := make([]*commonpb.UserId, 5)
	for i := range users {
		users[i] = model.MustGenerateUserID()
	}
	c := putGroupChat(t, s, "Ordered", at(10), users[0])
	for _, u := range users[1:] {
		settle()
		addGroupMembers(t, s, c.ID, u)
	}
	newestFirst := []*commonpb.UserId{users[4], users[3], users[2], users[1], users[0]}

	// Unbounded: the whole roster, newest first, each at the version of the
	// join that placed them.
	page, err := s.GetGroupRosterPage(ctx, c.ID, nil, 0)
	require.NoError(t, err)
	require.Equal(t, userIDValues(newestFirst), userIDValues(rosterUserIDs(page)))
	for i, m := range page {
		require.EqualValues(t, 4-i, m.Version)
	}

	// Paged by two: each page resumes after the last member of the one before.
	page, err = s.GetGroupRosterPage(ctx, c.ID, nil, 2)
	require.NoError(t, err)
	require.Equal(t, userIDValues(newestFirst[:2]), userIDValues(rosterUserIDs(page)))
	after := page[1].Position()
	page, err = s.GetGroupRosterPage(ctx, c.ID, &after, 2)
	require.NoError(t, err)
	require.Equal(t, userIDValues(newestFirst[2:4]), userIDValues(rosterUserIDs(page)))
	after = page[1].Position()
	page, err = s.GetGroupRosterPage(ctx, c.ID, &after, 2)
	require.NoError(t, err)
	require.Equal(t, userIDValues(newestFirst[4:]), userIDValues(rosterUserIDs(page)))
	after = page[0].Position()
	page, err = s.GetGroupRosterPage(ctx, c.ID, &after, 2)
	require.NoError(t, err)
	require.Empty(t, page)

	// A departed member leaves the order; a cursor naming their old position
	// still resumes from where they were.
	departed := newestFirst[1]
	departedPosition := page1Position(t, s, c.ID, departed)
	removeGroupMember(t, s, c.ID, departed)
	page, err = s.GetGroupRosterPage(ctx, c.ID, nil, 0)
	require.NoError(t, err)
	require.Equal(t, userIDValues([]*commonpb.UserId{newestFirst[0], newestFirst[2], newestFirst[3], newestFirst[4]}), userIDValues(rosterUserIDs(page)))
	page, err = s.GetGroupRosterPage(ctx, c.ID, &departedPosition, 0)
	require.NoError(t, err)
	require.Equal(t, userIDValues(newestFirst[2:]), userIDValues(rosterUserIDs(page)))

	// A rejoin is the newest join: the member is back at the head, at the
	// rejoin's version.
	settle()
	addGroupMembers(t, s, c.ID, departed)
	page, err = s.GetGroupRosterPage(ctx, c.ID, nil, 1)
	require.NoError(t, err)
	require.Len(t, page, 1)
	require.Equal(t, departed.Value, page[0].UserID.Value)
	require.EqualValues(t, 6, page[0].Version)

	// A group that does not exist is an empty page; a DM has no roster to page.
	page, err = s.GetGroupRosterPage(ctx, chat.MustGenerateGroupChatID(), nil, 0)
	require.NoError(t, err)
	require.Empty(t, page)
	dm := putDmChat(t, s, users[0], users[1], at(1))
	_, err = s.GetGroupRosterPage(ctx, dm.ID, nil, 0)
	require.Error(t, err)
}

// testStore_GroupChat_MemberRecords pins the batch read of a user's own
// records: one per group they are joined to, with the join's stamp; a group
// they left, never joined, or that does not exist is absent; a repeated ID
// collapses; a DM ID is refused.
func testStore_GroupChat_MemberRecords(t *testing.T, s chat.Store) {
	ctx := context.Background()

	user := model.MustGenerateUserID()
	other := model.MustGenerateUserID()
	founded := putGroupChat(t, s, "Founded", at(1), user, other)
	joined := putGroupChat(t, s, "Joined", at(2), other)
	settle()
	addGroupMembers(t, s, joined.ID, user)
	left := putGroupChat(t, s, "Left", at(3), user, other)
	removeGroupMember(t, s, left.ID, user)
	never := putGroupChat(t, s, "Never", at(4), other)

	records, err := s.GetGroupMemberRecords(ctx, user, []*commonpb.ChatId{
		founded.ID, joined.ID, left.ID, never.ID, chat.MustGenerateGroupChatID(), founded.ID,
	})
	require.NoError(t, err)
	require.Len(t, records, 2)

	r, ok := records[string(founded.ID.Value)]
	require.True(t, ok)
	require.Equal(t, user.Value, r.UserID.Value)
	require.Zero(t, r.Version)
	require.False(t, r.JoinedAt.IsZero())

	r, ok = records[string(joined.ID.Value)]
	require.True(t, ok)
	require.Equal(t, user.Value, r.UserID.Value)
	require.EqualValues(t, 1, r.Version)
	require.True(t, r.JoinedAt.After(records[string(founded.ID.Value)].JoinedAt))

	// The other member's records are their own: the same groups, read for
	// them, are the ones they are joined to.
	records, err = s.GetGroupMemberRecords(ctx, other, []*commonpb.ChatId{founded.ID, joined.ID, left.ID, never.ID})
	require.NoError(t, err)
	require.Len(t, records, 4)

	records, err = s.GetGroupMemberRecords(ctx, user, nil)
	require.NoError(t, err)
	require.Empty(t, records)

	dm := putDmChat(t, s, user, other, at(1))
	_, err = s.GetGroupMemberRecords(ctx, user, []*commonpb.ChatId{founded.ID, dm.ID})
	require.Error(t, err)
}

// settle spaces two membership transitions apart in wall-clock time, so their
// join times order the way the transitions did on any clock resolution.
func settle() { time.Sleep(2 * time.Millisecond) }

// page1Position is a current member's roster position, read off the whole
// roster.
func page1Position(t *testing.T, s chat.Store, chatID *commonpb.ChatId, userID *commonpb.UserId) chat.RosterPosition {
	t.Helper()
	_, members, err := s.GetGroupRoster(context.Background(), chatID)
	require.NoError(t, err)
	m, ok := rosterByUser(members)[string(userID.Value)]
	require.True(t, ok)
	return m.Position()
}

func rosterByUser(members []chat.GroupMember) map[string]chat.GroupMember {
	out := make(map[string]chat.GroupMember, len(members))
	for _, m := range members {
		out[string(m.UserID.Value)] = m
	}
	return out
}

func rosterUserIDs(members []chat.GroupMember) []*commonpb.UserId {
	out := make([]*commonpb.UserId, len(members))
	for i, m := range members {
		out[i] = m.UserID
	}
	return out
}

// testStore_GroupChat_CreationCap covers the boundary of the initial member set:
// a group at the cap is created whole, and one over it is rejected outright
// rather than partially written.
func testStore_GroupChat_CreationCap(t *testing.T, s chat.Store) {
	ctx := context.Background()

	members := make([]*commonpb.UserId, chat.MaxGroupChatCreationMembers)
	for i := range members {
		members[i] = model.MustGenerateUserID()
	}
	c := putGroupChat(t, s, "Big Group", at(10), members...)

	got, err := s.GetMembers(ctx, c.ID)
	require.NoError(t, err)
	require.ElementsMatch(t, userIDValues(members), userIDValues(got))

	// One past the cap is refused, and nothing is left behind — creation is
	// all-or-nothing, so the caller can retry at a legal size against the same
	// chat ID without first cleaning up.
	overCap := &chat.Chat{
		ID:           chat.MustGenerateGroupChatID(),
		Type:         chatpb.ChatType_GROUP,
		Members:      append(members, model.MustGenerateUserID()),
		Title:        "Too Big",
		LastActivity: at(10),
	}
	require.ErrorIs(t, s.PutChat(ctx, overCap), chat.ErrTooManyMembers)
	_, err = s.GetChatByID(ctx, overCap.ID)
	require.ErrorIs(t, err, chat.ErrChatNotFound)
}

// testStore_GroupChat_DuplicateMembers pins that a repeated member in the
// initial set collapses rather than failing the creation — the persistent
// stores write one record per member, and a repeat would otherwise be two
// writes to one key.
func testStore_GroupChat_DuplicateMembers(t *testing.T, s chat.Store) {
	ctx := context.Background()

	userA := model.MustGenerateUserID()
	userB := model.MustGenerateUserID()
	c := putGroupChat(t, s, "Dupes", at(10), userA, userB, userA)

	got, err := s.GetMembers(ctx, c.ID)
	require.NoError(t, err)
	require.ElementsMatch(t, userIDValues([]*commonpb.UserId{userA, userB}), userIDValues(got))

	// Duplicates collapse before the cap is applied, so a set that is only over
	// the cap by repetition is still legal.
	members := make([]*commonpb.UserId, 0, chat.MaxGroupChatCreationMembers+2)
	for i := 0; i < chat.MaxGroupChatCreationMembers; i++ {
		members = append(members, model.MustGenerateUserID())
	}
	members = append(members, members[0], members[1])
	atCap := putGroupChat(t, s, "Dupes At Cap", at(10), members...)

	got, err = s.GetMembers(ctx, atCap.ID)
	require.NoError(t, err)
	require.Len(t, got, chat.MaxGroupChatCreationMembers)
}

func testStore_GroupChat_AddMembersErrors(t *testing.T, s chat.Store) {
	ctx := context.Background()

	user := model.MustGenerateUserID()

	// Membership writes against a chat that does not exist must not accrete
	// orphaned records, and report the chat missing rather than a no-op.
	_, _, err := s.AddGroupMembers(ctx, chat.MustGenerateGroupChatID(), []*commonpb.UserId{user})
	require.ErrorIs(t, err, chat.ErrChatNotFound)
	_, _, err = s.RemoveGroupMember(ctx, chat.MustGenerateGroupChatID(), user, false)
	require.ErrorIs(t, err, chat.ErrChatNotFound)

	// Group membership methods reject DM chat IDs outright.
	dm := putDmChat(t, s, user, model.MustGenerateUserID(), at(1))
	_, _, err = s.AddGroupMembers(ctx, dm.ID, []*commonpb.UserId{user})
	require.Error(t, err)
	_, _, err = s.RemoveGroupMember(ctx, dm.ID, user, false)
	require.Error(t, err)

	// An unknown group chat has no members, as opposed to an empty set.
	_, err = s.GetMembers(ctx, chat.MustGenerateGroupChatID())
	require.ErrorIs(t, err, chat.ErrChatNotFound)
}

func testStore_GroupChat_AdvanceLastMessage(t *testing.T, s chat.Store) {
	ctx := context.Background()

	c := putGroupChat(t, s, "Group", at(100), model.MustGenerateUserID(), model.MustGenerateUserID())

	// Advancing touches only the canonical record — a group has no inline
	// member list, so no members are returned; group fan-out reads membership
	// explicitly via GetMembers.
	advanced, members, err := s.AdvanceLastMessage(ctx, c.ID, &messagingpb.MessageId{Value: 5}, at(200))
	require.NoError(t, err)
	require.True(t, advanced)
	require.Empty(t, members)

	got, err := s.GetChatByID(ctx, c.ID)
	require.NoError(t, err)
	require.True(t, got.LastActivity.Equal(at(200)))
	require.NotNil(t, got.LastMessageID)
	require.Equal(t, uint64(5), got.LastMessageID.Value)

	// Backward is a no-op, as for DMs.
	advanced, _, err = s.AdvanceLastMessage(ctx, c.ID, &messagingpb.MessageId{Value: 3}, at(150))
	require.NoError(t, err)
	require.False(t, advanced)
}

func testStore_GroupChat_NotInDmFeed(t *testing.T, s chat.Store) {
	ctx := context.Background()

	user := model.MustGenerateUserID()
	_ = putGroupChat(t, s, "Group", at(200), user, model.MustGenerateUserID())
	dm := putDmChat(t, s, user, model.MustGenerateUserID(), at(100))

	// A group chat never surfaces in a DM feed, neither in a DM type's feed nor
	// via a feed query for the group type itself — groups get their own
	// read-time feed.
	feed, err := s.GetDmFeedPage(ctx, user, chatpb.ChatType_CONTACT_DM, at(1000), nil, 0)
	require.NoError(t, err)
	require.Equal(t, [][]byte{dm.ID.Value}, chatIDValues(feed))

	feed, err = s.GetDmFeedPage(ctx, user, chatpb.ChatType_GROUP, at(1000), nil, 0)
	require.NoError(t, err)
	require.Empty(t, feed)
}

func testStore_GetDmFeedPage_Order(t *testing.T, s chat.Store) {
	ctx := context.Background()

	user := model.MustGenerateUserID()
	other := model.MustGenerateUserID()

	// Three chats the user is in, plus one they are not.
	c1 := putDmChat(t, s, user, other, at(100))
	c2 := putDmChat(t, s, user, other, at(300))
	c3 := putDmChat(t, s, user, other, at(200))
	_ = putDmChat(t, s, model.MustGenerateUserID(), model.MustGenerateUserID(), at(999))

	// A watermark above every chat includes them all, most recent first.
	got, err := s.GetDmFeedPage(ctx, user, chatpb.ChatType_CONTACT_DM, at(1000), nil, 0)
	require.NoError(t, err)
	require.Equal(t, [][]byte{c2.ID.Value, c3.ID.Value, c1.ID.Value}, chatIDValues(got))
}

func testStore_GetDmFeedPage_Watermark(t *testing.T, s chat.Store) {
	ctx := context.Background()

	user := model.MustGenerateUserID()
	other := model.MustGenerateUserID()
	c1 := putDmChat(t, s, user, other, at(100))
	_ = putDmChat(t, s, user, other, at(300)) // Above the watermark; excluded.
	c3 := putDmChat(t, s, user, other, at(200))

	// A watermark of 250 pins out the chat last active at 300.
	got, err := s.GetDmFeedPage(ctx, user, chatpb.ChatType_CONTACT_DM, at(250), nil, 0)
	require.NoError(t, err)
	require.Equal(t, [][]byte{c3.ID.Value, c1.ID.Value}, chatIDValues(got))
}

func testStore_GetDmFeedPage_Paging(t *testing.T, s chat.Store) {
	ctx := context.Background()

	user := model.MustGenerateUserID()
	other := model.MustGenerateUserID()
	c1 := putDmChat(t, s, user, other, at(100))
	c2 := putDmChat(t, s, user, other, at(300))
	c3 := putDmChat(t, s, user, other, at(200))

	snapshot := at(1000)

	// Page 1: most recent, limit 2 → [c2, c3].
	page1, err := s.GetDmFeedPage(ctx, user, chatpb.ChatType_CONTACT_DM, snapshot, nil, 2)
	require.NoError(t, err)
	require.Equal(t, [][]byte{c2.ID.Value, c3.ID.Value}, chatIDValues(page1))

	// Page 2: resume after the last chat of page 1 (c3) → [c1].
	page2, err := s.GetDmFeedPage(ctx, user, chatpb.ChatType_CONTACT_DM, snapshot, cursorOf(page1[len(page1)-1]), 2)
	require.NoError(t, err)
	require.Equal(t, [][]byte{c1.ID.Value}, chatIDValues(page2))

	// Resuming after the final chat yields an empty page.
	page3, err := s.GetDmFeedPage(ctx, user, chatpb.ChatType_CONTACT_DM, snapshot, cursorOf(c1), 2)
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
	c1 := putDmChat(t, s, user, other, at(100))
	c2 := putDmChat(t, s, user, other, at(200))
	c3 := putDmChat(t, s, user, other, at(300))

	snapshot := at(350) // All three are within the window.

	// Page 1: the most recent chat.
	page1, err := s.GetDmFeedPage(ctx, user, chatpb.ChatType_CONTACT_DM, snapshot, nil, 1)
	require.NoError(t, err)
	require.Equal(t, [][]byte{c3.ID.Value}, chatIDValues(page1))

	// c1, not yet paged, becomes active after the snapshot, moving above the
	// watermark.
	advanced, _, err := s.AdvanceLastMessage(ctx, c1.ID, &messagingpb.MessageId{Value: 1}, at(999))
	require.NoError(t, err)
	require.True(t, advanced)

	// Page 2 sees only c2: c1 has left the snapshot window, so it is neither
	// duplicated nor reordered into the read. Its freshness is the stream's job.
	page2, err := s.GetDmFeedPage(ctx, user, chatpb.ChatType_CONTACT_DM, snapshot, cursorOf(page1[len(page1)-1]), 10)
	require.NoError(t, err)
	require.Equal(t, [][]byte{c2.ID.Value}, chatIDValues(page2))
}

func testStore_GetDmFeedPage_Empty(t *testing.T, s chat.Store) {
	ctx := context.Background()

	got, err := s.GetDmFeedPage(ctx, model.MustGenerateUserID(), chatpb.ChatType_CONTACT_DM, at(1000), nil, 0)
	require.NoError(t, err)
	require.Empty(t, got)
}

func testStore_GetDmFeedPage_TypeScoped(t *testing.T, s chat.Store) {
	ctx := context.Background()

	user := model.MustGenerateUserID()
	other := model.MustGenerateUserID()

	contact1 := putDmChatOfType(t, s, chatpb.ChatType_CONTACT_DM, user, other, at(100))
	dm1 := putDmChatOfType(t, s, chatpb.ChatType_DM, user, other, at(200))
	contact2 := putDmChatOfType(t, s, chatpb.ChatType_CONTACT_DM, user, other, at(300))
	dm2 := putDmChatOfType(t, s, chatpb.ChatType_DM, user, other, at(400))

	// Each feed contains only its own type, most recent first.
	contacts, err := s.GetDmFeedPage(ctx, user, chatpb.ChatType_CONTACT_DM, at(1000), nil, 0)
	require.NoError(t, err)
	require.Equal(t, [][]byte{contact2.ID.Value, contact1.ID.Value}, chatIDValues(contacts))
	for _, c := range contacts {
		require.Equal(t, chatpb.ChatType_CONTACT_DM, c.Type)
	}

	dms, err := s.GetDmFeedPage(ctx, user, chatpb.ChatType_DM, at(1000), nil, 0)
	require.NoError(t, err)
	require.Equal(t, [][]byte{dm2.ID.Value, dm1.ID.Value}, chatIDValues(dms))
	for _, c := range dms {
		require.Equal(t, chatpb.ChatType_DM, c.Type)
	}

	// Paging within one feed steps over the other type's activity: a limit-1
	// DM page resumes at the older DM, not at the interleaved contact chats.
	dmPage1, err := s.GetDmFeedPage(ctx, user, chatpb.ChatType_DM, at(1000), nil, 1)
	require.NoError(t, err)
	require.Equal(t, [][]byte{dm2.ID.Value}, chatIDValues(dmPage1))

	dmPage2, err := s.GetDmFeedPage(ctx, user, chatpb.ChatType_DM, at(1000), cursorOf(dmPage1[0]), 1)
	require.NoError(t, err)
	require.Equal(t, [][]byte{dm1.ID.Value}, chatIDValues(dmPage2))
}

func cursorOf(c *chat.Chat) *chat.DmFeedCursor {
	return &chat.DmFeedCursor{LastActivity: c.LastActivity, ChatID: c.ID}
}

func putDmChat(t *testing.T, s chat.Store, a, b *commonpb.UserId, lastActivity time.Time) *chat.Chat {
	return putDmChatOfType(t, s, chatpb.ChatType_CONTACT_DM, a, b, lastActivity)
}

func putDmChatOfType(t *testing.T, s chat.Store, chatType chatpb.ChatType, a, b *commonpb.UserId, lastActivity time.Time) *chat.Chat {
	c := &chat.Chat{
		ID:           generateDmChatID(),
		Type:         chatType,
		Members:      []*commonpb.UserId{a, b},
		LastActivity: lastActivity,
	}
	require.NoError(t, s.PutChat(context.Background(), c))
	return c
}

func putGroupChat(t *testing.T, s chat.Store, title string, lastActivity time.Time, members ...*commonpb.UserId) *chat.Chat {
	c := &chat.Chat{
		ID:           chat.MustGenerateGroupChatID(),
		Type:         chatpb.ChatType_GROUP,
		Members:      members,
		Title:        title,
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

func generateDmChatID() *commonpb.ChatId {
	return &commonpb.ChatId{Value: randomBytes(chat.DmChatIDSize)}
}

func randomBytes(n int) []byte {
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		panic(err)
	}
	return b
}

func chatIDValues(chats []*chat.Chat) [][]byte {
	out := make([][]byte, len(chats))
	for i, c := range chats {
		out[i] = c.ID.Value
	}
	return out
}

// justPast returns a key that sorts immediately after user and before any
// other user ID: the ID with one more byte, which no real ID shares as a
// prefix.
func justPast(user *commonpb.UserId) *commonpb.UserId {
	return &commonpb.UserId{Value: append(append([]byte(nil), user.Value...), 0)}
}

func userIDValues(ids []*commonpb.UserId) [][]byte {
	out := make([][]byte, len(ids))
	for i, id := range ids {
		out[i] = id.Value
	}
	return out
}

// requireMutedUsers checks both chat-scoped reads: the set read, and the
// ordered walk — as one page, and paged one user at a time — which must agree
// with it and come back in ascending user-ID order.
func requireMutedUsers(t *testing.T, us chat.Store, chatID *commonpb.ChatId, now time.Time, want ...*commonpb.UserId) {
	t.Helper()
	ctx := context.Background()

	got, err := us.GetMutedUsers(ctx, chatID, now, 0)
	require.NoError(t, err)
	assert.ElementsMatch(t, userIDValues(want), userIDValues(got))

	wantOrdered := slices.Clone(want)
	slices.SortFunc(wantOrdered, func(a, b *commonpb.UserId) int { return bytes.Compare(a.Value, b.Value) })

	ordered, err := us.GetMutedUsersPage(ctx, chatID, now, nil, nil)
	require.NoError(t, err)
	assert.Equal(t, userIDValues(wantOrdered), userIDValues(ordered))

	// Read one user at a time as a closed range, and stitched together the
	// pages are the whole ordered set.
	var walked []*commonpb.UserId
	for _, user := range wantOrdered {
		page, err := us.GetMutedUsersPage(ctx, chatID, now, user, user)
		require.NoError(t, err)
		walked = append(walked, page...)
	}
	assert.Equal(t, userIDValues(wantOrdered), userIDValues(walked))
}

func requireMutedCount(t *testing.T, us chat.Store, chatID *commonpb.ChatId, want uint64) {
	t.Helper()
	got, err := us.GetMutedCount(context.Background(), chatID)
	require.NoError(t, err)
	assert.Equal(t, want, got)
}

func requireViewerState(t *testing.T, us chat.Store, chatID *commonpb.ChatId, userID *commonpb.UserId, want chat.ViewerState) {
	t.Helper()
	states, err := us.GetViewerStates(context.Background(), userID, []*commonpb.ChatId{chatID})
	require.NoError(t, err)
	got, ok := states[string(chatID.Value)]
	require.True(t, ok, "no viewer state recorded")
	assert.Equal(t, want, got)
}

func testStore_UserState_Empty(t *testing.T, s chat.Store) {
	ctx := context.Background()
	us := s
	user := model.MustGenerateUserID()
	dm := generateDmChatID()
	group := chat.MustGenerateGroupChatID()

	states, err := us.GetViewerStates(ctx, user, []*commonpb.ChatId{dm, group})
	require.NoError(t, err)
	assert.Empty(t, states)

	states, err = us.GetViewerStates(ctx, user, nil)
	require.NoError(t, err)
	assert.Empty(t, states)

	requireMutedUsers(t, us, dm, at(0))

	// Clearing what was never set is a no-op that leaves no record behind.
	state, changed, err := us.ClearMute(ctx, dm, user)
	require.NoError(t, err)
	assert.False(t, changed)
	assert.Equal(t, chat.ViewerState{}, state)

	states, err = us.GetViewerStates(ctx, user, []*commonpb.ChatId{dm})
	require.NoError(t, err)
	assert.Empty(t, states)
}

func testStore_UserState_SetMute_Until(t *testing.T, s chat.Store) {
	ctx := context.Background()
	us := s
	user := model.MustGenerateUserID()
	dm := generateDmChatID()

	// Until is recorded at second precision.
	state, changed, err := us.SetMute(ctx, dm, user, chat.Mute{Until: at(100).Add(750 * time.Millisecond)})
	require.NoError(t, err)
	assert.True(t, changed)
	want := chat.ViewerState{Mute: &chat.Mute{Until: at(100)}, Version: 1}
	assert.Equal(t, want, state)
	requireViewerState(t, us, dm, user, want)

	// Active strictly before Until, lapsed from Until on.
	requireMutedUsers(t, us, dm, at(50), user)
	requireMutedUsers(t, us, dm, at(99), user)
	requireMutedUsers(t, us, dm, at(100))
	requireMutedUsers(t, us, dm, at(150))

	// A lapsed mute is still recorded, as stored: nothing sweeps it.
	requireViewerState(t, us, dm, user, want)
	assert.Nil(t, state.ActiveMute(at(100)))
	assert.NotNil(t, state.ActiveMute(at(99)))
}

func testStore_UserState_SetMute_Forever(t *testing.T, s chat.Store) {
	ctx := context.Background()
	us := s
	user := model.MustGenerateUserID()
	group := chat.MustGenerateGroupChatID()

	// Until is ignored when Forever is set.
	state, changed, err := us.SetMute(ctx, group, user, chat.Mute{Forever: true, Until: at(100)})
	require.NoError(t, err)
	assert.True(t, changed)
	want := chat.ViewerState{Mute: &chat.Mute{Forever: true}, Version: 1}
	assert.Equal(t, want, state)
	requireViewerState(t, us, group, user, want)

	requireMutedUsers(t, us, group, at(0), user)
	requireMutedUsers(t, us, group, time.Date(3000, 1, 1, 0, 0, 0, 0, time.UTC), user)
}

func testStore_UserState_SetMute_Idempotent(t *testing.T, s chat.Store) {
	ctx := context.Background()
	us := s
	user := model.MustGenerateUserID()
	dm := generateDmChatID()

	_, changed, err := us.SetMute(ctx, dm, user, chat.Mute{Until: at(100)})
	require.NoError(t, err)
	assert.True(t, changed)

	// The mute already recorded — at second precision — moves nothing.
	for _, until := range []time.Time{at(100), at(100).Add(500 * time.Millisecond)} {
		state, changed, err := us.SetMute(ctx, dm, user, chat.Mute{Until: until})
		require.NoError(t, err)
		assert.False(t, changed)
		assert.Equal(t, chat.ViewerState{Mute: &chat.Mute{Until: at(100)}, Version: 1}, state)
	}
	requireViewerState(t, us, dm, user, chat.ViewerState{Mute: &chat.Mute{Until: at(100)}, Version: 1})

	_, changed, err = us.SetMute(ctx, dm, user, chat.Mute{Forever: true})
	require.NoError(t, err)
	assert.True(t, changed)

	state, changed, err := us.SetMute(ctx, dm, user, chat.Mute{Forever: true})
	require.NoError(t, err)
	assert.False(t, changed)
	assert.Equal(t, chat.ViewerState{Mute: &chat.Mute{Forever: true}, Version: 2}, state)
}

func testStore_UserState_SetMute_Replace(t *testing.T, s chat.Store) {
	ctx := context.Background()
	us := s
	user := model.MustGenerateUserID()
	dm := generateDmChatID()

	// Every real change — a different end, indefinite, timed again — is one
	// transition, and the version counts exactly those.
	for i, mute := range []chat.Mute{
		{Until: at(100)},
		{Until: at(200)},
		{Forever: true},
		{Until: at(200)},
	} {
		state, changed, err := us.SetMute(ctx, dm, user, mute)
		require.NoError(t, err)
		assert.True(t, changed)
		want := chat.ViewerState{Mute: &mute, Version: uint64(i + 1)}
		assert.Equal(t, want, state)
		requireViewerState(t, us, dm, user, want)
	}
}

func testStore_UserState_ClearMute(t *testing.T, s chat.Store) {
	ctx := context.Background()
	us := s
	user := model.MustGenerateUserID()
	group := chat.MustGenerateGroupChatID()

	_, _, err := us.SetMute(ctx, group, user, chat.Mute{Forever: true})
	require.NoError(t, err)

	state, changed, err := us.ClearMute(ctx, group, user)
	require.NoError(t, err)
	assert.True(t, changed)
	assert.Equal(t, chat.ViewerState{Version: 2}, state)

	// The record and its version outlive the mute.
	requireViewerState(t, us, group, user, chat.ViewerState{Version: 2})
	requireMutedUsers(t, us, group, at(0))

	state, changed, err = us.ClearMute(ctx, group, user)
	require.NoError(t, err)
	assert.False(t, changed)
	assert.Equal(t, chat.ViewerState{Version: 2}, state)

	// A new mute continues the version from where the clear left it.
	state, changed, err = us.SetMute(ctx, group, user, chat.Mute{Until: at(100)})
	require.NoError(t, err)
	assert.True(t, changed)
	assert.Equal(t, chat.ViewerState{Mute: &chat.Mute{Until: at(100)}, Version: 3}, state)

	// A lapsed mute clears like any other.
	state, changed, err = us.ClearMute(ctx, group, user)
	require.NoError(t, err)
	assert.True(t, changed)
	assert.Equal(t, chat.ViewerState{Version: 4}, state)
}

func testStore_UserState_OutOfRange(t *testing.T, s chat.Store) {
	ctx := context.Background()
	us := s
	user := model.MustGenerateUserID()
	dm := generateDmChatID()

	for _, until := range []time.Time{
		{},                               // the zero time predates the epoch
		time.Unix(-1, 0),                 // before the epoch
		chat.MaxMuteUntil,                // the boundary is exclusive
		chat.MaxMuteUntil.Add(time.Hour), // beyond it
	} {
		_, _, err := us.SetMute(ctx, dm, user, chat.Mute{Until: until})
		assert.ErrorIs(t, err, chat.ErrMuteUntilOutOfRange, "until %v", until)
	}

	// A rejected write leaves no record behind.
	states, err := us.GetViewerStates(ctx, user, []*commonpb.ChatId{dm})
	require.NoError(t, err)
	assert.Empty(t, states)

	// The extremes of the range are recordable, and distinct from indefinite.
	for _, until := range []time.Time{time.Unix(0, 0).UTC(), chat.MaxMuteUntil.Add(-time.Second)} {
		state, changed, err := us.SetMute(ctx, dm, user, chat.Mute{Until: until})
		require.NoError(t, err)
		assert.True(t, changed)
		assert.Equal(t, &chat.Mute{Until: until}, state.Mute)
	}
	requireMutedUsers(t, us, dm, at(0), user)
}

func testStore_UserState_GetViewerStates_Batch(t *testing.T, s chat.Store) {
	ctx := context.Background()
	us := s
	user := model.MustGenerateUserID()
	other := model.MustGenerateUserID()
	dm1 := generateDmChatID()
	dm2 := generateDmChatID()
	group1 := chat.MustGenerateGroupChatID()
	group2 := chat.MustGenerateGroupChatID()

	_, _, err := us.SetMute(ctx, dm1, user, chat.Mute{Until: at(100)})
	require.NoError(t, err)
	_, _, err = us.SetMute(ctx, group1, user, chat.Mute{Forever: true})
	require.NoError(t, err)
	_, _, err = us.SetMute(ctx, dm2, user, chat.Mute{Forever: true})
	require.NoError(t, err)
	_, _, err = us.ClearMute(ctx, dm2, user)
	require.NoError(t, err)
	_, _, err = us.SetMute(ctx, dm1, other, chat.Mute{Forever: true})
	require.NoError(t, err)

	// Both families in one read; a chat with no record is absent; duplicates
	// collapse; another user's records never bleed in.
	states, err := us.GetViewerStates(ctx, user, []*commonpb.ChatId{dm1, group1, dm2, group2, dm1})
	require.NoError(t, err)
	assert.Equal(t, map[string]chat.ViewerState{
		string(dm1.Value):    {Mute: &chat.Mute{Until: at(100)}, Version: 1},
		string(group1.Value): {Mute: &chat.Mute{Forever: true}, Version: 1},
		string(dm2.Value):    {Version: 2},
	}, states)

	states, err = us.GetViewerStates(ctx, other, []*commonpb.ChatId{dm1, group1, dm2})
	require.NoError(t, err)
	assert.Equal(t, map[string]chat.ViewerState{
		string(dm1.Value): {Mute: &chat.Mute{Forever: true}, Version: 1},
	}, states)

	// A single chat, present and absent.
	states, err = us.GetViewerStates(ctx, user, []*commonpb.ChatId{group1})
	require.NoError(t, err)
	assert.Equal(t, map[string]chat.ViewerState{
		string(group1.Value): {Mute: &chat.Mute{Forever: true}, Version: 1},
	}, states)

	states, err = us.GetViewerStates(ctx, user, []*commonpb.ChatId{group2})
	require.NoError(t, err)
	assert.Empty(t, states)
}

// testStore_UserState_GetViewerStates_Bounded asks for the two extremes of
// three chats in key order: the one between them lies inside any range read
// the store bounds to the request, and must still be left out.
func testStore_UserState_GetViewerStates_Bounded(t *testing.T, s chat.Store) {
	ctx := context.Background()
	us := s
	user := model.MustGenerateUserID()

	chats := []*commonpb.ChatId{generateDmChatID(), generateDmChatID(), generateDmChatID()}
	slices.SortFunc(chats, func(a, b *commonpb.ChatId) int { return bytes.Compare(a.Value, b.Value) })
	for _, chatID := range chats {
		_, _, err := us.SetMute(ctx, chatID, user, chat.Mute{Forever: true})
		require.NoError(t, err)
	}

	states, err := us.GetViewerStates(ctx, user, []*commonpb.ChatId{chats[0], chats[2]})
	require.NoError(t, err)
	assert.Equal(t, map[string]chat.ViewerState{
		string(chats[0].Value): {Mute: &chat.Mute{Forever: true}, Version: 1},
		string(chats[2].Value): {Mute: &chat.Mute{Forever: true}, Version: 1},
	}, states)
}

func testStore_UserState_GetMutedUsers(t *testing.T, s chat.Store) {
	ctx := context.Background()
	us := s
	group := chat.MustGenerateGroupChatID()
	otherChat := generateDmChatID()

	timed := model.MustGenerateUserID()
	forever := model.MustGenerateUserID()
	lapsed := model.MustGenerateUserID()
	cleared := model.MustGenerateUserID()
	elsewhere := model.MustGenerateUserID()

	_, _, err := us.SetMute(ctx, group, timed, chat.Mute{Until: at(200)})
	require.NoError(t, err)
	_, _, err = us.SetMute(ctx, group, forever, chat.Mute{Forever: true})
	require.NoError(t, err)
	_, _, err = us.SetMute(ctx, group, lapsed, chat.Mute{Until: at(100)})
	require.NoError(t, err)
	_, _, err = us.SetMute(ctx, group, cleared, chat.Mute{Forever: true})
	require.NoError(t, err)
	_, _, err = us.ClearMute(ctx, group, cleared)
	require.NoError(t, err)
	_, _, err = us.SetMute(ctx, otherChat, elsewhere, chat.Mute{Forever: true})
	require.NoError(t, err)

	requireMutedUsers(t, us, group, at(150), timed, forever)
	requireMutedUsers(t, us, group, at(50), timed, forever, lapsed)
	requireMutedUsers(t, us, group, at(250), forever)
	requireMutedUsers(t, us, otherChat, at(150), elsewhere)

	// A positive limit caps the result; a limit above the count is not a floor.
	got, err := us.GetMutedUsers(ctx, group, at(150), 1)
	require.NoError(t, err)
	require.Len(t, got, 1)
	assert.Contains(t, userIDValues([]*commonpb.UserId{timed, forever}), got[0].Value)

	got, err = us.GetMutedUsers(ctx, group, at(150), 5)
	require.NoError(t, err)
	assert.ElementsMatch(t, userIDValues([]*commonpb.UserId{timed, forever}), userIDValues(got))
}

// testStore_UserState_GetMutedUsersPage_Bounds reads a chat's muted users over
// key ranges: closed on both sides, open on either, bounds that are not
// themselves muted users, and a range that holds an unmuted record.
func testStore_UserState_GetMutedUsersPage_Bounds(t *testing.T, s chat.Store) {
	ctx := context.Background()
	us := s
	group := chat.MustGenerateGroupChatID()

	users := []*commonpb.UserId{model.MustGenerateUserID(), model.MustGenerateUserID(), model.MustGenerateUserID(), model.MustGenerateUserID()}
	slices.SortFunc(users, func(a, b *commonpb.UserId) int { return bytes.Compare(a.Value, b.Value) })
	for _, user := range users {
		_, _, err := us.SetMute(ctx, group, user, chat.Mute{Forever: true})
		require.NoError(t, err)
	}
	// A record with a cleared mute inside the range is read past, not
	// returned.
	between := model.MustGenerateUserID()
	_, _, err := us.SetMute(ctx, group, between, chat.Mute{Forever: true})
	require.NoError(t, err)
	_, _, err = us.ClearMute(ctx, group, between)
	require.NoError(t, err)

	// Both bounds are inclusive.
	got, err := us.GetMutedUsersPage(ctx, group, at(0), users[1], users[2])
	require.NoError(t, err)
	assert.Equal(t, userIDValues(users[1:3]), userIDValues(got))

	// Open on the low side, then on the high side.
	got, err = us.GetMutedUsersPage(ctx, group, at(0), nil, users[1])
	require.NoError(t, err)
	assert.Equal(t, userIDValues(users[:2]), userIDValues(got))

	got, err = us.GetMutedUsersPage(ctx, group, at(0), users[2], nil)
	require.NoError(t, err)
	assert.Equal(t, userIDValues(users[2:]), userIDValues(got))

	// Bounds need not be muted users, or users at all: the roster page that
	// supplies them may well hold no muter at either end.
	lo := justPast(users[0])
	got, err = us.GetMutedUsersPage(ctx, group, at(0), lo, users[3])
	require.NoError(t, err)
	assert.Equal(t, userIDValues(users[1:]), userIDValues(got))

	// A range past every user is empty.
	past := justPast(users[3])
	got, err = us.GetMutedUsersPage(ctx, group, at(0), past, nil)
	require.NoError(t, err)
	assert.Empty(t, got)
}

func testStore_UserState_MutedCount(t *testing.T, s chat.Store) {
	ctx := context.Background()
	us := s
	group := chat.MustGenerateGroupChatID()
	other := generateDmChatID()
	a := model.MustGenerateUserID()
	b := model.MustGenerateUserID()

	requireMutedCount(t, us, group, 0)

	// A first mute counts; the same mute again, or a different one replacing
	// it, does not.
	_, _, err := us.SetMute(ctx, group, a, chat.Mute{Until: at(100)})
	require.NoError(t, err)
	requireMutedCount(t, us, group, 1)
	_, _, err = us.SetMute(ctx, group, a, chat.Mute{Until: at(100)})
	require.NoError(t, err)
	requireMutedCount(t, us, group, 1)
	_, _, err = us.SetMute(ctx, group, a, chat.Mute{Forever: true})
	require.NoError(t, err)
	requireMutedCount(t, us, group, 1)

	_, _, err = us.SetMute(ctx, group, b, chat.Mute{Until: at(100)})
	require.NoError(t, err)
	requireMutedCount(t, us, group, 2)

	// A lapsed mute is still recorded, and still counted: the count bounds
	// the active mutes from above.
	requireMutedUsers(t, us, group, at(150), a)
	requireMutedCount(t, us, group, 2)

	// A clear counts down; clearing again, or clearing what was never set,
	// does not.
	_, _, err = us.ClearMute(ctx, group, b)
	require.NoError(t, err)
	requireMutedCount(t, us, group, 1)
	_, _, err = us.ClearMute(ctx, group, b)
	require.NoError(t, err)
	requireMutedCount(t, us, group, 1)
	_, _, err = us.ClearMute(ctx, group, model.MustGenerateUserID())
	require.NoError(t, err)
	requireMutedCount(t, us, group, 1)

	// Muting again after a clear counts again.
	_, _, err = us.SetMute(ctx, group, b, chat.Mute{Forever: true})
	require.NoError(t, err)
	requireMutedCount(t, us, group, 2)

	// Counts are per chat.
	requireMutedCount(t, us, other, 0)
	_, _, err = us.SetMute(ctx, other, a, chat.Mute{Forever: true})
	require.NoError(t, err)
	requireMutedCount(t, us, other, 1)
	requireMutedCount(t, us, group, 2)
}

func testStore_Activity_RecentSenders(t *testing.T, s chat.Store) {
	ctx := context.Background()

	group := putGroupChat(t, s, "activity", at(0), model.MustGenerateUserID())
	a, b, c := model.MustGenerateUserID(), model.MustGenerateUserID(), model.MustGenerateUserID()

	senders, err := s.GetRecentSenders(ctx, group.ID, 0)
	require.NoError(t, err)
	require.Empty(t, senders)

	for _, send := range []struct {
		user *commonpb.UserId
		at   time.Time
	}{
		{a, at(100)},
		{b, at(300)},
		{c, at(200)},
	} {
		recorded, err := s.RecordSend(ctx, group.ID, send.user, send.at)
		require.NoError(t, err)
		require.True(t, recorded)
	}

	// Most recently sent first, whole when unbounded.
	senders, err = s.GetRecentSenders(ctx, group.ID, 0)
	require.NoError(t, err)
	requireRecentSenders(t, senders, b, at(300), c, at(200), a, at(100))

	// A limit keeps the most recent.
	senders, err = s.GetRecentSenders(ctx, group.ID, 2)
	require.NoError(t, err)
	requireRecentSenders(t, senders, b, at(300), c, at(200))

	// A later send moves its sender to the front.
	recorded, err := s.RecordSend(ctx, group.ID, a, at(400))
	require.NoError(t, err)
	require.True(t, recorded)
	senders, err = s.GetRecentSenders(ctx, group.ID, 0)
	require.NoError(t, err)
	requireRecentSenders(t, senders, a, at(400), b, at(300), c, at(200))
}

func testStore_Activity_Throttle(t *testing.T, s chat.Store) {
	ctx := context.Background()

	groupID := chat.MustGenerateGroupChatID()
	user := model.MustGenerateUserID()
	interval := chat.ActivityRecordInterval

	recorded, err := s.RecordSend(ctx, groupID, user, at(1000))
	require.NoError(t, err)
	require.True(t, recorded)

	// Within the interval: not recorded, and the record keeps its time.
	for _, sentAt := range []time.Time{
		at(1000),
		at(1000).Add(interval / 2),
		at(1000).Add(interval - time.Millisecond),
	} {
		recorded, err = s.RecordSend(ctx, groupID, user, sentAt)
		require.NoError(t, err)
		require.False(t, recorded, "sent at %v", sentAt)
	}
	senders, err := s.GetRecentSenders(ctx, groupID, 0)
	require.NoError(t, err)
	requireRecentSenders(t, senders, user, at(1000))

	// A send older than the record never moves it back.
	recorded, err = s.RecordSend(ctx, groupID, user, at(1000).Add(-time.Hour))
	require.NoError(t, err)
	require.False(t, recorded)

	// A full interval later is recorded.
	recorded, err = s.RecordSend(ctx, groupID, user, at(1000).Add(interval))
	require.NoError(t, err)
	require.True(t, recorded)
	senders, err = s.GetRecentSenders(ctx, groupID, 0)
	require.NoError(t, err)
	requireRecentSenders(t, senders, user, at(1000).Add(interval))
}

func testStore_Activity_Precision(t *testing.T, s chat.Store) {
	ctx := context.Background()

	groupID := chat.MustGenerateGroupChatID()
	user := model.MustGenerateUserID()

	// Recorded at millisecond precision.
	recorded, err := s.RecordSend(ctx, groupID, user, at(10).Add(1_234_567*time.Microsecond))
	require.NoError(t, err)
	require.True(t, recorded)
	senders, err := s.GetRecentSenders(ctx, groupID, 0)
	require.NoError(t, err)
	requireRecentSenders(t, senders, user, at(10).Add(1_234*time.Millisecond))

	_, err = s.RecordSend(ctx, groupID, user, time.Unix(0, 0))
	require.Error(t, err)
}

func testStore_Activity_Scope(t *testing.T, s chat.Store) {
	ctx := context.Background()

	// Recorded against the chat ID alone: no canonical record or membership
	// is read, so neither a group nobody created nor a non-member stops it.
	groupID := chat.MustGenerateGroupChatID()
	other := chat.MustGenerateGroupChatID()
	user := model.MustGenerateUserID()
	recorded, err := s.RecordSend(ctx, groupID, user, at(50))
	require.NoError(t, err)
	require.True(t, recorded)

	// Records belong to their group.
	senders, err := s.GetRecentSenders(ctx, other, 0)
	require.NoError(t, err)
	require.Empty(t, senders)
	recorded, err = s.RecordSend(ctx, other, user, at(50))
	require.NoError(t, err)
	require.True(t, recorded, "the throttle is per group")

	// Groups only.
	dmID := generateDmChatID()
	_, err = s.RecordSend(ctx, dmID, user, at(50))
	require.Error(t, err)
	_, err = s.GetRecentSenders(ctx, dmID, 0)
	require.Error(t, err)
}

func testStore_Activity_Concurrent(t *testing.T, s chat.Store) {
	ctx := context.Background()

	groupID := chat.MustGenerateGroupChatID()
	user := model.MustGenerateUserID()

	// A send delivered many times at once is recorded exactly once.
	const writers = 8
	var wg sync.WaitGroup
	results := make([]bool, writers)
	errs := make([]error, writers)
	for i := range writers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			results[i], errs[i] = s.RecordSend(ctx, groupID, user, at(70))
		}()
	}
	wg.Wait()

	recordedCount := 0
	for i := range writers {
		require.NoError(t, errs[i])
		if results[i] {
			recordedCount++
		}
	}
	require.Equal(t, 1, recordedCount)
	senders, err := s.GetRecentSenders(ctx, groupID, 0)
	require.NoError(t, err)
	requireRecentSenders(t, senders, user, at(70))
}

// requireRecentSenders asserts senders is exactly the given (user, last sent)
// pairs, in order.
func requireRecentSenders(t *testing.T, senders []chat.RecentSender, want ...any) {
	t.Helper()
	require.Len(t, senders, len(want)/2)
	for i, sender := range senders {
		require.Equal(t, want[2*i].(*commonpb.UserId).Value, sender.UserID.Value, "sender %d", i)
		require.True(t, want[2*i+1].(time.Time).Equal(sender.LastSentAt), "sender %d sent at %v, want %v", i, sender.LastSentAt, want[2*i+1])
	}
}

// keyEnvelope builds a key envelope stored by wrappedBy. The store never
// opens one, so any bytes of the right length stand in for a wrapped key.
func keyEnvelope(fill byte, wrappedBy *commonpb.UserId) chat.KeyEnvelope {
	return chat.KeyEnvelope{
		Scheme:     chatpb.KeyEnvelope_X25519_XCHACHA20POLY1305,
		Nonce:      bytes.Repeat([]byte{fill}, 24),
		Ciphertext: bytes.Repeat([]byte{fill}, 48),
		WrappedBy:  wrappedBy,
	}
}

func testStore_KeyEnvelope_SetAndGet(t *testing.T, s chat.Store) {
	ctx := context.Background()

	// Stored against the IDs alone: no canonical record or membership is
	// read, so neither a group nobody created nor a non-member stops it.
	groupID := chat.MustGenerateGroupChatID()
	user := model.MustGenerateUserID()

	_, err := s.GetKeyEnvelope(ctx, groupID, user)
	require.ErrorIs(t, err, chat.ErrKeyEnvelopeNotFound)

	// The first envelope a user stores for themself is stored as given, and
	// read back exactly.
	first := keyEnvelope(1, user)
	stored, err := s.SetKeyEnvelope(ctx, groupID, user, first)
	require.NoError(t, err)
	require.True(t, first.Equal(stored))
	got, err := s.GetKeyEnvelope(ctx, groupID, user)
	require.NoError(t, err)
	require.True(t, first.Equal(got))
	require.Equal(t, chatpb.KeyEnvelope_X25519_XCHACHA20POLY1305, got.Scheme)
	require.Equal(t, user.Value, got.WrappedBy.Value)

	// Storing it again is a no-op that returns it.
	stored, err = s.SetKeyEnvelope(ctx, groupID, user, first)
	require.NoError(t, err)
	require.True(t, first.Equal(stored))

	// An envelope belongs to one user on one chat.
	_, err = s.GetKeyEnvelope(ctx, groupID, model.MustGenerateUserID())
	require.ErrorIs(t, err, chat.ErrKeyEnvelopeNotFound)
	_, err = s.GetKeyEnvelope(ctx, chat.MustGenerateGroupChatID(), user)
	require.ErrorIs(t, err, chat.ErrKeyEnvelopeNotFound)
	otherGroup := chat.MustGenerateGroupChatID()
	second := keyEnvelope(2, user)
	stored, err = s.SetKeyEnvelope(ctx, otherGroup, user, second)
	require.NoError(t, err)
	require.True(t, second.Equal(stored))
	got, err = s.GetKeyEnvelope(ctx, groupID, user)
	require.NoError(t, err)
	require.True(t, first.Equal(got))

	// Groups only, and an envelope names who stored it.
	_, err = s.SetKeyEnvelope(ctx, generateDmChatID(), user, first)
	require.Error(t, err)
	_, err = s.SetKeyEnvelope(ctx, groupID, user, keyEnvelope(3, nil))
	require.Error(t, err)
}

// testStore_KeyEnvelope_OwnWrapStands pins which envelope stands (see
// chat.Store.SetKeyEnvelope): one the user wrapped themself is never
// replaced, and one someone else wrapped for them always is.
func testStore_KeyEnvelope_OwnWrapStands(t *testing.T, s chat.Store) {
	ctx := context.Background()

	groupID := chat.MustGenerateGroupChatID()
	creator := model.MustGenerateUserID()
	member := model.MustGenerateUserID()

	// A different envelope of the user's own changes nothing, and the stored
	// one is returned.
	first := keyEnvelope(1, creator)
	_, err := s.SetKeyEnvelope(ctx, groupID, creator, first)
	require.NoError(t, err)
	stored, err := s.SetKeyEnvelope(ctx, groupID, creator, keyEnvelope(2, creator))
	require.NoError(t, err)
	require.True(t, first.Equal(stored))
	got, err := s.GetKeyEnvelope(ctx, groupID, creator)
	require.NoError(t, err)
	require.True(t, first.Equal(got))

	// An envelope the creator wrapped for a member is stored, and replaced
	// by a later one from the creator...
	admitted := keyEnvelope(3, creator)
	stored, err = s.SetKeyEnvelope(ctx, groupID, member, admitted)
	require.NoError(t, err)
	require.True(t, admitted.Equal(stored))
	readmitted := keyEnvelope(4, creator)
	stored, err = s.SetKeyEnvelope(ctx, groupID, member, readmitted)
	require.NoError(t, err)
	require.True(t, readmitted.Equal(stored))

	// ...and by the member's own, which then stands against both.
	rewrapped := keyEnvelope(5, member)
	stored, err = s.SetKeyEnvelope(ctx, groupID, member, rewrapped)
	require.NoError(t, err)
	require.True(t, rewrapped.Equal(stored))
	for _, later := range []chat.KeyEnvelope{keyEnvelope(6, creator), keyEnvelope(7, member)} {
		stored, err = s.SetKeyEnvelope(ctx, groupID, member, later)
		require.NoError(t, err)
		require.True(t, rewrapped.Equal(stored))
	}
	got, err = s.GetKeyEnvelope(ctx, groupID, member)
	require.NoError(t, err)
	require.True(t, rewrapped.Equal(got))
	require.Equal(t, member.Value, got.WrappedBy.Value)

	// The same bytes stored by someone else are not the same envelope.
	require.False(t, rewrapped.Equal(keyEnvelope(5, creator)))
}

// testStore_KeyEnvelope_DiscardedOnLeave pins that a departure removes the
// departing user's envelope exactly when the caller asks, and only when the
// departure actually happens (see chat.Store.RemoveGroupMember).
func testStore_KeyEnvelope_DiscardedOnLeave(t *testing.T, s chat.Store) {
	ctx := context.Background()

	creator := model.MustGenerateUserID()
	member := model.MustGenerateUserID()
	stranger := model.MustGenerateUserID()
	c := putGroupChat(t, s, "Private", at(100), creator, member)

	creatorEnvelope := keyEnvelope(1, creator)
	_, err := s.SetKeyEnvelope(ctx, c.ID, creator, creatorEnvelope)
	require.NoError(t, err)
	_, err = s.SetKeyEnvelope(ctx, c.ID, member, keyEnvelope(2, member))
	require.NoError(t, err)
	strangerEnvelope := keyEnvelope(3, stranger)
	_, err = s.SetKeyEnvelope(ctx, c.ID, stranger, strangerEnvelope)
	require.NoError(t, err)

	// A departure asked to discard takes the envelope with it, and no one
	// else's.
	changed, _, err := s.RemoveGroupMember(ctx, c.ID, member, true)
	require.NoError(t, err)
	require.True(t, changed)
	_, err = s.GetKeyEnvelope(ctx, c.ID, member)
	require.ErrorIs(t, err, chat.ErrKeyEnvelopeNotFound)
	got, err := s.GetKeyEnvelope(ctx, c.ID, creator)
	require.NoError(t, err)
	require.True(t, creatorEnvelope.Equal(got))

	// Nothing stands once it is gone: a returning member's next envelope is
	// stored.
	_, _, err = s.AddGroupMembers(ctx, c.ID, []*commonpb.UserId{member})
	require.NoError(t, err)
	next := keyEnvelope(4, creator)
	stored, err := s.SetKeyEnvelope(ctx, c.ID, member, next)
	require.NoError(t, err)
	require.True(t, next.Equal(stored))

	// A departure not asked to discard keeps it, as the creator's is kept.
	changed, _, err = s.RemoveGroupMember(ctx, c.ID, creator, false)
	require.NoError(t, err)
	require.True(t, changed)
	got, err = s.GetKeyEnvelope(ctx, c.ID, creator)
	require.NoError(t, err)
	require.True(t, creatorEnvelope.Equal(got))

	// A user with no envelope leaves all the same.
	changed, _, err = s.RemoveGroupMember(ctx, c.ID, member, true)
	require.NoError(t, err)
	require.True(t, changed)
	changed, _, err = s.AddGroupMembers(ctx, c.ID, []*commonpb.UserId{member})
	require.NoError(t, err)
	require.True(t, changed)
	changed, _, err = s.RemoveGroupMember(ctx, c.ID, member, true)
	require.NoError(t, err)
	require.True(t, changed)

	// A no-op removes nothing: the envelope goes with a departure, not with
	// the request for one.
	changed, _, err = s.RemoveGroupMember(ctx, c.ID, stranger, true)
	require.NoError(t, err)
	require.False(t, changed)
	got, err = s.GetKeyEnvelope(ctx, c.ID, stranger)
	require.NoError(t, err)
	require.True(t, strangerEnvelope.Equal(got))
}

// testStore_KeyEnvelope_Concurrent pins that concurrent first writes agree:
// exactly one envelope is stored, and every caller is told which.
func testStore_KeyEnvelope_Concurrent(t *testing.T, s chat.Store) {
	ctx := context.Background()

	groupID := chat.MustGenerateGroupChatID()
	user := model.MustGenerateUserID()

	const writers = 8
	results := make([]chat.KeyEnvelope, writers)
	errs := make([]error, writers)
	var wg sync.WaitGroup
	for i := 0; i < writers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			results[i], errs[i] = s.SetKeyEnvelope(ctx, groupID, user, keyEnvelope(byte(i+1), user))
		}()
	}
	wg.Wait()

	got, err := s.GetKeyEnvelope(ctx, groupID, user)
	require.NoError(t, err)
	for i := 0; i < writers; i++ {
		require.NoError(t, errs[i])
		require.True(t, got.Equal(results[i]))
	}
}

// lobbyLimits are the caps the lobby tests run under unless one says
// otherwise: wide enough never to be hit.
var lobbyLimits = chat.LobbyLimits{LobbySize: 100, LobbiesPerUser: 100}

// waitForLobbyPage reads chatID's lobby whole until it holds n entries: the
// page is read off an index that may trail the writes (see
// chat.Store.GetLobbyPage).
func waitForLobbyPage(t *testing.T, s chat.Store, chatID *commonpb.ChatId, n int) []chat.LobbyEntry {
	t.Helper()
	var entries []chat.LobbyEntry
	require.Eventually(t, func() bool {
		var err error
		entries, err = s.GetLobbyPage(context.Background(), chatID, nil, 0)
		require.NoError(t, err)
		return len(entries) == n
	}, 5*time.Second, 10*time.Millisecond)
	return entries
}

// testStore_Lobby_EnterAndLeave pins the lifecycle of one entry: recorded
// against the IDs alone, read back strongly consistent, a repeat the no-op
// that keeps the first entry's time, and a leave that is a no-op once gone.
func testStore_Lobby_EnterAndLeave(t *testing.T, s chat.Store) {
	ctx := context.Background()
	groupID := chat.MustGenerateGroupChatID()
	user := model.MustGenerateUserID()

	entries, err := s.GetLobbyEntries(ctx, user, []*commonpb.ChatId{groupID})
	require.NoError(t, err)
	require.Empty(t, entries)
	changed, err := s.LeaveLobby(ctx, groupID, user)
	require.NoError(t, err)
	require.False(t, changed)

	before := time.Now()
	entry, changed, err := s.EnterLobby(ctx, groupID, user, lobbyLimits)
	require.NoError(t, err)
	require.True(t, changed)
	require.Equal(t, user.Value, entry.UserID.Value)
	require.False(t, entry.EnteredAt.Before(before.Add(-time.Second)))
	require.False(t, entry.EnteredAt.After(time.Now().Add(time.Second)))

	entries, err = s.GetLobbyEntries(ctx, user, []*commonpb.ChatId{groupID, chat.MustGenerateGroupChatID()})
	require.NoError(t, err)
	require.Len(t, entries, 1)
	require.Equal(t, user.Value, entries[string(groupID.Value)].UserID.Value)
	require.True(t, entry.EnteredAt.Equal(entries[string(groupID.Value)].EnteredAt))

	// A repeat is a no-op that returns the entry as it stands.
	again, changed, err := s.EnterLobby(ctx, groupID, user, lobbyLimits)
	require.NoError(t, err)
	require.False(t, changed)
	require.True(t, entry.EnteredAt.Equal(again.EnteredAt))

	// An entry belongs to one user in one lobby.
	other := model.MustGenerateUserID()
	entries, err = s.GetLobbyEntries(ctx, other, []*commonpb.ChatId{groupID})
	require.NoError(t, err)
	require.Empty(t, entries)
	otherGroup := chat.MustGenerateGroupChatID()
	_, changed, err = s.EnterLobby(ctx, otherGroup, user, lobbyLimits)
	require.NoError(t, err)
	require.True(t, changed)
	entries, err = s.GetLobbyEntries(ctx, user, []*commonpb.ChatId{groupID, otherGroup})
	require.NoError(t, err)
	require.Len(t, entries, 2)

	changed, err = s.LeaveLobby(ctx, groupID, user)
	require.NoError(t, err)
	require.True(t, changed)
	entries, err = s.GetLobbyEntries(ctx, user, []*commonpb.ChatId{groupID, otherGroup})
	require.NoError(t, err)
	require.Len(t, entries, 1)
	require.Contains(t, entries, string(otherGroup.Value))
	changed, err = s.LeaveLobby(ctx, groupID, user)
	require.NoError(t, err)
	require.False(t, changed)

	// Lobbies are a group's alone.
	dm := generateDmChatID()
	_, _, err = s.EnterLobby(ctx, dm, user, lobbyLimits)
	require.Error(t, err)
	_, err = s.LeaveLobby(ctx, dm, user)
	require.Error(t, err)
	_, err = s.GetLobbyPage(ctx, dm, nil, 0)
	require.Error(t, err)
}

// testStore_Lobby_Limits pins the caps: a lobby at LobbySize refuses the
// next user as ErrLobbyFull, a user in LobbiesPerUser lobbies is refused the
// next as ErrTooManyLobbies, the lobby's cap is judged first, a user already
// waiting is never refused, and a departure frees a place.
func testStore_Lobby_Limits(t *testing.T, s chat.Store) {
	ctx := context.Background()
	limits := chat.LobbyLimits{LobbySize: 2, LobbiesPerUser: 2}
	groupA, groupB, groupC := chat.MustGenerateGroupChatID(), chat.MustGenerateGroupChatID(), chat.MustGenerateGroupChatID()
	user1, user2, user3 := model.MustGenerateUserID(), model.MustGenerateUserID(), model.MustGenerateUserID()

	for _, u := range []*commonpb.UserId{user1, user2} {
		_, changed, err := s.EnterLobby(ctx, groupA, u, limits)
		require.NoError(t, err)
		require.True(t, changed)
	}
	_, _, err := s.EnterLobby(ctx, groupA, user3, limits)
	require.ErrorIs(t, err, chat.ErrLobbyFull)
	entries, err := s.GetLobbyEntries(ctx, user3, []*commonpb.ChatId{groupA})
	require.NoError(t, err)
	require.Empty(t, entries)

	_, changed, err := s.EnterLobby(ctx, groupB, user1, limits)
	require.NoError(t, err)
	require.True(t, changed)
	_, _, err = s.EnterLobby(ctx, groupC, user1, limits)
	require.ErrorIs(t, err, chat.ErrTooManyLobbies)

	// A user already waiting is the no-op whatever the counts say.
	_, changed, err = s.EnterLobby(ctx, groupA, user1, limits)
	require.NoError(t, err)
	require.False(t, changed)

	// A user at their cap entering a full lobby hears of the lobby first.
	_, _, err = s.EnterLobby(ctx, groupA, user1, chat.LobbyLimits{LobbySize: 1, LobbiesPerUser: 1})
	require.NoError(t, err) // still the no-op: already in it
	_, _, err = s.EnterLobby(ctx, groupA, user3, chat.LobbyLimits{LobbySize: 1, LobbiesPerUser: 0})
	require.ErrorIs(t, err, chat.ErrLobbyFull)

	// A departure frees the place, and the user's own count.
	changed, err = s.LeaveLobby(ctx, groupA, user2)
	require.NoError(t, err)
	require.True(t, changed)
	_, changed, err = s.EnterLobby(ctx, groupA, user3, limits)
	require.NoError(t, err)
	require.True(t, changed)
	changed, err = s.LeaveLobby(ctx, groupB, user1)
	require.NoError(t, err)
	require.True(t, changed)
	_, changed, err = s.EnterLobby(ctx, groupC, user1, limits)
	require.NoError(t, err)
	require.True(t, changed)
}

// testStore_Lobby_Page pins the lobby's order and its paging: earliest
// entered first, a page resuming strictly after a position, no limit reading
// the whole lobby, and an empty result for a lobby nobody is in.
func testStore_Lobby_Page(t *testing.T, s chat.Store) {
	ctx := context.Background()
	groupID := chat.MustGenerateGroupChatID()

	page, err := s.GetLobbyPage(ctx, groupID, nil, 0)
	require.NoError(t, err)
	require.Empty(t, page)

	const n = 5
	users := make([]*commonpb.UserId, n)
	for i := range users {
		users[i] = model.MustGenerateUserID()
		_, changed, err := s.EnterLobby(ctx, groupID, users[i], lobbyLimits)
		require.NoError(t, err)
		require.True(t, changed)
		// Entry times are the store's clock; a later entry must sort later.
		time.Sleep(2 * time.Millisecond)
	}
	// A different lobby is not in the page.
	_, _, err = s.EnterLobby(ctx, chat.MustGenerateGroupChatID(), model.MustGenerateUserID(), lobbyLimits)
	require.NoError(t, err)

	whole := waitForLobbyPage(t, s, groupID, n)
	for i, e := range whole {
		require.Equal(t, users[i].Value, e.UserID.Value, i)
		if i > 0 {
			require.Negative(t, whole[i-1].Position().Compare(e.Position()))
		}
	}

	// Pages of two walk the same order and stop at the end.
	var walked []chat.LobbyEntry
	var after *chat.LobbyPosition
	for {
		page, err := s.GetLobbyPage(ctx, groupID, after, 2)
		require.NoError(t, err)
		require.LessOrEqual(t, len(page), 2)
		walked = append(walked, page...)
		if len(page) < 2 {
			break
		}
		pos := page[len(page)-1].Position()
		after = &pos
	}
	require.Len(t, walked, n)
	for i, e := range walked {
		require.Equal(t, users[i].Value, e.UserID.Value, i)
		require.True(t, whole[i].EnteredAt.Equal(e.EnteredAt), i)
	}

	// Resuming from the last position reads nothing more.
	last := whole[n-1].Position()
	page, err = s.GetLobbyPage(ctx, groupID, &last, 10)
	require.NoError(t, err)
	require.Empty(t, page)
}

// testStore_Lobby_Admit pins the admission: in one write the user's entry
// leaves the lobby, their envelope lands (over any they held) and they join
// the roster at its next version; a member already is a no-op, a user not
// waiting ErrNotInLobby with nothing written, and a group that does not exist
// ErrChatNotFound. The lobby's count is freed by the admission.
func testStore_Lobby_Admit(t *testing.T, s chat.Store) {
	ctx := context.Background()
	creator := model.MustGenerateUserID()
	user := model.MustGenerateUserID()
	groupID := chat.MustGenerateGroupChatID()
	require.NoError(t, s.PutChat(ctx, &chat.Chat{
		ID:           groupID,
		Type:         chatpb.ChatType_GROUP,
		Members:      []*commonpb.UserId{creator},
		Title:        "Private",
		IsPrivate:    true,
		CreatorID:    creator,
		LastActivity: at(1),
	}))
	tight := chat.LobbyLimits{LobbySize: 1, LobbiesPerUser: 10}

	// A member has no place in the lobby: the creator is refused in the
	// write, and nothing is counted for the refusal.
	_, _, err := s.EnterLobby(ctx, groupID, creator, tight)
	require.ErrorIs(t, err, chat.ErrAlreadyMember)
	entries, err := s.GetLobbyEntries(ctx, creator, []*commonpb.ChatId{groupID})
	require.NoError(t, err)
	require.Empty(t, entries)

	// Not waiting: nothing happens.
	_, _, err = s.AdmitFromLobby(ctx, groupID, user, keyEnvelope(1, creator))
	require.ErrorIs(t, err, chat.ErrNotInLobby)
	isMember, err := s.IsMember(ctx, groupID, user)
	require.NoError(t, err)
	require.False(t, isMember)
	_, err = s.GetKeyEnvelope(ctx, groupID, user)
	require.ErrorIs(t, err, chat.ErrKeyEnvelopeNotFound)

	// Waiting, holding a stale envelope of their own from an earlier stay.
	_, changed, err := s.EnterLobby(ctx, groupID, user, tight)
	require.NoError(t, err)
	require.True(t, changed)
	_, err = s.SetKeyEnvelope(ctx, groupID, user, keyEnvelope(9, user))
	require.NoError(t, err)

	_, _, err = s.AdmitFromLobby(ctx, groupID, user, chat.KeyEnvelope{Scheme: chatpb.KeyEnvelope_X25519_XCHACHA20POLY1305})
	require.Error(t, err) // no wrapper
	_, _, err = s.AdmitFromLobby(ctx, chat.MustGenerateGroupChatID(), user, keyEnvelope(1, creator))
	require.ErrorIs(t, err, chat.ErrChatNotFound)
	_, _, err = s.AdmitFromLobby(ctx, generateDmChatID(), user, keyEnvelope(1, creator))
	require.Error(t, err)

	admitted := keyEnvelope(1, creator)
	changed, roster, err := s.AdmitFromLobby(ctx, groupID, user, admitted)
	require.NoError(t, err)
	require.True(t, changed)
	require.Equal(t, chat.RosterSummary{MemberCount: 2, Version: 1}, roster)
	isMember, err = s.IsMember(ctx, groupID, user)
	require.NoError(t, err)
	require.True(t, isMember)
	got, err := s.GetKeyEnvelope(ctx, groupID, user)
	require.NoError(t, err)
	require.True(t, admitted.Equal(got))
	entries, err = s.GetLobbyEntries(ctx, user, []*commonpb.ChatId{groupID})
	require.NoError(t, err)
	require.Empty(t, entries)
	records, err := s.GetGroupMemberRecords(ctx, user, []*commonpb.ChatId{groupID})
	require.NoError(t, err)
	require.Equal(t, uint64(1), records[string(groupID.Value)].Version)

	// The admitted member cannot re-enter the lobby, and the refusal left the
	// lobby's one place free.
	_, _, err = s.EnterLobby(ctx, groupID, user, tight)
	require.ErrorIs(t, err, chat.ErrAlreadyMember)

	// A member already: nothing written, the summary as it stands.
	changed, roster, err = s.AdmitFromLobby(ctx, groupID, user, keyEnvelope(2, creator))
	require.NoError(t, err)
	require.False(t, changed)
	require.Equal(t, chat.RosterSummary{MemberCount: 2, Version: 1}, roster)
	got, err = s.GetKeyEnvelope(ctx, groupID, user)
	require.NoError(t, err)
	require.True(t, admitted.Equal(got))

	// The admission freed the lobby's one place.
	_, changed, err = s.EnterLobby(ctx, groupID, model.MustGenerateUserID(), tight)
	require.NoError(t, err)
	require.True(t, changed)
	waitForLobbyPage(t, s, groupID, 1)
}

// testStore_Lobby_Concurrent pins that the lobby's cap holds under
// concurrent entries: exactly LobbySize of many simultaneous entrants are
// recorded, every other is ErrLobbyFull, and the count agrees with the page.
func testStore_Lobby_Concurrent(t *testing.T, s chat.Store) {
	ctx := context.Background()
	groupID := chat.MustGenerateGroupChatID()
	limits := chat.LobbyLimits{LobbySize: 5, LobbiesPerUser: 10}

	const entrants = 20
	results := make([]error, entrants)
	var wg sync.WaitGroup
	for i := range entrants {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _, results[i] = s.EnterLobby(ctx, groupID, model.MustGenerateUserID(), limits)
		}()
	}
	wg.Wait()

	var admitted, refused int
	for _, err := range results {
		switch {
		case err == nil:
			admitted++
		case errors.Is(err, chat.ErrLobbyFull):
			refused++
		default:
			require.NoError(t, err)
		}
	}
	require.Equal(t, limits.LobbySize, admitted)
	require.Equal(t, entrants-limits.LobbySize, refused)
	waitForLobbyPage(t, s, groupID, limits.LobbySize)

	// One leaves, one more gets in.
	page, err := s.GetLobbyPage(ctx, groupID, nil, 1)
	require.NoError(t, err)
	require.Len(t, page, 1)
	changed, err := s.LeaveLobby(ctx, groupID, page[0].UserID)
	require.NoError(t, err)
	require.True(t, changed)
	_, changed, err = s.EnterLobby(ctx, groupID, model.MustGenerateUserID(), limits)
	require.NoError(t, err)
	require.True(t, changed)
}
