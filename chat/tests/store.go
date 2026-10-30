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
		testStore_DmOpeningIntent,
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
		testStore_GroupChat_MinimumSpeakerBalance,
		testStore_GroupChat_Rules,
		testStore_GroupChat_Creator,
		testStore_GroupChat_Picture,
		testStore_GroupChat_Edit,
		testStore_GroupChat_EditMinimumBalances,
		testStore_GroupChat_DescriptionAndCover,
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
		testStore_GroupChat_ChatsByID,
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
		testStore_Restriction_Silence,
		testStore_Restriction_IndependentOfMute,
		testStore_Restriction_BanMember,
		testStore_Restriction_BanNonMember,
		testStore_Restriction_BanRefusesWaysIn,
		testStore_Restriction_BanRaces,
		testStore_Restriction_List,
		testStore_Restriction_ViewerStatesForUsers,
		testStore_GroupChat_MembersByID,
		testStore_Activity_RecentSenders,
		testStore_Activity_Throttle,
		testStore_Activity_Precision,
		testStore_Activity_Scope,
		testStore_Activity_Concurrent,
		testStore_Activity_Score,
		testStore_Activity_ScoreConcurrent,
		testStore_Activity_ActiveSenders,
		testStore_KeyEnvelope_SetAndGet,
		testStore_KeyEnvelope_OwnWrapStands,
		testStore_KeyEnvelope_DiscardedOnLeave,
		testStore_KeyEnvelope_Concurrent,
		testStore_KeyEnvelope_InviteCopy,
		testStore_Lobby_EnterAndLeave,
		testStore_Lobby_Limits,
		testStore_Lobby_Page,
		testStore_Lobby_Admit,
		testStore_Lobby_Concurrent,
		testStore_FeaturedGroups_SetAndGet,
		testStore_FeaturedGroups_NoOp,
		testStore_FeaturedGroups_Invalid,
		testStore_FeaturedGroups_Concurrent,
		testStore_Holdings_PutAndGet,
		testStore_Holdings_NeverBackwards,
		testStore_Holdings_Invalid,
		testStore_Holdings_Unlisted,
		testStore_Holdings_MintMismatch,
		testStore_Holdings_TopByMint,
		testStore_HoldingsQueue_EnqueuedAtCreation,
		testStore_HoldingsQueue_ClaimAndReschedule,
		testStore_HoldingsQueue_EntryWithoutSnapshot,
		testStore_Invite_CreateAndGet,
		testStore_Invite_CreateExisting,
		testStore_Invite_CapCountsActive,
		testStore_Invite_CreateConcurrent,
		testStore_Invite_Page,
		testStore_Invite_Revoke,
		testStore_InviteJoin_Join,
		testStore_InviteJoin_AlreadyMember,
		testStore_InviteJoin_OwnInvite,
		testStore_InviteJoin_Rejoin,
		testStore_InviteJoin_Limits,
		testStore_InviteJoin_Status,
		testStore_InviteJoin_Banned,
		testStore_InviteJoin_Lobby,
		testStore_Pin_Empty,
		testStore_Pin_PinAndGet,
		testStore_Pin_RepinNoOp,
		testStore_Pin_Order,
		testStore_Pin_LatestByPosition,
		testStore_Pin_Unpin,
		testStore_Pin_UnpinThenRepin,
		testStore_Pin_Cap,
		testStore_Pin_Invalid,
		testStore_Pin_ChatsIndependent,
		testStore_Pin_ConcurrentCap,
		testStore_Pin_ConcurrentSameMessage,
		testStore_InviteJoin_Envelope,
		testStore_InviteJoin_Revoke,
		testStore_InviteJoin_Concurrent,
		testStore_InviteJoin_RefusedKeepsLobby,
		testStore_InviteJoin_RaceAdmit,
		testStore_InviteJoin_RaceLobby,
		testStore_InviteJoin_RaceBan,
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

func testStore_DmOpeningIntent(t *testing.T, s chat.Store) {
	ctx := context.Background()

	newDm := func(opening *commonpb.IntentId) *chat.Chat {
		return &chat.Chat{
			ID:              generateDmChatID(),
			Type:            chatpb.ChatType_DM,
			Members:         []*commonpb.UserId{model.MustGenerateUserID(), model.MustGenerateUserID()},
			OpeningIntentID: opening,
			LastActivity:    at(1),
		}
	}

	t.Run("recorded at creation", func(t *testing.T) {
		first := &commonpb.IntentId{Value: randomBytes(32)}
		dm := newDm(first)
		require.NoError(t, s.PutChat(ctx, dm))

		got, err := s.GetDmOpeningIntentID(ctx, dm.ID)
		require.NoError(t, err)
		require.Equal(t, first.Value, got.Value)

		record, err := s.GetChatByID(ctx, dm.ID)
		require.NoError(t, err)
		require.Equal(t, first.Value, record.OpeningIntentID.Value)

		// A racing payment's PutChat loses, and the first opener stands.
		racer := dm.Clone()
		racer.OpeningIntentID = &commonpb.IntentId{Value: randomBytes(32)}
		require.ErrorIs(t, s.PutChat(ctx, racer), chat.ErrChatExists)

		got, err = s.GetDmOpeningIntentID(ctx, dm.ID)
		require.NoError(t, err)
		require.Equal(t, first.Value, got.Value)
	})

	t.Run("none recorded", func(t *testing.T) {
		dm := newDm(nil)
		require.NoError(t, s.PutChat(ctx, dm))

		got, err := s.GetDmOpeningIntentID(ctx, dm.ID)
		require.NoError(t, err)
		require.Nil(t, got)

		record, err := s.GetChatByID(ctx, dm.ID)
		require.NoError(t, err)
		require.Nil(t, record.OpeningIntentID)
	})

	t.Run("dm not found", func(t *testing.T) {
		_, err := s.GetDmOpeningIntentID(ctx, generateDmChatID())
		require.ErrorIs(t, err, chat.ErrChatNotFound)
	})

	t.Run("groups carry none", func(t *testing.T) {
		group := &chat.Chat{
			ID:              chat.MustGenerateGroupChatID(),
			Type:            chatpb.ChatType_GROUP,
			Members:         []*commonpb.UserId{model.MustGenerateUserID()},
			Title:           "Group",
			OpeningIntentID: &commonpb.IntentId{Value: randomBytes(32)},
			LastActivity:    at(1),
		}
		require.NoError(t, s.PutChat(ctx, group))

		record, err := s.GetChatByID(ctx, group.ID)
		require.NoError(t, err)
		require.Nil(t, record.OpeningIntentID)

		_, err = s.GetDmOpeningIntentID(ctx, group.ID)
		require.Error(t, err)
		require.NotErrorIs(t, err, chat.ErrChatNotFound)
	})
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

func testStore_GroupChat_DescriptionAndCover(t *testing.T, s chat.Store) {
	ctx := context.Background()

	// A group has neither unless one was set.
	creator := model.MustGenerateUserID()
	plain := putGroupChat(t, s, "Plain", at(100), creator)
	got, err := s.GetChatByID(ctx, plain.ID)
	require.NoError(t, err)
	require.Empty(t, got.Description)
	require.Nil(t, got.CoverPictureBlobID)

	// Both given at creation are on the canonical record, beside the profile
	// picture and apart from it.
	profile := &blobpb.BlobId{Value: []byte("picture-blob-0001")}
	cover := &blobpb.BlobId{Value: []byte("cover-blob-00001")}
	group := &chat.Chat{
		ID:                   chat.MustGenerateGroupChatID(),
		Type:                 chatpb.ChatType_GROUP,
		Members:              []*commonpb.UserId{creator},
		Title:                "Described",
		CreatorID:            creator,
		Description:          "Two\nlines, kept as written ",
		ProfilePictureBlobID: profile,
		CoverPictureBlobID:   cover,
		LastActivity:         at(100),
	}
	require.NoError(t, s.PutChat(ctx, group))
	got, err = s.GetChatByID(ctx, group.ID)
	require.NoError(t, err)
	require.Equal(t, "Two\nlines, kept as written ", got.Description)
	require.Equal(t, profile.Value, got.ProfilePictureBlobID.GetValue())
	require.Equal(t, cover.Value, got.CoverPictureBlobID.GetValue())

	// Each edits alone, leaving every other field as it was.
	description := "Edited"
	require.NoError(t, s.EditGroup(ctx, group.ID, chat.GroupEdit{Description: &description}))
	got, err = s.GetChatByID(ctx, group.ID)
	require.NoError(t, err)
	require.Equal(t, "Edited", got.Description)
	require.Equal(t, "Described", got.Title)
	require.Equal(t, profile.Value, got.ProfilePictureBlobID.GetValue())
	require.Equal(t, cover.Value, got.CoverPictureBlobID.GetValue())

	newCover := &blobpb.BlobId{Value: []byte("cover-blob-00002")}
	require.NoError(t, s.EditGroup(ctx, group.ID, chat.GroupEdit{CoverPictureBlobID: newCover}))
	got, err = s.GetChatByID(ctx, group.ID)
	require.NoError(t, err)
	require.Equal(t, "Edited", got.Description)
	require.Equal(t, profile.Value, got.ProfilePictureBlobID.GetValue())
	require.Equal(t, newCover.Value, got.CoverPictureBlobID.GetValue())

	// The empty description clears it, alone or beside a field it sets.
	cleared := ""
	require.NoError(t, s.EditGroup(ctx, group.ID, chat.GroupEdit{Description: &cleared}))
	got, err = s.GetChatByID(ctx, group.ID)
	require.NoError(t, err)
	require.Empty(t, got.Description)
	require.Equal(t, "Described", got.Title)
	require.Equal(t, newCover.Value, got.CoverPictureBlobID.GetValue())

	require.NoError(t, s.EditGroup(ctx, group.ID, chat.GroupEdit{Description: &description}))
	title := "Retitled"
	require.NoError(t, s.EditGroup(ctx, group.ID, chat.GroupEdit{Title: &title, Description: &cleared}))
	got, err = s.GetChatByID(ctx, group.ID)
	require.NoError(t, err)
	require.Empty(t, got.Description)
	require.Equal(t, "Retitled", got.Title)

	// Clearing one that was never set is harmless.
	require.NoError(t, s.EditGroup(ctx, plain.ID, chat.GroupEdit{Description: &cleared}))
	got, err = s.GetChatByID(ctx, plain.ID)
	require.NoError(t, err)
	require.Empty(t, got.Description)

	// Every field at once, on a group that had none of them.
	all := "All"
	require.NoError(t, s.EditGroup(ctx, plain.ID, chat.GroupEdit{
		Title:                &all,
		Description:          &description,
		ProfilePictureBlobID: profile,
		CoverPictureBlobID:   cover,
	}))
	got, err = s.GetChatByID(ctx, plain.ID)
	require.NoError(t, err)
	require.Equal(t, "All", got.Title)
	require.Equal(t, "Edited", got.Description)
	require.Equal(t, profile.Value, got.ProfilePictureBlobID.GetValue())
	require.Equal(t, cover.Value, got.CoverPictureBlobID.GetValue())

	// The edits survive the updates that touch the rest of the record, and
	// the profile picture's own write leaves the cover alone.
	advanced, _, err := s.AdvanceLastMessage(ctx, plain.ID, &messagingpb.MessageId{Value: 1}, at(200))
	require.NoError(t, err)
	require.True(t, advanced)
	require.NoError(t, s.SetGroupPicture(ctx, plain.ID, nil))
	got, err = s.GetChatByID(ctx, plain.ID)
	require.NoError(t, err)
	require.Equal(t, "Edited", got.Description)
	require.Nil(t, got.ProfilePictureBlobID)
	require.Equal(t, cover.Value, got.CoverPictureBlobID.GetValue())

	// A group that does not exist is refused, and no phantom is left behind,
	// even by an edit that only clears.
	unknown := chat.MustGenerateGroupChatID()
	require.ErrorIs(t, s.EditGroup(ctx, unknown, chat.GroupEdit{Description: &cleared}), chat.ErrChatNotFound)
	_, err = s.GetChatByID(ctx, unknown)
	require.ErrorIs(t, err, chat.ErrChatNotFound)
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

func testStore_GroupChat_MinimumSpeakerBalance(t *testing.T, s chat.Store) {
	ctx := context.Background()

	// A group has no minimum speaker balance unless it was written with one.
	plain := putGroupChat(t, s, "Weekend Trip", at(100), model.MustGenerateUserID())
	got, err := s.GetChatByID(ctx, plain.ID)
	require.NoError(t, err)
	require.Nil(t, got.MinimumSpeakerBalance)

	// The requirement round-trips as stored, independent of a listener
	// requirement beside it.
	usdfMint := &commonpb.PublicKey{Value: randomBytes(32)}
	creator := model.MustGenerateUserID()
	gated := &chat.Chat{
		ID:      chat.MustGenerateGroupChatID(),
		Type:    chatpb.ChatType_GROUP,
		Members: []*commonpb.UserId{creator},
		Title:   "Speakers",
		MinimumListenerBalance: &chat.MinimumBalance{
			Currency:     "usd",
			NativeAmount: 1,
		},
		IsCreatorOnlySpeaker: true,
		MinimumSpeakerBalance: &chat.MinimumBalance{
			Currency:     "eur",
			NativeAmount: 12.34,
			Mints:        []*commonpb.PublicKey{usdfMint},
		},
		CreatorID:    creator,
		LastActivity: at(100),
	}
	require.NoError(t, s.PutChat(ctx, gated))

	got, err = s.GetChatByID(ctx, gated.ID)
	require.NoError(t, err)
	require.Equal(t, gated.MinimumSpeakerBalance, got.MinimumSpeakerBalance)
	require.True(t, proto.Equal(gated.MinimumListenerBalance.ToProto(), got.MinimumListenerBalance.ToProto()))
	require.True(t, got.IsCreatorOnlySpeaker)

	// The stored record is independent of the one read.
	got.MinimumSpeakerBalance.Mints[0].Value[0]++
	again, err := s.GetChatByID(ctx, gated.ID)
	require.NoError(t, err)
	require.Equal(t, usdfMint.Value, again.MinimumSpeakerBalance.Mints[0].Value)

	// The rules read projects it as a speaker rule after the creator rule,
	// with the listener rule untouched, and matches the canonical record.
	rules, err := s.GetGroupRules(ctx, gated.ID)
	require.NoError(t, err)
	require.True(t, proto.Equal(gated.Rules(), rules.Rules))
	require.Len(t, rules.Rules.GetListener(), 1)
	require.True(t, proto.Equal(gated.MinimumListenerBalance.ToProto(), rules.Rules.GetListener()[0].GetMinimumBalance()))
	require.Len(t, rules.Rules.GetSpeaker(), 2)
	require.NotNil(t, rules.Rules.GetSpeaker()[0].GetCreator())
	require.True(t, proto.Equal(gated.MinimumSpeakerBalance.ToProto(), rules.Rules.GetSpeaker()[1].GetMinimumBalance()))
	require.Equal(t, creator.Value, rules.CreatorID.GetValue())
	require.True(t, proto.Equal(rules.Rules, again.Rules()))

	// A speaker requirement alone is a speaker rule and no listener rule; no
	// mints reads back as no mints.
	speakerOnly := &chat.Chat{
		ID:                    chat.MustGenerateGroupChatID(),
		Type:                  chatpb.ChatType_GROUP,
		Members:               []*commonpb.UserId{model.MustGenerateUserID()},
		MinimumSpeakerBalance: &chat.MinimumBalance{Currency: "usd", NativeAmount: 0.1 + 0.2},
		LastActivity:          at(100),
	}
	require.NoError(t, s.PutChat(ctx, speakerOnly))

	got, err = s.GetChatByID(ctx, speakerOnly.ID)
	require.NoError(t, err)
	require.Nil(t, got.MinimumListenerBalance)
	require.Equal(t, 0.1+0.2, got.MinimumSpeakerBalance.NativeAmount)
	require.Empty(t, got.MinimumSpeakerBalance.Mints)
	rules, err = s.GetGroupRules(ctx, speakerOnly.ID)
	require.NoError(t, err)
	require.Empty(t, rules.Rules.GetListener())
	require.Len(t, rules.Rules.GetSpeaker(), 1)
	require.NotNil(t, rules.Rules.GetSpeaker()[0].GetMinimumBalance())

	// The requirement is part of the canonical record and survives the updates
	// that touch it.
	advanced, _, err := s.AdvanceLastMessage(ctx, gated.ID, &messagingpb.MessageId{Value: 1}, at(200))
	require.NoError(t, err)
	require.True(t, advanced)
	require.NoError(t, s.SetGroupPicture(ctx, gated.ID, &blobpb.BlobId{Value: randomBytes(16)}))
	title := "Renamed"
	require.NoError(t, s.EditGroup(ctx, gated.ID, chat.GroupEdit{Title: &title}))

	got, err = s.GetChatByID(ctx, gated.ID)
	require.NoError(t, err)
	require.Equal(t, gated.MinimumSpeakerBalance, got.MinimumSpeakerBalance)
	require.Equal(t, "Renamed", got.Title)

	// A DM never carries one, whatever its record says.
	dm := &chat.Chat{
		ID:                    generateDmChatID(),
		Type:                  chatpb.ChatType_CONTACT_DM,
		Members:               []*commonpb.UserId{model.MustGenerateUserID(), model.MustGenerateUserID()},
		MinimumSpeakerBalance: gated.MinimumSpeakerBalance,
		LastActivity:          at(100),
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
	require.Nil(t, got.ProfilePictureBlobID)

	// A picture given at creation is on the canonical record.
	original := &blobpb.BlobId{Value: []byte("picture-blob-0001")}
	withPicture := &chat.Chat{
		ID:                   chat.MustGenerateGroupChatID(),
		Type:                 chatpb.ChatType_GROUP,
		Members:              []*commonpb.UserId{model.MustGenerateUserID()},
		Title:                "With Picture",
		ProfilePictureBlobID: original,
		LastActivity:         at(100),
	}
	require.NoError(t, s.PutChat(ctx, withPicture))
	got, err = s.GetChatByID(ctx, withPicture.ID)
	require.NoError(t, err)
	require.NotNil(t, got.ProfilePictureBlobID)
	require.Equal(t, original.Value, got.ProfilePictureBlobID.Value)

	// Setting a picture on a group that had none, and replacing one that did.
	first := &blobpb.BlobId{Value: []byte("picture-blob-0002")}
	require.NoError(t, s.SetGroupPicture(ctx, plain.ID, first))
	got, err = s.GetChatByID(ctx, plain.ID)
	require.NoError(t, err)
	require.Equal(t, first.Value, got.ProfilePictureBlobID.Value)

	second := &blobpb.BlobId{Value: []byte("picture-blob-0003")}
	require.NoError(t, s.SetGroupPicture(ctx, plain.ID, second))
	got, err = s.GetChatByID(ctx, plain.ID)
	require.NoError(t, err)
	require.Equal(t, second.Value, got.ProfilePictureBlobID.Value)

	// The picture is part of the canonical record and survives the updates that
	// touch it; the rest of the record survives the picture update.
	advanced, _, err := s.AdvanceLastMessage(ctx, plain.ID, &messagingpb.MessageId{Value: 1}, at(200))
	require.NoError(t, err)
	require.True(t, advanced)
	got, err = s.GetChatByID(ctx, plain.ID)
	require.NoError(t, err)
	require.Equal(t, second.Value, got.ProfilePictureBlobID.Value)
	require.Equal(t, "No Picture", got.Title)
	require.True(t, got.LastActivity.Equal(at(200)))

	// A nil picture clears it.
	require.NoError(t, s.SetGroupPicture(ctx, plain.ID, nil))
	got, err = s.GetChatByID(ctx, plain.ID)
	require.NoError(t, err)
	require.Nil(t, got.ProfilePictureBlobID)
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
	require.Nil(t, got.ProfilePictureBlobID)
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
		ProfilePictureBlobID:   original,
		LastActivity:           at(100),
	}
	require.NoError(t, s.PutChat(ctx, group))

	// The title alone: the picture and every other field are untouched.
	title := "After"
	require.NoError(t, s.EditGroup(ctx, group.ID, chat.GroupEdit{Title: &title}))
	got, err := s.GetChatByID(ctx, group.ID)
	require.NoError(t, err)
	require.Equal(t, "After", got.Title)
	require.Equal(t, original.Value, got.ProfilePictureBlobID.GetValue())
	require.True(t, got.IsStaffOnly)
	require.Equal(t, "usd", got.MinimumListenerBalance.Currency)
	require.Equal(t, float64(5), got.MinimumListenerBalance.NativeAmount)
	require.Equal(t, creator.Value, got.CreatorID.GetValue())
	require.True(t, got.LastActivity.Equal(at(100)))

	// The picture alone: the title stays edited.
	replacement := &blobpb.BlobId{Value: []byte("picture-blob-0002")}
	require.NoError(t, s.EditGroup(ctx, group.ID, chat.GroupEdit{ProfilePictureBlobID: replacement}))
	got, err = s.GetChatByID(ctx, group.ID)
	require.NoError(t, err)
	require.Equal(t, "After", got.Title)
	require.Equal(t, replacement.Value, got.ProfilePictureBlobID.GetValue())

	// Both at once, on a group that had no picture.
	plain := putGroupChat(t, s, "Plain", at(100), creator)
	both := "Plain, Edited"
	require.NoError(t, s.EditGroup(ctx, plain.ID, chat.GroupEdit{Title: &both, ProfilePictureBlobID: original}))
	got, err = s.GetChatByID(ctx, plain.ID)
	require.NoError(t, err)
	require.Equal(t, "Plain, Edited", got.Title)
	require.Equal(t, original.Value, got.ProfilePictureBlobID.GetValue())

	// The edit survives the updates that touch the rest of the record.
	advanced, _, err := s.AdvanceLastMessage(ctx, plain.ID, &messagingpb.MessageId{Value: 1}, at(200))
	require.NoError(t, err)
	require.True(t, advanced)
	got, err = s.GetChatByID(ctx, plain.ID)
	require.NoError(t, err)
	require.Equal(t, "Plain, Edited", got.Title)
	require.Equal(t, original.Value, got.ProfilePictureBlobID.GetValue())

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
	require.Nil(t, got.ProfilePictureBlobID)
}

func testStore_GroupChat_EditMinimumBalances(t *testing.T, s chat.Store) {
	ctx := context.Background()

	creator := model.MustGenerateUserID()
	mint := &commonpb.PublicKey{Value: randomBytes(32)}
	picture := &blobpb.BlobId{Value: []byte("picture-blob-0001")}
	group := &chat.Chat{
		ID:                     chat.MustGenerateGroupChatID(),
		Type:                   chatpb.ChatType_GROUP,
		Members:                []*commonpb.UserId{creator},
		Title:                  "Gated",
		IsStaffOnly:            true,
		MinimumListenerBalance: &chat.MinimumBalance{Currency: "usd", NativeAmount: 10, Mints: []*commonpb.PublicKey{mint}},
		IsCreatorOnlySpeaker:   true,
		MinimumSpeakerBalance:  &chat.MinimumBalance{Currency: "usd", NativeAmount: 20, Mints: []*commonpb.PublicKey{mint}},
		CreatorID:              creator,
		Description:            "Described",
		ProfilePictureBlobID:   picture,
		LastActivity:           at(100),
	}
	require.NoError(t, s.PutChat(ctx, group))

	// requireBalances checks the record and the rules read both carry
	// listener and speaker (nil for none), and that nothing else of the
	// record moved.
	requireBalances := func(listener, speaker *chat.MinimumBalance, title string) {
		t.Helper()
		got, err := s.GetChatByID(ctx, group.ID)
		require.NoError(t, err)
		require.True(t, proto.Equal(listener.ToProto(), got.MinimumListenerBalance.ToProto()))
		if speaker == nil {
			require.Nil(t, got.MinimumSpeakerBalance)
		} else {
			require.NotNil(t, got.MinimumSpeakerBalance)
			require.True(t, proto.Equal(speaker.ToProto(), got.MinimumSpeakerBalance.ToProto()))
		}
		require.True(t, got.IsStaffOnly)
		require.True(t, got.IsCreatorOnlySpeaker)
		require.Equal(t, creator.Value, got.CreatorID.GetValue())
		require.Equal(t, title, got.Title)
		require.Equal(t, "Described", got.Description)
		require.Equal(t, picture.Value, got.ProfilePictureBlobID.GetValue())
		require.True(t, got.LastActivity.Equal(at(100)))

		rules, err := s.GetGroupRules(ctx, group.ID)
		require.NoError(t, err)
		require.True(t, proto.Equal(got.Rules(), rules.Rules))
		require.Equal(t, creator.Value, rules.CreatorID.GetValue())
		require.False(t, rules.IsPrivate)
	}

	// Both amounts at once.
	listener := &chat.MinimumBalance{Currency: "usd", NativeAmount: 15, Mints: []*commonpb.PublicKey{mint}}
	speaker := &chat.MinimumBalance{Currency: "usd", NativeAmount: 0.1 + 0.2 + 30, Mints: []*commonpb.PublicKey{mint}}
	require.NoError(t, s.EditGroup(ctx, group.ID, chat.GroupEdit{
		MinimumBalances: &chat.GroupMinimumBalances{Listener: listener, Speaker: speaker},
	}))
	requireBalances(listener, speaker, "Gated")

	// No speaker balance removes the group's, leaving the creator rule.
	lowered := &chat.MinimumBalance{Currency: "usd", NativeAmount: 0.01, Mints: []*commonpb.PublicKey{mint}}
	require.NoError(t, s.EditGroup(ctx, group.ID, chat.GroupEdit{
		MinimumBalances: &chat.GroupMinimumBalances{Listener: lowered},
	}))
	requireBalances(lowered, nil, "Gated")
	rules, err := s.GetGroupRules(ctx, group.ID)
	require.NoError(t, err)
	require.Len(t, rules.Rules.GetSpeaker(), 1)
	require.NotNil(t, rules.Rules.GetSpeaker()[0].GetCreator())

	// Removing it again is harmless.
	require.NoError(t, s.EditGroup(ctx, group.ID, chat.GroupEdit{
		MinimumBalances: &chat.GroupMinimumBalances{Listener: lowered},
	}))
	requireBalances(lowered, nil, "Gated")

	// Adding one back, beside another field, in one write.
	title := "Regated"
	require.NoError(t, s.EditGroup(ctx, group.ID, chat.GroupEdit{
		Title:           &title,
		MinimumBalances: &chat.GroupMinimumBalances{Listener: listener, Speaker: speaker},
	}))
	requireBalances(listener, speaker, "Regated")

	// An edit of another field leaves the balances as they are.
	description := "Described"
	require.NoError(t, s.EditGroup(ctx, group.ID, chat.GroupEdit{Description: &description}))
	requireBalances(listener, speaker, "Regated")

	// A requirement on any mint reads back with none.
	anyMint := &chat.Chat{
		ID:                     chat.MustGenerateGroupChatID(),
		Type:                   chatpb.ChatType_GROUP,
		Members:                []*commonpb.UserId{creator},
		MinimumListenerBalance: &chat.MinimumBalance{Currency: "jpy", NativeAmount: 100},
		CreatorID:              creator,
		LastActivity:           at(100),
	}
	require.NoError(t, s.PutChat(ctx, anyMint))
	require.NoError(t, s.EditGroup(ctx, anyMint.ID, chat.GroupEdit{
		MinimumBalances: &chat.GroupMinimumBalances{
			Listener: &chat.MinimumBalance{Currency: "jpy", NativeAmount: 200},
			Speaker:  &chat.MinimumBalance{Currency: "jpy", NativeAmount: 201},
		},
	}))
	got, err := s.GetChatByID(ctx, anyMint.ID)
	require.NoError(t, err)
	require.Equal(t, float64(200), got.MinimumListenerBalance.NativeAmount)
	require.Empty(t, got.MinimumListenerBalance.Mints)
	require.Equal(t, float64(201), got.MinimumSpeakerBalance.NativeAmount)
	require.Empty(t, got.MinimumSpeakerBalance.Mints)

	balances := &chat.GroupMinimumBalances{Listener: &chat.MinimumBalance{Currency: "usd", NativeAmount: 1}}

	// A group that cannot carry them is refused, as something other than
	// missing, and left as it was, whatever else the edit names: one with no
	// listener balance, and a private one, even with one on its record.
	plain := putGroupChat(t, s, "Plain", at(100), creator)
	err = s.EditGroup(ctx, plain.ID, chat.GroupEdit{Title: &title, MinimumBalances: balances})
	require.Error(t, err)
	require.NotErrorIs(t, err, chat.ErrChatNotFound)
	got, err = s.GetChatByID(ctx, plain.ID)
	require.NoError(t, err)
	require.Equal(t, "Plain", got.Title)
	require.Nil(t, got.MinimumListenerBalance)
	require.Nil(t, got.MinimumSpeakerBalance)

	private := &chat.Chat{
		ID:                     chat.MustGenerateGroupChatID(),
		Type:                   chatpb.ChatType_GROUP,
		Members:                []*commonpb.UserId{creator},
		Title:                  "Private",
		MinimumListenerBalance: &chat.MinimumBalance{Currency: "usd", NativeAmount: 5},
		IsPrivate:              true,
		CreatorID:              creator,
		LastActivity:           at(100),
	}
	require.NoError(t, s.PutChat(ctx, private))
	err = s.EditGroup(ctx, private.ID, chat.GroupEdit{Title: &title, MinimumBalances: balances})
	require.Error(t, err)
	require.NotErrorIs(t, err, chat.ErrChatNotFound)
	got, err = s.GetChatByID(ctx, private.ID)
	require.NoError(t, err)
	require.Equal(t, "Private", got.Title)
	require.Equal(t, float64(5), got.MinimumListenerBalance.NativeAmount)

	// A group that does not exist is missing, and no phantom is left behind.
	unknown := chat.MustGenerateGroupChatID()
	require.ErrorIs(t, s.EditGroup(ctx, unknown, chat.GroupEdit{MinimumBalances: balances}), chat.ErrChatNotFound)
	_, err = s.GetChatByID(ctx, unknown)
	require.ErrorIs(t, err, chat.ErrChatNotFound)
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

// testStore_GroupChat_ChatsByID pins GetGroupChatsByID: the records of the
// groups that exist, whoever's they are, with no membership consulted.
func testStore_GroupChat_ChatsByID(t *testing.T, s chat.Store) {
	ctx := context.Background()

	userA := model.MustGenerateUserID()
	userB := model.MustGenerateUserID()

	groupA := putGroupChat(t, s, "Only A", at(1), userA)
	groupB := putGroupChat(t, s, "Only B", at(2), userB)
	dm := putDmChat(t, s, userA, userB, at(3))

	// No IDs is an empty result, not an error.
	chats, err := s.GetGroupChatsByID(ctx, nil)
	require.NoError(t, err)
	require.Empty(t, chats)

	// Every group that exists, member or not; a group that does not exist is
	// omitted, and a repeated ID collapses.
	missing := chat.MustGenerateGroupChatID()
	chats, err = s.GetGroupChatsByID(ctx, []*commonpb.ChatId{groupB.ID, missing, groupA.ID, groupB.ID})
	require.NoError(t, err)
	require.Len(t, chats, 2)
	require.NotContains(t, chats, string(missing.Value))
	for _, want := range []*chat.Chat{groupA, groupB} {
		got, ok := chats[string(want.ID.Value)]
		require.True(t, ok)
		require.Equal(t, want.ID.Value, got.ID.Value)
		require.Equal(t, want.Title, got.Title)
		require.Equal(t, chatpb.ChatType_GROUP, got.Type)
		require.Empty(t, got.Members)
		require.Equal(t, chat.RosterSummary{}, got.RosterSummary)
	}

	// A departed member changes nothing: the record is read whoever asks.
	removeGroupMember(t, s, groupA.ID, userA)
	chats, err = s.GetGroupChatsByID(ctx, []*commonpb.ChatId{groupA.ID})
	require.NoError(t, err)
	require.Contains(t, chats, string(groupA.ID.Value))

	// A DM ID is the wrong family: an error, not an omission.
	_, err = s.GetGroupChatsByID(ctx, []*commonpb.ChatId{groupA.ID, dm.ID})
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

func testStore_GroupChat_MembersByID(t *testing.T, s chat.Store) {
	ctx := context.Background()

	a, b, departed := model.MustGenerateUserID(), model.MustGenerateUserID(), model.MustGenerateUserID()
	stranger := model.MustGenerateUserID()
	groupID := putGroupChat(t, s, "members", at(0), a, b, departed).ID
	_, _, err := s.RemoveGroupMember(ctx, groupID, departed, false)
	require.NoError(t, err)
	added := model.MustGenerateUserID()
	_, roster, err := s.AddGroupMembers(ctx, groupID, []*commonpb.UserId{added})
	require.NoError(t, err)

	// Joined members only, with their records, keyed by user; duplicates
	// collapse.
	members, err := s.GetGroupMembersByID(ctx, groupID, []*commonpb.UserId{a, departed, stranger, added, a})
	require.NoError(t, err)
	require.Len(t, members, 2)
	require.Equal(t, a.Value, members[string(a.Value)].UserID.Value)
	require.Zero(t, members[string(a.Value)].Version)
	require.Equal(t, added.Value, members[string(added.Value)].UserID.Value)
	require.Equal(t, roster.Version, members[string(added.Value)].Version)
	require.False(t, members[string(added.Value)].JoinedAt.IsZero())

	// Nothing asked, nothing returned; a group that does not exist has no
	// members; groups only.
	members, err = s.GetGroupMembersByID(ctx, groupID, nil)
	require.NoError(t, err)
	require.Empty(t, members)
	members, err = s.GetGroupMembersByID(ctx, chat.MustGenerateGroupChatID(), []*commonpb.UserId{a})
	require.NoError(t, err)
	require.Empty(t, members)
	_, err = s.GetGroupMembersByID(ctx, generateDmChatID(), []*commonpb.UserId{a})
	require.Error(t, err)

	// A batch larger than one read is read whole.
	many := make([]*commonpb.UserId, 0, 120)
	for range 120 {
		many = append(many, model.MustGenerateUserID())
	}
	_, _, err = s.AddGroupMembers(ctx, groupID, many)
	require.NoError(t, err)
	members, err = s.GetGroupMembersByID(ctx, groupID, append(many, b))
	require.NoError(t, err)
	require.Len(t, members, 121)
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

	// A point read finds one user's record, whatever its rank; a user with
	// no record, or a group that does not exist, has none; groups only.
	lastSentAt, ok, err := s.GetLastSentAt(ctx, group.ID, c)
	require.NoError(t, err)
	require.True(t, ok)
	require.True(t, lastSentAt.Equal(at(200)))
	_, ok, err = s.GetLastSentAt(ctx, group.ID, model.MustGenerateUserID())
	require.NoError(t, err)
	require.False(t, ok)
	_, ok, err = s.GetLastSentAt(ctx, chat.MustGenerateGroupChatID(), a)
	require.NoError(t, err)
	require.False(t, ok)
	_, _, err = s.GetLastSentAt(ctx, generateDmChatID(), a)
	require.Error(t, err)
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
	require.True(t, senders[0].ActivityScore.Equal(at(70)), "scored once")
}

func testStore_Activity_Score(t *testing.T, s chat.Store) {
	ctx := context.Background()

	groupID := chat.MustGenerateGroupChatID()
	user := model.MustGenerateUserID()
	interval := chat.ActivityRecordInterval

	score := func() time.Time {
		t.Helper()
		senders, err := s.GetRecentSenders(ctx, groupID, 0)
		require.NoError(t, err)
		require.Len(t, senders, 1)
		return senders[0].ActivityScore
	}

	// A first send scores its own time.
	recorded, err := s.RecordSend(ctx, groupID, user, at(100))
	require.NoError(t, err)
	require.True(t, recorded)
	require.True(t, score().Equal(at(100)))

	// Each recorded send moves the score once, by the formula, against the
	// send before it.
	want, last := at(100), at(100)
	for _, sentAt := range []time.Time{
		at(100).Add(interval),
		at(100).Add(3 * interval),
		at(100).Add(24 * time.Hour),
	} {
		recorded, err = s.RecordSend(ctx, groupID, user, sentAt)
		require.NoError(t, err)
		require.True(t, recorded)
		want, last = chat.NextActivityScore(want, last, sentAt), sentAt
		require.True(t, score().Equal(want), "after send at %v: got %v, want %v", sentAt, score(), want)
	}
	require.True(t, want.After(at(100).Add(24*time.Hour)), "the score runs ahead of the last send")

	// A throttled send moves nothing.
	recorded, err = s.RecordSend(ctx, groupID, user, at(100).Add(24*time.Hour).Add(interval/2))
	require.NoError(t, err)
	require.False(t, recorded)
	require.True(t, score().Equal(want))

	// A frequent sender outranks a one-off sender who sent after them.
	other := model.MustGenerateUserID()
	recorded, err = s.RecordSend(ctx, groupID, other, at(100).Add(25*time.Hour))
	require.NoError(t, err)
	require.True(t, recorded)
	senders, err := s.GetRecentSenders(ctx, groupID, 0)
	require.NoError(t, err)
	requireRecentSenders(t, senders, other, at(100).Add(25*time.Hour), user, at(100).Add(24*time.Hour))
	require.True(t, senders[1].ActivityScore.After(senders[0].ActivityScore))
}

func testStore_Activity_ScoreConcurrent(t *testing.T, s chat.Store) {
	ctx := context.Background()

	groupID := chat.MustGenerateGroupChatID()
	user := model.MustGenerateUserID()

	// Sends far enough apart that none throttles another in order, recorded
	// all at once by many writers, as several servers would. Whichever are
	// recorded must have been recorded in time order, each scored once
	// against the one before, so the score is exactly theirs: no lost update,
	// no double count. A store may leave a send that loses its race twice
	// unrecorded, so which ones are recorded is not fixed.
	const writers = 4
	sendTimes := make([]time.Time, writers)
	for i := range writers {
		sendTimes[i] = at(1000).Add(time.Duration(i) * time.Hour)
	}
	var wg sync.WaitGroup
	results := make([]bool, writers)
	errs := make([]error, writers)
	for i := range writers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			results[i], errs[i] = s.RecordSend(ctx, groupID, user, sendTimes[i])
		}()
	}
	wg.Wait()

	var want, last time.Time
	for i := range writers {
		require.NoError(t, errs[i])
		if results[i] {
			want = chat.NextActivityScore(want, last, sendTimes[i])
			last = sendTimes[i]
		}
	}
	require.False(t, last.IsZero())
	senders, err := s.GetRecentSenders(ctx, groupID, 0)
	require.NoError(t, err)
	requireRecentSenders(t, senders, user, last)
	require.True(t, senders[0].ActivityScore.Equal(want), "got %v, want %v", senders[0].ActivityScore, want)

	// Of two such writers, the later send is always recorded, whichever
	// writes first: it loses at most once, to the earlier. (Against a store
	// whose first read may be stale it could lose once to that as well; the
	// stores under test read current data.)
	for range 5 {
		groupID := chat.MustGenerateGroupChatID()
		earlier, later := at(2000), at(2000).Add(time.Hour)
		var wg sync.WaitGroup
		var errEarlier, errLater error
		var recordedLater bool
		wg.Add(2)
		go func() {
			defer wg.Done()
			_, errEarlier = s.RecordSend(ctx, groupID, user, earlier)
		}()
		go func() {
			defer wg.Done()
			recordedLater, errLater = s.RecordSend(ctx, groupID, user, later)
		}()
		wg.Wait()
		require.NoError(t, errEarlier)
		require.NoError(t, errLater)
		require.True(t, recordedLater)
		senders, err := s.GetRecentSenders(ctx, groupID, 0)
		require.NoError(t, err)
		requireRecentSenders(t, senders, user, later)
	}
}

func testStore_Activity_ActiveSenders(t *testing.T, s chat.Store) {
	ctx := context.Background()

	groupID := chat.MustGenerateGroupChatID()
	regular, oneOff, quiet := model.MustGenerateUserID(), model.MustGenerateUserID(), model.MustGenerateUserID()
	day := 24 * time.Hour

	senders, err := s.GetActiveSenders(ctx, groupID, 0)
	require.NoError(t, err)
	require.Empty(t, senders)

	// A regular who sent daily for a week, a user who sent once after them,
	// and one who sent once before.
	recorded, err := s.RecordSend(ctx, groupID, quiet, at(0))
	require.NoError(t, err)
	require.True(t, recorded)
	for d := range 7 {
		recorded, err := s.RecordSend(ctx, groupID, regular, at(0).Add(time.Duration(d+1)*day))
		require.NoError(t, err)
		require.True(t, recorded)
	}
	recorded, err = s.RecordSend(ctx, groupID, oneOff, at(0).Add(8*day))
	require.NoError(t, err)
	require.True(t, recorded)

	// By recency the one-off sender leads; by activity the regular does.
	senders, err = s.GetRecentSenders(ctx, groupID, 0)
	require.NoError(t, err)
	requireRecentSenders(t, senders, oneOff, at(0).Add(8*day), regular, at(0).Add(7*day), quiet, at(0))
	senders, err = s.GetActiveSenders(ctx, groupID, 0)
	require.NoError(t, err)
	requireRecentSenders(t, senders, regular, at(0).Add(7*day), oneOff, at(0).Add(8*day), quiet, at(0))
	require.True(t, senders[0].ActivityScore.After(senders[1].ActivityScore))
	require.True(t, senders[1].ActivityScore.Equal(at(0).Add(8*day)))

	// A limit keeps the most active.
	senders, err = s.GetActiveSenders(ctx, groupID, 2)
	require.NoError(t, err)
	requireRecentSenders(t, senders, regular, at(0).Add(7*day), oneOff, at(0).Add(8*day))

	// Records belong to their group; groups only.
	senders, err = s.GetActiveSenders(ctx, chat.MustGenerateGroupChatID(), 0)
	require.NoError(t, err)
	require.Empty(t, senders)
	_, err = s.GetActiveSenders(ctx, generateDmChatID(), 0)
	require.Error(t, err)
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

func generateGroupChatIDs(n int) []*commonpb.ChatId {
	chatIDs := make([]*commonpb.ChatId, n)
	for i := range chatIDs {
		chatIDs[i] = chat.MustGenerateGroupChatID()
	}
	return chatIDs
}

func chatIDBytes(chatIDs []*commonpb.ChatId) [][]byte {
	out := make([][]byte, len(chatIDs))
	for i, chatID := range chatIDs {
		out[i] = chatID.Value
	}
	return out
}

func testStore_FeaturedGroups_SetAndGet(t *testing.T, s chat.Store) {
	ctx := context.Background()
	user := model.MustGenerateUserID()
	other := model.MustGenerateUserID()

	// Never set: none, at version zero.
	got, err := s.GetFeaturedGroups(ctx, user)
	require.NoError(t, err)
	require.Empty(t, got.ChatIDs)
	require.Zero(t, got.Version)

	// A full list, in the caller's order.
	full := generateGroupChatIDs(chat.MaxFeaturedGroups)
	set, changed, err := s.SetFeaturedGroups(ctx, user, full)
	require.NoError(t, err)
	require.True(t, changed)
	require.Equal(t, chatIDBytes(full), chatIDBytes(set.ChatIDs))
	require.EqualValues(t, 1, set.Version)
	got, err = s.GetFeaturedGroups(ctx, user)
	require.NoError(t, err)
	require.Equal(t, set, got)

	// Shorter: the positions past it go.
	shorter := []*commonpb.ChatId{full[3], full[0], full[7]}
	set, changed, err = s.SetFeaturedGroups(ctx, user, shorter)
	require.NoError(t, err)
	require.True(t, changed)
	require.EqualValues(t, 2, set.Version)
	got, err = s.GetFeaturedGroups(ctx, user)
	require.NoError(t, err)
	require.Equal(t, chatIDBytes(shorter), chatIDBytes(got.ChatIDs))
	require.EqualValues(t, 2, got.Version)

	// A reorder alone is a change.
	reordered := []*commonpb.ChatId{full[0], full[7], full[3]}
	set, changed, err = s.SetFeaturedGroups(ctx, user, reordered)
	require.NoError(t, err)
	require.True(t, changed)
	require.EqualValues(t, 3, set.Version)

	// Longer again, with groups never seen before.
	longer := append(slices.Clone(reordered), generateGroupChatIDs(4)...)
	_, changed, err = s.SetFeaturedGroups(ctx, user, longer)
	require.NoError(t, err)
	require.True(t, changed)
	got, err = s.GetFeaturedGroups(ctx, user)
	require.NoError(t, err)
	require.Equal(t, chatIDBytes(longer), chatIDBytes(got.ChatIDs))
	require.EqualValues(t, 4, got.Version)

	// Another user's list is their own.
	got, err = s.GetFeaturedGroups(ctx, other)
	require.NoError(t, err)
	require.Empty(t, got.ChatIDs)
	require.Zero(t, got.Version)

	// Clearing is a write like any other, and leaves the version.
	set, changed, err = s.SetFeaturedGroups(ctx, user, nil)
	require.NoError(t, err)
	require.True(t, changed)
	require.Empty(t, set.ChatIDs)
	require.EqualValues(t, 5, set.Version)
	got, err = s.GetFeaturedGroups(ctx, user)
	require.NoError(t, err)
	require.Empty(t, got.ChatIDs)
	require.EqualValues(t, 5, got.Version)

	// Set again after a clear.
	_, changed, err = s.SetFeaturedGroups(ctx, user, shorter)
	require.NoError(t, err)
	require.True(t, changed)
	got, err = s.GetFeaturedGroups(ctx, user)
	require.NoError(t, err)
	require.Equal(t, chatIDBytes(shorter), chatIDBytes(got.ChatIDs))
	require.EqualValues(t, 6, got.Version)

	// The list returned shares no memory with the stored one.
	got.ChatIDs[0].Value[0] ^= 0xff
	again, err := s.GetFeaturedGroups(ctx, user)
	require.NoError(t, err)
	require.Equal(t, chatIDBytes(shorter), chatIDBytes(again.ChatIDs))
}

func testStore_FeaturedGroups_NoOp(t *testing.T, s chat.Store) {
	ctx := context.Background()
	user := model.MustGenerateUserID()

	// Clearing a list never set writes nothing: still version zero.
	set, changed, err := s.SetFeaturedGroups(ctx, user, nil)
	require.NoError(t, err)
	require.False(t, changed)
	require.Zero(t, set.Version)

	list := generateGroupChatIDs(3)
	_, changed, err = s.SetFeaturedGroups(ctx, user, list)
	require.NoError(t, err)
	require.True(t, changed)

	// The same list, as fresh values, is the no-op, returning what stands.
	same := make([]*commonpb.ChatId, len(list))
	for i, chatID := range list {
		same[i] = &commonpb.ChatId{Value: slices.Clone(chatID.Value)}
	}
	set, changed, err = s.SetFeaturedGroups(ctx, user, same)
	require.NoError(t, err)
	require.False(t, changed)
	require.Equal(t, chatIDBytes(list), chatIDBytes(set.ChatIDs))
	require.EqualValues(t, 1, set.Version)

	got, err := s.GetFeaturedGroups(ctx, user)
	require.NoError(t, err)
	require.EqualValues(t, 1, got.Version)

	// So is clearing twice.
	_, changed, err = s.SetFeaturedGroups(ctx, user, []*commonpb.ChatId{})
	require.NoError(t, err)
	require.True(t, changed)
	set, changed, err = s.SetFeaturedGroups(ctx, user, nil)
	require.NoError(t, err)
	require.False(t, changed)
	require.EqualValues(t, 2, set.Version)
}

func testStore_FeaturedGroups_Invalid(t *testing.T, s chat.Store) {
	ctx := context.Background()
	user := model.MustGenerateUserID()

	stored := generateGroupChatIDs(2)
	_, _, err := s.SetFeaturedGroups(ctx, user, stored)
	require.NoError(t, err)

	group := chat.MustGenerateGroupChatID()
	for name, chatIDs := range map[string][]*commonpb.ChatId{
		"too many": generateGroupChatIDs(chat.MaxFeaturedGroups + 1),
		"dm":       {group, generateDmChatID()},
		"repeated": {group, chat.MustGenerateGroupChatID(), {Value: slices.Clone(group.Value)}},
		"nil id":   {group, nil},
	} {
		_, _, err := s.SetFeaturedGroups(ctx, user, chatIDs)
		require.Error(t, err, name)
	}

	// Nothing refused was written.
	got, err := s.GetFeaturedGroups(ctx, user)
	require.NoError(t, err)
	require.Equal(t, chatIDBytes(stored), chatIDBytes(got.ChatIDs))
	require.EqualValues(t, 1, got.Version)
}

// testStore_FeaturedGroups_Concurrent checks that concurrent replaces each land
// whole: every one succeeds, the list ends as exactly one of them at a version
// counting them all, and a read alongside them never sees a mix of two.
func testStore_FeaturedGroups_Concurrent(t *testing.T, s chat.Store) {
	ctx := context.Background()
	user := model.MustGenerateUserID()

	// Different lengths, so a mix would show as a list of the wrong length as
	// well as the wrong groups.
	lengths := []int{1, chat.MaxFeaturedGroups, 3, 7, 2}
	lists := make([][]*commonpb.ChatId, len(lengths))
	for i, n := range lengths {
		lists[i] = generateGroupChatIDs(n)
	}
	isWhole := func(chatIDs []*commonpb.ChatId) bool {
		if len(chatIDs) == 0 {
			return true
		}
		for _, list := range lists {
			if slices.EqualFunc(list, chatIDs, func(a, b *commonpb.ChatId) bool { return bytes.Equal(a.Value, b.Value) }) {
				return true
			}
		}
		return false
	}

	done := make(chan struct{})
	var readErr error
	var mixed []*commonpb.ChatId
	var readers sync.WaitGroup
	readers.Add(1)
	go func() {
		defer readers.Done()
		for {
			select {
			case <-done:
				return
			default:
			}
			got, err := s.GetFeaturedGroups(ctx, user)
			if err != nil {
				readErr = err
				return
			}
			if !isWhole(got.ChatIDs) {
				mixed = got.ChatIDs
				return
			}
		}
	}()

	results := make([]error, len(lists))
	var writers sync.WaitGroup
	for i, list := range lists {
		writers.Add(1)
		go func() {
			defer writers.Done()
			_, _, results[i] = s.SetFeaturedGroups(ctx, user, list)
		}()
	}
	writers.Wait()
	close(done)
	readers.Wait()

	for _, err := range results {
		require.NoError(t, err)
	}
	require.NoError(t, readErr)
	require.Nil(t, mixed, "a read saw a mix of two lists")

	got, err := s.GetFeaturedGroups(ctx, user)
	require.NoError(t, err)
	require.NotEmpty(t, got.ChatIDs)
	require.True(t, isWhole(got.ChatIDs))
	require.EqualValues(t, len(lists), got.Version)
}

// holdingsAt returns a snapshot of chatID's holdings of mint, taken at the
// given offset in seconds from at's epoch.
func holdingsAt(chatID *commonpb.ChatId, mint *commonpb.PublicKey, quarks, holders uint64, seconds int64) chat.GroupHoldings {
	return chat.GroupHoldings{ChatID: chatID, Mint: mint, Quarks: quarks, HolderCount: holders, AsOf: at(seconds)}
}

// requireHoldingsEqual checks that got is want, comparing as_of as an
// instant.
func requireHoldingsEqual(t *testing.T, want, got chat.GroupHoldings) {
	t.Helper()
	require.Equal(t, want.ChatID.Value, got.ChatID.Value)
	require.Equal(t, want.Mint.Value, got.Mint.Value)
	require.Equal(t, want.Quarks, got.Quarks)
	require.Equal(t, want.HolderCount, got.HolderCount)
	require.True(t, want.AsOf.Equal(got.AsOf), "as_of %v, want %v", got.AsOf, want.AsOf)
}

func testStore_Holdings_PutAndGet(t *testing.T, s chat.Store) {
	ctx := context.Background()
	mint := model.MustGenerateKeyPair().Proto()
	ids := generateGroupChatIDs(3)

	// Nothing put: nothing read.
	got, err := s.GetGroupHoldings(ctx, ids)
	require.NoError(t, err)
	require.Empty(t, got)
	got, err = s.GetGroupHoldings(ctx, nil)
	require.NoError(t, err)
	require.Empty(t, got)

	first := holdingsAt(ids[0], mint, 1_000, chat.MinGroupHoldingsHolders, 10)
	changed, err := s.PutGroupHoldings(ctx, first)
	require.NoError(t, err)
	require.True(t, changed)
	second := holdingsAt(ids[1], mint, 300, 12, 10)
	changed, err = s.PutGroupHoldings(ctx, second)
	require.NoError(t, err)
	require.True(t, changed)

	// Groups with none are absent, and duplicates collapse.
	got, err = s.GetGroupHoldings(ctx, []*commonpb.ChatId{ids[0], ids[1], ids[2], ids[0]})
	require.NoError(t, err)
	require.Len(t, got, 2)
	requireHoldingsEqual(t, first, got[string(ids[0].Value)])
	requireHoldingsEqual(t, second, got[string(ids[1].Value)])

	// A newer snapshot replaces the stored one.
	refreshed := holdingsAt(ids[0], mint, 5_000, 15, 20)
	changed, err = s.PutGroupHoldings(ctx, refreshed)
	require.NoError(t, err)
	require.True(t, changed)
	got, err = s.GetGroupHoldings(ctx, []*commonpb.ChatId{ids[0]})
	require.NoError(t, err)
	requireHoldingsEqual(t, refreshed, got[string(ids[0].Value)])

	// A DM ID is refused.
	_, err = s.GetGroupHoldings(ctx, []*commonpb.ChatId{generateDmChatID()})
	require.Error(t, err)
}

func testStore_Holdings_NeverBackwards(t *testing.T, s chat.Store) {
	ctx := context.Background()
	mint := model.MustGenerateKeyPair().Proto()
	chatID := chat.MustGenerateGroupChatID()

	current := holdingsAt(chatID, mint, 1_000, 12, 20)
	changed, err := s.PutGroupHoldings(ctx, current)
	require.NoError(t, err)
	require.True(t, changed)

	// An older snapshot, and one taken at the same instant, are no-ops.
	for _, stale := range []chat.GroupHoldings{
		holdingsAt(chatID, mint, 9_000, 19, 10),
		holdingsAt(chatID, mint, 9_000, 19, 20),
	} {
		changed, err = s.PutGroupHoldings(ctx, stale)
		require.NoError(t, err)
		require.False(t, changed)
	}
	got, err := s.GetGroupHoldings(ctx, []*commonpb.ChatId{chatID})
	require.NoError(t, err)
	requireHoldingsEqual(t, current, got[string(chatID.Value)])

	// Nor does a stale snapshot move the ranking.
	top, err := s.GetTopGroupsByHoldings(ctx, mint, 0)
	require.NoError(t, err)
	require.Len(t, top, 1)
	requireHoldingsEqual(t, current, top[0])
}

func testStore_Holdings_Invalid(t *testing.T, s chat.Store) {
	ctx := context.Background()
	mint := model.MustGenerateKeyPair().Proto()
	chatID := chat.MustGenerateGroupChatID()

	for name, h := range map[string]chat.GroupHoldings{
		"dm":                 holdingsAt(generateDmChatID(), mint, 100, 10, 1),
		"no mint":            holdingsAt(chatID, nil, 100, 10, 1),
		"short mint":         holdingsAt(chatID, &commonpb.PublicKey{Value: []byte{1}}, 100, 10, 1),
		"no holders":         holdingsAt(chatID, mint, 100, 0, 1),
		"holders of nothing": holdingsAt(chatID, mint, 0, 10, 1),
		"no as_of":           {ChatID: chatID, Mint: mint, Quarks: 100, HolderCount: 10},
	} {
		_, err := s.PutGroupHoldings(ctx, h)
		require.ErrorIs(t, err, chat.ErrInvalidGroupHoldings, name)
	}
	got, err := s.GetGroupHoldings(ctx, []*commonpb.ChatId{chatID})
	require.NoError(t, err)
	require.Empty(t, got)
}

func testStore_Holdings_TopByMint(t *testing.T, s chat.Store) {
	ctx := context.Background()
	mint := model.MustGenerateKeyPair().Proto()
	other := model.MustGenerateKeyPair().Proto()

	// Nothing names the mint: an empty ranking.
	top, err := s.GetTopGroupsByHoldings(ctx, mint, 10)
	require.NoError(t, err)
	require.Empty(t, top)

	// Five groups of the mint, two of them tied, and one of another mint
	// holding more than any.
	ids := generateGroupChatIDs(5)
	quarks := []uint64{300, 100, 500, 100, 50}
	var snapshots []chat.GroupHoldings
	for i, id := range ids {
		h := holdingsAt(id, mint, quarks[i], chat.MinGroupHoldingsHolders, 10)
		_, err := s.PutGroupHoldings(ctx, h)
		require.NoError(t, err)
		snapshots = append(snapshots, h)
	}
	_, err = s.PutGroupHoldings(ctx, holdingsAt(chat.MustGenerateGroupChatID(), other, 9_000, 10, 10))
	require.NoError(t, err)

	want := slices.Clone(snapshots)
	slices.SortFunc(want, func(a, b chat.GroupHoldings) int {
		if chat.HoldingsLess(a, b) {
			return -1
		}
		return 1
	})
	requireRanking := func(want []chat.GroupHoldings, got []chat.GroupHoldings) {
		t.Helper()
		require.Len(t, got, len(want))
		for i := range want {
			requireHoldingsEqual(t, want[i], got[i])
		}
	}

	// The whole ranking, ties to the greater chat ID.
	top, err = s.GetTopGroupsByHoldings(ctx, mint, 0)
	require.NoError(t, err)
	requireRanking(want, top)
	require.EqualValues(t, 500, top[0].Quarks)
	require.EqualValues(t, 50, top[4].Quarks)
	require.Positive(t, bytes.Compare(top[2].ChatID.Value, top[3].ChatID.Value))

	// A limit takes the head.
	top, err = s.GetTopGroupsByHoldings(ctx, mint, 2)
	require.NoError(t, err)
	requireRanking(want[:2], top)

	// A refresh moves a group: the last rises to the top, once.
	risen := holdingsAt(want[4].ChatID, mint, 1_000, 14, 20)
	changed, err := s.PutGroupHoldings(ctx, risen)
	require.NoError(t, err)
	require.True(t, changed)
	top, err = s.GetTopGroupsByHoldings(ctx, mint, 0)
	require.NoError(t, err)
	requireRanking(append([]chat.GroupHoldings{risen}, want[:4]...), top)
}

// gatedGroup returns a public group whose minimum listener balance names
// mint, for the store's queue tests.
func gatedGroup(mint *commonpb.PublicKey) *chat.Chat {
	return &chat.Chat{
		ID:      chat.MustGenerateGroupChatID(),
		Type:    chatpb.ChatType_GROUP,
		Members: []*commonpb.UserId{model.MustGenerateUserID()},
		MinimumListenerBalance: &chat.MinimumBalance{
			Currency:     "usd",
			NativeAmount: 1,
			Mints:        []*commonpb.PublicKey{mint},
		},
		LastActivity: at(0),
	}
}

// dueChatIDs returns the chat IDs of the refreshes due as of asOf, in queue
// order.
func dueChatIDs(t *testing.T, s chat.Store, asOf time.Time) [][]byte {
	t.Helper()
	due, err := s.GetDueHoldingsRefreshes(context.Background(), asOf, 0)
	require.NoError(t, err)
	out := make([][]byte, len(due))
	for i, r := range due {
		out[i] = r.ChatID.Value
	}
	return out
}

func testStore_HoldingsQueue_EnqueuedAtCreation(t *testing.T, s chat.Store) {
	ctx := context.Background()
	mint := model.MustGenerateKeyPair().Proto()

	before := time.Now()
	gated := gatedGroup(mint)
	require.NoError(t, s.PutChat(ctx, gated))

	// Not gated: no requirement, a private group, and a DM.
	putGroupChat(t, s, "Ungated", at(0), model.MustGenerateUserID())
	private := gatedGroup(mint)
	private.IsPrivate = true
	require.NoError(t, s.PutChat(ctx, private))
	putDmChat(t, s, model.MustGenerateUserID(), model.MustGenerateUserID(), at(0))

	// Due at once, with its mint and no failed attempts.
	due, err := s.GetDueHoldingsRefreshes(ctx, time.Now(), 0)
	require.NoError(t, err)
	require.Len(t, due, 1)
	require.Equal(t, gated.ID.Value, due[0].ChatID.Value)
	require.Equal(t, mint.Value, due[0].Mint.Value)
	require.Zero(t, due[0].Attempts)
	require.False(t, due[0].DueAt.Before(before.Truncate(time.Millisecond)))

	// A backfill of a group already queued leaves its entry alone.
	changed, err := s.EnqueueHoldingsRefresh(ctx, gated.ID, mint, at(-1_000_000))
	require.NoError(t, err)
	require.False(t, changed)
	again, err := s.GetDueHoldingsRefreshes(ctx, time.Now(), 0)
	require.NoError(t, err)
	require.Len(t, again, 1)
	require.True(t, due[0].DueAt.Equal(again[0].DueAt))
}

func testStore_HoldingsQueue_ClaimAndReschedule(t *testing.T, s chat.Store) {
	ctx := context.Background()
	mint := model.MustGenerateKeyPair().Proto()
	ids := generateGroupChatIDs(3)

	// A backfill enqueues each group once, due when it says.
	for i, id := range ids {
		changed, err := s.EnqueueHoldingsRefresh(ctx, id, mint, at(int64(10*(i+1))))
		require.NoError(t, err)
		require.True(t, changed)
	}
	_, err := s.EnqueueHoldingsRefresh(ctx, generateDmChatID(), mint, at(0))
	require.Error(t, err)
	_, err = s.EnqueueHoldingsRefresh(ctx, chat.MustGenerateGroupChatID(), &commonpb.PublicKey{Value: []byte{1}}, at(0))
	require.Error(t, err)

	// Due ones only, soonest first, up to a limit.
	require.Empty(t, dueChatIDs(t, s, at(5)))
	require.Equal(t, [][]byte{ids[0].Value, ids[1].Value}, dueChatIDs(t, s, at(20)))
	limited, err := s.GetDueHoldingsRefreshes(ctx, at(100), 2)
	require.NoError(t, err)
	require.Len(t, limited, 2)

	// A claim pushes the due time out, once; a group not yet due, or not
	// queued, cannot be claimed.
	claimed, err := s.ClaimHoldingsRefresh(ctx, ids[0], at(20), at(1_000))
	require.NoError(t, err)
	require.True(t, claimed)
	claimed, err = s.ClaimHoldingsRefresh(ctx, ids[0], at(20), at(1_000))
	require.NoError(t, err)
	require.False(t, claimed)
	claimed, err = s.ClaimHoldingsRefresh(ctx, ids[2], at(20), at(1_000))
	require.NoError(t, err)
	require.False(t, claimed)
	claimed, err = s.ClaimHoldingsRefresh(ctx, chat.MustGenerateGroupChatID(), at(20), at(1_000))
	require.NoError(t, err)
	require.False(t, claimed)
	require.Equal(t, [][]byte{ids[1].Value}, dueChatIDs(t, s, at(20)))

	// A reschedule sets the due time and the attempts.
	require.NoError(t, s.RescheduleHoldingsRefresh(ctx, ids[0], at(15), 3))
	due, err := s.GetDueHoldingsRefreshes(ctx, at(20), 0)
	require.NoError(t, err)
	require.Len(t, due, 2)
	require.Equal(t, ids[0].Value, due[0].ChatID.Value)
	require.True(t, at(15).Equal(due[0].DueAt))
	require.EqualValues(t, 3, due[0].Attempts)
	require.NoError(t, s.RescheduleHoldingsRefresh(ctx, ids[0], at(15), 0))
	due, err = s.GetDueHoldingsRefreshes(ctx, at(20), 1)
	require.NoError(t, err)
	require.Zero(t, due[0].Attempts)

	// Rescheduling a group that is not queued enqueues nothing.
	unqueued := chat.MustGenerateGroupChatID()
	require.NoError(t, s.RescheduleHoldingsRefresh(ctx, unqueued, at(0), 0))
	require.NotContains(t, dueChatIDs(t, s, at(1_000_000)), unqueued.Value)
}

func testStore_HoldingsQueue_EntryWithoutSnapshot(t *testing.T, s chat.Store) {
	ctx := context.Background()
	mint := model.MustGenerateKeyPair().Proto()
	chatID := chat.MustGenerateGroupChatID()

	_, err := s.EnqueueHoldingsRefresh(ctx, chatID, mint, at(0))
	require.NoError(t, err)

	// Queued but never refreshed: no snapshot, and not ranked.
	got, err := s.GetGroupHoldings(ctx, []*commonpb.ChatId{chatID})
	require.NoError(t, err)
	require.Empty(t, got)
	top, err := s.GetTopGroupsByHoldings(ctx, mint, 0)
	require.NoError(t, err)
	require.Empty(t, top)

	// The first snapshot lands on the entry, which stays queued.
	h := holdingsAt(chatID, mint, 700, 12, 10)
	changed, err := s.PutGroupHoldings(ctx, h)
	require.NoError(t, err)
	require.True(t, changed)
	got, err = s.GetGroupHoldings(ctx, []*commonpb.ChatId{chatID})
	require.NoError(t, err)
	requireHoldingsEqual(t, h, got[string(chatID.Value)])
	top, err = s.GetTopGroupsByHoldings(ctx, mint, 0)
	require.NoError(t, err)
	require.Len(t, top, 1)
	require.Equal(t, [][]byte{chatID.Value}, dueChatIDs(t, s, at(1)))
}

func testStore_Holdings_Unlisted(t *testing.T, s chat.Store) {
	ctx := context.Background()
	mint := model.MustGenerateKeyPair().Proto()
	chatID := chat.MustGenerateGroupChatID()
	listedGroup := chat.MustGenerateGroupChatID()

	requireStored := func(want chat.GroupHoldings) {
		t.Helper()
		got, err := s.GetGroupHoldings(ctx, []*commonpb.ChatId{chatID})
		require.NoError(t, err)
		requireHoldingsEqual(t, want, got[string(chatID.Value)])
	}
	requireRanked := func(want ...*commonpb.ChatId) {
		t.Helper()
		top, err := s.GetTopGroupsByHoldings(ctx, mint, 0)
		require.NoError(t, err)
		got := make([][]byte, len(top))
		for i, h := range top {
			got[i] = h.ChatID.Value
		}
		require.Equal(t, chatIDBytes(want), got)
	}

	// A listed group of the mint beside the one under test, queued.
	_, err := s.PutGroupHoldings(ctx, holdingsAt(listedGroup, mint, 100, chat.MinGroupHoldingsHolders, 10))
	require.NoError(t, err)
	_, err = s.EnqueueHoldingsRefresh(ctx, chatID, mint, at(0))
	require.NoError(t, err)

	// Below the minimum, however much is held, and nothing held at all: each
	// is stored, and neither is ranked.
	for i, unlisted := range []chat.GroupHoldings{
		holdingsAt(chatID, mint, 9_000_000, chat.MinGroupHoldingsHolders-1, 10),
		holdingsAt(chatID, mint, 0, 0, 20),
	} {
		require.False(t, unlisted.Listed(), i)
		changed, err := s.PutGroupHoldings(ctx, unlisted)
		require.NoError(t, err)
		require.True(t, changed)
		requireStored(unlisted)
		requireRanked(listedGroup)
	}

	// Reaching the minimum ranks it, above the other group.
	listed := holdingsAt(chatID, mint, 5_000, chat.MinGroupHoldingsHolders, 30)
	require.True(t, listed.Listed())
	changed, err := s.PutGroupHoldings(ctx, listed)
	require.NoError(t, err)
	require.True(t, changed)
	requireStored(listed)
	requireRanked(chatID, listedGroup)

	// Dropping below it again takes it out of the ranking with the snapshot
	// that records it, which stays stored.
	dropped := holdingsAt(chatID, mint, 4_000, chat.MinGroupHoldingsHolders-1, 40)
	changed, err = s.PutGroupHoldings(ctx, dropped)
	require.NoError(t, err)
	require.True(t, changed)
	requireStored(dropped)
	requireRanked(listedGroup)

	// An older listed snapshot landing late does not rank it again.
	changed, err = s.PutGroupHoldings(ctx, holdingsAt(chatID, mint, 9_000, 20, 35))
	require.NoError(t, err)
	require.False(t, changed)
	requireStored(dropped)
	requireRanked(listedGroup)

	// The queue entry is untouched throughout.
	require.Equal(t, [][]byte{chatID.Value}, dueChatIDs(t, s, at(1)))
}

func testStore_Holdings_MintMismatch(t *testing.T, s chat.Store) {
	ctx := context.Background()
	mint := model.MustGenerateKeyPair().Proto()
	other := model.MustGenerateKeyPair().Proto()

	requireQueuedFor := func(chatID *commonpb.ChatId, want *commonpb.PublicKey) {
		t.Helper()
		due, err := s.GetDueHoldingsRefreshes(ctx, at(1_000), 0)
		require.NoError(t, err)
		for _, r := range due {
			if bytes.Equal(r.ChatID.Value, chatID.Value) {
				require.Equal(t, want.Value, r.Mint.Value)
				return
			}
		}
		t.Fatalf("group not queued")
	}

	// A queued group's snapshots are of its queued mint: another is refused,
	// writes nothing, and leaves the queue totalling the queued one.
	queued := chat.MustGenerateGroupChatID()
	_, err := s.EnqueueHoldingsRefresh(ctx, queued, mint, at(0))
	require.NoError(t, err)
	_, err = s.PutGroupHoldings(ctx, holdingsAt(queued, other, 500, 10, 10))
	require.ErrorIs(t, err, chat.ErrHoldingsMintMismatch)
	got, err := s.GetGroupHoldings(ctx, []*commonpb.ChatId{queued})
	require.NoError(t, err)
	require.Empty(t, got)
	requireQueuedFor(queued, mint)

	// Its own mint lands; then another is refused whether older or newer
	// than the stored snapshot, which stands.
	stored := holdingsAt(queued, mint, 500, 10, 10)
	changed, err := s.PutGroupHoldings(ctx, stored)
	require.NoError(t, err)
	require.True(t, changed)
	for _, seconds := range []int64{5, 20} {
		_, err = s.PutGroupHoldings(ctx, holdingsAt(queued, other, 900, 12, seconds))
		require.ErrorIs(t, err, chat.ErrHoldingsMintMismatch)
	}
	got, err = s.GetGroupHoldings(ctx, []*commonpb.ChatId{queued})
	require.NoError(t, err)
	requireHoldingsEqual(t, stored, got[string(queued.Value)])
	requireQueuedFor(queued, mint)
	top, err := s.GetTopGroupsByHoldings(ctx, other, 0)
	require.NoError(t, err)
	require.Empty(t, top)

	// The same holds the other way round: a group with a snapshot of one
	// mint and no queue entry cannot be queued for another, only its own,
	// once.
	snapshotted := chat.MustGenerateGroupChatID()
	_, err = s.PutGroupHoldings(ctx, holdingsAt(snapshotted, mint, 500, 10, 10))
	require.NoError(t, err)
	_, err = s.EnqueueHoldingsRefresh(ctx, snapshotted, other, at(0))
	require.ErrorIs(t, err, chat.ErrHoldingsMintMismatch)
	changed, err = s.EnqueueHoldingsRefresh(ctx, snapshotted, mint, at(0))
	require.NoError(t, err)
	require.True(t, changed)
	changed, err = s.EnqueueHoldingsRefresh(ctx, snapshotted, mint, at(0))
	require.NoError(t, err)
	require.False(t, changed)
	requireQueuedFor(snapshotted, mint)
}

// restrictedAt is a restriction time a test passes and expects back: at(s),
// as a pointer for a ViewerState.
func restrictedAt(seconds int64) *time.Time {
	t := at(seconds)
	return &t
}

// privateGroup creates a private group with creator as its only member.
func privateGroup(t *testing.T, s chat.Store, creator *commonpb.UserId) *commonpb.ChatId {
	t.Helper()
	groupID := chat.MustGenerateGroupChatID()
	require.NoError(t, s.PutChat(context.Background(), &chat.Chat{
		ID:           groupID,
		Type:         chatpb.ChatType_GROUP,
		Members:      []*commonpb.UserId{creator},
		Title:        "Private",
		IsPrivate:    true,
		CreatorID:    creator,
		LastActivity: at(1),
	}))
	return groupID
}

// requireNoViewerState checks that the user has no record on the chat.
func requireNoViewerState(t *testing.T, s chat.Store, chatID *commonpb.ChatId, userID *commonpb.UserId) {
	t.Helper()
	states, err := s.GetViewerStates(context.Background(), userID, []*commonpb.ChatId{chatID})
	require.NoError(t, err)
	assert.Empty(t, states)
}

// waitForRestrictedUsers reads chatID's restricted users whole until they are
// exactly want, in order: the listing is read off an index that may trail the
// writes (see chat.Store.GetRestrictedUsersPage). It returns the listing.
func waitForRestrictedUsers(t *testing.T, s chat.Store, chatID *commonpb.ChatId, want ...*commonpb.UserId) []chat.RestrictedUser {
	t.Helper()
	var users []chat.RestrictedUser
	require.Eventually(t, func() bool {
		var err error
		users, err = s.GetRestrictedUsersPage(context.Background(), chatID, nil, 0)
		require.NoError(t, err)
		got := make([]*commonpb.UserId, len(users))
		for i, u := range users {
			got[i] = u.UserID
		}
		return slices.EqualFunc(userIDValues(want), userIDValues(got), bytes.Equal)
	}, 5*time.Second, 10*time.Millisecond)
	return users
}

// testStore_Restriction_Silence pins a silence's lifecycle on the record: set
// at second precision with RestrictedAt the time passed, the same silence a
// no-op that leaves RestrictedAt where it was, a different one a replace that
// moves it, and a clear that drops both and keeps the record and its version.
func testStore_Restriction_Silence(t *testing.T, s chat.Store) {
	ctx := context.Background()
	group := chat.MustGenerateGroupChatID()
	user := model.MustGenerateUserID()

	state, changed, err := s.SetSilence(ctx, group, user, chat.Silence{Until: at(100).Add(750 * time.Millisecond)}, at(10))
	require.NoError(t, err)
	assert.True(t, changed)
	want := chat.ViewerState{Silence: &chat.Silence{Until: at(100)}, RestrictedAt: restrictedAt(10), Version: 1}
	assert.Equal(t, want, state)
	requireViewerState(t, s, group, user, want)
	assert.NotNil(t, state.ActiveSilence(at(99)))
	assert.Nil(t, state.ActiveSilence(at(100)))
	assert.True(t, state.IsRestricted())
	assert.False(t, state.IsBanned())

	// The projection carries the silence under restrictions, lapsed or not.
	pb := state.ToProto(chat.Permissions{})
	require.NotNil(t, pb.Restrictions)
	assert.True(t, proto.Equal(chat.Silence{Until: at(100)}.ToProto(), pb.Restrictions.Silence))
	assert.Nil(t, pb.Settings.Mute)

	// The silence already recorded moves nothing, RestrictedAt included.
	state, changed, err = s.SetSilence(ctx, group, user, chat.Silence{Until: at(100)}, at(20))
	require.NoError(t, err)
	assert.False(t, changed)
	assert.Equal(t, want, state)

	// A different one replaces it and is a restriction made now.
	state, changed, err = s.SetSilence(ctx, group, user, chat.Silence{Forever: true, Until: at(5)}, at(30))
	require.NoError(t, err)
	assert.True(t, changed)
	want = chat.ViewerState{Silence: &chat.Silence{Forever: true}, RestrictedAt: restrictedAt(30), Version: 2}
	assert.Equal(t, want, state)
	requireViewerState(t, s, group, user, want)
	assert.NotNil(t, state.ActiveSilence(time.Date(3000, 1, 1, 0, 0, 0, 0, time.UTC)))

	state, changed, err = s.ClearSilence(ctx, group, user)
	require.NoError(t, err)
	assert.True(t, changed)
	assert.Equal(t, chat.ViewerState{Version: 3}, state)
	requireViewerState(t, s, group, user, chat.ViewerState{Version: 3})
	assert.Nil(t, state.ToProto(chat.Permissions{}).Restrictions)

	state, changed, err = s.ClearSilence(ctx, group, user)
	require.NoError(t, err)
	assert.False(t, changed)
	assert.Equal(t, chat.ViewerState{Version: 3}, state)

	// Clearing what was never set leaves no record behind.
	stranger := model.MustGenerateUserID()
	state, changed, err = s.ClearSilence(ctx, group, stranger)
	require.NoError(t, err)
	assert.False(t, changed)
	assert.Equal(t, chat.ViewerState{}, state)
	requireNoViewerState(t, s, group, stranger)

	// Out of range is refused with nothing written.
	_, _, err = s.SetSilence(ctx, group, stranger, chat.Silence{Until: chat.MaxMuteUntil}, at(10))
	require.ErrorIs(t, err, chat.ErrSilenceUntilOutOfRange)
	_, _, err = s.SetSilence(ctx, group, stranger, chat.Silence{Until: time.Unix(-1, 0)}, at(10))
	require.ErrorIs(t, err, chat.ErrSilenceUntilOutOfRange)
	requireNoViewerState(t, s, group, stranger)

	// Restrictions are a group's alone.
	dm := generateDmChatID()
	_, _, err = s.SetSilence(ctx, dm, user, chat.Silence{Forever: true}, at(10))
	require.Error(t, err)
	_, _, err = s.ClearSilence(ctx, dm, user)
	require.Error(t, err)
	_, err = s.BanUser(ctx, dm, user, at(10), false)
	require.Error(t, err)
	_, _, err = s.UnbanUser(ctx, dm, user)
	require.Error(t, err)
	_, err = s.GetRestrictedUsersPage(ctx, dm, nil, 0)
	require.Error(t, err)
}

// testStore_Restriction_IndependentOfMute pins that the user's mute and the
// creator's restrictions share the record and its version and nothing else:
// a mute's writes, first, replacing or clearing, never touch a restriction,
// and a restriction's never touch the mute or the chat's muted count.
func testStore_Restriction_IndependentOfMute(t *testing.T, s chat.Store) {
	ctx := context.Background()
	creator := model.MustGenerateUserID()
	user := model.MustGenerateUserID()
	c := putGroupChat(t, s, "Group", at(1), creator, user)
	group := c.ID

	// A silence first, then a first mute: the mute's counted write keeps the
	// silence.
	_, _, err := s.SetSilence(ctx, group, user, chat.Silence{Until: at(100)}, at(10))
	require.NoError(t, err)
	requireMutedCount(t, s, group, 0)
	requireMutedUsers(t, s, group, at(0))
	state, changed, err := s.SetMute(ctx, group, user, chat.Mute{Forever: true})
	require.NoError(t, err)
	assert.True(t, changed)
	want := chat.ViewerState{
		Mute:         &chat.Mute{Forever: true},
		Silence:      &chat.Silence{Until: at(100)},
		RestrictedAt: restrictedAt(10),
		Version:      2,
	}
	assert.Equal(t, want, state)
	requireViewerState(t, s, group, user, want)
	requireMutedCount(t, s, group, 1)

	// Replacing the mute keeps the silence too.
	state, _, err = s.SetMute(ctx, group, user, chat.Mute{Until: at(200)})
	require.NoError(t, err)
	want.Mute = &chat.Mute{Until: at(200)}
	want.Version = 3
	assert.Equal(t, want, state)

	// A silence replaced or cleared keeps the mute and its count.
	state, _, err = s.SetSilence(ctx, group, user, chat.Silence{Forever: true}, at(20))
	require.NoError(t, err)
	want.Silence = &chat.Silence{Forever: true}
	want.RestrictedAt = restrictedAt(20)
	want.Version = 4
	assert.Equal(t, want, state)
	requireMutedCount(t, s, group, 1)
	requireMutedUsers(t, s, group, at(0), user)

	// A ban keeps the mute and the silence.
	result, err := s.BanUser(ctx, group, user, at(30), false)
	require.NoError(t, err)
	assert.True(t, result.Changed)
	want.BannedAt = restrictedAt(30)
	want.RestrictedAt = restrictedAt(30)
	want.Version = 5
	assert.Equal(t, want, result.State)
	requireMutedCount(t, s, group, 1)

	// The departure's ClearMute clears the mute alone: the silence and the
	// ban stand.
	state, changed, err = s.ClearMute(ctx, group, user)
	require.NoError(t, err)
	assert.True(t, changed)
	want.Mute = nil
	want.Version = 6
	assert.Equal(t, want, state)
	requireViewerState(t, s, group, user, want)
	requireMutedCount(t, s, group, 0)

	// Clearing the silence keeps the ban, and RestrictedAt with it.
	state, changed, err = s.ClearSilence(ctx, group, user)
	require.NoError(t, err)
	assert.True(t, changed)
	want.Silence = nil
	want.Version = 7
	assert.Equal(t, want, state)
	requireMutedCount(t, s, group, 0)

	// A mute set on a banned user's record keeps the ban.
	state, _, err = s.SetMute(ctx, group, user, chat.Mute{Forever: true})
	require.NoError(t, err)
	want.Mute = &chat.Mute{Forever: true}
	want.Version = 8
	assert.Equal(t, want, state)

	// Unbanning, with no silence left, drops RestrictedAt and keeps the mute.
	state, changed, err = s.UnbanUser(ctx, group, user)
	require.NoError(t, err)
	assert.True(t, changed)
	assert.Equal(t, chat.ViewerState{Mute: &chat.Mute{Forever: true}, Version: 9}, state)
	requireViewerState(t, s, group, user, chat.ViewerState{Mute: &chat.Mute{Forever: true}, Version: 9})
	requireMutedCount(t, s, group, 1)
}

// testStore_Restriction_BanMember pins a ban of a joined member: one write
// that records the ban and departs them exactly as RemoveGroupMember would,
// roster version, tombstone and all, discarding their key envelope only when
// asked; a repeat is the no-op, and the restriction survives their absence.
func testStore_Restriction_BanMember(t *testing.T, s chat.Store) {
	ctx := context.Background()
	creator := model.MustGenerateUserID()
	member := model.MustGenerateUserID()
	keeper := model.MustGenerateUserID()
	c := putGroupChat(t, s, "Group", at(1), creator, member, keeper)
	group := c.ID
	for i, u := range []*commonpb.UserId{member, keeper} {
		_, err := s.SetKeyEnvelope(ctx, group, u, keyEnvelope(byte(i+1), u))
		require.NoError(t, err)
	}
	_, _, err := s.SetSilence(ctx, group, member, chat.Silence{Forever: true}, at(5))
	require.NoError(t, err)

	result, err := s.BanUser(ctx, group, member, at(10), true)
	require.NoError(t, err)
	assert.Equal(t, chat.BanResult{
		State: chat.ViewerState{
			Silence:      &chat.Silence{Forever: true},
			BannedAt:     restrictedAt(10),
			RestrictedAt: restrictedAt(10),
			Version:      2,
		},
		Changed:  true,
		Departed: true,
		Roster:   chat.RosterSummary{MemberCount: 2, Version: 1},
	}, result)
	requireViewerState(t, s, group, member, result.State)
	requireRosterSummary(t, s, group, 2, 1)
	isMember, err := s.IsMember(ctx, group, member)
	require.NoError(t, err)
	assert.False(t, isMember)
	_, err = s.GetKeyEnvelope(ctx, group, member)
	require.ErrorIs(t, err, chat.ErrKeyEnvelopeNotFound)

	// The departure is a tombstone at the ban's roster version, as any.
	memberships, err := s.GetGroupMembershipsForUser(ctx, member)
	require.NoError(t, err)
	require.Len(t, memberships, 1)
	assert.False(t, memberships[0].Joined)
	assert.EqualValues(t, 1, memberships[0].Version)

	// Banning again is the no-op: nothing is written, and the roster is as
	// it stands.
	again, err := s.BanUser(ctx, group, member, at(20), true)
	require.NoError(t, err)
	assert.Equal(t, chat.BanResult{State: result.State, Roster: chat.RosterSummary{MemberCount: 2, Version: 1}}, again)
	requireRosterSummary(t, s, group, 2, 1)

	// Not asked to discard, the envelope stays, as on a leave.
	result, err = s.BanUser(ctx, group, keeper, at(30), false)
	require.NoError(t, err)
	assert.True(t, result.Changed)
	assert.True(t, result.Departed)
	assert.False(t, result.LeftLobby)
	assert.Equal(t, chat.RosterSummary{MemberCount: 1, Version: 2}, result.Roster)
	assert.Equal(t, chat.ViewerState{BannedAt: restrictedAt(30), RestrictedAt: restrictedAt(30), Version: 1}, result.State)
	_, err = s.GetKeyEnvelope(ctx, group, keeper)
	require.NoError(t, err)

	// The restrictions outlive the departure, a departure's ClearMute
	// included, and come back with an unbanned user who rejoins.
	_, _, err = s.ClearMute(ctx, group, member)
	require.NoError(t, err)
	state, changed, err := s.UnbanUser(ctx, group, member)
	require.NoError(t, err)
	assert.True(t, changed)
	want := chat.ViewerState{Silence: &chat.Silence{Forever: true}, RestrictedAt: restrictedAt(10), Version: 3}
	assert.Equal(t, want, state)
	isMember, err = s.IsMember(ctx, group, member)
	require.NoError(t, err)
	assert.False(t, isMember, "unbanning does not readmit")
	changed, roster := addGroupMembers(t, s, group, member)
	assert.True(t, changed)
	assert.Equal(t, chat.RosterSummary{MemberCount: 2, Version: 3}, roster)
	requireViewerState(t, s, group, member, want)

	// A silenced member who leaves and returns is silenced still.
	removeGroupMember(t, s, group, member)
	_, _, err = s.ClearMute(ctx, group, member)
	require.NoError(t, err)
	addGroupMembers(t, s, group, member)
	requireViewerState(t, s, group, member, want)

	// A group that does not exist has no one to ban.
	_, err = s.BanUser(ctx, chat.MustGenerateGroupChatID(), member, at(40), false)
	require.ErrorIs(t, err, chat.ErrChatNotFound)
}

// testStore_Restriction_BanNonMember pins a ban of someone the group does not
// hold: the record alone, with the roster as it stands, and a user waiting in
// the lobby taken out of it with both counts.
func testStore_Restriction_BanNonMember(t *testing.T, s chat.Store) {
	ctx := context.Background()
	creator := model.MustGenerateUserID()
	group := privateGroup(t, s, creator)
	stranger := model.MustGenerateUserID()

	result, err := s.BanUser(ctx, group, stranger, at(10), true)
	require.NoError(t, err)
	assert.Equal(t, chat.BanResult{
		State:   chat.ViewerState{BannedAt: restrictedAt(10), RestrictedAt: restrictedAt(10), Version: 1},
		Changed: true,
		Roster:  chat.RosterSummary{MemberCount: 1},
	}, result)
	requireRosterSummary(t, s, group, 1, 0)
	memberships, err := s.GetGroupMembershipsForUser(ctx, stranger)
	require.NoError(t, err)
	assert.Empty(t, memberships)

	// A user waiting in the lobby leaves it. The caps are tight, so the
	// places the entry held, in the lobby and among the user's lobbies,
	// show as freed by the next entries.
	tight := chat.LobbyLimits{LobbySize: 1, LobbiesPerUser: 1}
	waiting := model.MustGenerateUserID()
	_, _, err = s.EnterLobby(ctx, group, waiting, tight)
	require.NoError(t, err)
	_, _, err = s.EnterLobby(ctx, group, model.MustGenerateUserID(), tight)
	require.ErrorIs(t, err, chat.ErrLobbyFull)

	result, err = s.BanUser(ctx, group, waiting, at(20), false)
	require.NoError(t, err)
	assert.True(t, result.Changed)
	assert.False(t, result.Departed)
	assert.True(t, result.LeftLobby)
	assert.Equal(t, chat.RosterSummary{MemberCount: 1}, result.Roster)
	entries, err := s.GetLobbyEntries(ctx, waiting, []*commonpb.ChatId{group})
	require.NoError(t, err)
	assert.Empty(t, entries)
	waitForLobbyPage(t, s, group, 0)

	_, changed, err := s.EnterLobby(ctx, group, model.MustGenerateUserID(), tight)
	require.NoError(t, err)
	assert.True(t, changed, "the lobby's place was freed")
	_, changed, err = s.EnterLobby(ctx, privateGroup(t, s, creator), waiting, tight)
	require.NoError(t, err)
	assert.True(t, changed, "the user's place was freed")
}

// testStore_Restriction_BanRefusesWaysIn pins ErrBanned: a banned user is
// refused every write that would let them in — a join, a lobby entry, an
// admission — with nothing written, and let in again once unbanned.
func testStore_Restriction_BanRefusesWaysIn(t *testing.T, s chat.Store) {
	ctx := context.Background()
	creator := model.MustGenerateUserID()
	group := privateGroup(t, s, creator)
	user := model.MustGenerateUserID()

	_, err := s.BanUser(ctx, group, user, at(10), false)
	require.NoError(t, err)

	changed, roster, err := s.AddGroupMembers(ctx, group, []*commonpb.UserId{user})
	require.ErrorIs(t, err, chat.ErrBanned)
	assert.False(t, changed)
	assert.Equal(t, chat.RosterSummary{MemberCount: 1}, roster)
	requireRosterSummary(t, s, group, 1, 0)

	// The users before the banned one in a batch are joined, the rest not.
	before, after := model.MustGenerateUserID(), model.MustGenerateUserID()
	changed, roster, err = s.AddGroupMembers(ctx, group, []*commonpb.UserId{before, user, after})
	require.ErrorIs(t, err, chat.ErrBanned)
	assert.True(t, changed)
	assert.Equal(t, chat.RosterSummary{MemberCount: 2, Version: 1}, roster)
	isMember, err := s.IsMember(ctx, group, after)
	require.NoError(t, err)
	assert.False(t, isMember)

	_, _, err = s.EnterLobby(ctx, group, user, lobbyLimits)
	require.ErrorIs(t, err, chat.ErrBanned)
	entries, err := s.GetLobbyEntries(ctx, user, []*commonpb.ChatId{group})
	require.NoError(t, err)
	assert.Empty(t, entries)

	// Judged before the lobby: a banned user holds no entry, and is told why.
	_, _, err = s.AdmitFromLobby(ctx, group, user, keyEnvelope(1, creator))
	require.ErrorIs(t, err, chat.ErrBanned)
	_, err = s.GetKeyEnvelope(ctx, group, user)
	require.ErrorIs(t, err, chat.ErrKeyEnvelopeNotFound)
	isMember, err = s.IsMember(ctx, group, user)
	require.NoError(t, err)
	assert.False(t, isMember)
	requireRosterSummary(t, s, group, 2, 1)

	// A member is a member whatever the record holds: the no-op is judged
	// first, and the join it would have been is not refused.
	changed, _, err = s.AddGroupMembers(ctx, group, []*commonpb.UserId{before})
	require.NoError(t, err)
	assert.False(t, changed)

	// Unbanned, the user comes in by any way; unbanning again is the no-op.
	state, changed, err := s.UnbanUser(ctx, group, user)
	require.NoError(t, err)
	assert.True(t, changed)
	assert.Equal(t, chat.ViewerState{Version: 2}, state)
	state, changed, err = s.UnbanUser(ctx, group, user)
	require.NoError(t, err)
	assert.False(t, changed)
	assert.Equal(t, chat.ViewerState{Version: 2}, state)

	_, changed, err = s.EnterLobby(ctx, group, user, lobbyLimits)
	require.NoError(t, err)
	assert.True(t, changed)
	changed, roster, err = s.AdmitFromLobby(ctx, group, user, keyEnvelope(1, creator))
	require.NoError(t, err)
	assert.True(t, changed)
	assert.Equal(t, chat.RosterSummary{MemberCount: 3, Version: 2}, roster)

	// Banned again as a member, and joined again once unbanned.
	result, err := s.BanUser(ctx, group, user, at(20), true)
	require.NoError(t, err)
	assert.True(t, result.Departed)
	_, _, err = s.AddGroupMembers(ctx, group, []*commonpb.UserId{user})
	require.ErrorIs(t, err, chat.ErrBanned)
	_, _, err = s.UnbanUser(ctx, group, user)
	require.NoError(t, err)
	changed, roster = addGroupMembers(t, s, group, user)
	assert.True(t, changed)
	assert.Equal(t, chat.RosterSummary{MemberCount: 3, Version: 4}, roster)

	// Unbanning someone never banned writes nothing.
	stranger := model.MustGenerateUserID()
	state, changed, err = s.UnbanUser(ctx, group, stranger)
	require.NoError(t, err)
	assert.False(t, changed)
	assert.Equal(t, chat.ViewerState{}, state)
	requireNoViewerState(t, s, group, stranger)
}

// testStore_Restriction_BanRaces pins that a ban and a way in are exclusive
// under concurrency: whichever lands first, the user ends banned, not a
// member and not waiting, and a refused way in is ErrBanned.
func testStore_Restriction_BanRaces(t *testing.T, s chat.Store) {
	ctx := context.Background()
	creator := model.MustGenerateUserID()
	group := privateGroup(t, s, creator)

	for i := 0; i < 8; i++ {
		joiner, waiter := model.MustGenerateUserID(), model.MustGenerateUserID()
		var wg sync.WaitGroup
		errs := make([]error, 4)
		wg.Add(4)
		go func() {
			defer wg.Done()
			_, _, errs[0] = s.AddGroupMembers(ctx, group, []*commonpb.UserId{joiner})
		}()
		go func() {
			defer wg.Done()
			_, errs[1] = s.BanUser(ctx, group, joiner, at(int64(i)), true)
		}()
		go func() {
			defer wg.Done()
			_, _, errs[2] = s.EnterLobby(ctx, group, waiter, lobbyLimits)
		}()
		go func() {
			defer wg.Done()
			_, errs[3] = s.BanUser(ctx, group, waiter, at(int64(i)), false)
		}()
		wg.Wait()

		for _, err := range []error{errs[0], errs[2]} {
			if err != nil {
				require.ErrorIs(t, err, chat.ErrBanned)
			}
		}
		require.NoError(t, errs[1])
		require.NoError(t, errs[3])
		for _, u := range []*commonpb.UserId{joiner, waiter} {
			isMember, err := s.IsMember(ctx, group, u)
			require.NoError(t, err)
			assert.False(t, isMember)
			entries, err := s.GetLobbyEntries(ctx, u, []*commonpb.ChatId{group})
			require.NoError(t, err)
			assert.Empty(t, entries)
			states, err := s.GetViewerStates(ctx, u, []*commonpb.ChatId{group})
			require.NoError(t, err)
			assert.True(t, states[string(group.Value)].IsBanned())
		}
	}

	// The roster summary agrees with the rows: every join undone by a ban
	// counted its departure.
	summary, members, err := s.GetGroupRoster(ctx, group)
	require.NoError(t, err)
	assert.EqualValues(t, len(members), summary.MemberCount)
	assert.EqualValues(t, 1, summary.MemberCount)
}

// testStore_Restriction_List pins the creator's listing: every user with a
// restriction recorded, most recently restricted first, paged strictly after
// a position, a replaced silence moving its user to the front, a user
// keeping their place while one of two restrictions is lifted, and leaving
// once both are; a mute alone is no restriction.
func testStore_Restriction_List(t *testing.T, s chat.Store) {
	ctx := context.Background()
	creator := model.MustGenerateUserID()
	c := putGroupChat(t, s, "Group", at(1), creator)
	group := c.ID
	other := putGroupChat(t, s, "Other", at(1), creator).ID
	a, b, d, e := model.MustGenerateUserID(), model.MustGenerateUserID(), model.MustGenerateUserID(), model.MustGenerateUserID()

	waitForRestrictedUsers(t, s, group)

	_, _, err := s.SetSilence(ctx, group, a, chat.Silence{Until: at(100)}, at(10))
	require.NoError(t, err)
	_, err = s.BanUser(ctx, group, b, at(20), false)
	require.NoError(t, err)
	_, _, err = s.SetSilence(ctx, group, d, chat.Silence{Forever: true}, at(30))
	require.NoError(t, err)
	_, err = s.BanUser(ctx, group, d, at(40), false)
	require.NoError(t, err)
	_, _, err = s.SetMute(ctx, group, e, chat.Mute{Forever: true})
	require.NoError(t, err)
	_, _, err = s.SetSilence(ctx, other, e, chat.Silence{Forever: true}, at(50))
	require.NoError(t, err)

	users := waitForRestrictedUsers(t, s, group, d, b, a)
	assert.Equal(t, chat.ViewerState{
		Silence:      &chat.Silence{Forever: true},
		BannedAt:     restrictedAt(40),
		RestrictedAt: restrictedAt(40),
		Version:      2,
	}, users[0].State)
	assert.Equal(t, chat.ViewerState{BannedAt: restrictedAt(20), RestrictedAt: restrictedAt(20), Version: 1}, users[1].State)
	// A lapsed silence is listed still: leaving it out is the reader's call.
	assert.Equal(t, chat.ViewerState{Silence: &chat.Silence{Until: at(100)}, RestrictedAt: restrictedAt(10), Version: 1}, users[2].State)
	waitForRestrictedUsers(t, s, other, e)

	// Paged one at a time, strictly after each position.
	var walked []*commonpb.UserId
	var after *chat.RestrictionPosition
	for {
		page, err := s.GetRestrictedUsersPage(ctx, group, after, 1)
		require.NoError(t, err)
		if len(page) == 0 {
			break
		}
		require.Len(t, page, 1)
		walked = append(walked, page[0].UserID)
		position := page[0].Position()
		after = &position
	}
	assert.Equal(t, userIDValues([]*commonpb.UserId{d, b, a}), userIDValues(walked))

	// A position need not name a listed user: the page resumes below it.
	page, err := s.GetRestrictedUsersPage(ctx, group, &chat.RestrictionPosition{RestrictedAt: at(25), UserID: e}, 0)
	require.NoError(t, err)
	assert.Len(t, page, 2)

	// A replaced silence is a restriction made now.
	_, _, err = s.SetSilence(ctx, group, a, chat.Silence{Forever: true}, at(60))
	require.NoError(t, err)
	waitForRestrictedUsers(t, s, group, a, d, b)

	// Lifting one of two keeps the user where they were; lifting the last
	// takes them out.
	_, _, err = s.ClearSilence(ctx, group, d)
	require.NoError(t, err)
	users = waitForRestrictedUsers(t, s, group, a, d, b)
	assert.Equal(t, chat.ViewerState{BannedAt: restrictedAt(40), RestrictedAt: restrictedAt(40), Version: 3}, users[1].State)
	_, _, err = s.UnbanUser(ctx, group, d)
	require.NoError(t, err)
	_, _, err = s.UnbanUser(ctx, group, b)
	require.NoError(t, err)
	waitForRestrictedUsers(t, s, group, a)
	_, _, err = s.ClearSilence(ctx, group, a)
	require.NoError(t, err)
	waitForRestrictedUsers(t, s, group)
}

// testStore_Restriction_ViewerStatesForUsers pins the chat-scoped batch read:
// each user's record on the one chat, whatever it holds, the users with
// none absent, duplicates collapsed, and more users than one batch read
// takes.
func testStore_Restriction_ViewerStatesForUsers(t *testing.T, s chat.Store) {
	ctx := context.Background()
	group := chat.MustGenerateGroupChatID()
	other := chat.MustGenerateGroupChatID()
	banned, silenced, muted := model.MustGenerateUserID(), model.MustGenerateUserID(), model.MustGenerateUserID()

	_, err := s.BanUser(ctx, group, banned, at(10), false)
	require.ErrorIs(t, err, chat.ErrChatNotFound)
	c := putGroupChat(t, s, "Group", at(1), model.MustGenerateUserID())
	group = c.ID
	_, err = s.BanUser(ctx, group, banned, at(10), false)
	require.NoError(t, err)
	_, _, err = s.SetSilence(ctx, group, silenced, chat.Silence{Forever: true}, at(20))
	require.NoError(t, err)
	_, _, err = s.SetMute(ctx, group, muted, chat.Mute{Forever: true})
	require.NoError(t, err)
	_, _, err = s.SetSilence(ctx, other, banned, chat.Silence{Forever: true}, at(30))
	require.NoError(t, err)

	states, err := s.GetViewerStatesForUsers(ctx, group, nil)
	require.NoError(t, err)
	assert.Empty(t, states)

	userIDs := []*commonpb.UserId{banned, silenced, muted, banned}
	for range 150 {
		userIDs = append(userIDs, model.MustGenerateUserID())
	}
	want := map[string]chat.ViewerState{
		string(banned.Value):   {BannedAt: restrictedAt(10), RestrictedAt: restrictedAt(10), Version: 1},
		string(silenced.Value): {Silence: &chat.Silence{Forever: true}, RestrictedAt: restrictedAt(20), Version: 1},
		string(muted.Value):    {Mute: &chat.Mute{Forever: true}, Version: 1},
	}
	require.Eventually(t, func() bool {
		states, err = s.GetViewerStatesForUsers(ctx, group, userIDs)
		require.NoError(t, err)
		return assert.ObjectsAreEqual(want, states)
	}, 5*time.Second, 10*time.Millisecond)

	// And the strongly consistent read the speaker gate uses carries the
	// restrictions too.
	requireViewerState(t, s, group, banned, want[string(banned.Value)])
	requireViewerState(t, s, group, silenced, want[string(silenced.Value)])
}

// inviteKeyCopy is an invite's copy of the chat key. The store never opens
// or verifies one, so any bytes of the right length stand in.
func inviteKeyCopy(fill byte) *chatpb.InviteKeyEnvelope {
	return &chatpb.InviteKeyEnvelope{
		Scheme:     chatpb.InviteKeyEnvelope_XCHACHA20POLY1305,
		Nonce:      bytes.Repeat([]byte{fill}, 24),
		Ciphertext: bytes.Repeat([]byte{fill}, 48),
		Signature:  bytes.Repeat([]byte{fill}, 64),
	}
}

// inviteSecret is a secret envelope, as an inviter or a joiner makes one.
func inviteSecret(fill byte) *chatpb.InviteSecretEnvelope {
	return &chatpb.InviteSecretEnvelope{
		Scheme:     chatpb.InviteSecretEnvelope_X25519_XCHACHA20POLY1305,
		Nonce:      bytes.Repeat([]byte{fill}, 24),
		Ciphertext: bytes.Repeat([]byte{fill}, 48),
	}
}

// newInvite is a new invite by creator to chatID, created at createdAt, with
// an ID derived from a fresh token as a client derives one.
func newInvite(chatID *commonpb.ChatId, creator *commonpb.UserId, maxUses uint32, createdAt time.Time, expiresAt *time.Time) chat.Invite {
	return chat.Invite{
		ID:             chat.MustDeriveInviteID(&chatpb.InviteToken{Value: randomBytes(chat.InviteTokenSize)}),
		ChatID:         chatID,
		CreatedBy:      creator,
		MaxUses:        maxUses,
		CreatedAt:      createdAt,
		ExpiresAt:      expiresAt,
		SecretEnvelope: inviteSecret(1),
	}
}

// createInvite creates invite under maxActive as of now, requiring that it
// is created, and returns it as stored.
func createInvite(t *testing.T, s chat.Store, invite chat.Invite, maxActive int, now time.Time) chat.Invite {
	t.Helper()
	stored, created, err := s.CreateInvite(context.Background(), invite, inviteKeyCopy(2), maxActive, now)
	require.NoError(t, err)
	require.True(t, created)
	requireInviteEqual(t, invite, stored)
	return stored
}

// createInviteEventually is createInvite for a create that the cap's count,
// which may trail writes (see chat.Store.CreateInvite), refuses until a
// revoke that freed a place reaches it. A refusal writes nothing, so the
// create is simply asked again.
func createInviteEventually(t *testing.T, s chat.Store, invite chat.Invite, maxActive int, now time.Time) {
	t.Helper()
	require.Eventually(t, func() bool {
		_, created, err := s.CreateInvite(context.Background(), invite, inviteKeyCopy(2), maxActive, now)
		if errors.Is(err, chat.ErrTooManyInvites) {
			return false
		}
		require.NoError(t, err)
		require.True(t, created)
		return true
	}, 5*time.Second, 10*time.Millisecond)
}

// requireInviteEqual checks that got is want, field by field, times to the
// nanosecond.
func requireInviteEqual(t *testing.T, want, got chat.Invite) {
	t.Helper()
	requireTimePtrEqual := func(name string, want, got *time.Time) {
		t.Helper()
		if want == nil {
			require.Nil(t, got, name)
			return
		}
		require.NotNil(t, got, name)
		require.True(t, want.Equal(*got), "%s: want %v, got %v", name, *want, *got)
	}
	require.Equal(t, want.ID.GetValue(), got.ID.GetValue())
	require.Equal(t, want.ChatID.GetValue(), got.ChatID.GetValue())
	require.Equal(t, want.CreatedBy.GetValue(), got.CreatedBy.GetValue())
	require.Equal(t, want.MaxUses, got.MaxUses)
	require.Equal(t, want.UseCount, got.UseCount)
	require.True(t, want.CreatedAt.Equal(got.CreatedAt), "created at: want %v, got %v", want.CreatedAt, got.CreatedAt)
	requireTimePtrEqual("expires at", want.ExpiresAt, got.ExpiresAt)
	requireTimePtrEqual("revoked at", want.RevokedAt, got.RevokedAt)
	require.True(t, proto.Equal(want.SecretEnvelope, got.SecretEnvelope), "secret envelope")
}

// inviteIDValues is the IDs of invites, in order.
func inviteIDValues(invites []chat.Invite) [][]byte {
	out := make([][]byte, len(invites))
	for i, invite := range invites {
		out[i] = invite.ID.Value
	}
	return out
}

// waitForInvites reads chatID's invites whole (createdBy's alone when set)
// until they are exactly want, in order: the listing is read off an index
// that may trail the writes (see chat.Store.GetChatInvitesPage).
func waitForInvites(t *testing.T, s chat.Store, chatID *commonpb.ChatId, createdBy *commonpb.UserId, want ...chat.Invite) []chat.Invite {
	t.Helper()
	var invites []chat.Invite
	require.Eventually(t, func() bool {
		var err error
		invites, err = s.GetChatInvitesPage(context.Background(), chatID, createdBy, nil, 0)
		require.NoError(t, err)
		return slices.EqualFunc(inviteIDValues(want), inviteIDValues(invites), bytes.Equal)
	}, 5*time.Second, 10*time.Millisecond)
	return invites
}

// testStore_Invite_CreateAndGet pins that an invite reads back exactly as
// created, with and without a limit and an expiry, at nanosecond precision,
// against the IDs alone, and that a create of the wrong shape is refused.
func testStore_Invite_CreateAndGet(t *testing.T, s chat.Store) {
	ctx := context.Background()
	now := time.Now()
	groupID := chat.MustGenerateGroupChatID()
	creator := model.MustGenerateUserID()

	_, err := s.GetInvite(ctx, newInvite(groupID, creator, 0, now, nil).ID)
	require.ErrorIs(t, err, chat.ErrInviteNotFound)

	// Unlimited and never expiring.
	open := newInvite(groupID, creator, 0, now, nil)
	createInvite(t, s, open, chat.MaxActiveInvites, now)
	got, err := s.GetInvite(ctx, open.ID)
	require.NoError(t, err)
	requireInviteEqual(t, open, got)
	require.Equal(t, chatpb.Invite_ACTIVE, got.StatusAt(now))

	// Limited and expiring, with its own secret envelope; times keep their
	// nanoseconds.
	expiresAt := now.Add(24*time.Hour + 123*time.Nanosecond)
	limited := newInvite(groupID, creator, 5, now.Add(7*time.Nanosecond), &expiresAt)
	limited.SecretEnvelope = inviteSecret(9)
	createInvite(t, s, limited, chat.MaxActiveInvites, now)
	got, err = s.GetInvite(ctx, limited.ID)
	require.NoError(t, err)
	requireInviteEqual(t, limited, got)
	require.Equal(t, chatpb.Invite_ACTIVE, got.StatusAt(now))
	require.Equal(t, chatpb.Invite_EXPIRED, got.StatusAt(expiresAt))
	require.Equal(t, bytes.Repeat([]byte{9}, 48), got.SecretEnvelope.Ciphertext)

	// The returned invite is the caller's to change.
	got.SecretEnvelope.Ciphertext[0] = 0
	got.ChatID.Value[0] ^= 0xff
	again, err := s.GetInvite(ctx, limited.ID)
	require.NoError(t, err)
	requireInviteEqual(t, limited, again)

	// A create of the wrong shape writes nothing.
	for name, mutate := range map[string]func(*chat.Invite){
		"dm chat":       func(i *chat.Invite) { i.ChatID = generateDmChatID() },
		"short id":      func(i *chat.Invite) { i.ID = &chatpb.InviteId{Value: randomBytes(8)} },
		"no creator":    func(i *chat.Invite) { i.CreatedBy = nil },
		"no secret":     func(i *chat.Invite) { i.SecretEnvelope = nil },
		"used":          func(i *chat.Invite) { i.UseCount = 1 },
		"revoked":       func(i *chat.Invite) { i.RevokedAt = &now },
		"before epoch":  func(i *chat.Invite) { i.CreatedAt = time.Unix(-1, 0) },
		"expiry before": func(i *chat.Invite) { e := time.Unix(-1, 0); i.ExpiresAt = &e },
	} {
		invite := newInvite(groupID, creator, 0, now, nil)
		mutate(&invite)
		_, _, err := s.CreateInvite(ctx, invite, inviteKeyCopy(2), chat.MaxActiveInvites, now)
		require.Error(t, err, name)
		_, err = s.GetInvite(ctx, invite.ID)
		require.ErrorIs(t, err, chat.ErrInviteNotFound, name)
	}
	invite := newInvite(groupID, creator, 0, now, nil)
	_, _, err = s.CreateInvite(ctx, invite, nil, chat.MaxActiveInvites, now)
	require.Error(t, err)
	_, _, err = s.CreateInvite(ctx, invite, inviteKeyCopy(2), 0, now)
	require.Error(t, err)
	_, err = s.GetInvite(ctx, invite.ID)
	require.ErrorIs(t, err, chat.ErrInviteNotFound)
}

// testStore_Invite_CreateExisting pins that an invite ID names one invite:
// a create naming one that is stored, whatever else it carries and whatever
// the stored one's status, writes nothing and returns the stored one, and
// is judged before the cap, so a retry at the cap is never refused.
func testStore_Invite_CreateExisting(t *testing.T, s chat.Store) {
	ctx := context.Background()
	now := time.Now()
	groupID := chat.MustGenerateGroupChatID()
	creator := model.MustGenerateUserID()

	first := createInvite(t, s, newInvite(groupID, creator, 3, now, nil), 2, now)

	// Another chat, creator, limit, expiry and secret: the stored invite.
	clash := first
	clash.ChatID = chat.MustGenerateGroupChatID()
	clash.CreatedBy = model.MustGenerateUserID()
	clash.MaxUses = 9
	expiresAt := now.Add(time.Hour)
	clash.ExpiresAt = &expiresAt
	clash.CreatedAt = now.Add(time.Minute)
	clash.SecretEnvelope = inviteSecret(7)
	stored, created, err := s.CreateInvite(ctx, clash, inviteKeyCopy(8), 2, now)
	require.NoError(t, err)
	require.False(t, created)
	requireInviteEqual(t, first, stored)
	got, err := s.GetInvite(ctx, first.ID)
	require.NoError(t, err)
	requireInviteEqual(t, first, got)
	waitForInvites(t, s, groupID, nil, first)
	page, err := s.GetChatInvitesPage(ctx, clash.ChatID, nil, nil, 0)
	require.NoError(t, err)
	require.Empty(t, page)

	// At the cap, a retry is still answered with the invite, while a new
	// invite is refused.
	second := createInvite(t, s, newInvite(groupID, creator, 0, now.Add(time.Second), nil), 2, now)
	stored, created, err = s.CreateInvite(ctx, first, inviteKeyCopy(2), 2, now)
	require.NoError(t, err)
	require.False(t, created)
	requireInviteEqual(t, first, stored)
	stored, created, err = s.CreateInvite(ctx, second, inviteKeyCopy(2), 1, now)
	require.NoError(t, err)
	require.False(t, created)
	requireInviteEqual(t, second, stored)
	refused := newInvite(groupID, creator, 0, now, nil)
	_, _, err = s.CreateInvite(ctx, refused, inviteKeyCopy(2), 2, now)
	require.ErrorIs(t, err, chat.ErrTooManyInvites)
	_, err = s.GetInvite(ctx, refused.ID)
	require.ErrorIs(t, err, chat.ErrInviteNotFound)

	// Whatever the stored invite's status.
	revoked, changed, err := s.RevokeInvite(ctx, groupID, first.ID, now)
	require.NoError(t, err)
	require.True(t, changed)
	stored, created, err = s.CreateInvite(ctx, first, inviteKeyCopy(2), 2, now)
	require.NoError(t, err)
	require.False(t, created)
	requireInviteEqual(t, revoked, stored)
}

// testStore_Invite_CapCountsActive pins that the cap counts the chat's
// invites ACTIVE as of the create's now, and no others: not a revoked one,
// not one expired by then, not one whose last use a join took, and not
// another chat's.
func testStore_Invite_CapCountsActive(t *testing.T, s chat.Store) {
	ctx := context.Background()
	now := time.Now()
	groupID := chat.MustGenerateGroupChatID()
	creator := model.MustGenerateUserID()
	const maxActive = 2

	expiresAt := now.Add(time.Hour)
	expiring := createInvite(t, s, newInvite(groupID, creator, 0, now, &expiresAt), maxActive, now)
	lasting := createInvite(t, s, newInvite(groupID, creator, 1, now, nil), maxActive, now)
	_, _, err := s.CreateInvite(ctx, newInvite(groupID, creator, 0, now, nil), inviteKeyCopy(2), maxActive, now)
	require.ErrorIs(t, err, chat.ErrTooManyInvites)

	// Another chat's invites are not this one's.
	createInvite(t, s, newInvite(chat.MustGenerateGroupChatID(), creator, 0, now, nil), maxActive, now)

	// A revoked invite frees its place.
	_, changed, err := s.RevokeInvite(ctx, groupID, lasting.ID, now)
	require.NoError(t, err)
	require.True(t, changed)
	createInviteEventually(t, s, newInvite(groupID, creator, 0, now, nil), maxActive, now)
	_, _, err = s.CreateInvite(ctx, newInvite(groupID, creator, 0, now, nil), inviteKeyCopy(2), maxActive, now)
	require.ErrorIs(t, err, chat.ErrTooManyInvites)

	// An expired one is not counted from the instant it expires, with
	// nothing written: just before, it still is.
	_, _, err = s.CreateInvite(ctx, newInvite(groupID, creator, 0, now, nil), inviteKeyCopy(2), maxActive, expiresAt.Add(-time.Nanosecond))
	require.ErrorIs(t, err, chat.ErrTooManyInvites)
	createInvite(t, s, newInvite(groupID, creator, 0, now, nil), maxActive, expiresAt)
	_, _, err = s.CreateInvite(ctx, newInvite(groupID, creator, 0, now, nil), inviteKeyCopy(2), maxActive, expiresAt)
	require.ErrorIs(t, err, chat.ErrTooManyInvites)
	got, err := s.GetInvite(ctx, expiring.ID)
	require.NoError(t, err)
	require.Equal(t, chatpb.Invite_EXPIRED, got.StatusAt(expiresAt))

	// A used-up one frees its place from the join that takes its last use,
	// and not before.
	privateID := privateGroup(t, s, creator)
	single := createInvite(t, s, newInvite(privateID, creator, 2, now, nil), 1, now)
	_, _, err = s.CreateInvite(ctx, newInvite(privateID, creator, 0, now, nil), inviteKeyCopy(2), 1, now)
	require.ErrorIs(t, err, chat.ErrTooManyInvites)
	joinWithInvite(t, s, privateID, model.MustGenerateUserID(), single, inviteSecret(5), now)
	_, _, err = s.CreateInvite(ctx, newInvite(privateID, creator, 0, now, nil), inviteKeyCopy(2), 1, now)
	require.ErrorIs(t, err, chat.ErrTooManyInvites)
	joinWithInvite(t, s, privateID, model.MustGenerateUserID(), single, inviteSecret(5), now)
	createInviteEventually(t, s, newInvite(privateID, creator, 0, now, nil), 1, now)
}

// testStore_Invite_CreateConcurrent pins that concurrent creates of one ID
// agree: exactly one creates it, and every caller is told the invite stored.
func testStore_Invite_CreateConcurrent(t *testing.T, s chat.Store) {
	ctx := context.Background()
	now := time.Now()
	groupID := chat.MustGenerateGroupChatID()
	creator := model.MustGenerateUserID()
	id := newInvite(groupID, creator, 0, now, nil).ID

	const writers = 8
	results := make([]chat.Invite, writers)
	createdBy := make([]bool, writers)
	errs := make([]error, writers)
	var wg sync.WaitGroup
	for i := 0; i < writers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			invite := newInvite(groupID, creator, uint32(i), now, nil)
			invite.ID = id
			results[i], createdBy[i], errs[i] = s.CreateInvite(ctx, invite, inviteKeyCopy(byte(i)), chat.MaxActiveInvites, now)
		}()
	}
	wg.Wait()

	got, err := s.GetInvite(ctx, id)
	require.NoError(t, err)
	created := 0
	for i := 0; i < writers; i++ {
		require.NoError(t, errs[i])
		requireInviteEqual(t, got, results[i])
		if createdBy[i] {
			created++
		}
	}
	require.Equal(t, 1, created)
}

// testStore_Invite_Page pins the listing: a chat's invites whatever their
// status, most recently created first with ties to the greater ID, paged
// strictly after a position without repeats or gaps, and one creator's alone
// in the same order when asked; another chat's never appear.
func testStore_Invite_Page(t *testing.T, s chat.Store) {
	ctx := context.Background()
	now := time.Now()
	groupID := chat.MustGenerateGroupChatID()
	alice := model.MustGenerateUserID()
	bob := model.MustGenerateUserID()

	page, err := s.GetChatInvitesPage(ctx, groupID, nil, nil, 0)
	require.NoError(t, err)
	require.Empty(t, page)

	// Seven invites, two of them created at the same instant, by two
	// creators; a revoked one is listed like any other.
	var invites []chat.Invite
	for i, creator := range []*commonpb.UserId{alice, bob, alice, alice, bob, alice, bob} {
		createdAt := now.Add(time.Duration(i) * time.Second)
		if i == 4 {
			createdAt = now.Add(3 * time.Second)
		}
		invites = append(invites, createInvite(t, s, newInvite(groupID, creator, 0, createdAt, nil), chat.MaxActiveInvites, now))
	}
	revoked, _, err := s.RevokeInvite(ctx, groupID, invites[2].ID, now)
	require.NoError(t, err)
	invites[2] = revoked
	createInvite(t, s, newInvite(chat.MustGenerateGroupChatID(), alice, 0, now.Add(time.Hour), nil), chat.MaxActiveInvites, now)

	want := slices.Clone(invites)
	slices.SortFunc(want, func(a, b chat.Invite) int { return a.Position().Compare(b.Position()) })
	whole := waitForInvites(t, s, groupID, nil, want...)
	for i := range want {
		requireInviteEqual(t, want[i], whole[i])
		if i > 0 {
			require.Negative(t, whole[i-1].Position().Compare(whole[i].Position()))
		}
	}
	require.True(t, whole[0].CreatedAt.After(whole[len(whole)-1].CreatedAt))

	walk := func(createdBy *commonpb.UserId, limit int) []chat.Invite {
		t.Helper()
		var walked []chat.Invite
		var after *chat.InvitePosition
		for {
			page, err := s.GetChatInvitesPage(ctx, groupID, createdBy, after, limit)
			require.NoError(t, err)
			require.LessOrEqual(t, len(page), limit)
			walked = append(walked, page...)
			if len(page) < limit {
				return walked
			}
			pos := page[len(page)-1].Position()
			after = &pos
		}
	}

	// Pages of two walk the same order and stop at the end.
	require.Equal(t, inviteIDValues(want), inviteIDValues(walk(nil, 2)))

	// One creator's invites alone, in the same order, paged too.
	for _, creator := range []*commonpb.UserId{alice, bob} {
		var theirs []chat.Invite
		for _, invite := range want {
			if bytes.Equal(invite.CreatedBy.Value, creator.Value) {
				theirs = append(theirs, invite)
			}
		}
		waitForInvites(t, s, groupID, creator, theirs...)
		require.Equal(t, inviteIDValues(theirs), inviteIDValues(walk(creator, 1)))
		require.Equal(t, inviteIDValues(theirs), inviteIDValues(walk(creator, 2)))
	}
	page, err = s.GetChatInvitesPage(ctx, groupID, model.MustGenerateUserID(), nil, 0)
	require.NoError(t, err)
	require.Empty(t, page)

	// Resuming from the last position reads nothing more, either way.
	last := want[len(want)-1].Position()
	for _, createdBy := range []*commonpb.UserId{nil, alice, bob} {
		page, err = s.GetChatInvitesPage(ctx, groupID, createdBy, &last, 10)
		require.NoError(t, err)
		require.Empty(t, page)
	}

	// Groups only, a creator is a user ID, and a position names an invite.
	_, err = s.GetChatInvitesPage(ctx, generateDmChatID(), nil, nil, 0)
	require.Error(t, err)
	_, err = s.GetChatInvitesPage(ctx, groupID, &commonpb.UserId{Value: randomBytes(8)}, nil, 0)
	require.Error(t, err)
	_, err = s.GetChatInvitesPage(ctx, groupID, nil, &chat.InvitePosition{CreatedAt: now, InviteID: &chatpb.InviteId{Value: randomBytes(4)}}, 0)
	require.Error(t, err)
}

// testStore_Invite_Revoke pins a revoke: recorded once, at its time, on an
// invite of the chat named, whatever its status; a repeat the no-op that
// keeps the first time; and an unknown invite, or another chat's, not found.
func testStore_Invite_Revoke(t *testing.T, s chat.Store) {
	ctx := context.Background()
	now := time.Now()
	groupID := chat.MustGenerateGroupChatID()
	creator := model.MustGenerateUserID()

	invite := createInvite(t, s, newInvite(groupID, creator, 2, now, nil), chat.MaxActiveInvites, now)

	// Another chat's invite, or none, is not found, and nothing is written.
	_, _, err := s.RevokeInvite(ctx, chat.MustGenerateGroupChatID(), invite.ID, now)
	require.ErrorIs(t, err, chat.ErrInviteNotFound)
	_, _, err = s.RevokeInvite(ctx, groupID, newInvite(groupID, creator, 0, now, nil).ID, now)
	require.ErrorIs(t, err, chat.ErrInviteNotFound)
	got, err := s.GetInvite(ctx, invite.ID)
	require.NoError(t, err)
	require.Nil(t, got.RevokedAt)

	revokedAt := now.Add(time.Minute + 5*time.Nanosecond)
	revoked, changed, err := s.RevokeInvite(ctx, groupID, invite.ID, revokedAt)
	require.NoError(t, err)
	require.True(t, changed)
	want := invite
	want.RevokedAt = &revokedAt
	requireInviteEqual(t, want, revoked)
	require.Equal(t, chatpb.Invite_REVOKED, revoked.StatusAt(revokedAt))
	got, err = s.GetInvite(ctx, invite.ID)
	require.NoError(t, err)
	requireInviteEqual(t, want, got)

	// A repeat keeps the first revoke.
	again, changed, err := s.RevokeInvite(ctx, groupID, invite.ID, revokedAt.Add(time.Hour))
	require.NoError(t, err)
	require.False(t, changed)
	requireInviteEqual(t, want, again)

	// An expired invite is revoked like any other, and reads REVOKED, the
	// first of the reasons, from then on.
	expiresAt := now.Add(-time.Hour)
	expired := createInvite(t, s, newInvite(groupID, creator, 0, now.Add(-2*time.Hour), &expiresAt), chat.MaxActiveInvites, now)
	require.Equal(t, chatpb.Invite_EXPIRED, expired.StatusAt(now))
	revoked, changed, err = s.RevokeInvite(ctx, groupID, expired.ID, now)
	require.NoError(t, err)
	require.True(t, changed)
	require.Equal(t, chatpb.Invite_REVOKED, revoked.StatusAt(now))
	require.True(t, revoked.RevokedAt.Equal(now))

	// Groups only.
	_, _, err = s.RevokeInvite(ctx, generateDmChatID(), invite.ID, now)
	require.Error(t, err)
}

// testStore_KeyEnvelope_InviteCopy pins that an invite's copy is a key
// envelope like any other: stored and read back whole, replaced by an
// envelope the member wraps for themself, which reads back with no copy, and
// discarded with a departure.
func testStore_KeyEnvelope_InviteCopy(t *testing.T, s chat.Store) {
	ctx := context.Background()
	now := time.Now()
	creator := model.MustGenerateUserID()
	joiner := model.MustGenerateUserID()
	leaver := model.MustGenerateUserID()
	groupID := privateGroup(t, s, creator)
	_, _, err := s.AddGroupMembers(ctx, groupID, []*commonpb.UserId{joiner, leaver})
	require.NoError(t, err)
	invite := createInvite(t, s, newInvite(groupID, creator, 0, now, nil), chat.MaxActiveInvites, now)

	copied := chat.NewInviteKeyEnvelope(invite, inviteKeyCopy(3), inviteSecret(4))
	stored, err := s.SetKeyEnvelope(ctx, groupID, joiner, copied)
	require.NoError(t, err)
	require.True(t, copied.Equal(stored))
	got, err := s.GetKeyEnvelope(ctx, groupID, joiner)
	require.NoError(t, err)
	require.True(t, copied.Equal(got))
	require.True(t, got.IsInviteCopy())
	require.Equal(t, creator.Value, got.WrappedBy.Value)
	require.Equal(t, invite.ID.Value, got.InviteCopy.InviteID.Value)
	require.True(t, proto.Equal(inviteKeyCopy(3), got.InviteCopy.Envelope))
	require.True(t, proto.Equal(inviteSecret(4), got.InviteCopy.SecretEnvelope))

	// The joiner's own envelope replaces the copy, which leaves nothing of
	// itself behind.
	own := keyEnvelope(5, joiner)
	stored, err = s.SetKeyEnvelope(ctx, groupID, joiner, own)
	require.NoError(t, err)
	require.True(t, own.Equal(stored))
	got, err = s.GetKeyEnvelope(ctx, groupID, joiner)
	require.NoError(t, err)
	require.True(t, own.Equal(got))
	require.False(t, got.IsInviteCopy())
	require.Nil(t, got.InviteCopy)

	// A copy goes with a departure that discards the envelope.
	_, err = s.SetKeyEnvelope(ctx, groupID, leaver, chat.NewInviteKeyEnvelope(invite, inviteKeyCopy(6), inviteSecret(7)))
	require.NoError(t, err)
	changed, _, err := s.RemoveGroupMember(ctx, groupID, leaver, true)
	require.NoError(t, err)
	require.True(t, changed)
	_, err = s.GetKeyEnvelope(ctx, groupID, leaver)
	require.ErrorIs(t, err, chat.ErrKeyEnvelopeNotFound)

	// A copy missing a part is refused.
	incomplete := chat.NewInviteKeyEnvelope(invite, nil, inviteSecret(8))
	_, err = s.SetKeyEnvelope(ctx, groupID, leaver, incomplete)
	require.Error(t, err)
	_, err = s.GetKeyEnvelope(ctx, groupID, leaver)
	require.ErrorIs(t, err, chat.ErrKeyEnvelopeNotFound)
}

// joinWithInvite joins user to chatID with invite as of now, requiring that
// the join is made, and returns what it did.
func joinWithInvite(t *testing.T, s chat.Store, chatID *commonpb.ChatId, user *commonpb.UserId, invite chat.Invite, secret *chatpb.InviteSecretEnvelope, now time.Time) chat.JoinWithInviteResult {
	t.Helper()
	result, err := s.JoinWithInvite(context.Background(), chatID, user, invite.ID, secret, now)
	require.NoError(t, err)
	require.True(t, result.Changed)
	return result
}

// requireUseCount checks the invite's stored use count.
func requireUseCount(t *testing.T, s chat.Store, invite chat.Invite, want uint32) {
	t.Helper()
	got, err := s.GetInvite(context.Background(), invite.ID)
	require.NoError(t, err)
	require.Equal(t, want, got.UseCount)
}

// requireNotJoined checks that a join refused for user wrote nothing of
// theirs: no membership and no key envelope.
func requireNotJoined(t *testing.T, s chat.Store, chatID *commonpb.ChatId, user *commonpb.UserId) {
	t.Helper()
	isMember, err := s.IsMember(context.Background(), chatID, user)
	require.NoError(t, err)
	require.False(t, isMember)
	_, err = s.GetKeyEnvelope(context.Background(), chatID, user)
	require.ErrorIs(t, err, chat.ErrKeyEnvelopeNotFound)
}

// testStore_InviteJoin_Join pins a join with an invite: in one write the
// user becomes a member at the roster's next version, the invite counts one
// use, and the user holds the invite's copy with their own secret envelope,
// wrapped by the invite's creator, over any envelope they held; and a call
// of the wrong shape, or for a group that does not exist, writes nothing.
func testStore_InviteJoin_Join(t *testing.T, s chat.Store) {
	ctx := context.Background()
	now := time.Now()
	creator := model.MustGenerateUserID()
	user := model.MustGenerateUserID()
	groupID := privateGroup(t, s, creator)
	invite := createInvite(t, s, newInvite(groupID, creator, 0, now, nil), chat.MaxActiveInvites, now)

	// Calls of the wrong shape.
	for name, call := range map[string]func() error{
		"dm chat": func() error {
			_, err := s.JoinWithInvite(ctx, generateDmChatID(), user, invite.ID, inviteSecret(5), now)
			return err
		},
		"short user": func() error {
			_, err := s.JoinWithInvite(ctx, groupID, &commonpb.UserId{Value: randomBytes(4)}, invite.ID, inviteSecret(5), now)
			return err
		},
		"short invite": func() error {
			_, err := s.JoinWithInvite(ctx, groupID, user, &chatpb.InviteId{Value: randomBytes(4)}, inviteSecret(5), now)
			return err
		},
		"no secret": func() error {
			_, err := s.JoinWithInvite(ctx, groupID, user, invite.ID, nil, now)
			return err
		},
		"before epoch": func() error {
			_, err := s.JoinWithInvite(ctx, groupID, user, invite.ID, inviteSecret(5), time.Unix(-1, 0))
			return err
		},
	} {
		require.Error(t, call(), name)
	}

	// A group that does not exist, though the invite names it.
	missing := chat.MustGenerateGroupChatID()
	orphan := createInvite(t, s, newInvite(missing, creator, 0, now, nil), chat.MaxActiveInvites, now)
	_, err := s.JoinWithInvite(ctx, missing, user, orphan.ID, inviteSecret(5), now)
	require.ErrorIs(t, err, chat.ErrChatNotFound)
	requireNotJoined(t, s, groupID, user)
	requireUseCount(t, s, invite, 0)

	// A stale envelope of their own, from an earlier stay, is replaced.
	_, err = s.SetKeyEnvelope(ctx, groupID, user, keyEnvelope(9, user))
	require.NoError(t, err)

	result := joinWithInvite(t, s, groupID, user, invite, inviteSecret(5), now)
	require.Equal(t, chat.RosterSummary{MemberCount: 2, Version: 1}, result.Roster)
	require.False(t, result.LeftLobby)
	want := invite
	want.UseCount = 1
	requireInviteEqual(t, want, result.Invite)
	requireUseCount(t, s, invite, 1)
	requireRosterSummary(t, s, groupID, 2, 1)
	isMember, err := s.IsMember(ctx, groupID, user)
	require.NoError(t, err)
	require.True(t, isMember)
	records, err := s.GetGroupMemberRecords(ctx, user, []*commonpb.ChatId{groupID})
	require.NoError(t, err)
	require.Equal(t, uint64(1), records[string(groupID.Value)].Version)

	got, err := s.GetKeyEnvelope(ctx, groupID, user)
	require.NoError(t, err)
	require.True(t, chat.NewInviteKeyEnvelope(invite, inviteKeyCopy(2), inviteSecret(5)).Equal(got))
	require.True(t, got.IsInviteCopy())
	require.Equal(t, creator.Value, got.WrappedBy.Value)
	require.Equal(t, invite.ID.Value, got.InviteCopy.InviteID.Value)

	// The result is the caller's to change.
	result.Invite.ChatID.Value[0] ^= 0xff
	requireUseCount(t, s, invite, 1)
	stored, err := s.GetInvite(ctx, invite.ID)
	require.NoError(t, err)
	require.Equal(t, groupID.Value, stored.ChatID.Value)
}

// testStore_InviteJoin_AlreadyMember pins that a member's join is the no-op
// whatever the invite's status: nothing written, no use counted, their
// envelope untouched, and the invite and roster as read, so a retry of a
// join that took the last use is the no-op and not ErrInviteExhausted.
func testStore_InviteJoin_AlreadyMember(t *testing.T, s chat.Store) {
	ctx := context.Background()
	now := time.Now()
	creator := model.MustGenerateUserID()
	user := model.MustGenerateUserID()
	groupID := privateGroup(t, s, creator)
	invite := createInvite(t, s, newInvite(groupID, creator, 0, now, nil), chat.MaxActiveInvites, now)

	joinWithInvite(t, s, groupID, user, invite, inviteSecret(5), now)
	own := keyEnvelope(6, user)
	_, err := s.SetKeyEnvelope(ctx, groupID, user, own)
	require.NoError(t, err)

	result, err := s.JoinWithInvite(ctx, groupID, user, invite.ID, inviteSecret(7), now)
	require.NoError(t, err)
	require.False(t, result.Changed)
	require.False(t, result.LeftLobby)
	require.Equal(t, chat.RosterSummary{MemberCount: 2, Version: 1}, result.Roster)
	want := invite
	want.UseCount = 1
	requireInviteEqual(t, want, result.Invite)
	requireUseCount(t, s, invite, 1)
	requireRosterSummary(t, s, groupID, 2, 1)
	got, err := s.GetKeyEnvelope(ctx, groupID, user)
	require.NoError(t, err)
	require.True(t, own.Equal(got))

	// The creator, a member from creation, too.
	result, err = s.JoinWithInvite(ctx, groupID, creator, invite.ID, inviteSecret(7), now)
	require.NoError(t, err)
	require.False(t, result.Changed)
	requireUseCount(t, s, invite, 1)
	_, err = s.GetKeyEnvelope(ctx, groupID, creator)
	require.ErrorIs(t, err, chat.ErrKeyEnvelopeNotFound)

	// A retry of the join that took the last use, and a member's join with
	// an invite revoked since: the no-op either way.
	single := createInvite(t, s, newInvite(groupID, creator, 1, now, nil), chat.MaxActiveInvites, now)
	other := model.MustGenerateUserID()
	result = joinWithInvite(t, s, groupID, other, single, inviteSecret(5), now)
	require.Equal(t, chatpb.Invite_EXHAUSTED, result.Invite.StatusAt(now))
	result, err = s.JoinWithInvite(ctx, groupID, other, single.ID, inviteSecret(5), now)
	require.NoError(t, err)
	require.False(t, result.Changed)
	require.EqualValues(t, 1, result.Invite.UseCount)
	_, _, err = s.RevokeInvite(ctx, groupID, invite.ID, now)
	require.NoError(t, err)
	result, err = s.JoinWithInvite(ctx, groupID, user, invite.ID, inviteSecret(7), now)
	require.NoError(t, err)
	require.False(t, result.Changed)
	require.Equal(t, chatpb.Invite_REVOKED, result.Invite.StatusAt(now))
	requireRosterSummary(t, s, groupID, 3, 2)

	// A member's join with an invite that is not the chat's is still not
	// found: the result has no invite to carry.
	_, err = s.JoinWithInvite(ctx, groupID, user, newInvite(groupID, creator, 0, now, nil).ID, inviteSecret(7), now)
	require.ErrorIs(t, err, chat.ErrInviteNotFound)
}

// testStore_InviteJoin_OwnInvite pins that the invite's creator never joins
// with it once they are not a member: the join would put the invite's copy,
// wrapped by them, over the envelope they hold, the group's key for its
// creator. Nothing is written, whatever the invite's status, while a member
// creator's join is still the no-op.
func testStore_InviteJoin_OwnInvite(t *testing.T, s chat.Store) {
	ctx := context.Background()
	now := time.Now()
	creator := model.MustGenerateUserID()
	groupID := privateGroup(t, s, creator)
	own := keyEnvelope(6, creator)
	_, err := s.SetKeyEnvelope(ctx, groupID, creator, own)
	require.NoError(t, err)
	invite := createInvite(t, s, newInvite(groupID, creator, 0, now, nil), chat.MaxActiveInvites, now)

	result, err := s.JoinWithInvite(ctx, groupID, creator, invite.ID, inviteSecret(5), now)
	require.NoError(t, err)
	require.False(t, result.Changed)

	changed, _, err := s.RemoveGroupMember(ctx, groupID, creator, false)
	require.NoError(t, err)
	require.True(t, changed)
	_, err = s.JoinWithInvite(ctx, groupID, creator, invite.ID, inviteSecret(5), now)
	require.ErrorIs(t, err, chat.ErrOwnInvite)
	isMember, err := s.IsMember(ctx, groupID, creator)
	require.NoError(t, err)
	require.False(t, isMember)
	requireUseCount(t, s, invite, 0)
	got, err := s.GetKeyEnvelope(ctx, groupID, creator)
	require.NoError(t, err)
	require.True(t, own.Equal(got))

	// Judged before the invite's status: a revoked one is refused the same.
	_, _, err = s.RevokeInvite(ctx, groupID, invite.ID, now)
	require.NoError(t, err)
	_, err = s.JoinWithInvite(ctx, groupID, creator, invite.ID, inviteSecret(5), now)
	require.ErrorIs(t, err, chat.ErrOwnInvite)
}

// testStore_InviteJoin_Rejoin pins that every join counts: a member who
// leaves, giving no use back, and joins again with the same invite uses
// another, and holds the copy again, the departure having discarded it.
func testStore_InviteJoin_Rejoin(t *testing.T, s chat.Store) {
	ctx := context.Background()
	now := time.Now()
	creator := model.MustGenerateUserID()
	user := model.MustGenerateUserID()
	groupID := privateGroup(t, s, creator)
	invite := createInvite(t, s, newInvite(groupID, creator, 5, now, nil), chat.MaxActiveInvites, now)

	joinWithInvite(t, s, groupID, user, invite, inviteSecret(5), now)
	changed, _, err := s.RemoveGroupMember(ctx, groupID, user, true)
	require.NoError(t, err)
	require.True(t, changed)
	_, err = s.GetKeyEnvelope(ctx, groupID, user)
	require.ErrorIs(t, err, chat.ErrKeyEnvelopeNotFound)
	requireUseCount(t, s, invite, 1)

	result := joinWithInvite(t, s, groupID, user, invite, inviteSecret(6), now)
	require.Equal(t, chat.RosterSummary{MemberCount: 2, Version: 3}, result.Roster)
	require.EqualValues(t, 2, result.Invite.UseCount)
	requireUseCount(t, s, invite, 2)
	got, err := s.GetKeyEnvelope(ctx, groupID, user)
	require.NoError(t, err)
	require.True(t, chat.NewInviteKeyEnvelope(invite, inviteKeyCopy(2), inviteSecret(6)).Equal(got))
}

// testStore_InviteJoin_Limits pins MaxUses: the join that takes the last
// use leaves the invite EXHAUSTED and out of the chat's ACTIVE count, and
// every join after it is ErrInviteExhausted with nothing written, a return
// by one of its own joiners included; an invite without a limit never runs
// out.
func testStore_InviteJoin_Limits(t *testing.T, s chat.Store) {
	ctx := context.Background()
	now := time.Now()
	creator := model.MustGenerateUserID()
	first, second, third := model.MustGenerateUserID(), model.MustGenerateUserID(), model.MustGenerateUserID()
	groupID := privateGroup(t, s, creator)
	invite := createInvite(t, s, newInvite(groupID, creator, 2, now, nil), 1, now)

	// While it is ACTIVE, it fills a cap of one.
	_, _, err := s.CreateInvite(ctx, newInvite(groupID, creator, 0, now, nil), inviteKeyCopy(2), 1, now)
	require.ErrorIs(t, err, chat.ErrTooManyInvites)

	result := joinWithInvite(t, s, groupID, first, invite, inviteSecret(5), now)
	require.Equal(t, chatpb.Invite_ACTIVE, result.Invite.StatusAt(now))
	require.True(t, result.Invite.IsLastUse())
	result = joinWithInvite(t, s, groupID, second, invite, inviteSecret(5), now)
	require.EqualValues(t, 2, result.Invite.UseCount)
	require.Equal(t, chatpb.Invite_EXHAUSTED, result.Invite.StatusAt(now))
	require.Equal(t, chat.RosterSummary{MemberCount: 3, Version: 2}, result.Roster)
	got, err := s.GetKeyEnvelope(ctx, groupID, second)
	require.NoError(t, err)
	require.True(t, got.IsInviteCopy())

	// Used up, it no longer counts toward the cap.
	createInviteEventually(t, s, newInvite(groupID, creator, 0, now, nil), 1, now)

	_, err = s.JoinWithInvite(ctx, groupID, third, invite.ID, inviteSecret(5), now)
	require.ErrorIs(t, err, chat.ErrInviteExhausted)
	requireNotJoined(t, s, groupID, third)
	requireUseCount(t, s, invite, 2)

	// A joiner who leaves gives no use back, and cannot return with it.
	changed, _, err := s.RemoveGroupMember(ctx, groupID, first, true)
	require.NoError(t, err)
	require.True(t, changed)
	_, err = s.JoinWithInvite(ctx, groupID, first, invite.ID, inviteSecret(5), now)
	require.ErrorIs(t, err, chat.ErrInviteExhausted)
	requireNotJoined(t, s, groupID, first)
	requireUseCount(t, s, invite, 2)
	requireRosterSummary(t, s, groupID, 2, 3)

	// Without a limit, many joins never use it up.
	unlimited := createInvite(t, s, newInvite(groupID, creator, 0, now, nil), chat.MaxActiveInvites, now)
	for i := range 5 {
		result := joinWithInvite(t, s, groupID, model.MustGenerateUserID(), unlimited, inviteSecret(5), now)
		require.EqualValues(t, i+1, result.Invite.UseCount)
		require.Equal(t, chatpb.Invite_ACTIVE, result.Invite.StatusAt(now))
		require.False(t, result.Invite.IsLastUse())
	}
	requireUseCount(t, s, unlimited, 5)
}

// testStore_InviteJoin_Status pins that an invite that is not ACTIVE admits
// no one, with nothing written, each refusal naming the first reason, and
// that an unknown invite, or another chat's, is not found.
func testStore_InviteJoin_Status(t *testing.T, s chat.Store) {
	ctx := context.Background()
	now := time.Now()
	creator := model.MustGenerateUserID()
	user := model.MustGenerateUserID()
	groupID := privateGroup(t, s, creator)

	refused := func(invite chat.Invite, at time.Time, want error) {
		t.Helper()
		_, err := s.JoinWithInvite(ctx, groupID, user, invite.ID, inviteSecret(5), at)
		require.ErrorIs(t, err, want)
		requireNotJoined(t, s, groupID, user)
		requireUseCount(t, s, invite, invite.UseCount)
	}

	// Expired: from the instant it expires.
	expiresAt := now.Add(-time.Hour)
	expired := createInvite(t, s, newInvite(groupID, creator, 0, now.Add(-2*time.Hour), &expiresAt), chat.MaxActiveInvites, now)
	refused(expired, now, chat.ErrInviteExpired)
	refused(expired, expiresAt, chat.ErrInviteExpired)
	expiresSoon := now.Add(time.Hour)
	expiring := createInvite(t, s, newInvite(groupID, creator, 0, now, &expiresSoon), chat.MaxActiveInvites, now)
	refused(expiring, expiresSoon, chat.ErrInviteExpired)

	// Revoked, and revoked and expired: revoked.
	revoked := createInvite(t, s, newInvite(groupID, creator, 0, now, nil), chat.MaxActiveInvites, now)
	_, _, err := s.RevokeInvite(ctx, groupID, revoked.ID, now)
	require.NoError(t, err)
	refused(revoked, now, chat.ErrInviteRevoked)
	_, _, err = s.RevokeInvite(ctx, groupID, expired.ID, now)
	require.NoError(t, err)
	refused(expired, now, chat.ErrInviteRevoked)

	// Unknown, and another chat's.
	_, err = s.JoinWithInvite(ctx, groupID, user, newInvite(groupID, creator, 0, now, nil).ID, inviteSecret(5), now)
	require.ErrorIs(t, err, chat.ErrInviteNotFound)
	requireNotJoined(t, s, groupID, user)
	otherGroup := privateGroup(t, s, creator)
	elsewhere := createInvite(t, s, newInvite(otherGroup, creator, 0, now, nil), chat.MaxActiveInvites, now)
	refused(elsewhere, now, chat.ErrInviteNotFound)
	isMember, err := s.IsMember(ctx, otherGroup, user)
	require.NoError(t, err)
	require.False(t, isMember)

	// The expiring invite still admits just before it expires.
	result := joinWithInvite(t, s, groupID, user, expiring, inviteSecret(5), expiresSoon.Add(-time.Nanosecond))
	require.EqualValues(t, 1, result.Invite.UseCount)
	requireRosterSummary(t, s, groupID, 2, 1)
}

// testStore_InviteJoin_Banned pins that a banned user is refused in the
// write with nothing written, and that the invite is judged first: a banned
// user holding a link that admits no one is told of the link.
func testStore_InviteJoin_Banned(t *testing.T, s chat.Store) {
	ctx := context.Background()
	now := time.Now()
	creator := model.MustGenerateUserID()
	user := model.MustGenerateUserID()
	groupID := privateGroup(t, s, creator)
	invite := createInvite(t, s, newInvite(groupID, creator, 1, now, nil), chat.MaxActiveInvites, now)
	_, err := s.BanUser(ctx, groupID, user, now, true)
	require.NoError(t, err)

	_, err = s.JoinWithInvite(ctx, groupID, user, invite.ID, inviteSecret(5), now)
	require.ErrorIs(t, err, chat.ErrBanned)
	requireNotJoined(t, s, groupID, user)
	requireUseCount(t, s, invite, 0)
	requireRosterSummary(t, s, groupID, 1, 0)

	// The refusal used nothing: the one use is still there for someone else.
	joinWithInvite(t, s, groupID, model.MustGenerateUserID(), invite, inviteSecret(5), now)

	revoked := createInvite(t, s, newInvite(groupID, creator, 0, now, nil), chat.MaxActiveInvites, now)
	_, _, err = s.RevokeInvite(ctx, groupID, revoked.ID, now)
	require.NoError(t, err)
	_, err = s.JoinWithInvite(ctx, groupID, user, revoked.ID, inviteSecret(5), now)
	require.ErrorIs(t, err, chat.ErrInviteRevoked)
	_, err = s.JoinWithInvite(ctx, groupID, user, invite.ID, inviteSecret(5), now)
	require.ErrorIs(t, err, chat.ErrInviteExhausted)

	// Unbanned, they join like anyone.
	_, _, err = s.UnbanUser(ctx, groupID, user)
	require.NoError(t, err)
	open := createInvite(t, s, newInvite(groupID, creator, 0, now, nil), chat.MaxActiveInvites, now)
	joinWithInvite(t, s, groupID, user, open, inviteSecret(5), now)
}

// testStore_InviteJoin_Lobby pins that a join with an invite takes the
// joiner out of the group's lobby, with both counts, in the same write, and
// reports it; a joiner not waiting leaves no lobby behind.
func testStore_InviteJoin_Lobby(t *testing.T, s chat.Store) {
	ctx := context.Background()
	now := time.Now()
	creator := model.MustGenerateUserID()
	waiting := model.MustGenerateUserID()
	groupID := privateGroup(t, s, creator)
	invite := createInvite(t, s, newInvite(groupID, creator, 0, now, nil), chat.MaxActiveInvites, now)
	tight := chat.LobbyLimits{LobbySize: 1, LobbiesPerUser: 1}

	_, changed, err := s.EnterLobby(ctx, groupID, waiting, tight)
	require.NoError(t, err)
	require.True(t, changed)
	waitForLobbyPage(t, s, groupID, 1)

	result := joinWithInvite(t, s, groupID, waiting, invite, inviteSecret(5), now)
	require.True(t, result.LeftLobby)
	entries, err := s.GetLobbyEntries(ctx, waiting, []*commonpb.ChatId{groupID})
	require.NoError(t, err)
	require.Empty(t, entries)
	waitForLobbyPage(t, s, groupID, 0)

	// Both counts were freed: the lobby's one place takes someone else, and
	// the joiner's one lobby is free for another group's.
	_, changed, err = s.EnterLobby(ctx, groupID, model.MustGenerateUserID(), tight)
	require.NoError(t, err)
	require.True(t, changed)
	elsewhere := chat.MustGenerateGroupChatID()
	_, changed, err = s.EnterLobby(ctx, elsewhere, waiting, tight)
	require.NoError(t, err)
	require.True(t, changed)

	// Waiting elsewhere is not waiting here.
	otherGroup := privateGroup(t, s, creator)
	otherInvite := createInvite(t, s, newInvite(otherGroup, creator, 0, now, nil), chat.MaxActiveInvites, now)
	result = joinWithInvite(t, s, otherGroup, waiting, otherInvite, inviteSecret(5), now)
	require.False(t, result.LeftLobby)
	entries, err = s.GetLobbyEntries(ctx, waiting, []*commonpb.ChatId{elsewhere, otherGroup})
	require.NoError(t, err)
	require.Len(t, entries, 1)
	require.Contains(t, entries, string(elsewhere.Value))

	// Not waiting at all.
	result = joinWithInvite(t, s, groupID, model.MustGenerateUserID(), invite, inviteSecret(5), now)
	require.False(t, result.LeftLobby)
}

// testStore_InviteJoin_Envelope pins that the copy a join stores is an
// envelope like any other: the joiner's own replaces it, and a departure or
// a ban that discards the envelope takes it.
func testStore_InviteJoin_Envelope(t *testing.T, s chat.Store) {
	ctx := context.Background()
	now := time.Now()
	creator := model.MustGenerateUserID()
	rewrapper, leaver, banned := model.MustGenerateUserID(), model.MustGenerateUserID(), model.MustGenerateUserID()
	groupID := privateGroup(t, s, creator)
	invite := createInvite(t, s, newInvite(groupID, creator, 0, now, nil), chat.MaxActiveInvites, now)
	for _, user := range []*commonpb.UserId{rewrapper, leaver, banned} {
		joinWithInvite(t, s, groupID, user, invite, inviteSecret(5), now)
	}

	own := keyEnvelope(6, rewrapper)
	stored, err := s.SetKeyEnvelope(ctx, groupID, rewrapper, own)
	require.NoError(t, err)
	require.True(t, own.Equal(stored))
	got, err := s.GetKeyEnvelope(ctx, groupID, rewrapper)
	require.NoError(t, err)
	require.True(t, own.Equal(got))
	require.Nil(t, got.InviteCopy)

	changed, _, err := s.RemoveGroupMember(ctx, groupID, leaver, true)
	require.NoError(t, err)
	require.True(t, changed)
	_, err = s.GetKeyEnvelope(ctx, groupID, leaver)
	require.ErrorIs(t, err, chat.ErrKeyEnvelopeNotFound)

	ban, err := s.BanUser(ctx, groupID, banned, now, true)
	require.NoError(t, err)
	require.True(t, ban.Departed)
	_, err = s.GetKeyEnvelope(ctx, groupID, banned)
	require.ErrorIs(t, err, chat.ErrKeyEnvelopeNotFound)
}

// testStore_InviteJoin_Revoke pins that a revoke lands on an invite that
// has been taking joins, keeping their count, and that it admits no one
// after.
func testStore_InviteJoin_Revoke(t *testing.T, s chat.Store) {
	ctx := context.Background()
	now := time.Now()
	creator := model.MustGenerateUserID()
	groupID := privateGroup(t, s, creator)
	invite := createInvite(t, s, newInvite(groupID, creator, 5, now, nil), chat.MaxActiveInvites, now)
	for range 2 {
		joinWithInvite(t, s, groupID, model.MustGenerateUserID(), invite, inviteSecret(5), now)
	}

	revoked, changed, err := s.RevokeInvite(ctx, groupID, invite.ID, now)
	require.NoError(t, err)
	require.True(t, changed)
	require.EqualValues(t, 2, revoked.UseCount)

	user := model.MustGenerateUserID()
	_, err = s.JoinWithInvite(ctx, groupID, user, invite.ID, inviteSecret(5), now)
	require.ErrorIs(t, err, chat.ErrInviteRevoked)
	requireNotJoined(t, s, groupID, user)
	requireUseCount(t, s, invite, 2)
	requireRosterSummary(t, s, groupID, 3, 2)
}

// testStore_InviteJoin_Concurrent pins that an invite never admits more
// than MaxUses joins, however many race for it: of many users joining at
// once, exactly MaxUses join, every other is ErrInviteExhausted, and the
// count and the roster agree.
func testStore_InviteJoin_Concurrent(t *testing.T, s chat.Store) {
	ctx := context.Background()
	now := time.Now()
	creator := model.MustGenerateUserID()

	for _, maxUses := range []uint32{1, 3} {
		groupID := privateGroup(t, s, creator)
		invite := createInvite(t, s, newInvite(groupID, creator, maxUses, now, nil), chat.MaxActiveInvites, now)

		const joiners = 8
		errs := make([]error, joiners)
		var wg sync.WaitGroup
		for i := range joiners {
			wg.Go(func() {
				_, errs[i] = s.JoinWithInvite(ctx, groupID, model.MustGenerateUserID(), invite.ID, inviteSecret(5), now)
			})
		}
		wg.Wait()

		var joined, exhausted int
		for _, err := range errs {
			switch {
			case err == nil:
				joined++
			case errors.Is(err, chat.ErrInviteExhausted):
				exhausted++
			default:
				require.NoError(t, err)
			}
		}
		require.EqualValues(t, maxUses, joined, "max uses %d", maxUses)
		require.Equal(t, joiners-int(maxUses), exhausted, "max uses %d", maxUses)
		requireUseCount(t, s, invite, maxUses)
		requireRosterSummary(t, s, groupID, 1+uint64(maxUses), uint64(maxUses))
	}
}

// inviteRaceRounds is how many times each race between a join with an
// invite and a lobby or ban write is run concurrently (see raceOrders).
const inviteRaceRounds = 8

// raceOrders runs round once per order of two writes a and b that round
// hands to its run: a then b, b then a, then both at once inviteRaceRounds
// times. The sequential orders pin both outcomes whatever the scheduler
// favours, since against one backend the same write tends to win every
// concurrent round; the concurrent rounds are what may catch one write
// between the other's read and its write. Each round sets up its own state.
func raceOrders(round func(run func(a, b func()))) {
	orders := []func(a, b func()){
		func(a, b func()) { a(); b() },
		func(a, b func()) { b(); a() },
	}
	for range inviteRaceRounds {
		orders = append(orders, func(a, b func()) {
			var wg sync.WaitGroup
			wg.Go(a)
			wg.Go(b)
			wg.Wait()
		})
	}
	for _, run := range orders {
		round(run)
	}
}

// requireLobbyFreed checks that user holds no entry in chatID's lobby and
// that neither count holds a place for one: the lobby's one place, under a
// LobbySize of 1, takes someone else, and the user's one lobby, under a
// LobbiesPerUser of 1, takes another group's. It is for a user who waited
// in no other lobby.
func requireLobbyFreed(t *testing.T, s chat.Store, chatID *commonpb.ChatId, user *commonpb.UserId) {
	t.Helper()
	ctx := context.Background()
	tight := chat.LobbyLimits{LobbySize: 1, LobbiesPerUser: 1}

	entries, err := s.GetLobbyEntries(ctx, user, []*commonpb.ChatId{chatID})
	require.NoError(t, err)
	require.Empty(t, entries)
	_, changed, err := s.EnterLobby(ctx, chatID, model.MustGenerateUserID(), tight)
	require.NoError(t, err)
	require.True(t, changed)
	_, changed, err = s.EnterLobby(ctx, chat.MustGenerateGroupChatID(), user, tight)
	require.NoError(t, err)
	require.True(t, changed)
}

// testStore_InviteJoin_RefusedKeepsLobby pins that a join with an invite
// that admits no one leaves a waiting user where they were: their entry and
// both counts untouched, so they keep their place, and a later join with an
// invite that admits them takes them out as any does.
func testStore_InviteJoin_RefusedKeepsLobby(t *testing.T, s chat.Store) {
	ctx := context.Background()
	now := time.Now()
	creator := model.MustGenerateUserID()
	user := model.MustGenerateUserID()
	groupID := privateGroup(t, s, creator)
	tight := chat.LobbyLimits{LobbySize: 1, LobbiesPerUser: 1}

	entry, changed, err := s.EnterLobby(ctx, groupID, user, tight)
	require.NoError(t, err)
	require.True(t, changed)

	expiresAt := now.Add(-time.Hour)
	expired := createInvite(t, s, newInvite(groupID, creator, 0, now.Add(-2*time.Hour), &expiresAt), chat.MaxActiveInvites, now)
	revoked := createInvite(t, s, newInvite(groupID, creator, 0, now, nil), chat.MaxActiveInvites, now)
	_, _, err = s.RevokeInvite(ctx, groupID, revoked.ID, now)
	require.NoError(t, err)
	usedUp := createInvite(t, s, newInvite(groupID, creator, 1, now, nil), chat.MaxActiveInvites, now)
	joinWithInvite(t, s, groupID, model.MustGenerateUserID(), usedUp, inviteSecret(5), now)
	elsewhere := createInvite(t, s, newInvite(privateGroup(t, s, creator), creator, 0, now, nil), chat.MaxActiveInvites, now)

	for _, refusal := range []struct {
		inviteID *chatpb.InviteId
		want     error
	}{
		{expired.ID, chat.ErrInviteExpired},
		{revoked.ID, chat.ErrInviteRevoked},
		{usedUp.ID, chat.ErrInviteExhausted},
		{elsewhere.ID, chat.ErrInviteNotFound},
		{newInvite(groupID, creator, 0, now, nil).ID, chat.ErrInviteNotFound},
	} {
		_, err := s.JoinWithInvite(ctx, groupID, user, refusal.inviteID, inviteSecret(5), now)
		require.ErrorIs(t, err, refusal.want)
		requireNotJoined(t, s, groupID, user)

		// Still waiting, at the first entry's time, and both places still
		// theirs: the lobby is full to anyone else, and they wait in no other.
		entries, err := s.GetLobbyEntries(ctx, user, []*commonpb.ChatId{groupID})
		require.NoError(t, err)
		require.Len(t, entries, 1)
		require.True(t, entry.EnteredAt.Equal(entries[string(groupID.Value)].EnteredAt))
		_, _, err = s.EnterLobby(ctx, groupID, model.MustGenerateUserID(), tight)
		require.ErrorIs(t, err, chat.ErrLobbyFull)
		_, _, err = s.EnterLobby(ctx, chat.MustGenerateGroupChatID(), user, tight)
		require.ErrorIs(t, err, chat.ErrTooManyLobbies)
	}

	// An invite that admits them takes them out with both counts.
	open := createInvite(t, s, newInvite(groupID, creator, 0, now, nil), chat.MaxActiveInvites, now)
	result := joinWithInvite(t, s, groupID, user, open, inviteSecret(5), now)
	require.True(t, result.LeftLobby)
	requireLobbyFreed(t, s, groupID, user)
}

// testStore_InviteJoin_RaceAdmit pins a waiting user's join with an invite
// racing the creator's admission of them: exactly one of the two joins them,
// the other is the no-op, and what stands is the winner's alone — the use
// counted only when the invite won, the envelope the winner stored, the entry
// and both counts gone, the roster moved once.
func testStore_InviteJoin_RaceAdmit(t *testing.T, s chat.Store) {
	ctx := context.Background()
	now := time.Now()
	creator := model.MustGenerateUserID()

	raceOrders(func(run func(a, b func())) {
		user := model.MustGenerateUserID()
		groupID := privateGroup(t, s, creator)
		invite := createInvite(t, s, newInvite(groupID, creator, 0, now, nil), chat.MaxActiveInvites, now)
		_, changed, err := s.EnterLobby(ctx, groupID, user, chat.LobbyLimits{LobbySize: 1, LobbiesPerUser: 1})
		require.NoError(t, err)
		require.True(t, changed)

		admitted := keyEnvelope(3, creator)
		var (
			admitChanged bool
			admitErr     error
			joinResult   chat.JoinWithInviteResult
			joinErr      error
		)
		run(func() {
			admitChanged, _, admitErr = s.AdmitFromLobby(ctx, groupID, user, admitted)
		}, func() {
			joinResult, joinErr = s.JoinWithInvite(ctx, groupID, user, invite.ID, inviteSecret(5), now)
		})
		require.NoError(t, admitErr)
		require.NoError(t, joinErr)
		require.NotEqual(t, admitChanged, joinResult.Changed, "exactly one of the two joins")

		isMember, err := s.IsMember(ctx, groupID, user)
		require.NoError(t, err)
		require.True(t, isMember)
		requireRosterSummary(t, s, groupID, 2, 1)
		got, err := s.GetKeyEnvelope(ctx, groupID, user)
		require.NoError(t, err)
		if joinResult.Changed {
			require.True(t, joinResult.LeftLobby)
			requireUseCount(t, s, invite, 1)
			require.NotNil(t, got.InviteCopy)
			require.Equal(t, invite.ID.Value, got.InviteCopy.InviteID.Value)
		} else {
			requireUseCount(t, s, invite, 0)
			require.True(t, admitted.Equal(got))
		}
		requireLobbyFreed(t, s, groupID, user)
	})
}

// testStore_InviteJoin_RaceLobby pins a join with an invite racing the
// joiner's own lobby writes: an entry is either refused (the join landed
// first) or taken out by the join (the entry landed first), and a withdrawal
// either removes the entry or finds the join had; either way the joiner ends
// a member with no entry and both counts freed, the lobby's side moved
// exactly once.
func testStore_InviteJoin_RaceLobby(t *testing.T, s chat.Store) {
	ctx := context.Background()
	now := time.Now()
	creator := model.MustGenerateUserID()

	// Entering.
	raceOrders(func(run func(a, b func())) {
		user := model.MustGenerateUserID()
		groupID := privateGroup(t, s, creator)
		invite := createInvite(t, s, newInvite(groupID, creator, 0, now, nil), chat.MaxActiveInvites, now)

		var (
			enterErr   error
			joinResult chat.JoinWithInviteResult
			joinErr    error
		)
		run(func() {
			_, _, enterErr = s.EnterLobby(ctx, groupID, user, lobbyLimits)
		}, func() {
			joinResult, joinErr = s.JoinWithInvite(ctx, groupID, user, invite.ID, inviteSecret(5), now)
		})
		require.NoError(t, joinErr)
		require.True(t, joinResult.Changed)
		if enterErr == nil {
			require.True(t, joinResult.LeftLobby, "an entry made first is taken out by the join")
		} else {
			require.ErrorIs(t, enterErr, chat.ErrAlreadyMember)
			require.False(t, joinResult.LeftLobby)
		}
		requireRosterSummary(t, s, groupID, 2, 1)
		requireLobbyFreed(t, s, groupID, user)
	})

	// Withdrawing.
	raceOrders(func(run func(a, b func())) {
		user := model.MustGenerateUserID()
		groupID := privateGroup(t, s, creator)
		invite := createInvite(t, s, newInvite(groupID, creator, 0, now, nil), chat.MaxActiveInvites, now)
		_, changed, err := s.EnterLobby(ctx, groupID, user, lobbyLimits)
		require.NoError(t, err)
		require.True(t, changed)

		var (
			left       bool
			leaveErr   error
			joinResult chat.JoinWithInviteResult
			joinErr    error
		)
		run(func() {
			left, leaveErr = s.LeaveLobby(ctx, groupID, user)
		}, func() {
			joinResult, joinErr = s.JoinWithInvite(ctx, groupID, user, invite.ID, inviteSecret(5), now)
		})
		require.NoError(t, leaveErr)
		require.NoError(t, joinErr)
		require.True(t, joinResult.Changed)
		require.NotEqual(t, left, joinResult.LeftLobby, "exactly one of the two removes the entry")
		requireRosterSummary(t, s, groupID, 2, 1)
		requireLobbyFreed(t, s, groupID, user)
	})
}

// testStore_InviteJoin_RaceBan pins a waiting user's join with an invite
// racing the creator's ban of them: either the ban lands first and the join
// is ErrBanned, using nothing, or the join lands first and the ban removes
// the member it made. Either way the user ends banned, not a member, with no
// envelope and no entry, both counts freed, and the lobby's side moved by
// exactly one of the two.
func testStore_InviteJoin_RaceBan(t *testing.T, s chat.Store) {
	ctx := context.Background()
	now := time.Now()
	creator := model.MustGenerateUserID()

	raceOrders(func(run func(a, b func())) {
		user := model.MustGenerateUserID()
		groupID := privateGroup(t, s, creator)
		invite := createInvite(t, s, newInvite(groupID, creator, 0, now, nil), chat.MaxActiveInvites, now)
		_, changed, err := s.EnterLobby(ctx, groupID, user, chat.LobbyLimits{LobbySize: 1, LobbiesPerUser: 1})
		require.NoError(t, err)
		require.True(t, changed)

		var (
			ban        chat.BanResult
			banErr     error
			joinResult chat.JoinWithInviteResult
			joinErr    error
		)
		run(func() {
			ban, banErr = s.BanUser(ctx, groupID, user, now, true)
		}, func() {
			joinResult, joinErr = s.JoinWithInvite(ctx, groupID, user, invite.ID, inviteSecret(5), now)
		})
		require.NoError(t, banErr)
		require.True(t, ban.Changed)

		if joinErr == nil {
			require.True(t, joinResult.Changed)
			require.True(t, joinResult.LeftLobby)
			require.True(t, ban.Departed, "a join landing first is undone by the ban")
			require.False(t, ban.LeftLobby)
			requireUseCount(t, s, invite, 1)
			requireRosterSummary(t, s, groupID, 1, 2)
		} else {
			require.ErrorIs(t, joinErr, chat.ErrBanned)
			require.False(t, ban.Departed)
			require.True(t, ban.LeftLobby)
			requireUseCount(t, s, invite, 0)
			requireRosterSummary(t, s, groupID, 1, 0)
		}

		requireNotJoined(t, s, groupID, user)
		states, err := s.GetViewerStates(ctx, user, []*commonpb.ChatId{groupID})
		require.NoError(t, err)
		require.True(t, states[string(groupID.Value)].IsBanned())
		requireLobbyFreed(t, s, groupID, user)
	})
}

// RunPinStoreTests runs the shared tests of a chat.Store's pins (see
// chat.Pin) against s, alone. They are part of RunStoreTests; this runs them
// for a store with no wrapper of its own for the whole suite, such as the
// cache over memory. teardown is called between tests to reset the store;
// every test names chats of its own, so a store that shares storage with
// another is safe.
func RunPinStoreTests(t *testing.T, s chat.Store, teardown func()) {
	for _, tf := range []func(t *testing.T, s chat.Store){
		testStore_Pin_Empty,
		testStore_Pin_PinAndGet,
		testStore_Pin_RepinNoOp,
		testStore_Pin_Order,
		testStore_Pin_LatestByPosition,
		testStore_Pin_Unpin,
		testStore_Pin_UnpinThenRepin,
		testStore_Pin_Cap,
		testStore_Pin_Invalid,
		testStore_Pin_ChatsIndependent,
		testStore_Pin_ConcurrentCap,
		testStore_Pin_ConcurrentSameMessage,
	} {
		tf(t, s)
		teardown()
	}
}

// pinAt is a pin time with nanoseconds, so a store that dropped precision
// would be caught.
func pinAt(seconds int64) time.Time {
	return time.Unix(1_700_000_000+seconds, 123_456_789).UTC()
}

func messageID(v uint64) *messagingpb.MessageId {
	return &messagingpb.MessageId{Value: v}
}

// requirePin checks that got is want exactly: its message, its pinner, and
// its time to the nanosecond, in UTC.
func requirePin(t *testing.T, want, got chat.Pin) {
	t.Helper()
	require.NotNil(t, got.MessageID)
	require.Equal(t, want.MessageID.Value, got.MessageID.Value)
	require.NotNil(t, got.PinnedBy)
	require.Equal(t, want.PinnedBy.Value, got.PinnedBy.Value)
	require.Equal(t, want.PinnedAt, got.PinnedAt)
}

// requirePinSummary checks a summary against the latest pin expected (nil
// for none), the count and the version.
func requirePinSummary(t *testing.T, got chat.PinSummary, latest *chat.Pin, count uint32, version uint64) {
	t.Helper()
	require.Equal(t, count, got.Count)
	require.Equal(t, version, got.Version)
	if latest == nil {
		require.Nil(t, got.Latest)
		return
	}
	require.NotNil(t, got.Latest)
	requirePin(t, *latest, *got.Latest)
}

// requireStoredPinSummary checks the summary GetPinSummaries reads for the
// chat, as requirePinSummary does.
func requireStoredPinSummary(t *testing.T, s chat.Store, chatID *commonpb.ChatId, latest *chat.Pin, count uint32, version uint64) {
	t.Helper()
	summaries, err := s.GetPinSummaries(context.Background(), []*commonpb.ChatId{chatID})
	require.NoError(t, err)
	got, ok := summaries[string(chatID.Value)]
	require.True(t, ok)
	requirePinSummary(t, got, latest, count, version)
}

// requirePins checks that the chat's pins, read whole, are want in order.
func requirePins(t *testing.T, s chat.Store, chatID *commonpb.ChatId, want ...chat.Pin) {
	t.Helper()
	got, err := s.GetPinsPage(context.Background(), chatID, nil, 0)
	require.NoError(t, err)
	require.Len(t, got, len(want))
	for i := range want {
		requirePin(t, want[i], got[i])
	}
}

// pinMessage pins pin in chatID under maxPins, requiring it to be a real
// pin.
func pinMessage(t *testing.T, s chat.Store, chatID *commonpb.ChatId, pin chat.Pin, maxPins int) chat.PinSummary {
	t.Helper()
	summary, changed, err := s.PinMessage(context.Background(), chatID, pin.MessageID, pin.PinnedBy, pin.PinnedAt, maxPins)
	require.NoError(t, err)
	require.True(t, changed)
	return summary
}

func testStore_Pin_Empty(t *testing.T, s chat.Store) {
	ctx := context.Background()

	for _, chatID := range []*commonpb.ChatId{generateDmChatID(), chat.MustGenerateGroupChatID()} {
		// A chat that has never had a pin is absent from the summaries.
		summaries, err := s.GetPinSummaries(ctx, []*commonpb.ChatId{chatID})
		require.NoError(t, err)
		require.Empty(t, summaries)

		page, err := s.GetPinsPage(ctx, chatID, nil, 10)
		require.NoError(t, err)
		require.Empty(t, page)

		// Unpinning in it is a no-op that creates nothing.
		summary, changed, err := s.UnpinMessage(ctx, chatID, messageID(1))
		require.NoError(t, err)
		require.False(t, changed)
		requirePinSummary(t, summary, nil, 0, 0)
		summaries, err = s.GetPinSummaries(ctx, []*commonpb.ChatId{chatID})
		require.NoError(t, err)
		require.Empty(t, summaries)
	}

	summaries, err := s.GetPinSummaries(ctx, nil)
	require.NoError(t, err)
	require.Empty(t, summaries)
}

func testStore_Pin_PinAndGet(t *testing.T, s chat.Store) {
	ctx := context.Background()

	for _, chatID := range []*commonpb.ChatId{generateDmChatID(), chat.MustGenerateGroupChatID()} {
		pin := chat.Pin{MessageID: messageID(7), PinnedBy: model.MustGenerateUserID(), PinnedAt: pinAt(10)}
		summary := pinMessage(t, s, chatID, pin, chat.MaxPinnedMessages)
		requirePinSummary(t, summary, &pin, 1, 1)
		requireStoredPinSummary(t, s, chatID, &pin, 1, 1)
		requirePins(t, s, chatID, pin)
	}

	// A time in another zone, or with a monotonic reading, is recorded as
	// the same instant in UTC.
	chatID := chat.MustGenerateGroupChatID()
	local := pinAt(20).In(time.FixedZone("x", 3600))
	pinnedBy := model.MustGenerateUserID()
	summary, changed, err := s.PinMessage(ctx, chatID, messageID(1), pinnedBy, local, chat.MaxPinnedMessages)
	require.NoError(t, err)
	require.True(t, changed)
	want := chat.Pin{MessageID: messageID(1), PinnedBy: pinnedBy, PinnedAt: pinAt(20)}
	requirePinSummary(t, summary, &want, 1, 1)

	chatID = chat.MustGenerateGroupChatID()
	now := time.Now()
	_, changed, err = s.PinMessage(ctx, chatID, messageID(1), pinnedBy, now, chat.MaxPinnedMessages)
	require.NoError(t, err)
	require.True(t, changed)
	requirePins(t, s, chatID, chat.Pin{MessageID: messageID(1), PinnedBy: pinnedBy, PinnedAt: chat.PinTime(now)})
}

func testStore_Pin_RepinNoOp(t *testing.T, s chat.Store) {
	ctx := context.Background()
	chatID := chat.MustGenerateGroupChatID()

	a := chat.Pin{MessageID: messageID(1), PinnedBy: model.MustGenerateUserID(), PinnedAt: pinAt(1)}
	b := chat.Pin{MessageID: messageID(2), PinnedBy: model.MustGenerateUserID(), PinnedAt: pinAt(2)}
	pinMessage(t, s, chatID, a, 2)
	pinMessage(t, s, chatID, b, 2)

	// Pinning a again, by someone else and later, changes nothing: a keeps
	// its time, its pinner and its place below b, and the version holds. The
	// chat is at the cap, and a lower one, which the no-op is judged before.
	for _, maxPins := range []int{chat.MaxPinnedMessages, 2, 1} {
		summary, changed, err := s.PinMessage(ctx, chatID, a.MessageID, model.MustGenerateUserID(), pinAt(3), maxPins)
		require.NoError(t, err)
		require.False(t, changed)
		requirePinSummary(t, summary, &b, 2, 2)
	}
	requireStoredPinSummary(t, s, chatID, &b, 2, 2)
	requirePins(t, s, chatID, b, a)

	// The latest pinned again is a no-op too.
	summary, changed, err := s.PinMessage(ctx, chatID, b.MessageID, b.PinnedBy, pinAt(4), 2)
	require.NoError(t, err)
	require.False(t, changed)
	requirePinSummary(t, summary, &b, 2, 2)
	requirePins(t, s, chatID, b, a)
}

func testStore_Pin_Order(t *testing.T, s chat.Store) {
	ctx := context.Background()
	chatID := chat.MustGenerateGroupChatID()
	pinnedBy := model.MustGenerateUserID()

	// Pinned out of order, with three pins at one instant.
	pins := []chat.Pin{
		{MessageID: messageID(10), PinnedBy: pinnedBy, PinnedAt: pinAt(5)},
		{MessageID: messageID(3), PinnedBy: pinnedBy, PinnedAt: pinAt(9)},
		{MessageID: messageID(4), PinnedBy: pinnedBy, PinnedAt: pinAt(7)},
		{MessageID: messageID(8), PinnedBy: pinnedBy, PinnedAt: pinAt(7)},
		{MessageID: messageID(1), PinnedBy: pinnedBy, PinnedAt: pinAt(1)},
		{MessageID: messageID(6), PinnedBy: pinnedBy, PinnedAt: pinAt(7)},
	}
	const maxPins = 10
	for _, pin := range pins {
		pinMessage(t, s, chatID, pin, maxPins)
	}

	// Most recently pinned first, the tie at 7 by message ID descending.
	want := []chat.Pin{pins[1], pins[3], pins[5], pins[2], pins[0], pins[4]}
	requirePins(t, s, chatID, want...)
	requireStoredPinSummary(t, s, chatID, &want[0], uint32(len(want)), uint64(len(want)))

	// Each page resumes strictly after the last pin of the one before,
	// whatever its size.
	for _, limit := range []int{1, 2, 4, len(want), len(want) + 5} {
		var got []chat.Pin
		var after *chat.PinPosition
		for {
			page, err := s.GetPinsPage(ctx, chatID, after, limit)
			require.NoError(t, err)
			require.LessOrEqual(t, len(page), limit)
			if len(page) == 0 {
				break
			}
			got = append(got, page...)
			pos := page[len(page)-1].Position()
			after = &pos
		}
		require.Len(t, got, len(want))
		for i := range want {
			requirePin(t, want[i], got[i])
		}
	}

	// A cursor naming each pin returns exactly the pins after it, the last
	// none.
	for i := range want {
		pos := want[i].Position()
		page, err := s.GetPinsPage(ctx, chatID, &pos, 0)
		require.NoError(t, err)
		require.Len(t, page, len(want)-i-1)
		for j, pin := range page {
			requirePin(t, want[i+1+j], pin)
		}
	}

	// A cursor naming no pin, inside the tie, still resumes in order: after
	// (7, 7) come (7, 6) and the earlier times.
	pos := chat.PinPosition{PinnedAt: pinAt(7), MessageID: messageID(7)}
	page, err := s.GetPinsPage(ctx, chatID, &pos, 0)
	require.NoError(t, err)
	require.Len(t, page, 4)
	for j, pin := range page {
		requirePin(t, want[2+j], pin)
	}

	// The cursor resumes after a pin removed since it was handed out.
	_, changed, err := s.UnpinMessage(ctx, chatID, want[1].MessageID)
	require.NoError(t, err)
	require.True(t, changed)
	pos = want[1].Position()
	page, err = s.GetPinsPage(ctx, chatID, &pos, 2)
	require.NoError(t, err)
	require.Len(t, page, 2)
	requirePin(t, want[2], page[0])
	requirePin(t, want[3], page[1])
}

func testStore_Pin_LatestByPosition(t *testing.T, s chat.Store) {
	ctx := context.Background()
	chatID := generateDmChatID()
	pinnedBy := model.MustGenerateUserID()

	// The latest is the first pin in order, not the last one made: a pin
	// with an earlier time than the latest leaves it the latest.
	latest := chat.Pin{MessageID: messageID(5), PinnedBy: pinnedBy, PinnedAt: pinAt(10)}
	requirePinSummary(t, pinMessage(t, s, chatID, latest, chat.MaxPinnedMessages), &latest, 1, 1)
	earlier := chat.Pin{MessageID: messageID(9), PinnedBy: pinnedBy, PinnedAt: pinAt(3)}
	requirePinSummary(t, pinMessage(t, s, chatID, earlier, chat.MaxPinnedMessages), &latest, 2, 2)
	requireStoredPinSummary(t, s, chatID, &latest, 2, 2)

	// At the latest's instant, the smaller message ID comes after it and the
	// greater before.
	tieBelow := chat.Pin{MessageID: messageID(4), PinnedBy: pinnedBy, PinnedAt: pinAt(10)}
	requirePinSummary(t, pinMessage(t, s, chatID, tieBelow, chat.MaxPinnedMessages), &latest, 3, 3)
	tieAbove := chat.Pin{MessageID: messageID(6), PinnedBy: pinnedBy, PinnedAt: pinAt(10)}
	requirePinSummary(t, pinMessage(t, s, chatID, tieAbove, chat.MaxPinnedMessages), &tieAbove, 4, 4)
	requireStoredPinSummary(t, s, chatID, &tieAbove, 4, 4)
	requirePins(t, s, chatID, tieAbove, latest, tieBelow, earlier)

	// Unpinning the latest makes the next in order the latest, however
	// they were pinned.
	summary, changed, err := s.UnpinMessage(ctx, chatID, tieAbove.MessageID)
	require.NoError(t, err)
	require.True(t, changed)
	requirePinSummary(t, summary, &latest, 3, 5)
	summary, changed, err = s.UnpinMessage(ctx, chatID, latest.MessageID)
	require.NoError(t, err)
	require.True(t, changed)
	requirePinSummary(t, summary, &tieBelow, 2, 6)
	requireStoredPinSummary(t, s, chatID, &tieBelow, 2, 6)
}

func testStore_Pin_Unpin(t *testing.T, s chat.Store) {
	ctx := context.Background()
	chatID := generateDmChatID()

	a := chat.Pin{MessageID: messageID(1), PinnedBy: model.MustGenerateUserID(), PinnedAt: pinAt(1)}
	b := chat.Pin{MessageID: messageID(2), PinnedBy: model.MustGenerateUserID(), PinnedAt: pinAt(2)}
	c := chat.Pin{MessageID: messageID(3), PinnedBy: model.MustGenerateUserID(), PinnedAt: pinAt(3)}
	for _, pin := range []chat.Pin{a, b, c} {
		pinMessage(t, s, chatID, pin, chat.MaxPinnedMessages)
	}
	requireStoredPinSummary(t, s, chatID, &c, 3, 3)

	// Unpinning the latest makes the next in order the latest.
	summary, changed, err := s.UnpinMessage(ctx, chatID, c.MessageID)
	require.NoError(t, err)
	require.True(t, changed)
	requirePinSummary(t, summary, &b, 2, 4)
	requireStoredPinSummary(t, s, chatID, &b, 2, 4)
	requirePins(t, s, chatID, b, a)

	// A message not pinned, or no longer pinned, is a no-op.
	for _, id := range []*messagingpb.MessageId{messageID(99), c.MessageID} {
		summary, changed, err = s.UnpinMessage(ctx, chatID, id)
		require.NoError(t, err)
		require.False(t, changed)
		requirePinSummary(t, summary, &b, 2, 4)
	}

	// Unpinning one below the latest leaves the latest.
	summary, changed, err = s.UnpinMessage(ctx, chatID, a.MessageID)
	require.NoError(t, err)
	require.True(t, changed)
	requirePinSummary(t, summary, &b, 1, 5)
	requirePins(t, s, chatID, b)

	// Unpinning the last leaves none, with the version kept: the chat has
	// had pins, so it is no longer absent from the summaries.
	summary, changed, err = s.UnpinMessage(ctx, chatID, b.MessageID)
	require.NoError(t, err)
	require.True(t, changed)
	requirePinSummary(t, summary, nil, 0, 6)
	requireStoredPinSummary(t, s, chatID, nil, 0, 6)
	requirePins(t, s, chatID)

	summary, changed, err = s.UnpinMessage(ctx, chatID, b.MessageID)
	require.NoError(t, err)
	require.False(t, changed)
	requirePinSummary(t, summary, nil, 0, 6)
}

func testStore_Pin_UnpinThenRepin(t *testing.T, s chat.Store) {
	ctx := context.Background()
	chatID := chat.MustGenerateGroupChatID()

	a := chat.Pin{MessageID: messageID(1), PinnedBy: model.MustGenerateUserID(), PinnedAt: pinAt(1)}
	b := chat.Pin{MessageID: messageID(2), PinnedBy: model.MustGenerateUserID(), PinnedAt: pinAt(2)}
	pinMessage(t, s, chatID, a, chat.MaxPinnedMessages)
	pinMessage(t, s, chatID, b, chat.MaxPinnedMessages)

	// Unpinned then pinned again, a is a fresh pin: its new time and pinner,
	// at the top, two versions on.
	_, changed, err := s.UnpinMessage(ctx, chatID, a.MessageID)
	require.NoError(t, err)
	require.True(t, changed)
	again := chat.Pin{MessageID: a.MessageID, PinnedBy: model.MustGenerateUserID(), PinnedAt: pinAt(3)}
	summary := pinMessage(t, s, chatID, again, chat.MaxPinnedMessages)
	requirePinSummary(t, summary, &again, 2, 4)
	requireStoredPinSummary(t, s, chatID, &again, 2, 4)
	requirePins(t, s, chatID, again, b)
}

func testStore_Pin_Cap(t *testing.T, s chat.Store) {
	ctx := context.Background()
	chatID := chat.MustGenerateGroupChatID()
	pinnedBy := model.MustGenerateUserID()

	var pins []chat.Pin
	for i := range chat.MaxPinnedMessages {
		pin := chat.Pin{MessageID: messageID(uint64(i + 1)), PinnedBy: pinnedBy, PinnedAt: pinAt(int64(i))}
		pinMessage(t, s, chatID, pin, chat.MaxPinnedMessages)
		pins = append(pins, pin)
	}
	latest := pins[len(pins)-1]

	// One more is refused, with nothing written.
	over := chat.Pin{MessageID: messageID(100), PinnedBy: pinnedBy, PinnedAt: pinAt(100)}
	_, changed, err := s.PinMessage(ctx, chatID, over.MessageID, over.PinnedBy, over.PinnedAt, chat.MaxPinnedMessages)
	require.ErrorIs(t, err, chat.ErrTooManyPins)
	require.False(t, changed)
	requireStoredPinSummary(t, s, chatID, &latest, chat.MaxPinnedMessages, chat.MaxPinnedMessages)
	page, err := s.GetPinsPage(ctx, chatID, nil, 0)
	require.NoError(t, err)
	require.Len(t, page, chat.MaxPinnedMessages)
	for _, pin := range page {
		require.NotEqual(t, over.MessageID.Value, pin.MessageID.Value)
	}

	// A lower cap than the chat holds refuses too.
	_, _, err = s.PinMessage(ctx, chatID, over.MessageID, over.PinnedBy, over.PinnedAt, 2)
	require.ErrorIs(t, err, chat.ErrTooManyPins)

	// After an unpin there is room again.
	_, changed, err = s.UnpinMessage(ctx, chatID, pins[0].MessageID)
	require.NoError(t, err)
	require.True(t, changed)
	summary := pinMessage(t, s, chatID, over, chat.MaxPinnedMessages)
	requirePinSummary(t, summary, &over, chat.MaxPinnedMessages, chat.MaxPinnedMessages+2)

	// A cap below one is a programming error, with nothing written.
	other := chat.MustGenerateGroupChatID()
	for _, maxPins := range []int{0, -1} {
		_, changed, err = s.PinMessage(ctx, other, messageID(1), pinnedBy, pinAt(1), maxPins)
		require.Error(t, err)
		require.NotErrorIs(t, err, chat.ErrTooManyPins)
		require.False(t, changed)
	}
	summaries, err := s.GetPinSummaries(ctx, []*commonpb.ChatId{other})
	require.NoError(t, err)
	require.Empty(t, summaries)
}

func testStore_Pin_Invalid(t *testing.T, s chat.Store) {
	ctx := context.Background()
	chatID := chat.MustGenerateGroupChatID()
	pinnedBy := model.MustGenerateUserID()
	badChat := &commonpb.ChatId{Value: randomBytes(20)}

	for _, tc := range []struct {
		name      string
		chatID    *commonpb.ChatId
		messageID *messagingpb.MessageId
		pinnedBy  *commonpb.UserId
		pinnedAt  time.Time
	}{
		{"chat id length", badChat, messageID(1), pinnedBy, pinAt(1)},
		{"nil chat id", nil, messageID(1), pinnedBy, pinAt(1)},
		{"nil message id", chatID, nil, pinnedBy, pinAt(1)},
		{"nil pinner", chatID, messageID(1), nil, pinAt(1)},
		{"pinner length", chatID, messageID(1), &commonpb.UserId{Value: randomBytes(5)}, pinAt(1)},
		{"before the epoch", chatID, messageID(1), pinnedBy, time.Unix(-1, 0)},
		{"past unix nanos", chatID, messageID(1), pinnedBy, time.Date(2300, 1, 1, 0, 0, 0, 0, time.UTC)},
	} {
		_, changed, err := s.PinMessage(ctx, tc.chatID, tc.messageID, tc.pinnedBy, tc.pinnedAt, chat.MaxPinnedMessages)
		require.Error(t, err, tc.name)
		require.False(t, changed, tc.name)
	}
	summaries, err := s.GetPinSummaries(ctx, []*commonpb.ChatId{chatID})
	require.NoError(t, err)
	require.Empty(t, summaries)

	_, _, err = s.UnpinMessage(ctx, badChat, messageID(1))
	require.Error(t, err)
	_, _, err = s.UnpinMessage(ctx, chatID, nil)
	require.Error(t, err)

	_, err = s.GetPinSummaries(ctx, []*commonpb.ChatId{chatID, badChat})
	require.Error(t, err)

	_, err = s.GetPinsPage(ctx, badChat, nil, 0)
	require.Error(t, err)
	for _, pos := range []chat.PinPosition{
		{PinnedAt: pinAt(1)},
		{PinnedAt: time.Unix(-1, 0), MessageID: messageID(1)},
	} {
		_, err = s.GetPinsPage(ctx, chatID, &pos, 0)
		require.Error(t, err)
	}
}

func testStore_Pin_ChatsIndependent(t *testing.T, s chat.Store) {
	ctx := context.Background()
	dmID := generateDmChatID()
	groupID := chat.MustGenerateGroupChatID()
	neverID := chat.MustGenerateGroupChatID()

	// One message ID pinned in two chats is two pins.
	dmPin := chat.Pin{MessageID: messageID(5), PinnedBy: model.MustGenerateUserID(), PinnedAt: pinAt(1)}
	groupPin := chat.Pin{MessageID: messageID(5), PinnedBy: model.MustGenerateUserID(), PinnedAt: pinAt(2)}
	groupPin2 := chat.Pin{MessageID: messageID(6), PinnedBy: model.MustGenerateUserID(), PinnedAt: pinAt(3)}
	pinMessage(t, s, dmID, dmPin, 1)
	pinMessage(t, s, groupID, groupPin, chat.MaxPinnedMessages)
	pinMessage(t, s, groupID, groupPin2, chat.MaxPinnedMessages)

	// The DM at its cap of one does not hold the group back, nor the reverse.
	summaries, err := s.GetPinSummaries(ctx, []*commonpb.ChatId{dmID, groupID, neverID, dmID})
	require.NoError(t, err)
	require.Len(t, summaries, 2)
	requirePinSummary(t, summaries[string(dmID.Value)], &dmPin, 1, 1)
	requirePinSummary(t, summaries[string(groupID.Value)], &groupPin2, 2, 2)

	// Unpinning in one leaves the other.
	_, changed, err := s.UnpinMessage(ctx, groupID, groupPin.MessageID)
	require.NoError(t, err)
	require.True(t, changed)
	requireStoredPinSummary(t, s, dmID, &dmPin, 1, 1)
	requirePins(t, s, dmID, dmPin)
	requireStoredPinSummary(t, s, groupID, &groupPin2, 1, 3)
	requirePins(t, s, groupID, groupPin2)
}

func testStore_Pin_ConcurrentCap(t *testing.T, s chat.Store) {
	ctx := context.Background()
	chatID := chat.MustGenerateGroupChatID()
	pinnedBy := model.MustGenerateUserID()

	const pinners = 20
	results := make([]error, pinners)
	var wg sync.WaitGroup
	for i := range pinners {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _, results[i] = s.PinMessage(ctx, chatID, messageID(uint64(i+1)), pinnedBy, pinAt(int64(i)), chat.MaxPinnedMessages)
		}()
	}
	wg.Wait()

	var pinned, refused int
	for _, err := range results {
		switch {
		case err == nil:
			pinned++
		case errors.Is(err, chat.ErrTooManyPins):
			refused++
		default:
			require.NoError(t, err)
		}
	}
	require.Equal(t, chat.MaxPinnedMessages, pinned)
	require.Equal(t, pinners-chat.MaxPinnedMessages, refused)

	summaries, err := s.GetPinSummaries(ctx, []*commonpb.ChatId{chatID})
	require.NoError(t, err)
	summary := summaries[string(chatID.Value)]
	require.EqualValues(t, chat.MaxPinnedMessages, summary.Count)
	require.EqualValues(t, chat.MaxPinnedMessages, summary.Version)
	page, err := s.GetPinsPage(ctx, chatID, nil, 0)
	require.NoError(t, err)
	require.Len(t, page, chat.MaxPinnedMessages)
	requirePin(t, page[0], *summary.Latest)
}

func testStore_Pin_ConcurrentSameMessage(t *testing.T, s chat.Store) {
	ctx := context.Background()
	chatID := generateDmChatID()

	const pinners = 20
	changes := make([]bool, pinners)
	results := make([]error, pinners)
	var wg sync.WaitGroup
	for i := range pinners {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, changes[i], results[i] = s.PinMessage(ctx, chatID, messageID(1), model.MustGenerateUserID(), pinAt(int64(i)), chat.MaxPinnedMessages)
		}()
	}
	wg.Wait()

	var changed int
	for i := range pinners {
		require.NoError(t, results[i])
		if changes[i] {
			changed++
		}
	}
	require.Equal(t, 1, changed)

	summaries, err := s.GetPinSummaries(ctx, []*commonpb.ChatId{chatID})
	require.NoError(t, err)
	summary := summaries[string(chatID.Value)]
	require.EqualValues(t, 1, summary.Count)
	require.EqualValues(t, 1, summary.Version)
	requirePins(t, s, chatID, *summary.Latest)
}
