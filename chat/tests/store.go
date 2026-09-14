package tests

import (
	"context"
	"crypto/rand"
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

// RunStoreTests runs the shared chat.Store test suite against s. teardown is
// called between tests to reset the store.
func RunStoreTests(t *testing.T, s chat.Store, teardown func()) {
	for _, tf := range []func(t *testing.T, s chat.Store){
		testStore_PutAndGet,
		testStore_PutChat_Duplicate,
		testStore_PutChat_TypeIDMismatch,
		testStore_PutChat_NoMembers,
		testStore_GetChatByID_NotFound,
		testStore_Members,
		testStore_IsMember,
		testStore_AdvanceLastMessage,
		testStore_GroupChat_PutAndGet,
		testStore_GroupChat_StaffOnly,
		testStore_GroupChat_MinimumListenerBalance,
		testStore_GroupChat_Rules,
		testStore_GroupChat_Creator,
		testStore_GroupChat_Picture,
		testStore_GroupChat_Membership,
		testStore_GroupChat_RosterSummary,
		testStore_GroupChat_ConcurrentTransitions,
		testStore_GroupChat_IDsForUser,
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

	// A group without a requirement has no rules — nil, not an empty set.
	plain := putGroupChat(t, s, "Weekend Trip", at(100), model.MustGenerateUserID())
	rules, err := s.GetGroupRules(ctx, plain.ID)
	require.NoError(t, err)
	require.Nil(t, rules)

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

	rules, err = s.GetGroupRules(ctx, staff.ID)
	require.NoError(t, err)
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

	rules, err = s.GetGroupRules(ctx, gated.ID)
	require.NoError(t, err)
	require.True(t, proto.Equal(gated.Rules(), rules))
	require.Len(t, rules.GetListener(), 1)
	require.True(t, proto.Equal(gated.MinimumListenerBalance.ToProto(), rules.GetListener()[0].GetMinimumBalance()))
	require.Empty(t, rules.GetSpeaker())

	both := gated.Clone()
	both.ID = chat.MustGenerateGroupChatID()
	both.Members = []*commonpb.UserId{model.MustGenerateUserID()}
	both.IsStaffOnly = true
	require.NoError(t, s.PutChat(ctx, both))

	rules, err = s.GetGroupRules(ctx, both.ID)
	require.NoError(t, err)
	require.True(t, proto.Equal(both.Rules(), rules))
	require.Len(t, rules.GetListener(), 2)
	require.NotNil(t, rules.GetListener()[0].GetStaff())
	require.NotNil(t, rules.GetListener()[1].GetMinimumBalance())
	require.Empty(t, rules.GetSpeaker())

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
	changed, roster, err := s.RemoveGroupMember(context.Background(), chatID, userID)
	require.NoError(t, err)
	return changed, roster
}

// testStore_GroupChat_IDsForUser pins the inverse membership read: exactly the
// groups the user is currently joined to, tracking joins, departures, and
// rejoins, with DMs never included.
func testStore_GroupChat_IDsForUser(t *testing.T, s chat.Store) {
	ctx := context.Background()

	userA := model.MustGenerateUserID()
	userB := model.MustGenerateUserID()

	// No memberships is an empty result, not an error.
	chatIDs, err := s.GetGroupChatIDsForUser(ctx, userA)
	require.NoError(t, err)
	require.Empty(t, chatIDs)

	groupAB := putGroupChat(t, s, "Both", at(1), userA, userB)
	groupA := putGroupChat(t, s, "Only A", at(2), userA)
	groupB := putGroupChat(t, s, "Only B", at(3), userB)

	// A DM must never surface as a group membership.
	putDmChat(t, s, userA, userB, at(4))

	chatIDs, err = s.GetGroupChatIDsForUser(ctx, userA)
	require.NoError(t, err)
	require.ElementsMatch(t, chatIDValues([]*chat.Chat{groupAB, groupA}), rawChatIDValues(chatIDs))

	// Departure excludes the group; a tombstoned membership is not a membership.
	removeGroupMember(t, s, groupAB.ID, userA)
	chatIDs, err = s.GetGroupChatIDsForUser(ctx, userA)
	require.NoError(t, err)
	require.ElementsMatch(t, chatIDValues([]*chat.Chat{groupA}), rawChatIDValues(chatIDs))

	// Rejoining restores it; joining another user's group adds it.
	addGroupMembers(t, s, groupAB.ID, userA)
	addGroupMembers(t, s, groupB.ID, userA)
	chatIDs, err = s.GetGroupChatIDsForUser(ctx, userA)
	require.NoError(t, err)
	require.ElementsMatch(t, chatIDValues([]*chat.Chat{groupAB, groupA, groupB}), rawChatIDValues(chatIDs))

	// userB's view was never disturbed by userA's churn.
	chatIDs, err = s.GetGroupChatIDsForUser(ctx, userB)
	require.NoError(t, err)
	require.ElementsMatch(t, chatIDValues([]*chat.Chat{groupAB, groupB}), rawChatIDValues(chatIDs))
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
	// orphaned records.
	_, _, err := s.AddGroupMembers(ctx, chat.MustGenerateGroupChatID(), []*commonpb.UserId{user})
	require.ErrorIs(t, err, chat.ErrChatNotFound)

	// Group membership methods reject DM chat IDs outright.
	dm := putDmChat(t, s, user, model.MustGenerateUserID(), at(1))
	_, _, err = s.AddGroupMembers(ctx, dm.ID, []*commonpb.UserId{user})
	require.Error(t, err)
	_, _, err = s.RemoveGroupMember(ctx, dm.ID, user)
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
	tip1 := putDmChatOfType(t, s, chatpb.ChatType_TIP_DM, user, other, at(200))
	contact2 := putDmChatOfType(t, s, chatpb.ChatType_CONTACT_DM, user, other, at(300))
	tip2 := putDmChatOfType(t, s, chatpb.ChatType_TIP_DM, user, other, at(400))

	// Each feed contains only its own type, most recent first.
	contacts, err := s.GetDmFeedPage(ctx, user, chatpb.ChatType_CONTACT_DM, at(1000), nil, 0)
	require.NoError(t, err)
	require.Equal(t, [][]byte{contact2.ID.Value, contact1.ID.Value}, chatIDValues(contacts))
	for _, c := range contacts {
		require.Equal(t, chatpb.ChatType_CONTACT_DM, c.Type)
	}

	tips, err := s.GetDmFeedPage(ctx, user, chatpb.ChatType_TIP_DM, at(1000), nil, 0)
	require.NoError(t, err)
	require.Equal(t, [][]byte{tip2.ID.Value, tip1.ID.Value}, chatIDValues(tips))
	for _, c := range tips {
		require.Equal(t, chatpb.ChatType_TIP_DM, c.Type)
	}

	// Paging within one feed steps over the other type's activity: a limit-1
	// tip page resumes at the older tip, not at the interleaved contact chats.
	tipPage1, err := s.GetDmFeedPage(ctx, user, chatpb.ChatType_TIP_DM, at(1000), nil, 1)
	require.NoError(t, err)
	require.Equal(t, [][]byte{tip2.ID.Value}, chatIDValues(tipPage1))

	tipPage2, err := s.GetDmFeedPage(ctx, user, chatpb.ChatType_TIP_DM, at(1000), cursorOf(tipPage1[0]), 1)
	require.NoError(t, err)
	require.Equal(t, [][]byte{tip1.ID.Value}, chatIDValues(tipPage2))
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

func rawChatIDValues(ids []*commonpb.ChatId) [][]byte {
	out := make([][]byte, len(ids))
	for i, id := range ids {
		out[i] = id.Value
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
