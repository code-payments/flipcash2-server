package chat

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	chatpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/chat/v1"
	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"

	"github.com/code-payments/flipcash2-server/account"
	"github.com/code-payments/flipcash2-server/model"
)

// fakeAccounts is an account.Store that answers IsStaff from a set and records
// who was asked. Only IsStaff is exercised; the embedded nil interface makes
// any other call panic, which is the intent.
type fakeAccounts struct {
	account.Store

	staff map[string]bool
	asked int
	err   error
}

func (f *fakeAccounts) IsStaff(_ context.Context, userID *commonpb.UserId) (bool, error) {
	f.asked++
	if f.err != nil {
		return false, f.err
	}
	return f.staff[string(userID.Value)], nil
}

// fakeChats is a Store that serves rules from a map of group records and
// counts the reads, so a test can see which evaluations touched the store.
// Only GetGroupRules is exercised, as above.
type fakeChats struct {
	Store

	chats map[string]*Chat
	reads int
}

func (f *fakeChats) put(c *Chat) *Chat {
	f.chats[string(c.ID.Value)] = c
	return c
}

func (f *fakeChats) GetGroupRules(_ context.Context, chatID *commonpb.ChatId) (*chatpb.Rules, error) {
	f.reads++
	if !IsGroupChatID(chatID) {
		return nil, errors.New("not a group chat id")
	}
	c, ok := f.chats[string(chatID.Value)]
	if !ok {
		return nil, ErrChatNotFound
	}
	return c.Rules(), nil
}

func TestChat_Rules(t *testing.T) {
	a := model.MustGenerateUserID()
	b := model.MustGenerateUserID()

	// A DM never carries rules, and neither does a group not created with one.
	dm := &Chat{ID: MustDeriveDmChatID(chatpb.ChatType_CONTACT_DM, a, b), Type: chatpb.ChatType_CONTACT_DM, Members: []*commonpb.UserId{a, b}}
	require.Nil(t, dm.Rules())
	require.Nil(t, dm.ToProto().GetRules())

	plain := &Chat{ID: MustGenerateGroupChatID(), Type: chatpb.ChatType_GROUP, Title: "Weekend Trip"}
	require.Nil(t, plain.Rules())
	require.Nil(t, plain.ToProto().GetRules())

	// A staff-only group is exactly one listener rule, StaffRequirement, and no
	// speaker rules: restricting the audience already restricts the speakers.
	staffOnly := &Chat{ID: MustGenerateGroupChatID(), Type: chatpb.ChatType_GROUP, Title: "Staff", IsStaffOnly: true}
	rules := staffOnly.Rules()
	require.Len(t, rules.GetListener(), 1)
	require.NotNil(t, rules.GetListener()[0].GetStaff())
	require.Empty(t, rules.GetSpeaker())
	require.NoError(t, rules.Validate())

	// The projection onto Metadata carries the same rules.
	require.Equal(t, rules, staffOnly.ToProto().GetRules())

	// The flag is group-only: a DM record that somehow carries it still has no
	// rules.
	flaggedDm := dm.Clone()
	flaggedDm.IsStaffOnly = true
	require.Nil(t, flaggedDm.Rules())
}

func TestRuleEvaluator(t *testing.T) {
	ctx := context.Background()
	staffUser := model.MustGenerateUserID()
	civilian := model.MustGenerateUserID()
	staff := &fakeAccounts{staff: map[string]bool{string(staffUser.Value): true}}
	chats := &fakeChats{chats: make(map[string]*Chat)}
	e := NewRuleEvaluator(staff, chats)

	// A DM is answered without a read: it can carry no rules.
	dmID := MustDeriveDmChatID(chatpb.ChatType_CONTACT_DM, staffUser, civilian)
	ok, err := e.CanListen(ctx, dmID, civilian)
	require.NoError(t, err)
	require.True(t, ok)
	ok, err = e.CanSpeak(ctx, dmID, civilian)
	require.NoError(t, err)
	require.True(t, ok)
	require.Zero(t, chats.reads)

	// A group without rules admits everyone, and never consults the staff
	// reader.
	plain := chats.put(&Chat{ID: MustGenerateGroupChatID(), Type: chatpb.ChatType_GROUP})
	for _, u := range []*commonpb.UserId{staffUser, civilian} {
		ok, err := e.CanListen(ctx, plain.ID, u)
		require.NoError(t, err)
		require.True(t, ok)
		ok, err = e.CanSpeak(ctx, plain.ID, u)
		require.NoError(t, err)
		require.True(t, ok)
	}
	require.Zero(t, staff.asked)

	// A staff-only chat admits staff, to listen and to speak...
	staffOnly := chats.put(&Chat{ID: MustGenerateGroupChatID(), Type: chatpb.ChatType_GROUP, IsStaffOnly: true})
	ok, err = e.CanListen(ctx, staffOnly.ID, staffUser)
	require.NoError(t, err)
	require.True(t, ok)
	ok, err = e.CanSpeak(ctx, staffOnly.ID, staffUser)
	require.NoError(t, err)
	require.True(t, ok)

	// ...and nobody else: a user who cannot listen cannot speak either.
	ok, err = e.CanListen(ctx, staffOnly.ID, civilian)
	require.NoError(t, err)
	require.False(t, ok)
	ok, err = e.CanSpeak(ctx, staffOnly.ID, civilian)
	require.NoError(t, err)
	require.False(t, ok)

	// The rule is evaluated against the reader's current answer, so a revoked
	// flag is denied on the next evaluation.
	delete(staff.staff, string(staffUser.Value))
	ok, err = e.CanListen(ctx, staffOnly.ID, staffUser)
	require.NoError(t, err)
	require.False(t, ok)

	// A reader failure is an error, never a pass.
	staff.err = errors.New("unavailable")
	ok, err = e.CanListen(ctx, staffOnly.ID, staffUser)
	require.Error(t, err)
	require.False(t, ok)

	// An unknown group is the store's not-found, surfaced as-is.
	_, err = e.CanListen(ctx, MustGenerateGroupChatID(), civilian)
	require.ErrorIs(t, err, ErrChatNotFound)
}
