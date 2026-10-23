package tests

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	chatpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/chat/v1"
	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"
	messagingpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/messaging/v1"

	"github.com/code-payments/flipcash2-server/chat"
	"github.com/code-payments/flipcash2-server/model"
	"github.com/code-payments/flipcash2-server/protoutil"
)

func (e *serverEnv) silenceUser(keys model.KeyPair, chatID *commonpb.ChatId, userID *commonpb.UserId, silence *chatpb.SilenceState) (*chatpb.SilenceUserResponse, error) {
	req := &chatpb.SilenceUserRequest{ChatId: chatID, UserId: userID, Silence: silence}
	require.NoError(e.t, keys.Auth(req, &req.Auth))
	return e.client.SilenceUser(e.ctx, req)
}

func (e *serverEnv) mustSilenceUser(keys model.KeyPair, chatID *commonpb.ChatId, userID *commonpb.UserId, silence *chatpb.SilenceState) chatpb.SilenceUserResponse_Result {
	resp, err := e.silenceUser(keys, chatID, userID, silence)
	require.NoError(e.t, err)
	return resp.Result
}

func (e *serverEnv) mustUnsilenceUser(keys model.KeyPair, chatID *commonpb.ChatId, userID *commonpb.UserId) chatpb.UnsilenceUserResponse_Result {
	req := &chatpb.UnsilenceUserRequest{ChatId: chatID, UserId: userID}
	require.NoError(e.t, keys.Auth(req, &req.Auth))
	resp, err := e.client.UnsilenceUser(e.ctx, req)
	require.NoError(e.t, err)
	return resp.Result
}

func (e *serverEnv) mustBanUser(keys model.KeyPair, chatID *commonpb.ChatId, userID *commonpb.UserId) chatpb.BanUserResponse_Result {
	req := &chatpb.BanUserRequest{ChatId: chatID, UserId: userID}
	require.NoError(e.t, keys.Auth(req, &req.Auth))
	resp, err := e.client.BanUser(e.ctx, req)
	require.NoError(e.t, err)
	return resp.Result
}

func (e *serverEnv) mustUnbanUser(keys model.KeyPair, chatID *commonpb.ChatId, userID *commonpb.UserId) chatpb.UnbanUserResponse_Result {
	req := &chatpb.UnbanUserRequest{ChatId: chatID, UserId: userID}
	require.NoError(e.t, keys.Auth(req, &req.Auth))
	resp, err := e.client.UnbanUser(e.ctx, req)
	require.NoError(e.t, err)
	return resp.Result
}

func (e *serverEnv) getRestrictedUsers(keys model.KeyPair, chatID *commonpb.ChatId, opts *commonpb.QueryOptions) (*chatpb.GetRestrictedUsersResponse, error) {
	req := &chatpb.GetRestrictedUsersRequest{ChatId: chatID, QueryOptions: opts}
	require.NoError(e.t, keys.Auth(req, &req.Auth))
	return e.client.GetRestrictedUsers(e.ctx, req)
}

func (e *serverEnv) mustGetRestrictedUsers(keys model.KeyPair, chatID *commonpb.ChatId, opts *commonpb.QueryOptions) *chatpb.GetRestrictedUsersResponse {
	resp, err := e.getRestrictedUsers(keys, chatID, opts)
	require.NoError(e.t, err)
	return resp
}

// waitForRestrictedListing waits until the store's listing of chatID's
// restricted users holds n users: it is read from an index that trails writes
// (see chat.Store.GetRestrictedUsersPage).
func (e *serverEnv) waitForRestrictedListing(chatID *commonpb.ChatId, n int) {
	require.Eventually(e.t, func() bool {
		users, err := e.store.GetRestrictedUsersPage(e.ctx, chatID, nil, 0)
		require.NoError(e.t, err)
		return len(users) == n
	}, 5*time.Second, 10*time.Millisecond)
}

func silenceUntil(until time.Time) *chatpb.SilenceState {
	return &chatpb.SilenceState{Duration: &chatpb.SilenceState_Until{Until: timestamppb.New(until)}}
}

func silenceForever() *chatpb.SilenceState {
	return &chatpb.SilenceState{Duration: &chatpb.SilenceState_Forever_{Forever: &chatpb.SilenceState_Forever{}}}
}

// silencedViewerState is viewerState with a silence under restrictions.
func silencedViewerState(version uint64, silence *chatpb.SilenceState, isCreator bool) *chatpb.ViewerState {
	state := viewerState(version, nil, isCreator)
	if silence != nil {
		state.Restrictions = &chatpb.ViewerState_Restrictions{Silence: silence}
	}
	return state
}

func restrictedUserIDs(users []*chatpb.RestrictedUser) [][]byte {
	out := make([][]byte, len(users))
	for i, u := range users {
		out[i] = u.UserId.Value
	}
	return out
}

// testServer_Moderation_Gates pins who may moderate: the recorded creator of
// a group, public or private, while a member, and no one else; and that the
// creator cannot be restricted, while lifting a restriction from them is the
// no-op. permissions.moderation on the hydrated ViewerState agrees.
func testServer_Moderation_Gates(t *testing.T, s chat.Store) {
	e := newServerEnv(t, s)

	memberID, memberKeys := e.addUser()
	target := model.MustGenerateUserID()
	public := e.putOwnedGroup("Public", nil, memberID)
	private := e.putKeyedPrivateGroup("Private", memberID)

	type results struct {
		silence   chatpb.SilenceUserResponse_Result
		unsilence chatpb.UnsilenceUserResponse_Result
		ban       chatpb.BanUserResponse_Result
		unban     chatpb.UnbanUserResponse_Result
		list      chatpb.GetRestrictedUsersResponse_Result
	}
	all := func(keys model.KeyPair, chatID *commonpb.ChatId, userID *commonpb.UserId) results {
		t.Helper()
		list := e.mustGetRestrictedUsers(keys, chatID, nil)
		return results{
			silence:   e.mustSilenceUser(keys, chatID, userID, silenceForever()),
			unsilence: e.mustUnsilenceUser(keys, chatID, userID),
			ban:       e.mustBanUser(keys, chatID, userID),
			unban:     e.mustUnbanUser(keys, chatID, userID),
			list:      list.Result,
		}
	}
	denied := results{
		silence:   chatpb.SilenceUserResponse_DENIED,
		unsilence: chatpb.UnsilenceUserResponse_DENIED,
		ban:       chatpb.BanUserResponse_DENIED,
		unban:     chatpb.UnbanUserResponse_DENIED,
		list:      chatpb.GetRestrictedUsersResponse_DENIED,
	}

	// A DM is DENIED before anything is read, whoever asks.
	dm := e.putDM(at(1))
	require.Equal(t, denied, all(e.keys, dm, target))

	// A group that is not there is NOT_FOUND.
	require.Equal(t, results{
		silence:   chatpb.SilenceUserResponse_NOT_FOUND,
		unsilence: chatpb.UnsilenceUserResponse_NOT_FOUND,
		ban:       chatpb.BanUserResponse_NOT_FOUND,
		unban:     chatpb.UnbanUserResponse_NOT_FOUND,
		list:      chatpb.GetRestrictedUsersResponse_NOT_FOUND,
	}, all(e.keys, chat.MustGenerateGroupChatID(), target))

	for _, chatID := range []*commonpb.ChatId{public, private} {
		// A member who is not the creator may not moderate, and is told so.
		require.Equal(t, denied, all(memberKeys, chatID, target))
		md := e.getChat(memberKeys, chatID).Metadata
		require.Nil(t, md.GetViewerState().GetPermissions().GetModeration())

		// The creator may, and is told so.
		md = e.getChat(e.keys, chatID).Metadata
		require.True(t, md.GetViewerState().GetPermissions().GetModeration().GetCanSilence())
		require.True(t, md.GetViewerState().GetPermissions().GetModeration().GetCanBan())
		require.Equal(t, results{}, all(e.keys, chatID, target))

		// The creator cannot be restricted; lifting a restriction from them
		// is the no-op.
		require.Equal(t, chatpb.SilenceUserResponse_CANNOT_RESTRICT_CREATOR, e.mustSilenceUser(e.keys, chatID, e.userID, silenceForever()))
		require.Equal(t, chatpb.BanUserResponse_CANNOT_RESTRICT_CREATOR, e.mustBanUser(e.keys, chatID, e.userID))
		require.Equal(t, chatpb.UnsilenceUserResponse_OK, e.mustUnsilenceUser(e.keys, chatID, e.userID))
		require.Equal(t, chatpb.UnbanUserResponse_OK, e.mustUnbanUser(e.keys, chatID, e.userID))
	}

	// A timed silence must end in the future, checked before anything is
	// read.
	_, err := e.silenceUser(e.keys, public, target, silenceUntil(time.Now().Add(-time.Minute)))
	require.Equal(t, codes.InvalidArgument, status.Code(err))
	_, err = e.silenceUser(e.keys, dm, target, silenceUntil(time.Now().Add(-time.Minute)))
	require.Equal(t, codes.InvalidArgument, status.Code(err))

	// A creator who has left moderates nothing until they rejoin.
	require.Equal(t, chatpb.LeaveChatResponse_OK, e.mustLeaveChat(e.keys, public).Result)
	require.Equal(t, denied, all(e.keys, public, target))
	require.Equal(t, chatpb.JoinChatResponse_OK, e.mustJoinChat(e.keys, public).Result)
	require.Equal(t, results{}, all(e.keys, public, target))
}

// testServer_Moderation_Silence pins a silence on a member: it reaches the
// member's own devices, and no one else, on every real change and never on a
// no-op; it is shown on their ViewerState until lifted; and it survives
// their leaving and rejoining.
func testServer_Moderation_Silence(t *testing.T, s chat.Store) {
	e := newServerEnv(t, s)

	memberID, memberKeys := e.addUser()
	otherID, _ := e.addUser()
	chatID := e.putOwnedGroup("Group", nil, memberID, otherID)

	until := time.Now().Add(time.Hour).Truncate(time.Second)
	require.Equal(t, chatpb.SilenceUserResponse_OK, e.mustSilenceUser(e.keys, chatID, memberID, silenceUntil(until)))
	want := silencedViewerState(1, silenceUntil(until), false)
	states := e.waitForViewerStateUpdates(memberID, chatID, 1)
	require.NoError(t, protoutil.ProtoEqualError(want, states[0]))
	require.NoError(t, protoutil.ProtoEqualError(want, e.getChat(memberKeys, chatID).Metadata.ViewerState))
	require.Empty(t, e.viewerStateUpdatesOnUserTopic(e.userID, chatID))
	require.Empty(t, e.viewerStateUpdatesOnUserTopic(otherID, chatID))

	// The same silence again is the no-op: nothing is published.
	require.Equal(t, chatpb.SilenceUserResponse_OK, e.mustSilenceUser(e.keys, chatID, memberID, silenceUntil(until)))
	e.waitForViewerStateUpdates(memberID, chatID, 1)

	// A different one replaces it.
	require.Equal(t, chatpb.SilenceUserResponse_OK, e.mustSilenceUser(e.keys, chatID, memberID, silenceForever()))
	want = silencedViewerState(2, silenceForever(), false)
	states = e.waitForViewerStateUpdates(memberID, chatID, 2)
	require.NoError(t, protoutil.ProtoEqualError(want, states[1]))

	// It survives leaving and rejoining, which clears a mute but not a
	// silence.
	require.Equal(t, chatpb.LeaveChatResponse_OK, e.mustLeaveChat(memberKeys, chatID).Result)
	require.Equal(t, chatpb.JoinChatResponse_OK, e.mustJoinChat(memberKeys, chatID).Result)
	require.NoError(t, protoutil.ProtoEqualError(want, e.getChat(memberKeys, chatID).Metadata.ViewerState))

	// A silenced member gets no mention suggestions: they may not speak.
	require.Equal(t, chatpb.GetMentionSuggestionsResponse_DENIED, e.getMentionSuggestions(memberKeys, chatID).Result)

	// Lifting it reaches them too; lifting it again is the no-op.
	require.Equal(t, chatpb.UnsilenceUserResponse_OK, e.mustUnsilenceUser(e.keys, chatID, memberID))
	want = silencedViewerState(3, nil, false)
	states = e.waitForViewerStateUpdates(memberID, chatID, 3)
	require.NoError(t, protoutil.ProtoEqualError(want, states[2]))
	require.NoError(t, protoutil.ProtoEqualError(want, e.getChat(memberKeys, chatID).Metadata.ViewerState))
	require.Equal(t, chatpb.UnsilenceUserResponse_OK, e.mustUnsilenceUser(e.keys, chatID, memberID))
	e.waitForViewerStateUpdates(memberID, chatID, 3)
	require.Equal(t, chatpb.GetMentionSuggestionsResponse_OK, e.getMentionSuggestions(memberKeys, chatID).Result)

	// A silence that has lapsed no longer stops them, but is still shown as
	// recorded until it is replaced or lifted.
	_, _, err := s.SetSilence(e.ctx, chatID, memberID, chat.Silence{Until: time.Now().Add(-time.Minute).Truncate(time.Second)}, time.Now())
	require.NoError(t, err)
	require.Equal(t, chatpb.GetMentionSuggestionsResponse_OK, e.getMentionSuggestions(memberKeys, chatID).Result)
	require.NotNil(t, e.getChat(memberKeys, chatID).Metadata.ViewerState.GetRestrictions().GetSilence())
}

// testServer_Moderation_SilenceNonMember pins a silence on a user who is not
// a member: it is recorded and announced to no one, and they find it on their
// ViewerState when they join.
func testServer_Moderation_SilenceNonMember(t *testing.T, s chat.Store) {
	e := newServerEnv(t, s)

	outsiderID, outsiderKeys := e.addUser()
	chatID := e.putOwnedGroup("Group", nil)

	require.Equal(t, chatpb.SilenceUserResponse_OK, e.mustSilenceUser(e.keys, chatID, outsiderID, silenceForever()))
	time.Sleep(100 * time.Millisecond)
	require.Empty(t, e.viewerStateUpdatesOnUserTopic(outsiderID, chatID))

	// A non-member is shown no ViewerState; once they join, the silence is
	// on it.
	require.Nil(t, e.getChat(outsiderKeys, chatID).Metadata.ViewerState)
	joined := e.mustJoinChat(outsiderKeys, chatID)
	require.Equal(t, chatpb.JoinChatResponse_OK, joined.Result)
	require.NoError(t, protoutil.ProtoEqualError(silencedViewerState(1, silenceForever(), false), joined.Chat.ViewerState))
}

// testServer_Moderation_BanMember pins a ban of a member of a public group:
// a departure announced with reason BANNED to them and naming no one to the
// rest, their mute cleared, joining refused until unbanned, and no
// readmission on the unban.
func testServer_Moderation_BanMember(t *testing.T, s chat.Store) {
	e := newServerEnv(t, s)

	memberID, memberKeys := e.addUser()
	otherID, _ := e.addUser()
	chatID := e.putOwnedGroup("Group", nil, memberID, otherID)
	require.Equal(t, chatpb.MuteChatResponse_OK, e.mustMuteChat(memberKeys, chatID, muteForever()).Result)
	e.waitForViewerStateUpdates(memberID, chatID, 1)

	require.Equal(t, chatpb.BanUserResponse_OK, e.mustBanUser(e.keys, chatID, memberID))

	isMember, err := s.IsMember(e.ctx, chatID, memberID)
	require.NoError(t, err)
	require.False(t, isMember)

	// Announced as a departure: to the banned user with the reason, to the
	// members naming no one, with the banned user excluded.
	e.waitForRosterUpdates(memberID, chatID, 1)
	summary := &chatpb.RosterSummary{MemberCount: 2, Version: 1}
	toMembers, excludes := e.rosterUpdatesOnChatTopic(chatID)
	require.Len(t, toMembers, 1)
	require.NotNil(t, toMembers[0].GetMembershipChanged())
	require.NoError(t, protoutil.ProtoEqualError(summary, toMembers[0].RosterSummary))
	require.Len(t, excludes[0], 1)
	require.Equal(t, memberID.Value, excludes[0][0].Value)
	toBanned := e.rosterUpdatesOnUserTopic(memberID, chatID)
	require.Len(t, toBanned, 1)
	require.Equal(t, memberID.Value, toBanned[0].GetMemberLeft().GetUserId().GetValue())
	require.NoError(t, protoutil.ProtoEqualError(summary, toBanned[0].RosterSummary))
	require.Empty(t, e.rosterUpdatesOnUserTopic(otherID, chatID))

	// Their mute is cleared as a leave clears one.
	states := e.waitForViewerStateUpdates(memberID, chatID, 2)
	require.Nil(t, states[1].GetSettings().GetMute())
	require.Nil(t, states[1].GetPermissions().GetModeration())
	viewerStates, err := s.GetViewerStates(e.ctx, memberID, []*commonpb.ChatId{chatID})
	require.NoError(t, err)
	require.Nil(t, viewerStates[string(chatID.Value)].Mute)
	require.True(t, viewerStates[string(chatID.Value)].IsBanned())

	// A repeated ban is the no-op: nothing more is announced.
	require.Equal(t, chatpb.BanUserResponse_OK, e.mustBanUser(e.keys, chatID, memberID))
	time.Sleep(100 * time.Millisecond)
	require.Len(t, e.rosterUpdatesOnUserTopic(memberID, chatID), 1)

	// They cannot rejoin until unbanned, and the unban does not readmit them.
	require.Equal(t, chatpb.JoinChatResponse_BANNED, e.mustJoinChat(memberKeys, chatID).Result)
	require.Equal(t, chatpb.UnbanUserResponse_OK, e.mustUnbanUser(e.keys, chatID, memberID))
	isMember, err = s.IsMember(e.ctx, chatID, memberID)
	require.NoError(t, err)
	require.False(t, isMember)
	require.Equal(t, chatpb.JoinChatResponse_OK, e.mustJoinChat(memberKeys, chatID).Result)
}

// testServer_Moderation_BanNonMember pins a ban of a user who has never
// joined: nothing is announced, and they cannot join until unbanned.
func testServer_Moderation_BanNonMember(t *testing.T, s chat.Store) {
	e := newServerEnv(t, s)

	outsiderID, outsiderKeys := e.addUser()
	chatID := e.putOwnedGroup("Group", nil)

	require.Equal(t, chatpb.BanUserResponse_OK, e.mustBanUser(e.keys, chatID, outsiderID))
	e.requireNoRosterUpdates(outsiderID, chatID)
	require.Empty(t, e.lobbyUpdatesOnUserTopic(e.userID, chatID))

	require.Equal(t, chatpb.JoinChatResponse_BANNED, e.mustJoinChat(outsiderKeys, chatID).Result)
	require.Equal(t, chatpb.UnbanUserResponse_OK, e.mustUnbanUser(e.keys, chatID, outsiderID))
	require.Equal(t, chatpb.JoinChatResponse_OK, e.mustJoinChat(outsiderKeys, chatID).Result)
}

// testServer_Moderation_BanPrivateGroup pins a ban in a private group: a
// member loses their key envelope with their membership, a user waiting in
// the lobby loses their entry (told to the creator as any departure from the
// lobby is), and a banned user can neither enter the lobby nor be admitted
// until unbanned.
func testServer_Moderation_BanPrivateGroup(t *testing.T, s chat.Store) {
	e := newServerEnv(t, s)

	memberID, _ := e.addUser()
	waitingID, waitingKeys := e.addBoundUser("Waiting")
	chatID := e.putKeyedPrivateGroup("Private", memberID)
	_, err := s.SetKeyEnvelope(e.ctx, chatID, memberID, chat.KeyEnvelopeFromProto(keyEnvelopeProto(2), e.userID))
	require.NoError(t, err)

	// A member: their envelope goes with them.
	require.Equal(t, chatpb.BanUserResponse_OK, e.mustBanUser(e.keys, chatID, memberID))
	e.waitForRosterUpdates(memberID, chatID, 1)
	require.Equal(t, memberID.Value, e.rosterUpdatesOnUserTopic(memberID, chatID)[0].GetMemberLeft().GetUserId().GetValue())
	_, err = s.GetKeyEnvelope(e.ctx, chatID, memberID)
	require.ErrorIs(t, err, chat.ErrKeyEnvelopeNotFound)
	// The creator's is untouched.
	_, err = s.GetKeyEnvelope(e.ctx, chatID, e.userID)
	require.NoError(t, err)

	// A user in the lobby: their entry goes, and the creator hears of it as
	// of any departure from the lobby.
	require.Equal(t, chatpb.EnterLobbyResponse_OK, e.enterLobby(waitingKeys, chatID).Result)
	e.waitForLobbyUpdates(e.userID, chatID, 1)
	require.Equal(t, chatpb.BanUserResponse_OK, e.mustBanUser(e.keys, chatID, waitingID))
	updates := e.waitForLobbyUpdates(e.userID, chatID, 2)
	require.Equal(t, waitingID.Value, updates[1].GetMemberLeft().GetUserId().GetValue())
	entries, err := s.GetLobbyEntries(e.ctx, waitingID, []*commonpb.ChatId{chatID})
	require.NoError(t, err)
	require.Empty(t, entries)
	e.requireNoLobbyUpdatesOnChatTopic(chatID)

	// Banned: no entry, and so no admission.
	require.Equal(t, chatpb.EnterLobbyResponse_BANNED, e.enterLobby(waitingKeys, chatID).Result)
	require.Equal(t, chatpb.AdmitLobbyMemberResponse_NOT_IN_LOBBY, e.admitLobbyMember(e.keys, chatID, waitingID, keyEnvelopeProto(3)).Result)

	// Unbanned, they enter again like anyone, and may be admitted.
	require.Equal(t, chatpb.UnbanUserResponse_OK, e.mustUnbanUser(e.keys, chatID, waitingID))
	require.Equal(t, chatpb.EnterLobbyResponse_OK, e.enterLobby(waitingKeys, chatID).Result)
	require.Equal(t, chatpb.AdmitLobbyMemberResponse_OK, e.admitLobbyMember(e.keys, chatID, waitingID, keyEnvelopeProto(3)).Result)
}

// testServer_Moderation_BannedReads pins what a banned non-member reads of a
// public group: no full read, whatever its rules say of them, and the
// redacted read anyone has.
func testServer_Moderation_BannedReads(t *testing.T, s chat.Store) {
	e := newServerEnv(t, s)

	outsiderID, outsiderKeys := e.addUser()
	chatID := chat.MustGenerateGroupChatID()
	require.NoError(t, s.PutChat(e.ctx, &chat.Chat{
		ID:            chatID,
		Type:          chatpb.ChatType_GROUP,
		Members:       []*commonpb.UserId{e.userID},
		Title:         "Open",
		CreatorID:     e.userID,
		LastActivity:  at(1),
		LastMessageID: &messagingpb.MessageId{Value: 7},
	}))
	key := string(chatID.Value)
	e.messaging.lastMessages[key] = textMessage(7, e.userID, "hello")
	e.messaging.latestEventSeqs[key] = 7

	// An open group: read in full by any non-member.
	resp := e.getChat(outsiderKeys, chatID)
	require.Equal(t, chatpb.GetChatResponse_OK, resp.Result)
	require.False(t, resp.Metadata.GetLastMessage().GetRedacted())
	require.NotNil(t, resp.Metadata.GetLastMessage())

	require.Equal(t, chatpb.BanUserResponse_OK, e.mustBanUser(e.keys, chatID, outsiderID))

	// Banned: the record alone under FULL, and redacted where redaction is
	// allowed.
	resp = e.getChat(outsiderKeys, chatID)
	require.Equal(t, chatpb.GetChatResponse_OK, resp.Result)
	require.Nil(t, resp.Metadata.LastMessage)
	for _, mode := range []messagingpb.ViewMode{messagingpb.ViewMode_FULL_OR_REDACTED, messagingpb.ViewMode_REDACTED} {
		resp = e.getChatWithMode(outsiderKeys, chatID, mode)
		require.Equal(t, chatpb.GetChatResponse_OK, resp.Result)
		require.True(t, resp.Metadata.GetLastMessage().GetRedacted(), mode)
	}

	// Unbanned, they read in full again at once.
	require.Equal(t, chatpb.UnbanUserResponse_OK, e.mustUnbanUser(e.keys, chatID, outsiderID))
	resp = e.getChat(outsiderKeys, chatID)
	require.False(t, resp.Metadata.GetLastMessage().GetRedacted())
	require.NotNil(t, resp.Metadata.GetLastMessage())
}

// testServer_Moderation_GetRestrictedUsers pins the listing's content: every
// user banned or silenced, most recently restricted first, each with their
// profile, their silence while in force and the time of their ban, and no one
// whose only restriction is a silence that has lapsed.
func testServer_Moderation_GetRestrictedUsers(t *testing.T, s chat.Store) {
	e := newServerEnv(t, s)

	silenced, banned, both, lapsed := model.MustGenerateUserID(), model.MustGenerateUserID(), model.MustGenerateUserID(), model.MustGenerateUserID()
	e.profiles.usernames[string(silenced.Value)] = "silenced"
	chatID := e.putOwnedGroup("Group", nil, silenced, banned)

	until := time.Now().Add(time.Hour).Truncate(time.Second)
	_, _, err := s.SetSilence(e.ctx, chatID, silenced, chat.Silence{Until: until}, at(10))
	require.NoError(t, err)
	_, err = s.BanUser(e.ctx, chatID, banned, at(20), false)
	require.NoError(t, err)
	_, _, err = s.SetSilence(e.ctx, chatID, both, chat.Silence{Forever: true}, at(30))
	require.NoError(t, err)
	_, err = s.BanUser(e.ctx, chatID, both, at(40), false)
	require.NoError(t, err)
	_, _, err = s.SetSilence(e.ctx, chatID, lapsed, chat.Silence{Until: time.Now().Add(-time.Hour).Truncate(time.Second)}, at(50))
	require.NoError(t, err)
	e.waitForRestrictedListing(chatID, 4)

	resp := e.mustGetRestrictedUsers(e.keys, chatID, nil)
	require.Equal(t, chatpb.GetRestrictedUsersResponse_OK, resp.Result)
	require.False(t, resp.HasMore)
	require.Nil(t, resp.PagingToken)
	require.Equal(t, [][]byte{both.Value, banned.Value, silenced.Value}, restrictedUserIDs(resp.Users))

	require.NoError(t, protoutil.ProtoEqualError(silenceForever(), resp.Users[0].Silence))
	require.True(t, resp.Users[0].GetBan().GetBannedAt().AsTime().Equal(at(40)))
	require.Nil(t, resp.Users[1].Silence)
	require.True(t, resp.Users[1].GetBan().GetBannedAt().AsTime().Equal(at(20)))
	require.NoError(t, protoutil.ProtoEqualError(silenceUntil(until), resp.Users[2].Silence))
	require.Nil(t, resp.Users[2].Ban)
	require.Equal(t, "silenced", resp.Users[2].UserProfile.GetUsername().GetValue())
	for _, u := range resp.Users {
		require.Equal(t, u.UserId.Value, u.UserProfile.GetUserId().GetValue())
	}

	// Lifted restrictions leave the listing.
	require.Equal(t, chatpb.UnbanUserResponse_OK, e.mustUnbanUser(e.keys, chatID, banned))
	require.Equal(t, chatpb.UnsilenceUserResponse_OK, e.mustUnsilenceUser(e.keys, chatID, lapsed))
	e.waitForRestrictedListing(chatID, 2)
	resp = e.mustGetRestrictedUsers(e.keys, chatID, nil)
	require.Equal(t, [][]byte{both.Value, silenced.Value}, restrictedUserIDs(resp.Users))
}

// testServer_Moderation_GetRestrictedUsers_Paging pins the listing's paging:
// exact pages with an exact has_more, lapsed silences skipped without
// shrinking a page, a page that runs out of reads ending early with a token
// past what it skipped, and a token bound to its chat.
func testServer_Moderation_GetRestrictedUsers_Paging(t *testing.T, s chat.Store) {
	e := newServerEnv(t, s)

	chatID := e.putOwnedGroup("Group", nil)
	other := e.putOwnedGroup("Other", nil)

	// Five active silences at 1..5, and a lapsed one at 6, listed first.
	var active []*commonpb.UserId
	for i := int64(1); i <= 5; i++ {
		userID := model.MustGenerateUserID()
		_, _, err := s.SetSilence(e.ctx, chatID, userID, chat.Silence{Forever: true}, at(i))
		require.NoError(t, err)
		active = append(active, userID)
	}
	_, _, err := s.SetSilence(e.ctx, chatID, model.MustGenerateUserID(), chat.Silence{Until: time.Now().Add(-time.Hour).Truncate(time.Second)}, at(6))
	require.NoError(t, err)
	e.waitForRestrictedListing(chatID, 6)

	var pages [][][]byte
	var token *commonpb.PagingToken
	for {
		resp := e.mustGetRestrictedUsers(e.keys, chatID, &commonpb.QueryOptions{PageSize: 2, PagingToken: token})
		require.Equal(t, chatpb.GetRestrictedUsersResponse_OK, resp.Result)
		pages = append(pages, restrictedUserIDs(resp.Users))
		if !resp.HasMore {
			require.Nil(t, resp.PagingToken)
			break
		}
		token = resp.PagingToken
	}
	require.Equal(t, [][][]byte{
		{active[4].Value, active[3].Value},
		{active[2].Value, active[1].Value},
		{active[0].Value},
	}, pages)

	// A token is refused with another chat.
	first := e.mustGetRestrictedUsers(e.keys, chatID, &commonpb.QueryOptions{PageSize: 2})
	require.True(t, first.HasMore)
	_, err = e.getRestrictedUsers(e.keys, other, &commonpb.QueryOptions{PageSize: 2, PagingToken: first.PagingToken})
	require.Equal(t, codes.InvalidArgument, status.Code(err))

	// A run of lapsed silences longer than a page's reads can cover ends the
	// page early, empty, with a token past them; the next page resumes there.
	crowded := e.putOwnedGroup("Crowded", nil)
	survivor := model.MustGenerateUserID()
	_, _, err = s.SetSilence(e.ctx, crowded, survivor, chat.Silence{Forever: true}, at(1))
	require.NoError(t, err)
	for i := int64(2); i <= 9; i++ {
		_, _, err := s.SetSilence(e.ctx, crowded, model.MustGenerateUserID(), chat.Silence{Until: time.Now().Add(-time.Hour).Truncate(time.Second)}, at(i))
		require.NoError(t, err)
	}
	e.waitForRestrictedListing(crowded, 9)
	resp := e.mustGetRestrictedUsers(e.keys, crowded, &commonpb.QueryOptions{PageSize: 1})
	require.Empty(t, resp.Users)
	require.True(t, resp.HasMore)
	resp = e.mustGetRestrictedUsers(e.keys, crowded, &commonpb.QueryOptions{PageSize: 1, PagingToken: resp.PagingToken})
	require.Equal(t, [][]byte{survivor.Value}, restrictedUserIDs(resp.Users))
	require.False(t, resp.HasMore)
}

// testServer_Moderation_MentionSuggestions pins that a banned user is not
// suggested while a silenced one still is.
func testServer_Moderation_MentionSuggestions(t *testing.T, s chat.Store) {
	e := newServerEnv(t, s)

	silenced, banned := model.MustGenerateUserID(), model.MustGenerateUserID()
	e.profiles.usernames[string(silenced.Value)] = "silenced"
	e.profiles.usernames[string(banned.Value)] = "banned"
	chatID := e.putOwnedGroup("Group", nil, silenced, banned)
	e.recordSend(chatID, silenced, at(10))
	e.recordSend(chatID, banned, at(20))

	require.Equal(t, chatpb.SilenceUserResponse_OK, e.mustSilenceUser(e.keys, chatID, silenced, silenceForever()))
	require.Equal(t, chatpb.BanUserResponse_OK, e.mustBanUser(e.keys, chatID, banned))

	// The ban is found with an eventually consistent read.
	require.Eventually(t, func() bool {
		resp := e.getMentionSuggestions(e.keys, chatID)
		require.Equal(t, chatpb.GetMentionSuggestionsResponse_OK, resp.Result)
		got := suggestedUserIDs(resp.Suggestions)
		return len(got) == 1 && string(got[0]) == string(silenced.Value)
	}, 5*time.Second, 10*time.Millisecond)
}

// testServer_Moderation_MalformedTarget pins that a target which is not a
// well-formed user ID is refused as malformed by every write, before
// anything is read or recorded.
func testServer_Moderation_MalformedTarget(t *testing.T, s chat.Store) {
	e := newServerEnv(t, s)
	chatID := e.putOwnedGroup("Group", nil)

	for _, value := range [][]byte{{1}, make([]byte, 32)} {
		target := &commonpb.UserId{Value: value}

		_, err := e.silenceUser(e.keys, chatID, target, silenceForever())
		require.Equal(t, codes.InvalidArgument, status.Code(err))

		unsilence := &chatpb.UnsilenceUserRequest{ChatId: chatID, UserId: target}
		require.NoError(t, e.keys.Auth(unsilence, &unsilence.Auth))
		_, err = e.client.UnsilenceUser(e.ctx, unsilence)
		require.Equal(t, codes.InvalidArgument, status.Code(err))

		ban := &chatpb.BanUserRequest{ChatId: chatID, UserId: target}
		require.NoError(t, e.keys.Auth(ban, &ban.Auth))
		_, err = e.client.BanUser(e.ctx, ban)
		require.Equal(t, codes.InvalidArgument, status.Code(err))

		unban := &chatpb.UnbanUserRequest{ChatId: chatID, UserId: target}
		require.NoError(t, e.keys.Auth(unban, &unban.Auth))
		_, err = e.client.UnbanUser(e.ctx, unban)
		require.Equal(t, codes.InvalidArgument, status.Code(err))
	}

	resp := e.mustGetRestrictedUsers(e.keys, chatID, nil)
	require.Empty(t, resp.Users)
}

// testServer_Moderation_GetRestrictedUsers_UnknownUser pins that a restricted
// user the profile domain does not know is left out of the listing rather
// than failing it, so the creator pages past them.
func testServer_Moderation_GetRestrictedUsers_UnknownUser(t *testing.T, s chat.Store) {
	e := newServerEnv(t, s)
	chatID := e.putOwnedGroup("Group", nil)

	known, unknown, last := model.MustGenerateUserID(), model.MustGenerateUserID(), model.MustGenerateUserID()
	e.profiles.unknown[string(unknown.Value)] = struct{}{}
	for i, userID := range []*commonpb.UserId{known, unknown, last} {
		_, err := s.BanUser(e.ctx, chatID, userID, at(int64(10*(i+1))), false)
		require.NoError(t, err)
	}
	e.waitForRestrictedListing(chatID, 3)

	resp := e.mustGetRestrictedUsers(e.keys, chatID, nil)
	require.Equal(t, chatpb.GetRestrictedUsersResponse_OK, resp.Result)
	require.Equal(t, [][]byte{last.Value, known.Value}, restrictedUserIDs(resp.Users))

	// Paged one at a time, the walk passes the unknown user: the page that
	// read them comes back empty, with a token past them.
	var walked [][]byte
	opts := &commonpb.QueryOptions{PageSize: 1}
	for {
		resp = e.mustGetRestrictedUsers(e.keys, chatID, opts)
		require.Equal(t, chatpb.GetRestrictedUsersResponse_OK, resp.Result)
		walked = append(walked, restrictedUserIDs(resp.Users)...)
		if !resp.HasMore {
			break
		}
		opts = &commonpb.QueryOptions{PageSize: 1, PagingToken: resp.PagingToken}
	}
	require.Equal(t, [][]byte{last.Value, known.Value}, walked)
}
