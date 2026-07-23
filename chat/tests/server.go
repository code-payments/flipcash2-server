package tests

import (
	"context"
	"encoding/hex"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	blobpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/blob/v1"
	chatpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/chat/v1"
	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"
	messagingpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/messaging/v1"
	phonepb "github.com/code-payments/flipcash2-protobuf-api/generated/go/phone/v1"

	"github.com/code-payments/flipcash2-server/auth"
	"github.com/code-payments/flipcash2-server/chat"
	"github.com/code-payments/flipcash2-server/model"
	"github.com/code-payments/flipcash2-server/testutil"
)

// RunServerTests runs the shared chat.Server test suite against s. teardown is
// called between tests to reset the store.
func RunServerTests(t *testing.T, s chat.Store, teardown func()) {
	for _, tf := range []func(t *testing.T, s chat.Store){
		testServer_GetChat_OK,
		testServer_GetChat_NotFound,
		testServer_GetChat_Denied,
		testServer_GetChat_Hydrates,
		testServer_GetChat_TipDm_HidesPhoneNumbers,
		testServer_GetChat_HiddenWhenPeerBlocked,
		testServer_GetDmChatFeed_Empty,
		testServer_GetDmChatFeed_OrderAndContent,
		testServer_GetDmChatFeed_Paging,
		testServer_GetDmChatFeed_Hydrates,
		testServer_GetDmChatFeed_TypeScoped,
		testServer_GetDmChatFeed_TokenBoundToType,
		testServer_GetDmChatFeed_HiddenPerViewer,
	} {
		tf(t, s)
		teardown()
	}
}

type serverEnv struct {
	t         *testing.T
	ctx       context.Context
	client    chatpb.ChatClient
	authz     *auth.StaticAuthorizer
	store     chat.Store
	messaging *fakeMessagingReader
	profiles  *fakeProfileReader
	blocklist *fakeBlocklistReader

	userID *commonpb.UserId
	keys   model.KeyPair
}

func newServerEnv(t *testing.T, s chat.Store) *serverEnv {
	ctx := context.Background()
	log := zaptest.NewLogger(t)

	authz := auth.NewStaticAuthorizer(log)
	userID := model.MustGenerateUserID()
	keys := model.MustGenerateKeyPair()
	authz.Add(userID, keys)

	messaging := newFakeMessagingReader()
	profiles := newFakeProfileReader()
	blocklist := newFakeBlocklistReader()
	server := chat.NewServer(log, authz, s, messaging, profiles, blocklist)
	cc := testutil.RunGRPCServer(t, log, testutil.WithService(func(s *grpc.Server) {
		chatpb.RegisterChatServer(s, server)
	}))

	return &serverEnv{
		t:         t,
		ctx:       ctx,
		client:    chatpb.NewChatClient(cc),
		authz:     authz,
		store:     s,
		messaging: messaging,
		profiles:  profiles,
		blocklist: blocklist,
		userID:    userID,
		keys:      keys,
	}
}

// fakeMessagingReader is a canned chat.MessagingReader for server tests: it
// returns whatever last messages, pointers, and head event sequences a test
// registers per chat.
type fakeMessagingReader struct {
	lastMessages    map[string]*messagingpb.Message
	pointers        map[string][]*messagingpb.Pointer
	latestEventSeqs map[string]uint64
}

func newFakeMessagingReader() *fakeMessagingReader {
	return &fakeMessagingReader{
		lastMessages:    make(map[string]*messagingpb.Message),
		pointers:        make(map[string][]*messagingpb.Pointer),
		latestEventSeqs: make(map[string]uint64),
	}
}

func (f *fakeMessagingReader) LastMessages(_ context.Context, refs []chat.MessageRef) (map[string]*messagingpb.Message, error) {
	out := make(map[string]*messagingpb.Message)
	for _, ref := range refs {
		if m, ok := f.lastMessages[string(ref.ChatID.Value)]; ok {
			out[string(ref.ChatID.Value)] = m
		}
	}
	return out, nil
}

func (f *fakeMessagingReader) Pointers(_ context.Context, refs []chat.PointerRef) (map[string][]*messagingpb.Pointer, error) {
	out := make(map[string][]*messagingpb.Pointer)
	for _, ref := range refs {
		if p, ok := f.pointers[string(ref.ChatID.Value)]; ok {
			out[string(ref.ChatID.Value)] = p
		}
	}
	return out, nil
}

func (f *fakeMessagingReader) LatestEventSequences(_ context.Context, chatIDs []*commonpb.ChatId) (map[string]uint64, error) {
	out := make(map[string]uint64)
	for _, chatID := range chatIDs {
		if seq, ok := f.latestEventSeqs[string(chatID.Value)]; ok {
			out[string(chatID.Value)] = seq
		}
	}
	return out, nil
}

// fakeProfileReader is a canned chat.ProfileReader for server tests: it returns
// whatever phone number, display name and profile picture a test registers per user
// ID.
type fakeProfileReader struct {
	phoneNumbers    map[string]*phonepb.PhoneNumber
	displayNames    map[string]string
	profilePictures map[string]*blobpb.Media
}

func newFakeProfileReader() *fakeProfileReader {
	return &fakeProfileReader{
		phoneNumbers:    make(map[string]*phonepb.PhoneNumber),
		displayNames:    make(map[string]string),
		profilePictures: make(map[string]*blobpb.Media),
	}
}

// setProfilePicture registers a picture for a user, already hydrated the way the
// real reader returns it — the renditions carry their resolved blob metadata.
func (f *fakeProfileReader) setProfilePicture(userID *commonpb.UserId, blobID *blobpb.BlobId) *blobpb.Media {
	picture := &blobpb.Media{
		Renditions: []*blobpb.Rendition{{
			Role:   blobpb.Rendition_ORIGINAL,
			BlobId: blobID,
			Blob: &blobpb.BlobMetadata{
				MimeType:  "image/jpeg",
				SizeBytes: 1024,
				DownloadUrl: &blobpb.DownloadUrl{
					Url:       "https://cdn.blobs.test/" + hex.EncodeToString(blobID.Value),
					ExpiresAt: timestamppb.New(at(1).Add(time.Hour)),
				},
			},
		}},
	}
	f.profilePictures[string(userID.Value)] = picture
	return picture
}

func (f *fakeProfileReader) GetPhoneNumbers(_ context.Context, userIDs []*commonpb.UserId) (map[string]*phonepb.PhoneNumber, error) {
	out := make(map[string]*phonepb.PhoneNumber)
	for _, userID := range userIDs {
		if p, ok := f.phoneNumbers[string(userID.Value)]; ok {
			out[string(userID.Value)] = p
		}
	}
	return out, nil
}

func (f *fakeProfileReader) GetDisplayNames(_ context.Context, userIDs []*commonpb.UserId) (map[string]string, error) {
	out := make(map[string]string)
	for _, userID := range userIDs {
		if d, ok := f.displayNames[string(userID.Value)]; ok {
			out[string(userID.Value)] = d
		}
	}
	return out, nil
}

func (f *fakeProfileReader) GetProfilePictures(_ context.Context, userIDs []*commonpb.UserId) (map[string]*blobpb.Media, error) {
	out := make(map[string]*blobpb.Media)
	for _, userID := range userIDs {
		if p, ok := f.profilePictures[string(userID.Value)]; ok {
			out[string(userID.Value)] = p
		}
	}
	return out, nil
}

// fakeBlocklistReader is a canned chat.BlocklistReader for server tests: it
// reports, for each owner, which candidates the test has registered as blocked.
type fakeBlocklistReader struct {
	// blocked maps an owner to the set of user IDs they have blocked.
	blocked map[string]map[string]bool
}

func newFakeBlocklistReader() *fakeBlocklistReader {
	return &fakeBlocklistReader{blocked: make(map[string]map[string]bool)}
}

// block registers that owner has blocked blockedID.
func (f *fakeBlocklistReader) block(owner, blockedID *commonpb.UserId) {
	set, ok := f.blocked[string(owner.Value)]
	if !ok {
		set = make(map[string]bool)
		f.blocked[string(owner.Value)] = set
	}
	set[string(blockedID.Value)] = true
}

func (f *fakeBlocklistReader) GetBlocked(_ context.Context, ownerID *commonpb.UserId, candidateIDs []*commonpb.UserId) (map[string]bool, error) {
	set := f.blocked[string(ownerID.Value)]
	if len(set) == 0 {
		return nil, nil
	}
	out := make(map[string]bool)
	for _, c := range candidateIDs {
		if set[string(c.Value)] {
			out[string(c.Value)] = true
		}
	}
	return out, nil
}

// putDM persists a contact DM the env user is a member of, with the given last
// activity.
func (e *serverEnv) putDM(lastActivity time.Time) *commonpb.ChatId {
	return e.putDMOfType(chatpb.ChatType_CONTACT_DM, lastActivity)
}

// putDMOfType persists a DM of the given type the env user is a member of,
// with the given last activity.
func (e *serverEnv) putDMOfType(chatType chatpb.ChatType, lastActivity time.Time) *commonpb.ChatId {
	return e.putDMWithPeer(chatType, model.MustGenerateUserID(), lastActivity)
}

// putDMWithPeer persists a DM of the given type between the env user and the
// given peer, with the given last activity. It lets a test control the peer's
// identity (e.g. to then block it).
func (e *serverEnv) putDMWithPeer(chatType chatpb.ChatType, peer *commonpb.UserId, lastActivity time.Time) *commonpb.ChatId {
	chatID := generateChatID()
	require.NoError(e.t, e.store.PutChat(e.ctx, &chat.Chat{
		ID:           chatID,
		Type:         chatType,
		Members:      []*commonpb.UserId{e.userID, peer},
		LastActivity: lastActivity,
	}))
	return chatID
}

func (e *serverEnv) getChat(keys model.KeyPair, chatID *commonpb.ChatId) *chatpb.GetChatResponse {
	req := &chatpb.GetChatRequest{ChatId: chatID}
	require.NoError(e.t, keys.Auth(req, &req.Auth))
	resp, err := e.client.GetChat(e.ctx, req)
	require.NoError(e.t, err)
	return resp
}

func (e *serverEnv) getDmFeed(opts *commonpb.QueryOptions) *chatpb.GetDmChatFeedResponse {
	resp, err := e.getDmFeedOfType(chatpb.ChatType_CONTACT_DM, opts)
	require.NoError(e.t, err)
	return resp
}

func (e *serverEnv) getDmFeedOfType(chatType chatpb.ChatType, opts *commonpb.QueryOptions) (*chatpb.GetDmChatFeedResponse, error) {
	req := &chatpb.GetDmChatFeedRequest{DmChatType: chatType, QueryOptions: opts}
	require.NoError(e.t, e.keys.Auth(req, &req.Auth))
	return e.client.GetDmChatFeed(e.ctx, req)
}

func testServer_GetChat_OK(t *testing.T, s chat.Store) {
	e := newServerEnv(t, s)

	chatID := e.putDM(at(1))

	resp := e.getChat(e.keys, chatID)
	require.Equal(t, chatpb.GetChatResponse_OK, resp.Result)
	require.NotNil(t, resp.Metadata)
	require.Equal(t, chatID.Value, resp.Metadata.ChatId.Value)
	require.Equal(t, chatpb.ChatType_CONTACT_DM, resp.Metadata.Type)
	require.Len(t, resp.Metadata.Members, 2)
	require.Equal(t, e.userID.Value, resp.Metadata.Members[0].UserId.Value)
	require.True(t, resp.Metadata.LastActivity.AsTime().Equal(at(1)))
}

func testServer_GetChat_NotFound(t *testing.T, s chat.Store) {
	e := newServerEnv(t, s)

	resp := e.getChat(e.keys, generateChatID())
	require.Equal(t, chatpb.GetChatResponse_NOT_FOUND, resp.Result)
	require.Nil(t, resp.Metadata)
}

func testServer_GetChat_Denied(t *testing.T, s chat.Store) {
	e := newServerEnv(t, s)

	chatID := e.putDM(at(1))

	// A registered user who is not a member of the chat is denied.
	strangerID := model.MustGenerateUserID()
	strangerKeys := model.MustGenerateKeyPair()
	e.authz.Add(strangerID, strangerKeys)

	resp := e.getChat(strangerKeys, chatID)
	require.Equal(t, chatpb.GetChatResponse_DENIED, resp.Result)
	require.Nil(t, resp.Metadata)
}

func testServer_GetChat_HiddenWhenPeerBlocked(t *testing.T, s chat.Store) {
	e := newServerEnv(t, s)

	// A DM whose peer the viewer has blocked comes back hidden.
	blockedPeer := model.MustGenerateUserID()
	hidden := e.putDMWithPeer(chatpb.ChatType_CONTACT_DM, blockedPeer, at(1))
	e.blocklist.block(e.userID, blockedPeer)

	resp := e.getChat(e.keys, hidden)
	require.Equal(t, chatpb.GetChatResponse_OK, resp.Result)
	require.True(t, resp.Metadata.IsHidden)

	// A DM whose peer the viewer has not blocked stays visible — proving the flag
	// tracks the block, not merely DM-ness.
	visible := e.putDMWithPeer(chatpb.ChatType_CONTACT_DM, model.MustGenerateUserID(), at(1))
	resp = e.getChat(e.keys, visible)
	require.Equal(t, chatpb.GetChatResponse_OK, resp.Result)
	require.False(t, resp.Metadata.IsHidden)
}

func testServer_GetDmChatFeed_Empty(t *testing.T, s chat.Store) {
	e := newServerEnv(t, s)

	resp := e.getDmFeed(&commonpb.QueryOptions{})
	require.Equal(t, chatpb.GetDmChatFeedResponse_OK, resp.Result)
	require.Empty(t, resp.Chats)
	require.False(t, resp.HasMore)
	require.Nil(t, resp.PagingToken)
}

func testServer_GetDmChatFeed_HiddenPerViewer(t *testing.T, s chat.Store) {
	e := newServerEnv(t, s)

	// Two DMs in the viewer's feed; the peer of one is blocked. A single batched
	// blocklist read must mark exactly that chat hidden.
	blockedPeer := model.MustGenerateUserID()
	hidden := e.putDMWithPeer(chatpb.ChatType_CONTACT_DM, blockedPeer, at(2))
	visible := e.putDMWithPeer(chatpb.ChatType_CONTACT_DM, model.MustGenerateUserID(), at(1))
	e.blocklist.block(e.userID, blockedPeer)

	resp := e.getDmFeed(&commonpb.QueryOptions{})
	require.Equal(t, chatpb.GetDmChatFeedResponse_OK, resp.Result)
	require.Len(t, resp.Chats, 2)

	byID := make(map[string]*chatpb.Metadata, len(resp.Chats))
	for _, c := range resp.Chats {
		byID[string(c.ChatId.Value)] = c
	}
	require.True(t, byID[string(hidden.Value)].IsHidden)
	require.False(t, byID[string(visible.Value)].IsHidden)
}

func testServer_GetDmChatFeed_OrderAndContent(t *testing.T, s chat.Store) {
	e := newServerEnv(t, s)

	// Persist out of order; the feed must return most-recent activity first.
	older := e.putDM(at(1))
	newer := e.putDM(at(2))

	resp := e.getDmFeed(&commonpb.QueryOptions{})
	require.Equal(t, chatpb.GetDmChatFeedResponse_OK, resp.Result)
	require.False(t, resp.HasMore)
	require.Len(t, resp.Chats, 2)

	require.Equal(t, newer.Value, resp.Chats[0].ChatId.Value)
	require.Equal(t, older.Value, resp.Chats[1].ChatId.Value)

	// The chat-domain metadata is populated for each entry.
	first := resp.Chats[0]
	require.Equal(t, chatpb.ChatType_CONTACT_DM, first.Type)
	require.Len(t, first.Members, 2)
	require.Equal(t, e.userID.Value, first.Members[0].UserId.Value)
	require.True(t, first.LastActivity.AsTime().Equal(at(2)))

	// A paging token is minted (it opaquely pins the snapshot and cursor).
	require.NotNil(t, resp.PagingToken)
}

func testServer_GetDmChatFeed_Paging(t *testing.T, s chat.Store) {
	e := newServerEnv(t, s)

	const total = 5
	want := make([][]byte, total)
	for i := 0; i < total; i++ {
		// Increasing activity, so DESC order is the reverse of insertion order.
		chatID := e.putDM(at(int64(i + 1)))
		want[total-1-i] = chatID.Value
	}

	var got [][]byte
	var token *commonpb.PagingToken
	for {
		resp := e.getDmFeed(&commonpb.QueryOptions{PageSize: 2, PagingToken: token})
		require.Equal(t, chatpb.GetDmChatFeedResponse_OK, resp.Result)
		require.LessOrEqual(t, len(resp.Chats), 2)
		for _, c := range resp.Chats {
			got = append(got, c.ChatId.Value)
		}
		if !resp.HasMore {
			break
		}
		require.NotNil(t, resp.PagingToken)
		token = resp.PagingToken
	}

	require.Equal(t, want, got)
}

func testServer_GetChat_Hydrates(t *testing.T, s chat.Store) {
	e := newServerEnv(t, s)

	peer := model.MustGenerateUserID()
	chatID := generateChatID()
	require.NoError(t, s.PutChat(e.ctx, &chat.Chat{
		ID:            chatID,
		Type:          chatpb.ChatType_CONTACT_DM,
		Members:       []*commonpb.UserId{e.userID, peer},
		LastActivity:  at(1),
		LastMessageID: &messagingpb.MessageId{Value: 7},
	}))

	e.messaging.lastMessages[string(chatID.Value)] = textMessage(7, peer, "hi")
	e.messaging.pointers[string(chatID.Value)] = []*messagingpb.Pointer{
		{Type: messagingpb.Pointer_READ, UserId: e.userID, Value: &messagingpb.MessageId{Value: 7}, Ts: timestamppb.New(at(7))},
		{Type: messagingpb.Pointer_DELIVERED, UserId: peer, Value: &messagingpb.MessageId{Value: 7}, Ts: timestamppb.New(at(7))},
	}
	// The head sits ahead of the last message's event sequence (e.g. an older
	// message was since edited), so this exercises that it is read from the
	// messaging reader rather than derived from last_message.
	e.messaging.latestEventSeqs[string(chatID.Value)] = 9
	e.profiles.phoneNumbers[string(peer.Value)] = &phonepb.PhoneNumber{Value: "+15551234567"}
	e.profiles.displayNames[string(peer.Value)] = "Peer Name"
	peerPicture := e.profiles.setProfilePicture(peer, &blobpb.BlobId{Value: make([]byte, 16)})

	resp := e.getChat(e.keys, chatID)
	require.Equal(t, chatpb.GetChatResponse_OK, resp.Result)

	// The last message is hydrated from the messaging reader.
	require.NotNil(t, resp.Metadata.LastMessage)
	require.Equal(t, uint64(7), resp.Metadata.LastMessage.MessageId.Value)
	require.Equal(t, "hi", resp.Metadata.LastMessage.Content[0].GetText().Text)

	// The chat's head event sequence is hydrated, independent of last_message.
	require.Equal(t, uint64(9), resp.Metadata.LatestEventSequence)

	// Pointers are distributed onto the matching member by user ID.
	members := byUserID(resp.Metadata.Members)
	require.Len(t, members[string(e.userID.Value)].Pointers, 1)
	require.Equal(t, messagingpb.Pointer_READ, members[string(e.userID.Value)].Pointers[0].Type)
	require.Len(t, members[string(peer.Value)].Pointers, 1)
	require.Equal(t, messagingpb.Pointer_DELIVERED, members[string(peer.Value)].Pointers[0].Type)

	// The other DM member's phone number, display name and profile picture are
	// hydrated onto their profile.
	require.NotNil(t, members[string(peer.Value)].UserProfile)
	require.NotNil(t, members[string(peer.Value)].UserProfile.PhoneNumber)
	require.Equal(t, "+15551234567", members[string(peer.Value)].UserProfile.PhoneNumber.Value)
	require.Equal(t, "Peer Name", members[string(peer.Value)].UserProfile.DisplayName)

	// The avatar arrives with its blob metadata already resolved, so the client can
	// render it without a follow-up GetBlobs.
	picture := members[string(peer.Value)].UserProfile.GetProfilePicture()
	require.NotNil(t, picture)
	require.Len(t, picture.Renditions, 1)
	require.Equal(t, peerPicture.Renditions[0].BlobId.Value, picture.Renditions[0].BlobId.Value)
	require.NotNil(t, picture.Renditions[0].Blob)
	require.NotEmpty(t, picture.Renditions[0].Blob.GetDownloadUrl().GetUrl())

	// The env user registered none of them, so they all stay unset.
	require.NotNil(t, members[string(e.userID.Value)].UserProfile)
	require.Nil(t, members[string(e.userID.Value)].UserProfile.PhoneNumber)
	require.Empty(t, members[string(e.userID.Value)].UserProfile.DisplayName)
	require.Nil(t, members[string(e.userID.Value)].UserProfile.GetProfilePicture())
}

func testServer_GetChat_TipDm_HidesPhoneNumbers(t *testing.T, s chat.Store) {
	e := newServerEnv(t, s)

	peer := model.MustGenerateUserID()
	chatID := generateChatID()
	require.NoError(t, s.PutChat(e.ctx, &chat.Chat{
		ID:           chatID,
		Type:         chatpb.ChatType_TIP_DM,
		Members:      []*commonpb.UserId{e.userID, peer},
		LastActivity: at(1),
	}))

	e.profiles.phoneNumbers[string(peer.Value)] = &phonepb.PhoneNumber{Value: "+15551234567"}
	e.profiles.displayNames[string(peer.Value)] = "Peer Name"
	e.profiles.setProfilePicture(peer, &blobpb.BlobId{Value: make([]byte, 16)})

	resp := e.getChat(e.keys, chatID)
	require.Equal(t, chatpb.GetChatResponse_OK, resp.Result)
	require.Equal(t, chatpb.ChatType_TIP_DM, resp.Metadata.Type)

	members := byUserID(resp.Metadata.Members)
	profile := members[string(peer.Value)].UserProfile
	require.NotNil(t, profile)
	require.Nil(t, profile.PhoneNumber)
	require.Equal(t, "Peer Name", profile.DisplayName)
	require.NotNil(t, profile.GetProfilePicture())
}

func testServer_GetDmChatFeed_TypeScoped(t *testing.T, s chat.Store) {
	e := newServerEnv(t, s)

	contactID := e.putDMOfType(chatpb.ChatType_CONTACT_DM, at(1))
	tipID := e.putDMOfType(chatpb.ChatType_TIP_DM, at(2))

	contactResp, err := e.getDmFeedOfType(chatpb.ChatType_CONTACT_DM, &commonpb.QueryOptions{})
	require.NoError(t, err)
	require.Equal(t, chatpb.GetDmChatFeedResponse_OK, contactResp.Result)
	require.Len(t, contactResp.Chats, 1)
	require.Equal(t, contactID.Value, contactResp.Chats[0].ChatId.Value)
	require.Equal(t, chatpb.ChatType_CONTACT_DM, contactResp.Chats[0].Type)

	tipResp, err := e.getDmFeedOfType(chatpb.ChatType_TIP_DM, &commonpb.QueryOptions{})
	require.NoError(t, err)
	require.Equal(t, chatpb.GetDmChatFeedResponse_OK, tipResp.Result)
	require.Len(t, tipResp.Chats, 1)
	require.Equal(t, tipID.Value, tipResp.Chats[0].ChatId.Value)
	require.Equal(t, chatpb.ChatType_TIP_DM, tipResp.Chats[0].Type)
}

func testServer_GetDmChatFeed_TokenBoundToType(t *testing.T, s chat.Store) {
	e := newServerEnv(t, s)

	// Two contact DMs so the first page (size 1) yields a resumable token.
	e.putDMOfType(chatpb.ChatType_CONTACT_DM, at(1))
	e.putDMOfType(chatpb.ChatType_CONTACT_DM, at(2))

	resp, err := e.getDmFeedOfType(chatpb.ChatType_CONTACT_DM, &commonpb.QueryOptions{PageSize: 1})
	require.NoError(t, err)
	require.True(t, resp.HasMore)
	require.NotNil(t, resp.PagingToken)

	// The same token resumes its own feed...
	resumed, err := e.getDmFeedOfType(chatpb.ChatType_CONTACT_DM, &commonpb.QueryOptions{PageSize: 1, PagingToken: resp.PagingToken})
	require.NoError(t, err)
	require.Equal(t, chatpb.GetDmChatFeedResponse_OK, resumed.Result)
	require.Len(t, resumed.Chats, 1)

	// ...but is rejected against the other feed.
	_, err = e.getDmFeedOfType(chatpb.ChatType_TIP_DM, &commonpb.QueryOptions{PageSize: 1, PagingToken: resp.PagingToken})
	require.Equal(t, codes.InvalidArgument, status.Code(err))
}

func testServer_GetDmChatFeed_Hydrates(t *testing.T, s chat.Store) {
	e := newServerEnv(t, s)

	// A chat with a last message, and one without.
	withMsg := generateChatID()
	peer := model.MustGenerateUserID()
	require.NoError(t, s.PutChat(e.ctx, &chat.Chat{
		ID:            withMsg,
		Type:          chatpb.ChatType_CONTACT_DM,
		Members:       []*commonpb.UserId{e.userID, peer},
		LastActivity:  at(2),
		LastMessageID: &messagingpb.MessageId{Value: 3},
	}))
	withoutMsg := e.putDM(at(1))

	e.messaging.lastMessages[string(withMsg.Value)] = textMessage(3, e.userID, "yo")
	e.messaging.latestEventSeqs[string(withMsg.Value)] = 3
	e.profiles.displayNames[string(e.userID.Value)] = "Env User"
	e.profiles.displayNames[string(peer.Value)] = "Peer Name"

	resp := e.getDmFeed(&commonpb.QueryOptions{})
	require.Equal(t, chatpb.GetDmChatFeedResponse_OK, resp.Result)
	require.Len(t, resp.Chats, 2)

	byChat := make(map[string]*chatpb.Metadata)
	for _, md := range resp.Chats {
		byChat[string(md.ChatId.Value)] = md
	}
	// The chat with a last message ID gets its message and head sequence hydrated...
	require.NotNil(t, byChat[string(withMsg.Value)].LastMessage)
	require.Equal(t, uint64(3), byChat[string(withMsg.Value)].LastMessage.MessageId.Value)
	require.Equal(t, uint64(3), byChat[string(withMsg.Value)].LatestEventSequence)
	// ...and the one without is left nil, its head defaulting to 0 (no ref is
	// issued for it).
	require.Nil(t, byChat[string(withoutMsg.Value)].LastMessage)
	require.Zero(t, byChat[string(withoutMsg.Value)].LatestEventSequence)

	// Display names are hydrated for every member of every chat on the page: the
	// single batched lookup spans chats, so the env user's name lands on both.
	require.Equal(t, "Peer Name", byUserID(byChat[string(withMsg.Value)].Members)[string(peer.Value)].UserProfile.DisplayName)
	for _, chatID := range []*commonpb.ChatId{withMsg, withoutMsg} {
		members := byUserID(byChat[string(chatID.Value)].Members)
		require.Equal(t, "Env User", members[string(e.userID.Value)].UserProfile.DisplayName)
	}
}

func textMessage(id uint64, sender *commonpb.UserId, text string) *messagingpb.Message {
	return &messagingpb.Message{
		MessageId: &messagingpb.MessageId{Value: id},
		SenderId:  sender,
		Content: []*messagingpb.Content{{
			Type: &messagingpb.Content_Text{Text: &messagingpb.TextContent{Text: text}},
		}},
		Ts:            timestamppb.New(at(int64(id))),
		EventSequence: id,
	}
}

func byUserID(members []*chatpb.Member) map[string]*chatpb.Member {
	out := make(map[string]*chatpb.Member, len(members))
	for _, m := range members {
		out[string(m.UserId.Value)] = m
	}
	return out
}
