package tests

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/timestamppb"

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
		testServer_GetDmChatFeed_Empty,
		testServer_GetDmChatFeed_OrderAndContent,
		testServer_GetDmChatFeed_Paging,
		testServer_GetDmChatFeed_Hydrates,
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
	server := chat.NewServer(log, authz, s, messaging, profiles)
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
		userID:    userID,
		keys:      keys,
	}
}

// fakeMessagingReader is a canned chat.MessagingReader for server tests: it
// returns whatever last messages and pointers a test registers per chat.
type fakeMessagingReader struct {
	lastMessages map[string]*messagingpb.Message
	pointers     map[string][]*messagingpb.Pointer
}

func newFakeMessagingReader() *fakeMessagingReader {
	return &fakeMessagingReader{
		lastMessages: make(map[string]*messagingpb.Message),
		pointers:     make(map[string][]*messagingpb.Pointer),
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

// fakeProfileReader is a canned chat.ProfileReader for server tests: it returns
// whatever phone number a test registers per user ID.
type fakeProfileReader struct {
	phoneNumbers map[string]*phonepb.PhoneNumber
}

func newFakeProfileReader() *fakeProfileReader {
	return &fakeProfileReader{phoneNumbers: make(map[string]*phonepb.PhoneNumber)}
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

// putDM persists a DM the env user is a member of, with the given last activity.
func (e *serverEnv) putDM(lastActivity time.Time) *commonpb.ChatId {
	chatID := generateChatID()
	require.NoError(e.t, e.store.PutChat(e.ctx, &chat.Chat{
		ID:           chatID,
		Type:         chatpb.Metadata_DM,
		Members:      []*commonpb.UserId{e.userID, model.MustGenerateUserID()},
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
	req := &chatpb.GetDmChatFeedRequest{QueryOptions: opts}
	require.NoError(e.t, e.keys.Auth(req, &req.Auth))
	resp, err := e.client.GetDmChatFeed(e.ctx, req)
	require.NoError(e.t, err)
	return resp
}

func testServer_GetChat_OK(t *testing.T, s chat.Store) {
	e := newServerEnv(t, s)

	chatID := e.putDM(at(1))

	resp := e.getChat(e.keys, chatID)
	require.Equal(t, chatpb.GetChatResponse_OK, resp.Result)
	require.NotNil(t, resp.Metadata)
	require.Equal(t, chatID.Value, resp.Metadata.ChatId.Value)
	require.Equal(t, chatpb.Metadata_DM, resp.Metadata.Type)
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

func testServer_GetDmChatFeed_Empty(t *testing.T, s chat.Store) {
	e := newServerEnv(t, s)

	resp := e.getDmFeed(&commonpb.QueryOptions{})
	require.Equal(t, chatpb.GetDmChatFeedResponse_OK, resp.Result)
	require.Empty(t, resp.Chats)
	require.False(t, resp.HasMore)
	require.Nil(t, resp.PagingToken)
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
	require.Equal(t, chatpb.Metadata_DM, first.Type)
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
		Type:          chatpb.Metadata_DM,
		Members:       []*commonpb.UserId{e.userID, peer},
		LastActivity:  at(1),
		LastMessageID: &messagingpb.MessageId{Value: 7},
	}))

	e.messaging.lastMessages[string(chatID.Value)] = textMessage(7, peer, "hi")
	e.messaging.pointers[string(chatID.Value)] = []*messagingpb.Pointer{
		{Type: messagingpb.Pointer_READ, UserId: e.userID, Value: &messagingpb.MessageId{Value: 7}, Ts: timestamppb.New(at(7))},
		{Type: messagingpb.Pointer_DELIVERED, UserId: peer, Value: &messagingpb.MessageId{Value: 7}, Ts: timestamppb.New(at(7))},
	}
	e.profiles.phoneNumbers[string(peer.Value)] = &phonepb.PhoneNumber{Value: "+15551234567"}

	resp := e.getChat(e.keys, chatID)
	require.Equal(t, chatpb.GetChatResponse_OK, resp.Result)

	// The last message is hydrated from the messaging reader.
	require.NotNil(t, resp.Metadata.LastMessage)
	require.Equal(t, uint64(7), resp.Metadata.LastMessage.MessageId.Value)
	require.Equal(t, "hi", resp.Metadata.LastMessage.Content[0].GetText().Text)

	// Pointers are distributed onto the matching member by user ID.
	members := byUserID(resp.Metadata.Members)
	require.Len(t, members[string(e.userID.Value)].Pointers, 1)
	require.Equal(t, messagingpb.Pointer_READ, members[string(e.userID.Value)].Pointers[0].Type)
	require.Len(t, members[string(peer.Value)].Pointers, 1)
	require.Equal(t, messagingpb.Pointer_DELIVERED, members[string(peer.Value)].Pointers[0].Type)

	// The other DM member's phone number is hydrated onto their profile.
	require.NotNil(t, members[string(peer.Value)].UserProfile)
	require.NotNil(t, members[string(peer.Value)].UserProfile.PhoneNumber)
	require.Equal(t, "+15551234567", members[string(peer.Value)].UserProfile.PhoneNumber.Value)
	// The env user registered no phone, so theirs stays unset.
	require.NotNil(t, members[string(e.userID.Value)].UserProfile)
	require.Nil(t, members[string(e.userID.Value)].UserProfile.PhoneNumber)
}

func testServer_GetDmChatFeed_Hydrates(t *testing.T, s chat.Store) {
	e := newServerEnv(t, s)

	// A chat with a last message, and one without.
	withMsg := generateChatID()
	require.NoError(t, s.PutChat(e.ctx, &chat.Chat{
		ID:            withMsg,
		Type:          chatpb.Metadata_DM,
		Members:       []*commonpb.UserId{e.userID, model.MustGenerateUserID()},
		LastActivity:  at(2),
		LastMessageID: &messagingpb.MessageId{Value: 3},
	}))
	withoutMsg := e.putDM(at(1))

	e.messaging.lastMessages[string(withMsg.Value)] = textMessage(3, e.userID, "yo")

	resp := e.getDmFeed(&commonpb.QueryOptions{})
	require.Equal(t, chatpb.GetDmChatFeedResponse_OK, resp.Result)
	require.Len(t, resp.Chats, 2)

	byChat := make(map[string]*chatpb.Metadata)
	for _, md := range resp.Chats {
		byChat[string(md.ChatId.Value)] = md
	}
	// The chat with a last message ID gets its message hydrated...
	require.NotNil(t, byChat[string(withMsg.Value)].LastMessage)
	require.Equal(t, uint64(3), byChat[string(withMsg.Value)].LastMessage.MessageId.Value)
	// ...and the one without is left nil (no ref is issued for it).
	require.Nil(t, byChat[string(withoutMsg.Value)].LastMessage)
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
