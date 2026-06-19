package tests

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"
	"google.golang.org/grpc"

	chatpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/chat/v1"
	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"
	eventpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/event/v1"
	messagingpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/messaging/v1"

	"github.com/code-payments/flipcash2-server/auth"
	"github.com/code-payments/flipcash2-server/badge"
	"github.com/code-payments/flipcash2-server/chat"
	"github.com/code-payments/flipcash2-server/event"
	"github.com/code-payments/flipcash2-server/messaging"
	"github.com/code-payments/flipcash2-server/model"
	"github.com/code-payments/flipcash2-server/profile"
	"github.com/code-payments/flipcash2-server/push"
	"github.com/code-payments/flipcash2-server/testutil"
	ocp_data "github.com/code-payments/ocp-server/ocp/data"
)

// RunServerTests runs the shared messaging.Server test suite. chats and messages
// are the backing stores; teardown resets both between tests.
func RunServerTests(t *testing.T, badges badge.Store, chats chat.Store, messages messaging.Store, profiles profile.Store, teardown func()) {
	for _, tf := range []func(t *testing.T, chats chat.Store, messages messaging.Store, profiles profile.Store, badges badge.Store){
		testServer_SendAndGet,
		testServer_SendMessage_Idempotent,
		testServer_GetMessages_NotFound,
		testServer_GetMessages_Paging,
		testServer_GetMessages_ByIDs,
		testServer_GetMessage_NotFound,
		testServer_NonMember_Denied,
		testServer_AdvancePointer,
		testServer_SendMessage_Broadcast,
		testServer_SendReply,
		testServer_NotifyIsTyping,
	} {
		tf(t, chats, messages, profiles, badges)
		teardown()
	}
}

type serverEnv struct {
	t        *testing.T
	ctx      context.Context
	client   messagingpb.MessagingClient
	authz    *auth.StaticAuthorizer
	observer *event.TestEventObserver[*commonpb.UserId, *eventpb.Event]

	chatID *commonpb.ChatId
	userA  *commonpb.UserId
	keysA  model.KeyPair
	userB  *commonpb.UserId
	keysB  model.KeyPair
}

func newServerEnv(t *testing.T, badges badge.Store, chats chat.Store, messages messaging.Store, profiles profile.Store) *serverEnv {
	ctx := context.Background()
	log := zaptest.NewLogger(t)

	authz := auth.NewStaticAuthorizer(log)
	bus := event.NewBus[*commonpb.UserId, *eventpb.Event]()
	observer := event.NewTestEventObserver[*commonpb.UserId, *eventpb.Event]()
	bus.AddHandler(observer)

	env := &serverEnv{
		t:        t,
		ctx:      ctx,
		authz:    authz,
		observer: observer,
		chatID:   generateChatID(),
	}
	env.userA, env.keysA = env.addUser()
	env.userB, env.keysB = env.addUser()

	require.NoError(t, chats.PutChat(ctx, &chat.Chat{
		ID:           env.chatID,
		Type:         chatpb.Metadata_DM,
		Members:      []*commonpb.UserId{env.userA, env.userB},
		LastActivity: at(1),
	}))

	sender := messaging.NewSender(log, badges, chats, messages, profiles, ocp_data.NewTestDataProvider(), push.NewNoOpPusher(), bus)
	server := messaging.NewServer(log, authz, chats, messages, sender)
	cc := testutil.RunGRPCServer(t, log, testutil.WithService(func(s *grpc.Server) {
		messagingpb.RegisterMessagingServer(s, server)
	}))
	env.client = messagingpb.NewMessagingClient(cc)
	return env
}

func protoMessageIDs(msgs []*messagingpb.Message) []uint64 {
	out := make([]uint64, len(msgs))
	for i, m := range msgs {
		out[i] = m.MessageId.Value
	}
	return out
}

func lastMessageID(resp *messagingpb.GetMessagesResponse) *messagingpb.MessageId {
	msgs := resp.Messages.Messages
	return msgs[len(msgs)-1].MessageId
}

func (e *serverEnv) addUser() (*commonpb.UserId, model.KeyPair) {
	userID := model.MustGenerateUserID()
	keys := model.MustGenerateKeyPair()
	e.authz.Add(userID, keys)
	return userID, keys
}

func (e *serverEnv) send(keys model.KeyPair, text string, clientID *messagingpb.ClientMessageId) (*messagingpb.SendMessageResponse, error) {
	req := &messagingpb.SendMessageRequest{
		ChatId:          e.chatID,
		Content:         textContent(text),
		ClientMessageId: clientID,
	}
	require.NoError(e.t, keys.Auth(req, &req.Auth))
	return e.client.SendMessage(e.ctx, req)
}

func testServer_SendAndGet(t *testing.T, chats chat.Store, messages messaging.Store, profiles profile.Store, badges badge.Store) {
	e := newServerEnv(t, badges, chats, messages, profiles)

	resp, err := e.send(e.keysA, "hello", generateClientID())
	require.NoError(t, err)
	require.Equal(t, messagingpb.SendMessageResponse_OK, resp.Result)
	require.Equal(t, uint64(1), resp.Message.MessageId.Value)
	require.Equal(t, e.userA.Value, resp.Message.SenderId.Value)

	getReq := &messagingpb.GetMessageRequest{ChatId: e.chatID, MessageId: resp.Message.MessageId}
	require.NoError(t, e.keysB.Auth(getReq, &getReq.Auth))
	getResp, err := e.client.GetMessage(e.ctx, getReq)
	require.NoError(t, err)
	require.Equal(t, messagingpb.GetMessageResponse_OK, getResp.Result)
	require.Equal(t, "hello", getResp.Message.Content[0].GetText().Text)

	listReq := &messagingpb.GetMessagesRequest{
		ChatId: e.chatID,
		Query:  &messagingpb.GetMessagesRequest_Options{Options: &commonpb.QueryOptions{}},
	}
	require.NoError(t, e.keysB.Auth(listReq, &listReq.Auth))
	listResp, err := e.client.GetMessages(e.ctx, listReq)
	require.NoError(t, err)
	require.Equal(t, messagingpb.GetMessagesResponse_OK, listResp.Result)
	require.Len(t, listResp.Messages.Messages, 1)
}

func testServer_SendReply(t *testing.T, chats chat.Store, messages messaging.Store, profiles profile.Store, badges badge.Store) {
	e := newServerEnv(t, badges, chats, messages, profiles)

	// Seed a message to reply to.
	original, err := e.send(e.keysA, "original", generateClientID())
	require.NoError(t, err)

	// A text reply to that message is accepted and round-trips its content.
	replyReq := &messagingpb.SendMessageRequest{
		ChatId:          e.chatID,
		Content:         replyContent(original.Message.MessageId.Value, "replying"),
		ClientMessageId: generateClientID(),
	}
	require.NoError(t, e.keysB.Auth(replyReq, &replyReq.Auth))
	replyResp, err := e.client.SendMessage(e.ctx, replyReq)
	require.NoError(t, err)
	require.Equal(t, messagingpb.SendMessageResponse_OK, replyResp.Result)

	reply := replyResp.Message.Content[0].GetReply()
	require.NotNil(t, reply)
	require.Equal(t, original.Message.MessageId.Value, reply.RepliedMessageId.Value)
	require.Equal(t, "replying", reply.Content[0].GetText().Text)

	// A reply wrapping unsupported content (e.g. a nested reply) is denied.
	deniedReq := &messagingpb.SendMessageRequest{
		ChatId: e.chatID,
		Content: []*messagingpb.Content{{
			Type: &messagingpb.Content_Reply{
				Reply: &messagingpb.ReplyContent{
					RepliedMessageId: original.Message.MessageId,
					Content:          replyContent(original.Message.MessageId.Value, "nested"),
				},
			},
		}},
		ClientMessageId: generateClientID(),
	}
	require.NoError(t, e.keysB.Auth(deniedReq, &deniedReq.Auth))
	deniedResp, err := e.client.SendMessage(e.ctx, deniedReq)
	require.NoError(t, err)
	require.Equal(t, messagingpb.SendMessageResponse_DENIED, deniedResp.Result)

	// Replying to a message that does not exist is denied.
	missingReq := &messagingpb.SendMessageRequest{
		ChatId:          e.chatID,
		Content:         replyContent(original.Message.MessageId.Value+999, "ghost"),
		ClientMessageId: generateClientID(),
	}
	require.NoError(t, e.keysB.Auth(missingReq, &missingReq.Auth))
	missingResp, err := e.client.SendMessage(e.ctx, missingReq)
	require.NoError(t, err)
	require.Equal(t, messagingpb.SendMessageResponse_DENIED, missingResp.Result)

	// Replying to a non-replyable (system) message is denied.
	systemMsg, _, err := messages.PutMessage(e.ctx, e.chatID, nil, systemContent("joined"), at(100), generateClientID(), false)
	require.NoError(t, err)
	systemReplyReq := &messagingpb.SendMessageRequest{
		ChatId:          e.chatID,
		Content:         replyContent(systemMsg.ID.Value, "to a system message"),
		ClientMessageId: generateClientID(),
	}
	require.NoError(t, e.keysB.Auth(systemReplyReq, &systemReplyReq.Auth))
	systemReplyResp, err := e.client.SendMessage(e.ctx, systemReplyReq)
	require.NoError(t, err)
	require.Equal(t, messagingpb.SendMessageResponse_DENIED, systemReplyResp.Result)
}

func testServer_SendMessage_Idempotent(t *testing.T, chats chat.Store, messages messaging.Store, profiles profile.Store, badges badge.Store) {
	e := newServerEnv(t, badges, chats, messages, profiles)

	clientID := generateClientID()
	first, err := e.send(e.keysA, "hi", clientID)
	require.NoError(t, err)
	again, err := e.send(e.keysA, "hi", clientID)
	require.NoError(t, err)
	require.Equal(t, first.Message.MessageId.Value, again.Message.MessageId.Value)

	next, err := e.send(e.keysA, "hi again", generateClientID())
	require.NoError(t, err)
	require.Equal(t, first.Message.MessageId.Value+1, next.Message.MessageId.Value)

	// The retried send must not re-run side effects (most importantly pushes,
	// which ride the same broadcast). Wait for the follow-up message to reach
	// userB, then check the first message was broadcast to them exactly once.
	countBroadcasts := func(messageID uint64) (n int) {
		for _, ev := range e.observer.GetEvents(func(k *commonpb.UserId) bool { return bytes.Equal(k.Value, e.userB.Value) }) {
			update := ev.Event.GetChatUpdate()
			if update == nil || update.NewMessages == nil {
				continue
			}
			for _, m := range update.NewMessages.Messages {
				if m.MessageId.Value == messageID {
					n++
				}
			}
		}
		return n
	}
	e.observer.WaitFor(t, func([]*event.KeyAndEvent[*commonpb.UserId, *eventpb.Event]) bool {
		return countBroadcasts(next.Message.MessageId.Value) > 0
	})
	require.Equal(t, 1, countBroadcasts(first.Message.MessageId.Value))
}

func testServer_GetMessages_NotFound(t *testing.T, chats chat.Store, messages messaging.Store, profiles profile.Store, badges badge.Store) {
	e := newServerEnv(t, badges, chats, messages, profiles)

	req := &messagingpb.GetMessagesRequest{
		ChatId: e.chatID,
		Query:  &messagingpb.GetMessagesRequest_Options{Options: &commonpb.QueryOptions{}},
	}
	require.NoError(t, e.keysA.Auth(req, &req.Auth))
	resp, err := e.client.GetMessages(e.ctx, req)
	require.NoError(t, err)
	require.Equal(t, messagingpb.GetMessagesResponse_NOT_FOUND, resp.Result)
}

func testServer_GetMessages_Paging(t *testing.T, chats chat.Store, messages messaging.Store, profiles profile.Store, badges badge.Store) {
	e := newServerEnv(t, badges, chats, messages, profiles)

	// Seed 5 messages, assigned gapless IDs 1..5.
	for i := 0; i < 5; i++ {
		_, err := e.send(e.keysA, "m", generateClientID())
		require.NoError(t, err)
	}

	getPage := func(opts *commonpb.QueryOptions) *messagingpb.GetMessagesResponse {
		req := &messagingpb.GetMessagesRequest{
			ChatId: e.chatID,
			Query:  &messagingpb.GetMessagesRequest_Options{Options: opts},
		}
		require.NoError(t, e.keysB.Auth(req, &req.Auth))
		resp, err := e.client.GetMessages(e.ctx, req)
		require.NoError(t, err)
		return resp
	}

	// Page ascending in chunks of 2: [1,2], [3,4], [5], then empty → NOT_FOUND.
	page1 := getPage(&commonpb.QueryOptions{PageSize: 2, Order: commonpb.QueryOptions_ASC})
	require.Equal(t, messagingpb.GetMessagesResponse_OK, page1.Result)
	require.Equal(t, []uint64{1, 2}, protoMessageIDs(page1.Messages.Messages))

	page2 := getPage(&commonpb.QueryOptions{
		PageSize:    2,
		Order:       commonpb.QueryOptions_ASC,
		PagingToken: messaging.PageTokenFromID(lastMessageID(page1)),
	})
	require.Equal(t, messagingpb.GetMessagesResponse_OK, page2.Result)
	require.Equal(t, []uint64{3, 4}, protoMessageIDs(page2.Messages.Messages))

	page3 := getPage(&commonpb.QueryOptions{
		PageSize:    2,
		Order:       commonpb.QueryOptions_ASC,
		PagingToken: messaging.PageTokenFromID(lastMessageID(page2)),
	})
	require.Equal(t, messagingpb.GetMessagesResponse_OK, page3.Result)
	require.Equal(t, []uint64{5}, protoMessageIDs(page3.Messages.Messages))

	page4 := getPage(&commonpb.QueryOptions{
		PageSize:    2,
		Order:       commonpb.QueryOptions_ASC,
		PagingToken: messaging.PageTokenFromID(lastMessageID(page3)),
	})
	require.Equal(t, messagingpb.GetMessagesResponse_NOT_FOUND, page4.Result)

	// Descending first page returns the newest messages.
	desc := getPage(&commonpb.QueryOptions{PageSize: 2, Order: commonpb.QueryOptions_DESC})
	require.Equal(t, messagingpb.GetMessagesResponse_OK, desc.Result)
	require.Equal(t, []uint64{5, 4}, protoMessageIDs(desc.Messages.Messages))
}

func testServer_GetMessages_ByIDs(t *testing.T, chats chat.Store, messages messaging.Store, profiles profile.Store, badges badge.Store) {
	e := newServerEnv(t, badges, chats, messages, profiles)

	for i := 0; i < 5; i++ {
		_, err := e.send(e.keysA, "m", generateClientID())
		require.NoError(t, err)
	}

	getByIDs := func(vals ...uint64) *messagingpb.GetMessagesResponse {
		req := &messagingpb.GetMessagesRequest{
			ChatId: e.chatID,
			Query: &messagingpb.GetMessagesRequest_MessageIds{
				MessageIds: &messagingpb.MessageIdBatch{MessageIds: ids(vals...)},
			},
		}
		require.NoError(t, e.keysB.Auth(req, &req.Auth))
		resp, err := e.client.GetMessages(e.ctx, req)
		require.NoError(t, err)
		return resp
	}

	// A mix of existing and missing IDs returns only the existing ones, sorted
	// ascending; the missing 99 is omitted and duplicates collapse.
	resp := getByIDs(4, 2, 99, 2)
	require.Equal(t, messagingpb.GetMessagesResponse_OK, resp.Result)
	require.Equal(t, []uint64{2, 4}, protoMessageIDs(resp.Messages.Messages))

	// All IDs missing → NOT_FOUND.
	none := getByIDs(100, 200)
	require.Equal(t, messagingpb.GetMessagesResponse_NOT_FOUND, none.Result)
}

func testServer_GetMessage_NotFound(t *testing.T, chats chat.Store, messages messaging.Store, profiles profile.Store, badges badge.Store) {
	e := newServerEnv(t, badges, chats, messages, profiles)

	req := &messagingpb.GetMessageRequest{ChatId: e.chatID, MessageId: &messagingpb.MessageId{Value: 99}}
	require.NoError(t, e.keysA.Auth(req, &req.Auth))
	resp, err := e.client.GetMessage(e.ctx, req)
	require.NoError(t, err)
	require.Equal(t, messagingpb.GetMessageResponse_NOT_FOUND, resp.Result)
}

func testServer_NonMember_Denied(t *testing.T, chats chat.Store, messages messaging.Store, profiles profile.Store, badges badge.Store) {
	e := newServerEnv(t, badges, chats, messages, profiles)
	_, strangerKeys := e.addUser()

	sendReq := &messagingpb.SendMessageRequest{
		ChatId:          e.chatID,
		Content:         textContent("intruder"),
		ClientMessageId: generateClientID(),
	}
	require.NoError(t, strangerKeys.Auth(sendReq, &sendReq.Auth))
	sendResp, err := e.client.SendMessage(e.ctx, sendReq)
	require.NoError(t, err)
	require.Equal(t, messagingpb.SendMessageResponse_DENIED, sendResp.Result)

	listReq := &messagingpb.GetMessagesRequest{
		ChatId: e.chatID,
		Query:  &messagingpb.GetMessagesRequest_Options{Options: &commonpb.QueryOptions{}},
	}
	require.NoError(t, strangerKeys.Auth(listReq, &listReq.Auth))
	listResp, err := e.client.GetMessages(e.ctx, listReq)
	require.NoError(t, err)
	require.Equal(t, messagingpb.GetMessagesResponse_DENIED, listResp.Result)

	getReq := &messagingpb.GetMessageRequest{ChatId: e.chatID, MessageId: &messagingpb.MessageId{Value: 1}}
	require.NoError(t, strangerKeys.Auth(getReq, &getReq.Auth))
	getResp, err := e.client.GetMessage(e.ctx, getReq)
	require.NoError(t, err)
	require.Equal(t, messagingpb.GetMessageResponse_DENIED, getResp.Result)

	advReq := &messagingpb.AdvancePointerRequest{
		ChatId:      e.chatID,
		PointerType: messagingpb.Pointer_READ,
		NewValue:    &messagingpb.MessageId{Value: 1},
	}
	require.NoError(t, strangerKeys.Auth(advReq, &advReq.Auth))
	advResp, err := e.client.AdvancePointer(e.ctx, advReq)
	require.NoError(t, err)
	require.Equal(t, messagingpb.AdvancePointerResponse_DENIED, advResp.Result)

	typingReq := &messagingpb.NotifyIsTypingRequest{
		ChatId: e.chatID,
		State:  messagingpb.IsTypingNotification_STARTED_TYPING,
	}
	require.NoError(t, strangerKeys.Auth(typingReq, &typingReq.Auth))
	typingResp, err := e.client.NotifyIsTyping(e.ctx, typingReq)
	require.NoError(t, err)
	require.Equal(t, messagingpb.NotifyIsTypingResponse_DENIED, typingResp.Result)
}

func testServer_AdvancePointer(t *testing.T, chats chat.Store, messages messaging.Store, profiles profile.Store, badges badge.Store) {
	e := newServerEnv(t, badges, chats, messages, profiles)

	m1, err := e.send(e.keysA, "first", generateClientID())
	require.NoError(t, err)
	m2, err := e.send(e.keysA, "second", generateClientID())
	require.NoError(t, err)

	// Counts userB READ pointer updates broadcast to the other member (userA).
	// Filtering on the pointer's own UserId discriminates these from the sender's
	// auto-advanced READ pointers that ride along with each SendMessage above.
	countUserBRead := func() int {
		n := 0
		for _, ev := range e.observer.GetEvents(func(k *commonpb.UserId) bool { return bytes.Equal(k.Value, e.userA.Value) }) {
			u := ev.Event.GetChatUpdate()
			if u == nil || u.PointerUpdates == nil {
				continue
			}
			for _, p := range u.PointerUpdates.Pointers {
				if p.Type == messagingpb.Pointer_READ && bytes.Equal(p.UserId.Value, e.userB.Value) {
					n++
				}
			}
		}
		return n
	}

	// A forward advance succeeds and broadcasts userB's new READ pointer to the
	// other member.
	okReq := &messagingpb.AdvancePointerRequest{
		ChatId:      e.chatID,
		PointerType: messagingpb.Pointer_READ,
		NewValue:    m2.Message.MessageId,
	}
	require.NoError(t, e.keysB.Auth(okReq, &okReq.Auth))
	okResp, err := e.client.AdvancePointer(e.ctx, okReq)
	require.NoError(t, err)
	require.Equal(t, messagingpb.AdvancePointerResponse_OK, okResp.Result)

	e.observer.WaitFor(t, func(events []*event.KeyAndEvent[*commonpb.UserId, *eventpb.Event]) bool {
		for _, ev := range events {
			if !bytes.Equal(ev.Key.Value, e.userA.Value) {
				continue
			}
			u := ev.Event.GetChatUpdate()
			if u == nil || u.PointerUpdates == nil {
				continue
			}
			for _, p := range u.PointerUpdates.Pointers {
				if p.Type == messagingpb.Pointer_READ &&
					bytes.Equal(p.UserId.Value, e.userB.Value) &&
					p.Value.Value == m2.Message.MessageId.Value {
					return true
				}
			}
		}
		return false
	})

	before := countUserBRead()

	// Moving the pointer backward is a monotonic no-op: the result is still OK,
	// but nothing must be broadcast.
	backReq := &messagingpb.AdvancePointerRequest{
		ChatId:      e.chatID,
		PointerType: messagingpb.Pointer_READ,
		NewValue:    m1.Message.MessageId,
	}
	require.NoError(t, e.keysB.Auth(backReq, &backReq.Auth))
	backResp, err := e.client.AdvancePointer(e.ctx, backReq)
	require.NoError(t, err)
	require.Equal(t, messagingpb.AdvancePointerResponse_OK, backResp.Result)

	// Give any erroneous broadcast a moment to land, then assert none did.
	time.Sleep(50 * time.Millisecond)
	require.Equal(t, before, countUserBRead(), "a no-op pointer move must not broadcast")

	// A pointer past the last message is rejected.
	missReq := &messagingpb.AdvancePointerRequest{
		ChatId:      e.chatID,
		PointerType: messagingpb.Pointer_READ,
		NewValue:    &messagingpb.MessageId{Value: 999},
	}
	require.NoError(t, e.keysB.Auth(missReq, &missReq.Auth))
	missResp, err := e.client.AdvancePointer(e.ctx, missReq)
	require.NoError(t, err)
	require.Equal(t, messagingpb.AdvancePointerResponse_MESSAGE_NOT_FOUND, missResp.Result)
}

func testServer_SendMessage_Broadcast(t *testing.T, chats chat.Store, messages messaging.Store, profiles profile.Store, badges badge.Store) {
	e := newServerEnv(t, badges, chats, messages, profiles)

	resp, err := e.send(e.keysA, "broadcast me", generateClientID())
	require.NoError(t, err)

	// userB receives a single ChatUpdate carrying the new message, a last-activity
	// metadata update, and the sender's auto-advanced READ pointer (the sender has
	// implicitly read their own message).
	e.observer.WaitFor(t, func(events []*event.KeyAndEvent[*commonpb.UserId, *eventpb.Event]) bool {
		for _, ev := range events {
			if !bytes.Equal(ev.Key.Value, e.userB.Value) {
				continue
			}
			update := ev.Event.GetChatUpdate()
			if update == nil || update.NewMessages == nil {
				continue
			}
			carriesMessage := false
			for _, m := range update.NewMessages.Messages {
				if m.MessageId.Value == resp.Message.MessageId.Value {
					carriesMessage = true
				}
			}
			if !carriesMessage || len(update.MetadataUpdates) == 0 {
				continue
			}
			if update.PointerUpdates == nil {
				continue
			}
			for _, p := range update.PointerUpdates.Pointers {
				if p.Type == messagingpb.Pointer_READ &&
					bytes.Equal(p.UserId.Value, e.userA.Value) &&
					p.Value.Value == resp.Message.MessageId.Value {
					return true
				}
			}
		}
		return false
	})
}

func testServer_NotifyIsTyping(t *testing.T, chats chat.Store, messages messaging.Store, profiles profile.Store, badges badge.Store) {
	e := newServerEnv(t, badges, chats, messages, profiles)

	req := &messagingpb.NotifyIsTypingRequest{
		ChatId: e.chatID,
		State:  messagingpb.IsTypingNotification_STARTED_TYPING,
	}
	require.NoError(t, e.keysA.Auth(req, &req.Auth))
	resp, err := e.client.NotifyIsTyping(e.ctx, req)
	require.NoError(t, err)
	require.Equal(t, messagingpb.NotifyIsTypingResponse_OK, resp.Result)

	// userB (the other member) is notified; the sender is excluded.
	e.observer.WaitFor(t, func(events []*event.KeyAndEvent[*commonpb.UserId, *eventpb.Event]) bool {
		for _, ev := range events {
			if !bytes.Equal(ev.Key.Value, e.userB.Value) {
				continue
			}
			if u := ev.Event.GetChatUpdate(); u != nil && u.IsTypingNotifications != nil {
				return true
			}
		}
		return false
	})

	// Give any erroneous self-notification a moment, then assert none landed for userA.
	time.Sleep(50 * time.Millisecond)
	selfEvents := e.observer.GetEvents(func(k *commonpb.UserId) bool { return bytes.Equal(k.Value, e.userA.Value) })
	for _, ev := range selfEvents {
		if u := ev.Event.GetChatUpdate(); u != nil {
			require.Nil(t, u.IsTypingNotifications, "sender should not receive their own typing notification")
		}
	}
}
