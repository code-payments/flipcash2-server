package tests

import (
	"bytes"
	"context"
	"fmt"
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
		testServer_Reactions,
		testServer_Reactions_Reactors,
		testServer_Reactions_Summaries,
		testServer_Reactions_Errors,
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

func (e *serverEnv) addReaction(keys model.KeyPair, msgID *messagingpb.MessageId, emoji string) (*messagingpb.AddReactionResponse, error) {
	req := &messagingpb.AddReactionRequest{ChatId: e.chatID, MessageId: msgID, Emoji: &messagingpb.Emoji{Value: emoji}}
	require.NoError(e.t, keys.Auth(req, &req.Auth))
	return e.client.AddReaction(e.ctx, req)
}

func (e *serverEnv) removeReaction(keys model.KeyPair, msgID *messagingpb.MessageId, emoji string) (*messagingpb.RemoveReactionResponse, error) {
	req := &messagingpb.RemoveReactionRequest{ChatId: e.chatID, MessageId: msgID, Emoji: &messagingpb.Emoji{Value: emoji}}
	require.NoError(e.t, keys.Auth(req, &req.Auth))
	return e.client.RemoveReaction(e.ctx, req)
}

func (e *serverEnv) getReactionSummary(keys model.KeyPair, msgID *messagingpb.MessageId) (*messagingpb.GetReactionSummaryResponse, error) {
	req := &messagingpb.GetReactionSummaryRequest{ChatId: e.chatID, MessageId: msgID}
	require.NoError(e.t, keys.Auth(req, &req.Auth))
	return e.client.GetReactionSummary(e.ctx, req)
}

// getReactionSummaries sends req after stamping in the chat ID and auth, so
// callers only supply the query branch (options vs message IDs).
func (e *serverEnv) getReactionSummaries(keys model.KeyPair, req *messagingpb.GetReactionSummariesRequest) (*messagingpb.GetReactionSummariesResponse, error) {
	req.ChatId = e.chatID
	require.NoError(e.t, keys.Auth(req, &req.Auth))
	return e.client.GetReactionSummaries(e.ctx, req)
}

func (e *serverEnv) getReactors(keys model.KeyPair, msgID *messagingpb.MessageId, emoji string, opts *commonpb.QueryOptions) (*messagingpb.GetReactorsResponse, error) {
	req := &messagingpb.GetReactorsRequest{ChatId: e.chatID, MessageId: msgID, Emoji: &messagingpb.Emoji{Value: emoji}, Options: opts}
	require.NoError(e.t, keys.Auth(req, &req.Auth))
	return e.client.GetReactors(e.ctx, req)
}

// waitForReactionUpdate blocks until recipient observes a ReactionUpdate matching
// the given action, message, emoji, and actor — the reaction-broadcast analogue
// of the message/pointer WaitFor blocks elsewhere in this suite.
func (e *serverEnv) waitForReactionUpdate(recipient *commonpb.UserId, action messagingpb.ReactionUpdate_Action, msgID uint64, emoji string, actor *commonpb.UserId) {
	e.observer.WaitFor(e.t, func(events []*event.KeyAndEvent[*commonpb.UserId, *eventpb.Event]) bool {
		for _, ev := range events {
			if !bytes.Equal(ev.Key.Value, recipient.Value) {
				continue
			}
			u := ev.Event.GetChatUpdate()
			if u == nil || u.ReactionUpdates == nil {
				continue
			}
			for _, ru := range u.ReactionUpdates.ReactionUpdates {
				if ru.Action == action &&
					ru.MessageId.Value == msgID &&
					ru.Emoji.Value == emoji &&
					bytes.Equal(ru.Actor.Value, actor.Value) {
					return true
				}
			}
		}
		return false
	})
}

// testServer_Reactions covers the core add/remove lifecycle: the aggregate and
// its per-viewer reacted_by_self bit, idempotent re-add, a second reactor, the
// ADDED/REMOVED broadcasts to the other member, and removal down to empty (the
// aggregate is retained while a reactor remains, then omitted).
func testServer_Reactions(t *testing.T, chats chat.Store, messages messaging.Store, profiles profile.Store, badges badge.Store) {
	e := newServerEnv(t, badges, chats, messages, profiles)
	const emoji = "👍"

	sent, err := e.send(e.keysA, "react to me", generateClientID())
	require.NoError(t, err)
	msgID := sent.Message.MessageId

	// userB reacts: the aggregate returns with the reactor's own bit set.
	addResp, err := e.addReaction(e.keysB, msgID, emoji)
	require.NoError(t, err)
	require.Equal(t, messagingpb.AddReactionResponse_OK, addResp.Result)
	require.Equal(t, emoji, addResp.Reaction.Emoji.Value)
	require.Equal(t, uint64(1), addResp.Reaction.Count)
	require.True(t, addResp.Reaction.ReactedBySelf)

	// The add is broadcast to the other member (userA) as an ADDED update.
	e.waitForReactionUpdate(e.userA, messagingpb.ReactionUpdate_ADDED, msgID.Value, emoji, e.userB)

	// reacted_by_self is per-viewer: true for the reactor, false for the other.
	sumB, err := e.getReactionSummary(e.keysB, msgID)
	require.NoError(t, err)
	require.Equal(t, messagingpb.GetReactionSummaryResponse_OK, sumB.Result)
	require.Len(t, sumB.Summary.Reactions, 1)
	require.True(t, sumB.Summary.Reactions[0].ReactedBySelf)
	sumA, err := e.getReactionSummary(e.keysA, msgID)
	require.NoError(t, err)
	require.Len(t, sumA.Summary.Reactions, 1)
	require.False(t, sumA.Summary.Reactions[0].ReactedBySelf)

	// Re-adding the same emoji is idempotent: the count holds at 1.
	again, err := e.addReaction(e.keysB, msgID, emoji)
	require.NoError(t, err)
	require.Equal(t, messagingpb.AddReactionResponse_OK, again.Result)
	require.Equal(t, uint64(1), again.Reaction.Count)

	// A second, distinct reactor (userA) on the same emoji brings the count to 2.
	addA, err := e.addReaction(e.keysA, msgID, emoji)
	require.NoError(t, err)
	require.Equal(t, uint64(2), addA.Reaction.Count)
	require.True(t, addA.Reaction.ReactedBySelf)

	// userB removes: one reactor (userA) remains, so the aggregate still stands
	// (count 1), with reacted_by_self false for the now-removed userB.
	rmB, err := e.removeReaction(e.keysB, msgID, emoji)
	require.NoError(t, err)
	require.Equal(t, messagingpb.RemoveReactionResponse_OK, rmB.Result)
	require.NotNil(t, rmB.Reaction)
	require.Equal(t, uint64(1), rmB.Reaction.Count)
	require.False(t, rmB.Reaction.ReactedBySelf)

	// The removal is broadcast to the other member (userA) as a REMOVED update.
	e.waitForReactionUpdate(e.userA, messagingpb.ReactionUpdate_REMOVED, msgID.Value, emoji, e.userB)

	// Removing the last reactor still surfaces the emoji's aggregate, now at Count 0
	// (it carries the advanced sequence), and drops it from the message's summary.
	rmA, err := e.removeReaction(e.keysA, msgID, emoji)
	require.NoError(t, err)
	require.Equal(t, messagingpb.RemoveReactionResponse_OK, rmA.Result)
	require.NotNil(t, rmA.Reaction)
	require.Equal(t, emoji, rmA.Reaction.Emoji.Value)
	require.Zero(t, rmA.Reaction.Count)
	// The message still exists, so the summary is returned but empty (not omitted).
	emptied, err := e.getReactionSummary(e.keysA, msgID)
	require.NoError(t, err)
	require.Equal(t, messagingpb.GetReactionSummaryResponse_OK, emptied.Result)
	require.NotNil(t, emptied.Summary)
	require.Equal(t, msgID.Value, emptied.Summary.MessageId.Value)
	require.Empty(t, emptied.Summary.Reactions)
}

// testServer_Reactions_Reactors covers the reactor drill-down (GetReactors):
// paging with the server-issued token round-tripped through options.paging_token,
// and an empty result for an emoji with no reactors.
func testServer_Reactions_Reactors(t *testing.T, chats chat.Store, messages messaging.Store, profiles profile.Store, badges badge.Store) {
	e := newServerEnv(t, badges, chats, messages, profiles)
	const emoji = "👍"

	sent, err := e.send(e.keysA, "react", generateClientID())
	require.NoError(t, err)
	msgID := sent.Message.MessageId

	_, err = e.addReaction(e.keysB, msgID, emoji)
	require.NoError(t, err)
	_, err = e.addReaction(e.keysA, msgID, emoji)
	require.NoError(t, err)

	// Full list: both reactors, no further pages.
	full, err := e.getReactors(e.keysA, msgID, emoji, &commonpb.QueryOptions{})
	require.NoError(t, err)
	require.Equal(t, messagingpb.GetReactorsResponse_OK, full.Result)
	require.False(t, full.HasMore)
	require.Len(t, full.Reactors, 2)

	// Page one row at a time, resuming via the server-issued token (set only while
	// more remain, cleared on the final page).
	page1, err := e.getReactors(e.keysA, msgID, emoji, &commonpb.QueryOptions{PageSize: 1})
	require.NoError(t, err)
	require.Len(t, page1.Reactors, 1)
	require.True(t, page1.HasMore)
	require.NotNil(t, page1.PagingToken)

	page2, err := e.getReactors(e.keysA, msgID, emoji, &commonpb.QueryOptions{PageSize: 1, PagingToken: page1.PagingToken})
	require.NoError(t, err)
	require.Len(t, page2.Reactors, 1)
	require.False(t, page2.HasMore)
	require.Nil(t, page2.PagingToken)

	// The two single-row pages cover both reactors exactly once (no overlap).
	covered := map[string]bool{
		string(page1.Reactors[0].UserId.Value): true,
		string(page2.Reactors[0].UserId.Value): true,
	}
	require.Len(t, covered, 2)
	require.True(t, covered[string(e.userA.Value)])
	require.True(t, covered[string(e.userB.Value)])

	// An unknown (but valid) emoji on a real message is an empty OK.
	none, err := e.getReactors(e.keysB, msgID, "🚀", &commonpb.QueryOptions{})
	require.NoError(t, err)
	require.Equal(t, messagingpb.GetReactorsResponse_OK, none.Result)
	require.Empty(t, none.Reactors)
	require.False(t, none.HasMore)
}

// testServer_Reactions_Summaries covers GetReactionSummaries across both request
// branches (paged query options and an explicit message-ID batch): the per-viewer
// reacted_by_self overlay is resolved correctly across messages, and a message
// with no reactions is returned with an empty summary rather than omitted.
func testServer_Reactions_Summaries(t *testing.T, chats chat.Store, messages messaging.Store, profiles profile.Store, badges badge.Store) {
	e := newServerEnv(t, badges, chats, messages, profiles)
	const thumbsUp = "👍"
	const heart = "❤️"

	m1, err := e.send(e.keysA, "first", generateClientID())
	require.NoError(t, err)
	msg1 := m1.Message.MessageId
	m2, err := e.send(e.keysA, "second", generateClientID())
	require.NoError(t, err)
	msg2 := m2.Message.MessageId
	m3, err := e.send(e.keysA, "third", generateClientID())
	require.NoError(t, err)
	msg3 := m3.Message.MessageId // stays reactionless

	// Both members react 👍 on msg1; only userB reacts ❤️ on msg2; msg3 gets none.
	_, err = e.addReaction(e.keysA, msg1, thumbsUp)
	require.NoError(t, err)
	_, err = e.addReaction(e.keysB, msg1, thumbsUp)
	require.NoError(t, err)
	_, err = e.addReaction(e.keysB, msg2, heart)
	require.NoError(t, err)

	selfOf := func(summaries []*messagingpb.ReactionSummary, msgID uint64, emoji string) (found, self bool) {
		for _, sm := range summaries {
			if sm.MessageId.Value != msgID {
				continue
			}
			for _, r := range sm.Reactions {
				if r.Emoji.Value == emoji {
					return true, r.ReactedBySelf
				}
			}
		}
		return false, false
	}

	reactionsOf := func(summaries []*messagingpb.ReactionSummary, msgID uint64) (found bool, n int) {
		for _, sm := range summaries {
			if sm.MessageId.Value == msgID {
				return true, len(sm.Reactions)
			}
		}
		return false, 0
	}

	// Paged, as userA: every message in the page is present — msg1's 👍 overlays as
	// self (userA reacted), msg2's ❤️ does not, and the reactionless msg3 comes back
	// with an empty summary rather than being omitted.
	byOpts, err := e.getReactionSummaries(e.keysA, &messagingpb.GetReactionSummariesRequest{
		Query: &messagingpb.GetReactionSummariesRequest_Options{Options: &commonpb.QueryOptions{Order: commonpb.QueryOptions_ASC}},
	})
	require.NoError(t, err)
	require.Equal(t, messagingpb.GetReactionSummariesResponse_OK, byOpts.Result)
	require.Len(t, byOpts.Summaries, 3)
	found, self := selfOf(byOpts.Summaries, msg1.Value, thumbsUp)
	require.True(t, found)
	require.True(t, self)
	found, self = selfOf(byOpts.Summaries, msg2.Value, heart)
	require.True(t, found)
	require.False(t, self)
	found, n := reactionsOf(byOpts.Summaries, msg3.Value)
	require.True(t, found)
	require.Zero(t, n)

	// By explicit IDs, as userB: both of userB's own reactions overlay as self, and
	// the requested-but-reactionless msg3 is echoed with an empty summary.
	byIDs, err := e.getReactionSummaries(e.keysB, &messagingpb.GetReactionSummariesRequest{
		Query: &messagingpb.GetReactionSummariesRequest_MessageIds{
			MessageIds: &messagingpb.MessageIdBatch{MessageIds: []*messagingpb.MessageId{msg3, msg2, msg1}},
		},
	})
	require.NoError(t, err)
	require.Len(t, byIDs.Summaries, 3)
	_, self = selfOf(byIDs.Summaries, msg1.Value, thumbsUp)
	require.True(t, self)
	_, self = selfOf(byIDs.Summaries, msg2.Value, heart)
	require.True(t, self)
	found, n = reactionsOf(byIDs.Summaries, msg3.Value)
	require.True(t, found)
	require.Zero(t, n)
}

// testServer_Reactions_Errors covers the rejection matrix across every reaction
// RPC: invalid emoji, non-member denial, missing message, the idempotent no-op
// remove, an unknown-emoji empty read, non-reactable messages, and the
// per-message distinct-emoji cap.
func testServer_Reactions_Errors(t *testing.T, chats chat.Store, messages messaging.Store, profiles profile.Store, badges badge.Store) {
	e := newServerEnv(t, badges, chats, messages, profiles)
	const emoji = "👍"

	sent, err := e.send(e.keysA, "hi", generateClientID())
	require.NoError(t, err)
	msgID := sent.Message.MessageId
	_, strangerKeys := e.addUser()
	ghost := &messagingpb.MessageId{Value: msgID.Value + 999}

	// A non-emoji value is rejected by the RPCs that carry one, even though it
	// clears the proto's size bounds.
	bad, err := e.addReaction(e.keysB, msgID, "abc")
	require.NoError(t, err)
	require.Equal(t, messagingpb.AddReactionResponse_DENIED, bad.Result)
	badRm, err := e.removeReaction(e.keysB, msgID, "abc")
	require.NoError(t, err)
	require.Equal(t, messagingpb.RemoveReactionResponse_DENIED, badRm.Result)
	badReactors, err := e.getReactors(e.keysB, msgID, "abc", &commonpb.QueryOptions{})
	require.NoError(t, err)
	require.Equal(t, messagingpb.GetReactorsResponse_DENIED, badReactors.Result)

	// Non-members are denied on every reaction RPC.
	dAdd, err := e.addReaction(strangerKeys, msgID, emoji)
	require.NoError(t, err)
	require.Equal(t, messagingpb.AddReactionResponse_DENIED, dAdd.Result)
	dRm, err := e.removeReaction(strangerKeys, msgID, emoji)
	require.NoError(t, err)
	require.Equal(t, messagingpb.RemoveReactionResponse_DENIED, dRm.Result)
	dSum, err := e.getReactionSummary(strangerKeys, msgID)
	require.NoError(t, err)
	require.Equal(t, messagingpb.GetReactionSummaryResponse_DENIED, dSum.Result)
	dSums, err := e.getReactionSummaries(strangerKeys, &messagingpb.GetReactionSummariesRequest{
		Query: &messagingpb.GetReactionSummariesRequest_Options{Options: &commonpb.QueryOptions{}},
	})
	require.NoError(t, err)
	require.Equal(t, messagingpb.GetReactionSummariesResponse_DENIED, dSums.Result)
	dReactors, err := e.getReactors(strangerKeys, msgID, emoji, &commonpb.QueryOptions{})
	require.NoError(t, err)
	require.Equal(t, messagingpb.GetReactorsResponse_DENIED, dReactors.Result)

	// A missing message is MESSAGE_NOT_FOUND for the RPCs that probe one — checked
	// after membership, so a member (not the stranger) surfaces it.
	mAdd, err := e.addReaction(e.keysB, ghost, emoji)
	require.NoError(t, err)
	require.Equal(t, messagingpb.AddReactionResponse_MESSAGE_NOT_FOUND, mAdd.Result)
	mRm, err := e.removeReaction(e.keysB, ghost, emoji)
	require.NoError(t, err)
	require.Equal(t, messagingpb.RemoveReactionResponse_MESSAGE_NOT_FOUND, mRm.Result)
	mSum, err := e.getReactionSummary(e.keysB, ghost)
	require.NoError(t, err)
	require.Equal(t, messagingpb.GetReactionSummaryResponse_MESSAGE_NOT_FOUND, mSum.Result)
	mReactors, err := e.getReactors(e.keysB, ghost, emoji, &commonpb.QueryOptions{})
	require.NoError(t, err)
	require.Equal(t, messagingpb.GetReactorsResponse_MESSAGE_NOT_FOUND, mReactors.Result)

	// Removing an emoji the caller never added is an idempotent OK with no aggregate.
	noop, err := e.removeReaction(e.keysB, msgID, emoji)
	require.NoError(t, err)
	require.Equal(t, messagingpb.RemoveReactionResponse_OK, noop.Result)
	require.Nil(t, noop.Reaction)

	// A non-reactable (system) message is rejected with CANNOT_REACT.
	systemMsg, _, err := messages.PutMessage(e.ctx, e.chatID, nil, systemContent("joined"), at(100), generateClientID(), false)
	require.NoError(t, err)
	cannot, err := e.addReaction(e.keysB, systemMsg.ID, emoji)
	require.NoError(t, err)
	require.Equal(t, messagingpb.AddReactionResponse_CANNOT_REACT, cannot.Result)

	// Exceeding the per-message distinct-emoji cap yields TOO_MANY_REACTION_TYPES.
	// The cap is filled straight through the store (which treats the emoji as an
	// opaque key) to avoid enumerating MaxReactionTypesPerMessage real emoji; the
	// server path then validates and rejects one real emoji over the line.
	capped, err := e.send(e.keysA, "popular", generateClientID())
	require.NoError(t, err)
	for i := 0; i < messaging.MaxReactionTypesPerMessage; i++ {
		_, _, tooMany, err := messages.AddReaction(e.ctx, e.chatID, capped.Message.MessageId, e.userA, fmt.Sprintf("e-%d", i), at(int64(i+1)))
		require.NoError(t, err)
		require.False(t, tooMany)
	}
	over, err := e.addReaction(e.keysB, capped.Message.MessageId, "🎉")
	require.NoError(t, err)
	require.Equal(t, messagingpb.AddReactionResponse_TOO_MANY_REACTION_TYPES, over.Result)
	require.Nil(t, over.Reaction)
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
