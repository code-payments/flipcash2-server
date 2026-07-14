package tests

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"
	"google.golang.org/grpc"

	blobpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/blob/v1"
	chatpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/chat/v1"
	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"
	eventpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/event/v1"
	messagingpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/messaging/v1"

	"github.com/code-payments/flipcash2-server/auth"
	"github.com/code-payments/flipcash2-server/badge"
	"github.com/code-payments/flipcash2-server/blob"
	blob_memory "github.com/code-payments/flipcash2-server/blob/memory"
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
		// Messaging
		testServer_SendAndGet,
		testServer_SendMessage_Idempotent,
		testServer_SendReply,
		testServer_SendMessage_DisallowedContent,
		testServer_SendMedia,
		testServer_ResolvesMediaOnRead,
		testServer_SendMessage_Broadcast,
		testServer_EditMessage,
		testServer_DeleteMessage,
		testServer_GetMessage_NotFound,
		testServer_GetMessages_NotFound,
		testServer_GetMessages_Paging,
		testServer_GetMessages_ByIDs,
		testServer_GetDelta,
		testServer_GetDelta_ResetRequired,
		// Pointers
		testServer_AdvancePointer,
		testServer_AdvancePointer_PointerTypes,
		// Reactions
		testServer_Reactions,
		testServer_Reactions_Reactors,
		testServer_Reactions_Summaries,
		testServer_Reactions_Errors,
		// Typing
		testServer_NotifyIsTyping,
		// Cross-cutting
		testServer_NonMember_Denied,
		testServer_Broadcast_IncludesActor,
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

	blobStore  blob.Store
	blobAccess blob.AccessStore
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

	blobStore := blob_memory.NewInMemory()
	blobAccess := blob_memory.NewInMemoryAccessStore()
	env.blobStore = blobStore
	env.blobAccess = blobAccess
	media := blob.NewIntegration(blobStore, blob_memory.NewInMemoryStorage(), blobAccess)

	sender := messaging.NewSender(log, badges, chats, messages, profiles, media, ocp_data.NewTestDataProvider(), push.NewNoOpPusher(), bus)
	server := messaging.NewServer(log, authz, chats, messages, media, sender)
	cc := testutil.RunGRPCServer(t, log, testutil.WithService(func(s *grpc.Server) {
		messagingpb.RegisterMessagingServer(s, server)
	}))
	env.client = messagingpb.NewMessagingClient(cc)
	return env
}

func (e *serverEnv) addUser() (*commonpb.UserId, model.KeyPair) {
	userID := model.MustGenerateUserID()
	keys := model.MustGenerateKeyPair()
	e.authz.Add(userID, keys)
	return userID, keys
}

// ============================================================================
// Request helpers
//
// One per RPC, each stamping the chat ID and signing as the given caller so a
// test expresses only what differs (who calls, with what payload). Every RPC —
// messaging, pointers, reactions, typing — is invoked through these, so the
// call sites read uniformly across the suite.
// ============================================================================

// --- messaging ---

func (e *serverEnv) send(keys model.KeyPair, text string, clientID *messagingpb.ClientMessageId) (*messagingpb.SendMessageResponse, error) {
	return e.sendContent(keys, textContent(text), clientID)
}

func (e *serverEnv) sendContent(keys model.KeyPair, content []*messagingpb.Content, clientID *messagingpb.ClientMessageId) (*messagingpb.SendMessageResponse, error) {
	req := &messagingpb.SendMessageRequest{ChatId: e.chatID, Content: content, ClientMessageId: clientID}
	require.NoError(e.t, keys.Auth(req, &req.Auth))
	return e.client.SendMessage(e.ctx, req)
}

func (e *serverEnv) getMessage(keys model.KeyPair, msgID *messagingpb.MessageId) (*messagingpb.GetMessageResponse, error) {
	req := &messagingpb.GetMessageRequest{ChatId: e.chatID, MessageId: msgID}
	require.NoError(e.t, keys.Auth(req, &req.Auth))
	return e.client.GetMessage(e.ctx, req)
}

func (e *serverEnv) getMessagesByOptions(keys model.KeyPair, opts *commonpb.QueryOptions) (*messagingpb.GetMessagesResponse, error) {
	req := &messagingpb.GetMessagesRequest{
		ChatId: e.chatID,
		Query:  &messagingpb.GetMessagesRequest_Options{Options: opts},
	}
	require.NoError(e.t, keys.Auth(req, &req.Auth))
	return e.client.GetMessages(e.ctx, req)
}

func (e *serverEnv) getMessagesByIDs(keys model.KeyPair, vals ...uint64) (*messagingpb.GetMessagesResponse, error) {
	req := &messagingpb.GetMessagesRequest{
		ChatId: e.chatID,
		Query:  &messagingpb.GetMessagesRequest_MessageIds{MessageIds: &messagingpb.MessageIdBatch{MessageIds: ids(vals...)}},
	}
	require.NoError(e.t, keys.Auth(req, &req.Auth))
	return e.client.GetMessages(e.ctx, req)
}

// getDelta opens the GetDelta server stream and drains it to completion,
// returning every response batch in order (or any non-EOF receive error).
func (e *serverEnv) getDelta(keys model.KeyPair, afterSequence uint64) ([]*messagingpb.GetDeltaResponse, error) {
	req := &messagingpb.GetDeltaRequest{ChatId: e.chatID, AfterSequence: afterSequence}
	require.NoError(e.t, keys.Auth(req, &req.Auth))
	stream, err := e.client.GetDelta(e.ctx, req)
	if err != nil {
		return nil, err
	}
	var out []*messagingpb.GetDeltaResponse
	for {
		resp, err := stream.Recv()
		if err == io.EOF {
			return out, nil
		}
		if err != nil {
			return out, err
		}
		out = append(out, resp)
	}
}

func (e *serverEnv) editMessage(keys model.KeyPair, msgID *messagingpb.MessageId, content []*messagingpb.Content, expectedEventSeq uint64) (*messagingpb.EditMessageResponse, error) {
	req := &messagingpb.EditMessageRequest{ChatId: e.chatID, MessageId: msgID, Content: content, ExpectedEventSequence: expectedEventSeq}
	require.NoError(e.t, keys.Auth(req, &req.Auth))
	return e.client.EditMessage(e.ctx, req)
}

func (e *serverEnv) deleteMessage(keys model.KeyPair, msgID *messagingpb.MessageId, expectedEventSeq uint64) (*messagingpb.DeleteMessageResponse, error) {
	req := &messagingpb.DeleteMessageRequest{ChatId: e.chatID, MessageId: msgID, ExpectedEventSequence: expectedEventSeq}
	require.NoError(e.t, keys.Auth(req, &req.Auth))
	return e.client.DeleteMessage(e.ctx, req)
}

// --- pointers ---

func (e *serverEnv) advancePointer(keys model.KeyPair, pointerType messagingpb.Pointer_Type, newValue *messagingpb.MessageId) (*messagingpb.AdvancePointerResponse, error) {
	req := &messagingpb.AdvancePointerRequest{ChatId: e.chatID, PointerType: pointerType, NewValue: newValue}
	require.NoError(e.t, keys.Auth(req, &req.Auth))
	return e.client.AdvancePointer(e.ctx, req)
}

// --- reactions ---

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

func (e *serverEnv) getReactionSummariesByOptions(keys model.KeyPair, opts *commonpb.QueryOptions) (*messagingpb.GetReactionSummariesResponse, error) {
	req := &messagingpb.GetReactionSummariesRequest{
		ChatId: e.chatID,
		Query:  &messagingpb.GetReactionSummariesRequest_Options{Options: opts},
	}
	require.NoError(e.t, keys.Auth(req, &req.Auth))
	return e.client.GetReactionSummaries(e.ctx, req)
}

func (e *serverEnv) getReactionSummariesByIDs(keys model.KeyPair, vals ...uint64) (*messagingpb.GetReactionSummariesResponse, error) {
	req := &messagingpb.GetReactionSummariesRequest{
		ChatId: e.chatID,
		Query:  &messagingpb.GetReactionSummariesRequest_MessageIds{MessageIds: &messagingpb.MessageIdBatch{MessageIds: ids(vals...)}},
	}
	require.NoError(e.t, keys.Auth(req, &req.Auth))
	return e.client.GetReactionSummaries(e.ctx, req)
}

func (e *serverEnv) getReactors(keys model.KeyPair, msgID *messagingpb.MessageId, emoji string, opts *commonpb.QueryOptions) (*messagingpb.GetReactorsResponse, error) {
	req := &messagingpb.GetReactorsRequest{ChatId: e.chatID, MessageId: msgID, Emoji: &messagingpb.Emoji{Value: emoji}, Options: opts}
	require.NoError(e.t, keys.Auth(req, &req.Auth))
	return e.client.GetReactors(e.ctx, req)
}

// --- typing ---

func (e *serverEnv) notifyIsTyping(keys model.KeyPair, state messagingpb.IsTypingNotification_State) (*messagingpb.NotifyIsTypingResponse, error) {
	req := &messagingpb.NotifyIsTypingRequest{ChatId: e.chatID, State: state}
	require.NoError(e.t, keys.Auth(req, &req.Auth))
	return e.client.NotifyIsTyping(e.ctx, req)
}

// ============================================================================
// Event-observation helpers
//
// Every broadcast a server RPC emits arrives as a ChatUpdate delivered to a
// member. The whole suite asserts on those updates through this one pair —
// chatUpdatesFor for synchronous counts, waitForChatUpdate for async arrival —
// so a new-message, pointer, reaction, or typing broadcast is checked the same
// way. The typed waiters/counters below are thin wrappers expressing the per-
// kind predicate.
// ============================================================================

// chatUpdatesFor returns every ChatUpdate observed for recipient so far, in
// observation order.
func (e *serverEnv) chatUpdatesFor(recipient *commonpb.UserId) []*eventpb.ChatUpdate {
	var out []*eventpb.ChatUpdate
	for _, ev := range e.observer.GetEvents(func(k *commonpb.UserId) bool { return bytes.Equal(k.Value, recipient.Value) }) {
		if u := ev.Event.GetChatUpdate(); u != nil {
			out = append(out, u)
		}
	}
	return out
}

// waitForChatUpdate blocks until some ChatUpdate observed for recipient matches.
func (e *serverEnv) waitForChatUpdate(recipient *commonpb.UserId, match func(*eventpb.ChatUpdate) bool) {
	e.observer.WaitFor(e.t, func(events []*event.KeyAndEvent[*commonpb.UserId, *eventpb.Event]) bool {
		for _, ev := range events {
			if !bytes.Equal(ev.Key.Value, recipient.Value) {
				continue
			}
			if u := ev.Event.GetChatUpdate(); u != nil && match(u) {
				return true
			}
		}
		return false
	})
}

// waitForNewMessage blocks until recipient observes a broadcast carrying msgID.
func (e *serverEnv) waitForNewMessage(recipient *commonpb.UserId, msgID uint64) {
	e.waitForChatUpdate(recipient, func(u *eventpb.ChatUpdate) bool {
		return u.NewMessages != nil && containsMessage(u.NewMessages.Messages, msgID)
	})
}

// waitForMessageDeleted blocks until recipient observes a message_deleted event
// for msgID. A delete rides only the event log, so this asserts on Events (not the
// deprecated new_messages, which a delete never populates).
func (e *serverEnv) waitForMessageDeleted(recipient *commonpb.UserId, msgID uint64) {
	e.waitForChatUpdate(recipient, func(u *eventpb.ChatUpdate) bool {
		if u.Events == nil {
			return false
		}
		for _, ev := range u.Events.Events {
			for _, mut := range ev.Mutations {
				if d := mut.GetMessageDeleted(); d != nil && d.MessageId.Value == msgID {
					return true
				}
			}
		}
		return false
	})
}

// waitForMessageEdited blocks until recipient observes a message_edited event for
// msgID. An edit rides only the event log, so this asserts on Events (not the
// deprecated new_messages, which an edit never populates).
func (e *serverEnv) waitForMessageEdited(recipient *commonpb.UserId, msgID uint64) {
	e.waitForChatUpdate(recipient, func(u *eventpb.ChatUpdate) bool {
		if u.Events == nil {
			return false
		}
		for _, ev := range u.Events.Events {
			for _, mut := range ev.Mutations {
				if d := mut.GetMessageEdited(); d != nil && d.MessageId.Value == msgID {
					return true
				}
			}
		}
		return false
	})
}

// countNewMessages returns how many observed broadcasts carried msgID to
// recipient — used to assert a retried send doesn't re-broadcast.
func (e *serverEnv) countNewMessages(recipient *commonpb.UserId, msgID uint64) (n int) {
	for _, u := range e.chatUpdatesFor(recipient) {
		if u.NewMessages != nil && containsMessage(u.NewMessages.Messages, msgID) {
			n++
		}
	}
	return n
}

// waitForPointerUpdate blocks until recipient observes actor's pointer of the
// given type advanced to value.
func (e *serverEnv) waitForPointerUpdate(recipient *commonpb.UserId, pointerType messagingpb.Pointer_Type, actor *commonpb.UserId, value uint64) {
	e.waitForChatUpdate(recipient, func(u *eventpb.ChatUpdate) bool {
		return u.PointerUpdates != nil && hasPointer(u.PointerUpdates.Pointers, pointerType, actor, value)
	})
}

// countPointerUpdates returns how many observed broadcasts carried actor's
// pointer of the given type to recipient. Filtering on the pointer's own actor
// discriminates a member's explicit advances from the sender's auto-advanced
// READ pointers that ride along with each send.
func (e *serverEnv) countPointerUpdates(recipient *commonpb.UserId, pointerType messagingpb.Pointer_Type, actor *commonpb.UserId) (n int) {
	for _, u := range e.chatUpdatesFor(recipient) {
		if u.PointerUpdates == nil {
			continue
		}
		for _, p := range u.PointerUpdates.Pointers {
			if p.Type == pointerType && bytes.Equal(p.UserId.Value, actor.Value) {
				n++
			}
		}
	}
	return n
}

// waitForReactionUpdate blocks until recipient observes a ReactionUpdate matching
// the given action, message, emoji, and actor.
func (e *serverEnv) waitForReactionUpdate(recipient *commonpb.UserId, action messagingpb.ReactionUpdate_Action, msgID uint64, emoji string, actor *commonpb.UserId) {
	e.waitForChatUpdate(recipient, func(u *eventpb.ChatUpdate) bool {
		if u.ReactionUpdates == nil {
			return false
		}
		for _, ru := range u.ReactionUpdates.ReactionUpdates {
			if ru.Action == action &&
				ru.MessageId.Value == msgID &&
				ru.Emoji.Value == emoji &&
				bytes.Equal(ru.Actor.Value, actor.Value) {
				return true
			}
		}
		return false
	})
}

// waitForTyping blocks until recipient observes a typing notification.
func (e *serverEnv) waitForTyping(recipient *commonpb.UserId) {
	e.waitForChatUpdate(recipient, func(u *eventpb.ChatUpdate) bool {
		return u.IsTypingNotifications != nil
	})
}

// collectDelta flattens a drained GetDelta stream: every message across batches in
// order, the head (latest_sequence, constant across the stream), and the final
// checkpoint_sequence reached.
func collectDelta(resps []*messagingpb.GetDeltaResponse) (msgs []*messagingpb.Message, latest, checkpoint uint64) {
	for _, r := range resps {
		if r.Messages != nil {
			msgs = append(msgs, r.Messages.Messages...)
		}
		if r.LatestSequence != 0 {
			latest = r.LatestSequence
		}
		if r.CheckpointSequence != 0 {
			checkpoint = r.CheckpointSequence
		}
	}
	return msgs, latest, checkpoint
}

func containsMessage(msgs []*messagingpb.Message, msgID uint64) bool {
	for _, m := range msgs {
		if m.MessageId.Value == msgID {
			return true
		}
	}
	return false
}

func hasPointer(pointers []*messagingpb.Pointer, pointerType messagingpb.Pointer_Type, actor *commonpb.UserId, value uint64) bool {
	for _, p := range pointers {
		if p.Type == pointerType && bytes.Equal(p.UserId.Value, actor.Value) && p.Value.Value == value {
			return true
		}
	}
	return false
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

// putReadyBlob seeds a READY original blob owned by owner into the blob store and
// returns its id, so a media message can reference a real, shareable blob.
// newBlobID returns a random, well-formed (16-byte) blob id.
func (e *serverEnv) putReadyBlob(owner *commonpb.UserId) *blobpb.BlobId {
	id := blob.MustGenerateID()
	require.NoError(e.t, e.blobStore.CreatePending(e.ctx, &blob.Blob{
		ID:         id,
		Rendition:  blob.RenditionOriginal,
		Owner:      owner,
		State:      blob.StatePending,
		StorageKey: "images/x/original.png",
		MimeType:   "image/png",
		SizeBytes:  1,
	}))
	_, err := e.blobStore.Advance(e.ctx, id, blob.StateReady, nil)
	require.NoError(e.t, err)
	return id
}

// chatGrantedRead reports whether the chat has been granted read access to
// blobID — i.e. whether a chat-read grant was written when the blob was shared.
func (e *serverEnv) chatGrantedRead(blobID *blobpb.BlobId) bool {
	has, err := e.blobAccess.HasGrant(e.ctx, blobID, blob.PrincipalForChat(e.chatID), blob.PermissionRead)
	require.NoError(e.t, err)
	return has
}

// ============================================================================
// Messaging
// ============================================================================

func testServer_SendAndGet(t *testing.T, chats chat.Store, messages messaging.Store, profiles profile.Store, badges badge.Store) {
	e := newServerEnv(t, badges, chats, messages, profiles)

	resp, err := e.send(e.keysA, "hello", generateClientID())
	require.NoError(t, err)
	require.Equal(t, messagingpb.SendMessageResponse_OK, resp.Result)
	require.Equal(t, uint64(1), resp.Message.MessageId.Value)
	require.Equal(t, e.userA.Value, resp.Message.SenderId.Value)

	getResp, err := e.getMessage(e.keysB, resp.Message.MessageId)
	require.NoError(t, err)
	require.Equal(t, messagingpb.GetMessageResponse_OK, getResp.Result)
	require.Equal(t, "hello", getResp.Message.Content[0].GetText().Text)

	listResp, err := e.getMessagesByOptions(e.keysB, &commonpb.QueryOptions{})
	require.NoError(t, err)
	require.Equal(t, messagingpb.GetMessagesResponse_OK, listResp.Result)
	require.Len(t, listResp.Messages.Messages, 1)
}

func testServer_SendMedia(t *testing.T, chats chat.Store, messages messaging.Store, profiles profile.Store, badges badge.Store) {
	e := newServerEnv(t, badges, chats, messages, profiles)

	// Media owned by the sender is sent, and the blob is granted to the chat.
	ownedBlob := e.putReadyBlob(e.userA)
	ownedResp, err := e.sendContent(e.keysA, mediaContent(ownedBlob), generateClientID())
	require.NoError(t, err)
	require.Equal(t, messagingpb.SendMessageResponse_OK, ownedResp.Result)
	require.True(t, e.chatGrantedRead(ownedBlob))

	// Media owned by another user is denied, and nothing is granted.
	othersBlob := e.putReadyBlob(e.userB)
	deniedResp, err := e.sendContent(e.keysA, mediaContent(othersBlob), generateClientID())
	require.NoError(t, err)
	require.Equal(t, messagingpb.SendMessageResponse_DENIED, deniedResp.Result)
	require.False(t, e.chatGrantedRead(othersBlob))

	// A reply whose body is media shares the media too.
	original, err := e.send(e.keysA, "original", generateClientID())
	require.NoError(t, err)
	replyBlob := e.putReadyBlob(e.userB)
	replyResp, err := e.sendContent(e.keysB, replyMediaContent(original.Message.MessageId.Value, replyBlob), generateClientID())
	require.NoError(t, err)
	require.Equal(t, messagingpb.SendMessageResponse_OK, replyResp.Result)
	require.True(t, e.chatGrantedRead(replyBlob))
}

func testServer_ResolvesMediaOnRead(t *testing.T, chats chat.Store, messages messaging.Store, profiles profile.Store, badges badge.Store) {
	e := newServerEnv(t, badges, chats, messages, profiles)

	// A sends a media message referencing a blob.
	blobID := e.putReadyBlob(e.userA)
	sendResp, err := e.sendContent(e.keysA, mediaContent(blobID), generateClientID())
	require.NoError(t, err)
	require.Equal(t, messagingpb.SendMessageResponse_OK, sendResp.Result)

	// The send response itself carries the resolved metadata.
	sent := sendResp.Message.Content[0].GetMedia().Items[0].Renditions[0]
	require.NotNil(t, sent.Blob)
	require.NotEmpty(t, sent.Blob.GetDownloadUrl().GetUrl())

	// The live broadcast (message_sent event) carries the resolved metadata too.
	e.waitForChatUpdate(e.userB, func(u *eventpb.ChatUpdate) bool {
		if u.Events == nil || len(u.Events.Events) != 1 || len(u.Events.Events[0].Mutations) != 1 {
			return false
		}
		sent := u.Events.Events[0].Mutations[0].GetMessageSent()
		if sent == nil || sent.MessageId.Value != sendResp.Message.MessageId.Value {
			return false
		}
		r := sent.Content[0].GetMedia().Items[0].Renditions[0]
		return r.GetBlob().GetDownloadUrl().GetUrl() != ""
	})

	// B (a member) reads it, and the server fills the rendition's resolved metadata
	// — a download URL it can fetch the bytes from — alongside the blob id.
	getResp, err := e.getMessage(e.keysB, sendResp.Message.MessageId)
	require.NoError(t, err)
	require.Equal(t, messagingpb.GetMessageResponse_OK, getResp.Result)

	got := getResp.Message.Content[0].GetMedia().Items[0].Renditions[0]
	require.Equal(t, blobID.Value, got.BlobId.Value)
	require.NotNil(t, got.Blob)
	require.Equal(t, "image/png", got.Blob.MimeType)
	require.NotNil(t, got.Blob.DownloadUrl)
	require.NotEmpty(t, got.Blob.DownloadUrl.Url)

	// The same metadata is filled on the list (GetMessages) path.
	listResp, err := e.getMessagesByOptions(e.keysB, &commonpb.QueryOptions{})
	require.NoError(t, err)
	require.Equal(t, messagingpb.GetMessagesResponse_OK, listResp.Result)
	listed := listResp.Messages.Messages[0].Content[0].GetMedia().Items[0].Renditions[0]
	require.NotNil(t, listed.Blob)
	require.NotEmpty(t, listed.Blob.DownloadUrl.Url)

	// And on the delta (GetDelta) catch-up path.
	deltaResps, err := e.getDelta(e.keysB, 0)
	require.NoError(t, err)
	deltaMsgs, _, _ := collectDelta(deltaResps)
	var delivered *blobpb.Rendition
	for _, m := range deltaMsgs {
		if m.MessageId.Value == sendResp.Message.MessageId.Value {
			delivered = m.Content[0].GetMedia().Items[0].Renditions[0]
		}
	}
	require.NotNil(t, delivered)
	require.NotNil(t, delivered.Blob)
	require.NotEmpty(t, delivered.Blob.DownloadUrl.Url)
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

	// The retried send must not re-run side effects (most importantly pushes, which
	// ride the same broadcast). Wait for the follow-up message to reach userB, then
	// check the first message was broadcast to them exactly once.
	e.waitForNewMessage(e.userB, next.Message.MessageId.Value)
	require.Equal(t, 1, e.countNewMessages(e.userB, first.Message.MessageId.Value))
}

func testServer_SendReply(t *testing.T, chats chat.Store, messages messaging.Store, profiles profile.Store, badges badge.Store) {
	e := newServerEnv(t, badges, chats, messages, profiles)

	// Seed a message to reply to.
	original, err := e.send(e.keysA, "original", generateClientID())
	require.NoError(t, err)

	// A text reply to that message is accepted and round-trips its content.
	replyResp, err := e.sendContent(e.keysB, replyContent(original.Message.MessageId.Value, "replying"), generateClientID())
	require.NoError(t, err)
	require.Equal(t, messagingpb.SendMessageResponse_OK, replyResp.Result)

	reply := replyResp.Message.Content[0].GetReply()
	require.NotNil(t, reply)
	require.Equal(t, original.Message.MessageId.Value, reply.RepliedMessageId.Value)
	require.Equal(t, "replying", reply.Content[0].GetText().Text)

	// A reply wrapping unsupported content (e.g. a nested reply) is denied.
	nested := []*messagingpb.Content{{
		Type: &messagingpb.Content_Reply{
			Reply: &messagingpb.ReplyContent{
				RepliedMessageId: original.Message.MessageId,
				Content:          replyContent(original.Message.MessageId.Value, "nested"),
			},
		},
	}}
	deniedResp, err := e.sendContent(e.keysB, nested, generateClientID())
	require.NoError(t, err)
	require.Equal(t, messagingpb.SendMessageResponse_DENIED, deniedResp.Result)

	// Replying to a message that does not exist is denied.
	missingResp, err := e.sendContent(e.keysB, replyContent(original.Message.MessageId.Value+999, "ghost"), generateClientID())
	require.NoError(t, err)
	require.Equal(t, messagingpb.SendMessageResponse_DENIED, missingResp.Result)

	// Replying to a non-replyable (system) message is denied.
	systemMsg, _, err := messages.PutMessage(e.ctx, e.chatID, nil, systemContent("joined"), at(100), generateClientID(), false)
	require.NoError(t, err)
	systemReplyResp, err := e.sendContent(e.keysB, replyContent(systemMsg.ID.Value, "to a system message"), generateClientID())
	require.NoError(t, err)
	require.Equal(t, messagingpb.SendMessageResponse_DENIED, systemReplyResp.Result)
}

func testServer_SendMessage_DisallowedContent(t *testing.T, chats chat.Store, messages messaging.Store, profiles profile.Store, badges badge.Store) {
	e := newServerEnv(t, badges, chats, messages, profiles)

	// A server-injected system message may not be authored by a client.
	systemResp, err := e.sendContent(e.keysA, systemContent("i joined"), generateClientID())
	require.NoError(t, err)
	require.Equal(t, messagingpb.SendMessageResponse_DENIED, systemResp.Result)

	// Media referencing a non-ORIGINAL rendition is rejected: a client references
	// only the original; the server derives and serves the rest.
	derivedRole := mediaContent(blob.MustGenerateID())
	derivedRole[0].GetMedia().Items[0].Renditions[0].Role = blobpb.Rendition_DISPLAY
	roleResp, err := e.sendContent(e.keysA, derivedRole, generateClientID())
	require.NoError(t, err)
	require.Equal(t, messagingpb.SendMessageResponse_DENIED, roleResp.Result)

	// Media whose item carries more than the single ORIGINAL rendition a client may
	// supply is rejected.
	extraRendition := mediaContent(blob.MustGenerateID())
	item := extraRendition[0].GetMedia().Items[0]
	item.Renditions = append(item.Renditions, &blobpb.Rendition{
		Role:   blobpb.Rendition_ORIGINAL,
		BlobId: blob.MustGenerateID(),
	})
	extraResp, err := e.sendContent(e.keysA, extraRendition, generateClientID())
	require.NoError(t, err)
	require.Equal(t, messagingpb.SendMessageResponse_DENIED, extraResp.Result)
}

func testServer_SendMessage_Broadcast(t *testing.T, chats chat.Store, messages messaging.Store, profiles profile.Store, badges badge.Store) {
	e := newServerEnv(t, badges, chats, messages, profiles)

	resp, err := e.send(e.keysA, "broadcast me", generateClientID())
	require.NoError(t, err)
	id := resp.Message.MessageId.Value

	// The response message carries event_sequence == message_id (every event is a
	// new message in this phase).
	require.Equal(t, id, resp.Message.EventSequence)

	// userB receives a single ChatUpdate carrying the send as a gap-detected,
	// single-mutation message_sent event (sequenced at its event_sequence) and, for
	// old clients, the same message in the deprecated new_messages; alongside a
	// last-activity metadata update and the sender's auto-advanced READ pointer (the
	// sender has implicitly read their own message).
	e.waitForChatUpdate(e.userB, func(u *eventpb.ChatUpdate) bool {
		if u.Events == nil || len(u.Events.Events) != 1 {
			return false
		}
		ev := u.Events.Events[0]
		if ev.Sequence != id || ev.Count != 1 || len(ev.Mutations) != 1 {
			return false
		}
		sent := ev.Mutations[0].GetMessageSent()
		if sent == nil || sent.MessageId.Value != id || sent.EventSequence != id {
			return false
		}
		return u.NewMessages != nil && containsMessage(u.NewMessages.Messages, id) &&
			len(u.MetadataUpdates) > 0 &&
			u.PointerUpdates != nil && hasPointer(u.PointerUpdates.Pointers, messagingpb.Pointer_READ, e.userA, id)
	})
}

func testServer_GetMessage_NotFound(t *testing.T, chats chat.Store, messages messaging.Store, profiles profile.Store, badges badge.Store) {
	e := newServerEnv(t, badges, chats, messages, profiles)

	resp, err := e.getMessage(e.keysA, &messagingpb.MessageId{Value: 99})
	require.NoError(t, err)
	require.Equal(t, messagingpb.GetMessageResponse_NOT_FOUND, resp.Result)
}

func testServer_EditMessage(t *testing.T, chats chat.Store, messages messaging.Store, profiles profile.Store, badges badge.Store) {
	e := newServerEnv(t, badges, chats, messages, profiles)

	// userA sends a message; in this phase event_sequence == message_id.
	sent, err := e.send(e.keysA, "original", generateClientID())
	require.NoError(t, err)
	require.Equal(t, messagingpb.SendMessageResponse_OK, sent.Result)
	msgID := sent.Message.MessageId
	eventSeq := sent.Message.EventSequence

	// A non-sender cannot edit someone else's message.
	denied, err := e.editMessage(e.keysB, msgID, textContent("hijacked"), eventSeq)
	require.NoError(t, err)
	require.Equal(t, messagingpb.EditMessageResponse_DENIED, denied.Result)

	// Disallowed replacement content (a system message a client may not author) is
	// rejected before the message is touched.
	badContent, err := e.editMessage(e.keysA, msgID, systemContent("system"), eventSeq)
	require.NoError(t, err)
	require.Equal(t, messagingpb.EditMessageResponse_DENIED, badContent.Result)

	// A missing message is a not-found.
	notFound, err := e.editMessage(e.keysA, &messagingpb.MessageId{Value: 999}, textContent("nope"), eventSeq)
	require.NoError(t, err)
	require.Equal(t, messagingpb.EditMessageResponse_MESSAGE_NOT_FOUND, notFound.Result)

	// A stale expected event_sequence conflicts, returning the current (untouched)
	// state rather than clobbering it.
	conflict, err := e.editMessage(e.keysA, msgID, textContent("edited"), eventSeq+1)
	require.NoError(t, err)
	require.Equal(t, messagingpb.EditMessageResponse_CONFLICT, conflict.Result)
	require.Equal(t, msgID.Value, conflict.Message.MessageId.Value)
	require.Equal(t, "original", conflict.Message.Content[0].GetText().Text)
	require.Nil(t, conflict.Message.LastEditedTs)

	// The sender edits their own message with the matching expected event_sequence.
	ok, err := e.editMessage(e.keysA, msgID, textContent("edited"), eventSeq)
	require.NoError(t, err)
	require.Equal(t, messagingpb.EditMessageResponse_OK, ok.Result)
	// The response carries the new content, last_edited_ts set, event_sequence
	// advanced past the message ID, ID unchanged.
	require.Equal(t, msgID.Value, ok.Message.MessageId.Value)
	require.Equal(t, "edited", ok.Message.Content[0].GetText().Text)
	require.NotNil(t, ok.Message.LastEditedTs)
	require.Greater(t, ok.Message.EventSequence, msgID.Value)

	// Members observe the edit as a message_edited event on the event log.
	e.waitForMessageEdited(e.userB, msgID.Value)

	// A second edit must supply the now-current event_sequence; the original is stale.
	editedSeq := ok.Message.EventSequence
	reEditStale, err := e.editMessage(e.keysA, msgID, textContent("edited again"), eventSeq)
	require.NoError(t, err)
	require.Equal(t, messagingpb.EditMessageResponse_CONFLICT, reEditStale.Result)

	reEdit, err := e.editMessage(e.keysA, msgID, textContent("edited again"), editedSeq)
	require.NoError(t, err)
	require.Equal(t, messagingpb.EditMessageResponse_OK, reEdit.Result)
	require.Equal(t, "edited again", reEdit.Message.Content[0].GetText().Text)
	require.Greater(t, reEdit.Message.EventSequence, editedSeq)

	// Editing to media the sender owns shares the blob into the chat and resolves
	// its metadata onto the returned message, just like a send.
	ownedBlob := e.putReadyBlob(e.userA)
	toMedia, err := e.editMessage(e.keysA, msgID, mediaContent(ownedBlob), reEdit.Message.EventSequence)
	require.NoError(t, err)
	require.Equal(t, messagingpb.EditMessageResponse_OK, toMedia.Result)
	require.True(t, e.chatGrantedRead(ownedBlob))
	rendition := toMedia.Message.Content[0].GetMedia().Items[0].Renditions[0]
	require.Equal(t, ownedBlob.Value, rendition.BlobId.Value)
	require.NotNil(t, rendition.Blob)
	require.NotEmpty(t, rendition.Blob.GetDownloadUrl().GetUrl())

	// Editing to media owned by another user is denied, and nothing is granted.
	othersBlob := e.putReadyBlob(e.userB)
	deniedMedia, err := e.editMessage(e.keysA, msgID, mediaContent(othersBlob), toMedia.Message.EventSequence)
	require.NoError(t, err)
	require.Equal(t, messagingpb.EditMessageResponse_DENIED, deniedMedia.Result)
	require.False(t, e.chatGrantedRead(othersBlob))

	// A non-editable message the caller authored (seeded directly as a system
	// message, which SendMessage won't accept) is rejected with CANNOT_EDIT.
	sysMsg, _, err := messages.PutMessage(e.ctx, e.chatID, e.userA, systemContent("system"), at(50), generateClientID(), false)
	require.NoError(t, err)
	cannot, err := e.editMessage(e.keysA, sysMsg.ID, textContent("edited"), sysMsg.EventSequence)
	require.NoError(t, err)
	require.Equal(t, messagingpb.EditMessageResponse_CANNOT_EDIT, cannot.Result)

	// A tombstone is not editable: deleting then editing is rejected with CANNOT_EDIT.
	del, err := e.deleteMessage(e.keysA, msgID, toMedia.Message.EventSequence)
	require.NoError(t, err)
	require.Equal(t, messagingpb.DeleteMessageResponse_OK, del.Result)
	cannotEditDeleted, err := e.editMessage(e.keysA, msgID, textContent("resurrect"), del.Message.EventSequence)
	require.NoError(t, err)
	require.Equal(t, messagingpb.EditMessageResponse_CANNOT_EDIT, cannotEditDeleted.Result)
}

func testServer_DeleteMessage(t *testing.T, chats chat.Store, messages messaging.Store, profiles profile.Store, badges badge.Store) {
	e := newServerEnv(t, badges, chats, messages, profiles)

	// userA sends a message; in this phase event_sequence == message_id.
	sent, err := e.send(e.keysA, "delete me", generateClientID())
	require.NoError(t, err)
	require.Equal(t, messagingpb.SendMessageResponse_OK, sent.Result)
	msgID := sent.Message.MessageId
	eventSeq := sent.Message.EventSequence

	// A non-sender cannot delete someone else's message.
	denied, err := e.deleteMessage(e.keysB, msgID, eventSeq)
	require.NoError(t, err)
	require.Equal(t, messagingpb.DeleteMessageResponse_DENIED, denied.Result)

	// A missing message is a not-found.
	notFound, err := e.deleteMessage(e.keysA, &messagingpb.MessageId{Value: 999}, eventSeq)
	require.NoError(t, err)
	require.Equal(t, messagingpb.DeleteMessageResponse_MESSAGE_NOT_FOUND, notFound.Result)

	// A stale expected event_sequence conflicts, returning the current (untouched)
	// state rather than clobbering it.
	conflict, err := e.deleteMessage(e.keysA, msgID, eventSeq+1)
	require.NoError(t, err)
	require.Equal(t, messagingpb.DeleteMessageResponse_CONFLICT, conflict.Result)
	require.Equal(t, msgID.Value, conflict.Message.MessageId.Value)
	require.Nil(t, conflict.Message.Content[0].GetDeleted())

	// The sender deletes their own message with the matching expected event_sequence.
	ok, err := e.deleteMessage(e.keysA, msgID, eventSeq)
	require.NoError(t, err)
	require.Equal(t, messagingpb.DeleteMessageResponse_OK, ok.Result)
	// The response is the tombstone: content replaced with DeletedContent (attributed
	// to the deleter), event_sequence advanced past the message ID, ID unchanged.
	require.Equal(t, msgID.Value, ok.Message.MessageId.Value)
	deleted := ok.Message.Content[0].GetDeleted()
	require.NotNil(t, deleted)
	require.Equal(t, e.userA.Value, deleted.DeletedBy.Value)
	require.Greater(t, ok.Message.EventSequence, msgID.Value)

	// Members observe the tombstone as a message_deleted event on the event log.
	e.waitForMessageDeleted(e.userB, msgID.Value)

	// Re-deleting an already-deleted message is an idempotent no-op: even though the
	// original expected_event_sequence is now stale, the desired end state already
	// holds, so it returns OK with the unchanged tombstone and does not advance the
	// event log a second time.
	deletedSeq := ok.Message.EventSequence
	reDelete, err := e.deleteMessage(e.keysA, msgID, eventSeq)
	require.NoError(t, err)
	require.Equal(t, messagingpb.DeleteMessageResponse_OK, reDelete.Result)
	require.NotNil(t, reDelete.Message.Content[0].GetDeleted())
	require.Equal(t, deletedSeq, reDelete.Message.EventSequence)
	head, err := messages.GetLatestEventSequence(e.ctx, e.chatID)
	require.NoError(t, err)
	require.Equal(t, deletedSeq, head)

	// A non-deletable message the caller authored (seeded directly as a system
	// message, which SendMessage won't accept) is rejected with CANNOT_DELETE.
	sysMsg, _, err := messages.PutMessage(e.ctx, e.chatID, e.userA, systemContent("system"), at(50), generateClientID(), false)
	require.NoError(t, err)
	cannot, err := e.deleteMessage(e.keysA, sysMsg.ID, sysMsg.EventSequence)
	require.NoError(t, err)
	require.Equal(t, messagingpb.DeleteMessageResponse_CANNOT_DELETE, cannot.Result)
}

func testServer_GetMessages_NotFound(t *testing.T, chats chat.Store, messages messaging.Store, profiles profile.Store, badges badge.Store) {
	e := newServerEnv(t, badges, chats, messages, profiles)

	resp, err := e.getMessagesByOptions(e.keysA, &commonpb.QueryOptions{})
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

	asc := func(token *commonpb.PagingToken) *commonpb.QueryOptions {
		return &commonpb.QueryOptions{PageSize: 2, Order: commonpb.QueryOptions_ASC, PagingToken: token}
	}

	// Page ascending in chunks of 2: [1,2], [3,4], [5], then empty → NOT_FOUND.
	page1, err := e.getMessagesByOptions(e.keysB, asc(nil))
	require.NoError(t, err)
	require.Equal(t, messagingpb.GetMessagesResponse_OK, page1.Result)
	require.Equal(t, []uint64{1, 2}, protoMessageIDs(page1.Messages.Messages))

	page2, err := e.getMessagesByOptions(e.keysB, asc(messaging.PageTokenFromID(lastMessageID(page1))))
	require.NoError(t, err)
	require.Equal(t, messagingpb.GetMessagesResponse_OK, page2.Result)
	require.Equal(t, []uint64{3, 4}, protoMessageIDs(page2.Messages.Messages))

	page3, err := e.getMessagesByOptions(e.keysB, asc(messaging.PageTokenFromID(lastMessageID(page2))))
	require.NoError(t, err)
	require.Equal(t, messagingpb.GetMessagesResponse_OK, page3.Result)
	require.Equal(t, []uint64{5}, protoMessageIDs(page3.Messages.Messages))

	page4, err := e.getMessagesByOptions(e.keysB, asc(messaging.PageTokenFromID(lastMessageID(page3))))
	require.NoError(t, err)
	require.Equal(t, messagingpb.GetMessagesResponse_NOT_FOUND, page4.Result)

	// Descending first page returns the newest messages.
	desc, err := e.getMessagesByOptions(e.keysB, &commonpb.QueryOptions{PageSize: 2, Order: commonpb.QueryOptions_DESC})
	require.NoError(t, err)
	require.Equal(t, messagingpb.GetMessagesResponse_OK, desc.Result)
	require.Equal(t, []uint64{5, 4}, protoMessageIDs(desc.Messages.Messages))
}

func testServer_GetMessages_ByIDs(t *testing.T, chats chat.Store, messages messaging.Store, profiles profile.Store, badges badge.Store) {
	e := newServerEnv(t, badges, chats, messages, profiles)

	for i := 0; i < 5; i++ {
		_, err := e.send(e.keysA, "m", generateClientID())
		require.NoError(t, err)
	}

	// A mix of existing and missing IDs returns only the existing ones, sorted
	// ascending; the missing 99 is omitted and duplicates collapse.
	resp, err := e.getMessagesByIDs(e.keysB, 4, 2, 99, 2)
	require.NoError(t, err)
	require.Equal(t, messagingpb.GetMessagesResponse_OK, resp.Result)
	require.Equal(t, []uint64{2, 4}, protoMessageIDs(resp.Messages.Messages))

	// All IDs missing → NOT_FOUND.
	none, err := e.getMessagesByIDs(e.keysB, 100, 200)
	require.NoError(t, err)
	require.Equal(t, messagingpb.GetMessagesResponse_NOT_FOUND, none.Result)
}

func testServer_GetDelta(t *testing.T, chats chat.Store, messages messaging.Store, profiles profile.Store, badges badge.Store) {
	e := newServerEnv(t, badges, chats, messages, profiles)

	// Seed 3 messages, assigned gapless IDs 1..3.
	for i := 0; i < 3; i++ {
		_, err := e.send(e.keysA, "m", generateClientID())
		require.NoError(t, err)
	}

	// Cold boot (after=0): the full history comes back as a state delta, ascending,
	// each message at event_sequence == its ID, converging on head == 3.
	resps, err := e.getDelta(e.keysB, 0)
	require.NoError(t, err)
	msgs, latest, checkpoint := collectDelta(resps)
	require.Equal(t, []uint64{1, 2, 3}, protoMessageIDs(msgs))
	for _, m := range msgs {
		require.Equal(t, m.MessageId.Value, m.EventSequence)
	}
	require.Equal(t, uint64(3), latest)
	require.Equal(t, uint64(3), checkpoint)

	// Incremental catch-up from a mid cursor returns only what changed past it.
	resps, err = e.getDelta(e.keysB, 1)
	require.NoError(t, err)
	msgs, latest, checkpoint = collectDelta(resps)
	require.Equal(t, []uint64{2, 3}, protoMessageIDs(msgs))
	require.Equal(t, uint64(3), latest)
	require.Equal(t, uint64(3), checkpoint)

	// Already current (cursor == head): a single response, no messages, head still
	// reported, and checkpoint left unset so the client's cursor is unchanged.
	resps, err = e.getDelta(e.keysB, 3)
	require.NoError(t, err)
	require.Len(t, resps, 1)
	require.Equal(t, messagingpb.GetDeltaResponse_OK, resps[0].Result)
	require.Nil(t, resps[0].Messages)
	require.Equal(t, uint64(3), resps[0].LatestSequence)
	require.Zero(t, resps[0].CheckpointSequence)

	// A cursor past the head (client somehow ahead) is treated as already current.
	resps, err = e.getDelta(e.keysB, 99)
	require.NoError(t, err)
	require.Len(t, resps, 1)
	require.Equal(t, messagingpb.GetDeltaResponse_OK, resps[0].Result)
	require.Nil(t, resps[0].Messages)
	require.Equal(t, uint64(3), resps[0].LatestSequence)

	// Deleting the middle message exercises the first divergence of the event
	// sequence from the message ID through the full RPC path: the delete advances
	// the event-log head to 4 and re-stamps message 2's event_sequence to 4, leaving
	// its ID untouched.
	del, err := e.deleteMessage(e.keysA, &messagingpb.MessageId{Value: 2}, 2)
	require.NoError(t, err)
	require.Equal(t, messagingpb.DeleteMessageResponse_OK, del.Result)
	require.Equal(t, uint64(4), del.Message.EventSequence)

	// A cold catch-up now returns each message once at its CURRENT event sequence —
	// the untouched 1 and 3, then the tombstone at the new head 4 — so IDs order
	// [1, 3, 2] and the head/checkpoint converge on 4 (not the message-ID head 3).
	resps, err = e.getDelta(e.keysB, 0)
	require.NoError(t, err)
	msgs, latest, checkpoint = collectDelta(resps)
	require.Equal(t, []uint64{1, 3, 2}, protoMessageIDs(msgs))
	require.Equal(t, uint64(4), latest)
	require.Equal(t, uint64(4), checkpoint)

	// Message 2 is surfaced as a materialized tombstone at event_sequence 4 — proving
	// the delta reflects the delete rather than the pre-delete text.
	require.True(t, containsMessage(msgs, 2))
	for _, m := range msgs {
		if m.MessageId.Value == 2 {
			require.Equal(t, uint64(4), m.EventSequence)
			require.NotNil(t, m.Content[0].GetDeleted())
			require.Equal(t, e.userA.Value, m.Content[0].GetDeleted().DeletedBy.Value)
		}
	}

	// An incremental catch-up from just past the untouched tail (cursor 3) returns
	// only the tombstone: it alone advanced past the cursor, and it appears exactly
	// once (no echo at its vacated original position 2).
	resps, err = e.getDelta(e.keysB, 3)
	require.NoError(t, err)
	msgs, latest, checkpoint = collectDelta(resps)
	require.Equal(t, []uint64{2}, protoMessageIDs(msgs))
	require.Equal(t, uint64(4), latest)
	require.Equal(t, uint64(4), checkpoint)
}

func testServer_GetDelta_ResetRequired(t *testing.T, chats chat.Store, messages messaging.Store, profiles profile.Store, badges badge.Store) {
	e := newServerEnv(t, badges, chats, messages, profiles)

	// Seed one past the cap (maxDeltaEvents + 1 == 1001), assigning gapless IDs
	// 1..1001. Seeded straight through the store to skip the per-send RPC and
	// broadcast overhead.
	const seeded = 1001
	for i := 0; i < seeded; i++ {
		_, _, err := messages.PutMessage(e.ctx, e.chatID, e.userA, textContent("m"), at(int64(i+1)), generateClientID(), true)
		require.NoError(t, err)
	}

	// Cold boot (after=0): the gap of 1001 exceeds the cap, so the server returns a
	// single RESET_REQUIRED and no messages.
	resps, err := e.getDelta(e.keysB, 0)
	require.NoError(t, err)
	require.Len(t, resps, 1)
	require.Equal(t, messagingpb.GetDeltaResponse_RESET_REQUIRED, resps[0].Result)
	require.Nil(t, resps[0].Messages)
	require.Equal(t, uint64(seeded), resps[0].LatestSequence)

	// A cursor at the cap boundary (gap of exactly 1000) still streams the delta,
	// in ascending 100-message batches, converging on the head.
	resps, err = e.getDelta(e.keysB, 1)
	require.NoError(t, err)
	msgs, latest, checkpoint := collectDelta(resps)
	require.Len(t, msgs, seeded-1)
	require.Equal(t, uint64(2), msgs[0].MessageId.Value)
	require.Equal(t, uint64(seeded), msgs[len(msgs)-1].MessageId.Value)
	require.Equal(t, uint64(seeded), latest)
	require.Equal(t, uint64(seeded), checkpoint)
}

// ============================================================================
// Pointers
// ============================================================================

func testServer_AdvancePointer(t *testing.T, chats chat.Store, messages messaging.Store, profiles profile.Store, badges badge.Store) {
	e := newServerEnv(t, badges, chats, messages, profiles)

	m1, err := e.send(e.keysA, "first", generateClientID())
	require.NoError(t, err)
	m2, err := e.send(e.keysA, "second", generateClientID())
	require.NoError(t, err)

	// A forward advance succeeds and broadcasts userB's new READ pointer to the
	// other member.
	okResp, err := e.advancePointer(e.keysB, messagingpb.Pointer_READ, m2.Message.MessageId)
	require.NoError(t, err)
	require.Equal(t, messagingpb.AdvancePointerResponse_OK, okResp.Result)
	e.waitForPointerUpdate(e.userA, messagingpb.Pointer_READ, e.userB, m2.Message.MessageId.Value)

	// Snapshot userB's broadcast count before the no-op below.
	before := e.countPointerUpdates(e.userA, messagingpb.Pointer_READ, e.userB)

	// Moving the pointer backward is a monotonic no-op: the result is still OK, but
	// nothing must be broadcast.
	backResp, err := e.advancePointer(e.keysB, messagingpb.Pointer_READ, m1.Message.MessageId)
	require.NoError(t, err)
	require.Equal(t, messagingpb.AdvancePointerResponse_OK, backResp.Result)

	// Give any erroneous broadcast a moment to land, then assert none did.
	time.Sleep(50 * time.Millisecond)
	require.Equal(t, before, e.countPointerUpdates(e.userA, messagingpb.Pointer_READ, e.userB), "a no-op pointer move must not broadcast")

	// A pointer past the last message is rejected.
	missResp, err := e.advancePointer(e.keysB, messagingpb.Pointer_READ, &messagingpb.MessageId{Value: 999})
	require.NoError(t, err)
	require.Equal(t, messagingpb.AdvancePointerResponse_MESSAGE_NOT_FOUND, missResp.Result)
}

func testServer_AdvancePointer_PointerTypes(t *testing.T, chats chat.Store, messages messaging.Store, profiles profile.Store, badges badge.Store) {
	e := newServerEnv(t, badges, chats, messages, profiles)

	m1, err := e.send(e.keysA, "first", generateClientID())
	require.NoError(t, err)
	m2, err := e.send(e.keysA, "second", generateClientID())
	require.NoError(t, err)

	// Advance userB's DELIVERED pointer to m1: it broadcasts a DELIVERED update to
	// userA and leaves userB's (distinct-typed) READ pointer untouched — no READ
	// update is broadcast.
	delResp, err := e.advancePointer(e.keysB, messagingpb.Pointer_DELIVERED, m1.Message.MessageId)
	require.NoError(t, err)
	require.Equal(t, messagingpb.AdvancePointerResponse_OK, delResp.Result)
	e.waitForPointerUpdate(e.userA, messagingpb.Pointer_DELIVERED, e.userB, m1.Message.MessageId.Value)
	require.Zero(t, e.countPointerUpdates(e.userA, messagingpb.Pointer_READ, e.userB))

	// Advance userB's READ pointer further, to m2: it broadcasts a READ update,
	// while the DELIVERED pointer stays at m1.
	readResp, err := e.advancePointer(e.keysB, messagingpb.Pointer_READ, m2.Message.MessageId)
	require.NoError(t, err)
	require.Equal(t, messagingpb.AdvancePointerResponse_OK, readResp.Result)
	e.waitForPointerUpdate(e.userA, messagingpb.Pointer_READ, e.userB, m2.Message.MessageId.Value)

	// Give any stray broadcast a moment, then assert each type advanced exactly
	// once — the READ advance did not disturb or re-broadcast DELIVERED.
	time.Sleep(50 * time.Millisecond)
	require.Equal(t, 1, e.countPointerUpdates(e.userA, messagingpb.Pointer_DELIVERED, e.userB))
	require.Equal(t, 1, e.countPointerUpdates(e.userA, messagingpb.Pointer_READ, e.userB))
}

// ============================================================================
// Reactions
// ============================================================================

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
	byOpts, err := e.getReactionSummariesByOptions(e.keysA, &commonpb.QueryOptions{Order: commonpb.QueryOptions_ASC})
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
	byIDs, err := e.getReactionSummariesByIDs(e.keysB, msg3.Value, msg2.Value, msg1.Value)
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

// testServer_Reactions_Errors covers the reaction-specific rejection matrix:
// invalid emoji, missing message, the idempotent no-op remove, an unknown-emoji
// empty read, non-reactable messages, and the per-message distinct-emoji cap.
// Non-member denial is covered uniformly across all RPCs by
// testServer_NonMember_Denied.
func testServer_Reactions_Errors(t *testing.T, chats chat.Store, messages messaging.Store, profiles profile.Store, badges badge.Store) {
	e := newServerEnv(t, badges, chats, messages, profiles)
	const emoji = "👍"

	sent, err := e.send(e.keysA, "hi", generateClientID())
	require.NoError(t, err)
	msgID := sent.Message.MessageId
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

// ============================================================================
// Typing
// ============================================================================

func testServer_NotifyIsTyping(t *testing.T, chats chat.Store, messages messaging.Store, profiles profile.Store, badges badge.Store) {
	e := newServerEnv(t, badges, chats, messages, profiles)

	resp, err := e.notifyIsTyping(e.keysA, messagingpb.IsTypingNotification_STARTED_TYPING)
	require.NoError(t, err)
	require.Equal(t, messagingpb.NotifyIsTypingResponse_OK, resp.Result)

	// userB (the other member) is notified; the sender is excluded.
	e.waitForTyping(e.userB)

	// Give any erroneous self-notification a moment, then assert none landed for userA.
	time.Sleep(50 * time.Millisecond)
	for _, u := range e.chatUpdatesFor(e.userA) {
		require.Nil(t, u.IsTypingNotifications, "sender should not receive their own typing notification")
	}
}

// ============================================================================
// Cross-cutting
// ============================================================================

// testServer_NonMember_Denied asserts that a non-member is denied on every RPC
// the service exposes. Membership is checked before any payload-specific
// validation, so a valid message ID and emoji still come back DENIED.
func testServer_NonMember_Denied(t *testing.T, chats chat.Store, messages messaging.Store, profiles profile.Store, badges badge.Store) {
	e := newServerEnv(t, badges, chats, messages, profiles)
	_, strangerKeys := e.addUser()

	msgID := &messagingpb.MessageId{Value: 1}
	const emoji = "👍"

	// Messaging.
	sendResp, err := e.send(strangerKeys, "intruder", generateClientID())
	require.NoError(t, err)
	require.Equal(t, messagingpb.SendMessageResponse_DENIED, sendResp.Result)

	getResp, err := e.getMessage(strangerKeys, msgID)
	require.NoError(t, err)
	require.Equal(t, messagingpb.GetMessageResponse_DENIED, getResp.Result)

	editResp, err := e.editMessage(strangerKeys, msgID, textContent("intruder edit"), 1)
	require.NoError(t, err)
	require.Equal(t, messagingpb.EditMessageResponse_DENIED, editResp.Result)

	delResp, err := e.deleteMessage(strangerKeys, msgID, 1)
	require.NoError(t, err)
	require.Equal(t, messagingpb.DeleteMessageResponse_DENIED, delResp.Result)

	listResp, err := e.getMessagesByOptions(strangerKeys, &commonpb.QueryOptions{})
	require.NoError(t, err)
	require.Equal(t, messagingpb.GetMessagesResponse_DENIED, listResp.Result)

	// Delta (server stream): a single DENIED response ends the stream.
	deltaResps, err := e.getDelta(strangerKeys, 0)
	require.NoError(t, err)
	require.Len(t, deltaResps, 1)
	require.Equal(t, messagingpb.GetDeltaResponse_DENIED, deltaResps[0].Result)

	// Pointers.
	advResp, err := e.advancePointer(strangerKeys, messagingpb.Pointer_READ, msgID)
	require.NoError(t, err)
	require.Equal(t, messagingpb.AdvancePointerResponse_DENIED, advResp.Result)

	// Reactions.
	addResp, err := e.addReaction(strangerKeys, msgID, emoji)
	require.NoError(t, err)
	require.Equal(t, messagingpb.AddReactionResponse_DENIED, addResp.Result)

	rmResp, err := e.removeReaction(strangerKeys, msgID, emoji)
	require.NoError(t, err)
	require.Equal(t, messagingpb.RemoveReactionResponse_DENIED, rmResp.Result)

	sumResp, err := e.getReactionSummary(strangerKeys, msgID)
	require.NoError(t, err)
	require.Equal(t, messagingpb.GetReactionSummaryResponse_DENIED, sumResp.Result)

	sumsResp, err := e.getReactionSummariesByOptions(strangerKeys, &commonpb.QueryOptions{})
	require.NoError(t, err)
	require.Equal(t, messagingpb.GetReactionSummariesResponse_DENIED, sumsResp.Result)

	reactorsResp, err := e.getReactors(strangerKeys, msgID, emoji, &commonpb.QueryOptions{})
	require.NoError(t, err)
	require.Equal(t, messagingpb.GetReactorsResponse_DENIED, reactorsResp.Result)

	// Typing.
	typingResp, err := e.notifyIsTyping(strangerKeys, messagingpb.IsTypingNotification_STARTED_TYPING)
	require.NoError(t, err)
	require.Equal(t, messagingpb.NotifyIsTypingResponse_DENIED, typingResp.Result)
}

func testServer_Broadcast_IncludesActor(t *testing.T, chats chat.Store, messages messaging.Store, profiles profile.Store, badges badge.Store) {
	e := newServerEnv(t, badges, chats, messages, profiles)
	const emoji = "👍"

	sent, err := e.send(e.keysA, "react to me", generateClientID())
	require.NoError(t, err)
	msgID := sent.Message.MessageId

	// userB advances their own READ pointer: the update is broadcast back to userB.
	advResp, err := e.advancePointer(e.keysB, messagingpb.Pointer_READ, msgID)
	require.NoError(t, err)
	require.Equal(t, messagingpb.AdvancePointerResponse_OK, advResp.Result)
	e.waitForPointerUpdate(e.userB, messagingpb.Pointer_READ, e.userB, msgID.Value)

	// userB reacts: the ADDED update is broadcast back to userB.
	addResp, err := e.addReaction(e.keysB, msgID, emoji)
	require.NoError(t, err)
	require.Equal(t, messagingpb.AddReactionResponse_OK, addResp.Result)
	e.waitForReactionUpdate(e.userB, messagingpb.ReactionUpdate_ADDED, msgID.Value, emoji, e.userB)

	// userB removes it: the REMOVED update is likewise broadcast back to userB.
	rmResp, err := e.removeReaction(e.keysB, msgID, emoji)
	require.NoError(t, err)
	require.Equal(t, messagingpb.RemoveReactionResponse_OK, rmResp.Result)
	e.waitForReactionUpdate(e.userB, messagingpb.ReactionUpdate_REMOVED, msgID.Value, emoji, e.userB)
}
