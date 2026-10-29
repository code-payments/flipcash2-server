package messaging

import (
	"context"

	"go.uber.org/zap"

	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"
	messagingpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/messaging/v1"

	"github.com/code-payments/flipcash2-server/chat"
)

// chatMessagingReader adapts a messaging Store to chat.MessagingReader, the read
// slice the Chat service uses to hydrate feed metadata and to show the
// messages it has chosen (see Messages). It lives here (rather than in chat)
// because chat must not import messaging; messaging already depends on chat,
// so the adapter sits on this side of the boundary.
type chatMessagingReader struct {
	log   *zap.Logger
	store Store
	media Media
}

// NewChatMessagingReader returns a chat.MessagingReader backed by the given
// messaging store, resolving media through the given Media as the Messaging
// service's reads do, for wiring the Chat service.
func NewChatMessagingReader(log *zap.Logger, store Store, media Media) chat.MessagingReader {
	return &chatMessagingReader{log: log, store: store, media: media}
}

func (r *chatMessagingReader) LastMessages(ctx context.Context, refs []chat.MessageRef) (map[string]*messagingpb.Message, error) {
	if len(refs) == 0 {
		return map[string]*messagingpb.Message{}, nil
	}

	storeRefs := make([]MessageRef, len(refs))
	for i, ref := range refs {
		storeRefs[i] = MessageRef{ChatID: ref.ChatID, MessageID: ref.MessageID}
	}

	msgs, err := r.store.GetMessagesByRefs(ctx, storeRefs)
	if err != nil {
		return nil, err
	}

	out := make(map[string]*messagingpb.Message, len(msgs))
	for _, m := range msgs {
		out[string(m.ChatID.Value)] = m.ToProto()
	}
	return out, nil
}

// Messages reads the refs in one batch and presents them exactly as the
// Messaging service's reads do (see presentMessages).
func (r *chatMessagingReader) Messages(ctx context.Context, refs []chat.MessageRef, reading chat.Reading) (map[chat.MessageKey]*messagingpb.Message, error) {
	if len(refs) == 0 {
		return map[chat.MessageKey]*messagingpb.Message{}, nil
	}

	storeRefs := make([]MessageRef, len(refs))
	for i, ref := range refs {
		storeRefs[i] = MessageRef{ChatID: ref.ChatID, MessageID: ref.MessageID}
	}

	msgs, err := r.store.GetMessagesByRefs(ctx, storeRefs)
	if err != nil {
		return nil, err
	}
	protos, err := presentMessages(ctx, r.log, r.media, reading, msgs)
	if err != nil {
		return nil, err
	}

	out := make(map[chat.MessageKey]*messagingpb.Message, len(protos))
	for i, p := range protos {
		out[chat.MessageKeyOf(msgs[i].ChatID, p.MessageId)] = p
	}
	return out, nil
}

func (r *chatMessagingReader) Pointers(ctx context.Context, refs []chat.PointerRef) (map[string][]*messagingpb.Pointer, error) {
	if len(refs) == 0 {
		return map[string][]*messagingpb.Pointer{}, nil
	}

	storeRefs := make([]PointerRef, len(refs))
	for i, ref := range refs {
		storeRefs[i] = PointerRef{ChatID: ref.ChatID, Members: ref.Members}
	}
	return r.store.GetPointersForChats(ctx, storeRefs)
}

func (r *chatMessagingReader) LatestEventSequences(ctx context.Context, chatIDs []*commonpb.ChatId) (map[string]uint64, error) {
	if len(chatIDs) == 0 {
		return map[string]uint64{}, nil
	}
	return r.store.GetLatestEventSequencesForChats(ctx, chatIDs)
}
