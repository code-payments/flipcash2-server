package messaging

import (
	"context"

	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"
	messagingpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/messaging/v1"

	"github.com/code-payments/flipcash2-server/chat"
)

// chatMessagingReader adapts a messaging Store to chat.MessagingReader, the read
// slice the Chat service uses to hydrate feed metadata. It lives here (rather
// than in chat) because chat must not import messaging; messaging already
// depends on chat, so the adapter sits on this side of the boundary.
type chatMessagingReader struct {
	store Store
}

// NewChatMessagingReader returns a chat.MessagingReader backed by the given
// messaging store, for wiring the Chat service.
func NewChatMessagingReader(store Store) chat.MessagingReader {
	return &chatMessagingReader{store: store}
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
