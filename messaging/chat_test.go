package messaging_test

import (
	"context"
	"crypto/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	blobpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/blob/v1"
	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"
	messagingpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/messaging/v1"

	"github.com/code-payments/flipcash2-server/chat"
	"github.com/code-payments/flipcash2-server/messaging"
	"github.com/code-payments/flipcash2-server/messaging/memory"
	"github.com/code-payments/flipcash2-server/model"
)

func TestChatMessagingReader(t *testing.T) {
	ctx := context.Background()
	store := memory.NewInMemory()
	reader := messaging.NewChatMessagingReader(zap.NewNop(), store, &fakeMedia{})

	chatA := randomChatID()
	chatB := randomChatID()
	sender := model.MustGenerateUserID()

	_, _, err := store.PutMessage(ctx, chatA, sender, textContent("a1"), time.Unix(1, 0), randomClientID(), true)
	require.NoError(t, err)
	a2, _, err := store.PutMessage(ctx, chatA, sender, textContent("a2"), time.Unix(2, 0), randomClientID(), true)
	require.NoError(t, err)
	b1, _, err := store.PutMessage(ctx, chatB, sender, textContent("b1"), time.Unix(1, 0), randomClientID(), true)
	require.NoError(t, err)

	// LastMessages: one ref per chat comes back keyed by chat ID, as proto.
	got, err := reader.LastMessages(ctx, []chat.MessageRef{
		{ChatID: chatA, MessageID: a2.ID},
		{ChatID: chatB, MessageID: b1.ID},
	})
	require.NoError(t, err)
	require.Len(t, got, 2)
	require.Equal(t, a2.ID.Value, got[string(chatA.Value)].MessageId.Value)
	require.Equal(t, "a2", got[string(chatA.Value)].Content[0].GetText().Text)
	require.Equal(t, b1.ID.Value, got[string(chatB.Value)].MessageId.Value)

	// Empty refs → empty map, no error.
	none, err := reader.LastMessages(ctx, nil)
	require.NoError(t, err)
	require.Empty(t, none)

	// Pointers: delegated, keyed by chat; a chat with none is absent.
	_, _, err = store.AdvancePointer(ctx, chatA, sender, messagingpb.Pointer_READ, a2.ID)
	require.NoError(t, err)
	pointers, err := reader.Pointers(ctx, []chat.PointerRef{
		{ChatID: chatA, Members: []*commonpb.UserId{sender}},
		{ChatID: chatB, Members: []*commonpb.UserId{sender}},
	})
	require.NoError(t, err)
	require.Len(t, pointers[string(chatA.Value)], 1)
	require.Equal(t, messagingpb.Pointer_READ, pointers[string(chatA.Value)][0].Type)
	_, ok := pointers[string(chatB.Value)]
	require.False(t, ok)

	// LatestEventSequences: delegated, keyed by chat; empty input → empty map.
	seqs, err := reader.LatestEventSequences(ctx, []*commonpb.ChatId{chatA, chatB})
	require.NoError(t, err)
	require.Equal(t, uint64(2), seqs[string(chatA.Value)])
	require.Equal(t, uint64(1), seqs[string(chatB.Value)])
	noSeqs, err := reader.LatestEventSequences(ctx, nil)
	require.NoError(t, err)
	require.Empty(t, noSeqs)
}

func TestChatMessagingReader_Messages(t *testing.T) {
	ctx := context.Background()
	store := memory.NewInMemory()
	original := randomBlobID()
	thumbnail := randomBlobID()
	media := &fakeMedia{renditions: map[string][]*blobpb.Rendition{
		string(original.Value): {
			{Role: blobpb.Rendition_ORIGINAL, BlobId: original},
			{Role: blobpb.Rendition_THUMBNAIL, BlobId: thumbnail},
		},
	}}
	reader := messaging.NewChatMessagingReader(zap.NewNop(), store, media)

	chatA := randomChatID()
	chatB := randomChatID()
	sender := model.MustGenerateUserID()

	a1, _, err := store.PutMessage(ctx, chatA, sender, textContent("a1"), time.Unix(1, 0), randomClientID(), true)
	require.NoError(t, err)
	a2, _, err := store.PutMessage(ctx, chatA, sender, mediaContent(original), time.Unix(2, 0), randomClientID(), true)
	require.NoError(t, err)
	a3, _, err := store.PutMessage(ctx, chatA, sender, textContent("a3"), time.Unix(3, 0), randomClientID(), true)
	require.NoError(t, err)
	b1, _, err := store.PutMessage(ctx, chatB, sender, textContent("b1"), time.Unix(1, 0), randomClientID(), true)
	require.NoError(t, err)
	_, err = store.DeleteMessage(ctx, chatA, a3.ID, sender, time.Unix(4, 0), a3.EventSequence)
	require.NoError(t, err)

	refs := []chat.MessageRef{
		{ChatID: chatA, MessageID: a1.ID},
		{ChatID: chatA, MessageID: a2.ID},
		{ChatID: chatA, MessageID: a3.ID},
		{ChatID: chatB, MessageID: b1.ID},
		{ChatID: chatB, MessageID: &messagingpb.MessageId{Value: 99}}, // missing
		{ChatID: chatA, MessageID: a1.ID},                             // repeated
	}

	// Full: several messages of one chat and one of another, each under its
	// own key, a missing ref absent, media resolved, a deleted message as its
	// tombstone.
	got, err := reader.Messages(ctx, refs, chat.ReadingFull)
	require.NoError(t, err)
	require.Len(t, got, 4)
	require.Equal(t, "a1", got[chat.MessageKeyOf(chatA, a1.ID)].Content[0].GetText().Text)
	require.Equal(t, "b1", got[chat.MessageKeyOf(chatB, b1.ID)].Content[0].GetText().Text)
	require.Len(t, got[chat.MessageKeyOf(chatA, a2.ID)].Content[0].GetMedia().Items[0].Renditions, 2)
	require.NotNil(t, got[chat.MessageKeyOf(chatA, a3.ID)].Content[0].GetDeleted())
	_, ok := got[chat.MessageKeyOf(chatB, &messagingpb.MessageId{Value: 99})]
	require.False(t, ok)
	for _, m := range got {
		require.False(t, m.Redacted)
	}

	// Redacted: every message redacted, media still resolved first.
	redacted, err := reader.Messages(ctx, refs, chat.ReadingRedacted)
	require.NoError(t, err)
	require.Len(t, redacted, 4)
	for _, m := range redacted {
		require.True(t, m.Redacted)
	}
	require.NotEqual(t, "a1", redacted[chat.MessageKeyOf(chatA, a1.ID)].Content[0].GetText().Text)
	require.Len(t, redacted[chat.MessageKeyOf(chatA, a2.ID)].Content[0].GetMedia().Items[0].Renditions, 2)

	// Empty refs → empty map, no error.
	none, err := reader.Messages(ctx, nil, chat.ReadingFull)
	require.NoError(t, err)
	require.Empty(t, none)
}

// fakeMedia resolves the renditions registered per ORIGINAL blob ID and
// shares nothing.
type fakeMedia struct {
	renditions map[string][]*blobpb.Rendition
}

func (f *fakeMedia) ShareIntoChat(context.Context, *commonpb.UserId, *commonpb.ChatId, []*blobpb.BlobId) error {
	return nil
}

func (f *fakeMedia) ResolveRenditions(_ context.Context, ids []*blobpb.BlobId) (map[string][]*blobpb.Rendition, error) {
	out := make(map[string][]*blobpb.Rendition)
	for _, id := range ids {
		if r, ok := f.renditions[string(id.Value)]; ok {
			out[string(id.Value)] = r
		}
	}
	return out, nil
}

func mediaContent(original *blobpb.BlobId) []*messagingpb.Content {
	return []*messagingpb.Content{{
		Type: &messagingpb.Content_Media{Media: &messagingpb.MediaContent{
			Items: []*blobpb.Media{{Renditions: []*blobpb.Rendition{{Role: blobpb.Rendition_ORIGINAL, BlobId: original}}}},
		}},
	}}
}

func randomBlobID() *blobpb.BlobId {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		panic(err)
	}
	return &blobpb.BlobId{Value: b}
}

func textContent(text string) []*messagingpb.Content {
	return []*messagingpb.Content{{
		Type: &messagingpb.Content_Text{Text: &messagingpb.TextContent{Text: text}},
	}}
}

func randomChatID() *commonpb.ChatId {
	b := make([]byte, chat.DmChatIDSize)
	if _, err := rand.Read(b); err != nil {
		panic(err)
	}
	return &commonpb.ChatId{Value: b}
}

func randomClientID() *messagingpb.ClientMessageId {
	b := make([]byte, messaging.ClientMessageIDSize)
	if _, err := rand.Read(b); err != nil {
		panic(err)
	}
	return &messagingpb.ClientMessageId{Value: b}
}
