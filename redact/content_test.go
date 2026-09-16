package redact

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	blobpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/blob/v1"
	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"
	messagingpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/messaging/v1"
)

const secret = "Meet me at the old bank at 6:30, bring the cash 💰"

var (
	chatID    = &commonpb.ChatId{Value: bytes.Repeat([]byte{7}, 16)}
	messageID = &messagingpb.MessageId{Value: 42}
	// contentSeed is what Content seeds its texts with, so a test can
	// predict a placeholder with Text.
	contentSeed = messageSeed(chatID, messageID)
)

func textFixture() *messagingpb.Content {
	return &messagingpb.Content{Type: &messagingpb.Content_Text{
		Text: &messagingpb.TextContent{Text: secret},
	}}
}

func mediaFixture() *messagingpb.Content {
	return &messagingpb.Content{Type: &messagingpb.Content_Media{
		Media: &messagingpb.MediaContent{
			Items: []*blobpb.Media{{
				Renditions: []*blobpb.Rendition{
					{
						Role:   blobpb.Rendition_ORIGINAL,
						BlobId: &blobpb.BlobId{Value: bytes.Repeat([]byte{1}, 16)},
						Blob: &blobpb.BlobMetadata{
							MimeType:    "image/jpeg",
							SizeBytes:   123456,
							DownloadUrl: &blobpb.DownloadUrl{Url: "https://cdn.example/secret?sig=abc"},
							Kind: &blobpb.BlobMetadata_Image{Image: &blobpb.ImageMetadata{
								Width: 1024, Height: 768, Blurhash: "LEHV6nWB2yk8pyo0adR*.7kCMdnj",
							}},
						},
					},
					{
						Role:   blobpb.Rendition_THUMBNAIL,
						BlobId: &blobpb.BlobId{Value: bytes.Repeat([]byte{2}, 16)},
					},
				},
			}},
			Caption: &messagingpb.TextContent{Text: secret},
		},
	}}
}

func cashFixture() *messagingpb.Content {
	return &messagingpb.Content{Type: &messagingpb.Content_Cash{
		Cash: &messagingpb.CashContent{
			IntentId: &commonpb.IntentId{Value: bytes.Repeat([]byte{3}, 32)},
			Amount: &commonpb.CryptoPaymentAmount{
				Currency:     "usd",
				NativeAmount: 42.5,
				Quarks:       42_500_000,
				Mint:         &commonpb.PublicKey{Value: bytes.Repeat([]byte{4}, 32)},
			},
			Verb: messagingpb.CashContent_TIPPED,
		},
	}}
}

func replyFixture(body *messagingpb.Content) *messagingpb.Content {
	return &messagingpb.Content{Type: &messagingpb.Content_Reply{
		Reply: &messagingpb.ReplyContent{
			RepliedMessageId: &messagingpb.MessageId{Value: 17},
			Content:          []*messagingpb.Content{body},
		},
	}}
}

func systemFixture() *messagingpb.Content {
	return &messagingpb.Content{Type: &messagingpb.Content_System{
		System: &messagingpb.SystemContent{FallbackText: "Alice added Bob to the chat"},
	}}
}

func deletedFixture() *messagingpb.Content {
	return &messagingpb.Content{Type: &messagingpb.Content_Deleted{
		Deleted: &messagingpb.DeletedContent{
			DeletedTs: timestamppb.New(timestamppb.Now().AsTime()),
			DeletedBy: &commonpb.UserId{Value: bytes.Repeat([]byte{5}, 16)},
		},
	}}
}

// mustContent redacts c and fails the test on error.
func mustContent(t *testing.T, chatID *commonpb.ChatId, messageID *messagingpb.MessageId, c *messagingpb.Content) *messagingpb.Content {
	t.Helper()
	out, err := Content(chatID, messageID, c)
	require.NoError(t, err)
	require.NotNil(t, out)
	return out
}

func TestContent_Text(t *testing.T) {
	out := mustContent(t, chatID, messageID, textFixture())
	assert.Equal(t, Text(contentSeed, secret), out.GetText().GetText())
	assert.NotContains(t, out.GetText().GetText(), "bank")
}

func TestContent_Reply(t *testing.T) {
	out := mustContent(t, chatID, messageID, replyFixture(textFixture()))
	reply := out.GetReply()
	require.NotNil(t, reply)
	assert.EqualValues(t, 17, reply.GetRepliedMessageId().GetValue())
	require.Len(t, reply.GetContent(), 1)
	assert.Equal(t, Text(contentSeed, secret), reply.GetContent()[0].GetText().GetText())

	// A reply body of a kind that cannot be redacted fails the whole reply,
	// so nothing is passed through.
	_, err := Content(chatID, messageID, replyFixture(&messagingpb.Content{}))
	assert.ErrorIs(t, err, ErrUnsupportedContent)
	_, err = Content(chatID, messageID, replyFixture(systemFixture()))
	assert.ErrorIs(t, err, ErrUnsupportedContent)
}

func TestContent_Media(t *testing.T) {
	out := mustContent(t, chatID, messageID, mediaFixture())
	media := out.GetMedia()
	require.NotNil(t, media)
	assert.Equal(t, Text(contentSeed, secret), media.GetCaption().GetText())

	// Every rendition survives with its blob ID and metadata, less the
	// download URL.
	in := mediaFixture().GetMedia()
	require.Len(t, media.GetItems(), 1)
	require.Len(t, media.GetItems()[0].GetRenditions(), 2)
	for i, r := range media.GetItems()[0].GetRenditions() {
		want := proto.Clone(in.GetItems()[0].GetRenditions()[i]).(*blobpb.Rendition)
		if want.Blob != nil {
			want.Blob.DownloadUrl = nil
		}
		assert.True(t, proto.Equal(want, r), "rendition %d", i)
	}
	original := media.GetItems()[0].GetRenditions()[0]
	assert.Equal(t, bytes.Repeat([]byte{1}, 16), original.GetBlobId().GetValue())
	assert.Equal(t, "LEHV6nWB2yk8pyo0adR*.7kCMdnj", original.GetBlob().GetImage().GetBlurhash())
	assert.Nil(t, original.GetBlob().GetDownloadUrl())

	bs, err := proto.Marshal(out)
	require.NoError(t, err)
	assert.NotContains(t, string(bs), "cdn.example")

	// A media without a caption stays without one.
	noCaption := mediaFixture()
	noCaption.GetMedia().Caption = nil
	assert.Nil(t, mustContent(t, chatID, messageID, noCaption).GetMedia().GetCaption())
}

func TestContent_Cash(t *testing.T) {
	in := cashFixture()
	out := mustContent(t, chatID, messageID, in)
	assert.True(t, proto.Equal(in, out))
	assert.NotSame(t, in.GetCash(), out.GetCash())
}

// TestContent_System: no placeholder is defined for a system message yet,
// so it is refused rather than guessed at.
func TestContent_System(t *testing.T) {
	out, err := Content(chatID, messageID, systemFixture())
	assert.ErrorIs(t, err, ErrUnsupportedContent)
	assert.Nil(t, out)
}

func TestContent_Deleted(t *testing.T) {
	in := deletedFixture()
	out := mustContent(t, chatID, messageID, in)
	assert.True(t, proto.Equal(in, out))
	assert.NotSame(t, in.GetDeleted(), out.GetDeleted())
}

// TestContent_SeedIsChatScoped: message IDs are per-chat sequence numbers,
// so the same number in two chats must not render the same placeholder,
// while the same message must wherever it is read.
func TestContent_SeedIsChatScoped(t *testing.T) {
	// A long text has enough draws that two seeds practically never agree.
	long := &messagingpb.Content{Type: &messagingpb.Content_Text{
		Text: &messagingpb.TextContent{Text: conversation[31].text},
	}}
	same := mustContent(t, chatID, messageID, long)
	assert.True(t, proto.Equal(same, mustContent(t, chatID, messageID, long)))

	otherChat := &commonpb.ChatId{Value: bytes.Repeat([]byte{8}, 16)}
	assert.False(t, proto.Equal(same, mustContent(t, otherChat, messageID, long)))
	assert.False(t, proto.Equal(same, mustContent(t, chatID, &messagingpb.MessageId{Value: 43}, long)))
}

func TestContent_Unknown(t *testing.T) {
	for _, in := range []*messagingpb.Content{nil, {}} {
		out, err := Content(chatID, messageID, in)
		assert.ErrorIs(t, err, ErrUnsupportedContent)
		assert.Nil(t, out)
	}
}

// TestContent_Idempotent: a redacted content is its own redaction, so the
// chokepoint may run over a message more than once.
func TestContent_Idempotent(t *testing.T) {
	for _, in := range fixturesForAllKinds() {
		once := mustContent(t, chatID, messageID, in)
		twice := mustContent(t, chatID, messageID, once)
		assert.True(t, proto.Equal(once, twice), "%v", in)
	}
}

// TestContent_LeavesInputAlone: the input may be a stored or cached proto, so
// it is never modified.
func TestContent_LeavesInputAlone(t *testing.T) {
	for _, in := range fixturesForAllKinds() {
		before := proto.Clone(in)
		out := mustContent(t, chatID, messageID, in)
		assert.True(t, proto.Equal(before, in), "%v", in)
		if in.GetDeleted() == nil && in.GetCash() == nil {
			assert.False(t, proto.Equal(in, out), "%v", in)
		}
	}
}

func fixturesForAllKinds() []*messagingpb.Content {
	return []*messagingpb.Content{
		textFixture(),
		replyFixture(textFixture()),
		replyFixture(mediaFixture()),
		mediaFixture(),
		cashFixture(),
		deletedFixture(),
	}
}
