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
	pollpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/poll/v1"
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

// linkPreviewFixture is a text carrying a preview of a link in it, with an
// image resolved as a read resolves it.
func linkPreviewFixture() *messagingpb.Content {
	return &messagingpb.Content{Type: &messagingpb.Content_Text{
		Text: &messagingpb.TextContent{
			Text: "read https://news.example/bank-heist " + secret,
			LinkPreviews: []*messagingpb.LinkPreview{{
				Url:         "https://news.example/bank-heist",
				Title:       "Bank heist foiled",
				Description: "Police were waiting at the old bank at 6:30",
				Image:       mediaFixture().GetMedia().GetItems()[0],
			}},
		},
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
			Receiver: &commonpb.UserId{Value: bytes.Repeat([]byte{5}, 16)},
			Verb:     messagingpb.CashContent_TIPPED,
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

func widgetFixture() *messagingpb.Content {
	return &messagingpb.Content{Type: &messagingpb.Content_Widget{
		Widget: &messagingpb.WidgetContent{Type: &messagingpb.WidgetContent_ShareProfile{
			ShareProfile: &messagingpb.ShareProfileWidget{Username: &commonpb.Username{Value: "alice_1"}},
		}},
	}}
}

func systemFixture() *messagingpb.Content {
	return &messagingpb.Content{Type: &messagingpb.Content_System{
		System: &messagingpb.SystemContent{
			FallbackText: "A new member joined the group",
			Type: &messagingpb.SystemContent_UserJoined_{UserJoined: &messagingpb.SystemContent_UserJoined{
				UserId: &commonpb.UserId{Value: bytes.Repeat([]byte{2}, 16)},
				Method: &messagingpb.SystemContent_UserJoined_Admitted_{Admitted: &messagingpb.SystemContent_UserJoined_Admitted{
					AdmittedBy: &commonpb.UserId{Value: bytes.Repeat([]byte{1}, 16)},
				}},
			}},
		},
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

func encryptedFixture() *messagingpb.Content {
	return &messagingpb.Content{Type: &messagingpb.Content_Encrypted{
		Encrypted: &messagingpb.EncryptedContent{
			Scheme:     messagingpb.EncryptedContent_X25519_XCHACHA20POLY1305,
			Nonce:      bytes.Repeat([]byte{6}, 24),
			Ciphertext: bytes.Repeat([]byte{9}, 64),
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

func TestContent_TextLinkPreviews(t *testing.T) {
	in := linkPreviewFixture()
	out := mustContent(t, chatID, messageID, in)
	text := out.GetText()
	require.NotNil(t, text)
	assert.Equal(t, Text(contentSeed, in.GetText().GetText()), text.GetText())

	// The preview is kept, as a placeholder: a fixed URL, title and
	// description redacted like text, the image like media.
	require.Len(t, text.GetLinkPreviews(), 1)
	got := text.GetLinkPreviews()[0]
	want := in.GetText().GetLinkPreviews()[0]
	assert.Equal(t, redactedURL, got.GetUrl())
	assert.Equal(t, Text(contentSeed, want.GetTitle()), got.GetTitle())
	assert.Equal(t, Text(contentSeed, want.GetDescription()), got.GetDescription())
	require.Len(t, got.GetImage().GetRenditions(), 2)
	for i, r := range got.GetImage().GetRenditions() {
		wantRendition := proto.Clone(want.GetImage().GetRenditions()[i]).(*blobpb.Rendition)
		if wantRendition.Blob != nil {
			wantRendition.Blob.DownloadUrl = nil
		}
		assert.True(t, proto.Equal(wantRendition, r), "rendition %d", i)
	}

	bs, err := proto.Marshal(out)
	require.NoError(t, err)
	for _, leak := range []string{"news.example", "heist", "Police", "cdn.example", "bank"} {
		assert.NotContains(t, string(bs), leak)
	}

	// A preview with no description or image stays without them, and a text
	// with no previews has none.
	bare := linkPreviewFixture()
	bare.GetText().GetLinkPreviews()[0].Description = ""
	bare.GetText().GetLinkPreviews()[0].Image = nil
	bareOut := mustContent(t, chatID, messageID, bare).GetText().GetLinkPreviews()[0]
	assert.Empty(t, bareOut.GetDescription())
	assert.Nil(t, bareOut.GetImage())
	assert.Empty(t, mustContent(t, chatID, messageID, textFixture()).GetText().GetLinkPreviews())

	// In a reply's body too.
	reply := mustContent(t, chatID, messageID, replyFixture(linkPreviewFixture())).GetReply()
	require.Len(t, reply.GetContent(), 1)
	require.Len(t, reply.GetContent()[0].GetText().GetLinkPreviews(), 1)
	assert.Equal(t, redactedURL, reply.GetContent()[0].GetText().GetLinkPreviews()[0].GetUrl())
}

func TestContent_Cash(t *testing.T) {
	in := cashFixture()
	out := mustContent(t, chatID, messageID, in)
	assert.True(t, proto.Equal(in, out))
	assert.NotSame(t, in.GetCash(), out.GetCash())
}

// TestContent_System: a system message is the server's, not a member's, and
// passes through unchanged, as the proto contract says it does.
func TestContent_System(t *testing.T) {
	in := systemFixture()
	out := mustContent(t, chatID, messageID, in)
	assert.True(t, proto.Equal(in, out))
	assert.NotSame(t, in.GetSystem(), out.GetSystem())
}

// TestContent_Encrypted: the server cannot read a ciphertext, so it passes
// through unchanged; only a DM's members, who hold the key, ever read it.
func TestContent_Encrypted(t *testing.T) {
	in := encryptedFixture()
	out := mustContent(t, chatID, messageID, in)
	assert.True(t, proto.Equal(in, out))
	assert.NotSame(t, in.GetEncrypted(), out.GetEncrypted())
}

// TestContent_Widget: a widget keeps its variant, so the viewer sees a
// profile was shared, but not whose.
func TestContent_Widget(t *testing.T) {
	out := mustContent(t, chatID, messageID, widgetFixture())
	username := out.GetWidget().GetShareProfile().GetUsername()
	assert.Equal(t, redactedUsername, username.GetValue())
	assert.NoError(t, out.Validate())

	// A poll widget is kept as is: its ID says nothing of the poll.
	pollWidget := &messagingpb.Content{Type: &messagingpb.Content_Widget{
		Widget: &messagingpb.WidgetContent{Type: &messagingpb.WidgetContent_Poll{
			Poll: &messagingpb.PollWidget{PollId: &pollpb.PollId{Value: bytes.Repeat([]byte{9}, 16)}},
		}},
	}}
	redactedPoll := mustContent(t, chatID, messageID, pollWidget)
	assert.True(t, proto.Equal(pollWidget, redactedPoll))
	assert.NotSame(t, pollWidget.GetWidget().GetPoll(), redactedPoll.GetWidget().GetPoll())

	// A widget variant this function does not know is not passed through.
	for _, in := range []*messagingpb.Content{
		{Type: &messagingpb.Content_Widget{}},
		{Type: &messagingpb.Content_Widget{Widget: &messagingpb.WidgetContent{}}},
	} {
		_, err := Content(chatID, messageID, in)
		assert.ErrorIs(t, err, ErrUnsupportedContent)
	}
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
		if in.GetDeleted() == nil && in.GetCash() == nil && in.GetSystem() == nil && in.GetEncrypted() == nil {
			assert.False(t, proto.Equal(in, out), "%v", in)
		}
	}
}

func fixturesForAllKinds() []*messagingpb.Content {
	return []*messagingpb.Content{
		textFixture(),
		linkPreviewFixture(),
		replyFixture(textFixture()),
		replyFixture(mediaFixture()),
		replyFixture(linkPreviewFixture()),
		mediaFixture(),
		cashFixture(),
		systemFixture(),
		deletedFixture(),
		encryptedFixture(),
		widgetFixture(),
	}
}
