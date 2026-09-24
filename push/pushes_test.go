package push

import (
	"bytes"
	"context"
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"
	messagingpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/messaging/v1"
	pushpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/push/v1"
)

func TestTruncatePushBody(t *testing.T) {
	for _, tc := range []struct {
		name string
		body string
		want string
	}{
		{"empty", "", ""},
		{"short", "hello", "hello"},
		{"exactly the limit", strings.Repeat("a", maxPushBodyChars), strings.Repeat("a", maxPushBodyChars)},
		{"one over the limit", strings.Repeat("a", maxPushBodyChars+1), strings.Repeat("a", maxPushBodyChars) + "..."},
		// Multi-byte characters count once each, so a body at the limit in
		// characters is kept whole however many bytes it spans.
		{"multi-byte at the limit", strings.Repeat("é", maxPushBodyChars), strings.Repeat("é", maxPushBodyChars)},
		{"multi-byte over the limit", strings.Repeat("😀", maxPushBodyChars+5), strings.Repeat("😀", maxPushBodyChars) + "..."},
		// A cut landing just after a multi-byte character keeps it whole.
		{"cut after a multi-byte character", strings.Repeat("a", maxPushBodyChars-1) + "😀tail", strings.Repeat("a", maxPushBodyChars-1) + "😀..."},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := truncatePushBody(tc.body)
			assert.Equal(t, tc.want, got)
			assert.True(t, utf8.ValidString(got))
		})
	}
}

// TestChatMessagePush_MessageOrID sweeps message sizes through every chat
// push builder: a push carries the full message exactly when that measures
// within maxChatPushBytes, and otherwise only the message's ID, which always
// fits. Either way the sender, title and body are set, and the muted copy
// carries the same reference.
func TestChatMessagePush_MessageOrID(t *testing.T) {
	ctx := context.Background()
	chatID := &commonpb.ChatId{Value: bytes.Repeat([]byte{3}, 32)}
	groupID := &commonpb.ChatId{Value: bytes.Repeat([]byte{4}, 16)}
	senderID := &commonpb.UserId{Value: bytes.Repeat([]byte{2}, 16)}
	phone := &commonpb.PhoneNumber{Value: "+15555555555"}

	builders := map[string]func(*messagingpb.Message) (*ChatMessagePush, error){
		"contact dm": func(m *messagingpb.Message) (*ChatMessagePush, error) {
			return BuildContactDmPush(ctx, nil, chatID, m, senderID, phone)
		},
		"tip dm": func(m *messagingpb.Message) (*ChatMessagePush, error) {
			return BuildTipDmPush(ctx, nil, chatID, m, senderID, "Sender Name")
		},
		"group": func(m *messagingpb.Message) (*ChatMessagePush, error) {
			return BuildGroupChatPush(ctx, nil, groupID, m, senderID, "Sender Name", "Group Title")
		},
	}

	var contents []*messagingpb.Content
	for n := 1; n <= 4096; n += 97 {
		contents = append(contents, &messagingpb.Content{Type: &messagingpb.Content_Text{
			Text: &messagingpb.TextContent{Text: strings.Repeat("a", n)},
		}})
	}
	for n := 17; n <= 16640; n += 397 {
		contents = append(contents, &messagingpb.Content{Type: &messagingpb.Content_Encrypted{
			Encrypted: &messagingpb.EncryptedContent{
				Scheme:     messagingpb.EncryptedContent_X25519_XCHACHA20POLY1305,
				Nonce:      bytes.Repeat([]byte{6}, 24),
				Ciphertext: bytes.Repeat([]byte{9}, n),
			},
		}})
	}

	for name, build := range builders {
		t.Run(name, func(t *testing.T) {
			var carried, referenced int
			for _, content := range contents {
				if name == "group" && content.GetEncrypted() != nil {
					continue // encrypted content never reaches a group
				}
				message := testChatMessage(content)
				p, err := build(message)
				require.NoError(t, err)
				require.NotNil(t, p)
				require.NotEmpty(t, p.title)
				require.NotEmpty(t, p.body)
				require.True(t, proto.Equal(senderID, p.payload.ChatMetadata.SendingUserId))

				size, err := payloadSize(p.title, p.body, p.payload)
				require.NoError(t, err)
				require.LessOrEqual(t, size, maxChatPushBytes)

				if got := p.payload.ChatMetadata.GetMessage(); got != nil {
					carried++
					require.True(t, proto.Equal(message, got))
				} else {
					referenced++
					require.True(t, proto.Equal(message.MessageId, p.payload.ChatMetadata.GetMessageId()))

					// The full message was left out only because it did not fit.
					full := proto.Clone(p.payload).(*pushpb.Payload)
					full.ChatMetadata.MessageRef = &pushpb.ChatMetadata_Message{Message: message}
					fullSize, err := payloadSize(p.title, p.body, full)
					require.NoError(t, err)
					require.Greater(t, fullSize, maxChatPushBytes)
				}

				muted := proto.Clone(p.mutedPayload).(*pushpb.Payload)
				require.True(t, muted.ChatMetadata.Muted)
				muted.ChatMetadata.Muted = false
				require.True(t, proto.Equal(p.payload, muted))
			}
			require.NotZero(t, carried, "no push carried its message")
			require.NotZero(t, referenced, "no push fell back to the message ID")
		})
	}
}
