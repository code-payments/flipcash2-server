package push

import (
	"bytes"
	"context"
	"encoding/json"
	"testing"
	"time"

	"firebase.google.com/go/v4/messaging"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"google.golang.org/protobuf/types/known/timestamppb"

	chatpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/chat/v1"
	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"
	messagingpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/messaging/v1"
	pushpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/push/v1"
)

// recordingFCMClient keeps every message it is asked to send.
type recordingFCMClient struct {
	messages []*messaging.Message
}

func (c *recordingFCMClient) SendEach(_ context.Context, messages []*messaging.Message) (*messaging.BatchResponse, error) {
	c.messages = append(c.messages, messages...)
	return &messaging.BatchResponse{
		SuccessCount: len(messages),
		Responses:    make([]*messaging.SendResponse, len(messages)),
	}, nil
}

// fixedTokens answers GetTokensBatch with its tokens, whoever is asked for.
// A send that succeeds calls no other TokenStore method.
type fixedTokens struct {
	TokenStore
	tokens []Token
}

func (s fixedTokens) GetTokensBatch(context.Context, ...*commonpb.UserId) ([]Token, error) {
	return s.tokens, nil
}

// testChatMessage is a valid message with the given content.
func testChatMessage(content *messagingpb.Content) *messagingpb.Message {
	return &messagingpb.Message{
		MessageId:     &messagingpb.MessageId{Value: 123456},
		SenderId:      &commonpb.UserId{Value: bytes.Repeat([]byte{2}, 16)},
		Content:       []*messagingpb.Content{content},
		Ts:            timestamppb.New(time.Unix(1790000000, 0)),
		EventSequence: 123460,
	}
}

// TestPayloadSize_MatchesSentPush: payloadSize measures exactly what the
// pusher sends, on the larger platform, when the badge is measuredBadge — so
// the chat push's decision to carry the message is made against the bytes
// the providers see.
func TestPayloadSize_MatchesSentPush(t *testing.T) {
	ctx := context.Background()
	user := &commonpb.UserId{Value: bytes.Repeat([]byte{1}, 16)}
	client := &recordingFCMClient{}
	pusher := NewFCMPusher(zap.NewNop(), fixedTokens{tokens: []Token{
		{UserID: user, Type: pushpb.TokenType_FCM_APNS, Token: "ios"},
	}}, client)
	resolveBadges := func(context.Context, []*commonpb.UserId) (BadgeCounts, error) {
		return BadgeCounts{string(user.Value): measuredBadge}, nil
	}

	chatID := &commonpb.ChatId{Value: bytes.Repeat([]byte{3}, 32)}
	for name, payload := range map[string]*pushpb.Payload{
		"empty": {},
		"chat message": {
			Category:   pushpb.Payload_CHAT,
			GroupKey:   "group",
			Navigation: &pushpb.Navigation{Type: &pushpb.Navigation_ChatId{ChatId: chatID}},
			ChatMetadata: &pushpb.ChatMetadata{
				Type: chatpb.ChatType_TIP_DM,
				MessageRef: &pushpb.ChatMetadata_Message{Message: testChatMessage(&messagingpb.Content{
					Type: &messagingpb.Content_Text{Text: &messagingpb.TextContent{Text: "hello <world> & \"friends\" 😀"}},
				})},
			},
		},
		"substitutions": {
			TitleSubstitutions: []*commonpb.Substitution{{
				Fallback: "+15555555555",
				Kind: &commonpb.Substitution_PhoneNumberToContactName{
					PhoneNumberToContactName: &commonpb.PhoneNumber{Value: "+15555555555"},
				},
			}},
			Navigation: &pushpb.Navigation{Type: &pushpb.Navigation_CurrencyInfo{
				CurrencyInfo: &commonpb.PublicKey{Value: bytes.Repeat([]byte{4}, 32)},
			}},
		},
	} {
		t.Run(name, func(t *testing.T) {
			client.messages = nil
			title, body := "Title {0}", "Body with <markup> & \"quotes\" 😀"
			require.NoError(t, pusher.SendPushesWithBadges(ctx, title, body, payload, resolveBadges, user))
			require.Len(t, client.messages, 1)
			sent := client.messages[0]

			var android int
			for k, v := range sent.Android.Data {
				android += len(k) + len(v)
			}
			apns, err := json.Marshal(sent.APNS.Payload)
			require.NoError(t, err)

			got, err := payloadSize(title, body, payload)
			require.NoError(t, err)
			require.Equal(t, max(android, len(apns)), got)
		})
	}
}
