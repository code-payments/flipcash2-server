package tests

import (
	"context"
	"encoding/base64"
	"fmt"
	"testing"

	"firebase.google.com/go/v4/messaging"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"
	phonepb "github.com/code-payments/flipcash2-protobuf-api/generated/go/phone/v1"
	pushpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/push/v1"

	"github.com/code-payments/flipcash2-server/push"
)

// testFCMClient captures the messages sent for verification
type testFCMClient struct {
	sentMessage *messaging.MulticastMessage
}

func (c *testFCMClient) SendEachForMulticast(_ context.Context, message *messaging.MulticastMessage) (*messaging.BatchResponse, error) {
	c.sentMessage = message
	return &messaging.BatchResponse{
		SuccessCount: len(message.Tokens),
		Responses:    make([]*messaging.SendResponse, len(message.Tokens)),
	}, nil
}

func RunPusherTests(t *testing.T, s push.TokenStore, teardown func()) {
	for _, tf := range []func(t *testing.T, s push.TokenStore){
		testFCMPusher_SendBasicPushes,
		testFCMPusher_SendPushesWithSubstitutions,
	} {
		tf(t, s)
		teardown()
	}
}

func testFCMPusher_SendBasicPushes(t *testing.T, store push.TokenStore) {
	ctx := context.Background()

	fcmClient := &testFCMClient{}
	pusher := push.NewFCMPusher(zap.NewNop(), store, fcmClient)

	users := make([]*commonpb.UserId, 5)
	for i := 0; i < 5; i++ {
		users[i] = &commonpb.UserId{Value: []byte(fmt.Sprintf("user%d", i))}

		installId := &commonpb.AppInstallId{Value: fmt.Sprintf("install%d_1", i)}
		err := store.AddToken(ctx, users[i], installId, pushpb.TokenType_FCM_APNS, fmt.Sprintf("token%d_1", i))
		require.NoError(t, err)

		installId = &commonpb.AppInstallId{Value: fmt.Sprintf("install%d_2", i)}
		err = store.AddToken(ctx, users[i], installId, pushpb.TokenType_FCM_APNS, fmt.Sprintf("token%d_2", i))
		require.NoError(t, err)
	}

	targetUsers := users[:3]

	customPayload := &pushpb.Payload{
		Navigation: &pushpb.Navigation{
			Type: &pushpb.Navigation_CurrencyInfo{
				CurrencyInfo: &commonpb.PublicKey{Value: make([]byte, 32)},
			},
		},
	}

	marshalledCustomPayload, err := proto.Marshal(customPayload)
	require.NoError(t, err)
	expectedEncodedCustomPayload := base64.StdEncoding.EncodeToString(marshalledCustomPayload)

	require.NoError(t, pusher.SendPushes(ctx, "title", "body", customPayload, targetUsers...))

	require.NotNil(t, fcmClient.sentMessage)

	require.Len(t, fcmClient.sentMessage.Tokens, 6)
	expectedTokens := []string{
		"token0_1", "token0_2",
		"token1_1", "token1_2",
		"token2_1", "token2_2",
	}
	require.ElementsMatch(t, expectedTokens, fcmClient.sentMessage.Tokens)

	require.NotNil(t, fcmClient.sentMessage.Android)
	require.Len(t, fcmClient.sentMessage.Android.Data, 4)
	require.Equal(t, "title", fcmClient.sentMessage.Android.Data["push_notification_title"])
	require.Equal(t, "body", fcmClient.sentMessage.Android.Data["push_notification_body"])
	require.Equal(t, expectedEncodedCustomPayload, fcmClient.sentMessage.Android.Data["flipcash_payload"])
	require.Equal(t, "https://app.flipcash.com/token/11111111111111111111111111111111", fcmClient.sentMessage.Android.Data["target_url"])

	require.NotNil(t, fcmClient.sentMessage.APNS)
	require.Equal(t, "title", fcmClient.sentMessage.APNS.Payload.Aps.Alert.Title)
	require.Equal(t, "body", fcmClient.sentMessage.APNS.Payload.Aps.Alert.Body)
	require.False(t, fcmClient.sentMessage.APNS.Payload.Aps.MutableContent)
	require.Len(t, fcmClient.sentMessage.APNS.Payload.Aps.CustomData, 4)
	require.Equal(t, "title", fcmClient.sentMessage.APNS.Payload.Aps.CustomData["push_notification_title"])
	require.Equal(t, "body", fcmClient.sentMessage.APNS.Payload.Aps.CustomData["push_notification_body"])
	require.Equal(t, expectedEncodedCustomPayload, fcmClient.sentMessage.APNS.Payload.Aps.CustomData["flipcash_payload"])
	require.Equal(t, "https://app.flipcash.com/token/11111111111111111111111111111111", fcmClient.sentMessage.APNS.Payload.Aps.CustomData["target_url"])
}

func testFCMPusher_SendPushesWithSubstitutions(t *testing.T, store push.TokenStore) {
	ctx := context.Background()

	user := &commonpb.UserId{Value: []byte("user_subs")}
	installId := &commonpb.AppInstallId{Value: "install_subs"}
	require.NoError(t, store.AddToken(ctx, user, installId, pushpb.TokenType_FCM_APNS, "token_subs"))

	titleSub := &pushpb.Substitution{
		Fallback: "Alice",
		Kind: &pushpb.Substitution_Contact{
			Contact: &phonepb.PhoneNumber{Value: "+14155551111"},
		},
	}
	bodySub := &pushpb.Substitution{
		Fallback: "Bob",
		Kind: &pushpb.Substitution_Contact{
			Contact: &phonepb.PhoneNumber{Value: "+14155552222"},
		},
	}

	for _, tc := range []struct {
		name        string
		payload     *pushpb.Payload
		wantMutable bool
	}{
		{
			name:        "no substitutions",
			payload:     &pushpb.Payload{},
			wantMutable: false,
		},
		{
			name: "title substitutions only",
			payload: &pushpb.Payload{
				TitleSubstitutions: []*pushpb.Substitution{titleSub},
			},
			wantMutable: true,
		},
		{
			name: "body substitutions only",
			payload: &pushpb.Payload{
				BodySubstitutions: []*pushpb.Substitution{bodySub},
			},
			wantMutable: true,
		},
		{
			name: "title and body substitutions",
			payload: &pushpb.Payload{
				TitleSubstitutions: []*pushpb.Substitution{titleSub},
				BodySubstitutions:  []*pushpb.Substitution{bodySub, bodySub},
			},
			wantMutable: true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fcmClient := &testFCMClient{}
			pusher := push.NewFCMPusher(zap.NewNop(), store, fcmClient)

			require.NoError(t, pusher.SendPushes(ctx, "title", "body", tc.payload, user))

			marshalled, err := proto.Marshal(tc.payload)
			require.NoError(t, err)
			expectedEncoded := base64.StdEncoding.EncodeToString(marshalled)

			require.NotNil(t, fcmClient.sentMessage)

			require.NotNil(t, fcmClient.sentMessage.APNS)
			require.Equal(t, tc.wantMutable, fcmClient.sentMessage.APNS.Payload.Aps.MutableContent)
			require.Equal(t, expectedEncoded, fcmClient.sentMessage.APNS.Payload.Aps.CustomData["flipcash_payload"])

			require.NotNil(t, fcmClient.sentMessage.Android)
			require.Equal(t, expectedEncoded, fcmClient.sentMessage.Android.Data["flipcash_payload"])
		})
	}
}
