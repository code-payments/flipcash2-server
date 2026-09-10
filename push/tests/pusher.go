package tests

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"

	"firebase.google.com/go/v4/messaging"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	chatpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/chat/v1"
	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"
	pushpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/push/v1"

	"github.com/code-payments/flipcash2-server/push"
)

// testFCMClient captures the messages sent for verification. Batches may be
// sent concurrently, so it accumulates under a lock and records each batch's
// size in arrival order.
type testFCMClient struct {
	mu           sync.Mutex
	sentMessages []*messaging.Message
	batchSizes   []int

	// failBatch, when set, makes the batch containing that token fail with
	// failErr, standing in for a transport-level failure of one SendEach call.
	failBatch string
	failErr   error
}

func (c *testFCMClient) SendEach(_ context.Context, messages []*messaging.Message) (*messaging.BatchResponse, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.batchSizes = append(c.batchSizes, len(messages))
	for _, m := range messages {
		if c.failBatch != "" && m.Token == c.failBatch {
			return nil, c.failErr
		}
	}
	c.sentMessages = append(c.sentMessages, messages...)
	return &messaging.BatchResponse{
		SuccessCount: len(messages),
		Responses:    make([]*messaging.SendResponse, len(messages)),
	}, nil
}

// tokens returns the device tokens the messages were addressed to, in send order.
func (c *testFCMClient) tokens() []string {
	tokens := make([]string, len(c.sentMessages))
	for i, m := range c.sentMessages {
		tokens[i] = m.Token
	}
	return tokens
}

// byToken returns the message addressed to the given device token.
func (c *testFCMClient) byToken(t *testing.T, token string) *messaging.Message {
	for _, m := range c.sentMessages {
		if m.Token == token {
			return m
		}
	}
	require.FailNow(t, "no message sent to token", token)
	return nil
}

func RunPusherTests(t *testing.T, s push.TokenStore, teardown func()) {
	for _, tf := range []func(t *testing.T, s push.TokenStore){
		testFCMPusher_SendBasicPushes,
		testFCMPusher_SendPushesWithSubstitutions,
		testFCMPusher_SendPushesWithChatMetadata,
		testFCMPusher_SendPushesWithBadges,
		testFCMPusher_SendPushesWithBadges_ResolverFailure,
		testFCMPusher_SendPushesWithBadges_NoIOSUsers,
		testFCMPusher_SendPushes_Batched,
		testFCMPusher_SendPushes_BatchFailureIsolated,
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
		Category: pushpb.Payload_BUY_SELL,
		GroupKey: "Jeffy",
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

	require.Len(t, fcmClient.sentMessages, 6)
	expectedTokens := []string{
		"token0_1", "token0_2",
		"token1_1", "token1_2",
		"token2_1", "token2_2",
	}
	require.ElementsMatch(t, expectedTokens, fcmClient.tokens())

	// Every device receives the same notification, with no badge.
	for _, sentMessage := range fcmClient.sentMessages {
		require.NotNil(t, sentMessage.Android)
		require.Len(t, sentMessage.Android.Data, 4)
		require.Equal(t, "title", sentMessage.Android.Data["push_notification_title"])
		require.Equal(t, "body", sentMessage.Android.Data["push_notification_body"])
		require.Equal(t, expectedEncodedCustomPayload, sentMessage.Android.Data["flipcash_payload"])
		require.Equal(t, "https://app.flipcash.com/token/11111111111111111111111111111111", sentMessage.Android.Data["target_url"])

		require.NotNil(t, sentMessage.APNS)
		require.Equal(t, "title", sentMessage.APNS.Payload.Aps.Alert.Title)
		require.Equal(t, "body", sentMessage.APNS.Payload.Aps.Alert.Body)
		require.Equal(t, pushpb.Payload_BUY_SELL.String(), sentMessage.APNS.Payload.Aps.Category)
		require.Equal(t, "Jeffy", sentMessage.APNS.Payload.Aps.ThreadID)
		require.False(t, sentMessage.APNS.Payload.Aps.MutableContent)
		require.Nil(t, sentMessage.APNS.Payload.Aps.Badge)
		require.Len(t, sentMessage.APNS.Payload.Aps.CustomData, 4)
		require.Equal(t, "title", sentMessage.APNS.Payload.Aps.CustomData["push_notification_title"])
		require.Equal(t, "body", sentMessage.APNS.Payload.Aps.CustomData["push_notification_body"])
		require.Equal(t, expectedEncodedCustomPayload, sentMessage.APNS.Payload.Aps.CustomData["flipcash_payload"])
		require.Equal(t, "https://app.flipcash.com/token/11111111111111111111111111111111", sentMessage.APNS.Payload.Aps.CustomData["target_url"])
	}
}

func testFCMPusher_SendPushesWithSubstitutions(t *testing.T, store push.TokenStore) {
	ctx := context.Background()

	user := &commonpb.UserId{Value: []byte("user_subs")}
	installId := &commonpb.AppInstallId{Value: "install_subs"}
	require.NoError(t, store.AddToken(ctx, user, installId, pushpb.TokenType_FCM_APNS, "token_subs"))

	titleSub := &commonpb.Substitution{
		Fallback: "Alice",
		Kind: &commonpb.Substitution_PhoneNumberToContactName{
			PhoneNumberToContactName: &commonpb.PhoneNumber{Value: "+14155551111"},
		},
	}
	bodySub := &commonpb.Substitution{
		Fallback: "Bob",
		Kind: &commonpb.Substitution_PhoneNumberToContactName{
			PhoneNumberToContactName: &commonpb.PhoneNumber{Value: "+14155552222"},
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
				TitleSubstitutions: []*commonpb.Substitution{titleSub},
			},
			wantMutable: true,
		},
		{
			name: "body substitutions only",
			payload: &pushpb.Payload{
				BodySubstitutions: []*commonpb.Substitution{bodySub},
			},
			wantMutable: true,
		},
		{
			name: "title and body substitutions",
			payload: &pushpb.Payload{
				TitleSubstitutions: []*commonpb.Substitution{titleSub},
				BodySubstitutions:  []*commonpb.Substitution{bodySub, bodySub},
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

			require.Len(t, fcmClient.sentMessages, 1)
			sentMessage := fcmClient.sentMessages[0]

			require.NotNil(t, sentMessage.APNS)
			require.Equal(t, tc.wantMutable, sentMessage.APNS.Payload.Aps.MutableContent)
			require.Equal(t, expectedEncoded, sentMessage.APNS.Payload.Aps.CustomData["flipcash_payload"])

			require.NotNil(t, sentMessage.Android)
			require.Equal(t, expectedEncoded, sentMessage.Android.Data["flipcash_payload"])
		})
	}
}

// testFCMPusher_SendPushesWithChatMetadata verifies that the presence of chat
// metadata alone makes the APNs payload mutable, so the client can render it
// (e.g. resolve the sending user) before display — independent of whether the
// payload also carries substitutions.
func testFCMPusher_SendPushesWithChatMetadata(t *testing.T, store push.TokenStore) {
	ctx := context.Background()

	user := &commonpb.UserId{Value: []byte("user_chat_meta")}
	installId := &commonpb.AppInstallId{Value: "install_chat_meta"}
	require.NoError(t, store.AddToken(ctx, user, installId, pushpb.TokenType_FCM_APNS, "token_chat_meta"))

	titleSub := &commonpb.Substitution{
		Fallback: "Alice",
		Kind: &commonpb.Substitution_PhoneNumberToContactName{
			PhoneNumberToContactName: &commonpb.PhoneNumber{Value: "+14155551111"},
		},
	}
	chatMetadata := &pushpb.ChatMetadata{
		SendingUserId: &commonpb.UserId{Value: []byte("sender")},
		Type:          chatpb.ChatType_CONTACT_DM,
	}

	for _, tc := range []struct {
		name        string
		payload     *pushpb.Payload
		wantMutable bool
	}{
		{
			name:        "no chat metadata",
			payload:     &pushpb.Payload{},
			wantMutable: false,
		},
		{
			name: "chat metadata with sending user id",
			payload: &pushpb.Payload{
				ChatMetadata: chatMetadata,
			},
			wantMutable: true,
		},
		{
			// A non-nil ChatMetadata makes the push mutable even when it carries
			// no fields (e.g. a system message with no sending user).
			name: "empty chat metadata",
			payload: &pushpb.Payload{
				ChatMetadata: &pushpb.ChatMetadata{
					Type: chatpb.ChatType_CONTACT_DM,
				},
			},
			wantMutable: true,
		},
		{
			// Chat metadata and substitutions independently force mutability, so
			// the combination is mutable too.
			name: "chat metadata and substitutions",
			payload: &pushpb.Payload{
				TitleSubstitutions: []*commonpb.Substitution{titleSub},
				ChatMetadata:       chatMetadata,
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

			require.Len(t, fcmClient.sentMessages, 1)
			sentMessage := fcmClient.sentMessages[0]

			require.NotNil(t, sentMessage.APNS)
			require.Equal(t, tc.wantMutable, sentMessage.APNS.Payload.Aps.MutableContent)
			require.Equal(t, expectedEncoded, sentMessage.APNS.Payload.Aps.CustomData["flipcash_payload"])

			require.NotNil(t, sentMessage.Android)
			require.Equal(t, expectedEncoded, sentMessage.Android.Data["flipcash_payload"])
		})
	}
}

// testFCMPusher_SendPushesWithBadges verifies that a per-user badge rides on
// the notification itself, and that badges are resolved only for users who own
// an iOS device: each iOS device gets its own user's count, an Android-only
// user is never asked about, and a user the resolver produced no count for
// gets the alert without a badge.
func testFCMPusher_SendPushesWithBadges(t *testing.T, store push.TokenStore) {
	ctx := context.Background()

	fcmClient := &testFCMClient{}
	pusher := push.NewFCMPusher(zap.NewNop(), store, fcmClient)

	alice := &commonpb.UserId{Value: []byte("alice")}
	bob := &commonpb.UserId{Value: []byte("bob")}
	carol := &commonpb.UserId{Value: []byte("carol")}
	dave := &commonpb.UserId{Value: []byte("dave")}
	erin := &commonpb.UserId{Value: []byte("erin")}

	// Alice has both an iOS and an Android device, Bob and Carol are iOS only,
	// Dave is Android only, and Erin has no devices at all.
	require.NoError(t, store.AddToken(ctx, alice, &commonpb.AppInstallId{Value: "ios"}, pushpb.TokenType_FCM_APNS, "alice_apns"))
	require.NoError(t, store.AddToken(ctx, alice, &commonpb.AppInstallId{Value: "android"}, pushpb.TokenType_FCM_ANDROID, "alice_android"))
	require.NoError(t, store.AddToken(ctx, bob, &commonpb.AppInstallId{Value: "ios"}, pushpb.TokenType_FCM_APNS, "bob_apns"))
	require.NoError(t, store.AddToken(ctx, carol, &commonpb.AppInstallId{Value: "ios"}, pushpb.TokenType_FCM_APNS, "carol_apns"))
	require.NoError(t, store.AddToken(ctx, dave, &commonpb.AppInstallId{Value: "android"}, pushpb.TokenType_FCM_ANDROID, "dave_android"))

	var resolvedFor []*commonpb.UserId
	resolve := func(_ context.Context, users []*commonpb.UserId) (push.BadgeCounts, error) {
		resolvedFor = users
		return push.BadgeCounts{
			string(alice.Value): 7,
			string(bob.Value):   2,
			// Carol has no count (e.g. her increment failed).
		}, nil
	}
	require.NoError(t, pusher.SendPushesWithBadges(ctx, "title", "body", &pushpb.Payload{}, resolve, alice, bob, carol, dave, erin))

	// Only users with an iOS device are resolved, in recipient order: Dave is
	// Android only and Erin has no devices, so neither is asked about.
	require.Equal(t, []*commonpb.UserId{alice, bob, carol}, resolvedFor)

	require.ElementsMatch(t, []string{"alice_apns", "alice_android", "bob_apns", "carol_apns", "dave_android"}, fcmClient.tokens())

	// Every device gets the alert: the badge is carried on it, not sent alone.
	for _, sentMessage := range fcmClient.sentMessages {
		require.NotNil(t, sentMessage.Android)
		require.Equal(t, "title", sentMessage.Android.Data["push_notification_title"])
		require.NotNil(t, sentMessage.APNS)
		require.Equal(t, "title", sentMessage.APNS.Payload.Aps.Alert.Title)
		require.Equal(t, "body", sentMessage.APNS.Payload.Aps.Alert.Body)
	}

	// Each iOS device carries its own user's count.
	aliceApns := fcmClient.byToken(t, "alice_apns")
	require.NotNil(t, aliceApns.APNS.Payload.Aps.Badge)
	require.Equal(t, 7, *aliceApns.APNS.Payload.Aps.Badge)

	bobApns := fcmClient.byToken(t, "bob_apns")
	require.NotNil(t, bobApns.APNS.Payload.Aps.Badge)
	require.Equal(t, 2, *bobApns.APNS.Payload.Aps.Badge)

	// No count, no badge.
	require.Nil(t, fcmClient.byToken(t, "carol_apns").APNS.Payload.Aps.Badge)
	require.Nil(t, fcmClient.byToken(t, "dave_android").APNS.Payload.Aps.Badge)
}

// testFCMPusher_SendPushesWithBadges_ResolverFailure verifies that a failing
// badge resolver costs the badges, not the push: the alerts still go out with
// whatever counts were produced, and the error is reported afterwards.
func testFCMPusher_SendPushesWithBadges_ResolverFailure(t *testing.T, store push.TokenStore) {
	ctx := context.Background()

	fcmClient := &testFCMClient{}
	pusher := push.NewFCMPusher(zap.NewNop(), store, fcmClient)

	alice := &commonpb.UserId{Value: []byte("alice")}
	bob := &commonpb.UserId{Value: []byte("bob")}
	require.NoError(t, store.AddToken(ctx, alice, &commonpb.AppInstallId{Value: "ios"}, pushpb.TokenType_FCM_APNS, "alice_apns"))
	require.NoError(t, store.AddToken(ctx, bob, &commonpb.AppInstallId{Value: "ios"}, pushpb.TokenType_FCM_APNS, "bob_apns"))

	resolverErr := errors.New("badge store unavailable")
	resolve := func(_ context.Context, _ []*commonpb.UserId) (push.BadgeCounts, error) {
		// Partial result: Alice's count came through before the failure.
		return push.BadgeCounts{string(alice.Value): 4}, resolverErr
	}
	err := pusher.SendPushesWithBadges(ctx, "title", "body", &pushpb.Payload{}, resolve, alice, bob)
	require.ErrorIs(t, err, resolverErr)

	require.ElementsMatch(t, []string{"alice_apns", "bob_apns"}, fcmClient.tokens())

	aliceApns := fcmClient.byToken(t, "alice_apns")
	require.NotNil(t, aliceApns.APNS.Payload.Aps.Badge)
	require.Equal(t, 4, *aliceApns.APNS.Payload.Aps.Badge)
	require.Nil(t, fcmClient.byToken(t, "bob_apns").APNS.Payload.Aps.Badge)
}

// testFCMPusher_SendPushesWithBadges_NoIOSUsers verifies the resolver is never
// invoked when no recipient can display a badge.
func testFCMPusher_SendPushesWithBadges_NoIOSUsers(t *testing.T, store push.TokenStore) {
	ctx := context.Background()

	fcmClient := &testFCMClient{}
	pusher := push.NewFCMPusher(zap.NewNop(), store, fcmClient)

	user := &commonpb.UserId{Value: []byte("android_only")}
	require.NoError(t, store.AddToken(ctx, user, &commonpb.AppInstallId{Value: "android"}, pushpb.TokenType_FCM_ANDROID, "android_token"))

	resolve := func(_ context.Context, _ []*commonpb.UserId) (push.BadgeCounts, error) {
		require.FailNow(t, "resolver invoked with no iOS recipients")
		return nil, nil
	}
	require.NoError(t, pusher.SendPushesWithBadges(ctx, "title", "body", &pushpb.Payload{}, resolve, user))

	// The alert still goes out.
	require.Equal(t, []string{"android_token"}, fcmClient.tokens())
	require.Equal(t, "title", fcmClient.sentMessages[0].Android.Data["push_notification_title"])
}

// addManyTokens registers count iOS tokens for user, named "<prefix>_<i>".
func addManyTokens(t *testing.T, store push.TokenStore, user *commonpb.UserId, prefix string, count int) {
	ctx := context.Background()
	for i := range count {
		installId := &commonpb.AppInstallId{Value: fmt.Sprintf("%s_install_%d", prefix, i)}
		require.NoError(t, store.AddToken(ctx, user, installId, pushpb.TokenType_FCM_APNS, fmt.Sprintf("%s_%d", prefix, i)))
	}
}

// testFCMPusher_SendPushes_Batched verifies a push addressed to more devices
// than FCM accepts per call is split into batches rather than dropped, with
// every device reached exactly once and each device's badge intact.
func testFCMPusher_SendPushes_Batched(t *testing.T, store push.TokenStore) {
	ctx := context.Background()

	fcmClient := &testFCMClient{}
	pusher := push.NewFCMPusher(zap.NewNop(), store, fcmClient)

	// 1200 devices across three users: two full batches of 500 and one of 200.
	alice := &commonpb.UserId{Value: []byte("alice")}
	bob := &commonpb.UserId{Value: []byte("bob")}
	carol := &commonpb.UserId{Value: []byte("carol")}
	addManyTokens(t, store, alice, "alice", 400)
	addManyTokens(t, store, bob, "bob", 400)
	addManyTokens(t, store, carol, "carol", 400)

	resolve := func(_ context.Context, _ []*commonpb.UserId) (push.BadgeCounts, error) {
		return push.BadgeCounts{
			string(alice.Value): 1,
			string(bob.Value):   2,
			string(carol.Value): 3,
		}, nil
	}
	require.NoError(t, pusher.SendPushesWithBadges(ctx, "title", "body", &pushpb.Payload{}, resolve, alice, bob, carol))

	require.ElementsMatch(t, []int{500, 500, 200}, fcmClient.batchSizes)

	// Every device is reached exactly once.
	seen := make(map[string]int)
	for _, token := range fcmClient.tokens() {
		seen[token]++
	}
	require.Len(t, seen, 1200)
	for token, n := range seen {
		require.Equal(t, 1, n, "token %s sent %d times", token, n)
	}

	// Batching never separates a device from its own user's badge.
	wantBadge := map[string]int{"alice": 1, "bob": 2, "carol": 3}
	for _, sentMessage := range fcmClient.sentMessages {
		prefix, _, ok := strings.Cut(sentMessage.Token, "_")
		require.True(t, ok, "token %s", sentMessage.Token)
		require.NotNil(t, sentMessage.APNS.Payload.Aps.Badge, "token %s", sentMessage.Token)
		require.Equal(t, wantBadge[prefix], *sentMessage.APNS.Payload.Aps.Badge, "token %s", sentMessage.Token)
		require.Equal(t, "title", sentMessage.APNS.Payload.Aps.Alert.Title)
	}
}

// testFCMPusher_SendPushes_BatchFailureIsolated verifies a failing batch costs
// only its own devices: the other batches are still delivered, and the failure
// is reported.
func testFCMPusher_SendPushes_BatchFailureIsolated(t *testing.T, store push.TokenStore) {
	ctx := context.Background()

	user := &commonpb.UserId{Value: []byte("many_devices")}
	addManyTokens(t, store, user, "device", 1200)

	// Fail whichever batch carries one specific device; the batch boundaries
	// depend on the store's return order, so the test only relies on there
	// being three batches and exactly one of them failing.
	batchErr := errors.New("fcm unavailable")
	fcmClient := &testFCMClient{failBatch: "device_7", failErr: batchErr}
	pusher := push.NewFCMPusher(zap.NewNop(), store, fcmClient)

	err := pusher.SendPushes(ctx, "title", "body", &pushpb.Payload{}, user)
	require.ErrorIs(t, err, batchErr)

	require.Len(t, fcmClient.batchSizes, 3)

	// The two surviving batches delivered everything they carried, and nothing
	// from the failed batch leaked through.
	delivered := fcmClient.tokens()
	require.Contains(t, []int{700, 1000}, len(delivered))
	require.NotContains(t, delivered, "device_7")
}
