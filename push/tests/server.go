package tests

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"
	pushpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/push/v1"

	"github.com/code-payments/flipcash2-server/auth"
	"github.com/code-payments/flipcash2-server/model"
	"github.com/code-payments/flipcash2-server/push"
)

func RunServerTests(t *testing.T, s push.TokenStore, teardown func()) {
	for _, tf := range []func(t *testing.T, s push.TokenStore){
		testServer_AddToken,
		testServer_DeleteTokens,
		testServer_SendTestPush,
	} {
		tf(t, s)
		teardown()
	}
}

func testServer_AddToken(t *testing.T, store push.TokenStore) {
	ctx := context.Background()
	log := zaptest.NewLogger(t)

	authz := auth.NewStaticAuthorizer(log)
	server := push.NewServer(log, authz, store, push.NewNoOpPusher())

	userID := &commonpb.UserId{Value: []byte("test-user")}
	keyPair := model.MustGenerateKeyPair()
	authz.Add(userID, keyPair)

	tests := []struct {
		name      string
		req       *pushpb.AddTokenRequest
		setup     func() *pushpb.AddTokenRequest
		wantError codes.Code
	}{
		{
			name: "success",
			setup: func() *pushpb.AddTokenRequest {
				req := &pushpb.AddTokenRequest{
					AppInstall: &commonpb.AppInstallId{Value: "test-install"},
					TokenType:  pushpb.TokenType_FCM_ANDROID,
					PushToken:  "test-token",
				}
				require.NoError(t, keyPair.Auth(req, &req.Auth))
				return req
			},
			wantError: codes.OK,
		},
		{
			name: "unauthorized",
			setup: func() *pushpb.AddTokenRequest {
				req := &pushpb.AddTokenRequest{
					AppInstall: &commonpb.AppInstallId{Value: "test-install"},
					TokenType:  pushpb.TokenType_FCM_ANDROID,
					PushToken:  "test-token",
				}
				require.NoError(t, model.MustGenerateKeyPair().Auth(req, &req.Auth))
				return req
			},
			wantError: codes.PermissionDenied,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := tt.setup()

			_, err := server.AddToken(ctx, req)
			if tt.wantError != codes.OK {
				require.Error(t, err)
				require.Equal(t, tt.wantError, status.Code(err))
				return
			}

			require.NoError(t, err)

			// Verify token was stored
			tokens, err := store.GetTokens(ctx, userID)
			require.NoError(t, err)
			require.Len(t, tokens, 1)
			require.Equal(t, req.TokenType, tokens[0].Type)
			require.Equal(t, req.PushToken, tokens[0].Token)
			require.Equal(t, req.AppInstall.Value, tokens[0].AppInstallID)
		})
	}
}

func testServer_DeleteTokens(t *testing.T, store push.TokenStore) {
	ctx := context.Background()
	log := zaptest.NewLogger(t)

	authz := auth.NewStaticAuthorizer(log)
	server := push.NewServer(log, authz, store, push.NewNoOpPusher())

	userID := &commonpb.UserId{Value: []byte("test-user")}
	keyPair := model.MustGenerateKeyPair()
	authz.Add(userID, keyPair)

	// Add a token that we can delete
	appInstall1 := &commonpb.AppInstallId{Value: "test-install1"}
	appInstall2 := &commonpb.AppInstallId{Value: "test-install2"}
	require.NoError(t, store.AddToken(ctx, userID, appInstall1, pushpb.TokenType_FCM_ANDROID, "test-token1"))
	require.NoError(t, store.AddToken(ctx, userID, appInstall2, pushpb.TokenType_FCM_ANDROID, "test-token2"))

	tests := []struct {
		name      string
		setup     func() *pushpb.DeleteTokensRequest
		wantError codes.Code
	}{
		{
			name: "app install not found",
			setup: func() *pushpb.DeleteTokensRequest {
				req := &pushpb.DeleteTokensRequest{
					AppInstall: &commonpb.AppInstallId{Value: "non-existant-install"},
				}
				require.NoError(t, keyPair.Auth(req, &req.Auth))
				return req
			},
			wantError: codes.OK,
		},
		{
			name: "unauthorized",
			setup: func() *pushpb.DeleteTokensRequest {
				req := &pushpb.DeleteTokensRequest{
					AppInstall: &commonpb.AppInstallId{Value: "non-existant-install"},
				}
				require.NoError(t, model.MustGenerateKeyPair().Auth(req, &req.Auth))
				return req
			},
			wantError: codes.PermissionDenied,
		},
		{
			name: "success",
			setup: func() *pushpb.DeleteTokensRequest {
				req := &pushpb.DeleteTokensRequest{
					AppInstall: appInstall1,
				}
				require.NoError(t, keyPair.Auth(req, &req.Auth))
				return req
			},
			wantError: codes.OK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := tt.setup()

			_, err := server.DeleteTokens(ctx, req)
			if tt.wantError != codes.OK {
				require.Error(t, err)
				require.Equal(t, tt.wantError, status.Code(err))
				return
			}

			require.NoError(t, err)

			// Verify token state
			tokens, err := store.GetTokens(ctx, userID)
			require.NoError(t, err)
			if req.AppInstall.Value == appInstall1.Value {
				require.Len(t, tokens, 1)
				require.Equal(t, tokens[0].AppInstallID, appInstall2.Value)
			} else {
				require.Len(t, tokens, 2)
			}
		})
	}
}

func testServer_SendTestPush(t *testing.T, store push.TokenStore) {
	ctx := context.Background()
	log := zaptest.NewLogger(t)

	fcmClient := &testFCMClient{}
	authz := auth.NewStaticAuthorizer(log)
	server := push.NewServer(log, authz, store, push.NewFCMPusher(log, store, fcmClient))

	userID := &commonpb.UserId{Value: []byte("test-user")}
	keyPair := model.MustGenerateKeyPair()
	authz.Add(userID, keyPair)

	otherUserID := &commonpb.UserId{Value: []byte("other-user")}

	require.NoError(t, store.AddToken(ctx, userID, &commonpb.AppInstallId{Value: "install-ios"}, pushpb.TokenType_FCM_APNS, "token-ios"))
	require.NoError(t, store.AddToken(ctx, userID, &commonpb.AppInstallId{Value: "install-android"}, pushpb.TokenType_FCM_ANDROID, "token-android"))
	require.NoError(t, store.AddToken(ctx, otherUserID, &commonpb.AppInstallId{Value: "install-other"}, pushpb.TokenType_FCM_APNS, "token-other"))

	t.Run("unauthorized", func(t *testing.T) {
		req := &pushpb.SendTestPushRequest{}
		require.NoError(t, model.MustGenerateKeyPair().Auth(req, &req.Auth))

		_, err := server.SendTestPush(ctx, req)
		require.Equal(t, codes.PermissionDenied, status.Code(err))
		require.Empty(t, fcmClient.sentMessages)
	})

	t.Run("success", func(t *testing.T) {
		req := &pushpb.SendTestPushRequest{}
		require.NoError(t, keyPair.Auth(req, &req.Auth))

		resp, err := server.SendTestPush(ctx, req)
		require.NoError(t, err)
		require.Equal(t, pushpb.SendTestPushResponse_OK, resp.Result)

		// Every one of the caller's devices, and no one else's.
		require.ElementsMatch(t, []string{"token-ios", "token-android"}, fcmClient.tokens())
		for _, m := range fcmClient.sentMessages {
			require.Equal(t, "Test Notification", m.Android.Data["push_notification_title"])
			require.Equal(t, "If you can see this, notifications are working.", m.Android.Data["push_notification_body"])
			require.Empty(t, m.Android.Data["target_url"])
			require.Equal(t, "Test Notification", m.APNS.Payload.Aps.Alert.Title)
			require.Equal(t, "If you can see this, notifications are working.", m.APNS.Payload.Aps.Alert.Body)
			require.Nil(t, m.APNS.Payload.Aps.Badge)
		}
	})

	t.Run("no tokens", func(t *testing.T) {
		noTokensUserID := &commonpb.UserId{Value: []byte("no-tokens-user")}
		noTokensKeyPair := model.MustGenerateKeyPair()
		authz.Add(noTokensUserID, noTokensKeyPair)

		req := &pushpb.SendTestPushRequest{}
		require.NoError(t, noTokensKeyPair.Auth(req, &req.Auth))

		sent := len(fcmClient.sentMessages)
		resp, err := server.SendTestPush(ctx, req)
		require.NoError(t, err)
		require.Equal(t, pushpb.SendTestPushResponse_OK, resp.Result)
		require.Len(t, fcmClient.sentMessages, sent)
	})
}
