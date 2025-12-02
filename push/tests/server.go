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
	} {
		tf(t, s)
		teardown()
	}
}

func testServer_AddToken(t *testing.T, store push.TokenStore) {
	ctx := context.Background()
	log := zaptest.NewLogger(t)

	authz := auth.NewStaticAuthorizer(log)
	server := push.NewServer(log, authz, store)

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
	server := push.NewServer(log, authz, store)

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
