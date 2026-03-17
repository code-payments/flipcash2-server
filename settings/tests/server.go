package tests

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"
	settingspb "github.com/code-payments/flipcash2-protobuf-api/generated/go/settings/v1"

	"github.com/code-payments/flipcash2-server/account"
	"github.com/code-payments/flipcash2-server/auth"
	"github.com/code-payments/flipcash2-server/model"
	"github.com/code-payments/flipcash2-server/settings"
	"github.com/code-payments/flipcash2-server/testutil"
)

func RunServerTests(t *testing.T, accounts account.Store, store settings.Store, teardown func()) {
	for _, tf := range []func(t *testing.T, accounts account.Store, store settings.Store){
		testServer,
	} {
		tf(t, accounts, store)
		teardown()
	}
}

func testServer(t *testing.T, accounts account.Store, store settings.Store) {
	ctx := context.Background()
	log := zaptest.NewLogger(t)

	authz := account.NewAuthorizer(log, accounts, auth.NewKeyPairAuthenticator(log))

	serv := settings.NewServer(log, authz, store)
	cc := testutil.RunGRPCServer(t, log, testutil.WithService(func(s *grpc.Server) {
		settingspb.RegisterSettingsServer(s, serv)
	}))

	client := settingspb.NewSettingsClient(cc)

	t.Run("No User", func(t *testing.T) {
		keyPair := model.MustGenerateKeyPair()
		req := &settingspb.UpdateSettingsRequest{
			Locale: &commonpb.Locale{Value: "fr"},
		}
		require.NoError(t, keyPair.Auth(req, &req.Auth))
		_, err := client.UpdateSettings(ctx, req)
		require.Equal(t, codes.PermissionDenied, status.Code(err))
	})

	t.Run("Registered user", func(t *testing.T) {
		userID := model.MustGenerateUserID()
		keyPair := model.MustGenerateKeyPair()

		_, err := accounts.Bind(ctx, userID, keyPair.Proto())
		require.NoError(t, err)
		require.NoError(t, accounts.SetRegistrationFlag(ctx, userID, true))

		t.Run("Update locale only", func(t *testing.T) {
			req := &settingspb.UpdateSettingsRequest{
				Locale: &commonpb.Locale{Value: "fr"},
			}
			require.NoError(t, keyPair.Auth(req, &req.Auth))
			resp, err := client.UpdateSettings(ctx, req)
			require.NoError(t, err)
			require.Equal(t, settingspb.UpdateSettingsResponse_OK, resp.Result)

			s, err := store.GetSettings(ctx, userID)
			require.NoError(t, err)
			require.Equal(t, "fr", s.Locale.Value)
		})

		t.Run("Update region only", func(t *testing.T) {
			req := &settingspb.UpdateSettingsRequest{
				Region: &commonpb.Region{Value: "eur"},
			}
			require.NoError(t, keyPair.Auth(req, &req.Auth))
			resp, err := client.UpdateSettings(ctx, req)
			require.NoError(t, err)
			require.Equal(t, settingspb.UpdateSettingsResponse_OK, resp.Result)

			s, err := store.GetSettings(ctx, userID)
			require.NoError(t, err)
			require.Equal(t, "eur", s.Region.Value)
		})

		t.Run("Update both", func(t *testing.T) {
			req := &settingspb.UpdateSettingsRequest{
				Locale: &commonpb.Locale{Value: "es"},
				Region: &commonpb.Region{Value: "gbp"},
			}
			require.NoError(t, keyPair.Auth(req, &req.Auth))
			resp, err := client.UpdateSettings(ctx, req)
			require.NoError(t, err)
			require.Equal(t, settingspb.UpdateSettingsResponse_OK, resp.Result)

			s, err := store.GetSettings(ctx, userID)
			require.NoError(t, err)
			require.Equal(t, "es", s.Locale.Value)
			require.Equal(t, "gbp", s.Region.Value)
		})

		t.Run("No fields to update", func(t *testing.T) {
			req := &settingspb.UpdateSettingsRequest{}
			require.NoError(t, keyPair.Auth(req, &req.Auth))
			resp, err := client.UpdateSettings(ctx, req)
			require.NoError(t, err)
			require.Equal(t, settingspb.UpdateSettingsResponse_OK, resp.Result)
		})
	})
}
