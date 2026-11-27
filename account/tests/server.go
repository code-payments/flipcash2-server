package tests

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/timestamppb"

	accountpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/account/v1"
	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"

	ocp_data "github.com/code-payments/ocp-server/ocp/data"
	ocp_testutil "github.com/code-payments/ocp-server/testutil"

	"github.com/code-payments/flipcash2-server/account"
	"github.com/code-payments/flipcash2-server/auth"
	"github.com/code-payments/flipcash2-server/model"
	"github.com/code-payments/flipcash2-server/protoutil"
	"github.com/code-payments/flipcash2-server/testutil"
)

func RunServerTests(t *testing.T, s account.Store, teardown func()) {
	for _, tf := range []func(t *testing.T, s account.Store){
		testServer,
	} {
		tf(t, s)
		teardown()
	}
}

func testServer(t *testing.T, store account.Store) {
	log := zaptest.NewLogger(t)

	ocpData := ocp_data.NewTestDataProvider()

	server := account.NewServer(
		log,
		store,
		auth.NewKeyPairAuthenticator(log),
	)

	cc := testutil.RunGRPCServer(t, log, testutil.WithService(func(s *grpc.Server) {
		accountpb.RegisterAccountServer(s, server)
	}))

	ctx := context.Background()
	client := accountpb.NewAccountClient(cc)

	ocp_testutil.SetupRandomSubsidizer(t, ocpData)

	var keys []model.KeyPair
	var userId *commonpb.UserId

	t.Run("Register", func(t *testing.T) {
		keys = append(keys, model.MustGenerateKeyPair())
		req := &accountpb.RegisterRequest{
			PublicKey: keys[0].Proto(),
		}
		require.NoError(t, keys[0].Sign(req, &req.Signature))

		for range 2 {
			resp, err := client.Register(ctx, req)
			require.NoError(t, err)
			require.Equal(t, accountpb.RegisterResponse_OK, resp.Result)
			require.NotNil(t, resp.UserId)

			if userId == nil {
				userId = resp.UserId
			} else {
				require.NoError(t, protoutil.ProtoEqualError(userId, resp.UserId))
			}

			isRegistered, err := store.IsRegistered(ctx, userId)
			require.NoError(t, err)
			require.True(t, isRegistered)
		}
	})

	t.Run("Login", func(t *testing.T) {
		for _, key := range keys {
			req := &accountpb.LoginRequest{
				Timestamp: timestamppb.Now(),
			}
			require.NoError(t, key.Auth(req, &req.Auth))

			resp, err := client.Login(ctx, req)
			require.NoError(t, err)
			require.Equal(t, accountpb.LoginResponse_OK, resp.Result)
			require.NotNil(t, resp.UserId)
			require.NoError(t, protoutil.ProtoEqualError(userId, resp.UserId))
		}
	})

	t.Run("GetUserFlags", func(t *testing.T) {
		newRequest := func() *accountpb.GetUserFlagsRequest {
			return &accountpb.GetUserFlagsRequest{
				UserId:      userId,
				Platform:    commonpb.Platform_APPLE,
				CountryCode: &commonpb.CountryCode{Value: "us"},
			}
		}

		t.Run("Signed by the user's key", func(t *testing.T) {
			req := newRequest()
			require.NoError(t, keys[0].Auth(req, &req.Auth))

			resp, err := client.GetUserFlags(ctx, req)
			require.NoError(t, err)
			require.Equal(t, accountpb.GetUserFlagsResponse_OK, resp.Result)
			require.True(t, resp.UserFlags.IsRegisteredAccount)
			require.False(t, resp.UserFlags.IsStaff)
		})

		t.Run("User's public key without its signature", func(t *testing.T) {
			// Anyone can learn a user's public key, so naming it must not be
			// enough: sign with another key and present the user's.
			req := newRequest()
			require.NoError(t, model.MustGenerateKeyPair().Auth(req, &req.Auth))
			req.Auth.GetKeyPair().PubKey = keys[0].Proto()

			resp, err := client.GetUserFlags(ctx, req)
			require.NoError(t, err)
			require.Equal(t, accountpb.GetUserFlagsResponse_DENIED, resp.Result)
			require.Nil(t, resp.UserFlags)
		})

		t.Run("Request changed after signing", func(t *testing.T) {
			req := newRequest()
			require.NoError(t, keys[0].Auth(req, &req.Auth))
			req.Platform = commonpb.Platform_GOOGLE

			resp, err := client.GetUserFlags(ctx, req)
			require.NoError(t, err)
			require.Equal(t, accountpb.GetUserFlagsResponse_DENIED, resp.Result)
			require.Nil(t, resp.UserFlags)
		})

		t.Run("Signed by a key that is not the user's", func(t *testing.T) {
			req := newRequest()
			require.NoError(t, model.MustGenerateKeyPair().Auth(req, &req.Auth))

			resp, err := client.GetUserFlags(ctx, req)
			require.NoError(t, err)
			require.Equal(t, accountpb.GetUserFlagsResponse_DENIED, resp.Result)
			require.Nil(t, resp.UserFlags)
		})

		t.Run("Unknown user", func(t *testing.T) {
			req := newRequest()
			req.UserId = model.MustGenerateUserID()
			require.NoError(t, keys[0].Auth(req, &req.Auth))

			resp, err := client.GetUserFlags(ctx, req)
			require.NoError(t, err)
			require.Equal(t, accountpb.GetUserFlagsResponse_DENIED, resp.Result)
			require.Nil(t, resp.UserFlags)
		})
	})
}
