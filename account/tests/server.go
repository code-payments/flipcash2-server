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

	ocpdata "github.com/code-payments/ocp-server/ocp/data"
	ocptestutil "github.com/code-payments/ocp-server/testutil"

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

	codeStores := ocpdata.NewTestDataProvider()

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

	ocptestutil.SetupRandomSubsidizer(t, codeStores)

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
}
