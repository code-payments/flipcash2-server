package tests

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	accountpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/account/v1"

	"github.com/code-payments/flipcash2-server/account"
	"github.com/code-payments/flipcash2-server/auth"
	"github.com/code-payments/flipcash2-server/model"
	"github.com/code-payments/flipcash2-server/protoutil"
)

func RunAuthorizerTests(t *testing.T, s account.Store, teardown func()) {
	for _, tf := range []func(t *testing.T, s account.Store){
		testAuthorizer,
	} {
		tf(t, s)
		teardown()
	}
}

func testAuthorizer(t *testing.T, store account.Store) {
	log := zaptest.NewLogger(t)
	authn := auth.NewKeyPairAuthenticator(log)

	authz := account.NewAuthorizer(log, store, authn)

	userID := model.MustGenerateUserID()
	signer := model.MustGenerateKeyPair()

	t.Run("UserNotFound", func(t *testing.T) {
		req := &accountpb.GetUserFlagsRequest{
			UserId: model.MustGenerateUserID(),
			Auth:   nil,
		}
		require.NoError(t, signer.Auth(req, &req.Auth))

		_, err := authz.Authorize(context.Background(), req, &req.Auth)
		require.Equal(t, codes.PermissionDenied, status.Code(err))
		require.NotNil(t, req.Auth)
	})

	t.Run("Authorized", func(t *testing.T) {
		_, err := store.Bind(context.Background(), userID, signer.Proto())
		require.NoError(t, err)

		req := &accountpb.GetUserFlagsRequest{
			UserId: userID,
			Auth:   nil,
		}
		require.NoError(t, signer.Auth(req, &req.Auth))

		actual, err := authz.Authorize(context.Background(), req, &req.Auth)
		require.NoError(t, err)
		require.NoError(t, protoutil.ProtoEqualError(userID, actual))
	})

	t.Run("Unauthenticated - Missing", func(t *testing.T) {
		req := &accountpb.GetUserFlagsRequest{
			UserId: userID,
			Auth:   nil,
		}

		_, err := authz.Authorize(context.Background(), req, &req.Auth)
		require.Equal(t, codes.InvalidArgument, status.Code(err))
	})

	t.Run("Unauthenticated - Invalid", func(t *testing.T) {
		req := &accountpb.GetUserFlagsRequest{
			UserId: userID,
			Auth:   nil,
		}

		// Sign with a different key pair, then present the bound user's public key
		// so the signature is well-formed but does not verify against the claimed
		// key. Don't use an all-zero public key here: it decodes to a small-order
		// Ed25519 point, which ed25519.Verify accepts for an all-zero signature
		// depending on the message hash, making the test flaky.
		require.NoError(t, model.MustGenerateKeyPair().Auth(req, &req.Auth))
		req.Auth.GetKeyPair().PubKey = signer.Proto()

		_, err := authz.Authorize(context.Background(), req, &req.Auth)
		require.Equal(t, codes.Unauthenticated, status.Code(err))
		require.NotNil(t, req.Auth)
	})
}
