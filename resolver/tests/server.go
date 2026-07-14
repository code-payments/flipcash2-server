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
	phonepb "github.com/code-payments/flipcash2-protobuf-api/generated/go/phone/v1"
	resolverpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/resolver/v1"

	"github.com/code-payments/flipcash2-server/account"
	"github.com/code-payments/flipcash2-server/auth"
	"github.com/code-payments/flipcash2-server/model"
	"github.com/code-payments/flipcash2-server/profile"
	"github.com/code-payments/flipcash2-server/resolver"
	"github.com/code-payments/flipcash2-server/testutil"
)

func RunServerTests(t *testing.T, accounts account.Store, profiles profile.Store) {
	for _, tf := range []func(t *testing.T, accounts account.Store, profiles profile.Store){
		testServer_Resolve_ByUserID_OK,
		testServer_Resolve_ByUserID_NotFound,
		testServer_Resolve_ByPhoneNumber_OK,
		testServer_Resolve_ByPhoneNumber_NotFound,
		testServer_Resolve_ByPhoneNumber_NotEnabledForPayment,
		testServer_Resolve_Denied_Unregistered,
		testServer_Resolve_Unauthorized,
	} {
		tf(t, accounts, profiles)
	}
}

type serverFixture struct {
	t        *testing.T
	client   resolverpb.ResolverClient
	keys     model.KeyPair
	userID   *commonpb.UserId
	accounts account.Store
	profiles profile.Store
}

func newServerFixture(t *testing.T, accounts account.Store, profiles profile.Store) *serverFixture {
	ctx := context.Background()
	log := zaptest.NewLogger(t)

	authz := account.NewAuthorizer(log, accounts, auth.NewKeyPairAuthenticator(log))
	server := resolver.NewServer(log, authz, accounts, profiles)

	userID := model.MustGenerateUserID()
	keys := model.MustGenerateKeyPair()

	_, err := accounts.Bind(ctx, userID, keys.Proto())
	require.NoError(t, err)
	require.NoError(t, accounts.SetRegistrationFlag(ctx, userID, true))

	cc := testutil.RunGRPCServer(t, log,
		testutil.WithService(func(s *grpc.Server) {
			resolverpb.RegisterResolverServer(s, server)
		}),
	)
	return &serverFixture{
		t:        t,
		client:   resolverpb.NewResolverClient(cc),
		keys:     keys,
		userID:   userID,
		accounts: accounts,
		profiles: profiles,
	}
}

func testServer_Resolve_ByUserID_OK(t *testing.T, accounts account.Store, profiles profile.Store) {
	ctx := context.Background()
	f := newServerFixture(t, accounts, profiles)

	// Seed a separate Flipcash user, without any phone number linked for payment.
	targetID := model.MustGenerateUserID()
	targetKeys := model.MustGenerateKeyPair()
	_, err := accounts.Bind(ctx, targetID, targetKeys.Proto())
	require.NoError(t, err)
	require.NoError(t, accounts.SetRegistrationFlag(ctx, targetID, true))

	req := &resolverpb.ResolveRequest{
		Identifier: &resolverpb.Identifier{
			Kind: &resolverpb.Identifier_UserId{
				UserId: targetID,
			},
		},
	}
	require.NoError(t, f.keys.Auth(req, &req.Auth))

	resp, err := f.client.Resolve(ctx, req)
	require.NoError(t, err)
	require.Equal(t, resolverpb.ResolveResponse_OK, resp.Result)
	require.NotNil(t, resp.Resolution)
	require.Equal(t, targetKeys.Proto().Value, resp.Resolution.GetAddress().Value)
}

func testServer_Resolve_ByUserID_NotFound(t *testing.T, accounts account.Store, profiles profile.Store) {
	ctx := context.Background()
	f := newServerFixture(t, accounts, profiles)

	// No user exists with this ID, so there's no payment address to resolve to.
	req := &resolverpb.ResolveRequest{
		Identifier: &resolverpb.Identifier{
			Kind: &resolverpb.Identifier_UserId{
				UserId: model.MustGenerateUserID(),
			},
		},
	}
	require.NoError(t, f.keys.Auth(req, &req.Auth))

	resp, err := f.client.Resolve(ctx, req)
	require.NoError(t, err)
	require.Equal(t, resolverpb.ResolveResponse_NOT_FOUND, resp.Result)
	require.Nil(t, resp.Resolution)
}

func testServer_Resolve_ByPhoneNumber_OK(t *testing.T, accounts account.Store, profiles profile.Store) {
	ctx := context.Background()
	f := newServerFixture(t, accounts, profiles)

	// Seed a separate Flipcash user with a linked phone number.
	targetID := model.MustGenerateUserID()
	targetKeys := model.MustGenerateKeyPair()
	_, err := accounts.Bind(ctx, targetID, targetKeys.Proto())
	require.NoError(t, err)
	require.NoError(t, accounts.SetRegistrationFlag(ctx, targetID, true))
	require.NoError(t, profiles.LinkPhoneNumber(ctx, targetID, "+12223334444", &commonpb.Hash{Value: []byte("phone-hash-ok")}))
	flipped, err := profiles.LinkPhoneNumberForPayment(ctx, targetID, "+12223334444")
	require.NoError(t, err)
	require.True(t, flipped)

	req := &resolverpb.ResolveRequest{
		Identifier: &resolverpb.Identifier{
			Kind: &resolverpb.Identifier_Phone{
				Phone: &phonepb.PhoneNumber{Value: "+12223334444"},
			},
		},
	}
	require.NoError(t, f.keys.Auth(req, &req.Auth))

	resp, err := f.client.Resolve(ctx, req)
	require.NoError(t, err)
	require.Equal(t, resolverpb.ResolveResponse_OK, resp.Result)
	require.NotNil(t, resp.Resolution)
	require.Equal(t, targetKeys.Proto().Value, resp.Resolution.GetAddress().Value)
}

func testServer_Resolve_ByPhoneNumber_NotFound(t *testing.T, accounts account.Store, profiles profile.Store) {
	ctx := context.Background()
	f := newServerFixture(t, accounts, profiles)

	// No user is linked to this number — pick one no other subtest uses.
	req := &resolverpb.ResolveRequest{
		Identifier: &resolverpb.Identifier{
			Kind: &resolverpb.Identifier_Phone{
				Phone: &phonepb.PhoneNumber{Value: "+15550000404"},
			},
		},
	}
	require.NoError(t, f.keys.Auth(req, &req.Auth))

	resp, err := f.client.Resolve(ctx, req)
	require.NoError(t, err)
	require.Equal(t, resolverpb.ResolveResponse_NOT_FOUND, resp.Result)
	require.Nil(t, resp.Resolution)
}

func testServer_Resolve_ByPhoneNumber_NotEnabledForPayment(t *testing.T, accounts account.Store, profiles profile.Store) {
	ctx := context.Background()
	f := newServerFixture(t, accounts, profiles)

	// Seed a user with a linked phone number, but without enabling it for payment.
	targetID := model.MustGenerateUserID()
	targetKeys := model.MustGenerateKeyPair()
	_, err := accounts.Bind(ctx, targetID, targetKeys.Proto())
	require.NoError(t, err)
	require.NoError(t, accounts.SetRegistrationFlag(ctx, targetID, true))
	require.NoError(t, profiles.LinkPhoneNumber(ctx, targetID, "+12223335555", &commonpb.Hash{Value: []byte("phone-hash-no-pay")}))

	req := &resolverpb.ResolveRequest{
		Identifier: &resolverpb.Identifier{
			Kind: &resolverpb.Identifier_Phone{
				Phone: &phonepb.PhoneNumber{Value: "+12223335555"},
			},
		},
	}
	require.NoError(t, f.keys.Auth(req, &req.Auth))

	resp, err := f.client.Resolve(ctx, req)
	require.NoError(t, err)
	require.Equal(t, resolverpb.ResolveResponse_NOT_FOUND, resp.Result)
	require.Nil(t, resp.Resolution)
}

func testServer_Resolve_Denied_Unregistered(t *testing.T, accounts account.Store, profiles profile.Store) {
	ctx := context.Background()
	log := zaptest.NewLogger(t)

	authz := account.NewAuthorizer(log, accounts, auth.NewKeyPairAuthenticator(log))
	server := resolver.NewServer(log, authz, accounts, profiles)

	// Bind a caller, but do not mark them registered.
	userID := model.MustGenerateUserID()
	keys := model.MustGenerateKeyPair()
	_, err := accounts.Bind(ctx, userID, keys.Proto())
	require.NoError(t, err)

	cc := testutil.RunGRPCServer(t, log,
		testutil.WithService(func(s *grpc.Server) {
			resolverpb.RegisterResolverServer(s, server)
		}),
	)
	client := resolverpb.NewResolverClient(cc)

	req := &resolverpb.ResolveRequest{
		Identifier: &resolverpb.Identifier{
			Kind: &resolverpb.Identifier_Phone{
				Phone: &phonepb.PhoneNumber{Value: "+15550000503"},
			},
		},
	}
	require.NoError(t, keys.Auth(req, &req.Auth))

	resp, err := client.Resolve(ctx, req)
	require.NoError(t, err)
	require.Equal(t, resolverpb.ResolveResponse_DENIED, resp.Result)
	require.Nil(t, resp.Resolution)
}

func testServer_Resolve_Unauthorized(t *testing.T, accounts account.Store, profiles profile.Store) {
	ctx := context.Background()
	f := newServerFixture(t, accounts, profiles)

	// Signed by a key that isn't bound to the request's user.
	other := model.MustGenerateKeyPair()
	req := &resolverpb.ResolveRequest{
		Identifier: &resolverpb.Identifier{
			Kind: &resolverpb.Identifier_Phone{
				Phone: &phonepb.PhoneNumber{Value: "+15550000401"},
			},
		},
	}
	require.NoError(t, other.Auth(req, &req.Auth))

	_, err := f.client.Resolve(ctx, req)
	require.Error(t, err)
	require.Equal(t, codes.PermissionDenied, status.Code(err))
}
