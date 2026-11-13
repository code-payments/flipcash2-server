package email

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"
	"google.golang.org/grpc"

	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"
	emailpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/email/v1"

	"github.com/code-payments/flipcash2-server/account"
	account_memory "github.com/code-payments/flipcash2-server/account/memory"
	"github.com/code-payments/flipcash2-server/auth"
	"github.com/code-payments/flipcash2-server/model"
	profile_memory "github.com/code-payments/flipcash2-server/profile/memory"
	"github.com/code-payments/flipcash2-server/testutil"
)

const testCode = "123456"

// codeVerifier accepts testCode for any address, and sends nothing.
type codeVerifier struct{}

func (codeVerifier) SendCode(context.Context, string, string) (string, error) { return "id", nil }
func (codeVerifier) Check(_ context.Context, _, code string) error {
	if code != testCode {
		return ErrInvalidVerificationCode
	}
	return nil
}
func (codeVerifier) Cancel(context.Context, string) error                       { return nil }
func (codeVerifier) IsVerificationActive(context.Context, string) (bool, error) { return true, nil }
func (codeVerifier) IsValidEmailAddress(context.Context, string) (bool, error)  { return true, nil }

func TestServer_LinkUnverified(t *testing.T) {
	ctx := context.Background()
	log := zaptest.NewLogger(t)

	accounts := account_memory.NewInMemory()
	profiles := profile_memory.NewInMemory()
	authz := account.NewAuthorizer(log, accounts, auth.NewKeyPairAuthenticator(log))

	serv := NewServer(log, authz, accounts, profiles, codeVerifier{})
	cc := testutil.RunGRPCServer(t, log, testutil.WithService(func(s *grpc.Server) {
		emailpb.RegisterEmailVerificationServer(s, serv)
	}))
	client := emailpb.NewEmailVerificationClient(cc)

	newUser := func(t *testing.T, registered bool) (model.KeyPair, *commonpb.UserId) {
		keyPair := model.MustGenerateKeyPair()
		userID := model.MustGenerateUserID()
		_, err := accounts.Bind(ctx, userID, keyPair.Proto())
		require.NoError(t, err)
		require.NoError(t, accounts.SetRegistrationFlag(ctx, userID, registered))
		return keyPair, userID
	}

	linkUnverified := func(t *testing.T, keyPair model.KeyPair, email string) emailpb.LinkUnverifiedResponse_Result {
		req := &emailpb.LinkUnverifiedRequest{EmailAddress: &commonpb.EmailAddress{Value: email}}
		require.NoError(t, keyPair.Auth(req, &req.Auth))
		resp, err := client.LinkUnverified(ctx, req)
		require.NoError(t, err)
		return resp.Result
	}

	verify := func(t *testing.T, keyPair model.KeyPair, email string) {
		req := &emailpb.CheckVerificationCodeRequest{
			EmailAddress: &commonpb.EmailAddress{Value: email},
			Code:         &emailpb.VerificationCode{Value: testCode},
		}
		require.NoError(t, keyPair.Auth(req, &req.Auth))
		resp, err := client.CheckVerificationCode(ctx, req)
		require.NoError(t, err)
		require.Equal(t, emailpb.CheckVerificationCodeResponse_OK, resp.Result)
	}

	requireLinked := func(t *testing.T, userID *commonpb.UserId, email string, verified bool) {
		p, err := profiles.GetFullProfile(ctx, userID, true)
		require.NoError(t, err)
		require.Equal(t, email, p.GetEmailAddress().GetValue())
		require.Equal(t, verified, p.IsEmailAddressVerified)
	}

	t.Run("an unregistered user is denied", func(t *testing.T) {
		keyPair, userID := newUser(t, false)
		require.Equal(t, emailpb.LinkUnverifiedResponse_DENIED, linkUnverified(t, keyPair, "someone@gmail.com"))
		_, err := profiles.GetFullProfile(ctx, userID, true)
		require.Error(t, err)
	})

	t.Run("links unverified, and verification upgrades it", func(t *testing.T) {
		keyPair, userID := newUser(t, true)

		require.Equal(t, emailpb.LinkUnverifiedResponse_OK, linkUnverified(t, keyPair, "someone@gmail.com"))
		requireLinked(t, userID, "someone@gmail.com", false)

		verify(t, keyPair, "someone@gmail.com")
		requireLinked(t, userID, "someone@gmail.com", true)

		// The verified address relinked unverified stays verified.
		require.Equal(t, emailpb.LinkUnverifiedResponse_OK, linkUnverified(t, keyPair, "someone@gmail.com"))
		requireLinked(t, userID, "someone@gmail.com", true)

		// Another address replaces it, unverified.
		require.Equal(t, emailpb.LinkUnverifiedResponse_OK, linkUnverified(t, keyPair, "someone.else@gmail.com"))
		requireLinked(t, userID, "someone.else@gmail.com", false)
	})

	t.Run("an address another user holds is linked to both", func(t *testing.T) {
		holder, holderID := newUser(t, true)
		verify(t, holder, "shared@gmail.com")

		keyPair, userID := newUser(t, true)
		require.Equal(t, emailpb.LinkUnverifiedResponse_OK, linkUnverified(t, keyPair, "shared@gmail.com"))
		requireLinked(t, userID, "shared@gmail.com", false)
		requireLinked(t, holderID, "shared@gmail.com", true)
	})
}
