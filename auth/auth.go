package auth

import (
	"context"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"
	codecommonpb "github.com/code-payments/ocp-protobuf-api/generated/go/common/v1"

	"github.com/code-payments/flipcash2-server/model"
	auth2 "github.com/code-payments/ocp-server/pkg/code/auth"
	ocpcommon "github.com/code-payments/ocp-server/pkg/code/common"
)

// Authorizer authorizes an action for a UserId with the given auth.
//
// If the auth is authorized, it is also authenticated. Authorization is more expensive
// than authentication as lookups must be performed.
type Authorizer interface {
	Authorize(ctx context.Context, m proto.Message, authField **commonpb.Auth) (*commonpb.UserId, error)
}

type StaticAuthorizer struct {
	auth Authenticator

	keyPairs map[string]string
}

func NewStaticAuthorizer() *StaticAuthorizer {
	return &StaticAuthorizer{
		auth:     NewKeyPairAuthenticator(),
		keyPairs: make(map[string]string),
	}
}

func (a *StaticAuthorizer) Authorize(ctx context.Context, m proto.Message, authField **commonpb.Auth) (*commonpb.UserId, error) {
	authMessage := *authField
	*authField = nil

	defer func() {
		*authField = authMessage
	}()

	if err := a.auth.Verify(ctx, m, authMessage); err != nil {
		return nil, err
	}

	userID, ok := a.keyPairs[string(authMessage.GetKeyPair().GetPubKey().GetValue())]
	if !ok {
		return nil, status.Error(codes.PermissionDenied, "permission denied")
	}

	return &commonpb.UserId{Value: []byte(userID)}, nil
}

func (a *StaticAuthorizer) Add(userID *commonpb.UserId, pair model.KeyPair) {
	a.keyPairs[string(pair.Public())] = string(userID.GetValue())
}

// Authenticator authenticates a message with the provided auth.
//
// It is not usually sufficient to rely purely on authentication for permissions,
// as a lookup must be completed. However, if a message is not authentic, we can
// short circuit authorization.
//
// In general, users should use an Authorizer, and Authorizer's should use an
// Authenticator.
type Authenticator interface {
	Verify(ctx context.Context, m proto.Message, auth *commonpb.Auth) error
}

// NewKeyPairAuthenticator authenticates pub key based auth.
func NewKeyPairAuthenticator() Authenticator {
	return &authenticator{
		auth: auth2.NewRPCSignatureVerifier(nil),
	}
}

type authenticator struct {
	auth *auth2.RPCSignatureVerifier
}

func (v *authenticator) Verify(ctx context.Context, m proto.Message, auth *commonpb.Auth) error {
	keyPair := auth.GetKeyPair()
	if keyPair == nil {
		return status.Error(codes.InvalidArgument, "missing keypair")
	}

	if err := keyPair.Validate(); err != nil {
		return status.Error(codes.InvalidArgument, "invalid auth")
	}

	account, err := ocpcommon.NewAccountFromPublicKeyBytes(keyPair.PubKey.Value)
	if err != nil {
		return status.Error(codes.InvalidArgument, "invalid pubkey")
	}

	return v.auth.Authenticate(ctx, account, m, &codecommonpb.Signature{Value: keyPair.Signature.Value})
}
