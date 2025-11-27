package account

import (
	"context"
	"errors"

	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"
)

var (
	ErrNotFound       = errors.New("not found")
	ErrManyPublicKeys = errors.New("detected multiple keys for user")
)

type Store interface {
	// Bind binds a public key to a UserId, or returns the previously bound UserId.
	Bind(ctx context.Context, userID *commonpb.UserId, pubKey *commonpb.PublicKey) (*commonpb.UserId, error)

	// GetUserId returns the UserId associated with a public key.
	//
	/// ErrNotFound is returned if no binding exists.
	GetUserId(ctx context.Context, pubKey *commonpb.PublicKey) (*commonpb.UserId, error)

	// GetPubKeys returns the set of public keys associated with an account.
	GetPubKeys(ctx context.Context, userID *commonpb.UserId) ([]*commonpb.PublicKey, error)

	// IsAuthorized returns whether or not a pubKey is authorized to perform actions on behalf of the user.
	IsAuthorized(ctx context.Context, userID *commonpb.UserId, pubKey *commonpb.PublicKey) (bool, error)

	// IsStaff returns whether or not a userID is a staff user
	IsStaff(ctx context.Context, userID *commonpb.UserId) (bool, error)

	// IsRegistered returns whether or not a userID is a registered account
	IsRegistered(ctx context.Context, userID *commonpb.UserId) (bool, error)

	// SetRegistrationFlag sets wether a userID is a registered account
	SetRegistrationFlag(ctx context.Context, userID *commonpb.UserId, isRegistered bool) error
}
