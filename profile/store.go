package profile

import (
	"context"
	"errors"

	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"
	profilepb "github.com/code-payments/flipcash2-protobuf-api/generated/go/profile/v1"
)

var ErrNotFound = errors.New("not found")
var ErrInvalidDisplayName = errors.New("invalid display name")
var ErrExistingSocialLink = errors.New("existing social link")

type Store interface {
	// GetProfile returns the user profile for a user, or ErrNotFound.
	GetProfile(ctx context.Context, id *commonpb.UserId, includePrivateProfile bool) (*profilepb.UserProfile, error)

	// SetDisplayName sets the display name for a user, provided they exist.
	//
	// ErrInvalidDisplayName is returned if there is an issue with the display name.
	SetDisplayName(ctx context.Context, id *commonpb.UserId, displayName string) error

	// SetPhoneNumber sets the phone number for a user, provided they exist.
	SetPhoneNumber(ctx context.Context, id *commonpb.UserId, phoneNumber string) error

	// UnlinkPhoneNumber removes the link for the phone number
	UnlinkPhoneNumber(ctx context.Context, userID *commonpb.UserId, phoneNumber string) error

	// SetEmailAddress sets the email address for a user, provided they exist.
	SetEmailAddress(ctx context.Context, id *commonpb.UserId, emailAddress string) error

	// UnlinkPhoneNumber removes the link for the email address
	UnlinkEmailAddress(ctx context.Context, userID *commonpb.UserId, emailAddress string) error

	// LinkXAccount links a X account to a user ID
	LinkXAccount(ctx context.Context, userID *commonpb.UserId, xProfile *profilepb.XProfile, accessToken string) error

	// UnlinkXAccount removes the link to the X account
	UnlinkXAccount(ctx context.Context, userID *commonpb.UserId, xUserID string) error

	// GetXProfile gets a user's X profile if it has been linked
	GetXProfile(ctx context.Context, userID *commonpb.UserId) (*profilepb.XProfile, error)
}
