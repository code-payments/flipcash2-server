package profile

import (
	"context"
	"errors"

	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"
	phonepb "github.com/code-payments/flipcash2-protobuf-api/generated/go/phone/v1"
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

	// LinkPhoneNumber links the phone number and its precomputed hash to a user, provided
	// they exist. Any other user previously holding the same phone number has both fields
	// cleared.
	LinkPhoneNumber(ctx context.Context, id *commonpb.UserId, phoneNumber string, phoneNumberHash *commonpb.Hash) error

	// UnlinkPhoneNumber removes the link for the phone number
	UnlinkPhoneNumber(ctx context.Context, userID *commonpb.UserId, phoneNumber string) error

	// GetPhonesByHashes returns the phone numbers for users whose stored
	// phoneNumberHash matches any of the provided hashes. Order is unspecified.
	GetPhonesByHashes(ctx context.Context, hashes []*commonpb.Hash) ([]*phonepb.PhoneNumber, error)

	// GetUserIdByPhoneNumber returns the UserId currently linked to the given
	// phone number. Returns ErrNotFound when no user holds the number.
	GetUserIdByPhoneNumber(ctx context.Context, phoneNumber string) (*commonpb.UserId, error)

	// LinkEmailAddress links the email address to a user, provided they exist. Any other
	// user previously holding the same email address has it cleared.
	LinkEmailAddress(ctx context.Context, id *commonpb.UserId, emailAddress string) error

	// UnlinkPhoneNumber removes the link for the email address
	UnlinkEmailAddress(ctx context.Context, userID *commonpb.UserId, emailAddress string) error

	// LinkXAccount links a X account to a user ID
	LinkXAccount(ctx context.Context, userID *commonpb.UserId, xProfile *profilepb.XProfile, accessToken string) error

	// UnlinkXAccount removes the link to the X account
	UnlinkXAccount(ctx context.Context, userID *commonpb.UserId, xUserID string) error

	// GetXProfile gets a user's X profile if it has been linked
	GetXProfile(ctx context.Context, userID *commonpb.UserId) (*profilepb.XProfile, error)
}
