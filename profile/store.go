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

// PhoneForPayment is a payment-enabled phone number together with the user that
// owns it.
type PhoneForPayment struct {
	PhoneNumber *phonepb.PhoneNumber
	UserID      *commonpb.UserId
}

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

	// LinkPhoneNumberForPayment marks the user's linked phone number as enabled for
	// payment, provided the given phone number is currently linked to the user. It
	// reports whether the flag transitioned from false to true (false if it was
	// already enabled).
	//
	// ErrNotFound is returned when the phone number is not associated with the user.
	LinkPhoneNumberForPayment(ctx context.Context, userID *commonpb.UserId, phoneNumber string) (bool, error)

	// IsPhoneNumberLinkedForPayment reports whether the given phone number is
	// currently linked to the user and enabled for payment.
	IsPhoneNumberLinkedForPayment(ctx context.Context, userID *commonpb.UserId, phoneNumber string) (bool, error)

	// GetPhonesByHashes returns the phone numbers for users whose stored
	// phoneNumberHash matches any of the provided hashes. Order is unspecified.
	GetPhonesByHashes(ctx context.Context, hashes []*commonpb.Hash) ([]*phonepb.PhoneNumber, error)

	// GetPhonesByHashesForPayment returns the payment-enabled phone numbers,
	// each paired with the user that owns it, for users whose stored
	// phoneNumberHash matches any of the provided hashes and who have enabled
	// their phone number for payment. Order is unspecified.
	GetPhonesByHashesForPayment(ctx context.Context, hashes []*commonpb.Hash) ([]*PhoneForPayment, error)

	// GetPhoneNumbersForPayment returns, for each of the given users that has a
	// phone number enabled for payment, that phone number keyed by
	// string(userID.Value). Users without a payment-enabled phone number are
	// absent from the map. It resolves the whole set in a single lookup.
	GetPhoneNumbersForPayment(ctx context.Context, userIDs []*commonpb.UserId) (map[string]*phonepb.PhoneNumber, error)

	// GetUserIdByPhoneNumber returns the UserId currently linked to the given
	// phone number. Returns ErrNotFound when no user holds the number.
	GetUserIdByPhoneNumber(ctx context.Context, phoneNumber string) (*commonpb.UserId, error)

	// GetUserIdByPhoneNumberForPayment returns the UserId currently linked to the
	// given phone number, but only when that user has enabled the phone number for
	// payment. Returns ErrNotFound when no user holds the number or the number is
	// not enabled for payment.
	GetUserIdByPhoneNumberForPayment(ctx context.Context, phoneNumber string) (*commonpb.UserId, error)

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
