package profile

import (
	"context"
	"errors"
	"time"

	blobpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/blob/v1"
	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"
	profilepb "github.com/code-payments/flipcash2-protobuf-api/generated/go/profile/v1"
)

var ErrNotFound = errors.New("not found")
var ErrInvalidDisplayName = errors.New("invalid display name")
var ErrInvalidUsername = errors.New("invalid username")
var ErrUsernameTaken = errors.New("username taken")
var ErrExistingSocialLink = errors.New("existing social link")

// PhoneForPayment is a payment-enabled phone number together with the user that
// owns it.
type PhoneForPayment struct {
	PhoneNumber *commonpb.PhoneNumber
	UserID      *commonpb.UserId
	JoinedAt    time.Time
}

// DefaultUsernameResult is what SetDisplayNameWithDefaultUsername did about the
// user's handle.
type DefaultUsernameResult struct {
	// Username is the handle assigned, or "" when none was.
	Username string

	// NoneAvailable is set when the user was eligible for a handle — they held
	// none — but no number yielded one both free and usable, or every one tried
	// was lost to a concurrent assignment, so they were left without. It is the one
	// outcome that leaves an eligible user with no handle. The display name is set
	// regardless, and the user stays eligible, so a later display name tries again.
	NoneAvailable bool
}

type Store interface {
	// GetProfile returns the user profile for a user, or ErrNotFound when the store
	// does not know the user.
	GetProfile(ctx context.Context, id *commonpb.UserId, includePrivateProfile bool) (*profilepb.UserProfile, error)

	// SetDisplayName sets the display name for a user, provided they exist.
	//
	// ErrInvalidDisplayName is returned if there is an issue with the display name.
	SetDisplayName(ctx context.Context, id *commonpb.UserId, displayName string) error

	// SetDisplayNameWithDefaultUsername sets the display name exactly as
	// SetDisplayName does and, when the user holds no handle, assigns them a
	// default handle for usernameBase: the
	// lowest-numbered one nobody holds, or under contention one of the next few or
	// a random one (see AssignDefaultUsername). Both are written in one
	// transaction, so a handle is never assigned without the display name that
	// earned it, and no reader sees that display name before its handle.
	//
	// The result carries the handle assigned, if any. None is when the user
	// already held a handle, or when no number yields a handle
	// both free and usable (NoneAvailable), in which case the display name is
	// still set. A handle taken concurrently by another user is retried against
	// another number, and running out of retries is NoneAvailable too: contention
	// never fails the display name.
	SetDisplayNameWithDefaultUsername(ctx context.Context, id *commonpb.UserId, displayName, usernameBase string) (DefaultUsernameResult, error)

	// SetUsername claims username as the user's handle, replacing any handle they
	// already hold — which, having no holder any more, is immediately claimable by
	// anyone else.
	//
	// username must already be in canonical form, so callers normalize what a user
	// typed before claiming it; ErrInvalidUsername is returned otherwise. A handle
	// has one holder at a time: ErrUsernameTaken is returned when another user
	// holds it, while re-claiming the handle the user already holds is a no-op.
	SetUsername(ctx context.Context, id *commonpb.UserId, username string) error

	// GetUserIdByUsername returns the user currently holding the given handle,
	// matched case-insensitively since handles are only ever held in canonical
	// form. Returns ErrNotFound when nobody holds it.
	GetUserIdByUsername(ctx context.Context, username string) (*commonpb.UserId, error)

	// GetPublicProfiles returns, for each of the given users the store knows, the
	// public part of that user's profile keyed by string(userID.Value): the
	// fields any viewer may see, carrying no private ones. Unknown users are
	// absent from the map. It resolves the whole set in a single lookup.
	//
	// Display name is empty, and username, profile picture and minimum DM chat
	// initialization fee nil, for a user who has set none of them, while the join
	// timestamp and Flipcard customization are always set — so a present entry
	// means "this user exists", not "this user filled in a profile".
	//
	// The returned picture carries only the blob holding its ORIGINAL rendition;
	// resolving that blob's metadata is left to the caller.
	GetPublicProfiles(ctx context.Context, userIDs []*commonpb.UserId) (map[string]*profilepb.UserProfile, error)

	// SetProfilePicture sets the user's profile picture to the blob holding its
	// ORIGINAL rendition, replacing any picture already set.
	SetProfilePicture(ctx context.Context, id *commonpb.UserId, blobID *blobpb.BlobId) error

	// SetFlipcardColor sets the colour of the user's Flipcard, provided they
	// exist, replacing any colour already picked. colorHex is stored as given, so
	// callers normalize it first.
	SetFlipcardColor(ctx context.Context, id *commonpb.UserId, colorHex string) error

	// SetMinDmChatInitFee sets the minimum fee another user must pay to
	// initialize a DM chat with the user, provided they exist, replacing any fee
	// already set. fee is stored as given, so callers validate it first.
	SetMinDmChatInitFee(ctx context.Context, id *commonpb.UserId, fee *commonpb.FiatPaymentAmount) error

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
	GetPhonesByHashes(ctx context.Context, hashes []*commonpb.Hash) ([]*commonpb.PhoneNumber, error)

	// GetPhonesByHashesForPayment returns the payment-enabled phone numbers,
	// each paired with the user that owns it, for users whose stored
	// phoneNumberHash matches any of the provided hashes and who have enabled
	// their phone number for payment. Order is unspecified.
	GetPhonesByHashesForPayment(ctx context.Context, hashes []*commonpb.Hash) ([]*PhoneForPayment, error)

	// GetPhoneNumbersForPayment returns, for each of the given users that has a
	// phone number enabled for payment, that phone number keyed by
	// string(userID.Value). Users without a payment-enabled phone number are
	// absent from the map. It resolves the whole set in a single lookup.
	GetPhoneNumbersForPayment(ctx context.Context, userIDs []*commonpb.UserId) (map[string]*commonpb.PhoneNumber, error)

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
