package contact

import (
	"context"
	"errors"

	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"
)

// MaxContactsPerUser is the maximum number of contacts a single user may have
// stored on the server.
const MaxContactsPerUser = 10_000

// ChecksumSize is the length, in bytes, of a contact list checksum.
const ChecksumSize = 32

// ErrNotFound indicates that the user has no stored contact list row.
var ErrNotFound = errors.New("contact list not found")

// ErrChecksumDrift indicates that the stored checksum did not match
// old_checksum (and was not new_checksum either, so a retry can't be
// inferred). The client must call FullUpload to reconcile.
var ErrChecksumDrift = errors.New("contact list checksum drift")

// ErrTooManyContacts indicates that applying the request would exceed
// MaxContactsPerUser.
var ErrTooManyContacts = errors.New("too many contacts")

// ZeroChecksum is the all-zero 32-byte checksum, used as the default when
// a user has no stored contact list yet.
func ZeroChecksum() *commonpb.Hash {
	return &commonpb.Hash{Value: make([]byte, ChecksumSize)}
}

// Store persists per-user contact lists.
//
// Contacts are identified by an HMAC-SHA256 hash of the normalized E.164 phone
// number. The raw phone number is never stored. The checksum is provided by
// the client (a 32-byte XOR-of-SHA256 over the same set of phone numbers) and
// is persisted as the authoritative state.
type Store interface {
	// GetChecksum returns the user's stored checksum. Returns ErrNotFound when
	// the user has no stored contact list row.
	GetChecksum(ctx context.Context, userID *commonpb.UserId) (*commonpb.Hash, error)

	// GetHashes returns the HMAC-SHA256 hashes of every phone number in the
	// user's stored contact list. Returns ErrNotFound when the user has no
	// stored contact list row. Returns an empty slice (no error) when the
	// row exists but the contact list is empty.
	GetHashes(ctx context.Context, userID *commonpb.UserId) ([]*commonpb.Hash, error)

	// GetUserIdsByPhoneHash returns every user whose contact list contains
	// phoneNumberHash. Returns an empty slice (no error) when no users have
	// the hash. Order is unspecified.
	GetUserIdsByPhoneHash(ctx context.Context, phoneNumberHash *commonpb.Hash) ([]*commonpb.UserId, error)

	// IsContact reports whether phoneNumberHash appears in userID's contact
	// list. Returns false (no error) when the user has no stored contact list
	// row or the hash is not present.
	IsContact(ctx context.Context, userID *commonpb.UserId, phoneNumberHash *commonpb.Hash) (bool, error)

	// ApplyDelta applies adds and removes to the user's contact set under
	// compare-and-swap on the checksum:
	//
	//   - If the stored checksum equals oldChecksum, the delta is applied and
	//     the stored checksum becomes newChecksum.
	//   - If the stored checksum equals newChecksum, the call is treated as a
	//     no-op retry and nil is returned.
	//   - Otherwise ErrChecksumDrift is returned.
	//
	// addHashes and removeHashes are HMAC-SHA256 hashes of the phone numbers
	// being added/removed. Adds use INSERT-IF-NOT-EXISTS semantics; removes use
	// DELETE-IF-EXISTS semantics.
	//
	// If applying the delta would leave the user with more than
	// MaxContactsPerUser contacts, ErrTooManyContacts is returned and the delta
	// is not applied.
	ApplyDelta(
		ctx context.Context,
		userID *commonpb.UserId,
		addHashes []*commonpb.Hash,
		removeHashes []*commonpb.Hash,
		oldChecksum *commonpb.Hash,
		newChecksum *commonpb.Hash,
	) error

	// Replace replaces the user's contact set entirely with the provided
	// HMAC-SHA256 hashes and stores expectedChecksum as the new checksum.
	//
	// If len(hashes) > MaxContactsPerUser, ErrTooManyContacts is returned and
	// no changes are made.
	Replace(
		ctx context.Context,
		userID *commonpb.UserId,
		hashes []*commonpb.Hash,
		expectedChecksum *commonpb.Hash,
	) error
}
