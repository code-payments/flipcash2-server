package kyc

import (
	"context"
	"errors"
	"time"

	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"
	thirdpartypb "github.com/code-payments/flipcash2-protobuf-api/generated/go/thirdparty/v1"
)

var (
	// ErrNotFound indicates that no KYC record exists for the given key.
	ErrNotFound = errors.New("kyc record not found")

	// ErrExists indicates that a KYC record already exists for the given
	// (user, partner).
	ErrExists = errors.New("kyc record already exists")
)

// Record maps a user to their customer with a verification partner. It is the
// only KYC state at rest: verification status, requirements, and all identity
// data live with the partner and are fetched live. Records are immutable once
// created.
type Record struct {
	UserID     *commonpb.UserId
	Partner    thirdpartypb.Partner
	CustomerID string
	CreatedAt  time.Time
}

// Clone returns a deep copy of the record.
func (r *Record) Clone() *Record {
	return &Record{
		UserID:     &commonpb.UserId{Value: append([]byte(nil), r.UserID.Value...)},
		Partner:    r.Partner,
		CustomerID: r.CustomerID,
		CreatedAt:  r.CreatedAt,
	}
}

// Store persists user-to-partner customer mappings.
type Store interface {
	// Get returns the record for (userID, partner), or ErrNotFound.
	Get(ctx context.Context, userID *commonpb.UserId, partner thirdpartypb.Partner) (*Record, error)

	// GetByCustomerID returns the record for a partner's customer ID, or
	// ErrNotFound. This is the reverse lookup used when consuming partner
	// events, which reference customers by their partner-side ID.
	GetByCustomerID(ctx context.Context, partner thirdpartypb.Partner, customerID string) (*Record, error)

	// Create persists a new record. It returns ErrExists if a record already
	// exists for (userID, partner).
	Create(ctx context.Context, record *Record) error
}
