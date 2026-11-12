package coinbase

import (
	"context"
	"errors"
	"fmt"
	"time"

	"google.golang.org/protobuf/proto"

	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"

	"github.com/code-payments/flipcash2-server/cdp/customers"
	"github.com/code-payments/flipcash2-server/model"
)

var (
	// ErrNotFound is returned when no record matches: the user has no Coinbase
	// customer, or no user has the customer named.
	ErrNotFound = errors.New("coinbase customer record not found")

	// ErrInvalidRecord is returned, wrapped with what is wrong, by Validate and
	// so by Store.PutCustomer for a record it cannot store. Nothing is written.
	ErrInvalidRecord = errors.New("invalid coinbase customer record")
)

// CustomerRecord is the link between a Flipcash user and their Coinbase
// customer.
type CustomerRecord struct {
	// UserID is the Flipcash user, exactly model.UserIDSize bytes.
	UserID *commonpb.UserId

	// CustomerID is Coinbase's ID for the user's customer,
	// "customer_<36 lowercase hex digits and dashes>" (customers.IsCustomerID).
	CustomerID string

	// CreatedAt is when the link was recorded. Stores keep it to the
	// nanosecond, so a record reads back equal to the one written.
	CreatedAt time.Time
}

// Validate reports whether r is a record a store may write: a user ID of
// model.UserIDSize bytes, a Coinbase customer ID and a nonzero creation time.
// It refuses and never normalizes; every refusal wraps ErrInvalidRecord.
func (r *CustomerRecord) Validate() error {
	switch {
	case r == nil:
		return fmt.Errorf("%w: nil record", ErrInvalidRecord)
	case r.UserID == nil:
		return fmt.Errorf("%w: nil user id", ErrInvalidRecord)
	case len(r.UserID.Value) != model.UserIDSize:
		return fmt.Errorf("%w: user id must be %d bytes, got %d", ErrInvalidRecord, model.UserIDSize, len(r.UserID.Value))
	case !customers.IsCustomerID(r.CustomerID):
		return fmt.Errorf("%w: not a coinbase customer id", ErrInvalidRecord)
	case r.CreatedAt.IsZero():
		return fmt.Errorf("%w: zero creation time", ErrInvalidRecord)
	}
	return nil
}

// Clone returns a deep copy of r, or nil for a nil r.
func (r *CustomerRecord) Clone() *CustomerRecord {
	if r == nil {
		return nil
	}
	return &CustomerRecord{
		UserID:     proto.Clone(r.UserID).(*commonpb.UserId),
		CustomerID: r.CustomerID,
		CreatedAt:  r.CreatedAt,
	}
}

// Store records which Coinbase customer belongs to which Flipcash user. Each
// user has at most one customer. A customer is created for one user and so
// belongs to one; the store does not check that across users, and
// GetCustomerByID would return any one of several records naming a customer.
//
// It holds the link and nothing else. Nothing the user submitted to Coinbase
// (name, date of birth, address, SSN, email, phone) is kept here, nor the
// verification state, nor which versions of Coinbase's terms the user has
// accepted. Coinbase is the source of truth for all of that and is read every
// time it is needed; a copy here could only fall behind it.
//
// A record is written once and never updated or deleted today. Deleting a
// user's Coinbase customer when their Flipcash account is deleted is not
// built, and neither is any way to move a user to another customer.
type Store interface {
	// PutCustomer records record as its user's Coinbase customer unless the
	// user already has one. It returns the record that stands (a copy of the
	// given one, or the one stored before) and whether this call stored it. A
	// repeat of the same record is created == false with the stored record.
	//
	// The write is conditional on the user having no record, so two StartKyc
	// calls racing for one user end with one customer: the loser is answered
	// with the winner's record and goes on with it, rather than each holding a
	// customer of its own. A record that fails Validate is refused with its
	// error, which wraps ErrInvalidRecord, and nothing is written.
	PutCustomer(ctx context.Context, record *CustomerRecord) (stored *CustomerRecord, created bool, err error)

	// GetCustomer returns the user's record, strongly consistent, or
	// ErrNotFound when they have none.
	GetCustomer(ctx context.Context, userID *commonpb.UserId) (*CustomerRecord, error)

	// GetCustomerByID returns the record of the user customerID belongs to, or
	// ErrNotFound when no user has it. A string that is not a Coinbase customer
	// ID is ErrNotFound with nothing read, since no stored record can carry
	// one.
	//
	// It exists because Coinbase's webhook events name a customer, not a user
	// (the receiver is not built yet). The read is eventually consistent, which
	// is accepted: an event is only a hint to re-read the customer from
	// Coinbase, and a customer is recorded here well before Coinbase can send
	// an event about it.
	GetCustomerByID(ctx context.Context, customerID string) (*CustomerRecord, error)
}
