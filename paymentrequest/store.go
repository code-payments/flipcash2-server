package paymentrequest

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"time"

	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"
	paymentrequestpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/paymentrequest/v1"
)

var (
	// ErrNotFound is returned for a payment request that does not exist.
	ErrNotFound = errors.New("payment request not found")

	// ErrExists is returned by Create when a request with the same ID already
	// exists: an earlier attempt of the same CreatePaymentRequest created it
	// (see MustDeriveID).
	ErrExists = errors.New("payment request already exists")

	// ErrNotAShare is returned by PayShare when no share of the request names
	// the user.
	ErrNotAShare = errors.New("no share for user")

	// ErrNotOpen is returned by PayShare for a share marked paid by hand while
	// the request is not open as of now: stored completed, closed or expired,
	// or stored open but past its ends_at (see PaymentRequest.IsOpenAt).
	ErrNotOpen = errors.New("payment request is not open")

	// ErrInvalidPaidShare is returned by PayShare for a PaidShare it cannot
	// record: one without paid_at, or one paid other than by a payment or by
	// hand. A creator's share is paid from creation (see NewSplitRequest), so
	// PaidShare.Creator never reaches PayShare.
	ErrInvalidPaidShare = errors.New("invalid paid share")

	// ErrWrongIntent is returned by PayShare for a payment naming an intent
	// other than the one the share's payment must use (see
	// PaymentRequest.PaymentIntentIDs), the creator's own share included,
	// which no payment pays.
	ErrWrongIntent = errors.New("payment intent is not the share's")
)

// Store persists payment requests.
//
// A request is one item, written once with everything its creator chose (see
// PaymentRequest), of which only each share's paid and the state change
// afterward. Every write of an existing request is a compare-and-set on its
// version inside the store, retried from a fresh read when it loses, so
// concurrent payments of different shares, a payment racing a close, and the
// expiry worker racing either all serialise on the one item and none is lost.
//
// Status moves forward only: from open to completed, closed or expired, never
// back and never between the three. Every change advances the version by
// exactly one, and an idempotent no-op (a share already paid, a close of a
// request that is not open, an expiry of a final one) writes nothing and
// leaves it alone. The one write that changes a request without advancing its
// version is Expire: expiry is the clock's (see PaymentRequest.StatusAt), so
// storing it changes nothing a reader is shown.
//
// Every request stored open is on an expiry queue (GetDueForExpiry,
// ClaimForExpiry, DelayExpiry), due ExpireDelay after its ends_at, and leaves
// it in the write that makes it final, whichever that is: the last share paid,
// a close, or Expire.
//
// Reads of a request are strongly consistent: a creator reads their request
// right after creating it, and a payer theirs right after paying. A chat's
// listing (GetChatPage) and the expiry queue are eventually consistent, and
// need not be more: a listing is current only as of its read anyway (see the
// GetChatPaymentRequests RPC), and a claim re-checks the request it names.
type Store interface {
	// Create writes a new request as given, with status open, version 1 and no
	// ClosedAt whatever r says, and enqueues it for expiry, due at
	// ExpiryDueAt. A request failing PaymentRequest.Validate is refused with
	// its error. ErrExists is returned, and nothing is written, when a request
	// with its ID already exists.
	Create(ctx context.Context, r *PaymentRequest) error

	// Get returns a request, or ErrNotFound.
	Get(ctx context.Context, id *paymentrequestpb.PaymentRequestId) (*PaymentRequest, error)

	// GetChatPage returns up to limit of the requests made in the group chat
	// chatID in one section, in that section's order, strictly after cursor
	// (from the start when nil): the open section (stored open) by ends_at
	// ascending then ID ascending, and the resolved section (stored completed,
	// closed or expired) by ClosedAt descending then ID descending (see
	// CursorOf). Whether more remain is the caller's to judge, by asking for
	// one more than it shows.
	//
	// Sections are by stored status, so a request stored open past its ends_at
	// is still in the open section until the expiry worker stores it expired,
	// about ExpireDelay later, and only then in the resolved one. The reader
	// drops it from an open page by PaymentRequest.StatusAt and so, for that
	// moment, shows it in neither; the lag is accepted, since a client already
	// judges an open request past its ends_at as expired itself.
	GetChatPage(ctx context.Context, chatID *commonpb.ChatId, section Section, cursor *PageCursor, limit int) ([]*PaymentRequest, error)

	// PayShare records userID's share of a request as paid by paid, as of now,
	// and returns the request as it then is, reporting whether anything
	// changed. ErrNotFound is returned for a request that does not exist,
	// ErrNotAShare when no share names userID, ErrInvalidPaidShare for a paid
	// it cannot record, and ErrWrongIntent for a payment whose intent is not
	// the share's (see ValidatePaymentIntent), whether or not the share is
	// already paid.
	//
	// A share already paid is a no-op, whatever the request's status, except
	// that a payment replaces a hand mark (see PaidShareSupersedes): a
	// repeated payment task, or a creator marking by hand a share already
	// paid, changes nothing. A share has one intent that can pay it, so the
	// only payment it records is that intent's, once. Otherwise the rule
	// depends on how it is paid:
	//   - by hand (PaidShare.Manual): only while the request is open as of now
	//     (see PaymentRequest.IsOpenAt); ErrNotOpen otherwise.
	//   - by a payment (PaidShare.Intent): whatever the status. The money has
	//     moved, and the payment was validated while the request was open, so
	//     a close or an expiry, or a hand mark, that lands between the
	//     validation and this write must not lose it.
	//
	// A real change sets the share's paid and advances the version by one. If
	// it paid the last unpaid share of a request stored open, the request is
	// completed in the same write, with ClosedAt paid.paid_at, and leaves the
	// expiry queue, and completed is reported; a request already final keeps
	// its status and ClosedAt. A payment replacing a hand mark pays no share
	// that was unpaid, so it never completes a request, and leaves the status
	// and ClosedAt as they were, even when the hand mark completed it. The
	// request returned cannot tell those two apart (both are COMPLETED, the
	// share paid by a payment), which is why completed is reported: it is what
	// announcing the change needs (see PublishSharePaid).
	PayShare(ctx context.Context, id *paymentrequestpb.PaymentRequestId, userID *commonpb.UserId, paid *paymentrequestpb.PaidShare, now time.Time) (r *PaymentRequest, changed, completed bool, err error)

	// Close closes a request open as of now: status closed, ClosedAt now,
	// version advanced by one, and off the expiry queue. A request that is not
	// open as of now, because it is stored final or has reached its ends_at,
	// is a no-op. Either way it returns the request as it then is, and reports
	// whether it changed. ErrNotFound is returned for a request that does not
	// exist. Who may close is the caller's.
	Close(ctx context.Context, id *paymentrequestpb.PaymentRequestId, now time.Time) (r *PaymentRequest, changed bool, err error)

	// GetDueForExpiry returns up to limit requests on the expiry queue whose
	// due time is at or before asOf, soonest first. The queue is eventually
	// consistent: a request just claimed or made final may still be listed,
	// which ClaimForExpiry settles.
	GetDueForExpiry(ctx context.Context, asOf time.Time, limit int) ([]*ExpiryTask, error)

	// GetExpiryQueueStats reports the expiry backlog as of asOf: how many
	// queued requests are due, counting at most MaxExpiryQueueStatsDepth, and
	// the due time of the most overdue. Requests not yet due are never read,
	// so its cost follows the backlog, not the number of open requests. It is
	// eventually consistent, like GetDueForExpiry.
	GetExpiryQueueStats(ctx context.Context, asOf time.Time) (*ExpiryQueueStats, error)

	// ClaimForExpiry pushes a queued request's due time out to until, provided
	// it is still queued and due as of asOf. It reports whether the claim was
	// performed: false means the request left the queue (it was made final)
	// or is no longer due (another server claimed or delayed it first).
	//
	// A claim is an efficiency guard, not a lock: Expire is idempotent and
	// safe to run concurrently, and claiming only keeps servers from
	// duplicating it. A crashed claimant needs no recovery: the claim expires
	// when its until passes and the request simply becomes due again.
	ClaimForExpiry(ctx context.Context, id *paymentrequestpb.PaymentRequestId, asOf, until time.Time) (bool, error)

	// DelayExpiry reschedules a queued request after a failed attempt: its due
	// time moves to nextAttemptAt and its failed-attempt count increments. It
	// is a no-op on a request that is no longer queued (it was made final,
	// dequeuing it).
	DelayExpiry(ctx context.Context, id *paymentrequestpb.PaymentRequestId, nextAttemptAt time.Time) error

	// Expire stores a request stored open as expired: status expired, ClosedAt
	// its ends_at, and off the expiry queue, with the version unchanged (see
	// Store), and reports whether it changed anything. A request already final
	// is a no-op (dequeued, should it still be queued). ErrNotFound is
	// returned for a request that does not exist. Whether it has reached its
	// ends_at is the caller's (see ExpireDelay).
	//
	// A payment recorded after the expiry is still recorded (see PayShare),
	// and the request stays expired.
	Expire(ctx context.Context, id *paymentrequestpb.PaymentRequestId) (changed bool, err error)
}

// PaidShareSupersedes reports whether Store.PayShare records next over a
// share's current paid (nil while unpaid): always for an unpaid share, and for
// a share marked paid by hand when next is a payment, never otherwise.
//
// A hand mark is the creator's word that a share was settled some other way;
// a payment is the money itself. A creator can mark a share by hand while its
// debtor's payment is in flight (validated against the unpaid share, not yet
// recorded by its task), so a payment landing after a hand mark replaces it,
// and the record shows what was paid. Nothing replaces a payment, nor the
// creator's own share, and a hand mark never replaces anything.
func PaidShareSupersedes(current, next *paymentrequestpb.PaidShare) bool {
	if current == nil {
		return true
	}
	return current.GetManual() != nil && next.GetIntent() != nil
}

// ValidatePaymentIntent checks that a payment of userID's share of r names the
// intent that share's payment must use (see PaymentRequest.PaymentIntentIDs),
// returning ErrWrongIntent otherwise. A hand mark names no intent and passes.
// Every store checks it once the share is found, before deciding anything
// else, so a share already paid refuses a wrong intent too.
//
// It is not what keeps a share from being paid twice: that is intent
// validation, which refuses a payment with any other intent ID before money
// moves, and OCP, which stores an intent ID once (see PaymentIntentIDs). By
// the time a payment reaches the store its money has moved, so this check
// cannot stop it. It keeps the record honest, so a share is only ever recorded
// as paid by its own intent, and it makes any path that reaches the store
// without that validation fail loudly instead of passing as a no-op on a share
// already paid.
func ValidatePaymentIntent(r *PaymentRequest, userID *commonpb.UserId, paid *paymentrequestpb.PaidShare) error {
	intent := paid.GetIntent()
	if intent == nil {
		return nil
	}
	if want := r.PaymentIntentID(userID); want == nil || !bytes.Equal(intent.GetValue(), want) {
		return ErrWrongIntent
	}
	return nil
}

// ValidatePaidShare checks that paid is one Store.PayShare records: it has a
// paid_at, and it is paid by a payment naming its intent or marked paid by
// hand. It returns an error wrapping ErrInvalidPaidShare otherwise. Every
// store checks it before reading anything.
func ValidatePaidShare(paid *paymentrequestpb.PaidShare) error {
	if paid.GetPaidAt() == nil {
		return fmt.Errorf("%w: paid_at is unset", ErrInvalidPaidShare)
	}
	switch how := paid.GetHow().(type) {
	case *paymentrequestpb.PaidShare_Intent:
		if len(how.Intent.GetValue()) == 0 {
			return fmt.Errorf("%w: intent is unset", ErrInvalidPaidShare)
		}
	case *paymentrequestpb.PaidShare_Manual_:
	case *paymentrequestpb.PaidShare_Creator_:
		return fmt.Errorf("%w: a creator's share is paid only at creation", ErrInvalidPaidShare)
	default:
		return fmt.Errorf("%w: how it was paid is unset", ErrInvalidPaidShare)
	}
	return nil
}
