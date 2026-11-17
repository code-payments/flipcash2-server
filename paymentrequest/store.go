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
	// the user, which is always so of a collection: it has no shares, and
	// nothing in it is paid by PayShare (see RecordContribution).
	ErrNotAShare = errors.New("no share for user")

	// ErrNotACollection is returned by the collection-only methods
	// (GetOrCreatePayerIntent, RecordContribution) for a request that is not
	// a collection.
	ErrNotACollection = errors.New("payment request is not a collection")

	// ErrNotAPayer is returned by GetOrCreatePayerIntent for the request's
	// creator, who never pays into their own request.
	ErrNotAPayer = errors.New("user cannot pay into the payment request")

	// ErrNotOpen is returned by PayShare for a share marked paid by hand while
	// the request is not open as of now: stored completed, closed or expired,
	// or stored open but past its ends_at (see PaymentRequest.IsOpenAt).
	ErrNotOpen = errors.New("payment request is not open")

	// ErrInvalidPaidShare is returned by PayShare for a Paid it cannot
	// record: one without paid_at, or one paid other than by a payment or by
	// hand. A creator's share is paid from creation (see NewSplitRequest), so
	// Paid.Creator never reaches PayShare. RecordContribution returns it for
	// any Paid but a payment with its paid_at and intent (see
	// ValidateContribution).
	ErrInvalidPaidShare = errors.New("invalid paid share")

	// ErrWrongIntent is returned by PayShare for a payment naming an intent
	// other than the one the share's payment must use (see
	// PaymentRequest.PaymentIntentIDs), the creator's own share included,
	// which no payment pays, and by RecordContribution for a payment naming
	// an intent other than the one issued to the payer (see
	// GetOrCreatePayerIntent), including when none was.
	ErrWrongIntent = errors.New("payment intent is not the share's")
)

// PayerIntent is a member's part in a collection as a store holds it: the
// payment intent ID issued to them (see Store.GetOrCreatePayerIntent), and,
// once they have contributed with it, how (always Paid.Intent naming that
// ID; see Store.RecordContribution).
//
// Holding one does not make a member a Payer: until Paid is set they have
// contributed nothing, and they are neither counted, listed nor returned as a
// Payer. Paid is nil until then.
type PayerIntent struct {
	IntentID []byte
	Paid     *paymentrequestpb.Paid
}

// Store persists payment requests.
//
// A request is one record, written once with everything its creator chose
// (see PaymentRequest), of which only a split's shares' paid, a collection's
// PayerCount and the state change afterward. Every write of an existing
// request is a compare-and-set on its version inside the store, retried from
// a fresh read when it loses, so concurrent payments of different shares,
// concurrent contributions, a payment racing a close, and the expiry worker
// racing any of them all serialise on the one record and none is lost.
//
// A collection also has, per member who has asked to pay into it, a record of
// the payment intent ID issued to them and, once they contribute, their
// contribution (see PayerIntent). Issuing an ID writes only that record and
// is not a change to the request: it moves no version and counts no Payer.
// Recording a contribution writes that record and the request's together,
// atomically.
//
// Status moves forward only: from open to completed, closed or expired, never
// back and never between the three. Every change advances the version by
// exactly one, and an idempotent no-op (a share already paid, a close of a
// request that is not open, an expiry of a final one) writes nothing and
// leaves it alone. The one write that changes a request without advancing its
// version is Expire: expiry is the clock's (see PaymentRequest.StatusAt), so
// storing it changes nothing a reader is shown.
//
// Every request stored open that has an ends_at is on an expiry queue
// (GetDueForExpiry, ClaimForExpiry, DelayExpiry), due ExpireDelay after its
// ends_at, and leaves it in the write that makes it final, whichever that is:
// the last share paid, a close, or Expire. A collection with no ends_at is
// never queued: nothing but a close ends it.
//
// Reads of a request are strongly consistent: a creator reads their request
// right after creating it, and a payer theirs right after paying. A chat's
// listing (GetChatPage) and the expiry queue are eventually consistent, and
// need not be more: a listing is current only as of its read anyway (see the
// GetChatPaymentRequests RPC), and a claim re-checks the request it names.
type Store interface {
	// Create writes a new request as given, with status open, version 1, no
	// ClosedAt and PayerCount 0 whatever r says, and, when it has an ends_at,
	// enqueues it for expiry, due at ExpiryDueAt, and lists it in its chat's
	// open section. A request with no ends_at (a collection may have none) is
	// neither queued nor listed while open. A request failing
	// PaymentRequest.Validate is refused with its error. ErrExists is
	// returned, and nothing is written, when a request with its ID already
	// exists.
	Create(ctx context.Context, r *PaymentRequest) error

	// Get returns a request, or ErrNotFound.
	Get(ctx context.Context, id *paymentrequestpb.PaymentRequestId) (*PaymentRequest, error)

	// GetChatPage returns up to limit of the requests made in the group chat
	// chatID in one section, in that section's order, strictly after cursor
	// (from the start when nil): the open section (stored open, with an
	// ends_at) by ends_at ascending then ID ascending, and the resolved
	// section (stored completed, closed or expired) by ClosedAt descending
	// then ID descending (see CursorOf). A request stored open with no ends_at
	// is in neither; once closed it is in the resolved section like any
	// other. Whether more remain is the caller's to judge, by asking for
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
	//   - by hand (Paid.Manual): only while the request is open as of now
	//     (see PaymentRequest.IsOpenAt); ErrNotOpen otherwise.
	//   - by a payment (Paid.Intent): whatever the status. The money has
	//     moved, and the payment was validated while the request was open, so
	//     a close or an expiry, or a hand mark, that lands between the
	//     validation and this write must not lose it.
	//
	// A collection has no shares, so PayShare on one is ErrNotAShare (see
	// RecordContribution).
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
	PayShare(ctx context.Context, id *paymentrequestpb.PaymentRequestId, userID *commonpb.UserId, paid *paymentrequestpb.Paid, now time.Time) (r *PaymentRequest, changed, completed bool, err error)

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
	// is a no-op (dequeued, should it still be queued), and so is one with no
	// ends_at, which never expires. ErrNotFound is returned for a request that
	// does not exist. Whether it has reached its ends_at is the caller's (see
	// ExpireDelay).
	//
	// A payment recorded after the expiry is still recorded (see PayShare and
	// RecordContribution), and the request stays expired.
	Expire(ctx context.Context, id *paymentrequestpb.PaymentRequestId) (changed bool, err error)

	// GetOrCreatePayerIntent returns the payment intent ID issued to userID
	// for paying into a collection (see PayerIntent), issuing candidate as it
	// when none has been: the first call stores candidate, and every later
	// one returns that same ID and ignores its candidate, so concurrent first
	// calls agree on one ID. Paid is set in what it returns once userID has
	// contributed with it.
	//
	// candidate must be a fresh ID from NewPaymentIntentID: random, never
	// derived, so no one but the payer can know it, and PaymentIntentIDSize
	// bytes (an error otherwise). The ID is a secret until used, so it is
	// never logged.
	//
	// ErrNotFound is returned for a request that does not exist,
	// ErrNotACollection for a split, and ErrNotAPayer for the request's
	// creator. Nothing else is judged: whether the request is open and
	// whether userID may pay into it are the caller's.
	//
	// Issuing an ID is not a change to the request. It writes only userID's
	// record: the request's version does not move, and userID is not a Payer,
	// is not counted in PayerCount or PayerSummary, and is not listed by
	// GetCollectionPayersPage or returned by GetCollectionPayer, until
	// RecordContribution records a contribution with the ID. Nothing may count
	// or list members who hold an ID as Payers, since a member who asks to pay
	// and then does not has paid nothing.
	GetOrCreatePayerIntent(ctx context.Context, id *paymentrequestpb.PaymentRequestId, userID *commonpb.UserId, candidate []byte) (*PayerIntent, error)

	// GetPayerIntent returns userID's record in a collection: the payment
	// intent ID issued to them and, once they have contributed, how (see
	// PayerIntent). It is what a payment's validation checks a contribution
	// against. ErrNotFound is returned when no ID has been issued to userID,
	// which is always so in a split, and when the request does not exist;
	// callers have read the request already to tell those apart. It is
	// strongly consistent.
	GetPayerIntent(ctx context.Context, id *paymentrequestpb.PaymentRequestId, userID *commonpb.UserId) (*PayerIntent, error)

	// GetCollectionPayer returns userID's Payer in a collection (see
	// PaymentRequest.ContributionPayer) once they have contributed.
	// ErrNotFound is returned otherwise: when they hold an intent ID they have
	// not contributed with (which makes no one a Payer), hold none, the
	// request is a split, or it does not exist. It is strongly consistent.
	GetCollectionPayer(ctx context.Context, id *paymentrequestpb.PaymentRequestId, userID *commonpb.UserId) (*paymentrequestpb.Payer, error)

	// GetCollectionPayersPage returns up to limit of a collection's Payers,
	// the members who have contributed, in the paid section's order:
	// Paid.paid_at descending, then user ID ascending (see PaidBefore),
	// strictly after cursor (from the start when nil; see PaidPayerCursor for
	// a Payer's). Members holding an intent ID they have not contributed with
	// are not Payers and are never listed. Whether more remain is the
	// caller's to judge, by asking for one more than it shows. limit must be
	// positive. A split, or a request that does not exist, has none. It is
	// strongly consistent.
	GetCollectionPayersPage(ctx context.Context, id *paymentrequestpb.PaymentRequestId, cursor *PayerCursor, limit int) ([]*paymentrequestpb.Payer, error)

	// RecordContribution records userID's contribution to a collection, paid
	// by paid, as of now, and returns the request as it then is, reporting
	// whether anything changed.
	//
	// paid must be a payment (Paid.Intent) with a paid_at, which is when the
	// payment's intent was created, not when this runs (ErrInvalidPaidShare
	// otherwise, see ValidateContribution). ErrNotFound is returned for a
	// request that does not exist, ErrNotACollection for a split, and
	// ErrWrongIntent unless paid names the intent ID issued to userID (see
	// GetOrCreatePayerIntent), including when none was.
	//
	// A member contributes once, with their one intent ID, so userID having
	// contributed already is a no-op that writes nothing (a repeated task).
	// Otherwise it is recorded whatever the request's status, as a payment of
	// a share is (see PayShare): the money has moved, and the payment was
	// validated while the request was open, so a close or an expiry that
	// lands between the validation and this write must not lose it.
	//
	// A real change records userID's contribution (their PayerIntent's Paid,
	// and their Payer, see PaymentRequest.ContributionPayer), advances
	// PayerCount by one and the version by one, all in one atomic write. It
	// never changes the status or ClosedAt: a collection is never completed.
	RecordContribution(ctx context.Context, id *paymentrequestpb.PaymentRequestId, userID *commonpb.UserId, paid *paymentrequestpb.Paid, now time.Time) (r *PaymentRequest, changed bool, err error)
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
func PaidShareSupersedes(current, next *paymentrequestpb.Paid) bool {
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
func ValidatePaymentIntent(r *PaymentRequest, userID *commonpb.UserId, paid *paymentrequestpb.Paid) error {
	intent := paid.GetIntent()
	if intent == nil {
		return nil
	}
	if want := r.PaymentIntentID(userID); want == nil || !bytes.Equal(intent.GetValue(), want) {
		return ErrWrongIntent
	}
	return nil
}

// ValidateContribution checks that paid is one Store.RecordContribution
// records: a payment (Paid.Intent) naming its intent, with a paid_at. A hand
// mark and the creator's own are a split's alone. It returns an error
// wrapping ErrInvalidPaidShare otherwise. Every store checks it before reading
// anything.
func ValidateContribution(paid *paymentrequestpb.Paid) error {
	if paid.GetPaidAt() == nil {
		return fmt.Errorf("%w: paid_at is unset", ErrInvalidPaidShare)
	}
	if len(paid.GetIntent().GetValue()) == 0 {
		return fmt.Errorf("%w: a contribution is paid only by a payment naming its intent", ErrInvalidPaidShare)
	}
	return nil
}

// ValidatePayerIntentCandidate checks that candidate is an ID
// Store.GetOrCreatePayerIntent may issue: PaymentIntentIDSize bytes.
func ValidatePayerIntentCandidate(candidate []byte) error {
	if len(candidate) != PaymentIntentIDSize {
		return fmt.Errorf("payment intent id must be %d bytes, got %d", PaymentIntentIDSize, len(candidate))
	}
	return nil
}

// ValidatePaidShare checks that paid is one Store.PayShare records: it has a
// paid_at, and it is paid by a payment naming its intent or marked paid by
// hand. It returns an error wrapping ErrInvalidPaidShare otherwise. Every
// store checks it before reading anything.
func ValidatePaidShare(paid *paymentrequestpb.Paid) error {
	if paid.GetPaidAt() == nil {
		return fmt.Errorf("%w: paid_at is unset", ErrInvalidPaidShare)
	}
	switch how := paid.GetHow().(type) {
	case *paymentrequestpb.Paid_Intent:
		if len(how.Intent.GetValue()) == 0 {
			return fmt.Errorf("%w: intent is unset", ErrInvalidPaidShare)
		}
	case *paymentrequestpb.Paid_Manual_:
	case *paymentrequestpb.Paid_Creator_:
		return fmt.Errorf("%w: a creator's share is paid only at creation", ErrInvalidPaidShare)
	default:
		return fmt.Errorf("%w: how it was paid is unset", ErrInvalidPaidShare)
	}
	return nil
}
