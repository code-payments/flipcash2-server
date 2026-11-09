// Package paymentrequest implements payment requests (see
// paymentrequestpb.PaymentRequestServer): a creator asking others for money.
//
// Requests are layered, as the proto is. The first layer is where a request
// is made: today only a group chat (paymentrequestpb.GroupChatPaymentRequest),
// never a DM. The second is its type within that: today only a split
// (paymentrequestpb.Split), in which the creator asks named members of the
// chat (its debtors) each to pay them a share, all in one fiat currency. A
// kind or type added later is a new arm beside these (see PaymentRequest), not
// a rewrite of what is here.
//
// A group chat request belongs to the chat it was created for, and is shown
// there as a message carrying a messagingpb.PaymentRequestWidget, which names
// the request by ID alone: the shares and state come from this package's RPCs.
// It is live from creation, sent or not, and may be sent any number of times.
// A split's title and description are plaintext, and moderated, in a group
// that is not private, and encrypted with the chat key in a private group
// (see paymentrequestpb.EncryptedPaymentRequestText); everything else (the
// currency, ends_at, the shares and how each was paid) is plaintext on the
// server in either, since the server checks every payment against its share
// and decides when the request is complete.
//
// A request is OPEN until one of three things ends it, each final:
//   - every share is paid (COMPLETED). A share is paid all at once, for
//     exactly its amount, either by a Flipcash payment
//     (paymentrequestpb.PaidShare.Intent), at most once (see
//     PaymentRequest.PaymentIntentIDs), or by the creator marking it paid
//     by hand (PaidShare.Manual); the creator's own share is paid from
//     creation (PaidShare.Creator). A paid share stays paid, and only a hand
//     mark is ever replaced: by a payment that was in flight when it was
//     made (see PaidShareSupersedes).
//   - the creator closes it (CLOSED).
//   - its ends_at passes (EXPIRED). ends_at is the request's one hard
//     deadline, at most MaxDuration after its creation: no share can be paid
//     after it. Expiry is decided by the clock
//     (see PaymentRequest.StatusAt), and recorded later by a worker only so
//     that listings can file the request as resolved (see ExpireDelay).
//
// Nothing about a request is editable after creation: not its text, currency,
// deadline nor shares. No event announces a change to a request; clients
// refetch it, ordering copies by PaymentRequest.Version.
package paymentrequest

import (
	"crypto/rand"
	"crypto/sha256"
	"errors"
	"fmt"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"
	paymentrequestpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/paymentrequest/v1"

	"github.com/code-payments/flipcash2-server/model"
)

// IDSize is the length, in bytes, of a PaymentRequestId.
const IDSize = 16

// IdempotencyKeySize is the length, in bytes, of a client's IdempotencyKey.
const IdempotencyKeySize = 16

// PaymentIntentIDSize is the length, in bytes, of a share's payment intent ID
// (see PaymentRequest.PaymentIntentIDs): an OCP intent ID.
const PaymentIntentIDSize = 32

// groupChatIDSize is the length, in bytes, of a group chat ID, the only chat a
// request can be made in. It is chat.GroupChatIDSize, repeated so the model
// says what it holds without reference to the chat package. The package as a
// whole does import chat (see Server), so chat cannot import this one; a
// chat-side caller, like messaging's of poll, sits outside chat.
const groupChatIDSize = 16

// MaxDuration bounds how long after its creation a request may end (see
// Split.ends_at): four weeks, the longest a request is expected to stay open.
// It also keeps every ends_at, and so every time the stores derive from it (the
// expiry queue's due time and the open section's sort key, both Unix nanos),
// well inside the range an int64 of nanoseconds can represent.
const MaxDuration = 4 * 7 * 24 * time.Hour

// ExpireDelay is how long after a request's ends_at the expiry worker takes it
// up (see Store.GetDueForExpiry and Store.Expire), as poll.TallyDelay is for a
// poll's tally.
//
// Nothing waits on the worker: from ends_at on, a request stored OPEN is
// EXPIRED to every reader by the clock (see PaymentRequest.StatusAt), and no
// share can be marked paid. The worker only records it, so listings can file
// it as resolved. The delay is not needed for correctness either: a payment
// validated just before ends_at is recorded by a task moments later (see
// Store.PayShare), and that write and the worker's are each conditioned on the
// version they read, so they order themselves whichever lands first: a
// payment recorded first may complete the request, and one recorded after the
// expiry is still recorded on the EXPIRED request. The delay only keeps that
// race from being the common case, so a request whose last share is paid at
// its deadline usually reads COMPLETED rather than EXPIRED.
const ExpireDelay = time.Minute

// idDomain namespaces the request ID hash so it can never collide with an ID
// derived for another purpose.
const idDomain = "flipcash:paymentrequest"

// MustDeriveID returns the ID of the payment request that a
// CreatePaymentRequest from creatorID carrying key creates.
//
// Like a poll's ID (see poll.MustDerivePollID), a request's ID is derived from
// its creator and the request's idempotency key rather than minted at random,
// so the ID is itself the idempotency record: a retry derives the same ID, and
// the store's uniqueness condition turns the duplicate into a read of the
// original, whatever kind or parameters the retry carries. The creator is part
// of the input so no client can derive another user's request ID. The digest
// is truncated to IDSize bytes and stamped as a version 8 UUID, as a group ID
// is.
//
// It panics if either input is not its fixed width, which would be a
// programming error: user IDs are UUIDs, and a request's key is checked to
// width before it reaches here.
func MustDeriveID(creatorID *commonpb.UserId, key *paymentrequestpb.IdempotencyKey) *paymentrequestpb.PaymentRequestId {
	if len(creatorID.GetValue()) != model.UserIDSize {
		panic(fmt.Sprintf("user id must be %d bytes, got %d", model.UserIDSize, len(creatorID.GetValue())))
	}
	if len(key.GetValue()) != IdempotencyKeySize {
		panic(fmt.Sprintf("idempotency key must be %d bytes, got %d", IdempotencyKeySize, len(key.GetValue())))
	}

	h := sha256.New()
	h.Write([]byte(idDomain))
	h.Write(creatorID.Value)
	h.Write(key.Value)

	id := h.Sum(nil)[:IDSize]
	id[6] = (id[6] & 0x0f) | 0x80 // version 8
	id[8] = (id[8] & 0x3f) | 0x80 // RFC 9562 variant

	return &paymentrequestpb.PaymentRequestId{Value: id}
}

// ErrInvalidShares is wrapped by every refusal of a split's shares (see
// NewSplitRequest and PaymentRequest.Validate), so a caller can answer them
// all as INVALID_SHARES.
var ErrInvalidShares = errors.New("invalid shares")

// Status is a request's status. It only moves forward: from open to one of
// completed, closed or expired, which are final.
//
// All four are stored, but expired is also the clock's: a request stored open
// is expired to every reader from its ends_at (see PaymentRequest.StatusAt),
// and the expiry worker stores it later (see ExpireDelay and Store.Expire).
type Status uint8

const (
	StatusOpen Status = iota
	StatusCompleted
	StatusClosed
	StatusExpired
)

func (s Status) String() string {
	switch s {
	case StatusOpen:
		return "open"
	case StatusCompleted:
		return "completed"
	case StatusClosed:
		return "closed"
	case StatusExpired:
		return "expired"
	default:
		return fmt.Sprintf("status(%d)", uint8(s))
	}
}

// IsFinal reports whether s is one a request never leaves: anything but open.
func (s Status) IsFinal() bool {
	return s != StatusOpen
}

// ToProto projects the status onto paymentrequestpb.PaymentRequestStatus.
func (s Status) ToProto() paymentrequestpb.PaymentRequestStatus {
	switch s {
	case StatusOpen:
		return paymentrequestpb.PaymentRequestStatus_PAYMENT_REQUEST_STATUS_OPEN
	case StatusCompleted:
		return paymentrequestpb.PaymentRequestStatus_PAYMENT_REQUEST_STATUS_COMPLETED
	case StatusClosed:
		return paymentrequestpb.PaymentRequestStatus_PAYMENT_REQUEST_STATUS_CLOSED
	case StatusExpired:
		return paymentrequestpb.PaymentRequestStatus_PAYMENT_REQUEST_STATUS_EXPIRED
	default:
		return paymentrequestpb.PaymentRequestStatus_PAYMENT_REQUEST_STATUS_UNKNOWN
	}
}

// PaymentRequest is a payment request as stored.
//
// What every kind shares is held directly: ID, Creator, CreatedAt and the
// state (Status, Version, ClosedAt). The layers are held one field per arm, as
// the proto's oneofs are: ChatID is the first layer, a group chat request
// (paymentrequestpb.GroupChatPaymentRequest; a 16-byte group chat ID, never a
// DM's), and Split is the type within it. Exactly one type is set. Another
// kind or type is a new field beside these, with a case in EndsAt, Validate
// and ToProto, and nothing else changes.
//
// Split is exactly what the creator chose, except each share's paid, the only
// part of a request that changes after creation (see Store.PayShare): a share
// is addressed by its user ID (see Share), and once paid stays paid, changing
// only from a hand mark to a payment (see PaidShareSupersedes). The
// creator's own share, when they listed one, is paid from creation
// (paymentrequestpb.PaidShare.Creator with paid_at = CreatedAt; see
// NewSplitRequest). Its text is plaintext in a group that is not private and
// encrypted in a private group; the store holds either as given.
//
// Version starts at 1 on creation and advances by exactly one on every change
// (a share paid, by payment or by hand, a completion, a close); an idempotent
// no-op leaves it alone, and so does expiry, which is the clock's (see
// paymentrequestpb.PaymentRequestMetadata). ClosedAt is nil while Status is
// StatusOpen, and set once it is not: ends_at when expired, when the creator
// closed it when closed, when its last share was paid when completed.
type PaymentRequest struct {
	ID        *paymentrequestpb.PaymentRequestId
	Creator   *commonpb.UserId
	CreatedAt time.Time

	// ChatID is the group chat the request was made in.
	ChatID *commonpb.ChatId

	// Split is set for a split, the only type today.
	Split *paymentrequestpb.Split

	// PaymentIntentIDs holds, for every debtor's share (every share but the
	// creator's own), the OCP intent ID a payment of it must use, keyed by
	// the debtor's user ID (the string of its bytes). Each is
	// PaymentIntentIDSize random bytes, drawn once at creation (see
	// NewSplitRequest) and never changed.
	//
	// It is what makes a share payable at most once. OCP stores an intent
	// ID at most once, in the same transaction that moves the money, so a
	// payment refused for any ID but this one (see intent.Integration) can
	// commit once for the share, however many times or from however many
	// devices the debtor tries. The ID is the server's, not derived from
	// anything a client sees: an ID anyone could compute could be used up by
	// someone else first, leaving the share unpayable.
	//
	// It is a secret until used, so it is never projected (see ToProto): the
	// GetPaymentRequest RPC gives a debtor their own while their share is
	// unpaid, and no one else. Once the share is paid by it, the ID is
	// public in PaidShare.intent and on the cash message, which costs
	// nothing, since it can never be used again.
	PaymentIntentIDs map[string][]byte

	Status   Status
	Version  uint64
	ClosedAt *time.Time
}

// NewSplitRequest returns the split request that creator, in the group chat
// chatID, asks for with split at createdAt: stored open at version 1, with the
// creator's own share, if they listed one, paid from creation
// (paymentrequestpb.PaidShare.Creator at createdAt), and a random payment
// intent ID for every other share (see PaymentRequest.PaymentIntentIDs).
//
// split is as a client sends it, which it does not modify: an error wrapping
// ErrInvalidShares is returned when a share has paid set, two shares name the
// same user, or no share names anyone but the creator (see Validate for the
// rest). What it cannot judge alone is the caller's: that ends_at is in the
// future and at most MaxDuration away, that every debtor is a member of the
// chat, that each amount is a whole number, at least one, of the currency's
// minimum transfer value, and that the currency can be valued.
func NewSplitRequest(
	id *paymentrequestpb.PaymentRequestId,
	creator *commonpb.UserId,
	chatID *commonpb.ChatId,
	split *paymentrequestpb.Split,
	createdAt time.Time,
) (*PaymentRequest, error) {
	for i, share := range split.GetShares() {
		if share.GetPaid() != nil {
			return nil, fmt.Errorf("%w: share %d has paid set", ErrInvalidShares, i)
		}
	}

	r := &PaymentRequest{
		ID:        proto.Clone(id).(*paymentrequestpb.PaymentRequestId),
		Creator:   proto.Clone(creator).(*commonpb.UserId),
		CreatedAt: createdAt,
		ChatID:    proto.Clone(chatID).(*commonpb.ChatId),
		Split:     proto.Clone(split).(*paymentrequestpb.Split),
		Status:    StatusOpen,
		Version:   1,
	}
	if own := r.Share(creator); own != nil {
		own.Paid = &paymentrequestpb.PaidShare{
			PaidAt: timestamppb.New(createdAt),
			How:    &paymentrequestpb.PaidShare_Creator_{Creator: &paymentrequestpb.PaidShare_Creator{}},
		}
	}
	r.PaymentIntentIDs = make(map[string][]byte, len(r.Shares()))
	for _, share := range r.Shares() {
		if proto.Equal(share.GetUserId(), creator) {
			continue
		}
		intentID := make([]byte, PaymentIntentIDSize)
		if _, err := rand.Read(intentID); err != nil {
			return nil, err
		}
		r.PaymentIntentIDs[string(share.GetUserId().GetValue())] = intentID
	}
	if err := r.Validate(); err != nil {
		return nil, err
	}
	return r, nil
}

// Validate checks that the request is one a store may hold: every ID its fixed
// width, exactly one type set, and for a split an ends_at and shares that name
// distinct users, at least one of them not the creator, with the creator's own
// share (if any) paid from creation and no other share claiming to be, and a
// payment intent ID for every share but the creator's and for nothing else.
// Share problems wrap ErrInvalidShares.
//
// It checks shape only: nothing about the chat, the currency, the amounts or
// the clock, which are the creating caller's (see NewSplitRequest).
func (r *PaymentRequest) Validate() error {
	if len(r.ID.GetValue()) != IDSize {
		return fmt.Errorf("payment request id must be %d bytes, got %d", IDSize, len(r.ID.GetValue()))
	}
	if len(r.Creator.GetValue()) != model.UserIDSize {
		return fmt.Errorf("creator id must be %d bytes, got %d", model.UserIDSize, len(r.Creator.GetValue()))
	}
	if r.CreatedAt.IsZero() {
		return errors.New("created_at is unset")
	}
	if len(r.ChatID.GetValue()) != groupChatIDSize {
		return fmt.Errorf("chat id must be a %d-byte group chat id, got %d bytes", groupChatIDSize, len(r.ChatID.GetValue()))
	}

	switch {
	case r.Split != nil:
		return r.validateSplit()
	default:
		return errors.New("payment request has no type")
	}
}

func (r *PaymentRequest) validateSplit() error {
	if r.Split.GetEndsAt() == nil {
		return errors.New("split ends_at is unset")
	}
	if len(r.Split.GetShares()) == 0 {
		return fmt.Errorf("%w: no shares", ErrInvalidShares)
	}

	seen := make(map[string]bool, len(r.Split.Shares))
	debtors := 0
	for i, share := range r.Split.Shares {
		userID := share.GetUserId().GetValue()
		if len(userID) != model.UserIDSize {
			return fmt.Errorf("%w: share %d user id must be %d bytes, got %d", ErrInvalidShares, i, model.UserIDSize, len(userID))
		}
		if seen[string(userID)] {
			return fmt.Errorf("%w: share %d names a user another share names", ErrInvalidShares, i)
		}
		seen[string(userID)] = true

		isCreator := proto.Equal(share.UserId, r.Creator)
		byCreator := share.GetPaid().GetCreator() != nil
		switch {
		case isCreator && !byCreator:
			return fmt.Errorf("%w: the creator's share must be paid from creation", ErrInvalidShares)
		case isCreator && !share.Paid.GetPaidAt().AsTime().Equal(r.CreatedAt):
			return fmt.Errorf("%w: the creator's share must be paid at creation", ErrInvalidShares)
		case !isCreator && byCreator:
			return fmt.Errorf("%w: share %d is not the creator's but is paid as theirs", ErrInvalidShares, i)
		}
		if !isCreator {
			debtors++
		}
	}
	if debtors == 0 {
		return fmt.Errorf("%w: no share names anyone but the creator", ErrInvalidShares)
	}

	// One payment intent ID per debtor and none for anyone else: the shares
	// name distinct users, so as many IDs as debtors, each found under a
	// debtor, are exactly the debtors'. They are the server's, never a
	// client's, so a problem here is not ErrInvalidShares.
	if len(r.PaymentIntentIDs) != debtors {
		return fmt.Errorf("%d payment intent ids for %d debtors", len(r.PaymentIntentIDs), debtors)
	}
	for _, share := range r.Split.Shares {
		if proto.Equal(share.UserId, r.Creator) {
			continue
		}
		if intentID := r.PaymentIntentIDs[string(share.UserId.Value)]; len(intentID) != PaymentIntentIDSize {
			return fmt.Errorf("share payment intent id must be %d bytes, got %d", PaymentIntentIDSize, len(intentID))
		}
	}
	return nil
}

// EndsAt is when the request stops accepting payments: its type's deadline
// (for a split, Split.ends_at).
func (r *PaymentRequest) EndsAt() time.Time {
	switch {
	case r.Split != nil:
		return r.Split.GetEndsAt().AsTime()
	default:
		return time.Time{}
	}
}

// StatusAt is the status a reader is shown at now: the stored status, except
// that a request stored open is expired from its ends_at on, since nothing can
// be paid from then (see ExpireDelay).
func (r *PaymentRequest) StatusAt(now time.Time) Status {
	if r.Status == StatusOpen && !now.Before(r.EndsAt()) {
		return StatusExpired
	}
	return r.Status
}

// IsOpenAt reports whether the request accepts a share being marked paid, or
// closed, at now: it is stored open and has not reached its ends_at.
func (r *PaymentRequest) IsOpenAt(now time.Time) bool {
	return r.StatusAt(now) == StatusOpen
}

// ClosedAtAt is when the request stopped being open, as a reader is shown it
// at now: the stored ClosedAt, or ends_at for a request expired by the clock
// alone. ok is false while the request is open at now.
func (r *PaymentRequest) ClosedAtAt(now time.Time) (closedAt time.Time, ok bool) {
	switch {
	case r.ClosedAt != nil:
		return *r.ClosedAt, true
	case r.StatusAt(now) == StatusExpired:
		return r.EndsAt(), true
	default:
		return time.Time{}, false
	}
}

// Shares returns the request's shares, in the order the creator listed them.
// They are the stored values: callers must not modify them.
func (r *PaymentRequest) Shares() []*paymentrequestpb.Share {
	switch {
	case r.Split != nil:
		return r.Split.Shares
	default:
		return nil
	}
}

// Share returns the share naming userID, or nil when none does. It is the
// stored value, which only a store modifies (see Store.PayShare).
func (r *PaymentRequest) Share(userID *commonpb.UserId) *paymentrequestpb.Share {
	for _, share := range r.Shares() {
		if proto.Equal(share.GetUserId(), userID) {
			return share
		}
	}
	return nil
}

// PaymentIntentID returns the intent ID a payment of userID's share must use
// (see PaymentIntentIDs), or nil when they hold no share a payment can pay: no
// share at all, or the creator's own. It is the stored value: callers must not
// modify it.
func (r *PaymentRequest) PaymentIntentID(userID *commonpb.UserId) []byte {
	return r.PaymentIntentIDs[string(userID.GetValue())]
}

// CreatorShare returns the creator's own share, paid from creation, or nil
// when they listed none.
func (r *PaymentRequest) CreatorShare() *paymentrequestpb.Share {
	return r.Share(r.Creator)
}

// AllSharesPaid reports whether every share is paid, which is what completes
// an open request.
func (r *PaymentRequest) AllSharesPaid() bool {
	shares := r.Shares()
	for _, share := range shares {
		if share.GetPaid() == nil {
			return false
		}
	}
	return len(shares) > 0
}

// ToProto projects the request onto paymentrequestpb.PaymentRequestMetadata
// as a reader sees it at now: a request stored open past its ends_at is
// EXPIRED, closed at its ends_at, at the version it was stored with (see
// StatusAt and ClosedAtAt), which is exactly what the expiry worker will
// store, so what a reader is shown never changes when it does. It never
// carries PaymentIntentIDs, which only their debtor is given (see
// PaymentIntentIDs).
func (r *PaymentRequest) ToProto(now time.Time) *paymentrequestpb.PaymentRequestMetadata {
	metadata := &paymentrequestpb.PaymentRequestMetadata{
		PaymentRequestId: proto.Clone(r.ID).(*paymentrequestpb.PaymentRequestId),
		Creator:          proto.Clone(r.Creator).(*commonpb.UserId),
		CreatedAt:        timestamppb.New(r.CreatedAt),
		Status:           r.StatusAt(now).ToProto(),
		Version:          r.Version,
	}
	if closedAt, ok := r.ClosedAtAt(now); ok {
		metadata.ClosedAt = timestamppb.New(closedAt)
	}

	groupChat := &paymentrequestpb.GroupChatPaymentRequest{
		ChatId: proto.Clone(r.ChatID).(*commonpb.ChatId),
	}
	switch {
	case r.Split != nil:
		groupChat.Type = &paymentrequestpb.GroupChatPaymentRequest_Split{
			Split: proto.Clone(r.Split).(*paymentrequestpb.Split),
		}
	}
	metadata.Kind = &paymentrequestpb.PaymentRequestMetadata_GroupChat{GroupChat: groupChat}

	return metadata
}

// Clone returns a deep copy of the request.
func (r *PaymentRequest) Clone() *PaymentRequest {
	clone := &PaymentRequest{
		ID:        proto.Clone(r.ID).(*paymentrequestpb.PaymentRequestId),
		Creator:   proto.Clone(r.Creator).(*commonpb.UserId),
		CreatedAt: r.CreatedAt,
		ChatID:    proto.Clone(r.ChatID).(*commonpb.ChatId),
		Status:    r.Status,
		Version:   r.Version,
	}
	if r.Split != nil {
		clone.Split = proto.Clone(r.Split).(*paymentrequestpb.Split)
	}
	if r.PaymentIntentIDs != nil {
		clone.PaymentIntentIDs = make(map[string][]byte, len(r.PaymentIntentIDs))
		for userID, intentID := range r.PaymentIntentIDs {
			clone.PaymentIntentIDs[userID] = append([]byte(nil), intentID...)
		}
	}
	if r.ClosedAt != nil {
		closedAt := *r.ClosedAt
		clone.ClosedAt = &closedAt
	}
	return clone
}

// Section is one of the two sections a chat's requests are paged in (see
// Store.GetChatPage and paymentrequestpb.GetChatPaymentRequestsRequest_Section).
type Section uint8

const (
	// SectionOpen holds the requests stored open, ordered by ends_at
	// ascending, then ID ascending.
	SectionOpen Section = iota + 1

	// SectionResolved holds the requests stored completed, closed or expired,
	// ordered by ClosedAt descending, then ID descending.
	SectionResolved
)

func (s Section) String() string {
	switch s {
	case SectionOpen:
		return "open"
	case SectionResolved:
		return "resolved"
	default:
		return fmt.Sprintf("section(%d)", uint8(s))
	}
}

// PageCursor is a position in one section of a chat's requests: the sort time
// (ends_at in the open section, ClosedAt in the resolved one) and ID of the
// last request read. The ID makes the order total, so a page resumes strictly
// after the request the cursor names even when others share its time.
type PageCursor struct {
	At time.Time
	ID []byte
}

// CursorOf returns the cursor naming r's position in section.
func CursorOf(r *PaymentRequest, section Section) *PageCursor {
	cursor := &PageCursor{ID: append([]byte(nil), r.ID.GetValue()...)}
	switch section {
	case SectionOpen:
		cursor.At = r.EndsAt()
	case SectionResolved:
		if r.ClosedAt != nil {
			cursor.At = *r.ClosedAt
		}
	}
	return cursor
}

// ExpiryTask is a queued unit of expiry work: a request stored open, with the
// retry bookkeeping the worker schedules off of (see Store.GetDueForExpiry).
type ExpiryTask struct {
	ID *paymentrequestpb.PaymentRequestId

	// Attempts is the number of failed expiry attempts recorded so far (via
	// Store.DelayExpiry). The worker uses it to pace backoff.
	Attempts uint32

	// NextAttemptAt is when the task next becomes due: ExpireDelay after the
	// request's ends_at at first (see ExpiryDueAt), then whenever a claim or a
	// delay moves it.
	NextAttemptAt time.Time
}

// MaxExpiryQueueStatsDepth bounds the walk behind Store.GetExpiryQueueStats: a
// backlog deeper than this reports this depth, so the gauges cost one bounded
// read even mid-incident.
const MaxExpiryQueueStatsDepth = 1000

// ExpiryQueueStats is a snapshot of the expiry worker's backlog, backing its
// gauges (see Store.GetExpiryQueueStats). Only due requests count: one waiting
// for its ends_at is not work the worker is behind on. A claimed request is
// not due until its lease ends, and one whose attempt failed not until its
// retry, so a request stuck retrying shows here only between its retry coming
// due and a worker claiming it; its failures are logged by the worker.
type ExpiryQueueStats struct {
	// DueDepth is how many queued requests are due, at most
	// MaxExpiryQueueStatsDepth.
	DueDepth uint64

	// OldestDueAt is the due time of the most overdue request, or the zero
	// time when none is due.
	OldestDueAt time.Time
}

// ExpiryDueAt is when a request first comes due for the expiry worker:
// ExpireDelay after its ends_at.
func ExpiryDueAt(r *PaymentRequest) time.Time {
	return r.EndsAt().Add(ExpireDelay)
}
