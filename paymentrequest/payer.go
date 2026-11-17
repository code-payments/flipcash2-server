package paymentrequest

import (
	"bytes"
	"fmt"
	"math"
	"sort"
	"time"

	"google.golang.org/protobuf/proto"

	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"
	paymentrequestpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/paymentrequest/v1"

	currency_lib "github.com/code-payments/ocp-server/currency"

	"github.com/code-payments/flipcash2-server/send"
)

// A split's Payers (paymentrequestpb.Payer) and their totals
// (paymentrequestpb.PayerSummary) are not stored: they are computed on every
// read from what the request already holds, its shares, of which there is one
// Payer each, the creator's own included:
//   - what is due is the share's native_amount;
//   - a paid share, however it was paid (a payment, a hand mark, or the
//     creator's own share, paid from creation), has paid its native_amount, and
//     its status is its Paid;
//   - an unpaid share has paid nothing, and is unpaid.
//
// The summary totals every Payer, the creator's own share included, so it is
// what a client recomputes from the shares the request carries (see
// paymentrequestpb.PayerSummary).
//
// Payers are paged in two sections (see PayerSection) by GetPayers. Each is
// ordered by something a Payer keeps until it leaves the section, so a page
// resumes from a keyset cursor (PayerCursor) strictly after the last Payer it
// returned, which stays a valid position while other shares are paid.
//
// A collection's Payers are its contributions, one per member who has
// contributed, and are stored, not computed: a collection may have any number,
// so the request holds only their count (PaymentRequest.PayerCount) and each
// is a record of its own, read from the store (Store.GetCollectionPayer and
// Store.GetCollectionPayersPage, in the paid section's order; the unpaid
// section of a collection is always empty). Each is paid
// (ContributionPayer): nothing is due, and the fixed amount is paid. The
// summary is computed from the count alone (see PayerSummary). The methods
// below that compute Payers from shares (Payer, Payers, PayersPage) find none
// in a collection.

// Payer returns userID's Payer in a split, or nil when they have none: when
// they hold no share, and always for a collection, whose Payers are read from
// the store (see Store.GetCollectionPayer).
func (r *PaymentRequest) Payer(userID *commonpb.UserId) *paymentrequestpb.Payer {
	share := r.Share(userID)
	if share == nil {
		return nil
	}
	return payerOf(share)
}

// Payers returns every Payer of the request, in the order the creator listed
// the shares.
func (r *PaymentRequest) Payers() []*paymentrequestpb.Payer {
	shares := r.Shares()
	payers := make([]*paymentrequestpb.Payer, len(shares))
	for i, share := range shares {
		payers[i] = payerOf(share)
	}
	return payers
}

// payerOf is a split share's Payer (see above).
func payerOf(share *paymentrequestpb.Share) *paymentrequestpb.Payer {
	payer := &paymentrequestpb.Payer{
		UserId:          proto.Clone(share.UserId).(*commonpb.UserId),
		NativeAmountDue: share.NativeAmount,
	}
	if share.Paid == nil {
		payer.Status = &paymentrequestpb.Payer_Unpaid_{Unpaid: &paymentrequestpb.Payer_Unpaid{}}
		return payer
	}
	payer.NativeAmountPaid = share.NativeAmount
	payer.Status = &paymentrequestpb.Payer_Paid{Paid: proto.Clone(share.Paid).(*paymentrequestpb.Paid)}
	return payer
}

// PayerSummary totals the request's Payers (see paymentrequestpb.PayerSummary):
// for a split its shares, and for a collection its PayerCount contributions,
// each paid, of the fixed amount, with nothing due.
//
// Amounts are summed exactly, in whole minor units of the request's currency:
// every share, and a collection's fixed amount, is a whole number of them
// (see the CreatePaymentRequest RPC's share validation, and
// NewCollectionRequest), so each is rounded to its count of units, the counts
// are summed (or multiplied) as integers, and the total is divided back into
// the currency once. A float sum would drift (0.1 + 0.2 is
// 0.30000000000000004), where this gives the double nearest the exact decimal
// total, as a client parsing it would.
func (r *PaymentRequest) PayerSummary() *paymentrequestpb.PayerSummary {
	summary := &paymentrequestpb.PayerSummary{}
	unitsPerWhole := minorUnitsPerWhole(r.Currency())
	if r.Collection != nil {
		units := int64(math.Round(r.Collection.GetFixed().GetNativeAmount() * unitsPerWhole))
		summary.PayerCount = r.PayerCount
		summary.PaidCount = r.PayerCount
		summary.NativeAmountPaid = float64(int64(r.PayerCount)*units) / unitsPerWhole
		return summary
	}
	var due, paid int64
	for _, share := range r.Shares() {
		units := int64(math.Round(share.NativeAmount * unitsPerWhole))
		summary.PayerCount++
		due += units
		if share.Paid != nil {
			summary.PaidCount++
			paid += units
		}
	}
	summary.NativeAmountDue = float64(due) / unitsPerWhole
	summary.NativeAmountPaid = float64(paid) / unitsPerWhole
	return summary
}

// Currency is the request's currency: its type's (Split.currency, or
// Collection.currency).
func (r *PaymentRequest) Currency() string {
	switch {
	case r.Split != nil:
		return r.Split.Currency
	case r.Collection != nil:
		return r.Collection.Currency
	default:
		return ""
	}
}

// ContributionPayer is the Payer of userID's contribution to a collection, paid
// by paid (see Store.RecordContribution): nothing due, the fixed amount paid,
// and paid as its status. It is what a store records for a contribution and
// returns from Store.GetCollectionPayer and Store.GetCollectionPayersPage. It
// is nil for a request that is not a collection.
func (r *PaymentRequest) ContributionPayer(userID *commonpb.UserId, paid *paymentrequestpb.Paid) *paymentrequestpb.Payer {
	if r.Collection == nil {
		return nil
	}
	return &paymentrequestpb.Payer{
		UserId:           proto.Clone(userID).(*commonpb.UserId),
		NativeAmountDue:  0,
		NativeAmountPaid: r.Collection.GetFixed().GetNativeAmount(),
		Status:           &paymentrequestpb.Payer_Paid{Paid: proto.Clone(paid).(*paymentrequestpb.Paid)},
	}
}

// minorUnitsPerWhole is how many of a currency's minor units
// (send.MinimumTransferValue) make one whole unit: 100 for USD, 1 for a
// currency with no minor unit. It is an integer held as a float, so dividing a
// count of units by it is exact up to the one rounding to a double.
func minorUnitsPerWhole(currency string) float64 {
	return math.Round(1 / send.MinimumTransferValue(currency_lib.Code(currency)))
}

// PayerSection is one of the two sections a request's Payers are paged in
// (see PaymentRequest.PayersPage and paymentrequestpb.GetPayersRequest_Section).
type PayerSection uint8

const (
	// PayerSectionUnpaid holds the unpaid Payers in the order the creator
	// listed the shares.
	PayerSectionUnpaid PayerSection = iota + 1

	// PayerSectionPaid holds the paid Payers by Paid.paid_at descending, the
	// most recently paid first, then by user ID ascending.
	PayerSectionPaid
)

func (s PayerSection) String() string {
	switch s {
	case PayerSectionUnpaid:
		return "unpaid"
	case PayerSectionPaid:
		return "paid"
	default:
		return fmt.Sprintf("payer_section(%d)", uint8(s))
	}
}

// PayerCursor is a position in one section of a request's Payers, naming the
// last Payer a page returned:
//   - in the unpaid section, its share's Position in the request's shares, the
//     order the creator listed them;
//   - in the paid section, its PaidAt and UserID.
//
// Each is something a Payer keeps for as long as it is in the section (a
// share's position never changes, and a Payer leaves the unpaid section only
// for the paid one), and the paid section's user ID makes its order total, so
// a page resumes strictly after the Payer the cursor names whatever has been
// paid since. A Payer whose Paid is replaced (a hand mark by a payment) moves
// to its new paid_at, and so may be passed by, or met twice by, a walk; the
// RPC's clients cover that with the request's updates (see GetPayers).
type PayerCursor struct {
	// Position is the share's index in the request's shares. Unpaid section
	// only.
	Position uint32

	// PaidAt and UserID are the Payer's. Paid section only.
	PaidAt time.Time
	UserID []byte
}

// PayersPage returns up to limit of a split's Payers in section, in that
// section's order, strictly after cursor (from the start when nil). When more
// follow the page, next is the cursor naming its last Payer, to resume from;
// it is nil otherwise. limit must be positive. A collection's are paged from
// the store (see Store.GetCollectionPayersPage); here it has none.
func (r *PaymentRequest) PayersPage(section PayerSection, cursor *PayerCursor, limit int) (page []*paymentrequestpb.Payer, next *PayerCursor) {
	type entry struct {
		share  *paymentrequestpb.Share
		cursor *PayerCursor
	}
	var entries []entry
	switch section {
	case PayerSectionUnpaid:
		for i, share := range r.Shares() {
			if share.Paid != nil {
				continue
			}
			if cursor != nil && uint32(i) <= cursor.Position {
				continue
			}
			entries = append(entries, entry{share: share, cursor: &PayerCursor{Position: uint32(i)}})
		}
	case PayerSectionPaid:
		for _, share := range r.Shares() {
			if share.Paid == nil {
				continue
			}
			at := paidCursorOf(share)
			if cursor != nil && !PaidBefore(cursor, at) {
				continue
			}
			entries = append(entries, entry{share: share, cursor: at})
		}
		sort.Slice(entries, func(i, j int) bool {
			return PaidBefore(entries[i].cursor, entries[j].cursor)
		})
	default:
		return nil, nil
	}

	if len(entries) > limit {
		entries = entries[:limit]
		next = entries[limit-1].cursor
	}
	page = make([]*paymentrequestpb.Payer, len(entries))
	for i, e := range entries {
		page[i] = payerOf(e.share)
	}
	return page, next
}

// paidCursorOf is a paid share's position in the paid section.
func paidCursorOf(share *paymentrequestpb.Share) *PayerCursor {
	return &PayerCursor{
		PaidAt: share.Paid.GetPaidAt().AsTime(),
		UserID: share.GetUserId().GetValue(),
	}
}

// PaidPayerCursor is a paid Payer's position in the paid section: its
// Paid.paid_at and user ID. It is how a page of a collection's Payers (see
// Store.GetCollectionPayersPage) names its last Payer to resume from.
func PaidPayerCursor(payer *paymentrequestpb.Payer) *PayerCursor {
	return &PayerCursor{
		PaidAt: payer.GetPaid().GetPaidAt().AsTime(),
		UserID: append([]byte(nil), payer.GetUserId().GetValue()...),
	}
}

// PaidBefore reports whether a sorts ahead of b in the paid section: paid
// later, or paid at the same time with the lesser user ID.
func PaidBefore(a, b *PayerCursor) bool {
	if !a.PaidAt.Equal(b.PaidAt) {
		return a.PaidAt.After(b.PaidAt)
	}
	return bytes.Compare(a.UserID, b.UserID) < 0
}
