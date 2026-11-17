package paymentrequest

import (
	"bytes"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"
	paymentrequestpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/paymentrequest/v1"

	"github.com/code-payments/flipcash2-server/model"
)

func TestPayer(t *testing.T) {
	createdAt := time.Now()
	creator := model.MustGenerateUserID()
	unpaid := model.MustGenerateUserID()
	manual := model.MustGenerateUserID()
	intent := model.MustGenerateUserID()

	split := newSplit(createdAt.Add(time.Hour), creator, unpaid, manual, intent)
	split.Shares[0].NativeAmount = 4
	split.Shares[1].NativeAmount = 1.5
	split.Shares[2].NativeAmount = 2.25
	split.Shares[3].NativeAmount = 3
	r, err := NewSplitRequest(newID(), creator, newChatID(), split, createdAt)
	require.NoError(t, err)
	r.Share(manual).Paid = manualPaidAt(createdAt.Add(time.Minute))
	r.Share(intent).Paid = intentPaidAt(r, intent, createdAt.Add(2*time.Minute))

	// An unpaid share owes its amount and has paid nothing.
	payer := r.Payer(unpaid)
	require.NoError(t, payer.Validate())
	require.True(t, proto.Equal(unpaid, payer.UserId))
	require.Equal(t, 1.5, payer.NativeAmountDue)
	require.Zero(t, payer.NativeAmountPaid)
	require.NotNil(t, payer.GetUnpaid())
	require.Nil(t, payer.GetPaid())

	// A paid share has paid its amount, however it was paid,
	// and carries its Paid.
	for _, tc := range []struct {
		userID *commonpb.UserId
		amount float64
	}{
		{creator, 4},
		{manual, 2.25},
		{intent, 3},
	} {
		payer := r.Payer(tc.userID)
		require.NoError(t, payer.Validate())
		require.Equal(t, tc.amount, payer.NativeAmountDue)
		require.Equal(t, tc.amount, payer.NativeAmountPaid)
		require.Nil(t, payer.GetUnpaid())
		require.True(t, proto.Equal(r.Share(tc.userID).Paid, payer.GetPaid()))
	}
	require.NotNil(t, r.Payer(creator).GetPaid().GetCreator())
	require.NotNil(t, r.Payer(manual).GetPaid().GetManual())
	require.NotNil(t, r.Payer(intent).GetPaid().GetIntent())

	// No share, no Payer.
	require.Nil(t, r.Payer(model.MustGenerateUserID()))

	// Every Payer, in the order the shares are listed, each a copy.
	payers := r.Payers()
	require.Len(t, payers, 4)
	for i, share := range r.Shares() {
		require.True(t, proto.Equal(share.UserId, payers[i].UserId))
	}
	payers[0].GetPaid().PaidAt = timestamppb.New(createdAt.Add(time.Hour))
	require.True(t, createdAt.Equal(r.CreatorShare().Paid.PaidAt.AsTime()))
}

func TestPayerSummary(t *testing.T) {
	createdAt := time.Now()
	creator := model.MustGenerateUserID()
	a := model.MustGenerateUserID()
	b := model.MustGenerateUserID()
	c := model.MustGenerateUserID()

	// Variables, so the sum below is a float sum rather than an exact
	// constant one.
	tenth, fifth := 0.1, 0.2
	split := newSplit(createdAt.Add(time.Hour), creator, a, b, c)
	split.Shares[0].NativeAmount = tenth
	split.Shares[1].NativeAmount = fifth
	split.Shares[2].NativeAmount = tenth + fifth // 0.30000000000000004, a whole number of cents
	split.Shares[3].NativeAmount = 0.07
	r, err := NewSplitRequest(newID(), creator, newChatID(), split, createdAt)
	require.NoError(t, err)

	// The creator's own share is paid from creation.
	summary := r.PayerSummary()
	require.NoError(t, summary.Validate())
	require.EqualValues(t, 4, summary.PayerCount)
	require.EqualValues(t, 1, summary.PaidCount)
	require.Equal(t, 0.67, summary.NativeAmountDue)
	require.Equal(t, 0.1, summary.NativeAmountPaid)

	// Summed exactly, where a float sum drifts.
	r.Share(a).Paid = manualPaidAt(createdAt.Add(time.Minute))
	summary = r.PayerSummary()
	require.NotEqual(t, 0.3, tenth+fifth)
	require.Equal(t, 0.3, summary.NativeAmountPaid)
	require.EqualValues(t, 2, summary.PaidCount)

	r.Share(b).Paid = intentPaidAt(r, b, createdAt.Add(time.Minute))
	r.Share(c).Paid = intentPaidAt(r, c, createdAt.Add(time.Minute))
	summary = r.PayerSummary()
	require.EqualValues(t, 4, summary.PaidCount)
	require.Equal(t, 0.67, summary.NativeAmountPaid)
	require.Equal(t, summary.NativeAmountDue, summary.NativeAmountPaid)

	// In a currency with no minor unit, and one with three digits of it.
	for _, tc := range []struct {
		currency string
		amounts  []float64
		want     float64
	}{
		{"jpy", []float64{1000, 1, 250}, 1251},
		{"kwd", []float64{0.001, 0.1, 1.234}, 1.335},
	} {
		split := newSplit(createdAt.Add(time.Hour), a, b, c)
		split.Currency = tc.currency
		for i, amount := range tc.amounts {
			split.Shares[i].NativeAmount = amount
		}
		r, err := NewSplitRequest(newID(), creator, newChatID(), split, createdAt)
		require.NoError(t, err)
		summary := r.PayerSummary()
		require.Equal(t, tc.want, summary.NativeAmountDue, tc.currency)
		require.Zero(t, summary.NativeAmountPaid, tc.currency)
		require.Zero(t, summary.PaidCount, tc.currency)
	}

	// Every read carries it.
	metadata := r.ToProto(createdAt)
	require.NoError(t, metadata.Validate())
	require.True(t, proto.Equal(r.PayerSummary(), metadata.PayerSummary))
}

func TestPayersPage_Unpaid(t *testing.T) {
	createdAt := time.Now()
	creator := model.MustGenerateUserID()
	users := []*commonpb.UserId{creator}
	for range 5 {
		users = append(users, model.MustGenerateUserID())
	}
	r, err := NewSplitRequest(newID(), creator, newChatID(), newSplit(createdAt.Add(time.Hour), users...), createdAt)
	require.NoError(t, err)
	r.Share(users[2]).Paid = manualPaidAt(createdAt)

	// Unpaid shares, in the order listed: the creator's and the paid one are
	// not among them.
	want := []*commonpb.UserId{users[1], users[3], users[4], users[5]}
	requirePayers(t, want, walkPayers(t, r, PayerSectionUnpaid, 3))
	requirePayers(t, want, walkPayers(t, r, PayerSectionUnpaid, 1))
	page, next := r.PayersPage(PayerSectionUnpaid, nil, 4)
	requirePayers(t, want, page)
	require.Nil(t, next)

	// A cursor stays a position while other shares are paid: a page resumes
	// strictly after the share it names, even once that share is paid.
	page, next = r.PayersPage(PayerSectionUnpaid, nil, 2)
	requirePayers(t, want[:2], page)
	require.Equal(t, uint32(3), next.Position)
	r.Share(users[3]).Paid = manualPaidAt(createdAt)
	r.Share(users[4]).Paid = manualPaidAt(createdAt)
	page, next = r.PayersPage(PayerSectionUnpaid, next, 2)
	requirePayers(t, []*commonpb.UserId{users[5]}, page)
	require.Nil(t, next)

	// A cursor past the end is an empty page.
	page, next = r.PayersPage(PayerSectionUnpaid, &PayerCursor{Position: 100}, 2)
	require.Empty(t, page)
	require.Nil(t, next)
}

func TestPayersPage_Paid(t *testing.T) {
	createdAt := time.Now().UTC()
	creator := model.MustGenerateUserID()

	// Five debtors with IDs in a known order, so ties are predictable.
	debtors := make([]*commonpb.UserId, 5)
	for i := range debtors {
		debtors[i] = &commonpb.UserId{Value: bytes.Repeat([]byte{byte(0x10 * (i + 1))}, model.UserIDSize)}
	}
	r, err := NewSplitRequest(newID(), creator, newChatID(), newSplit(createdAt.Add(time.Hour), append([]*commonpb.UserId{creator}, debtors...)...), createdAt)
	require.NoError(t, err)

	// Paid at different times, two of them at the same one.
	r.Share(debtors[0]).Paid = manualPaidAt(createdAt.Add(3 * time.Minute))
	r.Share(debtors[3]).Paid = intentPaidAt(r, debtors[3], createdAt.Add(time.Minute))
	r.Share(debtors[1]).Paid = manualPaidAt(createdAt.Add(time.Minute))

	// Newest first, ties by user ID, the creator's own share (paid at
	// creation) last.
	want := []*commonpb.UserId{debtors[0], debtors[1], debtors[3], creator}
	requirePayers(t, want, walkPayers(t, r, PayerSectionPaid, 1))
	requirePayers(t, want, walkPayers(t, r, PayerSectionPaid, 3))
	page, next := r.PayersPage(PayerSectionPaid, nil, 10)
	requirePayers(t, want, page)
	require.Nil(t, next)

	// A cursor resumes strictly after its Payer, whatever is paid since: a
	// Payer paid after the walk passed it lands ahead of the cursor, unseen.
	page, next = r.PayersPage(PayerSectionPaid, nil, 2)
	requirePayers(t, want[:2], page)
	require.True(t, bytes.Equal(debtors[1].Value, next.UserID))
	r.Share(debtors[2]).Paid = manualPaidAt(createdAt.Add(time.Hour))
	page, next = r.PayersPage(PayerSectionPaid, next, 10)
	requirePayers(t, []*commonpb.UserId{debtors[3], creator}, page)
	require.Nil(t, next)

	// A hand mark replaced by a payment moves to the payment's paid_at.
	r.Share(debtors[1]).Paid = intentPaidAt(r, debtors[1], createdAt.Add(2*time.Hour))
	page, _ = r.PayersPage(PayerSectionPaid, nil, 10)
	requirePayers(t, []*commonpb.UserId{debtors[1], debtors[2], debtors[0], debtors[3], creator}, page)
	require.NotNil(t, page[0].GetPaid().GetIntent())

	require.Equal(t, PayerSectionPaid.String(), "paid")
	page, next = r.PayersPage(PayerSection(0), nil, 10)
	require.Empty(t, page)
	require.Nil(t, next)
}

func TestNewSplitRequest_NoText(t *testing.T) {
	createdAt := time.Now()
	creator := model.MustGenerateUserID()
	split := newSplit(createdAt.Add(time.Hour), creator, model.MustGenerateUserID())
	split.Text = nil

	r, err := NewSplitRequest(newID(), creator, newChatID(), split, createdAt)
	require.NoError(t, err)
	require.NoError(t, r.Validate())
	require.Nil(t, r.Split.Text)

	metadata := r.ToProto(createdAt)
	require.NoError(t, metadata.ValidateAll())
	require.Nil(t, metadata.GetGroupChat().GetSplit().GetText())
}

// walkPayers reads a whole section of r's Payers in pages of pageSize.
func walkPayers(t *testing.T, r *PaymentRequest, section PayerSection, pageSize int) []*paymentrequestpb.Payer {
	t.Helper()
	var (
		all    []*paymentrequestpb.Payer
		cursor *PayerCursor
	)
	for range 100 {
		page, next := r.PayersPage(section, cursor, pageSize)
		require.LessOrEqual(t, len(page), pageSize)
		all = append(all, page...)
		if next == nil {
			return all
		}
		require.Len(t, page, pageSize)
		cursor = next
	}
	t.Fatal("walk did not finish")
	return nil
}

func requirePayers(t *testing.T, want []*commonpb.UserId, got []*paymentrequestpb.Payer) {
	t.Helper()
	wantIDs := make([][]byte, len(want))
	for i, u := range want {
		wantIDs[i] = u.Value
	}
	gotIDs := make([][]byte, len(got))
	for i, p := range got {
		gotIDs[i] = p.UserId.Value
	}
	require.Equal(t, wantIDs, gotIDs)
}

func manualPaidAt(at time.Time) *paymentrequestpb.Paid {
	return &paymentrequestpb.Paid{
		PaidAt: timestamppb.New(at),
		How:    &paymentrequestpb.Paid_Manual_{Manual: &paymentrequestpb.Paid_Manual{}},
	}
}

func intentPaidAt(r *PaymentRequest, userID *commonpb.UserId, at time.Time) *paymentrequestpb.Paid {
	return &paymentrequestpb.Paid{
		PaidAt: timestamppb.New(at),
		How:    &paymentrequestpb.Paid_Intent{Intent: &commonpb.IntentId{Value: r.PaymentIntentID(userID)}},
	}
}

// TestPayerSummary_Collection pins a collection's summary: every contribution
// paid, nothing due, and the total summed exactly in minor units.
func TestPayerSummary_Collection(t *testing.T) {
	createdAt := time.Now()
	creator := model.MustGenerateUserID()
	r, err := NewCollectionRequest(newID(), creator, newChatID(), newCollection(nil, "usd", 0.1), createdAt)
	require.NoError(t, err)

	summary := r.PayerSummary()
	require.NoError(t, summary.Validate())
	require.True(t, proto.Equal(&paymentrequestpb.PayerSummary{}, summary))

	// 0.1 three times is 0.3 exactly, not 0.30000000000000004.
	r.PayerCount = 3
	summary = r.PayerSummary()
	require.EqualValues(t, 3, summary.PayerCount)
	require.EqualValues(t, 3, summary.PaidCount)
	require.Zero(t, summary.NativeAmountDue)
	require.Equal(t, 0.3, summary.NativeAmountPaid)

	r.PayerCount = 1_000_000
	r.Collection.GetFixed().NativeAmount = 12.34
	require.Equal(t, 12_340_000.0, r.PayerSummary().NativeAmountPaid)

	jpy, err := NewCollectionRequest(newID(), creator, newChatID(), newCollection(nil, "jpy", 500), createdAt)
	require.NoError(t, err)
	jpy.PayerCount = 7
	require.Equal(t, 3500.0, jpy.PayerSummary().NativeAmountPaid)
}

// TestContributionPayer pins a contribution's Payer, and that the share-based
// Payer methods find none in a collection.
func TestContributionPayer(t *testing.T) {
	createdAt := time.Now()
	creator := model.MustGenerateUserID()
	contributor := model.MustGenerateUserID()
	r, err := NewCollectionRequest(newID(), creator, newChatID(), newCollection(nil, "usd", 2.5), createdAt)
	require.NoError(t, err)

	paid := &paymentrequestpb.Paid{
		PaidAt: timestamppb.New(createdAt.Add(time.Minute)),
		How:    &paymentrequestpb.Paid_Intent{Intent: &commonpb.IntentId{Value: make([]byte, PaymentIntentIDSize)}},
	}
	payer := r.ContributionPayer(contributor, paid)
	require.NoError(t, payer.Validate())
	require.True(t, proto.Equal(contributor, payer.UserId))
	require.Zero(t, payer.NativeAmountDue)
	require.Equal(t, 2.5, payer.NativeAmountPaid)
	require.True(t, proto.Equal(paid, payer.GetPaid()))
	require.Nil(t, payer.GetUnpaid())

	// The Payer holds its own copies.
	paid.PaidAt = timestamppb.New(createdAt)
	require.False(t, proto.Equal(paid, payer.GetPaid()))

	require.True(t, proto.Equal(payer, r.ContributionPayer(contributor, payer.GetPaid())))
	cursor := PaidPayerCursor(payer)
	require.True(t, createdAt.Add(time.Minute).Equal(cursor.PaidAt))
	require.Equal(t, contributor.Value, cursor.UserID)

	require.Nil(t, r.Payer(contributor))
	require.Nil(t, r.Payer(creator))
	require.Empty(t, r.Payers())
	for _, section := range []PayerSection{PayerSectionUnpaid, PayerSectionPaid} {
		page, next := r.PayersPage(section, nil, 10)
		require.Empty(t, page)
		require.Nil(t, next)
	}

	// A split has no contributions.
	split, err := NewSplitRequest(newID(), creator, newChatID(), newSplit(createdAt.Add(time.Hour), creator, contributor), createdAt)
	require.NoError(t, err)
	require.Nil(t, split.ContributionPayer(contributor, paid))
}
