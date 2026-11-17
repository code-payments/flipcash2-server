package paymentrequest

import (
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"
	paymentrequestpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/paymentrequest/v1"

	"github.com/code-payments/flipcash2-server/model"
)

func TestMustDeriveID(t *testing.T) {
	creator := model.MustGenerateUserID()
	key := &paymentrequestpb.IdempotencyKey{Value: model.MustGenerateUserID().Value}

	id := MustDeriveID(creator, key)
	require.Len(t, id.Value, IDSize)
	require.Equal(t, byte(0x80), id.Value[6]&0xf0, "version 8")
	require.Equal(t, byte(0x80), id.Value[8]&0xc0, "RFC 9562 variant")

	// The same creator and key derive the same ID; another of either does not.
	require.True(t, proto.Equal(id, MustDeriveID(creator, key)))
	require.False(t, proto.Equal(id, MustDeriveID(model.MustGenerateUserID(), key)))
	require.False(t, proto.Equal(id, MustDeriveID(creator, &paymentrequestpb.IdempotencyKey{Value: model.MustGenerateUserID().Value})))

	require.Panics(t, func() { MustDeriveID(&commonpb.UserId{Value: []byte{1}}, key) })
	require.Panics(t, func() { MustDeriveID(creator, &paymentrequestpb.IdempotencyKey{Value: []byte{1}}) })
}

func TestNewSplitRequest(t *testing.T) {
	createdAt := time.Now()
	creator := model.MustGenerateUserID()
	debtor := model.MustGenerateUserID()

	r, err := NewSplitRequest(newID(), creator, newChatID(), newSplit(createdAt.Add(time.Hour), creator, debtor), createdAt)
	require.NoError(t, err)
	require.Equal(t, StatusOpen, r.Status)
	require.EqualValues(t, 1, r.Version)
	require.Nil(t, r.ClosedAt)

	// The creator's own share is paid from creation; the debtor's is not.
	own := r.CreatorShare()
	require.NotNil(t, own.Paid.GetCreator())
	require.True(t, createdAt.Equal(own.Paid.PaidAt.AsTime()))
	require.Nil(t, r.Share(debtor).Paid)
	require.False(t, r.AllSharesPaid())
	require.Nil(t, r.Share(model.MustGenerateUserID()))

	// Every debtor's share gets a random payment intent ID, its own; the
	// creator's, paid from creation, gets none.
	require.Len(t, r.PaymentIntentIDs, 1)
	require.Len(t, r.PaymentIntentID(debtor), PaymentIntentIDSize)
	require.Nil(t, r.PaymentIntentID(creator))
	again, err := NewSplitRequest(newID(), creator, newChatID(), newSplit(createdAt.Add(time.Hour), creator, debtor), createdAt)
	require.NoError(t, err)
	require.NotEqual(t, r.PaymentIntentID(debtor), again.PaymentIntentID(debtor))

	// A clone holds its own copy.
	clone := r.Clone()
	clone.PaymentIntentID(debtor)[0] ^= 0xff
	require.NotEqual(t, r.PaymentIntentID(debtor), clone.PaymentIntentID(debtor))

	// The split given is left as the client sent it.
	split := newSplit(createdAt.Add(time.Hour), creator, debtor)
	_, err = NewSplitRequest(newID(), creator, newChatID(), split, createdAt)
	require.NoError(t, err)
	require.Nil(t, split.Shares[0].Paid)

	// A creator need not list themself.
	r, err = NewSplitRequest(newID(), creator, newChatID(), newSplit(createdAt.Add(time.Hour), debtor), createdAt)
	require.NoError(t, err)
	require.Nil(t, r.CreatorShare())

	for name, split := range map[string]*paymentrequestpb.Split{
		"only the creator": newSplit(createdAt.Add(time.Hour), creator),
		"a repeated user":  newSplit(createdAt.Add(time.Hour), creator, debtor, debtor),
		"paid set": func() *paymentrequestpb.Split {
			split := newSplit(createdAt.Add(time.Hour), debtor)
			split.Shares[0].Paid = &paymentrequestpb.Paid{
				PaidAt: timestamppb.New(createdAt),
				How:    &paymentrequestpb.Paid_Manual_{Manual: &paymentrequestpb.Paid_Manual{}},
			}
			return split
		}(),
		"no shares": newSplit(createdAt.Add(time.Hour)),
	} {
		_, err := NewSplitRequest(newID(), creator, newChatID(), split, createdAt)
		require.ErrorIs(t, err, ErrInvalidShares, name)
	}

	// A DM is not a chat a request can be made in.
	_, err = NewSplitRequest(newID(), creator, &commonpb.ChatId{Value: make([]byte, 32)}, newSplit(createdAt.Add(time.Hour), debtor), createdAt)
	require.Error(t, err)
}

func TestValidate_CreatorPaidOnlyOnCreatorsShare(t *testing.T) {
	createdAt := time.Now()
	creator := model.MustGenerateUserID()
	debtor := model.MustGenerateUserID()

	r, err := NewSplitRequest(newID(), creator, newChatID(), newSplit(createdAt.Add(time.Hour), creator, debtor), createdAt)
	require.NoError(t, err)
	require.NoError(t, r.Validate())

	// A debtor's share is never paid as the creator's.
	bad := r.Clone()
	bad.Share(debtor).Paid = proto.Clone(bad.CreatorShare().Paid).(*paymentrequestpb.Paid)
	require.ErrorIs(t, bad.Validate(), ErrInvalidShares)

	// The creator's share is paid from creation, and at it.
	bad = r.Clone()
	bad.CreatorShare().Paid = nil
	require.ErrorIs(t, bad.Validate(), ErrInvalidShares)

	bad = r.Clone()
	bad.CreatorShare().Paid.PaidAt = timestamppb.New(createdAt.Add(time.Second))
	require.ErrorIs(t, bad.Validate(), ErrInvalidShares)

	bad = r.Clone()
	bad.Split = nil
	require.Error(t, bad.Validate())
}

func TestStatusAt(t *testing.T) {
	createdAt := time.Now()
	creator := model.MustGenerateUserID()
	r, err := NewSplitRequest(newID(), creator, newChatID(), newSplit(createdAt.Add(time.Hour), model.MustGenerateUserID()), createdAt)
	require.NoError(t, err)
	endsAt, hasEnd := r.EndsAt()
	require.True(t, hasEnd)

	// Open up to its end, and expired by the clock from it, at the same
	// version and closed at its end.
	before := endsAt.Add(-time.Nanosecond)
	require.Equal(t, StatusOpen, r.StatusAt(before))
	require.True(t, r.IsOpenAt(before))
	_, ok := r.ClosedAtAt(before)
	require.False(t, ok)

	state := r.ToProto(before)
	require.Equal(t, paymentrequestpb.PaymentRequestStatus_PAYMENT_REQUEST_STATUS_OPEN, state.Status)
	require.EqualValues(t, 1, state.Version)
	require.Nil(t, state.ClosedAt)

	require.Equal(t, StatusExpired, r.StatusAt(endsAt))
	require.False(t, r.IsOpenAt(endsAt))
	closedAt, ok := r.ClosedAtAt(endsAt)
	require.True(t, ok)
	require.True(t, endsAt.Equal(closedAt))

	clockExpired := r.ToProto(endsAt)
	require.Equal(t, paymentrequestpb.PaymentRequestStatus_PAYMENT_REQUEST_STATUS_EXPIRED, clockExpired.Status)
	require.EqualValues(t, 1, clockExpired.Version)
	require.True(t, endsAt.Equal(clockExpired.ClosedAt.AsTime()))

	// What the expiry worker stores reads exactly as the clock did.
	stored := r.Clone()
	stored.Status = StatusExpired
	stored.ClosedAt = &endsAt
	require.True(t, proto.Equal(clockExpired, stored.ToProto(endsAt)))

	// A final status stands whatever the clock says, closed when it closed.
	closed := r.Clone()
	closedAt = createdAt.Add(time.Minute)
	closed.Status = StatusClosed
	closed.ClosedAt = &closedAt
	closed.Version = 2
	for _, now := range []time.Time{before, endsAt.Add(time.Hour)} {
		require.Equal(t, StatusClosed, closed.StatusAt(now))
		got, ok := closed.ClosedAtAt(now)
		require.True(t, ok)
		require.True(t, closedAt.Equal(got))
	}
}

func TestToProto(t *testing.T) {
	createdAt := time.Now()
	creator := model.MustGenerateUserID()
	chatID := newChatID()
	split := newSplit(createdAt.Add(time.Hour), creator, model.MustGenerateUserID())
	r, err := NewSplitRequest(newID(), creator, chatID, split, createdAt)
	require.NoError(t, err)

	metadata := r.ToProto(createdAt)
	require.NoError(t, metadata.Validate())
	require.True(t, proto.Equal(r.ID, metadata.PaymentRequestId))
	require.True(t, proto.Equal(creator, metadata.Creator))
	require.True(t, createdAt.Equal(metadata.CreatedAt.AsTime()))
	require.True(t, proto.Equal(chatID, metadata.GetGroupChat().ChatId))
	require.True(t, proto.Equal(r.Split, metadata.GetGroupChat().GetSplit()))
	require.Equal(t, paymentrequestpb.PaymentRequestStatus_PAYMENT_REQUEST_STATUS_OPEN, metadata.Status)
	require.EqualValues(t, 1, metadata.Version)
	require.Nil(t, metadata.ClosedAt)

	// The projection is a copy.
	metadata.GetGroupChat().GetSplit().Shares[1].NativeAmount = 99
	require.NotEqual(t, 99.0, r.Split.Shares[1].NativeAmount)
}

func newID() *paymentrequestpb.PaymentRequestId {
	return MustDeriveID(model.MustGenerateUserID(), &paymentrequestpb.IdempotencyKey{Value: model.MustGenerateUserID().Value})
}

func newChatID() *commonpb.ChatId {
	return &commonpb.ChatId{Value: model.MustGenerateUserID().Value}
}

func newSplit(endsAt time.Time, users ...*commonpb.UserId) *paymentrequestpb.Split {
	shares := make([]*paymentrequestpb.Share, len(users))
	for i, user := range users {
		shares[i] = &paymentrequestpb.Share{UserId: user, NativeAmount: 1.25}
	}
	return &paymentrequestpb.Split{
		Text: &paymentrequestpb.Split_Plaintext{Plaintext: &paymentrequestpb.PaymentRequestText{
			Title: "Lunch",
		}},
		Currency: "usd",
		EndsAt:   timestamppb.New(endsAt),
		Shares:   shares,
	}
}

func TestNewCollectionRequest(t *testing.T) {
	createdAt := time.Now()
	creator := model.MustGenerateUserID()
	endsAt := createdAt.Add(time.Hour)

	for name, collection := range map[string]*paymentrequestpb.Collection{
		"dated":   newCollection(&endsAt, "usd", 12.34),
		"undated": newCollection(nil, "usd", 12.34),
		"no text": func() *paymentrequestpb.Collection {
			c := newCollection(nil, "usd", 0.01)
			c.Text = nil
			return c
		}(),
		"no minor unit": newCollection(nil, "jpy", 500),
		"float error":   newCollection(nil, "usd", 0.1+0.2),
	} {
		r, err := NewCollectionRequest(newID(), creator, newChatID(), collection, createdAt)
		require.NoError(t, err, name)
		require.True(t, r.IsCollection(), name)
		require.Nil(t, r.Split, name)
		require.Equal(t, StatusOpen, r.Status, name)
		require.EqualValues(t, 1, r.Version, name)
		require.Nil(t, r.ClosedAt, name)
		require.Zero(t, r.PayerCount, name)
		require.Empty(t, r.PaymentIntentIDs, name)
		require.Empty(t, r.Shares(), name)
		require.Nil(t, r.Share(creator), name)
		require.True(t, proto.Equal(collection, r.Collection), name)

		// The collection given is the request's own copy.
		collection.Currency = "cad"
		require.NotEqual(t, "cad", r.Collection.Currency, name)
	}

	// The amount must be a whole number, at least one, of the currency's
	// minor unit.
	for name, collection := range map[string]*paymentrequestpb.Collection{
		"zero":           newCollection(nil, "usd", 0),
		"under a cent":   newCollection(nil, "usd", 0.005),
		"between cents":  newCollection(nil, "usd", 12.345),
		"negative":       newCollection(nil, "usd", -1),
		"nan":            newCollection(nil, "usd", math.NaN()),
		"inf":            newCollection(nil, "usd", math.Inf(1)),
		"no minor units": newCollection(nil, "jpy", 1.5),
		"no amount": func() *paymentrequestpb.Collection {
			c := newCollection(nil, "usd", 1)
			c.PaymentAmount = nil
			return c
		}(),
	} {
		_, err := NewCollectionRequest(newID(), creator, newChatID(), collection, createdAt)
		require.ErrorIs(t, err, ErrInvalidAmount, name)
	}

	// The currency must be set.
	_, err := NewCollectionRequest(newID(), creator, newChatID(), newCollection(nil, "", 1), createdAt)
	require.Error(t, err)
	require.NotErrorIs(t, err, ErrInvalidAmount)
}

func TestValidate_Collection(t *testing.T) {
	createdAt := time.Now()
	creator := model.MustGenerateUserID()
	r, err := NewCollectionRequest(newID(), creator, newChatID(), newCollection(nil, "usd", 5), createdAt)
	require.NoError(t, err)
	require.NoError(t, r.Validate())

	both := r.Clone()
	both.Split = newSplit(createdAt.Add(time.Hour), creator, model.MustGenerateUserID())
	require.Error(t, both.Validate())

	withIntents := r.Clone()
	withIntents.PaymentIntentIDs = map[string][]byte{string(model.MustGenerateUserID().Value): make([]byte, PaymentIntentIDSize)}
	require.Error(t, withIntents.Validate())

	nonPositive := r.Clone()
	nonPositive.Collection.GetFixed().NativeAmount = 0
	require.ErrorIs(t, nonPositive.Validate(), ErrInvalidAmount)

	noCurrency := r.Clone()
	noCurrency.Collection.Currency = ""
	require.Error(t, noCurrency.Validate())
}

// TestStatusAt_Undated pins that a request with no ends_at is open until
// closed, whatever the clock says, and is never queued.
func TestStatusAt_Undated(t *testing.T) {
	createdAt := time.Now()
	r, err := NewCollectionRequest(newID(), model.MustGenerateUserID(), newChatID(), newCollection(nil, "usd", 5), createdAt)
	require.NoError(t, err)

	_, ok := r.EndsAt()
	require.False(t, ok)
	require.True(t, ExpiryDueAt(r).IsZero())

	for _, now := range []time.Time{createdAt, createdAt.Add(100 * 365 * 24 * time.Hour)} {
		require.Equal(t, StatusOpen, r.StatusAt(now))
		require.True(t, r.IsOpenAt(now))
		_, ok := r.ClosedAtAt(now)
		require.False(t, ok)

		metadata := r.ToProto(now)
		require.Equal(t, paymentrequestpb.PaymentRequestStatus_PAYMENT_REQUEST_STATUS_OPEN, metadata.Status)
		require.Nil(t, metadata.ClosedAt)
	}

	closedAt := createdAt.Add(time.Minute)
	closed := r.Clone()
	closed.Status = StatusClosed
	closed.ClosedAt = &closedAt
	require.Equal(t, StatusClosed, closed.StatusAt(createdAt.Add(time.Hour)))
	got, ok := closed.ClosedAtAt(createdAt.Add(time.Hour))
	require.True(t, ok)
	require.True(t, closedAt.Equal(got))

	// A dated collection expires by the clock like a split.
	endsAt := createdAt.Add(time.Hour)
	dated, err := NewCollectionRequest(newID(), model.MustGenerateUserID(), newChatID(), newCollection(&endsAt, "usd", 5), createdAt)
	require.NoError(t, err)
	gotEnd, ok := dated.EndsAt()
	require.True(t, ok)
	require.True(t, endsAt.Equal(gotEnd))
	require.True(t, endsAt.Add(ExpireDelay).Equal(ExpiryDueAt(dated)))
	require.True(t, dated.IsOpenAt(endsAt.Add(-time.Nanosecond)))
	require.Equal(t, StatusExpired, dated.StatusAt(endsAt))
	got, ok = dated.ClosedAtAt(endsAt)
	require.True(t, ok)
	require.True(t, endsAt.Equal(got))
}

func TestToProto_Collection(t *testing.T) {
	createdAt := time.Now()
	creator := model.MustGenerateUserID()
	chatID := newChatID()
	for _, endsAt := range []*time.Time{nil, func() *time.Time { at := createdAt.Add(time.Hour); return &at }()} {
		r, err := NewCollectionRequest(newID(), creator, chatID, newCollection(endsAt, "usd", 2.5), createdAt)
		require.NoError(t, err)
		r.PayerCount = 3
		r.Version = 4

		metadata := r.ToProto(createdAt)
		require.NoError(t, metadata.Validate())
		require.True(t, proto.Equal(chatID, metadata.GetGroupChat().ChatId))
		require.Nil(t, metadata.GetGroupChat().GetSplit())
		require.True(t, proto.Equal(r.Collection, metadata.GetGroupChat().GetCollection()))
		require.Equal(t, endsAt == nil, metadata.GetGroupChat().GetCollection().EndsAt == nil)
		require.EqualValues(t, 4, metadata.Version)
		require.True(t, proto.Equal(&paymentrequestpb.PayerSummary{
			PayerCount:       3,
			PaidCount:        3,
			NativeAmountPaid: 7.5,
		}, metadata.PayerSummary))

		// The projection is a copy, and so is a clone.
		metadata.GetGroupChat().GetCollection().GetFixed().NativeAmount = 99
		require.Equal(t, 2.5, r.Collection.GetFixed().NativeAmount)
		clone := r.Clone()
		require.EqualValues(t, 3, clone.PayerCount)
		clone.Collection.GetFixed().NativeAmount = 99
		require.Equal(t, 2.5, r.Collection.GetFixed().NativeAmount)
	}
}

func newCollection(endsAt *time.Time, currency string, amount float64) *paymentrequestpb.Collection {
	collection := &paymentrequestpb.Collection{
		Text: &paymentrequestpb.Collection_Plaintext{Plaintext: &paymentrequestpb.PaymentRequestText{
			Title: "Gift",
		}},
		Currency: currency,
		PaymentAmount: &paymentrequestpb.Collection_Fixed{Fixed: &paymentrequestpb.Collection_FixedAmount{
			NativeAmount: amount,
		}},
	}
	if endsAt != nil {
		collection.EndsAt = timestamppb.New(*endsAt)
	}
	return collection
}
