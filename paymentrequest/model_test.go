package paymentrequest

import (
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
			split.Shares[0].Paid = &paymentrequestpb.PaidShare{
				PaidAt: timestamppb.New(createdAt),
				How:    &paymentrequestpb.PaidShare_Manual_{Manual: &paymentrequestpb.PaidShare_Manual{}},
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
	bad.Share(debtor).Paid = proto.Clone(bad.CreatorShare().Paid).(*paymentrequestpb.PaidShare)
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
	endsAt := r.EndsAt()

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
