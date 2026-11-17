package tests

import (
	"bytes"
	"context"
	"math"
	"sync"
	"testing"
	"time"

	"github.com/mr-tron/base58"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	chatpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/chat/v1"
	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"
	eventpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/event/v1"
	moderationpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/moderation/v1"
	paymentrequestpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/paymentrequest/v1"
	ocp_balancepb "github.com/code-payments/ocp-protobuf-api/generated/go/balance/v1"

	ocp_common "github.com/code-payments/ocp-server/ocp/common"

	"github.com/code-payments/flipcash2-server/account"
	accountmemory "github.com/code-payments/flipcash2-server/account/memory"
	"github.com/code-payments/flipcash2-server/auth"
	"github.com/code-payments/flipcash2-server/balance"
	"github.com/code-payments/flipcash2-server/chat"
	chatmemory "github.com/code-payments/flipcash2-server/chat/memory"
	"github.com/code-payments/flipcash2-server/event"
	"github.com/code-payments/flipcash2-server/model"
	"github.com/code-payments/flipcash2-server/moderation"
	"github.com/code-payments/flipcash2-server/paymentrequest"
	"github.com/code-payments/flipcash2-server/protoutil"
	"github.com/code-payments/flipcash2-server/testutil"
)

// RunServerTests runs the shared paymentrequest.Server test suite against s;
// teardown resets it between tests. Every test builds its own in-memory chat
// and account stores.
func RunServerTests(t *testing.T, s paymentrequest.Store, teardown func()) {
	for _, tf := range []func(t *testing.T, s paymentrequest.Store){
		testServer_Create_PrivateGroup,
		testServer_Create_Idempotent,
		testServer_Create_Refused,
		testServer_Create_Order,
		testServer_Create_Amount,
		testServer_Create_EndsAt,
		testServer_Create_PublicGroup,
		testServer_Create_Moderation,
		testServer_Create_NoText,
		testServer_Create_EmptyText,
		testServer_Create_Collection,
		testServer_Create_Collection_Refused,
		testServer_Create_Collection_PublicGroup,
		testServer_Create_Collection_Idempotent,
		testServer_Collection_Get,
		testServer_Collection_GetPaymentIntentId,
		testServer_Collection_GetPaymentIntentId_PublicGroup,
		testServer_Collection_GetPayers,
		testServer_Collection_GetPayers_VersionBeforePage,
		testServer_Collection_MarkPaid,
		testServer_Collection_Close,
		testServer_Collection_List,
		testServer_Collection_Updates,
		testServer_Get,
		testServer_Get_PaymentIntentID,
		testServer_Get_Payers,
		testServer_GetPaymentIntentId,
		testServer_GetPayers,
		testServer_GetPayers_Order,
		testServer_List_Gates,
		testServer_List_Paging,
		testServer_List_ClockExpired,
		testServer_Close,
		testServer_MarkPaid,
		testServer_Updates,
	} {
		tf(t, s)
		teardown()
	}
}

// testServer_Create_PrivateGroup pins a request in a keyed private group: its
// text encrypted under the chat key and never moderated, the creator's own
// share paid from creation, and the request open at version 1.
func testServer_Create_PrivateGroup(t *testing.T, s paymentrequest.Store) {
	e := newServerEnv(t, s)
	alice := e.addUser()
	bob := e.addUser()
	chatID := e.putPrivateGroup(true, alice, bob)

	split := e.encryptedSplit(e.clock.Now().Add(time.Hour),
		share(e.creator, 10),
		share(alice, 12.5),
		share(bob, 7.25),
	)
	resp := e.create(e.creator, chatID, split, newKey())
	require.Equal(t, paymentrequestpb.CreatePaymentRequestResponse_OK, resp.Result)
	require.Equal(t, moderationpb.FlaggedCategory_NONE, resp.FlaggedCategory)
	require.Zero(t, e.moderator.classifiedCount())

	created := resp.PaymentRequest
	require.Equal(t, e.creator.id.Value, created.Creator.Value)
	require.True(t, e.clock.Now().Equal(created.CreatedAt.AsTime()))
	requireOpen(t, created, 1)

	got := created.GetGroupChat()
	require.NotNil(t, got)
	require.Equal(t, chatID.Value, got.ChatId.Value)
	gotSplit := got.GetSplit()
	require.True(t, proto.Equal(split.Text.(*paymentrequestpb.Split_Encrypted).Encrypted, gotSplit.GetEncrypted()))
	require.Equal(t, "usd", gotSplit.Currency)
	require.True(t, split.EndsAt.AsTime().Equal(gotSplit.EndsAt.AsTime()))
	require.Len(t, gotSplit.Shares, 3)

	// The creator's own share is paid from creation, at created_at; the
	// others are unpaid, in the order listed.
	own := gotSplit.Shares[0]
	require.Equal(t, e.creator.id.Value, own.UserId.Value)
	require.NotNil(t, own.Paid.GetCreator())
	require.True(t, created.CreatedAt.AsTime().Equal(own.Paid.PaidAt.AsTime()))
	require.Equal(t, alice.id.Value, gotSplit.Shares[1].UserId.Value)
	require.Equal(t, 12.5, gotSplit.Shares[1].NativeAmount)
	require.Nil(t, gotSplit.Shares[1].Paid)
	require.Equal(t, bob.id.Value, gotSplit.Shares[2].UserId.Value)
	require.Nil(t, gotSplit.Shares[2].Paid)

	stored, err := s.Get(e.ctx, created.PaymentRequestId)
	require.NoError(t, err)
	require.True(t, proto.Equal(created, stored.ToProto(e.clock.Now())))

	// The creator need not list a share of their own.
	resp = e.create(e.creator, chatID, e.encryptedSplit(e.clock.Now().Add(time.Hour), share(alice, 1)), newKey())
	require.Equal(t, paymentrequestpb.CreatePaymentRequestResponse_OK, resp.Result)
	require.Len(t, resp.PaymentRequest.GetGroupChat().GetSplit().Shares, 1)
	require.Nil(t, resp.PaymentRequest.GetGroupChat().GetSplit().Shares[0].Paid)

	// Any member who can speak creates one, not just the group's creator.
	resp = e.create(alice, chatID, e.encryptedSplit(e.clock.Now().Add(time.Hour), share(bob, 1), share(e.creator, 2)), newKey())
	require.Equal(t, paymentrequestpb.CreatePaymentRequestResponse_OK, resp.Result)
	require.Equal(t, alice.id.Value, resp.PaymentRequest.Creator.Value)
}

func testServer_Create_Idempotent(t *testing.T, s paymentrequest.Store) {
	e := newServerEnv(t, s)
	alice := e.addUser()
	chatID := e.putPrivateGroup(true, alice)

	key := newKey()
	first := e.create(e.creator, chatID, e.encryptedSplit(e.clock.Now().Add(time.Hour), share(alice, 5)), key)
	require.Equal(t, paymentrequestpb.CreatePaymentRequestResponse_OK, first.Result)

	// A retry is answered with the original request whatever it carries, even
	// a DM, an ends_at in the past and shares that would be refused: nothing
	// is checked again.
	e.clock.advance(time.Minute)
	dmID := &commonpb.ChatId{Value: bytes.Repeat([]byte{1}, chat.DmChatIDSize)}
	retried := e.encryptedSplit(e.clock.Now().Add(-time.Hour), share(e.creator, 5))
	again := e.create(e.creator, dmID, retried, key)
	require.Equal(t, paymentrequestpb.CreatePaymentRequestResponse_OK, again.Result)
	require.True(t, proto.Equal(first.PaymentRequest, again.PaymentRequest))

	// The same key from someone else is someone else's request.
	theirs := e.create(alice, chatID, e.encryptedSplit(e.clock.Now().Add(time.Hour), share(e.creator, 5)), key)
	require.Equal(t, paymentrequestpb.CreatePaymentRequestResponse_OK, theirs.Result)
	require.NotEqual(t, first.PaymentRequest.PaymentRequestId.Value, theirs.PaymentRequest.PaymentRequestId.Value)
	require.Equal(t, alice.id.Value, theirs.PaymentRequest.Creator.Value)

	// A retry that finds the request already stored but missed it on its
	// read (a twin's write landing in between) is answered the same way: the
	// store's uniqueness condition catches it.
	key = newKey()
	split := e.encryptedSplit(e.clock.Now().Add(time.Hour), share(alice, 5))
	twin, err := paymentrequest.NewSplitRequest(paymentrequest.MustDeriveID(e.creator.id, key), e.creator.id, chatID, split, e.clock.Now())
	require.NoError(t, err)
	racing := &racingStore{Store: s, beforeCreate: func() { assert.NoError(t, s.Create(e.ctx, twin)) }}
	re := newServerEnvWith(t, racing, e)
	resp := re.create(e.creator, chatID, split, key)
	require.Equal(t, paymentrequestpb.CreatePaymentRequestResponse_OK, resp.Result)
	require.True(t, proto.Equal(twin.ToProto(e.clock.Now()), resp.PaymentRequest))
}

func testServer_Create_Refused(t *testing.T, s paymentrequest.Store) {
	e := newServerEnv(t, s)
	member := e.addUser()
	other := e.addUser()
	outsider := e.addUser()
	nonMember := e.addUser()

	keyed := e.putPrivateGroup(true, member, other)
	keyless := e.putPrivateGroup(false, member, other)
	public := e.putPublicGroup(nil, member, other)
	dmID := &commonpb.ChatId{Value: bytes.Repeat([]byte{1}, chat.DmChatIDSize)}

	endsAt := e.clock.Now().Add(time.Hour)
	valid := func() *paymentrequestpb.Split {
		return e.encryptedSplit(endsAt, share(e.creator, 1), share(member, 2))
	}
	with := func(configure func(*paymentrequestpb.Split)) *paymentrequestpb.Split {
		split := valid()
		configure(split)
		return split
	}

	for _, tc := range []struct {
		name   string
		user   user
		chatID *commonpb.ChatId
		split  *paymentrequestpb.Split
		want   paymentrequestpb.CreatePaymentRequestResponse_Result
	}{
		{"dm", e.creator, dmID, valid(), paymentrequestpb.CreatePaymentRequestResponse_CHAT_NOT_SUPPORTED},
		{"non-member", nonMember, keyed, valid(), paymentrequestpb.CreatePaymentRequestResponse_DENIED},
		{"missing chat", e.creator, chat.MustGenerateGroupChatID(), valid(), paymentrequestpb.CreatePaymentRequestResponse_DENIED},
		{"keyless private group", e.creator, keyless, valid(), paymentrequestpb.CreatePaymentRequestResponse_DENIED},
		{"public group", e.creator, public, with(func(s *paymentrequestpb.Split) { s.Text = plaintext("Dinner", "") }), paymentrequestpb.CreatePaymentRequestResponse_CHAT_NOT_SUPPORTED},
		{"plaintext in private group", e.creator, keyed, with(func(s *paymentrequestpb.Split) { s.Text = plaintext("Dinner", "") }), paymentrequestpb.CreatePaymentRequestResponse_ENCRYPTION_REQUIRED},
		{"ends now", e.creator, keyed, with(func(s *paymentrequestpb.Split) { s.EndsAt = timestamppb.New(e.clock.Now()) }), paymentrequestpb.CreatePaymentRequestResponse_INVALID_ENDS_AT},
		{"ended", e.creator, keyed, with(func(s *paymentrequestpb.Split) { s.EndsAt = timestamppb.New(e.clock.Now().Add(-time.Second)) }), paymentrequestpb.CreatePaymentRequestResponse_INVALID_ENDS_AT},
		{"unsupported currency", e.creator, keyed, with(func(s *paymentrequestpb.Split) { s.Currency = "xyz" }), paymentrequestpb.CreatePaymentRequestResponse_INVALID_CURRENCY},
		{"share paid", e.creator, keyed, with(func(s *paymentrequestpb.Split) {
			s.Shares[1].Paid = &paymentrequestpb.Paid{
				PaidAt: timestamppb.New(e.clock.Now()),
				How:    &paymentrequestpb.Paid_Manual_{Manual: &paymentrequestpb.Paid_Manual{}},
			}
		}), paymentrequestpb.CreatePaymentRequestResponse_INVALID_SHARES},
		{"creator's share paid", e.creator, keyed, with(func(s *paymentrequestpb.Split) {
			s.Shares[0].Paid = &paymentrequestpb.Paid{
				PaidAt: timestamppb.New(e.clock.Now()),
				How:    &paymentrequestpb.Paid_Creator_{Creator: &paymentrequestpb.Paid_Creator{}},
			}
		}), paymentrequestpb.CreatePaymentRequestResponse_INVALID_SHARES},
		{"repeated user", e.creator, keyed, with(func(s *paymentrequestpb.Split) { s.Shares = append(s.Shares, share(member, 3)) }), paymentrequestpb.CreatePaymentRequestResponse_INVALID_SHARES},
		{"only the creator", e.creator, keyed, with(func(s *paymentrequestpb.Split) { s.Shares = s.Shares[:1] }), paymentrequestpb.CreatePaymentRequestResponse_INVALID_SHARES},
		{"non-member share", e.creator, keyed, with(func(s *paymentrequestpb.Split) { s.Shares = append(s.Shares, share(outsider, 3)) }), paymentrequestpb.CreatePaymentRequestResponse_INVALID_SHARES},
		{"below minimum", e.creator, keyed, with(func(s *paymentrequestpb.Split) { s.Shares[1].NativeAmount = 0.004 }), paymentrequestpb.CreatePaymentRequestResponse_INVALID_SHARES},
		{"creator's share below minimum", e.creator, keyed, with(func(s *paymentrequestpb.Split) { s.Shares[0].NativeAmount = 0 }), paymentrequestpb.CreatePaymentRequestResponse_INVALID_SHARES},
		{"not a number", e.creator, keyed, with(func(s *paymentrequestpb.Split) { s.Shares[1].NativeAmount = math.NaN() }), paymentrequestpb.CreatePaymentRequestResponse_INVALID_SHARES},
		{"infinite", e.creator, keyed, with(func(s *paymentrequestpb.Split) { s.Shares[1].NativeAmount = math.Inf(1) }), paymentrequestpb.CreatePaymentRequestResponse_INVALID_SHARES},
	} {
		key := newKey()
		resp := e.create(tc.user, tc.chatID, tc.split, key)
		require.Equal(t, tc.want, resp.Result, tc.name)
		require.Nil(t, resp.PaymentRequest, tc.name)
		require.Equal(t, moderationpb.FlaggedCategory_NONE, resp.FlaggedCategory, tc.name)

		_, err := s.Get(e.ctx, paymentrequest.MustDeriveID(tc.user.id, key))
		require.ErrorIs(t, err, paymentrequest.ErrNotFound, tc.name)
	}
	require.Zero(t, e.moderator.classifiedCount())
}

// testServer_Create_Order pins the order refusals are judged in, where a
// request fails more than one check.
func testServer_Create_Order(t *testing.T, s paymentrequest.Store) {
	e := newServerEnv(t, s, paymentrequest.WithPublicGroupRequests())
	member := e.addUser()
	outsider := e.addUser()
	keyed := e.putPrivateGroup(true, member)
	dmID := &commonpb.ChatId{Value: bytes.Repeat([]byte{1}, chat.DmChatIDSize)}

	e.moderator.flag("Bad", "impersonation")

	past := e.clock.Now().Add(-time.Hour)
	future := e.clock.Now().Add(time.Hour)
	badShares := []*paymentrequestpb.Share{share(e.creator, 1)}

	for _, tc := range []struct {
		name   string
		user   user
		chatID *commonpb.ChatId
		split  *paymentrequestpb.Split
		want   paymentrequestpb.CreatePaymentRequestResponse_Result
	}{
		{"dm, everything else wrong", e.creator, dmID, plaintextSplit(past, "Bad", "", badShares...), paymentrequestpb.CreatePaymentRequestResponse_CHAT_NOT_SUPPORTED},
		{"non-member, everything else wrong", outsider, keyed, plaintextSplit(past, "Bad", "", badShares...), paymentrequestpb.CreatePaymentRequestResponse_DENIED},
		{"text, then ends_at", e.creator, keyed, plaintextSplit(past, "Bad", "", badShares...), paymentrequestpb.CreatePaymentRequestResponse_ENCRYPTION_REQUIRED},
		{"ends_at, then currency", e.creator, keyed, withCurrency(e.encryptedSplit(past, badShares...), "xyz"), paymentrequestpb.CreatePaymentRequestResponse_INVALID_ENDS_AT},
		{"currency, then shares", e.creator, keyed, withCurrency(e.encryptedSplit(future, badShares...), "xyz"), paymentrequestpb.CreatePaymentRequestResponse_INVALID_CURRENCY},
	} {
		resp := e.create(tc.user, tc.chatID, tc.split, newKey())
		require.Equal(t, tc.want, resp.Result, tc.name)
	}

	// Shares are judged before moderation, which is the last check and never
	// reached by a request refused earlier.
	public := e.putPublicGroup(nil, member)
	resp := e.create(e.creator, public, plaintextSplit(future, "Bad", "", share(outsider, 1)), newKey())
	require.Equal(t, paymentrequestpb.CreatePaymentRequestResponse_INVALID_SHARES, resp.Result)
	require.Zero(t, e.moderator.classifiedCount())

	resp = e.create(e.creator, public, plaintextSplit(future, "Bad", "", share(member, 1)), newKey())
	require.Equal(t, paymentrequestpb.CreatePaymentRequestResponse_TEXT_MODERATED, resp.Result)
	require.Equal(t, 1, e.moderator.classifiedCount())
}

// testServer_Create_Amount pins what a share may ask: a whole number, at least
// one, of the currency's minimum transfer value (its minor unit), with float
// error alone forgiven.
func testServer_Create_Amount(t *testing.T, s paymentrequest.Store) {
	e := newServerEnv(t, s)
	member := e.addUser()
	chatID := e.putPrivateGroup(true, member)

	for _, tc := range []struct {
		currency string
		amount   float64
		want     paymentrequestpb.CreatePaymentRequestResponse_Result
	}{
		{"usd", 0.01, paymentrequestpb.CreatePaymentRequestResponse_OK},
		{"usd", 12.34, paymentrequestpb.CreatePaymentRequestResponse_OK},
		{"usd", 0.1 + 0.2, paymentrequestpb.CreatePaymentRequestResponse_OK},
		{"usd", 99999999.99, paymentrequestpb.CreatePaymentRequestResponse_OK},
		{"usd", 0.005, paymentrequestpb.CreatePaymentRequestResponse_INVALID_SHARES},
		{"usd", 0.0049, paymentrequestpb.CreatePaymentRequestResponse_INVALID_SHARES},
		{"usd", 0.015, paymentrequestpb.CreatePaymentRequestResponse_INVALID_SHARES},
		{"usd", 1.004, paymentrequestpb.CreatePaymentRequestResponse_INVALID_SHARES},
		{"usd", 0, paymentrequestpb.CreatePaymentRequestResponse_INVALID_SHARES},
		// A currency with no minor unit.
		{"jpy", 1, paymentrequestpb.CreatePaymentRequestResponse_OK},
		{"jpy", 1000, paymentrequestpb.CreatePaymentRequestResponse_OK},
		{"jpy", 0.5, paymentrequestpb.CreatePaymentRequestResponse_INVALID_SHARES},
		{"jpy", 1.5, paymentrequestpb.CreatePaymentRequestResponse_INVALID_SHARES},
	} {
		split := withCurrency(e.encryptedSplit(e.clock.Now().Add(time.Hour), share(member, tc.amount)), tc.currency)
		resp := e.create(e.creator, chatID, split, newKey())
		require.Equal(t, tc.want, resp.Result, "%s %v", tc.currency, tc.amount)
	}
}

// testServer_Create_EndsAt pins the bounds on a split's ends_at: after now and
// at most paymentrequest.MaxDuration away, so a far-off ends_at (past what Unix
// nanos can hold, say) never reaches a store.
func testServer_Create_EndsAt(t *testing.T, s paymentrequest.Store) {
	e := newServerEnv(t, s)
	member := e.addUser()
	chatID := e.putPrivateGroup(true, member)

	now := e.clock.Now()
	for _, tc := range []struct {
		name   string
		endsAt time.Time
		want   paymentrequestpb.CreatePaymentRequestResponse_Result
	}{
		{"just after now", now.Add(time.Nanosecond), paymentrequestpb.CreatePaymentRequestResponse_OK},
		{"at the maximum", now.Add(paymentrequest.MaxDuration), paymentrequestpb.CreatePaymentRequestResponse_OK},
		{"now", now, paymentrequestpb.CreatePaymentRequestResponse_INVALID_ENDS_AT},
		{"past the maximum", now.Add(paymentrequest.MaxDuration + time.Nanosecond), paymentrequestpb.CreatePaymentRequestResponse_INVALID_ENDS_AT},
		{"past unix nanos", time.Date(3000, time.January, 1, 0, 0, 0, 0, time.UTC), paymentrequestpb.CreatePaymentRequestResponse_INVALID_ENDS_AT},
		{"latest timestamp", time.Date(9999, time.December, 31, 23, 59, 59, 0, time.UTC), paymentrequestpb.CreatePaymentRequestResponse_INVALID_ENDS_AT},
	} {
		resp := e.create(e.creator, chatID, e.encryptedSplit(tc.endsAt, share(member, 1)), newKey())
		require.Equal(t, tc.want, resp.Result, tc.name)
		if tc.want == paymentrequestpb.CreatePaymentRequestResponse_OK {
			require.True(t, tc.endsAt.Equal(resp.PaymentRequest.GetGroupChat().GetSplit().GetEndsAt().AsTime()), tc.name)
		}
	}
}

// testServer_Create_PublicGroup pins public groups: refused for now, whoever
// asks, and under the option that lifts that, plaintext alone.
func testServer_Create_PublicGroup(t *testing.T, s paymentrequest.Store) {
	{
		e := newServerEnv(t, s)
		member := e.addUser()
		chatID := e.putPublicGroup(nil, member)
		resp := e.create(e.creator, chatID, plaintextSplit(e.clock.Now().Add(time.Hour), "Dinner", "", share(member, 1)), newKey())
		require.Equal(t, paymentrequestpb.CreatePaymentRequestResponse_CHAT_NOT_SUPPORTED, resp.Result)
		require.Zero(t, e.moderator.classifiedCount())
	}

	e := newServerEnv(t, s, paymentrequest.WithPublicGroupRequests())
	member := e.addUser()
	chatID := e.putPublicGroup(nil, member)

	split := plaintextSplit(e.clock.Now().Add(time.Hour), "Dinner", "Friday night", share(e.creator, 4), share(member, 6))
	resp := e.create(e.creator, chatID, split, newKey())
	require.Equal(t, paymentrequestpb.CreatePaymentRequestResponse_OK, resp.Result)
	requireOpen(t, resp.PaymentRequest, 1)
	require.True(t, proto.Equal(split.GetPlaintext(), resp.PaymentRequest.GetGroupChat().GetSplit().GetPlaintext()))

	// Encrypted text is not taken in a public group.
	encrypted := e.encryptedSplit(e.clock.Now().Add(time.Hour), share(member, 1))
	require.Equal(t, paymentrequestpb.CreatePaymentRequestResponse_ENCRYPTION_NOT_ALLOWED, e.create(e.creator, chatID, encrypted, newKey()).Result)

	// Any member who can speak creates one, not just the group's creator.
	require.Equal(t, paymentrequestpb.CreatePaymentRequestResponse_OK, e.create(member, chatID, plaintextSplit(e.clock.Now().Add(time.Hour), "Dinner", "", share(e.creator, 1)), newKey()).Result)

	// A creator-only group lets nobody but its creator speak, so nobody but
	// its creator creates a request in it.
	creatorOnly := e.putPublicGroup(func(c *chat.Chat) { c.IsCreatorOnlySpeaker = true }, member)
	require.Equal(t, paymentrequestpb.CreatePaymentRequestResponse_DENIED, e.create(member, creatorOnly, plaintextSplit(e.clock.Now().Add(time.Hour), "Dinner", "", share(e.creator, 1)), newKey()).Result)
	require.Equal(t, paymentrequestpb.CreatePaymentRequestResponse_OK, e.create(e.creator, creatorOnly, plaintextSplit(e.clock.Now().Add(time.Hour), "Dinner", "", share(member, 1)), newKey()).Result)
}

func testServer_Create_Moderation(t *testing.T, s paymentrequest.Store) {
	e := newServerEnv(t, s, paymentrequest.WithPublicGroupRequests())
	member := e.addUser()
	chatID := e.putPublicGroup(nil, member)

	// The title and description are classified as one text, the description on
	// its own line, so a flag in either is the request's.
	e.moderator.flag("Bad title\nfine", "impersonation")
	e.moderator.flag("Fine title\nbad description", "impersonation", "financial_claim")
	e.moderator.flag("Bad alone", "impersonation")
	e.moderator.fail("Unscorable", moderation.ErrUnsupportedLanguage)

	for _, tc := range []struct {
		name        string
		title       string
		description string
		want        paymentrequestpb.CreatePaymentRequestResponse_Result
		category    moderationpb.FlaggedCategory
	}{
		{"title flagged", "Bad title", "fine", paymentrequestpb.CreatePaymentRequestResponse_TEXT_MODERATED, moderationpb.FlaggedCategory_IMPERSONATION},
		{"description flagged", "Fine title", "bad description", paymentrequestpb.CreatePaymentRequestResponse_TEXT_MODERATED, moderationpb.FlaggedCategory_MISLEADING},
		{"no description", "Bad alone", "", paymentrequestpb.CreatePaymentRequestResponse_TEXT_MODERATED, moderationpb.FlaggedCategory_IMPERSONATION},
		{"clean", "Fine title", "fine", paymentrequestpb.CreatePaymentRequestResponse_OK, moderationpb.FlaggedCategory_NONE},
	} {
		key := newKey()
		resp := e.create(e.creator, chatID, plaintextSplit(e.clock.Now().Add(time.Hour), tc.title, tc.description, share(member, 1)), key)
		require.Equal(t, tc.want, resp.Result, tc.name)
		require.Equal(t, tc.category, resp.FlaggedCategory, tc.name)

		_, err := s.Get(e.ctx, paymentrequest.MustDeriveID(e.creator.id, key))
		if tc.want == paymentrequestpb.CreatePaymentRequestResponse_OK {
			require.NoError(t, err, tc.name)
			require.NotNil(t, resp.PaymentRequest, tc.name)
		} else {
			require.ErrorIs(t, err, paymentrequest.ErrNotFound, tc.name)
			require.Nil(t, resp.PaymentRequest, tc.name)
		}
	}
	require.Equal(t, []string{
		"Bad title\nfine",
		"Fine title\nbad description",
		"Bad alone",
		"Fine title\nfine",
	}, e.moderator.classifiedTexts())

	// Text the classifier cannot score is refused rather than posted
	// unmoderated, as is any failure to classify.
	key := newKey()
	req := &paymentrequestpb.CreatePaymentRequestRequest{
		IdempotencyKey: key,
		Kind:           groupChatKind(chatID, plaintextSplit(e.clock.Now().Add(time.Hour), "Unscorable", "", share(member, 1))),
	}
	require.NoError(t, e.creator.keys.Auth(req, &req.Auth))
	_, err := e.client.CreatePaymentRequest(e.ctx, req)
	require.Equal(t, codes.Internal, status.Code(err))
	_, err = s.Get(e.ctx, paymentrequest.MustDeriveID(e.creator.id, key))
	require.ErrorIs(t, err, paymentrequest.ErrNotFound)
}

// testServer_Create_NoText pins a split with no text: taken in a keyed private
// group and, under the option that lifts their refusal, a public group, never
// moderated, and read back with its text unset.
func testServer_Create_NoText(t *testing.T, s paymentrequest.Store) {
	e := newServerEnv(t, s, paymentrequest.WithPublicGroupRequests())
	member := e.addUser()
	private := e.putPrivateGroup(true, member)
	public := e.putPublicGroup(nil, member)

	for name, chatID := range map[string]*commonpb.ChatId{"private": private, "public": public} {
		split := noTextSplit(e.clock.Now().Add(time.Hour), share(e.creator, 1), share(member, 2))
		resp := e.create(e.creator, chatID, split, newKey())
		require.Equal(t, paymentrequestpb.CreatePaymentRequestResponse_OK, resp.Result, name)
		requireOpen(t, resp.PaymentRequest, 1)
		require.Nil(t, resp.PaymentRequest.GetGroupChat().GetSplit().GetText(), name)

		got := e.get(member, resp.PaymentRequest.PaymentRequestId)
		require.Equal(t, paymentrequestpb.GetPaymentRequestResponse_OK, got.Result, name)
		require.NoError(t, protoutil.ProtoEqualError(resp.PaymentRequest, got.PaymentRequest), name)
		require.Nil(t, got.PaymentRequest.GetGroupChat().GetSplit().GetText(), name)
	}
	require.Zero(t, e.moderator.classifiedCount())
}

// testServer_Create_EmptyText pins a plaintext set with neither a title nor a
// description: refused as invalid, in any chat, rather than taken as no text;
// and either alone is moderated as itself.
func testServer_Create_EmptyText(t *testing.T, s paymentrequest.Store) {
	e := newServerEnv(t, s, paymentrequest.WithPublicGroupRequests())
	member := e.addUser()
	public := e.putPublicGroup(nil, member)
	private := e.putPrivateGroup(true, member)

	for name, chatID := range map[string]*commonpb.ChatId{"private": private, "public": public} {
		key := newKey()
		req := &paymentrequestpb.CreatePaymentRequestRequest{
			IdempotencyKey: key,
			Kind:           groupChatKind(chatID, plaintextSplit(e.clock.Now().Add(time.Hour), "", "", share(member, 1))),
		}
		require.NoError(t, e.creator.keys.Auth(req, &req.Auth))
		_, err := e.client.CreatePaymentRequest(e.ctx, req)
		require.Equal(t, codes.InvalidArgument, status.Code(err), name)
		_, err = s.Get(e.ctx, paymentrequest.MustDeriveID(e.creator.id, key))
		require.ErrorIs(t, err, paymentrequest.ErrNotFound, name)
	}
	require.Zero(t, e.moderator.classifiedCount())

	// A title is not required: a description alone is taken, and moderated
	// alone.
	resp := e.create(e.creator, public, plaintextSplit(e.clock.Now().Add(time.Hour), "", "Friday night", share(member, 1)), newKey())
	require.Equal(t, paymentrequestpb.CreatePaymentRequestResponse_OK, resp.Result)
	require.Equal(t, "", resp.PaymentRequest.GetGroupChat().GetSplit().GetPlaintext().GetTitle())
	resp = e.create(e.creator, public, plaintextSplit(e.clock.Now().Add(time.Hour), "Dinner", "", share(member, 1)), newKey())
	require.Equal(t, paymentrequestpb.CreatePaymentRequestResponse_OK, resp.Result)
	require.Equal(t, []string{"Friday night", "Dinner"}, e.moderator.classifiedTexts())
}

// testServer_Get_Payers pins the Payers a read carries: the request's
// PayerSummary on every read, the creator's own share counted as paid, and
// the caller's own Payer when they hold a share, the creator's included.
func testServer_Get_Payers(t *testing.T, s paymentrequest.Store) {
	e := newServerEnv(t, s)
	debtor := e.addUser()
	other := e.addUser()
	member := e.addUser()
	chatID := e.putPrivateGroup(true, debtor, other, member)

	created := e.create(e.creator, chatID, e.encryptedSplit(e.clock.Now().Add(time.Hour),
		share(e.creator, 0.1),
		share(debtor, 0.2),
		share(other, 0.3),
	), newKey())
	require.Equal(t, paymentrequestpb.CreatePaymentRequestResponse_OK, created.Result)
	id := created.PaymentRequest.PaymentRequestId

	wantSummary := &paymentrequestpb.PayerSummary{
		PayerCount:       3,
		PaidCount:        1,
		NativeAmountDue:  0.6,
		NativeAmountPaid: 0.1,
	}
	require.NoError(t, protoutil.ProtoEqualError(wantSummary, created.PaymentRequest.PayerSummary))
	require.NoError(t, protoutil.ProtoEqualError(wantSummary, e.requireUpdates(chatID, id, 1)[0].GetCreated().GetPayerSummary()))
	list := e.list(member, chatID, paymentrequestpb.GetChatPaymentRequestsRequest_OPEN, 0, nil)
	require.Len(t, list.PaymentRequests, 1)
	require.NoError(t, protoutil.ProtoEqualError(wantSummary, list.PaymentRequests[0].PayerSummary))

	// A debtor is given their unpaid Payer, the creator their own share's,
	// paid from creation, and a member with no share none.
	got := e.get(debtor, id)
	require.NoError(t, protoutil.ProtoEqualError(wantSummary, got.PaymentRequest.PayerSummary))
	require.NoError(t, protoutil.ProtoEqualError(PayerOfShare(shareOf(got.PaymentRequest, debtor)), got.CallerPayer))
	require.NotNil(t, got.CallerPayer.GetUnpaid())
	require.Equal(t, 0.2, got.CallerPayer.NativeAmountDue)

	got = e.get(e.creator, id)
	require.NotNil(t, got.CallerPayer.GetPaid().GetCreator())
	require.Equal(t, 0.1, got.CallerPayer.NativeAmountPaid)

	require.Nil(t, e.get(member, id).CallerPayer)

	// Once paid, the debtor's Payer and the summary say so, and the legacy
	// intent ID is no longer given.
	require.Equal(t, paymentrequestpb.MarkPaidResponse_OK, e.markPaid(e.creator, id, debtor).Result)
	got = e.get(debtor, id)
	require.NotNil(t, got.CallerPayer.GetPaid().GetManual())
	require.Equal(t, 0.2, got.CallerPayer.NativeAmountPaid)
	require.Nil(t, got.PaymentIntentId)
	require.NoError(t, protoutil.ProtoEqualError(&paymentrequestpb.PayerSummary{
		PayerCount:       3,
		PaidCount:        2,
		NativeAmountDue:  0.6,
		NativeAmountPaid: 0.3,
	}, got.PaymentRequest.PayerSummary))
}

// testServer_GetPaymentIntentId pins every result, in the order they are
// decided, and that the ID given is the share's stored one.
func testServer_GetPaymentIntentId(t *testing.T, s paymentrequest.Store) {
	e := newServerEnv(t, s)
	debtor := e.addUser()
	other := e.addUser()
	member := e.addUser()
	outsider := e.addUser()
	chatID := e.putPrivateGroup(true, debtor, other, member)

	created := e.mustCreate(e.creator, chatID, e.encryptedSplit(e.clock.Now().Add(time.Hour),
		share(e.creator, 1),
		share(debtor, 2),
		share(other, 3),
	))
	id := created.PaymentRequestId
	stored, err := s.Get(e.ctx, id)
	require.NoError(t, err)

	require.Equal(t, paymentrequestpb.GetPaymentIntentIdResponse_NOT_FOUND, e.getIntentID(debtor, newID()).Result)
	require.Equal(t, paymentrequestpb.GetPaymentIntentIdResponse_DENIED, e.getIntentID(outsider, id).Result)
	require.Equal(t, paymentrequestpb.GetPaymentIntentIdResponse_NOT_A_PAYER, e.getIntentID(e.creator, id).Result)
	require.Equal(t, paymentrequestpb.GetPaymentIntentIdResponse_NOT_A_PAYER, e.getIntentID(member, id).Result)

	resp := e.getIntentID(debtor, id)
	require.Equal(t, paymentrequestpb.GetPaymentIntentIdResponse_OK, resp.Result)
	require.Equal(t, stored.PaymentIntentID(debtor.id), resp.PaymentIntentId.GetValue())
	require.True(t, proto.Equal(resp.PaymentIntentId, e.getIntentID(debtor, id).PaymentIntentId))
	require.True(t, proto.Equal(resp.PaymentIntentId, e.get(debtor, id).PaymentIntentId))
	require.Equal(t, stored.PaymentIntentID(other.id), e.getIntentID(other, id).PaymentIntentId.GetValue())

	// The creator is NOT_A_PAYER even with no share of their own.
	noOwn := e.mustCreate(e.creator, chatID, e.encryptedSplit(e.clock.Now().Add(time.Hour), share(debtor, 2)))
	require.Equal(t, paymentrequestpb.GetPaymentIntentIdResponse_NOT_A_PAYER, e.getIntentID(e.creator, noOwn.PaymentRequestId).Result)

	// A paid share is ALREADY_PAID whatever the status, and is judged before
	// it: after the close, the paid debtor is ALREADY_PAID, the unpaid one
	// NOT_OPEN.
	require.Equal(t, paymentrequestpb.MarkPaidResponse_OK, e.markPaid(e.creator, id, debtor).Result)
	resp = e.getIntentID(debtor, id)
	require.Equal(t, paymentrequestpb.GetPaymentIntentIdResponse_ALREADY_PAID, resp.Result)
	require.Nil(t, resp.PaymentIntentId)
	require.Equal(t, paymentrequestpb.ClosePaymentRequestResponse_OK, e.close(e.creator, id).Result)
	require.Equal(t, paymentrequestpb.GetPaymentIntentIdResponse_ALREADY_PAID, e.getIntentID(debtor, id).Result)
	resp = e.getIntentID(other, id)
	require.Equal(t, paymentrequestpb.GetPaymentIntentIdResponse_NOT_OPEN, resp.Result)
	require.Nil(t, resp.PaymentIntentId)

	// Past its ends_at, before the worker stores it expired, it is NOT_OPEN.
	expiring := e.mustCreate(e.creator, chatID, e.encryptedSplit(e.clock.Now().Add(time.Hour), share(debtor, 2)))
	require.Equal(t, paymentrequestpb.GetPaymentIntentIdResponse_OK, e.getIntentID(debtor, expiring.PaymentRequestId).Result)
	e.clock.set(expiring.GetGroupChat().GetSplit().EndsAt.AsTime())
	require.Equal(t, paymentrequestpb.GetPaymentIntentIdResponse_NOT_OPEN, e.getIntentID(debtor, expiring.PaymentRequestId).Result)

	// A debtor who has left the chat can no longer read it: DENIED.
	_, _, err = e.chats.RemoveGroupMember(e.ctx, chatID, debtor.id, false)
	require.NoError(t, err)
	require.Equal(t, paymentrequestpb.GetPaymentIntentIdResponse_DENIED, e.getIntentID(debtor, noOwn.PaymentRequestId).Result)
}

// testServer_GetPayers pins the GetPayers gates, sections, paging, tokens and
// version.
func testServer_GetPayers(t *testing.T, s paymentrequest.Store) {
	e := newServerEnv(t, s)
	member := e.addUser()
	outsider := e.addUser()
	debtors := make([]user, 5)
	for i := range debtors {
		debtors[i] = e.addUser()
	}
	chatID := e.putPrivateGroup(true, append([]user{member}, debtors...)...)

	shares := []*paymentrequestpb.Share{share(e.creator, 1)}
	for i, d := range debtors {
		shares = append(shares, share(d, float64(i+2)))
	}
	created := e.mustCreate(e.creator, chatID, e.encryptedSplit(e.clock.Now().Add(time.Hour), shares...))
	id := created.PaymentRequestId
	other := e.mustCreate(e.creator, chatID, e.encryptedSplit(e.clock.Now().Add(time.Hour), share(member, 1)))

	unpaid := paymentrequestpb.GetPayersRequest_UNPAID
	paid := paymentrequestpb.GetPayersRequest_PAID

	for _, section := range []paymentrequestpb.GetPayersRequest_Section{unpaid, paid} {
		require.Equal(t, paymentrequestpb.GetPayersResponse_NOT_FOUND, e.payers(member, newID(), section, 0, nil).Result)
		require.Equal(t, paymentrequestpb.GetPayersResponse_DENIED, e.payers(outsider, id, section, 0, nil).Result)
	}

	// UNPAID is the debtors in the order listed, PAID the creator alone; each
	// Payer is its share's, and every page carries the version read.
	gotUnpaid := e.walkPayers(member, id, unpaid, 2, 1)
	requirePayerIDs(t, debtors, gotUnpaid)
	for i, p := range gotUnpaid {
		require.NoError(t, protoutil.ProtoEqualError(PayerOfShare(shareOf(created, debtors[i])), p))
	}
	gotPaid := e.walkPayers(member, id, paid, 2, 1)
	requirePayerIDs(t, []user{e.creator}, gotPaid)
	require.NotNil(t, gotPaid[0].GetPaid().GetCreator())

	// A page size of zero is the cap, which holds a whole split.
	resp := e.payers(member, id, unpaid, 0, nil)
	require.Len(t, resp.Payers, 5)
	require.False(t, resp.HasMore)
	require.Nil(t, resp.PagingToken)

	// Pay two, at different times: they leave UNPAID for PAID, newest first,
	// and the version moves with them.
	e.clock.advance(time.Minute)
	require.Equal(t, paymentrequestpb.MarkPaidResponse_OK, e.markPaid(e.creator, id, debtors[3]).Result)
	e.clock.advance(time.Minute)
	require.Equal(t, paymentrequestpb.MarkPaidResponse_OK, e.markPaid(e.creator, id, debtors[0]).Result)
	requirePayerIDs(t, []user{debtors[1], debtors[2], debtors[4]}, e.walkPayers(member, id, unpaid, 2, 3))
	requirePayerIDs(t, []user{debtors[0], debtors[3], e.creator}, e.walkPayers(member, id, paid, 2, 3))

	// A cursor resumes strictly after its Payer while others are paid.
	first := e.payers(member, id, unpaid, 1, nil)
	requirePayerIDs(t, []user{debtors[1]}, first.Payers)
	require.True(t, first.HasMore)
	e.clock.advance(time.Minute)
	require.Equal(t, paymentrequestpb.MarkPaidResponse_OK, e.markPaid(e.creator, id, debtors[1]).Result)
	require.Equal(t, paymentrequestpb.MarkPaidResponse_OK, e.markPaid(e.creator, id, debtors[2]).Result)
	second := e.payers(member, id, unpaid, 1, first.PagingToken)
	require.Equal(t, paymentrequestpb.GetPayersResponse_OK, second.Result)
	requirePayerIDs(t, []user{debtors[4]}, second.Payers)
	require.False(t, second.HasMore)
	require.EqualValues(t, 5, second.Version)

	// The last share completes the request: UNPAID is an empty page, OK.
	require.Equal(t, paymentrequestpb.MarkPaidResponse_OK, e.markPaid(e.creator, id, debtors[4]).Result)
	resp = e.payers(member, id, unpaid, 0, nil)
	require.Equal(t, paymentrequestpb.GetPayersResponse_OK, resp.Result)
	require.Empty(t, resp.Payers)
	require.False(t, resp.HasMore)
	require.Nil(t, resp.PagingToken)
	require.EqualValues(t, 6, resp.Version)
	got := e.get(member, id)
	require.Equal(t, paymentrequestpb.PaymentRequestStatus_PAYMENT_REQUEST_STATUS_COMPLETED, got.PaymentRequest.Status)
	require.Equal(t, got.PaymentRequest.Version, resp.Version)
	require.Len(t, e.walkPayers(member, id, paid, 4, 6), 6)

	// A token is bound to its request and section, and refused with another
	// or when malformed.
	first = e.payers(member, id, paid, 1, nil)
	require.True(t, first.HasMore)
	for _, tc := range []struct {
		name    string
		id      *paymentrequestpb.PaymentRequestId
		section paymentrequestpb.GetPayersRequest_Section
		token   *commonpb.PagingToken
	}{
		{"other request", other.PaymentRequestId, paid, first.PagingToken},
		{"other section", id, unpaid, first.PagingToken},
		{"truncated", id, paid, &commonpb.PagingToken{Value: first.PagingToken.Value[:10]}},
		{"garbage", id, paid, &commonpb.PagingToken{Value: bytes.Repeat([]byte{0xff}, len(first.PagingToken.Value))}},
	} {
		req := &paymentrequestpb.GetPayersRequest{
			PaymentRequestId: tc.id,
			Section:          tc.section,
			QueryOptions:     &commonpb.QueryOptions{PagingToken: tc.token},
		}
		require.NoError(t, member.keys.Auth(req, &req.Auth))
		_, err := e.client.GetPayers(e.ctx, req)
		require.Equal(t, codes.InvalidArgument, status.Code(err), tc.name)
	}

	// An unknown section is refused.
	req := &paymentrequestpb.GetPayersRequest{PaymentRequestId: id}
	require.NoError(t, member.keys.Auth(req, &req.Auth))
	_, err := e.client.GetPayers(e.ctx, req)
	require.Equal(t, codes.InvalidArgument, status.Code(err))
}

// testServer_GetPayers_Order pins the PAID section's order: newest paid_at
// first, ties by user ID, and a hand mark replaced by a payment moving to the
// payment's paid_at.
func testServer_GetPayers_Order(t *testing.T, s paymentrequest.Store) {
	e := newServerEnv(t, s)
	a := e.addUser()
	b := e.addUser()
	c := e.addUser()
	chatID := e.putPrivateGroup(true, a, b, c)
	created := e.mustCreate(e.creator, chatID, e.encryptedSplit(e.clock.Now().Add(time.Hour),
		share(a, 1), share(b, 1), share(c, 1),
	))
	id := created.PaymentRequestId
	paid := paymentrequestpb.GetPayersRequest_PAID

	// a and b are marked at the same time; c later.
	e.clock.advance(time.Minute)
	require.Equal(t, paymentrequestpb.MarkPaidResponse_OK, e.markPaid(e.creator, id, a).Result)
	require.Equal(t, paymentrequestpb.MarkPaidResponse_OK, e.markPaid(e.creator, id, b).Result)
	tied := []user{a, b}
	if bytes.Compare(a.id.Value, b.id.Value) > 0 {
		tied = []user{b, a}
	}
	e.clock.advance(time.Minute)
	paymentAt := e.clock.Now()
	require.Equal(t, paymentrequestpb.MarkPaidResponse_OK, e.markPaid(e.creator, id, c).Result)
	requirePayerIDs(t, []user{c, tied[0], tied[1]}, e.walkPayers(a, id, paid, 1, 4))
	requirePayerIDs(t, []user{c, tied[0], tied[1]}, e.walkPayers(a, id, paid, 2, 4))

	// The payment tied[1] made before being marked lands later, replacing the
	// mark: tied[1] moves ahead of c, to the payment's paid_at.
	stored, err := s.Get(e.ctx, id)
	require.NoError(t, err)
	e.clock.advance(time.Minute)
	paymentAt = e.clock.Now()
	_, changed, completed, err := s.PayShare(e.ctx, id, tied[1].id, &paymentrequestpb.Paid{
		PaidAt: timestamppb.New(paymentAt),
		How:    &paymentrequestpb.Paid_Intent{Intent: &commonpb.IntentId{Value: stored.PaymentIntentID(tied[1].id)}},
	}, paymentAt)
	require.NoError(t, err)
	require.True(t, changed)
	require.False(t, completed)
	got := e.walkPayers(a, id, paid, 1, 5)
	requirePayerIDs(t, []user{tied[1], c, tied[0]}, got)
	require.NotNil(t, got[0].GetPaid().GetIntent())
	require.True(t, paymentAt.Equal(got[0].GetPaid().PaidAt.AsTime()))
}

// testServer_Get pins who sees a request: whoever may read its chat in full,
// which in a public group includes a non-member who satisfies its listener
// rules, and nobody but its members in a private group.
func testServer_Get(t *testing.T, s paymentrequest.Store) {
	e := newServerEnv(t, s, paymentrequest.WithPublicGroupRequests())
	member := e.addUser()
	debtor := e.addUser()
	outsider := e.addUser()

	private := e.putPrivateGroup(true, member, debtor)
	created := e.mustCreate(e.creator, private, e.encryptedSplit(e.clock.Now().Add(time.Hour), share(debtor, 3)))

	require.Equal(t, paymentrequestpb.GetPaymentRequestResponse_NOT_FOUND, e.get(member, newID()).Result)
	require.Equal(t, paymentrequestpb.GetPaymentRequestResponse_DENIED, e.get(outsider, created.PaymentRequestId).Result)
	for _, u := range []user{e.creator, member, debtor} {
		got := e.get(u, created.PaymentRequestId)
		require.Equal(t, paymentrequestpb.GetPaymentRequestResponse_OK, got.Result)
		require.True(t, proto.Equal(created, got.PaymentRequest))
	}

	// A former member is refused, debtor or not.
	_, _, err := e.chats.RemoveGroupMember(e.ctx, private, debtor.id, false)
	require.NoError(t, err)
	require.Equal(t, paymentrequestpb.GetPaymentRequestResponse_DENIED, e.get(debtor, created.PaymentRequestId).Result)

	// Past its ends_at it reads EXPIRED, closed at its ends_at, before the
	// worker has stored anything.
	endsAt := created.GetGroupChat().GetSplit().EndsAt.AsTime()
	e.clock.set(endsAt)
	got := e.get(member, created.PaymentRequestId)
	require.Equal(t, paymentrequestpb.GetPaymentRequestResponse_OK, got.Result)
	requireResolved(t, got.PaymentRequest, paymentrequestpb.PaymentRequestStatus_PAYMENT_REQUEST_STATUS_EXPIRED, 1, endsAt)

	// In a public group whose listener rules a non-member satisfies, they see
	// it too.
	const requirement = 10
	gated := e.putPublicGroup(func(c *chat.Chat) {
		c.MinimumListenerBalance = &chat.MinimumBalance{Currency: "usd", NativeAmount: requirement}
	}, member)
	e.fund(e.creator, requirement)
	public := e.mustCreate(e.creator, gated, plaintextSplit(e.clock.Now().Add(time.Hour), "Dinner", "", share(member, 1)))

	require.Equal(t, paymentrequestpb.GetPaymentRequestResponse_OK, e.get(member, public.PaymentRequestId).Result)
	require.Equal(t, paymentrequestpb.GetPaymentRequestResponse_DENIED, e.get(outsider, public.PaymentRequestId).Result)
	e.fund(outsider, requirement)
	got = e.get(outsider, public.PaymentRequestId)
	require.Equal(t, paymentrequestpb.GetPaymentRequestResponse_OK, got.Result)
	require.True(t, proto.Equal(public, got.PaymentRequest))
}

// testServer_Get_PaymentIntentID pins who is given a share's payment intent
// ID: its debtor alone, while the share is unpaid. Each debtor's is their own,
// stable across reads, and the stored one; the creator, a member with no
// share and a debtor whose share is paid are given none.
func testServer_Get_PaymentIntentID(t *testing.T, s paymentrequest.Store) {
	e := newServerEnv(t, s)
	member := e.addUser()
	debtor := e.addUser()
	other := e.addUser()

	private := e.putPrivateGroup(true, member, debtor, other)
	created := e.mustCreate(e.creator, private, e.encryptedSplit(e.clock.Now().Add(time.Hour),
		share(e.creator, 3),
		share(debtor, 3),
		share(other, 3),
	))

	stored, err := s.Get(e.ctx, created.PaymentRequestId)
	require.NoError(t, err)

	got := e.get(debtor, created.PaymentRequestId)
	require.Equal(t, paymentrequestpb.GetPaymentRequestResponse_OK, got.Result)
	require.Len(t, got.PaymentIntentId.GetValue(), paymentrequest.PaymentIntentIDSize)
	require.Equal(t, stored.PaymentIntentID(debtor.id), got.PaymentIntentId.Value)
	require.True(t, proto.Equal(got.PaymentIntentId, e.get(debtor, created.PaymentRequestId).PaymentIntentId))

	otherGot := e.get(other, created.PaymentRequestId)
	require.Equal(t, stored.PaymentIntentID(other.id), otherGot.PaymentIntentId.GetValue())
	require.NotEqual(t, got.PaymentIntentId.Value, otherGot.PaymentIntentId.GetValue())

	for _, u := range []user{e.creator, member} {
		resp := e.get(u, created.PaymentRequestId)
		require.Equal(t, paymentrequestpb.GetPaymentRequestResponse_OK, resp.Result)
		require.Nil(t, resp.PaymentIntentId)
	}

	// Once the share is paid there is nothing left to pay with it.
	require.Equal(t, paymentrequestpb.MarkPaidResponse_OK, e.markPaid(e.creator, created.PaymentRequestId, debtor).Result)
	require.Nil(t, e.get(debtor, created.PaymentRequestId).PaymentIntentId)
	require.NotNil(t, e.get(other, created.PaymentRequestId).PaymentIntentId)
}

func testServer_List_Gates(t *testing.T, s paymentrequest.Store) {
	e := newServerEnv(t, s, paymentrequest.WithPublicGroupRequests())
	member := e.addUser()
	outsider := e.addUser()
	private := e.putPrivateGroup(true, member)
	e.mustCreate(e.creator, private, e.encryptedSplit(e.clock.Now().Add(time.Hour), share(member, 1)))

	dmID := &commonpb.ChatId{Value: bytes.Repeat([]byte{1}, chat.DmChatIDSize)}
	for _, section := range []paymentrequestpb.GetChatPaymentRequestsRequest_Section{
		paymentrequestpb.GetChatPaymentRequestsRequest_OPEN,
		paymentrequestpb.GetChatPaymentRequestsRequest_RESOLVED,
	} {
		require.Equal(t, paymentrequestpb.GetChatPaymentRequestsResponse_DENIED, e.list(member, dmID, section, 0, nil).Result)
		require.Equal(t, paymentrequestpb.GetChatPaymentRequestsResponse_NOT_FOUND, e.list(member, chat.MustGenerateGroupChatID(), section, 0, nil).Result)
		require.Equal(t, paymentrequestpb.GetChatPaymentRequestsResponse_DENIED, e.list(outsider, private, section, 0, nil).Result)
		require.Equal(t, paymentrequestpb.GetChatPaymentRequestsResponse_OK, e.list(member, private, section, 0, nil).Result)
	}
	resp := e.list(member, private, paymentrequestpb.GetChatPaymentRequestsRequest_OPEN, 0, nil)
	require.Len(t, resp.PaymentRequests, 1)
	require.False(t, resp.HasMore)
	require.Nil(t, resp.PagingToken)

	// A non-member who satisfies a public group's listener rules lists it.
	const requirement = 10
	gated := e.putPublicGroup(func(c *chat.Chat) {
		c.MinimumListenerBalance = &chat.MinimumBalance{Currency: "usd", NativeAmount: requirement}
	}, member)
	e.fund(e.creator, requirement)
	e.mustCreate(e.creator, gated, plaintextSplit(e.clock.Now().Add(time.Hour), "Dinner", "", share(member, 1)))
	require.Equal(t, paymentrequestpb.GetChatPaymentRequestsResponse_DENIED, e.list(outsider, gated, paymentrequestpb.GetChatPaymentRequestsRequest_OPEN, 0, nil).Result)
	e.fund(outsider, requirement)
	resp = e.list(outsider, gated, paymentrequestpb.GetChatPaymentRequestsRequest_OPEN, 0, nil)
	require.Equal(t, paymentrequestpb.GetChatPaymentRequestsResponse_OK, resp.Result)
	require.Len(t, resp.PaymentRequests, 1)
}

func testServer_List_Paging(t *testing.T, s paymentrequest.Store) {
	e := newServerEnv(t, s)
	member := e.addUser()
	chatID := e.putPrivateGroup(true, member)
	otherChat := e.putPrivateGroup(true, member)

	// Five open requests, created out of ends_at order, and one in another
	// chat that never appears.
	start := e.clock.Now()
	var open []*paymentrequestpb.PaymentRequestMetadata
	for _, offset := range []time.Duration{3, 1, 5, 2, 4} {
		open = append(open, e.mustCreate(e.creator, chatID, e.encryptedSplit(start.Add(offset*time.Hour), share(member, 1))))
	}
	e.mustCreate(e.creator, otherChat, e.encryptedSplit(start.Add(time.Hour), share(member, 1)))
	wantOpen := []*paymentrequestpb.PaymentRequestMetadata{open[1], open[3], open[0], open[4], open[2]}

	gotOpen := e.walk(member, chatID, paymentrequestpb.GetChatPaymentRequestsRequest_OPEN, 2)
	requireRequestIDs(t, wantOpen, gotOpen)
	for i, r := range gotOpen {
		require.True(t, proto.Equal(wantOpen[i], r))
	}
	require.Empty(t, e.walk(member, chatID, paymentrequestpb.GetChatPaymentRequestsRequest_RESOLVED, 2))

	// A page size of zero is the cap, which holds them all.
	resp := e.list(member, chatID, paymentrequestpb.GetChatPaymentRequestsRequest_OPEN, 0, nil)
	require.Len(t, resp.PaymentRequests, 5)
	require.False(t, resp.HasMore)

	// Resolve three, at different times, each a different way: the resolved
	// section is most recently resolved first.
	e.clock.advance(time.Minute)
	require.Equal(t, paymentrequestpb.ClosePaymentRequestResponse_OK, e.close(e.creator, open[0].PaymentRequestId).Result)
	e.clock.advance(time.Minute)
	require.Equal(t, paymentrequestpb.MarkPaidResponse_OK, e.markPaid(e.creator, open[2].PaymentRequestId, member).Result)
	e.clock.advance(time.Minute)
	require.Equal(t, paymentrequestpb.ClosePaymentRequestResponse_OK, e.close(e.creator, open[4].PaymentRequestId).Result)

	gotOpen = e.walk(member, chatID, paymentrequestpb.GetChatPaymentRequestsRequest_OPEN, 1)
	requireRequestIDs(t, []*paymentrequestpb.PaymentRequestMetadata{open[1], open[3]}, gotOpen)

	gotResolved := e.walk(member, chatID, paymentrequestpb.GetChatPaymentRequestsRequest_RESOLVED, 2)
	requireRequestIDs(t, []*paymentrequestpb.PaymentRequestMetadata{open[4], open[2], open[0]}, gotResolved)
	require.Equal(t, paymentrequestpb.PaymentRequestStatus_PAYMENT_REQUEST_STATUS_CLOSED, gotResolved[0].Status)
	require.Equal(t, paymentrequestpb.PaymentRequestStatus_PAYMENT_REQUEST_STATUS_COMPLETED, gotResolved[1].Status)
	require.Equal(t, paymentrequestpb.PaymentRequestStatus_PAYMENT_REQUEST_STATUS_CLOSED, gotResolved[2].Status)

	// A token is bound to its chat and section, and refused with another or
	// when malformed.
	first := e.list(member, chatID, paymentrequestpb.GetChatPaymentRequestsRequest_OPEN, 1, nil)
	require.True(t, first.HasMore)
	require.NotNil(t, first.PagingToken)
	for _, tc := range []struct {
		name    string
		chatID  *commonpb.ChatId
		section paymentrequestpb.GetChatPaymentRequestsRequest_Section
		token   *commonpb.PagingToken
	}{
		{"other chat", otherChat, paymentrequestpb.GetChatPaymentRequestsRequest_OPEN, first.PagingToken},
		{"other section", chatID, paymentrequestpb.GetChatPaymentRequestsRequest_RESOLVED, first.PagingToken},
		{"truncated", chatID, paymentrequestpb.GetChatPaymentRequestsRequest_OPEN, &commonpb.PagingToken{Value: first.PagingToken.Value[:10]}},
		{"garbage", chatID, paymentrequestpb.GetChatPaymentRequestsRequest_OPEN, &commonpb.PagingToken{Value: bytes.Repeat([]byte{0xff}, len(first.PagingToken.Value))}},
	} {
		req := &paymentrequestpb.GetChatPaymentRequestsRequest{
			ChatId:       tc.chatID,
			Section:      tc.section,
			QueryOptions: &commonpb.QueryOptions{PagingToken: tc.token},
		}
		require.NoError(t, member.keys.Auth(req, &req.Auth))
		_, err := e.client.GetChatPaymentRequests(e.ctx, req)
		require.Equal(t, codes.InvalidArgument, status.Code(err), tc.name)
	}
	second := e.list(member, chatID, paymentrequestpb.GetChatPaymentRequestsRequest_OPEN, 1, first.PagingToken)
	require.Equal(t, paymentrequestpb.GetChatPaymentRequestsResponse_OK, second.Result)
	requireRequestIDs(t, []*paymentrequestpb.PaymentRequestMetadata{open[3]}, second.PaymentRequests)
	require.False(t, second.HasMore)
}

// testServer_List_ClockExpired pins requests stored open past their ends_at:
// dropped from the open section, which reads on past them so a page is never
// empty while claiming more, and in the resolved one only once stored
// expired.
func testServer_List_ClockExpired(t *testing.T, s paymentrequest.Store) {
	e := newServerEnv(t, s)
	member := e.addUser()
	chatID := e.putPrivateGroup(true, member)

	start := e.clock.Now()
	var expiring []*paymentrequestpb.PaymentRequestMetadata
	for i := 1; i <= 4; i++ {
		expiring = append(expiring, e.mustCreate(e.creator, chatID, e.encryptedSplit(start.Add(time.Duration(i)*time.Minute), share(member, 1))))
	}
	later := e.mustCreate(e.creator, chatID, e.encryptedSplit(start.Add(2*time.Hour), share(member, 1)))
	latest := e.mustCreate(e.creator, chatID, e.encryptedSplit(start.Add(3*time.Hour), share(member, 1)))

	e.clock.set(start.Add(time.Hour))

	// Every stored-open request ending soonest is past its ends_at: each page
	// of one reads past them to an open one.
	first := e.list(member, chatID, paymentrequestpb.GetChatPaymentRequestsRequest_OPEN, 1, nil)
	requireRequestIDs(t, []*paymentrequestpb.PaymentRequestMetadata{later}, first.PaymentRequests)
	requireOpen(t, first.PaymentRequests[0], 1)
	require.True(t, first.HasMore)
	second := e.list(member, chatID, paymentrequestpb.GetChatPaymentRequestsRequest_OPEN, 1, first.PagingToken)
	requireRequestIDs(t, []*paymentrequestpb.PaymentRequestMetadata{latest}, second.PaymentRequests)
	require.False(t, second.HasMore)

	// A section of nothing but such requests is empty, with nothing more.
	e.clock.set(start.Add(4 * time.Hour))
	resp := e.list(member, chatID, paymentrequestpb.GetChatPaymentRequestsRequest_OPEN, 2, nil)
	require.Empty(t, resp.PaymentRequests)
	require.False(t, resp.HasMore)
	require.Nil(t, resp.PagingToken)

	// Not yet stored expired, they are in neither section; once stored, they
	// are resolved, most recently ended first, closed at their ends_at.
	require.Empty(t, e.walk(member, chatID, paymentrequestpb.GetChatPaymentRequestsRequest_RESOLVED, 2))
	for _, r := range expiring {
		changed, err := s.Expire(e.ctx, r.PaymentRequestId)
		require.NoError(t, err)
		require.True(t, changed)
	}
	resolved := e.walk(member, chatID, paymentrequestpb.GetChatPaymentRequestsRequest_RESOLVED, 3)
	requireRequestIDs(t, []*paymentrequestpb.PaymentRequestMetadata{expiring[3], expiring[2], expiring[1], expiring[0]}, resolved)
	for i, r := range resolved {
		requireResolved(t, r, paymentrequestpb.PaymentRequestStatus_PAYMENT_REQUEST_STATUS_EXPIRED, 1, expiring[3-i].GetGroupChat().GetSplit().EndsAt.AsTime())
	}
}

func testServer_Close(t *testing.T, s paymentrequest.Store) {
	e := newServerEnv(t, s)
	member := e.addUser()
	chatID := e.putPrivateGroup(true, member)
	created := e.mustCreate(e.creator, chatID, e.encryptedSplit(e.clock.Now().Add(time.Hour), share(member, 1)))

	require.Equal(t, paymentrequestpb.ClosePaymentRequestResponse_NOT_FOUND, e.close(e.creator, newID()).Result)
	require.Equal(t, paymentrequestpb.ClosePaymentRequestResponse_DENIED, e.close(member, created.PaymentRequestId).Result)

	e.clock.advance(time.Minute)
	closedAt := e.clock.Now()
	resp := e.close(e.creator, created.PaymentRequestId)
	require.Equal(t, paymentrequestpb.ClosePaymentRequestResponse_OK, resp.Result)
	requireResolved(t, resp.PaymentRequest, paymentrequestpb.PaymentRequestStatus_PAYMENT_REQUEST_STATUS_CLOSED, 2, closedAt)

	// Closing again is the no-op, answered with the request as it is.
	e.clock.advance(time.Minute)
	resp = e.close(e.creator, created.PaymentRequestId)
	require.Equal(t, paymentrequestpb.ClosePaymentRequestResponse_OK, resp.Result)
	requireResolved(t, resp.PaymentRequest, paymentrequestpb.PaymentRequestStatus_PAYMENT_REQUEST_STATUS_CLOSED, 2, closedAt)

	// And so is closing one the clock has expired: it stays EXPIRED, closed
	// at its ends_at, at its version.
	expiring := e.mustCreate(e.creator, chatID, e.encryptedSplit(e.clock.Now().Add(time.Hour), share(member, 1)))
	endsAt := expiring.GetGroupChat().GetSplit().EndsAt.AsTime()
	e.clock.set(endsAt.Add(time.Second))
	resp = e.close(e.creator, expiring.PaymentRequestId)
	require.Equal(t, paymentrequestpb.ClosePaymentRequestResponse_OK, resp.Result)
	requireResolved(t, resp.PaymentRequest, paymentrequestpb.PaymentRequestStatus_PAYMENT_REQUEST_STATUS_EXPIRED, 1, endsAt)

	// The creator closes their request whatever their standing in the chat.
	other := e.mustCreate(e.creator, chatID, e.encryptedSplit(e.clock.Now().Add(time.Hour), share(member, 1)))
	_, _, err := e.chats.RemoveGroupMember(e.ctx, chatID, e.creator.id, false)
	require.NoError(t, err)
	resp = e.close(e.creator, other.PaymentRequestId)
	require.Equal(t, paymentrequestpb.ClosePaymentRequestResponse_OK, resp.Result)
	require.Equal(t, paymentrequestpb.PaymentRequestStatus_PAYMENT_REQUEST_STATUS_CLOSED, resp.PaymentRequest.Status)
}

// testServer_Updates pins what the service announces: the request whole on
// creation, then each real change as what it did, a share marked paid by hand
// (the last one also completing the request) and a close, so that applying
// them in order as a client does yields the request exactly as the RPC
// answered it; and nothing else: not a retry, a no-op or a refusal.
func testServer_Updates(t *testing.T, s paymentrequest.Store) {
	e := newServerEnv(t, s)
	alice := e.addUser()
	bob := e.addUser()
	chatID := e.putPrivateGroup(true, alice, bob)

	key := newKey()
	split := e.encryptedSplit(e.clock.Now().Add(time.Hour), share(e.creator, 1), share(alice, 2), share(bob, 3))
	created := e.create(e.creator, chatID, split, key)
	require.Equal(t, paymentrequestpb.CreatePaymentRequestResponse_OK, created.Result)
	id := created.PaymentRequest.PaymentRequestId
	updates := e.requireUpdates(chatID, id, 1)
	require.NoError(t, protoutil.ProtoEqualError(created.PaymentRequest, updates[0].GetCreated()))

	// A retry is answered from the store and announces nothing.
	require.Equal(t, paymentrequestpb.CreatePaymentRequestResponse_OK, e.create(e.creator, chatID, split, key).Result)
	e.requireUpdates(chatID, id, 1)

	// Refusals and no-ops announce nothing: marking someone else's request,
	// a user with no share, and the creator's own share, paid from creation.
	require.Equal(t, paymentrequestpb.MarkPaidResponse_DENIED, e.markPaid(alice, id, alice).Result)
	require.Equal(t, paymentrequestpb.MarkPaidResponse_NOT_A_SHARE, e.markPaid(e.creator, id, e.addUser()).Result)
	require.Equal(t, paymentrequestpb.MarkPaidResponse_OK, e.markPaid(e.creator, id, e.creator).Result)
	require.Equal(t, paymentrequestpb.ClosePaymentRequestResponse_DENIED, e.close(alice, id).Result)
	e.requireUpdates(chatID, id, 1)

	// A share marked paid is announced once, at the next version, as the
	// share, its Payer and the summary as they now stand.
	e.clock.advance(time.Minute)
	marked := e.markPaid(e.creator, id, alice)
	require.Equal(t, paymentrequestpb.MarkPaidResponse_OK, marked.Result)
	updates = e.requireUpdates(chatID, id, 2)
	require.EqualValues(t, 2, updates[1].GetUpdates().GetVersion())
	require.NoError(t, protoutil.ProtoEqualError(shareOf(marked.PaymentRequest, alice), sharePaymentUpdate(t, updates[1])))
	require.NotNil(t, sharePaymentUpdate(t, updates[1]).Paid.GetManual())
	changes := updates[1].GetUpdates().GetUpdates()
	require.NoError(t, protoutil.ProtoEqualError(PayerOfShare(shareOf(marked.PaymentRequest, alice)), changes[1].GetPayerUpdate().GetPayer()))
	require.NoError(t, protoutil.ProtoEqualError(marked.PaymentRequest.PayerSummary, changes[2].GetPayerSummaryUpdate().GetPayerSummary()))
	require.EqualValues(t, 2, changes[2].GetPayerSummaryUpdate().GetPayerSummary().PaidCount)
	e.requireApplied(updates, marked.PaymentRequest)
	require.Equal(t, paymentrequestpb.MarkPaidResponse_OK, e.markPaid(e.creator, id, alice).Result)
	e.requireUpdates(chatID, id, 2)

	// The last share is announced with the completion it made, after the
	// share, its Payer and the summary.
	e.clock.advance(time.Minute)
	completed := e.markPaid(e.creator, id, bob)
	require.Equal(t, paymentrequestpb.MarkPaidResponse_OK, completed.Result)
	updates = e.requireUpdates(chatID, id, 3)
	changes = updates[2].GetUpdates().GetUpdates()
	require.Len(t, changes, 4)
	require.NoError(t, protoutil.ProtoEqualError(shareOf(completed.PaymentRequest, bob), changes[0].GetSharePaymentUpdate().GetShare()))
	require.NoError(t, protoutil.ProtoEqualError(PayerOfShare(shareOf(completed.PaymentRequest, bob)), changes[1].GetPayerUpdate().GetPayer()))
	require.NoError(t, protoutil.ProtoEqualError(completed.PaymentRequest.PayerSummary, changes[2].GetPayerSummaryUpdate().GetPayerSummary()))
	status := statusUpdate(t, updates[2])
	require.Equal(t, paymentrequestpb.PaymentRequestStatus_PAYMENT_REQUEST_STATUS_COMPLETED, status.Status)
	require.True(t, status.ClosedAt.AsTime().Equal(shareOf(completed.PaymentRequest, bob).Paid.PaidAt.AsTime()))
	e.requireApplied(updates, completed.PaymentRequest)

	// Closing a completed request is the no-op.
	require.Equal(t, paymentrequestpb.ClosePaymentRequestResponse_OK, e.close(e.creator, id).Result)
	e.requireUpdates(chatID, id, 3)

	// A close is announced once, as the status change alone: it records no
	// payment, so no Payer or summary.
	closing := e.mustCreate(e.creator, chatID, e.encryptedSplit(e.clock.Now().Add(time.Hour), share(alice, 2)))
	e.clock.advance(time.Minute)
	closed := e.close(e.creator, closing.PaymentRequestId)
	require.Equal(t, paymentrequestpb.ClosePaymentRequestResponse_OK, closed.Result)
	updates = e.requireUpdates(chatID, closing.PaymentRequestId, 2)
	require.Len(t, updates[1].GetUpdates().GetUpdates(), 1)
	status = statusUpdate(t, updates[1])
	require.Equal(t, paymentrequestpb.PaymentRequestStatus_PAYMENT_REQUEST_STATUS_CLOSED, status.Status)
	require.True(t, status.ClosedAt.AsTime().Equal(e.clock.Now()))
	e.requireApplied(updates, closed.PaymentRequest)
	require.Equal(t, paymentrequestpb.ClosePaymentRequestResponse_OK, e.close(e.creator, closing.PaymentRequestId).Result)
	e.requireUpdates(chatID, closing.PaymentRequestId, 2)

	// Expiry is the clock's and is never announced, nor is closing or
	// marking a request the clock has expired.
	expiring := e.mustCreate(e.creator, chatID, e.encryptedSplit(e.clock.Now().Add(time.Hour), share(alice, 2)))
	e.clock.set(expiring.GetGroupChat().GetSplit().EndsAt.AsTime().Add(time.Second))
	require.Equal(t, paymentrequestpb.MarkPaidResponse_NOT_OPEN, e.markPaid(e.creator, expiring.PaymentRequestId, alice).Result)
	require.Equal(t, paymentrequestpb.ClosePaymentRequestResponse_OK, e.close(e.creator, expiring.PaymentRequestId).Result)
	e.requireUpdates(chatID, expiring.PaymentRequestId, 1)

	// A retry that loses the race to its twin is answered with the twin's
	// request and announces nothing: the twin's write was the creation.
	key = newKey()
	split = e.encryptedSplit(e.clock.Now().Add(time.Hour), share(alice, 5))
	twin, err := paymentrequest.NewSplitRequest(paymentrequest.MustDeriveID(e.creator.id, key), e.creator.id, chatID, split, e.clock.Now())
	require.NoError(t, err)
	racing := &racingStore{Store: s, beforeCreate: func() { assert.NoError(t, s.Create(e.ctx, twin)) }}
	re := newServerEnvWith(t, racing, e)
	require.Equal(t, paymentrequestpb.CreatePaymentRequestResponse_OK, re.create(e.creator, chatID, split, key).Result)
	re.requireUpdates(chatID, twin.ID, 0)
}

func testServer_MarkPaid(t *testing.T, s paymentrequest.Store) {
	e := newServerEnv(t, s)
	alice := e.addUser()
	bob := e.addUser()
	outsider := e.addUser()
	chatID := e.putPrivateGroup(true, alice, bob)
	created := e.mustCreate(e.creator, chatID, e.encryptedSplit(e.clock.Now().Add(time.Hour), share(e.creator, 1), share(alice, 2), share(bob, 3)))
	id := created.PaymentRequestId

	require.Equal(t, paymentrequestpb.MarkPaidResponse_NOT_FOUND, e.markPaid(e.creator, newID(), alice).Result)
	require.Equal(t, paymentrequestpb.MarkPaidResponse_DENIED, e.markPaid(alice, id, alice).Result)
	require.Equal(t, paymentrequestpb.MarkPaidResponse_DENIED, e.markPaid(outsider, id, outsider).Result)
	require.Equal(t, paymentrequestpb.MarkPaidResponse_NOT_A_SHARE, e.markPaid(e.creator, id, outsider).Result)

	// The creator's own share is paid from creation: marking it is the no-op.
	resp := e.markPaid(e.creator, id, e.creator)
	require.Equal(t, paymentrequestpb.MarkPaidResponse_OK, resp.Result)
	requireOpen(t, resp.PaymentRequest, 1)
	require.NotNil(t, shareOf(resp.PaymentRequest, e.creator).Paid.GetCreator())

	e.clock.advance(time.Minute)
	paidAt := e.clock.Now()
	resp = e.markPaid(e.creator, id, alice)
	require.Equal(t, paymentrequestpb.MarkPaidResponse_OK, resp.Result)
	requireOpen(t, resp.PaymentRequest, 2)
	paid := shareOf(resp.PaymentRequest, alice).Paid
	require.NotNil(t, paid.GetManual())
	require.True(t, paidAt.Equal(paid.PaidAt.AsTime()))
	require.Nil(t, shareOf(resp.PaymentRequest, bob).Paid)

	// Again is the no-op: the first marking stands.
	e.clock.advance(time.Minute)
	resp = e.markPaid(e.creator, id, alice)
	require.Equal(t, paymentrequestpb.MarkPaidResponse_OK, resp.Result)
	requireOpen(t, resp.PaymentRequest, 2)
	require.True(t, paidAt.Equal(shareOf(resp.PaymentRequest, alice).Paid.PaidAt.AsTime()))

	// The last unpaid share completes the request.
	completedAt := e.clock.Now()
	resp = e.markPaid(e.creator, id, bob)
	require.Equal(t, paymentrequestpb.MarkPaidResponse_OK, resp.Result)
	requireResolved(t, resp.PaymentRequest, paymentrequestpb.PaymentRequestStatus_PAYMENT_REQUEST_STATUS_COMPLETED, 3, completedAt)

	// A closed request takes no marking of an unpaid share, but a paid
	// share's marking is still the no-op OK.
	closing := e.mustCreate(e.creator, chatID, e.encryptedSplit(e.clock.Now().Add(time.Hour), share(alice, 2), share(bob, 3)))
	require.Equal(t, paymentrequestpb.MarkPaidResponse_OK, e.markPaid(e.creator, closing.PaymentRequestId, alice).Result)
	require.Equal(t, paymentrequestpb.ClosePaymentRequestResponse_OK, e.close(e.creator, closing.PaymentRequestId).Result)
	require.Equal(t, paymentrequestpb.MarkPaidResponse_NOT_OPEN, e.markPaid(e.creator, closing.PaymentRequestId, bob).Result)
	resp = e.markPaid(e.creator, closing.PaymentRequestId, alice)
	require.Equal(t, paymentrequestpb.MarkPaidResponse_OK, resp.Result)
	require.Equal(t, paymentrequestpb.PaymentRequestStatus_PAYMENT_REQUEST_STATUS_CLOSED, resp.PaymentRequest.Status)
	require.Equal(t, uint64(3), resp.PaymentRequest.Version)

	// Nor does one the clock has expired, before the worker stores it.
	expiring := e.mustCreate(e.creator, chatID, e.encryptedSplit(e.clock.Now().Add(time.Hour), share(alice, 2)))
	e.clock.set(expiring.GetGroupChat().GetSplit().EndsAt.AsTime())
	require.Equal(t, paymentrequestpb.MarkPaidResponse_NOT_OPEN, e.markPaid(e.creator, expiring.PaymentRequestId, alice).Result)
	stored, err := s.Get(e.ctx, expiring.PaymentRequestId)
	require.NoError(t, err)
	require.Equal(t, uint64(1), stored.Version)
	require.Nil(t, stored.Share(alice.id).Paid)
}

// testServer_Create_Collection pins a collection in a keyed private group:
// dated or not, its text encrypted under the chat key or unset, never
// moderated, open at version 1 with no Payers.
func testServer_Create_Collection(t *testing.T, s paymentrequest.Store) {
	e := newServerEnv(t, s)
	member := e.addUser()
	chatID := e.putPrivateGroup(true, member)

	endsAt := e.clock.Now().Add(time.Hour)
	for _, tc := range []struct {
		name       string
		collection *paymentrequestpb.Collection
	}{
		{"dated, encrypted text", encryptedCollection(endsAt, 12.34)},
		{"undated, encrypted text", encryptedCollection(time.Time{}, 5)},
		{"dated, no text", fixedCollection(endsAt, 0.01)},
		{"undated, no text", fixedCollection(time.Time{}, 1000)},
	} {
		resp := e.createCollection(e.creator, chatID, tc.collection, newKey())
		require.Equal(t, paymentrequestpb.CreatePaymentRequestResponse_OK, resp.Result, tc.name)
		require.Equal(t, moderationpb.FlaggedCategory_NONE, resp.FlaggedCategory, tc.name)

		created := resp.PaymentRequest
		require.Equal(t, e.creator.id.Value, created.Creator.Value, tc.name)
		require.True(t, e.clock.Now().Equal(created.CreatedAt.AsTime()), tc.name)
		requireOpen(t, created, 1)
		require.NoError(t, protoutil.ProtoEqualError(&paymentrequestpb.PayerSummary{}, created.PayerSummary), tc.name)
		require.Equal(t, chatID.Value, created.GetGroupChat().GetChatId().GetValue(), tc.name)
		require.NoError(t, protoutil.ProtoEqualError(tc.collection, created.GetGroupChat().GetCollection()), tc.name)
		require.Nil(t, created.GetGroupChat().GetSplit(), tc.name)

		stored, err := s.Get(e.ctx, created.PaymentRequestId)
		require.NoError(t, err, tc.name)
		require.True(t, stored.IsCollection(), tc.name)
		require.NoError(t, protoutil.ProtoEqualError(created, stored.ToProto(e.clock.Now())), tc.name)

		got := e.get(member, created.PaymentRequestId)
		require.Equal(t, paymentrequestpb.GetPaymentRequestResponse_OK, got.Result, tc.name)
		require.NoError(t, protoutil.ProtoEqualError(created, got.PaymentRequest), tc.name)
	}
	require.Zero(t, e.moderator.classifiedCount())

	// An undated collection never expires: long past what any ends_at could
	// be, it is still open.
	undated := e.mustCreateCollection(e.creator, chatID, fixedCollection(time.Time{}, 1))
	require.Nil(t, undated.GetGroupChat().GetCollection().GetEndsAt())
	e.clock.advance(10 * paymentrequest.MaxDuration)
	requireOpen(t, e.get(member, undated.PaymentRequestId).PaymentRequest, 1)

	// Any member who can speak creates one, not just the group's creator.
	resp := e.createCollection(member, chatID, fixedCollection(time.Time{}, 1), newKey())
	require.Equal(t, paymentrequestpb.CreatePaymentRequestResponse_OK, resp.Result)
	require.Equal(t, member.id.Value, resp.PaymentRequest.Creator.Value)
}

// testServer_Create_Collection_Refused pins what refuses a collection: the
// checks a split shares, an ends_at that is set badly, and an amount that is
// not a whole number, at least one, of the currency's minimum transfer value;
// each judged in the documented order, and nothing stored.
func testServer_Create_Collection_Refused(t *testing.T, s paymentrequest.Store) {
	e := newServerEnv(t, s)
	member := e.addUser()
	nonMember := e.addUser()
	keyed := e.putPrivateGroup(true, member)
	keyless := e.putPrivateGroup(false, member)
	public := e.putPublicGroup(nil, member)
	dmID := &commonpb.ChatId{Value: bytes.Repeat([]byte{1}, chat.DmChatIDSize)}

	now := e.clock.Now()
	endsAt := now.Add(time.Hour)
	with := func(c *paymentrequestpb.Collection, configure func(*paymentrequestpb.Collection)) *paymentrequestpb.Collection {
		configure(c)
		return c
	}
	plaintextIn := func(c *paymentrequestpb.Collection) *paymentrequestpb.Collection {
		return with(c, func(c *paymentrequestpb.Collection) { c.Text = collectionPlaintext("Gift", "") })
	}
	amount := func(currency string, amount float64) *paymentrequestpb.Collection {
		return with(encryptedCollection(endsAt, amount), func(c *paymentrequestpb.Collection) { c.Currency = currency })
	}

	for _, tc := range []struct {
		name       string
		user       user
		chatID     *commonpb.ChatId
		collection *paymentrequestpb.Collection
		want       paymentrequestpb.CreatePaymentRequestResponse_Result
	}{
		{"dm", e.creator, dmID, encryptedCollection(endsAt, 1), paymentrequestpb.CreatePaymentRequestResponse_CHAT_NOT_SUPPORTED},
		{"non-member", nonMember, keyed, encryptedCollection(endsAt, 1), paymentrequestpb.CreatePaymentRequestResponse_DENIED},
		{"missing chat", e.creator, chat.MustGenerateGroupChatID(), encryptedCollection(endsAt, 1), paymentrequestpb.CreatePaymentRequestResponse_DENIED},
		{"keyless private group", e.creator, keyless, encryptedCollection(endsAt, 1), paymentrequestpb.CreatePaymentRequestResponse_DENIED},
		{"public group", e.creator, public, plaintextIn(fixedCollection(endsAt, 1)), paymentrequestpb.CreatePaymentRequestResponse_CHAT_NOT_SUPPORTED},
		{"plaintext in private group", e.creator, keyed, plaintextIn(fixedCollection(endsAt, 1)), paymentrequestpb.CreatePaymentRequestResponse_ENCRYPTION_REQUIRED},
		{"ends now", e.creator, keyed, encryptedCollection(now, 1), paymentrequestpb.CreatePaymentRequestResponse_INVALID_ENDS_AT},
		{"ended", e.creator, keyed, encryptedCollection(now.Add(-time.Second), 1), paymentrequestpb.CreatePaymentRequestResponse_INVALID_ENDS_AT},
		{"past the maximum", e.creator, keyed, encryptedCollection(now.Add(paymentrequest.MaxDuration+time.Nanosecond), 1), paymentrequestpb.CreatePaymentRequestResponse_INVALID_ENDS_AT},
		{"latest timestamp", e.creator, keyed, encryptedCollection(time.Date(9999, time.December, 31, 23, 59, 59, 0, time.UTC), 1), paymentrequestpb.CreatePaymentRequestResponse_INVALID_ENDS_AT},
		{"unsupported currency", e.creator, keyed, amount("xyz", 1), paymentrequestpb.CreatePaymentRequestResponse_INVALID_CURRENCY},
		{"zero", e.creator, keyed, amount("usd", 0), paymentrequestpb.CreatePaymentRequestResponse_INVALID_AMOUNT},
		{"below minimum", e.creator, keyed, amount("usd", 0.004), paymentrequestpb.CreatePaymentRequestResponse_INVALID_AMOUNT},
		{"half a minor unit", e.creator, keyed, amount("usd", 0.005), paymentrequestpb.CreatePaymentRequestResponse_INVALID_AMOUNT},
		{"between minor units", e.creator, keyed, amount("usd", 1.004), paymentrequestpb.CreatePaymentRequestResponse_INVALID_AMOUNT},
		{"not a number", e.creator, keyed, amount("usd", math.NaN()), paymentrequestpb.CreatePaymentRequestResponse_INVALID_AMOUNT},
		{"infinite", e.creator, keyed, amount("usd", math.Inf(1)), paymentrequestpb.CreatePaymentRequestResponse_INVALID_AMOUNT},
		{"below a currency's whole unit", e.creator, keyed, amount("jpy", 0.5), paymentrequestpb.CreatePaymentRequestResponse_INVALID_AMOUNT},
		{"between a currency's whole units", e.creator, keyed, amount("jpy", 1.5), paymentrequestpb.CreatePaymentRequestResponse_INVALID_AMOUNT},

		// Where more than one check fails, the order is the split's: text,
		// then ends_at, then currency, then the amount.
		{"text, then ends_at", e.creator, keyed, plaintextIn(fixedCollection(now, 0)), paymentrequestpb.CreatePaymentRequestResponse_ENCRYPTION_REQUIRED},
		{"ends_at, then currency", e.creator, keyed, with(encryptedCollection(now, 0), func(c *paymentrequestpb.Collection) { c.Currency = "xyz" }), paymentrequestpb.CreatePaymentRequestResponse_INVALID_ENDS_AT},
		{"currency, then amount", e.creator, keyed, amount("xyz", 0), paymentrequestpb.CreatePaymentRequestResponse_INVALID_CURRENCY},
	} {
		key := newKey()
		resp := e.createCollection(tc.user, tc.chatID, tc.collection, key)
		require.Equal(t, tc.want, resp.Result, tc.name)
		require.Nil(t, resp.PaymentRequest, tc.name)

		_, err := s.Get(e.ctx, paymentrequest.MustDeriveID(tc.user.id, key))
		require.ErrorIs(t, err, paymentrequest.ErrNotFound, tc.name)
	}

	// Amounts on a whole minor unit, float error forgiven, are taken.
	for _, tc := range []struct {
		currency string
		amount   float64
	}{
		{"usd", 0.01}, {"usd", 0.1 + 0.2}, {"usd", 99999999.99}, {"jpy", 1}, {"jpy", 1000},
	} {
		resp := e.createCollection(e.creator, keyed, amount(tc.currency, tc.amount), newKey())
		require.Equal(t, paymentrequestpb.CreatePaymentRequestResponse_OK, resp.Result, "%s %v", tc.currency, tc.amount)
	}

	// A set plaintext with neither a title nor a description is invalid, not
	// taken as no text.
	key := newKey()
	req := &paymentrequestpb.CreatePaymentRequestRequest{
		IdempotencyKey: key,
		Kind:           collectionKind(keyed, with(fixedCollection(endsAt, 1), func(c *paymentrequestpb.Collection) { c.Text = collectionPlaintext("", "") })),
	}
	require.NoError(t, e.creator.keys.Auth(req, &req.Auth))
	_, err := e.client.CreatePaymentRequest(e.ctx, req)
	require.Equal(t, codes.InvalidArgument, status.Code(err))
	_, err = s.Get(e.ctx, paymentrequest.MustDeriveID(e.creator.id, key))
	require.ErrorIs(t, err, paymentrequest.ErrNotFound)

	require.Zero(t, e.moderator.classifiedCount())
}

// testServer_Create_Collection_PublicGroup pins a collection in a public
// group, under the option that lifts their refusal: plaintext text alone,
// moderated, and none at all taken unmoderated.
func testServer_Create_Collection_PublicGroup(t *testing.T, s paymentrequest.Store) {
	e := newServerEnv(t, s, paymentrequest.WithPublicGroupRequests())
	member := e.addUser()
	chatID := e.putPublicGroup(nil, member)

	collection := plaintextCollection(time.Time{}, "Gift", "For Sam", 5)
	resp := e.createCollection(e.creator, chatID, collection, newKey())
	require.Equal(t, paymentrequestpb.CreatePaymentRequestResponse_OK, resp.Result)
	requireOpen(t, resp.PaymentRequest, 1)
	require.NoError(t, protoutil.ProtoEqualError(collection, resp.PaymentRequest.GetGroupChat().GetCollection()))
	require.Equal(t, []string{"Gift\nFor Sam"}, e.moderator.classifiedTexts())

	resp = e.createCollection(e.creator, chatID, fixedCollection(time.Time{}, 5), newKey())
	require.Equal(t, paymentrequestpb.CreatePaymentRequestResponse_OK, resp.Result)
	require.Len(t, e.moderator.classifiedTexts(), 1)

	require.Equal(t, paymentrequestpb.CreatePaymentRequestResponse_ENCRYPTION_NOT_ALLOWED, e.createCollection(e.creator, chatID, encryptedCollection(time.Time{}, 5), newKey()).Result)

	// Moderation is last: an invalid amount never reaches it.
	e.moderator.flag("Bad", "impersonation")
	key := newKey()
	require.Equal(t, paymentrequestpb.CreatePaymentRequestResponse_INVALID_AMOUNT, e.createCollection(e.creator, chatID, plaintextCollection(time.Time{}, "Bad", "", 0.001), key).Result)
	require.Len(t, e.moderator.classifiedTexts(), 1)
	resp = e.createCollection(e.creator, chatID, plaintextCollection(time.Time{}, "Bad", "", 1), key)
	require.Equal(t, paymentrequestpb.CreatePaymentRequestResponse_TEXT_MODERATED, resp.Result)
	require.Equal(t, moderationpb.FlaggedCategory_IMPERSONATION, resp.FlaggedCategory)
	_, err := s.Get(e.ctx, paymentrequest.MustDeriveID(e.creator.id, key))
	require.ErrorIs(t, err, paymentrequest.ErrNotFound)
}

// testServer_Create_Collection_Idempotent pins a retry of a collection's
// creation: answered with the original whatever it carries, a split
// included, and a split's retry answered with the split even when it carries
// a collection.
func testServer_Create_Collection_Idempotent(t *testing.T, s paymentrequest.Store) {
	e := newServerEnv(t, s)
	member := e.addUser()
	chatID := e.putPrivateGroup(true, member)

	key := newKey()
	first := e.createCollection(e.creator, chatID, encryptedCollection(time.Time{}, 5), key)
	require.Equal(t, paymentrequestpb.CreatePaymentRequestResponse_OK, first.Result)

	e.clock.advance(time.Minute)
	again := e.createCollection(e.creator, chatID, encryptedCollection(e.clock.Now().Add(-time.Hour), 0), key)
	require.Equal(t, paymentrequestpb.CreatePaymentRequestResponse_OK, again.Result)
	require.NoError(t, protoutil.ProtoEqualError(first.PaymentRequest, again.PaymentRequest))
	asSplit := e.create(e.creator, chatID, e.encryptedSplit(e.clock.Now().Add(time.Hour), share(member, 1)), key)
	require.Equal(t, paymentrequestpb.CreatePaymentRequestResponse_OK, asSplit.Result)
	require.NoError(t, protoutil.ProtoEqualError(first.PaymentRequest, asSplit.PaymentRequest))

	key = newKey()
	split := e.create(e.creator, chatID, e.encryptedSplit(e.clock.Now().Add(time.Hour), share(member, 1)), key)
	require.Equal(t, paymentrequestpb.CreatePaymentRequestResponse_OK, split.Result)
	asCollection := e.createCollection(e.creator, chatID, encryptedCollection(time.Time{}, 5), key)
	require.Equal(t, paymentrequestpb.CreatePaymentRequestResponse_OK, asCollection.Result)
	require.NoError(t, protoutil.ProtoEqualError(split.PaymentRequest, asCollection.PaymentRequest))

	// The announcement is the first write's alone.
	e.requireUpdates(chatID, first.PaymentRequest.PaymentRequestId, 1)
}

// testServer_Collection_Get pins a collection read: its summary on every
// read, the caller's Payer once they have contributed and not before (an
// intent ID issued is not a Payer), never the creator's, and never the legacy
// payment intent ID.
func testServer_Collection_Get(t *testing.T, s paymentrequest.Store) {
	e := newServerEnv(t, s)
	alice := e.addUser()
	bob := e.addUser()
	carol := e.addUser()
	chatID := e.putPrivateGroup(true, alice, bob, carol)

	created := e.mustCreateCollection(e.creator, chatID, encryptedCollection(e.clock.Now().Add(time.Hour), 0.1))
	id := created.PaymentRequestId

	for _, u := range []user{e.creator, alice, bob} {
		got := e.get(u, id)
		require.Equal(t, paymentrequestpb.GetPaymentRequestResponse_OK, got.Result)
		require.Nil(t, got.CallerPayer)
		require.Nil(t, got.PaymentIntentId)
	}

	// Asking for an intent ID makes no one a Payer.
	require.Equal(t, paymentrequestpb.GetPaymentIntentIdResponse_OK, e.getIntentID(alice, id).Result)
	require.Equal(t, paymentrequestpb.GetPaymentIntentIdResponse_OK, e.getIntentID(bob, id).Result)
	got := e.get(alice, id)
	require.Nil(t, got.CallerPayer)
	require.Nil(t, got.PaymentIntentId)
	requireOpen(t, got.PaymentRequest, 1)
	require.NoError(t, protoutil.ProtoEqualError(&paymentrequestpb.PayerSummary{}, got.PaymentRequest.PayerSummary))

	// Each contribution is the contributor's Payer, and counts in the summary,
	// summed exactly (0.1 three times is 0.3).
	paid := e.contribute(alice, id)
	got = e.get(alice, id)
	require.NoError(t, protoutil.ProtoEqualError(&paymentrequestpb.Payer{
		UserId:           alice.id,
		NativeAmountPaid: 0.1,
		Status:           &paymentrequestpb.Payer_Paid{Paid: paid},
	}, got.CallerPayer))
	require.Nil(t, got.PaymentIntentId)
	requireOpen(t, got.PaymentRequest, 2)
	require.Nil(t, e.get(bob, id).CallerPayer)
	require.Nil(t, e.get(e.creator, id).CallerPayer)

	e.clock.advance(time.Second)
	e.contribute(bob, id)
	e.contribute(carol, id)
	got = e.get(e.creator, id)
	require.Nil(t, got.CallerPayer)
	requireOpen(t, got.PaymentRequest, 4)
	wantSummary := &paymentrequestpb.PayerSummary{PayerCount: 3, PaidCount: 3, NativeAmountPaid: 0.3}
	require.NoError(t, protoutil.ProtoEqualError(wantSummary, got.PaymentRequest.PayerSummary))
	list := e.list(alice, chatID, paymentrequestpb.GetChatPaymentRequestsRequest_OPEN, 0, nil)
	require.Len(t, list.PaymentRequests, 1)
	require.NoError(t, protoutil.ProtoEqualError(wantSummary, list.PaymentRequests[0].PayerSummary))
	require.NotNil(t, e.get(bob, id).CallerPayer.GetPaid().GetIntent())
}

// testServer_Collection_GetPaymentIntentId pins every result for a
// collection, in the order they are decided, and the ID given: issued on the
// first call, the same on every call after, each caller's their own.
func testServer_Collection_GetPaymentIntentId(t *testing.T, s paymentrequest.Store) {
	e := newServerEnv(t, s)
	alice := e.addUser()
	bob := e.addUser()
	outsider := e.addUser()
	chatID := e.putPrivateGroup(true, alice, bob)

	created := e.mustCreateCollection(e.creator, chatID, encryptedCollection(e.clock.Now().Add(time.Hour), 5))
	id := created.PaymentRequestId

	require.Equal(t, paymentrequestpb.GetPaymentIntentIdResponse_NOT_FOUND, e.getIntentID(alice, newID()).Result)
	require.Equal(t, paymentrequestpb.GetPaymentIntentIdResponse_DENIED, e.getIntentID(outsider, id).Result)
	require.Equal(t, paymentrequestpb.GetPaymentIntentIdResponse_NOT_A_PAYER, e.getIntentID(e.creator, id).Result)
	_, err := s.GetPayerIntent(e.ctx, id, e.creator.id)
	require.ErrorIs(t, err, paymentrequest.ErrNotFound)

	resp := e.getIntentID(alice, id)
	require.Equal(t, paymentrequestpb.GetPaymentIntentIdResponse_OK, resp.Result)
	require.Len(t, resp.PaymentIntentId.GetValue(), paymentrequest.PaymentIntentIDSize)
	held, err := s.GetPayerIntent(e.ctx, id, alice.id)
	require.NoError(t, err)
	require.Equal(t, held.IntentID, resp.PaymentIntentId.Value)
	require.Nil(t, held.Paid)
	require.True(t, proto.Equal(resp.PaymentIntentId, e.getIntentID(alice, id).PaymentIntentId))
	bobs := e.getIntentID(bob, id)
	require.Equal(t, paymentrequestpb.GetPaymentIntentIdResponse_OK, bobs.Result)
	require.NotEqual(t, resp.PaymentIntentId.Value, bobs.PaymentIntentId.Value)

	// Issuing an ID is not a change to the request.
	stored, err := s.Get(e.ctx, id)
	require.NoError(t, err)
	require.EqualValues(t, 1, stored.Version)
	e.requireUpdates(chatID, id, 1)

	// A caller who has contributed is ALREADY_PAID whatever the status, and
	// is judged before it: after the close, alice, who contributed, is
	// ALREADY_PAID, and bob NOT_OPEN.
	e.contribute(alice, id)
	resp = e.getIntentID(alice, id)
	require.Equal(t, paymentrequestpb.GetPaymentIntentIdResponse_ALREADY_PAID, resp.Result)
	require.Nil(t, resp.PaymentIntentId)
	require.Equal(t, paymentrequestpb.ClosePaymentRequestResponse_OK, e.close(e.creator, id).Result)
	require.Equal(t, paymentrequestpb.GetPaymentIntentIdResponse_ALREADY_PAID, e.getIntentID(alice, id).Result)
	resp = e.getIntentID(bob, id)
	require.Equal(t, paymentrequestpb.GetPaymentIntentIdResponse_NOT_OPEN, resp.Result)
	require.Nil(t, resp.PaymentIntentId)

	// Nothing is issued for a request that is not open.
	carol := e.addUser()
	_, _, err = e.chats.AddGroupMembers(e.ctx, chatID, []*commonpb.UserId{carol.id})
	require.NoError(t, err)
	require.Equal(t, paymentrequestpb.GetPaymentIntentIdResponse_NOT_OPEN, e.getIntentID(carol, id).Result)
	_, err = s.GetPayerIntent(e.ctx, id, carol.id)
	require.ErrorIs(t, err, paymentrequest.ErrNotFound)

	// Past its ends_at, before the worker stores it expired, it is NOT_OPEN;
	// an undated collection never is.
	expiring := e.mustCreateCollection(e.creator, chatID, encryptedCollection(e.clock.Now().Add(time.Hour), 5))
	undated := e.mustCreateCollection(e.creator, chatID, encryptedCollection(time.Time{}, 5))
	require.Equal(t, paymentrequestpb.GetPaymentIntentIdResponse_OK, e.getIntentID(alice, expiring.PaymentRequestId).Result)
	e.clock.set(expiring.GetGroupChat().GetCollection().EndsAt.AsTime())
	require.Equal(t, paymentrequestpb.GetPaymentIntentIdResponse_NOT_OPEN, e.getIntentID(alice, expiring.PaymentRequestId).Result)
	e.clock.advance(10 * paymentrequest.MaxDuration)
	require.Equal(t, paymentrequestpb.GetPaymentIntentIdResponse_OK, e.getIntentID(alice, undated.PaymentRequestId).Result)

	// A member who has left the chat can no longer read it: DENIED, even
	// holding an ID already.
	_, _, err = e.chats.RemoveGroupMember(e.ctx, chatID, alice.id, false)
	require.NoError(t, err)
	require.Equal(t, paymentrequestpb.GetPaymentIntentIdResponse_DENIED, e.getIntentID(alice, undated.PaymentRequestId).Result)
}

// testServer_Collection_GetPaymentIntentId_PublicGroup pins the membership a
// collection asks of a payer beyond reading the chat: a non-member who
// satisfies a public group's listener rules sees the request but is DENIED an
// intent ID until they join. Public groups are refused on creation for now,
// so this runs under the option that lifts that.
func testServer_Collection_GetPaymentIntentId_PublicGroup(t *testing.T, s paymentrequest.Store) {
	e := newServerEnv(t, s, paymentrequest.WithPublicGroupRequests())
	member := e.addUser()
	listener := e.addUser()
	chatID := e.putPublicGroup(nil, member)

	created := e.mustCreateCollection(e.creator, chatID, plaintextCollection(time.Time{}, "Gift", "", 5))
	id := created.PaymentRequestId

	require.Equal(t, paymentrequestpb.GetPaymentRequestResponse_OK, e.get(listener, id).Result)
	require.Equal(t, paymentrequestpb.GetPayersResponse_OK, e.payers(listener, id, paymentrequestpb.GetPayersRequest_PAID, 0, nil).Result)
	require.Equal(t, paymentrequestpb.GetPaymentIntentIdResponse_DENIED, e.getIntentID(listener, id).Result)
	_, err := s.GetPayerIntent(e.ctx, id, listener.id)
	require.ErrorIs(t, err, paymentrequest.ErrNotFound)
	require.Equal(t, paymentrequestpb.GetPaymentIntentIdResponse_OK, e.getIntentID(member, id).Result)

	_, _, err = e.chats.AddGroupMembers(e.ctx, chatID, []*commonpb.UserId{listener.id})
	require.NoError(t, err)
	require.Equal(t, paymentrequestpb.GetPaymentIntentIdResponse_OK, e.getIntentID(listener, id).Result)
}

// testServer_Collection_GetPayers pins a collection's Payers: an UNPAID
// section that is always an empty page, and a PAID section of its
// contributions, newest first, ties by user ID, paged with tokens bound to
// the request and section, each page carrying the version read.
func testServer_Collection_GetPayers(t *testing.T, s paymentrequest.Store) {
	e := newServerEnv(t, s)
	outsider := e.addUser()
	members := make([]user, 5)
	for i := range members {
		members[i] = e.addUser()
	}
	chatID := e.putPrivateGroup(true, members...)

	created := e.mustCreateCollection(e.creator, chatID, encryptedCollection(time.Time{}, 2))
	id := created.PaymentRequestId
	other := e.mustCreateCollection(e.creator, chatID, encryptedCollection(time.Time{}, 2))

	unpaid := paymentrequestpb.GetPayersRequest_UNPAID
	paid := paymentrequestpb.GetPayersRequest_PAID
	for _, section := range []paymentrequestpb.GetPayersRequest_Section{unpaid, paid} {
		require.Equal(t, paymentrequestpb.GetPayersResponse_NOT_FOUND, e.payers(members[0], newID(), section, 0, nil).Result)
		require.Equal(t, paymentrequestpb.GetPayersResponse_DENIED, e.payers(outsider, id, section, 0, nil).Result)
		resp := e.payers(members[0], id, section, 0, nil)
		require.Equal(t, paymentrequestpb.GetPayersResponse_OK, resp.Result)
		require.Empty(t, resp.Payers)
		require.False(t, resp.HasMore)
		require.Nil(t, resp.PagingToken)
		require.EqualValues(t, 1, resp.Version)
	}

	// An intent ID issued is not a Payer.
	require.Equal(t, paymentrequestpb.GetPaymentIntentIdResponse_OK, e.getIntentID(members[4], id).Result)

	// members[3] first, then [0] and [1] at the same time, then [2].
	e.clock.advance(time.Minute)
	e.contribute(members[3], id)
	e.clock.advance(time.Minute)
	e.contribute(members[0], id)
	e.contribute(members[1], id)
	tied := []user{members[0], members[1]}
	if bytes.Compare(tied[0].id.Value, tied[1].id.Value) > 0 {
		tied = []user{tied[1], tied[0]}
	}
	e.clock.advance(time.Minute)
	e.contribute(members[2], id)
	want := []user{members[2], tied[0], tied[1], members[3]}

	for _, pageSize := range []int32{1, 2, 3, 4} {
		got := e.walkPayers(members[0], id, paid, pageSize, 5)
		requirePayerIDs(t, want, got)
		for _, p := range got {
			require.Zero(t, p.NativeAmountDue)
			require.Equal(t, 2.0, p.NativeAmountPaid)
			require.NotNil(t, p.GetPaid().GetIntent())
		}
	}
	resp := e.payers(members[0], id, paid, 0, nil)
	require.Len(t, resp.Payers, 4)
	require.False(t, resp.HasMore)
	require.EqualValues(t, 5, resp.Version)
	require.Equal(t, resp.Version, e.get(members[0], id).PaymentRequest.Version)

	// UNPAID stays empty whatever has been paid.
	resp = e.payers(members[0], id, unpaid, 0, nil)
	require.Equal(t, paymentrequestpb.GetPayersResponse_OK, resp.Result)
	require.Empty(t, resp.Payers)
	require.False(t, resp.HasMore)
	require.EqualValues(t, 5, resp.Version)

	// A cursor resumes strictly after its Payer while others contribute.
	first := e.payers(members[0], id, paid, 2, nil)
	requirePayerIDs(t, want[:2], first.Payers)
	require.True(t, first.HasMore)
	e.clock.advance(time.Minute)
	e.contribute(members[4], id)
	second := e.payers(members[0], id, paid, 2, first.PagingToken)
	require.Equal(t, paymentrequestpb.GetPayersResponse_OK, second.Result)
	requirePayerIDs(t, want[2:], second.Payers)
	require.False(t, second.HasMore)
	require.EqualValues(t, 6, second.Version)

	// A token is bound to its request and section, and refused with another
	// or when malformed.
	for _, tc := range []struct {
		name    string
		id      *paymentrequestpb.PaymentRequestId
		section paymentrequestpb.GetPayersRequest_Section
		token   *commonpb.PagingToken
	}{
		{"other request", other.PaymentRequestId, paid, first.PagingToken},
		{"other section", id, unpaid, first.PagingToken},
		{"truncated", id, paid, &commonpb.PagingToken{Value: first.PagingToken.Value[:10]}},
		{"garbage", id, paid, &commonpb.PagingToken{Value: bytes.Repeat([]byte{0xff}, len(first.PagingToken.Value))}},
	} {
		req := &paymentrequestpb.GetPayersRequest{
			PaymentRequestId: tc.id,
			Section:          tc.section,
			QueryOptions:     &commonpb.QueryOptions{PagingToken: tc.token},
		}
		require.NoError(t, members[0].keys.Auth(req, &req.Auth))
		_, err := e.client.GetPayers(e.ctx, req)
		require.Equal(t, codes.InvalidArgument, status.Code(err), tc.name)
	}
}

// testServer_Collection_GetPayers_VersionBeforePage pins the order of a
// collection's two reads: the request, whose version the page carries, and
// then the page. A contribution recorded between them is on the page while the
// version predates it, which a client applying updates past the version
// replaces by user ID to no effect.
func testServer_Collection_GetPayers_VersionBeforePage(t *testing.T, s paymentrequest.Store) {
	e := newServerEnv(t, s)
	alice := e.addUser()
	bob := e.addUser()
	chatID := e.putPrivateGroup(true, alice, bob)
	created := e.mustCreateCollection(e.creator, chatID, encryptedCollection(time.Time{}, 2))
	id := created.PaymentRequestId
	e.contribute(alice, id)

	bobs := e.getIntentID(bob, id)
	require.Equal(t, paymentrequestpb.GetPaymentIntentIdResponse_OK, bobs.Result)
	racing := &racingPayersStore{Store: s, beforePage: func() {
		_, changed, err := s.RecordContribution(e.ctx, id, bob.id, &paymentrequestpb.Paid{
			PaidAt: timestamppb.New(e.clock.Now().Add(time.Second)),
			How:    &paymentrequestpb.Paid_Intent{Intent: bobs.PaymentIntentId},
		}, e.clock.Now())
		assert.NoError(t, err)
		assert.True(t, changed)
	}}
	re := newServerEnvWith(t, racing, e)
	resp := re.payers(alice, id, paymentrequestpb.GetPayersRequest_PAID, 0, nil)
	require.Equal(t, paymentrequestpb.GetPayersResponse_OK, resp.Result)
	requirePayerIDs(t, []user{bob, alice}, resp.Payers)
	require.EqualValues(t, 2, resp.Version)
	require.EqualValues(t, 3, e.get(alice, id).PaymentRequest.Version)
}

// testServer_Collection_MarkPaid pins MarkPaid on a collection: NOT_A_SHARE
// for anyone, a contributor included, and nothing changed. Only the creator
// is told so; anyone else is DENIED first.
func testServer_Collection_MarkPaid(t *testing.T, s paymentrequest.Store) {
	e := newServerEnv(t, s)
	alice := e.addUser()
	bob := e.addUser()
	chatID := e.putPrivateGroup(true, alice, bob)
	created := e.mustCreateCollection(e.creator, chatID, encryptedCollection(time.Time{}, 2))
	id := created.PaymentRequestId
	e.contribute(alice, id)

	require.Equal(t, paymentrequestpb.MarkPaidResponse_DENIED, e.markPaid(alice, id, bob).Result)
	for _, u := range []user{alice, bob, e.creator} {
		resp := e.markPaid(e.creator, id, u)
		require.Equal(t, paymentrequestpb.MarkPaidResponse_NOT_A_SHARE, resp.Result)
		require.Nil(t, resp.PaymentRequest)
	}
	requireOpen(t, e.get(alice, id).PaymentRequest, 2)
	// The creation and the contribution, and nothing for the refusals.
	e.requireUpdates(chatID, id, 2)
}

// testServer_Collection_Close pins closing a collection, dated or not: the
// creator's alone, CLOSED at the next version, and a no-op once closed or
// past its ends_at.
func testServer_Collection_Close(t *testing.T, s paymentrequest.Store) {
	e := newServerEnv(t, s)
	member := e.addUser()
	chatID := e.putPrivateGroup(true, member)

	for _, endsAt := range []time.Time{{}, e.clock.Now().Add(time.Hour)} {
		created := e.mustCreateCollection(e.creator, chatID, encryptedCollection(endsAt, 1))
		id := created.PaymentRequestId
		e.contribute(member, id)

		require.Equal(t, paymentrequestpb.ClosePaymentRequestResponse_NOT_FOUND, e.close(e.creator, newID()).Result)
		require.Equal(t, paymentrequestpb.ClosePaymentRequestResponse_DENIED, e.close(member, id).Result)

		e.clock.advance(time.Minute)
		closedAt := e.clock.Now()
		resp := e.close(e.creator, id)
		require.Equal(t, paymentrequestpb.ClosePaymentRequestResponse_OK, resp.Result)
		requireResolved(t, resp.PaymentRequest, paymentrequestpb.PaymentRequestStatus_PAYMENT_REQUEST_STATUS_CLOSED, 3, closedAt)
		require.EqualValues(t, 1, resp.PaymentRequest.PayerSummary.PaidCount)

		e.clock.advance(time.Minute)
		resp = e.close(e.creator, id)
		require.Equal(t, paymentrequestpb.ClosePaymentRequestResponse_OK, resp.Result)
		requireResolved(t, resp.PaymentRequest, paymentrequestpb.PaymentRequestStatus_PAYMENT_REQUEST_STATUS_CLOSED, 3, closedAt)
	}

	// One the clock has expired stays EXPIRED, closed at its ends_at.
	expiring := e.mustCreateCollection(e.creator, chatID, encryptedCollection(e.clock.Now().Add(time.Hour), 1))
	endsAt := expiring.GetGroupChat().GetCollection().EndsAt.AsTime()
	e.clock.set(endsAt.Add(time.Second))
	resp := e.close(e.creator, expiring.PaymentRequestId)
	require.Equal(t, paymentrequestpb.ClosePaymentRequestResponse_OK, resp.Result)
	requireResolved(t, resp.PaymentRequest, paymentrequestpb.PaymentRequestStatus_PAYMENT_REQUEST_STATUS_EXPIRED, 1, endsAt)
}

// testServer_Collection_List pins where collections are listed: a dated one
// in OPEN like a split, an undated one in neither section while open, and in
// RESOLVED once closed.
func testServer_Collection_List(t *testing.T, s paymentrequest.Store) {
	e := newServerEnv(t, s)
	member := e.addUser()
	chatID := e.putPrivateGroup(true, member)

	start := e.clock.Now()
	split := e.mustCreate(e.creator, chatID, e.encryptedSplit(start.Add(2*time.Hour), share(member, 1)))
	dated := e.mustCreateCollection(e.creator, chatID, encryptedCollection(start.Add(time.Hour), 1))
	undated := e.mustCreateCollection(e.creator, chatID, encryptedCollection(time.Time{}, 1))

	open := paymentrequestpb.GetChatPaymentRequestsRequest_OPEN
	resolved := paymentrequestpb.GetChatPaymentRequestsRequest_RESOLVED
	gotOpen := e.walk(member, chatID, open, 1)
	requireRequestIDs(t, []*paymentrequestpb.PaymentRequestMetadata{dated, split}, gotOpen)
	require.NoError(t, protoutil.ProtoEqualError(dated, gotOpen[0]))
	require.Empty(t, e.walk(member, chatID, resolved, 1))

	// Contributions leave an undated collection unlisted.
	e.contribute(member, undated.PaymentRequestId)
	requireRequestIDs(t, []*paymentrequestpb.PaymentRequestMetadata{dated, split}, e.walk(member, chatID, open, 2))
	require.Empty(t, e.walk(member, chatID, resolved, 2))

	e.clock.advance(time.Minute)
	require.Equal(t, paymentrequestpb.ClosePaymentRequestResponse_OK, e.close(e.creator, undated.PaymentRequestId).Result)
	e.clock.advance(time.Minute)
	require.Equal(t, paymentrequestpb.ClosePaymentRequestResponse_OK, e.close(e.creator, dated.PaymentRequestId).Result)

	requireRequestIDs(t, []*paymentrequestpb.PaymentRequestMetadata{split}, e.walk(member, chatID, open, 2))
	gotResolved := e.walk(member, chatID, resolved, 1)
	requireRequestIDs(t, []*paymentrequestpb.PaymentRequestMetadata{dated, undated}, gotResolved)
	requireResolved(t, gotResolved[1], paymentrequestpb.PaymentRequestStatus_PAYMENT_REQUEST_STATUS_CLOSED, 3, start.Add(time.Minute))
	require.Nil(t, gotResolved[1].GetGroupChat().GetCollection().GetEndsAt())
	require.EqualValues(t, 1, gotResolved[1].PayerSummary.PayerCount)
}

// testServer_Collection_Updates pins what is announced of a collection: the
// request whole on creation, each contribution as the contributor's Payer and
// the summary (published as the requested payment task does, see
// paymentrequest.PublishContribution), and a close; so that applying them in
// order yields the request exactly as it is read. Issuing an intent ID, a
// repeated contribution and a refused MarkPaid announce nothing.
func testServer_Collection_Updates(t *testing.T, s paymentrequest.Store) {
	e := newServerEnv(t, s)
	alice := e.addUser()
	bob := e.addUser()
	chatID := e.putPrivateGroup(true, alice, bob)

	created := e.createCollection(e.creator, chatID, encryptedCollection(time.Time{}, 0.2), newKey())
	require.Equal(t, paymentrequestpb.CreatePaymentRequestResponse_OK, created.Result)
	id := created.PaymentRequest.PaymentRequestId
	updates := e.requireUpdates(chatID, id, 1)
	require.NoError(t, protoutil.ProtoEqualError(created.PaymentRequest, updates[0].GetCreated()))

	require.Equal(t, paymentrequestpb.GetPaymentIntentIdResponse_OK, e.getIntentID(alice, id).Result)
	require.Equal(t, paymentrequestpb.MarkPaidResponse_NOT_A_SHARE, e.markPaid(e.creator, id, alice).Result)
	e.requireUpdates(chatID, id, 1)

	e.clock.advance(time.Minute)
	paid := e.contribute(alice, id)
	updates = e.requireUpdates(chatID, id, 2)
	changes := updates[1].GetUpdates()
	require.EqualValues(t, 2, changes.GetVersion())
	require.Len(t, changes.GetUpdates(), 2)
	got := e.get(alice, id)
	require.NoError(t, protoutil.ProtoEqualError(got.CallerPayer, changes.Updates[0].GetPayerUpdate().GetPayer()))
	require.True(t, proto.Equal(paid, changes.Updates[0].GetPayerUpdate().GetPayer().GetPaid()))
	require.NoError(t, protoutil.ProtoEqualError(got.PaymentRequest.PayerSummary, changes.Updates[1].GetPayerSummaryUpdate().GetPayerSummary()))
	e.requireApplied(updates, got.PaymentRequest)

	// A repeat of the contribution is the store's no-op, and announces
	// nothing.
	_, changed, err := s.RecordContribution(e.ctx, id, alice.id, paid, e.clock.Now())
	require.NoError(t, err)
	require.False(t, changed)
	e.requireUpdates(chatID, id, 2)

	e.clock.advance(time.Minute)
	e.contribute(bob, id)
	updates = e.requireUpdates(chatID, id, 3)
	require.EqualValues(t, 2, updates[2].GetUpdates().GetUpdates()[1].GetPayerSummaryUpdate().GetPayerSummary().GetPaidCount())
	require.Equal(t, 0.4, updates[2].GetUpdates().GetUpdates()[1].GetPayerSummaryUpdate().GetPayerSummary().GetNativeAmountPaid())
	e.requireApplied(updates, e.get(bob, id).PaymentRequest)

	// A close is the status change alone.
	e.clock.advance(time.Minute)
	closed := e.close(e.creator, id)
	require.Equal(t, paymentrequestpb.ClosePaymentRequestResponse_OK, closed.Result)
	updates = e.requireUpdates(chatID, id, 4)
	require.Len(t, updates[3].GetUpdates().GetUpdates(), 1)
	require.Equal(t, paymentrequestpb.PaymentRequestStatus_PAYMENT_REQUEST_STATUS_CLOSED, statusUpdate(t, updates[3]).Status)
	e.requireApplied(updates, closed.PaymentRequest)
}

type user struct {
	id   *commonpb.UserId
	keys model.KeyPair
}

type serverEnv struct {
	t   *testing.T
	ctx context.Context

	client paymentrequestpb.PaymentRequestClient
	authz  *auth.StaticAuthorizer

	chats      chat.Store
	requests   paymentrequest.Store
	accounts   account.Store
	access     *chat.Access
	ocpBalance *fakeOcpBalance
	moderator  *fakeModerator
	clock      *fakeClock

	// chatEventBus is the chat-keyed bus of the server last started, and
	// chatObserver sees every event published on it.
	chatEventBus *event.Bus[*commonpb.ChatId, *eventpb.ChatEvent]
	chatObserver *event.TestEventObserver[*commonpb.ChatId, *eventpb.ChatEvent]

	// creator is the user who creates every group the env puts.
	creator user
}

func newServerEnv(t *testing.T, requests paymentrequest.Store, opts ...paymentrequest.ServerOption) *serverEnv {
	log := zaptest.NewLogger(t)

	chats := chatmemory.NewInMemory(nil)
	accounts := accountmemory.NewInMemory()
	ocpBalance := &fakeOcpBalance{byOwner: make(map[string]uint64)}
	balances := balance.NewClient(log, accounts, ocpBalance)

	// Nothing is remembered between asks, so a test's changes to balances and
	// rosters show on the next call.
	access := chat.NewAccess(
		chats,
		chat.NewRuleEvaluator(accounts, balances, chats, nil),
		chat.WithListenerAdmissionTTL(0),
		chat.WithSpeakerAdmissionTTL(0),
	)

	e := &serverEnv{
		t:          t,
		ctx:        context.Background(),
		authz:      auth.NewStaticAuthorizer(log),
		chats:      chats,
		accounts:   accounts,
		access:     access,
		ocpBalance: ocpBalance,
		moderator:  newFakeModerator(),
		clock:      &fakeClock{now: time.Now().UTC().Truncate(time.Millisecond)},
	}
	e.start(requests, opts...)
	e.creator = e.addUser()
	return e
}

// newServerEnvWith returns an env sharing base's chats, accounts, users and
// clock, serving requests.
func newServerEnvWith(t *testing.T, requests paymentrequest.Store, base *serverEnv, opts ...paymentrequest.ServerOption) *serverEnv {
	e := *base
	e.t = t
	e.start(requests, opts...)
	return &e
}

func (e *serverEnv) start(requests paymentrequest.Store, opts ...paymentrequest.ServerOption) {
	log := zaptest.NewLogger(e.t)
	e.requests = requests
	chatEventBus := event.NewBus[*commonpb.ChatId, *eventpb.ChatEvent]()
	e.chatEventBus = chatEventBus
	e.chatObserver = event.NewTestEventObserver[*commonpb.ChatId, *eventpb.ChatEvent]()
	chatEventBus.AddHandler(e.chatObserver)
	server := paymentrequest.NewServer(
		log,
		e.authz,
		requests,
		e.chats,
		e.access,
		e.moderator,
		chatEventBus,
		append([]paymentrequest.ServerOption{paymentrequest.WithClock(e.clock.Now)}, opts...)...,
	)
	cc := testutil.RunGRPCServer(e.t, log, testutil.WithService(func(s *grpc.Server) {
		paymentrequestpb.RegisterPaymentRequestServer(s, server)
	}))
	e.client = paymentrequestpb.NewPaymentRequestClient(cc)
}

func (e *serverEnv) addUser() user {
	u := user{id: model.MustGenerateUserID(), keys: model.MustGenerateKeyPair()}
	e.authz.Add(u.id, u.keys)
	return u
}

// fund binds u's key, if it isn't yet, and gives them amount USD.
func (e *serverEnv) fund(u user, amount uint64) {
	bound, err := e.accounts.GetPubKeys(e.ctx, u.id)
	require.NoError(e.t, err)
	if len(bound) == 0 {
		_, err = e.accounts.Bind(e.ctx, u.id, u.keys.Proto())
		require.NoError(e.t, err)
	}
	e.ocpBalance.setBalance(u.keys.Proto(), ocp_common.ToCoreMintQuarks(amount))
}

// putPublicGroup stores a public group created by the env's creator, with
// members joined after them, and configure applied to its record first.
func (e *serverEnv) putPublicGroup(configure func(*chat.Chat), members ...user) *commonpb.ChatId {
	c := &chat.Chat{
		ID:           chat.MustGenerateGroupChatID(),
		Type:         chatpb.ChatType_GROUP,
		Members:      []*commonpb.UserId{e.creator.id},
		Title:        "Group",
		CreatorID:    e.creator.id,
		LastActivity: time.Now(),
	}
	if configure != nil {
		configure(c)
	}
	return e.putGroup(c, members)
}

// putPrivateGroup stores a private group created by the env's creator, with
// members joined after them, and its key when keyed.
func (e *serverEnv) putPrivateGroup(keyed bool, members ...user) *commonpb.ChatId {
	chatID := e.putGroup(&chat.Chat{
		ID:           chat.MustGenerateGroupChatID(),
		Type:         chatpb.ChatType_GROUP,
		Members:      []*commonpb.UserId{e.creator.id},
		Title:        "Private",
		IsPrivate:    true,
		CreatorID:    e.creator.id,
		LastActivity: time.Now(),
	}, members)
	if keyed {
		envelope := &chatpb.KeyEnvelope{
			Scheme:     chatpb.KeyEnvelope_X25519_XCHACHA20POLY1305,
			Nonce:      bytes.Repeat([]byte{1}, 24),
			Ciphertext: bytes.Repeat([]byte{1}, 48),
		}
		_, err := e.chats.SetKeyEnvelope(e.ctx, chatID, e.creator.id, chat.KeyEnvelopeFromProto(envelope, e.creator.id))
		require.NoError(e.t, err)
	}
	return chatID
}

func (e *serverEnv) putGroup(c *chat.Chat, members []user) *commonpb.ChatId {
	require.NoError(e.t, e.chats.PutChat(e.ctx, c))
	if len(members) > 0 {
		ids := make([]*commonpb.UserId, len(members))
		for i, m := range members {
			ids[i] = m.id
		}
		_, _, err := e.chats.AddGroupMembers(e.ctx, c.ID, ids)
		require.NoError(e.t, err)
	}
	return c.ID
}

// encryptedSplit is a USD split ending at endsAt, its text encrypted under
// the chat key as far as the server can tell.
func (e *serverEnv) encryptedSplit(endsAt time.Time, shares ...*paymentrequestpb.Share) *paymentrequestpb.Split {
	return &paymentrequestpb.Split{
		Text: &paymentrequestpb.Split_Encrypted{Encrypted: &paymentrequestpb.EncryptedPaymentRequestText{
			Scheme:     paymentrequestpb.EncryptedPaymentRequestText_CHAT_KEY_XCHACHA20POLY1305,
			Nonce:      bytes.Repeat([]byte{2}, 24),
			Ciphertext: bytes.Repeat([]byte{3}, 64),
		}},
		Currency: "usd",
		EndsAt:   timestamppb.New(endsAt),
		Shares:   shares,
	}
}

// plaintextSplit is a USD split ending at endsAt with a plaintext title and
// description.
func plaintextSplit(endsAt time.Time, title, description string, shares ...*paymentrequestpb.Share) *paymentrequestpb.Split {
	return &paymentrequestpb.Split{
		Text:     plaintext(title, description),
		Currency: "usd",
		EndsAt:   timestamppb.New(endsAt),
		Shares:   shares,
	}
}

// noTextSplit is a USD split ending at endsAt with no text.
func noTextSplit(endsAt time.Time, shares ...*paymentrequestpb.Share) *paymentrequestpb.Split {
	return &paymentrequestpb.Split{
		Currency: "usd",
		EndsAt:   timestamppb.New(endsAt),
		Shares:   shares,
	}
}

func plaintext(title, description string) *paymentrequestpb.Split_Plaintext {
	return &paymentrequestpb.Split_Plaintext{Plaintext: &paymentrequestpb.PaymentRequestText{
		Title:       title,
		Description: description,
	}}
}

func withCurrency(split *paymentrequestpb.Split, currency string) *paymentrequestpb.Split {
	split.Currency = currency
	return split
}

func share(u user, amount float64) *paymentrequestpb.Share {
	return &paymentrequestpb.Share{UserId: u.id, NativeAmount: amount}
}

func shareOf(r *paymentrequestpb.PaymentRequestMetadata, u user) *paymentrequestpb.Share {
	for _, s := range r.GetGroupChat().GetSplit().GetShares() {
		if bytes.Equal(s.UserId.Value, u.id.Value) {
			return s
		}
	}
	return nil
}

func groupChatKind(chatID *commonpb.ChatId, split *paymentrequestpb.Split) *paymentrequestpb.CreatePaymentRequestRequest_GroupChat {
	return &paymentrequestpb.CreatePaymentRequestRequest_GroupChat{GroupChat: &paymentrequestpb.GroupChatPaymentRequest{
		ChatId: chatID,
		Type:   &paymentrequestpb.GroupChatPaymentRequest_Split{Split: split},
	}}
}

// fixedCollection is a USD collection asking amount of each payer, ending at
// endsAt, or never when endsAt is zero, with no text.
func fixedCollection(endsAt time.Time, amount float64) *paymentrequestpb.Collection {
	c := &paymentrequestpb.Collection{
		Currency: "usd",
		PaymentAmount: &paymentrequestpb.Collection_Fixed{Fixed: &paymentrequestpb.Collection_FixedAmount{
			NativeAmount: amount,
		}},
	}
	if !endsAt.IsZero() {
		c.EndsAt = timestamppb.New(endsAt)
	}
	return c
}

// encryptedCollection is fixedCollection with its text encrypted under the
// chat key as far as the server can tell.
func encryptedCollection(endsAt time.Time, amount float64) *paymentrequestpb.Collection {
	c := fixedCollection(endsAt, amount)
	c.Text = &paymentrequestpb.Collection_Encrypted{Encrypted: &paymentrequestpb.EncryptedPaymentRequestText{
		Scheme:     paymentrequestpb.EncryptedPaymentRequestText_CHAT_KEY_XCHACHA20POLY1305,
		Nonce:      bytes.Repeat([]byte{2}, 24),
		Ciphertext: bytes.Repeat([]byte{3}, 64),
	}}
	return c
}

// plaintextCollection is fixedCollection with a plaintext title and
// description.
func plaintextCollection(endsAt time.Time, title, description string, amount float64) *paymentrequestpb.Collection {
	c := fixedCollection(endsAt, amount)
	c.Text = collectionPlaintext(title, description)
	return c
}

func collectionPlaintext(title, description string) *paymentrequestpb.Collection_Plaintext {
	return &paymentrequestpb.Collection_Plaintext{Plaintext: &paymentrequestpb.PaymentRequestText{
		Title:       title,
		Description: description,
	}}
}

func collectionKind(chatID *commonpb.ChatId, collection *paymentrequestpb.Collection) *paymentrequestpb.CreatePaymentRequestRequest_GroupChat {
	return &paymentrequestpb.CreatePaymentRequestRequest_GroupChat{GroupChat: &paymentrequestpb.GroupChatPaymentRequest{
		ChatId: chatID,
		Type:   &paymentrequestpb.GroupChatPaymentRequest_Collection{Collection: collection},
	}}
}

func (e *serverEnv) createCollection(u user, chatID *commonpb.ChatId, collection *paymentrequestpb.Collection, key *paymentrequestpb.IdempotencyKey) *paymentrequestpb.CreatePaymentRequestResponse {
	req := &paymentrequestpb.CreatePaymentRequestRequest{
		IdempotencyKey: key,
		Kind:           collectionKind(chatID, collection),
	}
	require.NoError(e.t, u.keys.Auth(req, &req.Auth))
	resp, err := e.client.CreatePaymentRequest(e.ctx, req)
	require.NoError(e.t, err)
	return resp
}

func (e *serverEnv) mustCreateCollection(u user, chatID *commonpb.ChatId, collection *paymentrequestpb.Collection) *paymentrequestpb.PaymentRequestMetadata {
	resp := e.createCollection(u, chatID, collection, newKey())
	require.Equal(e.t, paymentrequestpb.CreatePaymentRequestResponse_OK, resp.Result)
	return resp.PaymentRequest
}

// contribute makes u's contribution to the collection id as the requested
// payment flow does: the intent ID from GetPaymentIntentId, which must be OK,
// then the store's record of a payment under it, paid now, announced as the
// task announces it when it changed the request. It returns the Paid
// recorded.
func (e *serverEnv) contribute(u user, id *paymentrequestpb.PaymentRequestId) *paymentrequestpb.Paid {
	e.t.Helper()
	resp := e.getIntentID(u, id)
	require.Equal(e.t, paymentrequestpb.GetPaymentIntentIdResponse_OK, resp.Result)
	paid := &paymentrequestpb.Paid{
		PaidAt: timestamppb.New(e.clock.Now()),
		How:    &paymentrequestpb.Paid_Intent{Intent: resp.PaymentIntentId},
	}
	r, changed, err := e.requests.RecordContribution(e.ctx, id, u.id, paid, e.clock.Now())
	require.NoError(e.t, err)
	require.True(e.t, changed)
	paymentrequest.PublishContribution(e.chatEventBus, r, u.id, paid, e.clock.Now())
	return paid
}

func newKey() *paymentrequestpb.IdempotencyKey {
	return &paymentrequestpb.IdempotencyKey{Value: model.MustGenerateUserID().Value}
}

func (e *serverEnv) create(u user, chatID *commonpb.ChatId, split *paymentrequestpb.Split, key *paymentrequestpb.IdempotencyKey) *paymentrequestpb.CreatePaymentRequestResponse {
	req := &paymentrequestpb.CreatePaymentRequestRequest{
		IdempotencyKey: key,
		Kind:           groupChatKind(chatID, split),
	}
	require.NoError(e.t, u.keys.Auth(req, &req.Auth))
	resp, err := e.client.CreatePaymentRequest(e.ctx, req)
	require.NoError(e.t, err)
	return resp
}

func (e *serverEnv) mustCreate(u user, chatID *commonpb.ChatId, split *paymentrequestpb.Split) *paymentrequestpb.PaymentRequestMetadata {
	resp := e.create(u, chatID, split, newKey())
	require.Equal(e.t, paymentrequestpb.CreatePaymentRequestResponse_OK, resp.Result)
	return resp.PaymentRequest
}

func (e *serverEnv) get(u user, id *paymentrequestpb.PaymentRequestId) *paymentrequestpb.GetPaymentRequestResponse {
	req := &paymentrequestpb.GetPaymentRequestRequest{PaymentRequestId: id}
	require.NoError(e.t, u.keys.Auth(req, &req.Auth))
	resp, err := e.client.GetPaymentRequest(e.ctx, req)
	require.NoError(e.t, err)
	return resp
}

func (e *serverEnv) list(u user, chatID *commonpb.ChatId, section paymentrequestpb.GetChatPaymentRequestsRequest_Section, pageSize int32, token *commonpb.PagingToken) *paymentrequestpb.GetChatPaymentRequestsResponse {
	req := &paymentrequestpb.GetChatPaymentRequestsRequest{
		ChatId:       chatID,
		Section:      section,
		QueryOptions: &commonpb.QueryOptions{PageSize: pageSize, PagingToken: token},
	}
	require.NoError(e.t, u.keys.Auth(req, &req.Auth))
	resp, err := e.client.GetChatPaymentRequests(e.ctx, req)
	require.NoError(e.t, err)
	return resp
}

// walk reads a whole section of chatID in pages of pageSize, checking every
// page but the last is full and carries a token.
func (e *serverEnv) walk(u user, chatID *commonpb.ChatId, section paymentrequestpb.GetChatPaymentRequestsRequest_Section, pageSize int32) []*paymentrequestpb.PaymentRequestMetadata {
	e.t.Helper()

	var (
		all   []*paymentrequestpb.PaymentRequestMetadata
		token *commonpb.PagingToken
	)
	for range 100 {
		resp := e.list(u, chatID, section, pageSize, token)
		require.Equal(e.t, paymentrequestpb.GetChatPaymentRequestsResponse_OK, resp.Result)
		require.LessOrEqual(e.t, len(resp.PaymentRequests), int(pageSize))
		all = append(all, resp.PaymentRequests...)
		if !resp.HasMore {
			require.Nil(e.t, resp.PagingToken)
			return all
		}
		require.Len(e.t, resp.PaymentRequests, int(pageSize))
		require.NotNil(e.t, resp.PagingToken)
		token = resp.PagingToken
	}
	e.t.Fatal("walk did not finish")
	return nil
}

func (e *serverEnv) close(u user, id *paymentrequestpb.PaymentRequestId) *paymentrequestpb.ClosePaymentRequestResponse {
	req := &paymentrequestpb.ClosePaymentRequestRequest{PaymentRequestId: id}
	require.NoError(e.t, u.keys.Auth(req, &req.Auth))
	resp, err := e.client.ClosePaymentRequest(e.ctx, req)
	require.NoError(e.t, err)
	return resp
}

func (e *serverEnv) markPaid(u user, id *paymentrequestpb.PaymentRequestId, debtor user) *paymentrequestpb.MarkPaidResponse {
	req := &paymentrequestpb.MarkPaidRequest{PaymentRequestId: id, UserId: debtor.id}
	require.NoError(e.t, u.keys.Auth(req, &req.Auth))
	resp, err := e.client.MarkPaid(e.ctx, req)
	require.NoError(e.t, err)
	return resp
}

func (e *serverEnv) getIntentID(u user, id *paymentrequestpb.PaymentRequestId) *paymentrequestpb.GetPaymentIntentIdResponse {
	req := &paymentrequestpb.GetPaymentIntentIdRequest{PaymentRequestId: id}
	require.NoError(e.t, u.keys.Auth(req, &req.Auth))
	resp, err := e.client.GetPaymentIntentId(e.ctx, req)
	require.NoError(e.t, err)
	return resp
}

func (e *serverEnv) payers(u user, id *paymentrequestpb.PaymentRequestId, section paymentrequestpb.GetPayersRequest_Section, pageSize int32, token *commonpb.PagingToken) *paymentrequestpb.GetPayersResponse {
	req := &paymentrequestpb.GetPayersRequest{
		PaymentRequestId: id,
		Section:          section,
		QueryOptions:     &commonpb.QueryOptions{PageSize: pageSize, PagingToken: token},
	}
	require.NoError(e.t, u.keys.Auth(req, &req.Auth))
	resp, err := e.client.GetPayers(e.ctx, req)
	require.NoError(e.t, err)
	return resp
}

// walkPayers reads a whole section of id's Payers in pages of pageSize,
// checking every page but the last is full and carries a token, and every
// page carries version.
func (e *serverEnv) walkPayers(u user, id *paymentrequestpb.PaymentRequestId, section paymentrequestpb.GetPayersRequest_Section, pageSize int32, version uint64) []*paymentrequestpb.Payer {
	e.t.Helper()

	var (
		all   []*paymentrequestpb.Payer
		token *commonpb.PagingToken
	)
	for range 100 {
		resp := e.payers(u, id, section, pageSize, token)
		require.Equal(e.t, paymentrequestpb.GetPayersResponse_OK, resp.Result)
		require.Equal(e.t, version, resp.Version)
		require.LessOrEqual(e.t, len(resp.Payers), int(pageSize))
		all = append(all, resp.Payers...)
		if !resp.HasMore {
			require.Nil(e.t, resp.PagingToken)
			return all
		}
		require.Len(e.t, resp.Payers, int(pageSize))
		require.NotNil(e.t, resp.PagingToken)
		token = resp.PagingToken
	}
	e.t.Fatal("walk did not finish")
	return nil
}

func requirePayerIDs(t *testing.T, want []user, got []*paymentrequestpb.Payer) {
	t.Helper()
	wantIDs := make([][]byte, len(want))
	for i, u := range want {
		wantIDs[i] = u.id.Value
	}
	gotIDs := make([][]byte, len(got))
	for i, p := range got {
		gotIDs[i] = p.UserId.Value
	}
	require.Equal(t, wantIDs, gotIDs)
}

func requireOpen(t *testing.T, r *paymentrequestpb.PaymentRequestMetadata, version uint64) {
	t.Helper()
	require.Equal(t, paymentrequestpb.PaymentRequestStatus_PAYMENT_REQUEST_STATUS_OPEN, r.Status)
	require.Equal(t, version, r.Version)
	require.Nil(t, r.ClosedAt)
}

func requireResolved(t *testing.T, r *paymentrequestpb.PaymentRequestMetadata, want paymentrequestpb.PaymentRequestStatus, version uint64, closedAt time.Time) {
	t.Helper()
	require.Equal(t, want, r.Status)
	require.Equal(t, version, r.Version)
	require.NotNil(t, r.ClosedAt)
	require.True(t, closedAt.Equal(r.ClosedAt.AsTime()), "closed at %v, want %v", r.ClosedAt.AsTime(), closedAt)
}

func requireRequestIDs(t *testing.T, want, got []*paymentrequestpb.PaymentRequestMetadata) {
	t.Helper()
	wantIDs := make([][]byte, len(want))
	for i, r := range want {
		wantIDs[i] = r.PaymentRequestId.Value
	}
	gotIDs := make([][]byte, len(got))
	for i, r := range got {
		gotIDs[i] = r.PaymentRequestId.Value
	}
	require.Equal(t, wantIDs, gotIDs)
}

// racingStore is a paymentrequest.Store that runs beforeCreate ahead of every
// Create, so a test can land a twin's write between the server's read and its
// own write.
// updates returns every PaymentRequestUpdate for id published on chatID's
// topic, in publish order, checking each publish's shape on the way: on the
// chat's topic, for that chat, excluding no one, one update per event, and
// valid by the proto's rules.
func (e *serverEnv) updates(chatID *commonpb.ChatId, id *paymentrequestpb.PaymentRequestId) []*paymentrequestpb.PaymentRequestUpdate {
	var updates []*paymentrequestpb.PaymentRequestUpdate
	for _, ev := range e.chatObserver.GetEvents(func(k *commonpb.ChatId) bool { return bytes.Equal(k.Value, chatID.Value) }) {
		require.Equal(e.t, chatID.Value, ev.Event.ChatId.Value)
		require.Empty(e.t, ev.Event.ExcludeUserIds)
		require.NoError(e.t, ev.Event.Event.ValidateAll())
		update := ev.Event.Event.GetChatUpdate()
		require.Equal(e.t, chatID.Value, update.GetChat().GetValue())
		batch := update.GetPaymentRequestUpdates().GetPaymentRequestUpdates()
		require.Len(e.t, batch, 1)
		if bytes.Equal(batch[0].PaymentRequestId.Value, id.Value) {
			updates = append(updates, batch[0])
		}
	}
	return updates
}

// requireUpdates waits for id's updates on chatID's topic to number n, then
// gives the bus a moment more and asserts no more arrived. The bus hands
// events to observers on their own goroutines, so a test asserts on them only
// after waiting.
func (e *serverEnv) requireUpdates(chatID *commonpb.ChatId, id *paymentrequestpb.PaymentRequestId, n int) []*paymentrequestpb.PaymentRequestUpdate {
	if n > 0 {
		e.chatObserver.WaitFor(e.t, func([]*event.KeyAndEvent[*commonpb.ChatId, *eventpb.ChatEvent]) bool {
			return len(e.updates(chatID, id)) >= n
		})
	}
	time.Sleep(100 * time.Millisecond)
	updates := e.updates(chatID, id)
	require.Len(e.t, updates, n)
	return updates
}

// requireApplied asserts that applying updates, in order, as a client would
// (see ApplyUpdates), yields want, the request as an RPC last returned it.
func (e *serverEnv) requireApplied(updates []*paymentrequestpb.PaymentRequestUpdate, want *paymentrequestpb.PaymentRequestMetadata) {
	require.NoError(e.t, protoutil.ProtoEqualError(want, ApplyUpdates(e.t, updates)))
}

// ApplyUpdates applies all, in order and none missing, to a request held
// by no one before, the way the proto tells a client to (see
// paymentrequestpb.PaymentRequestUpdate), and returns the result. The first
// must be the request's creation; each after it must be the change at the
// next version, and must apply cleanly to the copy so far.
func ApplyUpdates(t *testing.T, all []*paymentrequestpb.PaymentRequestUpdate) *paymentrequestpb.PaymentRequestMetadata {
	require.NotEmpty(t, all)
	created := all[0].GetCreated()
	require.NotNil(t, created, "first update is not the creation")
	require.Equal(t, all[0].PaymentRequestId.Value, created.PaymentRequestId.Value)
	local := proto.Clone(created).(*paymentrequestpb.PaymentRequestMetadata)

	for _, u := range all[1:] {
		require.Equal(t, local.PaymentRequestId.Value, u.PaymentRequestId.Value)
		updates := u.GetUpdates()
		require.NotNil(t, updates, "a later update is not a change")
		require.Equal(t, local.Version+1, updates.Version, "a change is not at the next version")
		// A change that records a payment is, in order, for a split the
		// share, then the Payer and the summary; a status change is at most
		// one, and last.
		var paidShare *paymentrequestpb.Share
		for i, c := range updates.Updates {
			switch kind := c.Kind.(type) {
			case *paymentrequestpb.PaymentRequestUpdate_Update_SharePaymentUpdate:
				require.Equal(t, 0, i, "a share payment that is not first")
				share := kind.SharePaymentUpdate.Share
				require.NotNil(t, share.Paid, "a paid share without paid")
				shares := local.GetGroupChat().GetSplit().GetShares()
				found := false
				for j, held := range shares {
					if bytes.Equal(held.UserId.Value, share.UserId.Value) {
						shares[j] = proto.Clone(share).(*paymentrequestpb.Share)
						found = true
					}
				}
				require.True(t, found, "a paid share the request does not have")
				paidShare = share
			case *paymentrequestpb.PaymentRequestUpdate_Update_PayerUpdate:
				if collection := local.GetGroupChat().GetCollection(); collection != nil {
					// A collection's contribution: the Payer alone, paid by
					// a payment of the fixed amount, with nothing due.
					require.Equal(t, 0, i, "a contribution's payer update that is not first")
					payer := kind.PayerUpdate.Payer
					require.NotNil(t, payer.GetPaid().GetIntent(), "a contribution not paid by a payment")
					require.Zero(t, payer.NativeAmountDue)
					require.Equal(t, collection.GetFixed().GetNativeAmount(), payer.NativeAmountPaid)
					break
				}
				require.Equal(t, 1, i, "a payer update that does not follow its share")
				require.NotNil(t, paidShare)
				require.NoError(t, protoutil.ProtoEqualError(PayerOfShare(paidShare), kind.PayerUpdate.Payer))
			case *paymentrequestpb.PaymentRequestUpdate_Update_PayerSummaryUpdate:
				require.Positive(t, i, "a summary update that is first")
				require.NotNil(t, updates.Updates[i-1].GetPayerUpdate(), "a summary update that does not follow its payer")
				local.PayerSummary = proto.Clone(kind.PayerSummaryUpdate.PayerSummary).(*paymentrequestpb.PayerSummary)
			case *paymentrequestpb.PaymentRequestUpdate_Update_StatusUpdate:
				require.Equal(t, len(updates.Updates)-1, i, "a status change that is not last")
				require.Equal(t, paymentrequestpb.PaymentRequestStatus_PAYMENT_REQUEST_STATUS_OPEN, local.Status, "a status change of a request not open")
				local.Status = kind.StatusUpdate.Status
				local.ClosedAt = proto.Clone(kind.StatusUpdate.ClosedAt).(*timestamppb.Timestamp)
			default:
				require.Fail(t, "unknown change kind")
			}
		}
		local.Version = updates.Version
	}
	return local
}

// sharePaymentUpdate returns the share an update pays when a share payment
// is all it records (the share, its Payer and the summary, with no status
// change), and fails otherwise.
func sharePaymentUpdate(t *testing.T, u *paymentrequestpb.PaymentRequestUpdate) *paymentrequestpb.Share {
	changes := u.GetUpdates().GetUpdates()
	require.Len(t, changes, 3)
	require.NotNil(t, changes[0].GetSharePaymentUpdate())
	require.NotNil(t, changes[1].GetPayerUpdate())
	require.NotNil(t, changes[2].GetPayerSummaryUpdate())
	return changes[0].GetSharePaymentUpdate().Share
}

// PayerOfShare is the Payer a split's share is, as the proto defines it (see
// paymentrequestpb.Payer), computed here independently of the server.
func PayerOfShare(share *paymentrequestpb.Share) *paymentrequestpb.Payer {
	payer := &paymentrequestpb.Payer{
		UserId:          share.UserId,
		NativeAmountDue: share.NativeAmount,
		Status:          &paymentrequestpb.Payer_Unpaid_{Unpaid: &paymentrequestpb.Payer_Unpaid{}},
	}
	if share.Paid != nil {
		payer.NativeAmountPaid = share.NativeAmount
		payer.Status = &paymentrequestpb.Payer_Paid{Paid: share.Paid}
	}
	return payer
}

// statusUpdate returns the status update that is an update's last, and
// fails when it carries none.
func statusUpdate(t *testing.T, u *paymentrequestpb.PaymentRequestUpdate) *paymentrequestpb.PaymentRequestUpdate_StatusUpdate {
	changes := u.GetUpdates().GetUpdates()
	require.NotEmpty(t, changes)
	status := changes[len(changes)-1].GetStatusUpdate()
	require.NotNil(t, status)
	return status
}

type racingStore struct {
	paymentrequest.Store
	beforeCreate func()
}

func (r *racingStore) Create(ctx context.Context, req *paymentrequest.PaymentRequest) error {
	r.beforeCreate()
	return r.Store.Create(ctx, req)
}

// racingPayersStore is a paymentrequest.Store that runs beforePage ahead of
// every GetCollectionPayersPage, so a test can land a contribution between
// the server's read of the request and its read of the page.
type racingPayersStore struct {
	paymentrequest.Store
	beforePage func()
}

func (r *racingPayersStore) GetCollectionPayersPage(ctx context.Context, id *paymentrequestpb.PaymentRequestId, cursor *paymentrequest.PayerCursor, limit int) ([]*paymentrequestpb.Payer, error) {
	r.beforePage()
	return r.Store.GetCollectionPayersPage(ctx, id, cursor, limit)
}

// fakeClock is the server's clock, which a test moves.
type fakeClock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fakeClock) advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

func (c *fakeClock) set(now time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = now
}

// fakeModerator is a moderation.Client that classifies every text as clean,
// except the texts a test flags or fails, and records every text it was asked
// about, in order. The server classifies text alone: every other
// classification is clean.
type fakeModerator struct {
	mu         sync.Mutex
	flagged    map[string][]string
	failures   map[string]error
	classified []string
}

func newFakeModerator() *fakeModerator {
	return &fakeModerator{
		flagged:  make(map[string][]string),
		failures: make(map[string]error),
	}
}

func (m *fakeModerator) flag(text string, categories ...string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.flagged[text] = categories
}

func (m *fakeModerator) fail(text string, err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.failures[text] = err
}

// classifiedCount is how many requests reached moderation: each classifies
// its text once.
func (m *fakeModerator) classifiedCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.classified)
}

func (m *fakeModerator) classifiedTexts() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]string(nil), m.classified...)
}

func (m *fakeModerator) ClassifyImage(context.Context, []byte) (*moderation.Result, error) {
	return &moderation.Result{}, nil
}

func (m *fakeModerator) ClassifyCurrencyName(context.Context, string) (*moderation.Result, error) {
	return &moderation.Result{}, nil
}

func (m *fakeModerator) ClassifyUsername(context.Context, string) (*moderation.Result, error) {
	return &moderation.Result{}, nil
}

func (m *fakeModerator) ClassifyDisplayName(context.Context, string) (*moderation.Result, error) {
	return &moderation.Result{}, nil
}

func (m *fakeModerator) ClassifyGroupTitle(context.Context, string) (*moderation.Result, error) {
	return &moderation.Result{}, nil
}

// ClassifyText flags a text with its categories scored in ascending order, so
// the last listed is the highest.
func (m *fakeModerator) ClassifyText(_ context.Context, text string) (*moderation.Result, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.classified = append(m.classified, text)
	if err, ok := m.failures[text]; ok {
		return nil, err
	}
	if categories, ok := m.flagged[text]; ok {
		scores := make(map[string]float64, len(categories))
		for i, category := range categories {
			scores[category] = float64(i + 1)
		}
		return &moderation.Result{Flagged: true, FlaggedCategories: categories, CategoryScores: scores}, nil
	}
	return &moderation.Result{}, nil
}

// fakeOcpBalance is the OCP balance service behind the env's balance client,
// answering each owner's total from a ledger of USDF quarks a test sets. It
// ignores the request's mint filter and prices no fiat currency.
type fakeOcpBalance struct {
	mu      sync.Mutex
	byOwner map[string]uint64
}

func (f *fakeOcpBalance) setBalance(owner *commonpb.PublicKey, coreMintValue uint64) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.byOwner[base58.Encode(owner.Value)] = coreMintValue
}

func (f *fakeOcpBalance) GetBalances(_ context.Context, req *ocp_balancepb.GetBalancesRequest, _ ...grpc.CallOption) (*ocp_balancepb.GetBalancesResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	resp := &ocp_balancepb.GetBalancesResponse{BalancesByOwner: make(map[string]*ocp_balancepb.OwnerBalance)}
	for _, owner := range req.Owners {
		key := base58.Encode(owner.Value)
		value, ok := f.byOwner[key]
		if !ok {
			continue
		}
		resp.BalancesByOwner[key] = &ocp_balancepb.OwnerBalance{
			Owner:         owner,
			CoreMintValue: value,
		}
	}
	return resp, nil
}
