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
	"github.com/code-payments/flipcash2-server/model"
	"github.com/code-payments/flipcash2-server/moderation"
	"github.com/code-payments/flipcash2-server/paymentrequest"
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
		testServer_Get,
		testServer_Get_PaymentIntentID,
		testServer_List_Gates,
		testServer_List_Paging,
		testServer_List_ClockExpired,
		testServer_Close,
		testServer_MarkPaid,
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

	// Any staff member who can speak creates one, not just the group's creator.
	e.accounts.setStaff(alice.id, true)
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
	// a DM, an ends_at in the past and shares that would be refused, and
	// after the caller has lost their staff flag: nothing is checked again.
	e.clock.advance(time.Minute)
	e.accounts.setStaff(e.creator.id, false)
	dmID := &commonpb.ChatId{Value: bytes.Repeat([]byte{1}, chat.DmChatIDSize)}
	retried := e.encryptedSplit(e.clock.Now().Add(-time.Hour), share(e.creator, 5))
	again := e.create(e.creator, dmID, retried, key)
	require.Equal(t, paymentrequestpb.CreatePaymentRequestResponse_OK, again.Result)
	require.True(t, proto.Equal(first.PaymentRequest, again.PaymentRequest))

	// The same key from someone else is someone else's request.
	e.accounts.setStaff(alice.id, true)
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
	e.accounts.setStaff(e.creator.id, true)
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
	staffOutsider := e.addUser()
	e.accounts.setStaff(staffOutsider.id, true)

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
		{"not staff", member, keyed, with(func(s *paymentrequestpb.Split) { s.Shares = []*paymentrequestpb.Share{share(other, 1)} }), paymentrequestpb.CreatePaymentRequestResponse_DENIED},
		{"dm", e.creator, dmID, valid(), paymentrequestpb.CreatePaymentRequestResponse_CHAT_NOT_SUPPORTED},
		{"non-member", staffOutsider, keyed, valid(), paymentrequestpb.CreatePaymentRequestResponse_DENIED},
		{"missing chat", e.creator, chat.MustGenerateGroupChatID(), valid(), paymentrequestpb.CreatePaymentRequestResponse_DENIED},
		{"keyless private group", e.creator, keyless, valid(), paymentrequestpb.CreatePaymentRequestResponse_DENIED},
		{"public group", e.creator, public, with(func(s *paymentrequestpb.Split) { s.Text = plaintext("Dinner", "") }), paymentrequestpb.CreatePaymentRequestResponse_CHAT_NOT_SUPPORTED},
		{"plaintext in private group", e.creator, keyed, with(func(s *paymentrequestpb.Split) { s.Text = plaintext("Dinner", "") }), paymentrequestpb.CreatePaymentRequestResponse_ENCRYPTION_REQUIRED},
		{"ends now", e.creator, keyed, with(func(s *paymentrequestpb.Split) { s.EndsAt = timestamppb.New(e.clock.Now()) }), paymentrequestpb.CreatePaymentRequestResponse_INVALID_ENDS_AT},
		{"ended", e.creator, keyed, with(func(s *paymentrequestpb.Split) { s.EndsAt = timestamppb.New(e.clock.Now().Add(-time.Second)) }), paymentrequestpb.CreatePaymentRequestResponse_INVALID_ENDS_AT},
		{"unsupported currency", e.creator, keyed, with(func(s *paymentrequestpb.Split) { s.Currency = "xyz" }), paymentrequestpb.CreatePaymentRequestResponse_INVALID_CURRENCY},
		{"share paid", e.creator, keyed, with(func(s *paymentrequestpb.Split) {
			s.Shares[1].Paid = &paymentrequestpb.PaidShare{
				PaidAt: timestamppb.New(e.clock.Now()),
				How:    &paymentrequestpb.PaidShare_Manual_{Manual: &paymentrequestpb.PaidShare_Manual{}},
			}
		}), paymentrequestpb.CreatePaymentRequestResponse_INVALID_SHARES},
		{"creator's share paid", e.creator, keyed, with(func(s *paymentrequestpb.Split) {
			s.Shares[0].Paid = &paymentrequestpb.PaidShare{
				PaidAt: timestamppb.New(e.clock.Now()),
				How:    &paymentrequestpb.PaidShare_Creator_{Creator: &paymentrequestpb.PaidShare_Creator{}},
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
	e.accounts.setStaff(outsider.id, true)
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
		// Staff is judged before the chat is looked at at all.
		{"not staff, dm", member, dmID, e.encryptedSplit(future, share(e.creator, 1)), paymentrequestpb.CreatePaymentRequestResponse_DENIED},
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

	// The staff gate still holds.
	require.Equal(t, paymentrequestpb.CreatePaymentRequestResponse_DENIED, e.create(member, chatID, plaintextSplit(e.clock.Now().Add(time.Hour), "Dinner", "", share(e.creator, 1)), newKey()).Result)

	// A creator-only group lets nobody but its creator speak, so nobody but
	// its creator creates a request in it.
	e.accounts.setStaff(member.id, true)
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
	accounts   *staffAccounts
	access     *chat.Access
	ocpBalance *fakeOcpBalance
	moderator  *fakeModerator
	clock      *fakeClock

	// creator is the staff user who creates every group the env puts.
	creator user
}

func newServerEnv(t *testing.T, requests paymentrequest.Store, opts ...paymentrequest.ServerOption) *serverEnv {
	log := zaptest.NewLogger(t)

	chats := chatmemory.NewInMemory(nil)
	accounts := newStaffAccounts(accountmemory.NewInMemory())
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
	e.accounts.setStaff(e.creator.id, true)
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
	server := paymentrequest.NewServer(
		log,
		e.authz,
		requests,
		e.accounts,
		e.chats,
		e.access,
		e.moderator,
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
type racingStore struct {
	paymentrequest.Store
	beforeCreate func()
}

func (r *racingStore) Create(ctx context.Context, req *paymentrequest.PaymentRequest) error {
	r.beforeCreate()
	return r.Store.Create(ctx, req)
}

// staffAccounts is an account.Store whose staff flag a test can set: the
// in-memory store answers IsStaff false for everyone, and has no setter.
type staffAccounts struct {
	account.Store

	mu    sync.Mutex
	staff map[string]bool
}

func newStaffAccounts(db account.Store) *staffAccounts {
	return &staffAccounts{Store: db, staff: make(map[string]bool)}
}

func (a *staffAccounts) setStaff(userID *commonpb.UserId, isStaff bool) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.staff[string(userID.Value)] = isStaff
}

func (a *staffAccounts) IsStaff(_ context.Context, userID *commonpb.UserId) (bool, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.staff[string(userID.Value)], nil
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
