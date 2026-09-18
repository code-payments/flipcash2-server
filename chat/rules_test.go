package chat

import (
	"bytes"
	"context"
	"errors"
	"math"
	"testing"

	"github.com/mr-tron/base58"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"
	"google.golang.org/grpc"

	chatpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/chat/v1"
	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"
	ocp_balancepb "github.com/code-payments/ocp-protobuf-api/generated/go/balance/v1"

	ocp_common "github.com/code-payments/ocp-server/ocp/common"

	"github.com/code-payments/flipcash2-server/account"
	"github.com/code-payments/flipcash2-server/balance"
	"github.com/code-payments/flipcash2-server/model"
	"github.com/code-payments/flipcash2-server/protoutil"
)

// fakeAccounts is an account.Store that answers IsStaff from a set, records
// who was asked, and answers GetPubKeys from a map of bound keys — what the
// balance client needs to resolve a user to an owner account. Nothing else is
// exercised; the embedded nil interface makes any other call panic, which is
// the intent.
type fakeAccounts struct {
	account.Store

	staff map[string]bool
	asked int
	err   error

	keys map[string]*commonpb.PublicKey
}

func (f *fakeAccounts) IsStaff(_ context.Context, userID *commonpb.UserId) (bool, error) {
	f.asked++
	if f.err != nil {
		return false, f.err
	}
	return f.staff[string(userID.Value)], nil
}

func (f *fakeAccounts) GetPubKeys(_ context.Context, userID *commonpb.UserId) ([]*commonpb.PublicKey, error) {
	key, ok := f.keys[string(userID.Value)]
	if !ok {
		return nil, nil
	}
	return []*commonpb.PublicKey{key}, nil
}

// bind gives userID an owner key and returns it, so a balance can be recorded
// against it in fakeOcpBalance.
func (f *fakeAccounts) bind(userID *commonpb.UserId) *commonpb.PublicKey {
	key := model.MustGenerateKeyPair().Proto()
	f.keys[string(userID.Value)] = key
	return key
}

// fakeOcpBalance is the OCP balance service behind a real balance.Client,
// answering from a per-owner, per-mint ledger in USDF quarks and honouring the
// request's mint filter the way OCP does: the owner's total covers only the
// requested mints. It values the total in each requested currency it has a
// rate for, and silently not in one it lacks, as OCP answers a code it cannot
// price. It records how often it was asked.
type fakeOcpBalance struct {
	// ledger is owner base58 -> mint base58 -> quarks.
	ledger map[string]map[string]uint64
	// rates values a USDF unit in each fiat currency the fake can price.
	rates map[string]float64
	asked int
	err   error
}

func (f *fakeOcpBalance) set(owner, mint *commonpb.PublicKey, quarks uint64) {
	holdings, ok := f.ledger[base58.Encode(owner.Value)]
	if !ok {
		holdings = make(map[string]uint64)
		f.ledger[base58.Encode(owner.Value)] = holdings
	}
	holdings[base58.Encode(mint.Value)] = quarks
}

func (f *fakeOcpBalance) GetBalances(_ context.Context, req *ocp_balancepb.GetBalancesRequest, _ ...grpc.CallOption) (*ocp_balancepb.GetBalancesResponse, error) {
	f.asked++
	if f.err != nil {
		return nil, f.err
	}
	requested := make(map[string]bool, len(req.Mints))
	for _, mint := range req.Mints {
		requested[base58.Encode(mint.Value)] = true
	}
	resp := &ocp_balancepb.GetBalancesResponse{BalancesByOwner: make(map[string]*ocp_balancepb.OwnerBalance)}
	for _, owner := range req.Owners {
		holdings, ok := f.ledger[base58.Encode(owner.Value)]
		if !ok {
			continue // An owner OCP has no accounts for is left out.
		}
		ownerBalance := &ocp_balancepb.OwnerBalance{Owner: owner}
		for mint, quarks := range holdings {
			if len(requested) == 0 || requested[mint] {
				ownerBalance.CoreMintValue += quarks
			}
		}
		for _, code := range req.CurrencyCodes {
			rate, ok := f.rates[code]
			if !ok {
				continue
			}
			if ownerBalance.FiatValuesByCurrency == nil {
				ownerBalance.FiatValuesByCurrency = make(map[string]float64)
			}
			ownerBalance.FiatValuesByCurrency[code] = float64(ownerBalance.CoreMintValue) / float64(ocp_common.CoreMintQuarksPerUnit) * rate
		}
		resp.BalancesByOwner[base58.Encode(owner.Value)] = ownerBalance
	}
	return resp, nil
}

// fakeChats is a Store that serves rules from a map of group records and
// membership from a set, counting the reads of each, so a test can see which
// evaluations touched the store. Only GetGroupRules and IsMember are
// exercised, as above.
type fakeChats struct {
	Store

	chats map[string]*Chat
	reads int

	members         map[string]bool
	membershipReads int
}

func newFakeChats() *fakeChats {
	return &fakeChats{chats: make(map[string]*Chat), members: make(map[string]bool)}
}

func (f *fakeChats) put(c *Chat) *Chat {
	f.chats[string(c.ID.Value)] = c
	return c
}

func (f *fakeChats) join(chatID *commonpb.ChatId, userID *commonpb.UserId) {
	f.members[string(chatID.Value)+string(userID.Value)] = true
}

func (f *fakeChats) leave(chatID *commonpb.ChatId, userID *commonpb.UserId) {
	delete(f.members, string(chatID.Value)+string(userID.Value))
}

func (f *fakeChats) IsMember(_ context.Context, chatID *commonpb.ChatId, userID *commonpb.UserId) (bool, error) {
	f.membershipReads++
	return f.members[string(chatID.Value)+string(userID.Value)], nil
}

func (f *fakeChats) GetGroupRules(_ context.Context, chatID *commonpb.ChatId) (*chatpb.Rules, error) {
	f.reads++
	if !IsGroupChatID(chatID) {
		return nil, errors.New("not a group chat id")
	}
	c, ok := f.chats[string(chatID.Value)]
	if !ok {
		return nil, ErrChatNotFound
	}
	return c.Rules(), nil
}

// TestRulesFromProto_MinimumTransferValue pins the floor on a minimum balance
// requirement to the currency's minimum transfer value: a penny for USD. The
// floor is inclusive, and it is a floor on what was asked for — not the
// half-unit rounding slack OCP allows on a fiat amount derived from a rate,
// since a requirement is chosen, not quoted.
func TestRulesFromProto_MinimumTransferValue(t *testing.T) {
	rules := func(amount float64) *chatpb.Rules {
		return &chatpb.Rules{Listener: []*chatpb.ListenerRules{{Kind: &chatpb.ListenerRules_MinimumBalance{MinimumBalance: &chatpb.MinimumBalanceRequirement{
			Amount: &commonpb.FiatPaymentAmount{Currency: "usd", NativeAmount: amount},
		}}}}}
	}

	for _, amount := range []float64{0.01, 0.011, 1, 100} {
		_, minimum, err := RulesFromProto(rules(amount))
		require.NoError(t, err, "%v", amount)
		require.Equal(t, amount, minimum.NativeAmount)
	}
	for _, amount := range []float64{0.009, 0.005, 0.0099999, 0, -0.01, -1, math.NaN(), math.Inf(1), math.Inf(-1)} {
		_, _, err := RulesFromProto(rules(amount))
		require.ErrorIs(t, err, ErrInvalidRules, "%v", amount)
	}

	require.Equal(t, 0.01, minimumTransferValue("usd"))
	require.Equal(t, 1.0, minimumTransferValue("jpy"))
	require.Equal(t, 0.001, minimumTransferValue("kwd"))

	// The floor follows the currency: a yen for JPY, a fils for KWD. The
	// currency itself is stored as asked; whether OCP can value a balance in
	// it is not validation's question.
	in := func(currency string, amount float64) *chatpb.Rules {
		return &chatpb.Rules{Listener: []*chatpb.ListenerRules{{Kind: &chatpb.ListenerRules_MinimumBalance{MinimumBalance: &chatpb.MinimumBalanceRequirement{
			Amount: &commonpb.FiatPaymentAmount{Currency: currency, NativeAmount: amount},
		}}}}}
	}
	for _, tc := range []struct {
		currency string
		amount   float64
	}{{"jpy", 1}, {"jpy", 1.5}, {"kwd", 0.001}, {"eur", 0.01}, {"xyz", 0.01}} {
		_, minimum, err := RulesFromProto(in(tc.currency, tc.amount))
		require.NoError(t, err, "%s %v", tc.currency, tc.amount)
		require.Equal(t, tc.currency, minimum.Currency)
		require.Equal(t, tc.amount, minimum.NativeAmount)
	}
	for _, tc := range []struct {
		currency string
		amount   float64
	}{{"jpy", 0.5}, {"jpy", 0.99}, {"kwd", 0.0009}, {"eur", 0.009}, {"", 1}} {
		_, _, err := RulesFromProto(in(tc.currency, tc.amount))
		require.ErrorIs(t, err, ErrInvalidRules, "%s %v", tc.currency, tc.amount)
	}
}

func TestChat_Rules(t *testing.T) {
	a := model.MustGenerateUserID()
	b := model.MustGenerateUserID()

	// A DM never carries rules, and neither does a group not created with one.
	dm := &Chat{ID: MustDeriveDmChatID(chatpb.ChatType_CONTACT_DM, a, b), Type: chatpb.ChatType_CONTACT_DM, Members: []*commonpb.UserId{a, b}}
	require.Nil(t, dm.Rules())
	require.Nil(t, dm.ToProto().GetRules())

	plain := &Chat{ID: MustGenerateGroupChatID(), Type: chatpb.ChatType_GROUP, Title: "Weekend Trip"}
	require.Nil(t, plain.Rules())
	require.Nil(t, plain.ToProto().GetRules())

	// A staff-only group is exactly one listener rule, StaffRequirement, and no
	// speaker rules: restricting the audience already restricts the speakers.
	staffOnly := &Chat{ID: MustGenerateGroupChatID(), Type: chatpb.ChatType_GROUP, Title: "Staff", IsStaffOnly: true}
	rules := staffOnly.Rules()
	require.Len(t, rules.GetListener(), 1)
	require.NotNil(t, rules.GetListener()[0].GetStaff())
	require.Empty(t, rules.GetSpeaker())
	require.NoError(t, rules.Validate())

	// The projection onto Metadata carries the same rules.
	require.NoError(t, protoutil.ProtoEqualError(rules, staffOnly.ToProto().GetRules()))

	// A minimum listener balance is one listener rule too, carrying the
	// requirement as stored: the fiat amount, and the mints it may be held in.
	usdfMint := &commonpb.PublicKey{Value: bytes.Repeat([]byte{7}, 32)}
	gated := &Chat{ID: MustGenerateGroupChatID(), Type: chatpb.ChatType_GROUP, Title: "Whales", MinimumListenerBalance: &MinimumBalance{
		Currency:     "usd",
		NativeAmount: 250.5,
		Mints:        []*commonpb.PublicKey{usdfMint},
	}}
	rules = gated.Rules()
	require.Len(t, rules.GetListener(), 1)
	require.Empty(t, rules.GetSpeaker())
	require.NoError(t, rules.Validate())
	balance := rules.GetListener()[0].GetMinimumBalance()
	require.NotNil(t, balance)
	require.Equal(t, "usd", balance.GetAmount().GetCurrency())
	require.Equal(t, 250.5, balance.GetAmount().GetNativeAmount())
	require.Len(t, balance.GetMints(), 1)
	require.Equal(t, usdfMint.Value, balance.GetMints()[0].GetValue())
	require.NoError(t, protoutil.ProtoEqualError(rules, gated.ToProto().GetRules()))

	// No mints is "any mint": the projection carries an empty list, not a nil
	// requirement.
	anyMint := &Chat{ID: MustGenerateGroupChatID(), Type: chatpb.ChatType_GROUP, MinimumListenerBalance: &MinimumBalance{Currency: "usd", NativeAmount: 1}}
	rules = anyMint.Rules()
	require.Len(t, rules.GetListener(), 1)
	require.Empty(t, rules.GetListener()[0].GetMinimumBalance().GetMints())
	require.NoError(t, rules.Validate())

	// Both requirements together are two listener rules, cheapest first: the
	// staff flag before the balance.
	both := gated.Clone()
	both.IsStaffOnly = true
	rules = both.Rules()
	require.Len(t, rules.GetListener(), 2)
	require.NotNil(t, rules.GetListener()[0].GetStaff())
	require.NotNil(t, rules.GetListener()[1].GetMinimumBalance())
	require.Empty(t, rules.GetSpeaker())
	require.NoError(t, rules.Validate())

	// The projection is a copy: mutating the rules leaves the record alone.
	rules.GetListener()[1].GetMinimumBalance().GetMints()[0].Value[0] = 0
	require.Equal(t, byte(7), both.MinimumListenerBalance.Mints[0].Value[0])

	// The requirements are group-only: a DM record that somehow carries them
	// still has no rules.
	flaggedDm := dm.Clone()
	flaggedDm.IsStaffOnly = true
	flaggedDm.MinimumListenerBalance = gated.MinimumListenerBalance
	require.Nil(t, flaggedDm.Rules())
}

func TestChat_Clone_MinimumListenerBalance(t *testing.T) {
	// A group without a requirement clones to one without.
	plain := &Chat{ID: MustGenerateGroupChatID(), Type: chatpb.ChatType_GROUP}
	require.Nil(t, plain.Clone().MinimumListenerBalance)

	// A clone carries an equal, independent copy of the requirement.
	c := &Chat{ID: MustGenerateGroupChatID(), Type: chatpb.ChatType_GROUP, MinimumListenerBalance: &MinimumBalance{
		Currency:     "eur",
		NativeAmount: 10,
		Mints:        []*commonpb.PublicKey{{Value: bytes.Repeat([]byte{1}, 32)}},
	}}
	cloned := c.Clone()
	require.Equal(t, c.MinimumListenerBalance, cloned.MinimumListenerBalance)
	require.NotSame(t, c.MinimumListenerBalance, cloned.MinimumListenerBalance)

	cloned.MinimumListenerBalance.Currency = "gbp"
	cloned.MinimumListenerBalance.NativeAmount = 11
	cloned.MinimumListenerBalance.Mints[0].Value[0] = 2
	cloned.MinimumListenerBalance.Mints = append(cloned.MinimumListenerBalance.Mints, &commonpb.PublicKey{Value: bytes.Repeat([]byte{3}, 32)})
	require.Equal(t, "eur", c.MinimumListenerBalance.Currency)
	require.Equal(t, float64(10), c.MinimumListenerBalance.NativeAmount)
	require.Len(t, c.MinimumListenerBalance.Mints, 1)
	require.Equal(t, byte(1), c.MinimumListenerBalance.Mints[0].Value[0])
}

func TestRuleEvaluator(t *testing.T) {
	ctx := context.Background()
	staffUser := model.MustGenerateUserID()
	nonStaffUser := model.MustGenerateUserID()
	staff := &fakeAccounts{staff: map[string]bool{string(staffUser.Value): true}, keys: make(map[string]*commonpb.PublicKey)}
	ocpBalance := &fakeOcpBalance{ledger: make(map[string]map[string]uint64)}
	chats := &fakeChats{chats: make(map[string]*Chat)}
	e := NewRuleEvaluator(staff, balance.NewClient(zaptest.NewLogger(t), staff, ocpBalance), chats)

	// A DM is answered without a read: it can carry no rules.
	dmID := MustDeriveDmChatID(chatpb.ChatType_CONTACT_DM, staffUser, nonStaffUser)
	ok, err := e.CanListen(ctx, dmID, nonStaffUser)
	require.NoError(t, err)
	require.True(t, ok)
	ok, err = e.CanSpeak(ctx, dmID, nonStaffUser)
	require.NoError(t, err)
	require.True(t, ok)
	require.Zero(t, chats.reads)

	// A group without rules admits everyone, and never consults the staff
	// reader or the balance service.
	plain := chats.put(&Chat{ID: MustGenerateGroupChatID(), Type: chatpb.ChatType_GROUP})
	for _, u := range []*commonpb.UserId{staffUser, nonStaffUser} {
		ok, err := e.CanListen(ctx, plain.ID, u)
		require.NoError(t, err)
		require.True(t, ok)
		ok, err = e.CanSpeak(ctx, plain.ID, u)
		require.NoError(t, err)
		require.True(t, ok)
	}
	require.Zero(t, staff.asked)
	require.Zero(t, ocpBalance.asked)

	// A staff-only chat admits staff, to listen and to speak...
	staffOnly := chats.put(&Chat{ID: MustGenerateGroupChatID(), Type: chatpb.ChatType_GROUP, IsStaffOnly: true})
	ok, err = e.CanListen(ctx, staffOnly.ID, staffUser)
	require.NoError(t, err)
	require.True(t, ok)
	ok, err = e.CanSpeak(ctx, staffOnly.ID, staffUser)
	require.NoError(t, err)
	require.True(t, ok)

	// ...and unbound else: a user who cannot listen cannot speak either.
	ok, err = e.CanListen(ctx, staffOnly.ID, nonStaffUser)
	require.NoError(t, err)
	require.False(t, ok)
	ok, err = e.CanSpeak(ctx, staffOnly.ID, nonStaffUser)
	require.NoError(t, err)
	require.False(t, ok)

	// The rule is evaluated against the reader's current answer, so a revoked
	// flag is denied on the next evaluation.
	delete(staff.staff, string(staffUser.Value))
	ok, err = e.CanListen(ctx, staffOnly.ID, staffUser)
	require.NoError(t, err)
	require.False(t, ok)

	// A reader failure is an error, never a pass.
	staff.err = errors.New("unavailable")
	ok, err = e.CanListen(ctx, staffOnly.ID, staffUser)
	require.Error(t, err)
	require.False(t, ok)

	// An unknown group is the store's not-found, surfaced as-is.
	_, err = e.CanListen(ctx, MustGenerateGroupChatID(), nonStaffUser)
	require.ErrorIs(t, err, ErrChatNotFound)

}

func TestRuleEvaluator_MinimumBalance(t *testing.T) {
	ctx := context.Background()
	accounts := &fakeAccounts{staff: make(map[string]bool), keys: make(map[string]*commonpb.PublicKey)}
	ocpBalance := &fakeOcpBalance{ledger: make(map[string]map[string]uint64)}
	chats := &fakeChats{chats: make(map[string]*Chat)}
	e := NewRuleEvaluator(accounts, balance.NewClient(zaptest.NewLogger(t), accounts, ocpBalance), chats)

	usdf := model.MustGenerateKeyPair().Proto()
	other := model.MustGenerateKeyPair().Proto()

	// holder holds exactly the requirement; underfunded is one quark short; unbound
	// has no owner account at all.
	const requirement = 100
	holder := model.MustGenerateUserID()
	ocpBalance.set(accounts.bind(holder), usdf, ocp_common.ToCoreMintQuarks(requirement))
	underfunded := model.MustGenerateUserID()
	ocpBalance.set(accounts.bind(underfunded), usdf, ocp_common.ToCoreMintQuarks(requirement)-1)
	unbound := model.MustGenerateUserID()

	// A USD requirement is compared straight against the USDF valuation: a
	// balance exactly at the requirement satisfies it, one quark under does
	// not, and a user with no owner account holds nothing.
	gated := chats.put(&Chat{ID: MustGenerateGroupChatID(), Type: chatpb.ChatType_GROUP, MinimumListenerBalance: &MinimumBalance{Currency: "usd", NativeAmount: requirement}})
	for _, u := range []*commonpb.UserId{holder} {
		ok, err := e.CanListen(ctx, gated.ID, u)
		require.NoError(t, err)
		require.True(t, ok)
		ok, err = e.CanSpeak(ctx, gated.ID, u)
		require.NoError(t, err)
		require.True(t, ok)
	}
	for _, u := range []*commonpb.UserId{underfunded, unbound} {
		ok, err := e.CanListen(ctx, gated.ID, u)
		require.NoError(t, err)
		require.False(t, ok)
		ok, err = e.CanSpeak(ctx, gated.ID, u)
		require.NoError(t, err)
		require.False(t, ok)
	}

	// A fractional requirement is rounded to the quark, so a balance exactly
	// at the requirement satisfies it despite the float on the way in.
	cents := chats.put(&Chat{ID: MustGenerateGroupChatID(), Type: chatpb.ChatType_GROUP, MinimumListenerBalance: &MinimumBalance{Currency: "usd", NativeAmount: 0.1 + 0.2}})
	ocpBalance.set(accounts.keys[string(underfunded.Value)], usdf, ocp_common.CoreMintQuarksPerUnit*3/10)
	ok, err := e.CanListen(ctx, cents.ID, underfunded)
	require.NoError(t, err)
	require.True(t, ok)

	// The rule tracks the balance: a holder who drains their account is denied
	// on the next evaluation, and admitted again once refilled.
	ocpBalance.set(accounts.keys[string(holder.Value)], usdf, 0)
	ok, err = e.CanListen(ctx, gated.ID, holder)
	require.NoError(t, err)
	require.False(t, ok)
	ocpBalance.set(accounts.keys[string(holder.Value)], usdf, ocp_common.ToCoreMintQuarks(requirement))

	// A mint-restricted requirement counts only holdings in that mint: a
	// fortune in another mint does not satisfy it, and vice versa.
	inOther := chats.put(&Chat{ID: MustGenerateGroupChatID(), Type: chatpb.ChatType_GROUP, MinimumListenerBalance: &MinimumBalance{Currency: "usd", NativeAmount: requirement, Mints: []*commonpb.PublicKey{other}}})
	ok, err = e.CanListen(ctx, inOther.ID, holder)
	require.NoError(t, err)
	require.False(t, ok)
	ocpBalance.set(accounts.keys[string(holder.Value)], other, ocp_common.ToCoreMintQuarks(requirement))
	ok, err = e.CanListen(ctx, inOther.ID, holder)
	require.NoError(t, err)
	require.True(t, ok)
	inUsdf := chats.put(&Chat{ID: MustGenerateGroupChatID(), Type: chatpb.ChatType_GROUP, MinimumListenerBalance: &MinimumBalance{Currency: "usd", NativeAmount: 2 * requirement, Mints: []*commonpb.PublicKey{usdf}}})
	ok, err = e.CanListen(ctx, inUsdf.ID, holder)
	require.NoError(t, err)
	require.False(t, ok)
	unrestricted := chats.put(&Chat{ID: MustGenerateGroupChatID(), Type: chatpb.ChatType_GROUP, MinimumListenerBalance: &MinimumBalance{Currency: "usd", NativeAmount: 2 * requirement}})
	ok, err = e.CanListen(ctx, unrestricted.ID, holder)
	require.NoError(t, err)
	require.True(t, ok)

	// Alongside a staff rule, the balance is only consulted for staff: the
	// cheaper rule runs first and a failure stops the evaluation.
	both := chats.put(&Chat{ID: MustGenerateGroupChatID(), Type: chatpb.ChatType_GROUP, IsStaffOnly: true, MinimumListenerBalance: &MinimumBalance{Currency: "usd", NativeAmount: requirement}})
	asked := ocpBalance.asked
	ok, err = e.CanListen(ctx, both.ID, holder)
	require.NoError(t, err)
	require.False(t, ok)
	require.Equal(t, asked, ocpBalance.asked)
	accounts.staff[string(holder.Value)] = true
	ok, err = e.CanListen(ctx, both.ID, holder)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, asked+1, ocpBalance.asked)

	// A balance that cannot be read is an error, never a pass.
	ocpBalance.err = errors.New("unavailable")
	ok, err = e.CanListen(ctx, gated.ID, holder)
	require.Error(t, err)
	require.False(t, ok)
	ocpBalance.err = nil

	// Any other currency is answered by OCP's valuation of the same holdings in
	// it. Back at 100 USDF, holder is 90 EUR at the fake's rate: a requirement
	// of 90 is met, and one of 90.01 is not.
	ocpBalance.set(accounts.keys[string(holder.Value)], other, 0)
	ocpBalance.set(accounts.keys[string(underfunded.Value)], usdf, ocp_common.ToCoreMintQuarks(requirement)-1)
	ocpBalance.rates = map[string]float64{"eur": 0.9, "jpy": 150}
	eur := chats.put(&Chat{ID: MustGenerateGroupChatID(), Type: chatpb.ChatType_GROUP, MinimumListenerBalance: &MinimumBalance{Currency: "eur", NativeAmount: 90}})
	ok, err = e.CanListen(ctx, eur.ID, holder)
	require.NoError(t, err)
	require.True(t, ok)
	ok, err = e.CanListen(ctx, eur.ID, underfunded)
	require.NoError(t, err)
	require.True(t, ok, "a quark short of 100 USDF is still 90 EUR to the nearest cent")
	ok, err = e.CanListen(ctx, eur.ID, unbound)
	require.NoError(t, err)
	require.False(t, ok)

	dearer := chats.put(&Chat{ID: MustGenerateGroupChatID(), Type: chatpb.ChatType_GROUP, MinimumListenerBalance: &MinimumBalance{Currency: "eur", NativeAmount: 90.01}})
	ok, err = e.CanListen(ctx, dearer.ID, holder)
	require.NoError(t, err)
	require.False(t, ok)

	// The valuation is a quoted rate applied to quarks, so half a minor unit
	// of slack is allowed under the requirement — and no more. At 150 JPY per
	// USDF, 99.997 USDF is 14999.55 JPY: within half a yen of 15000, admitted;
	// 99.996 USDF is 14999.4 JPY, refused.
	jpy := chats.put(&Chat{ID: MustGenerateGroupChatID(), Type: chatpb.ChatType_GROUP, MinimumListenerBalance: &MinimumBalance{Currency: "jpy", NativeAmount: 15000}})
	ocpBalance.set(accounts.keys[string(underfunded.Value)], usdf, ocp_common.ToCoreMintQuarks(requirement)-3_000)
	ok, err = e.CanListen(ctx, jpy.ID, underfunded)
	require.NoError(t, err)
	require.True(t, ok)
	ocpBalance.set(accounts.keys[string(underfunded.Value)], usdf, ocp_common.ToCoreMintQuarks(requirement)-4_000)
	ok, err = e.CanListen(ctx, jpy.ID, underfunded)
	require.NoError(t, err)
	require.False(t, ok)

	// A currency OCP cannot value is an error, never a pass — and never a zero
	// balance.
	unpriced := chats.put(&Chat{ID: MustGenerateGroupChatID(), Type: chatpb.ChatType_GROUP, MinimumListenerBalance: &MinimumBalance{Currency: "xyz", NativeAmount: 1}})
	ok, err = e.CanListen(ctx, unpriced.ID, holder)
	require.ErrorIs(t, err, balance.ErrUnsupportedCurrency)
	require.False(t, ok)

	// A USD requirement never asks OCP for a valuation, so it is unaffected by
	// what OCP can price.
	ocpBalance.rates = nil
	ok, err = e.CanListen(ctx, gated.ID, holder)
	require.NoError(t, err)
	require.True(t, ok)
}
