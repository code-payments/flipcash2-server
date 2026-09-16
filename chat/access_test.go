package chat

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"

	chatpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/chat/v1"
	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"

	ocp_common "github.com/code-payments/ocp-server/ocp/common"

	"github.com/code-payments/flipcash2-server/balance"
	"github.com/code-payments/flipcash2-server/model"
)

// accessFixture is an Access over the rules_test fakes, with a funded and an
// unfunded user against a group requiring a balance between them.
type accessFixture struct {
	accounts   *fakeAccounts
	ocpBalance *fakeOcpBalance
	chats      *fakeChats
	rules      *RuleEvaluator

	usdf *commonpb.PublicKey

	// gated requires `requirement` USD to listen. funded holds exactly that,
	// unfunded one quark less.
	gated    *Chat
	funded   *commonpb.UserId
	unfunded *commonpb.UserId
}

const accessRequirement = 100

func newAccessFixture(t *testing.T) *accessFixture {
	f := &accessFixture{
		accounts:   &fakeAccounts{staff: make(map[string]bool), keys: make(map[string]*commonpb.PublicKey)},
		ocpBalance: &fakeOcpBalance{ledger: make(map[string]map[string]uint64)},
		chats:      newFakeChats(),
		usdf:       model.MustGenerateKeyPair().Proto(),
	}
	f.rules = NewRuleEvaluator(f.accounts, balance.NewClient(zaptest.NewLogger(t), f.accounts, f.ocpBalance), f.chats)

	f.funded = model.MustGenerateUserID()
	f.ocpBalance.set(f.accounts.bind(f.funded), f.usdf, ocp_common.ToCoreMintQuarks(accessRequirement))
	f.unfunded = model.MustGenerateUserID()
	f.ocpBalance.set(f.accounts.bind(f.unfunded), f.usdf, ocp_common.ToCoreMintQuarks(accessRequirement)-1)

	f.gated = f.chats.put(&Chat{ID: MustGenerateGroupChatID(), Type: chatpb.ChatType_GROUP, MinimumListenerBalance: &MinimumBalance{Currency: "usd", NativeAmount: accessRequirement}})
	return f
}

// setBalance moves userID's holdings to the given number of whole USD.
func (f *accessFixture) setBalance(userID *commonpb.UserId, usd uint64) {
	f.ocpBalance.set(f.accounts.keys[string(userID.Value)], f.usdf, ocp_common.ToCoreMintQuarks(usd))
}

func TestAccess_DM(t *testing.T) {
	ctx := context.Background()
	f := newAccessFixture(t)
	a := NewAccess(f.chats, f.rules)

	peer := model.MustGenerateUserID()
	dm := MustDeriveDmChatID(chatpb.ChatType_CONTACT_DM, f.funded, peer)
	f.chats.join(dm, f.funded)
	f.chats.join(dm, peer)

	// A DM's members read and speak; nothing is evaluated, since a DM has no
	// rules, and nothing is read from the store beyond membership.
	for _, u := range []*commonpb.UserId{f.funded, peer} {
		ok, err := a.IsMember(ctx, dm, u)
		require.NoError(t, err)
		require.True(t, ok)
		ok, err = a.CanListen(ctx, dm, u)
		require.NoError(t, err)
		require.True(t, ok)
		ok, err = a.CanSpeak(ctx, dm, u)
		require.NoError(t, err)
		require.True(t, ok)
	}
	require.Zero(t, f.chats.reads)
	require.Zero(t, f.ocpBalance.asked)

	// A non-member is refused everything, however well funded: a DM never
	// admits a third party, and its nil rules — which would admit anyone — are
	// never consulted.
	third := f.unfunded
	f.setBalance(third, 1_000_000)
	ok, err := a.IsMember(ctx, dm, third)
	require.NoError(t, err)
	require.False(t, ok)
	ok, err = a.CanListen(ctx, dm, third)
	require.NoError(t, err)
	require.False(t, ok)
	ok, err = a.CanSpeak(ctx, dm, third)
	require.NoError(t, err)
	require.False(t, ok)
	require.Zero(t, f.ocpBalance.asked)

	// So is anyone on a DM that does not exist.
	ok, err = a.CanListen(ctx, MustDeriveDmChatID(chatpb.ChatType_CONTACT_DM, third, peer), third)
	require.NoError(t, err)
	require.False(t, ok)
}

func TestAccess_GroupMember(t *testing.T) {
	ctx := context.Background()
	f := newAccessFixture(t)
	a := NewAccess(f.chats, f.rules)

	// A member reads on membership alone, whatever their balance: the rules are
	// not evaluated, so a member whose balance has dipped still reads.
	f.chats.join(f.gated.ID, f.unfunded)
	ok, err := a.IsMember(ctx, f.gated.ID, f.unfunded)
	require.NoError(t, err)
	require.True(t, ok)
	ok, err = a.CanListen(ctx, f.gated.ID, f.unfunded)
	require.NoError(t, err)
	require.True(t, ok)
	require.Zero(t, f.ocpBalance.asked)

	// Speaking is membership and the rules: the dipped member is refused, a
	// funded member admitted.
	ok, err = a.CanSpeak(ctx, f.gated.ID, f.unfunded)
	require.NoError(t, err)
	require.False(t, ok)
	require.Equal(t, 1, f.ocpBalance.asked)

	f.chats.join(f.gated.ID, f.funded)
	ok, err = a.CanSpeak(ctx, f.gated.ID, f.funded)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, 2, f.ocpBalance.asked)

	// A member's reads never populate the admission cache, so on leaving they
	// are judged afresh by the rules: the funded leaver still reads, the
	// unfunded one does not, and neither speaks.
	f.chats.leave(f.gated.ID, f.funded)
	f.chats.leave(f.gated.ID, f.unfunded)
	asked := f.ocpBalance.asked
	ok, err = a.CanListen(ctx, f.gated.ID, f.funded)
	require.NoError(t, err)
	require.True(t, ok)
	ok, err = a.CanListen(ctx, f.gated.ID, f.unfunded)
	require.NoError(t, err)
	require.False(t, ok)
	require.Equal(t, asked+2, f.ocpBalance.asked)
	for _, u := range []*commonpb.UserId{f.funded, f.unfunded} {
		ok, err = a.CanSpeak(ctx, f.gated.ID, u)
		require.NoError(t, err)
		require.False(t, ok)
	}
	// A non-member's send is refused on membership, before any rule.
	require.Equal(t, asked+2, f.ocpBalance.asked)
}

func TestAccess_GroupNonMember(t *testing.T) {
	ctx := context.Background()
	f := newAccessFixture(t)
	a := NewAccess(f.chats, f.rules)

	// A non-member who satisfies the listener rules reads; one who does not is
	// refused. Neither is a member and neither speaks.
	ok, err := a.CanListen(ctx, f.gated.ID, f.funded)
	require.NoError(t, err)
	require.True(t, ok)
	ok, err = a.CanListen(ctx, f.gated.ID, f.unfunded)
	require.NoError(t, err)
	require.False(t, ok)
	for _, u := range []*commonpb.UserId{f.funded, f.unfunded} {
		ok, err = a.IsMember(ctx, f.gated.ID, u)
		require.NoError(t, err)
		require.False(t, ok)
		ok, err = a.CanSpeak(ctx, f.gated.ID, u)
		require.NoError(t, err)
		require.False(t, ok)
	}

	// The admission is remembered: the funded reader's next reads cost no
	// valuation, and hold even after their balance drops, until the window
	// closes.
	asked := f.ocpBalance.asked
	ok, err = a.CanListen(ctx, f.gated.ID, f.funded)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, asked, f.ocpBalance.asked)
	f.setBalance(f.funded, 0)
	ok, err = a.CanListen(ctx, f.gated.ID, f.funded)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, asked, f.ocpBalance.asked)

	// A refusal is not remembered: the unfunded reader is re-evaluated on
	// every read, and admitted the moment they are funded.
	asked = f.ocpBalance.asked
	ok, err = a.CanListen(ctx, f.gated.ID, f.unfunded)
	require.NoError(t, err)
	require.False(t, ok)
	require.Equal(t, asked+1, f.ocpBalance.asked)
	f.setBalance(f.unfunded, accessRequirement)
	ok, err = a.CanListen(ctx, f.gated.ID, f.unfunded)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, asked+2, f.ocpBalance.asked)

	// An admission is per group: the same reader is evaluated again for
	// another group, whose requirement they may not meet.
	richer := f.chats.put(&Chat{ID: MustGenerateGroupChatID(), Type: chatpb.ChatType_GROUP, MinimumListenerBalance: &MinimumBalance{Currency: "usd", NativeAmount: 2 * accessRequirement}})
	asked = f.ocpBalance.asked
	ok, err = a.CanListen(ctx, richer.ID, f.unfunded)
	require.NoError(t, err)
	require.False(t, ok)
	require.Equal(t, asked+1, f.ocpBalance.asked)

	// A group that does not exist admits no one, and says so without error.
	ok, err = a.CanListen(ctx, MustGenerateGroupChatID(), f.funded)
	require.NoError(t, err)
	require.False(t, ok)
	ok, err = a.CanSpeak(ctx, MustGenerateGroupChatID(), f.funded)
	require.NoError(t, err)
	require.False(t, ok)

	// A group without listener rules admits no non-member, with or without the
	// record in hand: only a rule can admit one (see Access). Its members are
	// unaffected. Nothing is valued, since there is no rule to evaluate.
	open := f.chats.put(&Chat{ID: MustGenerateGroupChatID(), Type: chatpb.ChatType_GROUP})
	asked = f.ocpBalance.asked
	ok, err = a.CanListen(ctx, open.ID, f.funded)
	require.NoError(t, err)
	require.False(t, ok)
	ok, err = a.CanListenWithRules(ctx, open.ID, open.Rules(), f.funded)
	require.NoError(t, err)
	require.False(t, ok)
	require.Equal(t, asked, f.ocpBalance.asked)
	f.chats.join(open.ID, f.funded)
	ok, err = a.CanListen(ctx, open.ID, f.funded)
	require.NoError(t, err)
	require.True(t, ok)
	ok, err = a.CanSpeak(ctx, open.ID, f.funded)
	require.NoError(t, err)
	require.True(t, ok)
}

func TestAccess_AdmissionExpires(t *testing.T) {
	ctx := context.Background()
	f := newAccessFixture(t)
	const ttl = 30 * time.Millisecond
	a := NewAccess(f.chats, f.rules, WithListenerAdmissionTTL(ttl))

	ok, err := a.CanListen(ctx, f.gated.ID, f.funded)
	require.NoError(t, err)
	require.True(t, ok)
	f.setBalance(f.funded, 0)

	// Within the window the drained reader still reads. Reading does not extend
	// the window, so however often they read, once it closes they are evaluated
	// again and refused.
	ok, err = a.CanListen(ctx, f.gated.ID, f.funded)
	require.NoError(t, err)
	require.True(t, ok)
	require.Eventually(t, func() bool {
		ok, err := a.CanListen(ctx, f.gated.ID, f.funded)
		require.NoError(t, err)
		return !ok
	}, time.Second, ttl/10)

	// With no window, every read is evaluated.
	a = NewAccess(f.chats, f.rules, WithListenerAdmissionTTL(0))
	f.setBalance(f.funded, accessRequirement)
	asked := f.ocpBalance.asked
	for range 3 {
		ok, err = a.CanListen(ctx, f.gated.ID, f.funded)
		require.NoError(t, err)
		require.True(t, ok)
	}
	require.Equal(t, asked+3, f.ocpBalance.asked)
}

func TestAccess_CanListenWithRules(t *testing.T) {
	ctx := context.Background()
	f := newAccessFixture(t)
	a := NewAccess(f.chats, f.rules)

	// With the rules in hand they are not read from the store, and the
	// answer — and the remembered admission — is the same as CanListen's.
	reads := f.chats.reads
	ok, err := a.CanListenWithRules(ctx, f.gated.ID, f.gated.Rules(), f.funded)
	require.NoError(t, err)
	require.True(t, ok)
	ok, err = a.CanListenWithRules(ctx, f.gated.ID, f.gated.Rules(), f.unfunded)
	require.NoError(t, err)
	require.False(t, ok)
	require.Equal(t, reads, f.chats.reads)

	asked := f.ocpBalance.asked
	ok, err = a.CanListen(ctx, f.gated.ID, f.funded)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, asked, f.ocpBalance.asked)

	// A DM record admits only its members, as CanListen does.
	peer := model.MustGenerateUserID()
	dm := &Chat{ID: MustDeriveDmChatID(chatpb.ChatType_CONTACT_DM, f.funded, peer), Type: chatpb.ChatType_CONTACT_DM, Members: []*commonpb.UserId{f.funded, peer}}
	ok, err = a.CanListenWithRules(ctx, dm.ID, dm.Rules(), f.funded)
	require.NoError(t, err)
	require.False(t, ok)
	f.chats.join(dm.ID, f.funded)
	ok, err = a.CanListenWithRules(ctx, dm.ID, dm.Rules(), f.funded)
	require.NoError(t, err)
	require.True(t, ok)
}

func TestAccess_Errors(t *testing.T) {
	ctx := context.Background()
	f := newAccessFixture(t)
	a := NewAccess(f.chats, f.rules)

	// A rule that cannot be evaluated is an error, never a pass, and is not
	// remembered as either: once the service recovers the reader is admitted.
	f.ocpBalance.err = errors.New("unavailable")
	ok, err := a.CanListen(ctx, f.gated.ID, f.funded)
	require.Error(t, err)
	require.False(t, ok)
	ok, err = a.CanListenWithRules(ctx, f.gated.ID, f.gated.Rules(), f.funded)
	require.Error(t, err)
	require.False(t, ok)

	f.chats.join(f.gated.ID, f.funded)
	ok, err = a.CanSpeak(ctx, f.gated.ID, f.funded)
	require.Error(t, err)
	require.False(t, ok)
	f.chats.leave(f.gated.ID, f.funded)

	f.ocpBalance.err = nil
	ok, err = a.CanListen(ctx, f.gated.ID, f.funded)
	require.NoError(t, err)
	require.True(t, ok)
}
