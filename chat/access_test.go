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
	messagingpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/messaging/v1"

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
	f.rules = NewRuleEvaluator(f.accounts, balance.NewClient(zaptest.NewLogger(t), f.accounts, f.ocpBalance), f.chats, nil)

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

	// SpeakerStanding is membership and the rules: the dipped member is refused, a
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

// TestAccess_PrivateGroup: a private group is its members' alone in every
// form, and its members speak in it exactly when it has its key.
func TestAccess_PrivateGroup(t *testing.T) {
	ctx := context.Background()
	f := newAccessFixture(t)
	a := NewAccess(f.chats, f.rules)

	creator := model.MustGenerateUserID()
	private := f.chats.put(&Chat{ID: MustGenerateGroupChatID(), Type: chatpb.ChatType_GROUP, IsPrivate: true, CreatorID: creator})
	f.chats.join(private.ID, creator)

	// A member reads, and does not speak while the group has no key, which is
	// one read of the creator's envelope per ask.
	ok, err := a.CanListen(ctx, private.ID, creator)
	require.NoError(t, err)
	require.True(t, ok)
	ok, err = a.CanSpeak(ctx, private.ID, creator)
	require.NoError(t, err)
	require.False(t, ok)
	require.Equal(t, 1, f.chats.envelopeReads)

	// A non-member has no standing under any mode, however funded, whether
	// the rules are read from the store or come with the record.
	modes := []messagingpb.ViewMode{messagingpb.ViewMode_FULL, messagingpb.ViewMode_FULL_OR_REDACTED, messagingpb.ViewMode_REDACTED}
	for _, mode := range modes {
		standing, err := a.ListenerStanding(ctx, private.ID, f.funded, mode)
		require.NoError(t, err)
		require.Equal(t, ListenerStanding{}, standing, mode)
		standing, err = a.ListenerStandingWithChat(ctx, private, f.funded, mode)
		require.NoError(t, err)
		require.Equal(t, ListenerStanding{}, standing, mode)
	}
	ok, err = a.CanSpeak(ctx, private.ID, f.funded)
	require.NoError(t, err)
	require.False(t, ok)
	require.Equal(t, ListenerStanding{}, a.PublicListenerStanding(private))

	// It is not governed by rules, so it has no rule set to evaluate, and the
	// evaluator asked about it alone refuses to answer rather than find it
	// open to everyone, as an empty rule set would be.
	_, hasRuleSet := private.ChatRules().RuleSet()
	require.False(t, hasRuleSet)
	for _, u := range []*commonpb.UserId{creator, f.funded} {
		ok, err = f.rules.CanListen(ctx, private.ID, u)
		require.ErrorIs(t, err, ErrNotGovernedByRules)
		require.False(t, ok)
		ok, err = f.rules.CanSpeak(ctx, private.ID, u)
		require.ErrorIs(t, err, ErrNotGovernedByRules)
		require.False(t, ok)
	}

	// None of it rests on the group having no rules. StartChat writes none,
	// but a private group whose record carries a listener rule a non-member
	// satisfies is no more open to them: the flag decides, not the rules.
	ruled := f.chats.put(&Chat{
		ID:                     MustGenerateGroupChatID(),
		Type:                   chatpb.ChatType_GROUP,
		IsPrivate:              true,
		CreatorID:              creator,
		MinimumListenerBalance: &MinimumBalance{Currency: "usd", NativeAmount: accessRequirement},
	})
	for _, mode := range modes {
		standing, err := a.ListenerStanding(ctx, ruled.ID, f.funded, mode)
		require.NoError(t, err)
		require.Equal(t, ListenerStanding{}, standing, mode)
		standing, err = a.ListenerStandingWithChat(ctx, ruled, f.funded, mode)
		require.NoError(t, err)
		require.Equal(t, ListenerStanding{}, standing, mode)
	}
	require.Equal(t, ListenerStanding{}, a.PublicListenerStanding(ruled))
	_, isRuled := ruled.ChatRules().RuleSet()
	require.False(t, isRuled)
	ok, err = f.rules.CanListen(ctx, ruled.ID, f.funded)
	require.ErrorIs(t, err, ErrNotGovernedByRules)
	require.False(t, ok)

	// The creator's envelope is the group's key. Once it is stored every
	// member speaks — one who holds no envelope of their own included, since
	// the key is the group's — and a non-member still does not. The key is
	// found with one envelope read and remembered: it is never read again,
	// so an envelope that vanished (which no store allows) would not be
	// noticed.
	member := model.MustGenerateUserID()
	f.chats.join(private.ID, member)
	ok, err = a.CanSpeak(ctx, private.ID, member)
	require.NoError(t, err)
	require.False(t, ok)
	envelopeReads := f.chats.envelopeReads
	f.chats.storeKey(private.ID, creator)
	for _, u := range []*commonpb.UserId{creator, member, creator} {
		ok, err = a.CanSpeak(ctx, private.ID, u)
		require.NoError(t, err)
		require.True(t, ok)
	}
	require.Equal(t, envelopeReads+1, f.chats.envelopeReads)
	ok, err = a.CanSpeak(ctx, private.ID, f.funded)
	require.NoError(t, err)
	require.False(t, ok)
	f.chats.discardKey(private.ID, creator)
	ok, err = a.CanSpeak(ctx, private.ID, creator)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, envelopeReads+1, f.chats.envelopeReads)

	// The key opens the group to its members' sends and to nothing else: a
	// non-member has no more standing in a keyed group than in a keyless one.
	for _, mode := range modes {
		standing, err := a.ListenerStanding(ctx, private.ID, f.funded, mode)
		require.NoError(t, err)
		require.Equal(t, ListenerStanding{}, standing, mode)
	}

	// A member's own envelope is not the group's key: a group whose creator
	// has stored nothing is keyless whatever its other members hold.
	keyless := f.chats.put(&Chat{ID: MustGenerateGroupChatID(), Type: chatpb.ChatType_GROUP, IsPrivate: true, CreatorID: creator})
	f.chats.join(keyless.ID, member)
	f.chats.storeKey(keyless.ID, member)
	ok, err = a.CanSpeak(ctx, keyless.ID, member)
	require.NoError(t, err)
	require.False(t, ok)

	// A private group with no recorded creator can have no key, and nothing
	// is read to find that out.
	orphan := f.chats.put(&Chat{ID: MustGenerateGroupChatID(), Type: chatpb.ChatType_GROUP, IsPrivate: true})
	f.chats.join(orphan.ID, member)
	envelopeReads = f.chats.envelopeReads
	ok, err = a.CanSpeak(ctx, orphan.ID, member)
	require.NoError(t, err)
	require.False(t, ok)
	require.Equal(t, envelopeReads, f.chats.envelopeReads)

	// No rule was evaluated for any of it.
	require.Zero(t, f.ocpBalance.asked)
}

// TestAccess_Speaking: what each kind of chat takes from a speaker comes with
// the standing — a DM takes plaintext or its pairwise scheme, a public group
// plaintext alone, a keyed private group its chat key's scheme alone — and a
// refused speaker gets the zero value whatever the chat. A private group's
// key is found with one envelope read and then remembered.
func TestAccess_Speaking(t *testing.T) {
	ctx := context.Background()
	f := newAccessFixture(t)
	a := NewAccess(f.chats, f.rules)

	speaker := func(chatID *commonpb.ChatId, userID *commonpb.UserId) SpeakerStanding {
		t.Helper()
		s, err := a.SpeakerStanding(ctx, chatID, userID)
		require.NoError(t, err)
		return s
	}
	dmSpeaking := SpeakerStanding{CanSpeak: true, Encryption: EncryptionOptional, Scheme: messagingpb.EncryptedContent_X25519_XCHACHA20POLY1305}
	privateSpeaking := SpeakerStanding{CanSpeak: true, Encryption: EncryptionRequired, Scheme: messagingpb.EncryptedContent_CHAT_KEY_XCHACHA20POLY1305}

	// A DM, with no read beyond membership.
	peer := model.MustGenerateUserID()
	dm := MustDeriveDmChatID(chatpb.ChatType_DM, f.funded, peer)
	f.chats.join(dm, f.funded)
	f.chats.join(dm, peer)
	require.Equal(t, dmSpeaking, speaker(dm, f.funded))
	require.Equal(t, dmSpeaking, speaker(dm, peer))
	require.Equal(t, SpeakerStanding{}, speaker(dm, f.unfunded))
	require.Zero(t, f.chats.reads)
	require.Zero(t, f.chats.envelopeReads)
	require.True(t, dmSpeaking.takesEncrypted())

	// A public group: the rules decide, and nothing is said of encryption.
	f.chats.join(f.gated.ID, f.funded)
	f.chats.join(f.gated.ID, f.unfunded)
	require.Equal(t, SpeakerStanding{CanSpeak: true}, speaker(f.gated.ID, f.funded))
	require.Equal(t, SpeakerStanding{}, speaker(f.gated.ID, f.unfunded))
	require.Equal(t, 2, f.ocpBalance.asked)
	require.Zero(t, f.chats.envelopeReads)
	require.False(t, SpeakerStanding{CanSpeak: true}.takesEncrypted())

	// A private group: nothing until the key, then its scheme for every
	// member, found once. No rule is evaluated for it.
	creator := model.MustGenerateUserID()
	private := f.chats.put(&Chat{ID: MustGenerateGroupChatID(), Type: chatpb.ChatType_GROUP, IsPrivate: true, CreatorID: creator})
	f.chats.join(private.ID, creator)
	f.chats.join(private.ID, f.unfunded)
	require.Equal(t, SpeakerStanding{}, speaker(private.ID, creator))
	require.Equal(t, SpeakerStanding{}, speaker(private.ID, f.unfunded))
	require.Equal(t, 2, f.chats.envelopeReads)
	f.chats.storeKey(private.ID, creator)
	require.Equal(t, privateSpeaking, speaker(private.ID, creator))
	require.Equal(t, privateSpeaking, speaker(private.ID, f.unfunded))
	require.Equal(t, SpeakerStanding{}, speaker(private.ID, f.funded))
	require.Equal(t, 3, f.chats.envelopeReads)
	require.Equal(t, 2, f.ocpBalance.asked)
	require.True(t, privateSpeaking.takesEncrypted())

	// A chat that does not exist.
	require.Equal(t, SpeakerStanding{}, speaker(MustGenerateGroupChatID(), f.funded))
	require.False(t, SpeakerStanding{}.takesEncrypted())
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

	// A group without listener rules is open: every non-member reads it, with
	// or without the record in hand, funded or not (see Access). Nothing is
	// valued, since there is no rule to evaluate, and nothing is remembered,
	// since there is nothing to save. Its members are unaffected.
	open := f.chats.put(&Chat{ID: MustGenerateGroupChatID(), Type: chatpb.ChatType_GROUP})
	asked = f.ocpBalance.asked
	for _, userID := range []*commonpb.UserId{f.funded, f.unfunded} {
		ok, err = a.CanListen(ctx, open.ID, userID)
		require.NoError(t, err)
		require.True(t, ok)
		ok, err = a.CanListenWithRules(ctx, open.ID, open.ChatRules(), userID)
		require.NoError(t, err)
		require.True(t, ok)
		_, remembered := a.admitted.Get(admissionKey(open.ID, userID))
		require.False(t, remembered)
	}
	require.Equal(t, asked, f.ocpBalance.asked)
	// A non-member still does not speak in it.
	ok, err = a.CanSpeak(ctx, open.ID, f.funded)
	require.NoError(t, err)
	require.False(t, ok)
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
	ok, err := a.CanListenWithRules(ctx, f.gated.ID, f.gated.ChatRules(), f.funded)
	require.NoError(t, err)
	require.True(t, ok)
	ok, err = a.CanListenWithRules(ctx, f.gated.ID, f.gated.ChatRules(), f.unfunded)
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
	ok, err = a.CanListenWithRules(ctx, dm.ID, dm.ChatRules(), f.funded)
	require.NoError(t, err)
	require.False(t, ok)
	f.chats.join(dm.ID, f.funded)
	ok, err = a.CanListenWithRules(ctx, dm.ID, dm.ChatRules(), f.funded)
	require.NoError(t, err)
	require.True(t, ok)
}

// TestAccess_WithChat pins what a caller holding the canonical record saves:
// a DM's standing and membership are answered off the record's inline
// members with no store read at all, while a group's membership is still the
// store's, and its rules come off the record as for ListenerStandingWithRules.
func TestAccess_WithChat(t *testing.T) {
	ctx := context.Background()
	f := newAccessFixture(t)
	a := NewAccess(f.chats, f.rules)

	peer := model.MustGenerateUserID()
	third := model.MustGenerateUserID()
	dm := &Chat{ID: MustDeriveDmChatID(chatpb.ChatType_CONTACT_DM, f.funded, peer), Type: chatpb.ChatType_CONTACT_DM, Members: []*commonpb.UserId{f.funded, peer}}

	// The record alone decides a DM, whatever the membership store says: the
	// store is never asked. Neither the fixture's store nor its rules see a
	// read.
	reads, membershipReads := f.chats.reads, f.chats.membershipReads
	for _, u := range []*commonpb.UserId{f.funded, peer} {
		ok, err := a.IsMemberWithChat(ctx, dm, u)
		require.NoError(t, err)
		require.True(t, ok)
		standing, err := a.ListenerStandingWithChat(ctx, dm, u, messagingpb.ViewMode_FULL)
		require.NoError(t, err)
		require.Equal(t, memberListenerStanding, standing)
	}
	ok, err := a.IsMemberWithChat(ctx, dm, third)
	require.NoError(t, err)
	require.False(t, ok)
	standing, err := a.ListenerStandingWithChat(ctx, dm, third, messagingpb.ViewMode_FULL_OR_REDACTED)
	require.NoError(t, err)
	require.Equal(t, ListenerStanding{}, standing)
	require.Equal(t, reads, f.chats.reads)
	require.Equal(t, membershipReads, f.chats.membershipReads)

	// A group's record carries no members, so its membership is the store's
	// — one read per question — and a member is a member whatever the rules
	// say of them.
	f.chats.join(f.gated.ID, f.unfunded)
	membershipReads = f.chats.membershipReads
	ok, err = a.IsMemberWithChat(ctx, f.gated, f.unfunded)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, membershipReads+1, f.chats.membershipReads)
	standing, err = a.ListenerStandingWithChat(ctx, f.gated, f.unfunded, messagingpb.ViewMode_FULL)
	require.NoError(t, err)
	require.Equal(t, memberListenerStanding, standing)
	require.Equal(t, membershipReads+2, f.chats.membershipReads)

	// A group non-member is judged by the rules off the record, not the
	// store's copy of them.
	reads = f.chats.reads
	standing, err = a.ListenerStandingWithChat(ctx, f.gated, f.funded, messagingpb.ViewMode_FULL)
	require.NoError(t, err)
	require.Equal(t, ListenerStanding{CanListen: true, CanPreview: true}, standing)
	require.Equal(t, reads, f.chats.reads)
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
	ok, err = a.CanListenWithRules(ctx, f.gated.ID, f.gated.ChatRules(), f.funded)
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

// TestAccess_ViewMode pins the third standing — a non-member who may preview a
// gated group but not read it — and that asking for REDACTED never evaluates
// the rules, so a client that wants a group blurred pays no valuation for it.
func TestAccess_ViewMode(t *testing.T) {
	ctx := context.Background()
	f := newAccessFixture(t)
	a := NewAccess(f.chats, f.rules)

	preview := ListenerStanding{CanPreview: true}
	full := ListenerStanding{CanListen: true, CanPreview: true}
	evaluating := []messagingpb.ViewMode{messagingpb.ViewMode_FULL, messagingpb.ViewMode_FULL_OR_REDACTED}
	every := append(evaluating, messagingpb.ViewMode_REDACTED)

	// A non-member who fails the rules may preview the gated group and no more.
	// Under the modes that may answer in full the rules are evaluated to find
	// that out; under REDACTED the answer is the same without the valuation.
	for _, mode := range evaluating {
		asked := f.ocpBalance.asked
		standing, err := a.ListenerStanding(ctx, f.gated.ID, f.unfunded, mode)
		require.NoError(t, err)
		require.Equal(t, preview, standing, mode)
		require.Equal(t, asked+1, f.ocpBalance.asked, mode)
	}
	asked := f.ocpBalance.asked
	standing, err := a.ListenerStanding(ctx, f.gated.ID, f.unfunded, messagingpb.ViewMode_REDACTED)
	require.NoError(t, err)
	require.Equal(t, preview, standing)
	require.Equal(t, asked, f.ocpBalance.asked)

	// A qualifying non-member is not evaluated under REDACTED either — a
	// placeholder is the answer whatever the rules say — so their admission is
	// neither found nor remembered by it: the next evaluating read still pays,
	// and only then is remembered.
	standing, err = a.ListenerStanding(ctx, f.gated.ID, f.funded, messagingpb.ViewMode_REDACTED)
	require.NoError(t, err)
	require.Equal(t, preview, standing)
	require.Equal(t, asked, f.ocpBalance.asked)
	standing, err = a.ListenerStanding(ctx, f.gated.ID, f.funded, messagingpb.ViewMode_FULL_OR_REDACTED)
	require.NoError(t, err)
	require.Equal(t, full, standing)
	require.Equal(t, asked+1, f.ocpBalance.asked)
	standing, err = a.ListenerStanding(ctx, f.gated.ID, f.funded, messagingpb.ViewMode_FULL)
	require.NoError(t, err)
	require.Equal(t, full, standing)
	require.Equal(t, asked+1, f.ocpBalance.asked)
	standing, err = a.ListenerStanding(ctx, f.gated.ID, f.funded, messagingpb.ViewMode_REDACTED)
	require.NoError(t, err)
	require.Equal(t, preview, standing)
	require.Equal(t, asked+1, f.ocpBalance.asked)

	// A member is a member under every mode, with nothing evaluated.
	f.chats.join(f.gated.ID, f.unfunded)
	asked = f.ocpBalance.asked
	for _, mode := range every {
		standing, err := a.ListenerStanding(ctx, f.gated.ID, f.unfunded, mode)
		require.NoError(t, err)
		require.Equal(t, memberListenerStanding, standing, mode)
	}
	require.Equal(t, asked, f.ocpBalance.asked)

	// A group without listener rules is open: every non-member reads it in
	// full, and under REDACTED, which evaluates nothing, previews it. A DM
	// admits no third party, and a group that does not exist no one. None of
	// them are valued.
	open := f.chats.put(&Chat{ID: MustGenerateGroupChatID(), Type: chatpb.ChatType_GROUP})
	peer := model.MustGenerateUserID()
	dm := MustDeriveDmChatID(chatpb.ChatType_CONTACT_DM, f.funded, peer)
	f.chats.join(dm, f.funded)
	f.chats.join(dm, peer)
	third := model.MustGenerateUserID()
	f.ocpBalance.set(f.accounts.bind(third), f.usdf, ocp_common.ToCoreMintQuarks(1_000_000))
	for _, mode := range every {
		want := full
		if mode == messagingpb.ViewMode_REDACTED {
			want = preview
		}
		standing, err := a.ListenerStanding(ctx, open.ID, third, mode)
		require.NoError(t, err)
		require.Equal(t, want, standing, mode)
		for _, chatID := range []*commonpb.ChatId{dm, MustGenerateGroupChatID()} {
			standing, err := a.ListenerStanding(ctx, chatID, third, mode)
			require.NoError(t, err)
			require.Equal(t, ListenerStanding{}, standing, mode)
		}
	}
	require.Equal(t, asked, f.ocpBalance.asked)

	// With the rules in hand the answer is the same, without a store read.
	reads := f.chats.reads
	standing, err = a.ListenerStandingWithRules(ctx, f.gated.ID, f.gated.ChatRules(), third, messagingpb.ViewMode_REDACTED)
	require.NoError(t, err)
	require.Equal(t, preview, standing)
	require.Equal(t, reads, f.chats.reads)
	require.Equal(t, asked, f.ocpBalance.asked)
	standing, err = a.ListenerStandingWithRules(ctx, f.gated.ID, f.gated.ChatRules(), third, messagingpb.ViewMode_FULL_OR_REDACTED)
	require.NoError(t, err)
	require.Equal(t, full, standing)
	require.Equal(t, reads, f.chats.reads)
	require.Equal(t, asked+1, f.ocpBalance.asked)

	// A rule that cannot be evaluated fails an evaluating read and is no
	// obstacle to a redacted one, which never asks it. (A reader whose
	// admission is not yet remembered, so the rule is actually asked.)
	fourth := model.MustGenerateUserID()
	f.accounts.bind(fourth)
	f.ocpBalance.err = errors.New("unavailable")
	_, err = a.ListenerStanding(ctx, f.gated.ID, fourth, messagingpb.ViewMode_FULL_OR_REDACTED)
	require.Error(t, err)
	standing, err = a.ListenerStanding(ctx, f.gated.ID, fourth, messagingpb.ViewMode_REDACTED)
	require.NoError(t, err)
	require.Equal(t, preview, standing)
}

// TestStanding_Reading pins the mode table (see messagingpb.ViewMode): the
// mode never widens a standing, and a mode this version does not know denies.
func TestStanding_Reading(t *testing.T) {
	none := ListenerStanding{}
	preview := ListenerStanding{CanPreview: true}
	full := ListenerStanding{CanListen: true, CanPreview: true}
	for _, tc := range []struct {
		standing ListenerStanding
		mode     messagingpb.ViewMode
		want     Reading
	}{
		{none, messagingpb.ViewMode_FULL, ReadingDenied},
		{preview, messagingpb.ViewMode_FULL, ReadingDenied},
		{full, messagingpb.ViewMode_FULL, ReadingFull},
		{memberListenerStanding, messagingpb.ViewMode_FULL, ReadingFull},

		{none, messagingpb.ViewMode_FULL_OR_REDACTED, ReadingDenied},
		{preview, messagingpb.ViewMode_FULL_OR_REDACTED, ReadingRedacted},
		{full, messagingpb.ViewMode_FULL_OR_REDACTED, ReadingFull},
		{memberListenerStanding, messagingpb.ViewMode_FULL_OR_REDACTED, ReadingFull},

		{none, messagingpb.ViewMode_REDACTED, ReadingDenied},
		{preview, messagingpb.ViewMode_REDACTED, ReadingRedacted},
		{full, messagingpb.ViewMode_REDACTED, ReadingRedacted},
		{memberListenerStanding, messagingpb.ViewMode_REDACTED, ReadingRedacted},

		{none, messagingpb.ViewMode(99), ReadingDenied},
		{preview, messagingpb.ViewMode(99), ReadingDenied},
		{full, messagingpb.ViewMode(99), ReadingDenied},
		{memberListenerStanding, messagingpb.ViewMode(99), ReadingDenied},
	} {
		require.Equal(t, tc.want, tc.standing.Reading(tc.mode), "%+v under %v", tc.standing, tc.mode)
	}
}

// TestAccess_SpeakerAdmission pins the speaker cache (see
// DefaultSpeakerAdmissionTTL): a member's satisfied rules in a public group
// are remembered for the window, so their next sends cost no valuation and
// hold after their balance drops until it closes; a refusal or an error is
// never remembered; and membership is read on every ask, so a member who
// leaves is refused at once whatever is remembered.
func TestAccess_SpeakerAdmission(t *testing.T) {
	ctx := context.Background()
	f := newAccessFixture(t)
	const ttl = 30 * time.Millisecond
	a := NewAccess(f.chats, f.rules, WithSpeakerAdmissionTTL(ttl))
	f.chats.join(f.gated.ID, f.funded)
	f.chats.join(f.gated.ID, f.unfunded)

	canSpeak := func(chatID *commonpb.ChatId, userID *commonpb.UserId) bool {
		t.Helper()
		ok, err := a.CanSpeak(ctx, chatID, userID)
		require.NoError(t, err)
		return ok
	}

	// The first send is evaluated and remembered; the next ones are not
	// evaluated, and stand after the balance drops, until the window closes.
	// Speaking does not extend it.
	asked := f.ocpBalance.asked
	require.True(t, canSpeak(f.gated.ID, f.funded))
	require.True(t, canSpeak(f.gated.ID, f.funded))
	require.Equal(t, asked+1, f.ocpBalance.asked)
	f.setBalance(f.funded, 0)
	require.True(t, canSpeak(f.gated.ID, f.funded))
	require.Eventually(t, func() bool { return !canSpeak(f.gated.ID, f.funded) }, time.Second, ttl/10)
	f.setBalance(f.funded, accessRequirement)

	// A refusal is not remembered: the unfunded member is evaluated on every
	// send, and speaks the moment they are funded.
	asked = f.ocpBalance.asked
	require.False(t, canSpeak(f.gated.ID, f.unfunded))
	require.False(t, canSpeak(f.gated.ID, f.unfunded))
	require.Equal(t, asked+2, f.ocpBalance.asked)
	f.setBalance(f.unfunded, accessRequirement)
	require.True(t, canSpeak(f.gated.ID, f.unfunded))
	f.ocpBalance.set(f.accounts.keys[string(f.unfunded.Value)], f.usdf, ocp_common.ToCoreMintQuarks(accessRequirement)-1)

	// Membership is read on every ask: a remembered member who leaves is
	// refused at once, with nothing evaluated. Leaving does not forget the
	// admission, so one who rejoins within the window speaks on it.
	require.True(t, canSpeak(f.gated.ID, f.funded))
	f.chats.leave(f.gated.ID, f.funded)
	asked = f.ocpBalance.asked
	require.False(t, canSpeak(f.gated.ID, f.funded))
	f.chats.join(f.gated.ID, f.funded)
	require.True(t, canSpeak(f.gated.ID, f.funded))
	require.Equal(t, asked, f.ocpBalance.asked)

	// An admission is per group and per user: the same member is evaluated
	// again in another group, and another member in this one.
	other := f.chats.put(&Chat{ID: MustGenerateGroupChatID(), Type: chatpb.ChatType_GROUP, MinimumListenerBalance: &MinimumBalance{Currency: "usd", NativeAmount: accessRequirement}})
	f.chats.join(other.ID, f.funded)
	asked = f.ocpBalance.asked
	require.True(t, canSpeak(other.ID, f.funded))
	require.Equal(t, asked+1, f.ocpBalance.asked)

	// A speaker admission is not a listener admission, nor the other way
	// round: a non-member remembered as a reader has no standing to speak,
	// and their read is still evaluated by the listener cache's own rules.
	outsider := model.MustGenerateUserID()
	f.ocpBalance.set(f.accounts.bind(outsider), f.usdf, ocp_common.ToCoreMintQuarks(accessRequirement))
	ok, err := a.CanListen(ctx, f.gated.ID, outsider)
	require.NoError(t, err)
	require.True(t, ok)
	require.False(t, canSpeak(f.gated.ID, outsider))

	// An error is never remembered as either answer.
	fresh := model.MustGenerateUserID()
	f.ocpBalance.set(f.accounts.bind(fresh), f.usdf, ocp_common.ToCoreMintQuarks(accessRequirement))
	f.chats.join(f.gated.ID, fresh)
	f.ocpBalance.err = errors.New("unavailable")
	ok, err = a.CanSpeak(ctx, f.gated.ID, fresh)
	require.Error(t, err)
	require.False(t, ok)
	f.ocpBalance.err = nil
	asked = f.ocpBalance.asked
	require.True(t, canSpeak(f.gated.ID, fresh))
	require.Equal(t, asked+1, f.ocpBalance.asked)

	// DMs and private groups value nothing to speak, so nothing is
	// remembered for them: a private group's members speak the moment its key
	// lands, as before.
	creator := model.MustGenerateUserID()
	private := f.chats.put(&Chat{ID: MustGenerateGroupChatID(), Type: chatpb.ChatType_GROUP, IsPrivate: true, CreatorID: creator})
	f.chats.join(private.ID, creator)
	require.False(t, canSpeak(private.ID, creator))
	f.chats.storeKey(private.ID, creator)
	require.True(t, canSpeak(private.ID, creator))

	// Only a verdict that read something is remembered. A group with listener
	// rules and no speaker rules is (the gated group above), and so is one
	// with a speaker balance alone; a group with no rules, or whose only rule
	// is that its creator speaks, is decided off its rules and is not.
	remembered := func(chatID *commonpb.ChatId, userID *commonpb.UserId) bool {
		t.Helper()
		_, ok := a.speakers.Get(admissionKey(chatID, userID))
		return ok
	}
	require.True(t, remembered(f.gated.ID, fresh))
	speakerGated := f.chats.put(&Chat{ID: MustGenerateGroupChatID(), Type: chatpb.ChatType_GROUP, MinimumSpeakerBalance: &MinimumBalance{Currency: "usd", NativeAmount: accessRequirement}})
	open := f.chats.put(&Chat{ID: MustGenerateGroupChatID(), Type: chatpb.ChatType_GROUP})
	creatorOnly := f.chats.put(&Chat{ID: MustGenerateGroupChatID(), Type: chatpb.ChatType_GROUP, IsCreatorOnlySpeaker: true, CreatorID: f.funded})
	for _, c := range []*Chat{speakerGated, open, creatorOnly} {
		f.chats.join(c.ID, f.funded)
	}
	asked = f.ocpBalance.asked
	require.True(t, canSpeak(speakerGated.ID, f.funded))
	require.True(t, remembered(speakerGated.ID, f.funded))
	require.True(t, canSpeak(open.ID, f.funded))
	require.False(t, remembered(open.ID, f.funded))
	require.True(t, canSpeak(creatorOnly.ID, f.funded))
	require.False(t, remembered(creatorOnly.ID, f.funded))
	require.Equal(t, asked+1, f.ocpBalance.asked)

	// With no window, every send is evaluated.
	a = NewAccess(f.chats, f.rules, WithSpeakerAdmissionTTL(0))
	asked = f.ocpBalance.asked
	for range 3 {
		require.True(t, canSpeak(f.gated.ID, f.funded))
	}
	require.Equal(t, asked+3, f.ocpBalance.asked)
}

// TestAccess_Governance is every gate across every kind of chat (see
// governance): for each, who reads it under each view mode, who speaks in it
// and what it takes from them, and what an anonymous viewer sees. Each
// viewer's standing must agree whichever way it is asked — read from the
// store, off the record, or with the rules in hand — and a chat whose answer
// never rests on a rule must never value a balance to give it: above all a
// private group whose record carries rules, which govern nothing in it.
//
// Admissions are not remembered, so each mode's answer stands on its own
// (TestAccess_AdmissionExpires covers the window).
func TestAccess_Governance(t *testing.T) {
	ctx := context.Background()
	f := newAccessFixture(t)
	team := model.MustGenerateUserID()
	rules := NewRuleEvaluator(f.accounts, balance.NewClient(zaptest.NewLogger(t), f.accounts, f.ocpBalance), f.chats, team)
	a := NewAccess(f.chats, rules, WithListenerAdmissionTTL(0))

	// rich users hold exactly the requirement, poor ones a quark less; "in"
	// users are made members of the chats that list them, "out" users never.
	bound := func(quarks uint64) *commonpb.UserId {
		u := model.MustGenerateUserID()
		f.ocpBalance.set(f.accounts.bind(u), f.usdf, quarks)
		return u
	}
	requirement := ocp_common.ToCoreMintQuarks(accessRequirement)
	richIn, poorIn := bound(requirement), bound(requirement-1)
	richOut, poorOut := bound(requirement), bound(requirement-1)
	creator, peer := bound(requirement), model.MustGenerateUserID()
	minimum := func() *MinimumBalance { return &MinimumBalance{Currency: "usd", NativeAmount: accessRequirement} }

	modes := []messagingpb.ViewMode{messagingpb.ViewMode_FULL, messagingpb.ViewMode_FULL_OR_REDACTED, messagingpb.ViewMode_REDACTED}
	none := ListenerStanding{}
	preview := ListenerStanding{CanPreview: true}
	full := ListenerStanding{CanListen: true, CanPreview: true}
	member := [3]ListenerStanding{memberListenerStanding, memberListenerStanding, memberListenerStanding}
	outsider := [3]ListenerStanding{none, none, none}

	refused := SpeakerStanding{}
	plaintext := SpeakerStanding{CanSpeak: true}
	pairwise := SpeakerStanding{CanSpeak: true, Encryption: EncryptionOptional, Scheme: messagingpb.EncryptedContent_X25519_XCHACHA20POLY1305}
	chatKey := SpeakerStanding{CanSpeak: true, Encryption: EncryptionRequired, Scheme: messagingpb.EncryptedContent_CHAT_KEY_XCHACHA20POLY1305}

	type viewer struct {
		name     string
		user     *commonpb.UserId
		member   bool
		listener [3]ListenerStanding // under modes, in order
		speaker  SpeakerStanding
	}
	dm := func(chatType chatpb.ChatType, u1, u2 *commonpb.UserId) *Chat {
		return &Chat{ID: MustDeriveDmChatID(chatType, u1, u2), Type: chatType, Members: []*commonpb.UserId{u1, u2}}
	}
	group := func(c *Chat) *Chat {
		c.ID = MustGenerateGroupChatID()
		c.Type = chatpb.ChatType_GROUP
		return c
	}

	for _, tc := range []struct {
		name       string
		chat       *Chat
		keyed      bool
		governance governance
		viewers    []viewer
		public     ListenerStanding
		// neverValued is a chat whose every answer is given without a rule
		// being evaluated against a balance.
		neverValued bool
	}{
		{
			name:       "dm",
			chat:       dm(chatpb.ChatType_DM, richIn, peer),
			governance: governanceDm,
			viewers: []viewer{
				{"member", richIn, true, member, pairwise},
				{"peer", peer, true, member, pairwise},
				{"third party", richOut, false, outsider, refused},
			},
			public:      none,
			neverValued: true,
		},
		{
			name:       "dm with the team",
			chat:       dm(chatpb.ChatType_DM, poorIn, team),
			governance: governanceDm,
			viewers: []viewer{
				{"member", poorIn, true, member, refused},
				{"team", team, true, member, refused},
				{"third party", richOut, false, outsider, refused},
			},
			public:      none,
			neverValued: true,
		},
		{
			name:       "public group with a listener balance",
			chat:       group(&Chat{MinimumListenerBalance: minimum()}),
			governance: governancePublicGroup,
			viewers: []viewer{
				{"funded member", richIn, true, member, plaintext},
				{"unfunded member", poorIn, true, member, refused},
				{"funded non-member", richOut, false, [3]ListenerStanding{full, full, preview}, refused},
				{"unfunded non-member", poorOut, false, [3]ListenerStanding{preview, preview, preview}, refused},
			},
			public: preview,
		},
		{
			// Open: every non-member reads it, funded or not.
			name:       "public group with no rules",
			chat:       group(&Chat{}),
			governance: governancePublicGroup,
			viewers: []viewer{
				{"member", richIn, true, member, plaintext},
				{"funded non-member", richOut, false, [3]ListenerStanding{full, full, preview}, refused},
				{"unfunded non-member", poorOut, false, [3]ListenerStanding{full, full, preview}, refused},
			},
			public:      preview,
			neverValued: true,
		},
		{
			// No listener rule, so open to read: only speaking is gated.
			name:       "public group with a speaker balance alone",
			chat:       group(&Chat{MinimumSpeakerBalance: minimum()}),
			governance: governancePublicGroup,
			viewers: []viewer{
				{"funded member", richIn, true, member, plaintext},
				{"unfunded member", poorIn, true, member, refused},
				{"funded non-member", richOut, false, [3]ListenerStanding{full, full, preview}, refused},
				{"unfunded non-member", poorOut, false, [3]ListenerStanding{full, full, preview}, refused},
			},
			public: preview,
		},
		{
			name:       "creator-only public group",
			chat:       group(&Chat{IsCreatorOnlySpeaker: true, CreatorID: creator}),
			governance: governancePublicGroup,
			viewers: []viewer{
				{"creator", creator, true, member, plaintext},
				{"member", richIn, true, member, refused},
				{"non-member", richOut, false, [3]ListenerStanding{full, full, preview}, refused},
			},
			public:      preview,
			neverValued: true,
		},
		{
			name:       "keyless private group",
			chat:       group(&Chat{IsPrivate: true, CreatorID: creator}),
			governance: governancePrivateGroup,
			viewers: []viewer{
				{"creator", creator, true, member, refused},
				{"member", poorIn, true, member, refused},
				{"non-member", richOut, false, outsider, refused},
			},
			public:      none,
			neverValued: true,
		},
		{
			name:       "keyed private group",
			chat:       group(&Chat{IsPrivate: true, CreatorID: creator}),
			keyed:      true,
			governance: governancePrivateGroup,
			viewers: []viewer{
				{"creator", creator, true, member, chatKey},
				{"member", poorIn, true, member, chatKey},
				{"non-member", richOut, false, outsider, refused},
			},
			public:      none,
			neverValued: true,
		},
		{
			// Its rules govern nothing: the unfunded member speaks, and the
			// funded non-member they would admit gets nothing.
			name:       "keyed private group whose record carries rules",
			chat:       group(&Chat{IsPrivate: true, CreatorID: creator, MinimumListenerBalance: minimum(), MinimumSpeakerBalance: minimum()}),
			keyed:      true,
			governance: governancePrivateGroup,
			viewers: []viewer{
				{"unfunded member", poorIn, true, member, chatKey},
				{"funded non-member", richOut, false, outsider, refused},
			},
			public:      none,
			neverValued: true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := f.chats.put(tc.chat)
			for _, v := range tc.viewers {
				if v.member {
					f.chats.join(c.ID, v.user)
				}
			}
			if tc.keyed {
				f.chats.storeKey(c.ID, c.CreatorID)
			}
			asked := f.ocpBalance.asked

			gov, ruleSet := governanceOf(c.ID, c.ChatRules())
			require.Equal(t, tc.governance, gov)
			_, ruled := c.ChatRules().RuleSet()
			require.Equal(t, tc.governance != governancePrivateGroup, ruled)
			if !ruled {
				require.Equal(t, RuleSet{}, ruleSet)
			}

			require.Equal(t, tc.public, a.PublicListenerStanding(c))

			for _, v := range tc.viewers {
				t.Run(v.name, func(t *testing.T) {
					isMember, err := a.IsMember(ctx, c.ID, v.user)
					require.NoError(t, err)
					require.Equal(t, v.member, isMember)
					isMember, err = a.IsMemberWithChat(ctx, c, v.user)
					require.NoError(t, err)
					require.Equal(t, v.member, isMember)

					for i, mode := range modes {
						want := v.listener[i]
						standing, err := a.ListenerStanding(ctx, c.ID, v.user, mode)
						require.NoError(t, err)
						require.Equal(t, want, standing, "from the store, under %v", mode)
						standing, err = a.ListenerStandingWithChat(ctx, c, v.user, mode)
						require.NoError(t, err)
						require.Equal(t, want, standing, "off the record, under %v", mode)
						standing, err = a.ListenerStandingWithRules(ctx, c.ID, c.ChatRules(), v.user, mode)
						require.NoError(t, err)
						require.Equal(t, want, standing, "with the rules, under %v", mode)
					}
					canListen, err := a.CanListen(ctx, c.ID, v.user)
					require.NoError(t, err)
					require.Equal(t, v.listener[0].CanListen, canListen)

					speaker, err := a.SpeakerStanding(ctx, c.ID, v.user)
					require.NoError(t, err)
					require.Equal(t, v.speaker, speaker)
					canSpeak, err := a.CanSpeak(ctx, c.ID, v.user)
					require.NoError(t, err)
					require.Equal(t, v.speaker.CanSpeak, canSpeak)
				})
			}

			if tc.neverValued {
				require.Equal(t, asked, f.ocpBalance.asked, "a balance was valued")
			}
		})
	}
}
