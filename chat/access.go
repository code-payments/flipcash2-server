package chat

import (
	"context"
	"errors"
	"time"

	"github.com/ReneKroon/ttlcache"

	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"
	messagingpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/messaging/v1"
)

// DefaultListenerAdmissionTTL is how long Access remembers that a non-member
// satisfied a group's listener rules (see Access.CanListen). It bounds how long a
// reader whose balance has since dropped, or whose staff flag was since
// revoked, keeps reading before the rules are asked again.
const DefaultListenerAdmissionTTL = 30 * time.Second

// DefaultSpeakerAdmissionTTL is how long Access remembers that a member of a
// public group satisfied its listener and speaker rules (see
// Access.SpeakerStanding). It bounds how long a member whose balance has
// since dropped, or whose staff flag was since revoked, keeps speaking before
// the rules are asked again.
const DefaultSpeakerAdmissionTTL = 30 * time.Second

// Access answers the questions every chat and messaging RPC asks before acting
// on behalf of a user, so the chat and messaging services — and any other
// domain that gates on a chat, such as a blob granted to a chat's audience —
// enforce one definition of who may do what in a chat:
//
//   - IsMember: is the user on the chat's roster. The gate on every write that
//     is not a send but still belongs to a member alone — a pointer advance, a
//     reaction — and on anything that would leave a record in the chat on the
//     user's behalf.
//   - CanListen: may the user see the chat — its metadata, its messages, its
//     pointers and reactions. A member always may. A non-member may read a
//     public group, and only a public group, if they satisfy its listener
//     rules right now, which every non-member does when it carries none.
//   - CanSpeak: may the user produce something other members see — a message,
//     an edit, a deletion, a typing notification. Members only, and only as
//     the chat's governance allows.
//
// Membership is always checked first: it is the cheaper check (a keyed read,
// cached for a DM), and it answers for a member without evaluating a rule — a
// member whose balance has since dipped under the group's requirement is still
// a member and still reads (see messaging/access.go for why membership stands
// for the rules on a member's reads). Past membership, each gate switches on
// how the chat is governed (see governance): a DM, a public group governed by
// its rules, or a private group governed by its creator and key. Rules are
// evaluated only where they decide the answer: for a non-member's read of a
// public group, and for a send in a DM or a public group.
//
// Evaluating a group's listener rules on behalf of a non-member is what lets
// someone who satisfies them preview the group before joining. It is also a
// cost the RuleEvaluator was designed to avoid: a minimum balance is a
// valuation by the OCP server, and a non-member browsing a group would pay it
// on every page. So a non-member's admission is remembered for
// listenerAdmissionTTL, and only an admission — a refusal is never cached, so a
// user who tops up their balance is admitted on their very next read. The
// window is the same kind of lag a member already has indefinitely: for its
// duration a reader who no longer satisfies the rules keeps reading. A hit
// does not extend the window, so a reader who keeps reading is re-evaluated
// once per window, not never.
//
// A member's speech in a public group is remembered the same way, for
// speakerAdmissionTTL, since every send, edit, deletion and typing
// notification asks the rules again and a balance rule makes each one a
// valuation. The cache stands in for the rules' verdict only, never for
// membership, which is read on every ask: a member who leaves is refused at
// once (leaving does not forget the admission, so one who rejoins within the
// window speaks on it, having passed JoinChat's own rules). Only an admission is remembered, so a member who tops up speaks on
// their next send, and a hit does not extend the window. Only a verdict that
// read something is remembered: a public group with no rules, or whose only
// rule is that its creator speaks, is decided off the rules alone, as
// cheaply as a remembered verdict is found, so nothing is kept for it (see
// RuleSet.speakingReadsState). A group with listener rules and no speaker
// rules is remembered, since a speak check evaluates its listener rules.
// DMs and private groups are not remembered: neither values a balance to
// speak (see dmSpeaker and privateGroupSpeaker).
//
// A DM admits its two members and no one else (see governanceDm). A public
// group without listener rules is open: an empty set is satisfied by
// everyone, as the RuleEvaluator says of it, so every registered non-member
// reads it in full and anyone may join it (see Server.JoinChat), and its
// public view is shown (see PublicListenerStanding). Reads and joins agree on
// this, since gating a read that a join, open to all, would grant protects
// nothing. Every group created through StartChat carries a minimum listener
// balance (see RulesFromProto), but the store does not require one, so a
// group written before that rule existed, or by an operator, may carry none
// and is open. A group meant to be its members' alone carries a listener
// rule; one with speaker rules alone (see Chat.MinimumSpeakerBalance) is
// open to read and join, and gated only to speak.
//
// A non-member of a group whose listener rules they do not satisfy is not
// nothing to the group: they may read it redacted (see ListenerStanding.CanPreview and
// redact.Message) — that the messages exist and their shape, never what they
// say. Every non-member of a public group may, whatever its rules. What a read is answered
// with — full, redacted, or nothing — is the viewer's standing combined with
// what the client asked for (see ListenerStanding.Reading and messagingpb.ViewMode):
// a client that wants a group blurred asks for REDACTED, and is answered from
// membership and the rules' existence without the rules being evaluated.
//
// A private group (see Chat.IsPrivate) is its members' alone in every form:
// no non-member listens to it or previews it, whatever mode they ask under,
// and it has no public view. That is decided by its governance
// (governancePrivateGroup), on the flag itself, and never by its rules: it
// has no RuleSet, so a record written with rules cannot open it. Its record is still
// shown to any registered user (see Server.GetChat), which is how a user sees
// what they would ask to join. Its members speak in it on membership and the
// group's key alone (see privateGroupSpeaker).
//
// A chat that does not exist admits no one: IsMember, CanListen and CanSpeak all
// report false, not ErrChatNotFound, for a chat ID nothing is stored under. A
// caller that must tell NOT_FOUND from DENIED reads the canonical record itself
// and asks with CanListenWithRules or ListenerStandingWithRules.
type Access struct {
	chats Store
	rules *RuleEvaluator

	// admitted remembers, by (group, user), that a non-member satisfied the
	// group's listener rules. Positive entries only (see above). A nil cache
	// means admissions are not remembered (see WithListenerAdmissionTTL).
	admitted    *ttlcache.Cache
	admittedTTL time.Duration

	// speakers remembers, by (group, user), that a member satisfied a public
	// group's listener and speaker rules. Positive entries only (see above). A
	// nil cache means speech is not remembered (see WithSpeakerAdmissionTTL).
	speakers    *ttlcache.Cache
	speakersTTL time.Duration

	// keyed remembers, by group, that a private group has its key (see
	// privateGroupSpeaker). Positive entries only, held for the life of the process: a
	// group that has its key has it for good.
	keyed *ttlcache.Cache
}

// AccessOption configures an Access at construction.
type AccessOption func(*Access)

// WithListenerAdmissionTTL overrides DefaultListenerAdmissionTTL: how long a
// non-member's satisfied listener rules stand before they are evaluated again.
// A zero or negative TTL remembers nothing, so every non-member read evaluates
// the rules — for tests, or for a deployment that would rather pay the
// valuation than tolerate the window.
func WithListenerAdmissionTTL(ttl time.Duration) AccessOption {
	return func(a *Access) {
		a.admittedTTL = ttl
	}
}

// WithSpeakerAdmissionTTL overrides DefaultSpeakerAdmissionTTL: how long a
// member's satisfied rules in a public group stand before they are evaluated
// again for a send. A zero or negative TTL remembers nothing, so every send
// evaluates the rules — for tests, or for a deployment that would rather pay
// the valuation than tolerate the window.
func WithSpeakerAdmissionTTL(ttl time.Duration) AccessOption {
	return func(a *Access) {
		a.speakersTTL = ttl
	}
}

// NewAccess constructs an Access over the chat store the servers read
// membership from and the RuleEvaluator they evaluate rules with. The store
// should be the caching store in production, so a DM's membership and a
// group's rules cost no read in steady state.
//
// One Access is meant to serve every server that gates on a chat: the chat
// and messaging servers take it at construction rather than building their
// own, so a non-member's admission is evaluated and remembered once, in one
// window, whichever service they read through — and so the window is
// configured in one place.
func NewAccess(chats Store, rules *RuleEvaluator, opts ...AccessOption) *Access {
	a := &Access{
		chats:       chats,
		rules:       rules,
		admittedTTL: DefaultListenerAdmissionTTL,
		speakersTTL: DefaultSpeakerAdmissionTTL,
		keyed:       ttlcache.NewCache(),
	}
	for _, opt := range opts {
		opt(a)
	}
	if a.admittedTTL > 0 {
		a.admitted = ttlcache.NewCache()
		// A hit must not extend the entry, or a reader who keeps reading is
		// never re-evaluated.
		a.admitted.SkipTtlExtensionOnHit(true)
	}
	if a.speakersTTL > 0 {
		a.speakers = ttlcache.NewCache()
		a.speakers.SkipTtlExtensionOnHit(true)
	}
	return a
}

// Rules is the RuleEvaluator the Access evaluates rules with, for a caller
// that must evaluate rules outside of a standing — a join, which the rules
// admit to membership, or a creation, whose rules are not stored yet.
func (a *Access) Rules() *RuleEvaluator {
	return a.rules
}

// IsMember reports whether userID is on chatID's roster. It is false, not an
// error, for a chat that does not exist and for a group member since removed.
func (a *Access) IsMember(ctx context.Context, chatID *commonpb.ChatId, userID *commonpb.UserId) (bool, error) {
	return a.chats.IsMember(ctx, chatID, userID)
}

// ListenerStanding is a user's relation to a chat as Access sees it: whether they are
// on its roster, whether they may read it in full (see Access.CanListen), and
// whether they may read it redacted (see Access). A member may always read; a
// non-member with CanListen is a group's qualifying non-member, admitted by
// its listener rules (every non-member, when the group carries none); a
// non-member with CanPreview alone is a non-member of a public group whose
// listener rules they do not satisfy, or who asked under REDACTED, who may
// see that it has messages and their shape. CanListen implies CanPreview: whoever may read in full may
// read redacted (see Reading).
//
// A standing found under ViewMode REDACTED (see Access.ListenerStanding) never
// evaluates the rules, so its CanListen is false for a non-member whether or
// not they satisfy them. Such a standing answers only the question it was
// asked; a caller that needs CanListen asks under a mode that evaluates it.
type ListenerStanding struct {
	IsMember   bool
	CanListen  bool
	CanPreview bool
}

// memberListenerStanding is a member's standing, for a caller that has established
// membership by other means — a feed built from the viewer's own memberships, a
// join that just landed.
var memberListenerStanding = ListenerStanding{IsMember: true, CanListen: true, CanPreview: true}

// Reading is what a read of a chat is answered with: nothing, the messages in
// full, or the messages redacted (see redact.Message). It is the viewer's
// standing combined with the client's ViewMode (see ListenerStanding.Reading).
type Reading uint8

const (
	// ReadingDenied: the viewer may not read the chat under the mode asked.
	ReadingDenied Reading = iota
	// ReadingFull: the messages as sent.
	ReadingFull
	// ReadingRedacted: the messages redacted, with Message.redacted set.
	ReadingRedacted
)

// Reading resolves the standing against what the client asked for (see
// messagingpb.ViewMode for the contract): FULL is full content or nothing,
// FULL_OR_REDACTED the most the standing allows, REDACTED a placeholder for
// anyone who may read the chat at all — including a member. A mode this
// version does not know denies, on the same footing as an unknown rule: what
// the client wants is not understood, so nothing is shown. The mode never
// widens the standing: a redacted reader is never answered in full.
func (s ListenerStanding) Reading(mode messagingpb.ViewMode) Reading {
	switch mode {
	case messagingpb.ViewMode_FULL:
		if s.CanListen {
			return ReadingFull
		}
	case messagingpb.ViewMode_FULL_OR_REDACTED:
		if s.CanListen {
			return ReadingFull
		}
		if s.CanPreview {
			return ReadingRedacted
		}
	case messagingpb.ViewMode_REDACTED:
		if s.CanPreview {
			return ReadingRedacted
		}
	}
	return ReadingDenied
}

// CanListen reports whether userID may read chatID in full: they are a member,
// or chatID is a group whose listener rules they satisfy (see Access). It is
// false, not an error, for a chat that does not exist.
func (a *Access) CanListen(ctx context.Context, chatID *commonpb.ChatId, userID *commonpb.UserId) (bool, error) {
	standing, err := a.ListenerStanding(ctx, chatID, userID, messagingpb.ViewMode_FULL)
	return standing.CanListen, err
}

// CanListenWithRules is CanListen for a caller already holding the chat's rules
// (see ListenerStandingWithRules).
func (a *Access) CanListenWithRules(ctx context.Context, chatID *commonpb.ChatId, rules ChatRules, userID *commonpb.UserId) (bool, error) {
	standing, err := a.ListenerStandingWithRules(ctx, chatID, rules, userID, messagingpb.ViewMode_FULL)
	return standing.CanListen, err
}

// ListenerStanding is userID's standing in chatID (see ListenerStanding) as needed to answer
// a read under mode: membership, then for a non-member of a group only, the
// group's listener rules — unless mode is REDACTED, whether userID satisfies
// them, which everyone does when there are none. Under REDACTED the rules are not evaluated,
// since a placeholder is the answer either way, so a client that wants a group
// blurred never pays a valuation for it. The standing is zero, not an error,
// for a chat that does not exist.
func (a *Access) ListenerStanding(ctx context.Context, chatID *commonpb.ChatId, userID *commonpb.UserId, mode messagingpb.ViewMode) (ListenerStanding, error) {
	return a.standing(ctx, chatID, userID, mode, a.storedMembership(chatID, userID), func(ctx context.Context) (ChatRules, error) {
		return a.rules.rulesFor(ctx, chatID, userID)
	})
}

// ListenerStandingWithRules is ListenerStanding for a caller already holding the chat's rules
// (read off a canonical record it loaded for its own purposes, see
// Chat.ChatRules), so they are not read a second time. The chat is taken as
// existing: a caller that has its rules has already told NOT_FOUND from
// everything else. A public group whose rules carry no listener rule admits
// every non-member, and a private group none whatever its rules (see
// Access). On error the standing is zero.
func (a *Access) ListenerStandingWithRules(ctx context.Context, chatID *commonpb.ChatId, rules ChatRules, userID *commonpb.UserId, mode messagingpb.ViewMode) (ListenerStanding, error) {
	return a.standing(ctx, chatID, userID, mode, a.storedMembership(chatID, userID), func(context.Context) (ChatRules, error) {
		return rules, nil
	})
}

// ListenerStandingWithChat is ListenerStanding for a caller already holding the chat's
// canonical record, which decides all it can before the store is asked
// again: a DM's membership is read off the record's inline members (see
// Chat.HasMember), so a DM costs no membership read at all, and a group's
// rules come off the record as for ListenerStandingWithRules. A group's membership
// is not on its record and is read from the store, strongly consistent, as
// every gate reads it. The chat is taken as existing, as a caller holding
// its record has established.
func (a *Access) ListenerStandingWithChat(ctx context.Context, c *Chat, userID *commonpb.UserId, mode messagingpb.ViewMode) (ListenerStanding, error) {
	return a.standing(ctx, c.ID, userID, mode, a.recordMembership(c, userID), func(context.Context) (ChatRules, error) {
		return c.ChatRules(), nil
	})
}

// PublicListenerStanding is the standing of a viewer who is no one — an
// unauthenticated read of a chat's public view (see chat.Server.GetChat) —
// towards the chat whose canonical record the caller holds. They are on no
// roster and are evaluated against no rule, so they stand exactly where a
// registered non-member reading under REDACTED does: a public group, with or
// without listener rules, may be previewed, and nothing else admits them in
// any form. Nothing is read: the governance comes off the record.
func (a *Access) PublicListenerStanding(c *Chat) ListenerStanding {
	if gov, _ := governanceOf(c.ID, c.ChatRules()); gov != governancePublicGroup {
		return ListenerStanding{}
	}
	return ListenerStanding{CanPreview: true}
}

// IsMemberWithChat is IsMember for a caller already holding the chat's
// canonical record: a DM is answered off the record, a group from the store
// (see ListenerStandingWithChat).
func (a *Access) IsMemberWithChat(ctx context.Context, c *Chat, userID *commonpb.UserId) (bool, error) {
	return a.recordMembership(c, userID)(ctx)
}

// storedMembership is the membership read every standing makes unless the
// caller can answer it itself: the store's, strongly consistent.
func (a *Access) storedMembership(chatID *commonpb.ChatId, userID *commonpb.UserId) func(context.Context) (bool, error) {
	return func(ctx context.Context) (bool, error) {
		return a.chats.IsMember(ctx, chatID, userID)
	}
}

// recordMembership is the membership read for a caller holding the chat's
// canonical record: a DM's off the record's inline members, a group's from
// the store, since a group's record carries none.
func (a *Access) recordMembership(c *Chat, userID *commonpb.UserId) func(context.Context) (bool, error) {
	if IsGroupChatID(c.ID) {
		return a.storedMembership(c.ID, userID)
	}
	return func(context.Context) (bool, error) { return c.HasMember(userID), nil }
}

// governance is how a chat decides who reads and speaks in it beyond its
// roster. Every gate that looks past membership switches on it, so each kind
// of chat is answered in one named place rather than by exceptions to the
// others:
//
//   - governanceDm: a DM. Its two members are its audience, fixed by its ID;
//     no one else is admitted in any form. Its members are evaluated against
//     the rules derived for it (a Never speaker rule in a DM with the team
//     account, see RuleEvaluator.RulesOf), and speak plaintext or under the
//     pairwise scheme.
//   - governancePublicGroup: a public group. A non-member reads it in full
//     when they satisfy its listener rules (everyone does when it carries
//     none) and redacted otherwise; its members speak plaintext when they
//     satisfy its listener and speaker rules.
//   - governancePrivateGroup: a private group (see Chat.IsPrivate). Its
//     creator admits its members, so no non-member is admitted in any form,
//     and whether its members speak is decided by its key; rules are never
//     evaluated for it, whatever its record carries (see RuleSet).
type governance uint8

const (
	governanceDm governance = iota
	governancePublicGroup
	governancePrivateGroup
)

// governanceOf returns how chatID is governed (see governance) and, for a chat
// governed by rules — a DM's derived ones included — the rule set to evaluate.
// The rule set is zero for a private group, which has none. Privacy is a
// group's alone (see Chat.IsPrivate), so it is decided first.
func governanceOf(chatID *commonpb.ChatId, rules ChatRules) (governance, RuleSet) {
	ruleSet, ruled := rules.RuleSet()
	switch {
	case !ruled:
		return governancePrivateGroup, RuleSet{}
	case !IsGroupChatID(chatID):
		return governanceDm, ruleSet
	default:
		return governancePublicGroup, ruleSet
	}
}

// standing is the shared shape of ListenerStanding, ListenerStandingWithRules and
// ListenerStandingWithChat: membership, which answers for a member of any chat
// without the rules being read, then for a non-member, what the chat's
// governance admits them to. membership answers the membership question —
// from the store, or off a record the caller holds — and loadRules supplies
// the chat's rules likewise, called only for a non-member; ErrChatNotFound
// from it is a plain refusal.
func (a *Access) standing(ctx context.Context, chatID *commonpb.ChatId, userID *commonpb.UserId, mode messagingpb.ViewMode, membership func(context.Context) (bool, error), loadRules func(context.Context) (ChatRules, error)) (ListenerStanding, error) {
	isMember, err := membership(ctx)
	if err != nil {
		return ListenerStanding{}, err
	}
	if isMember {
		return memberListenerStanding, nil
	}

	// A remembered admission answers before the rules are read: only a
	// non-member of a group governed by its rules is ever remembered (see
	// publicGroupNonMember). A redacted read is answered without the rules'
	// verdict, so it neither consults nor makes one.
	if mode != messagingpb.ViewMode_REDACTED && a.admitted != nil {
		if _, ok := a.admitted.Get(admissionKey(chatID, userID)); ok {
			return ListenerStanding{CanListen: true, CanPreview: true}, nil
		}
	}

	rules, err := loadRules(ctx)
	if errors.Is(err, ErrChatNotFound) {
		return ListenerStanding{}, nil
	}
	if err != nil {
		return ListenerStanding{}, err
	}
	switch gov, ruleSet := governanceOf(chatID, rules); gov {
	case governanceDm:
		// Its two members are its audience; its rules, derived for them, say
		// nothing of anyone else.
		return ListenerStanding{}, nil
	case governancePublicGroup:
		return a.publicGroupNonMember(ctx, chatID, ruleSet, userID, mode)
	default:
		// Its creator admits its audience, so no non-member reads it in any
		// form, whatever rules its record carries.
		return ListenerStanding{}, nil
	}
}

// publicGroupNonMember is a non-member's standing in a group governed by its rules
// (see Access): under REDACTED a preview, without the rules being evaluated;
// otherwise a full read for whoever satisfies the listener rules — everyone,
// when the group carries none — and a preview for whoever does not. An
// admission is remembered for listenerAdmissionTTL only when a listener rule
// was evaluated for it: an open group's costs nothing to find again.
func (a *Access) publicGroupNonMember(ctx context.Context, chatID *commonpb.ChatId, rules RuleSet, userID *commonpb.UserId, mode messagingpb.ViewMode) (ListenerStanding, error) {
	if mode == messagingpb.ViewMode_REDACTED {
		return ListenerStanding{CanPreview: true}, nil
	}
	ok, err := a.rules.CanListenWithRules(ctx, rules, userID)
	if err != nil {
		return ListenerStanding{}, err
	}
	if !ok {
		return ListenerStanding{CanPreview: true}, nil
	}
	if a.admitted != nil && rules.hasListenerRules() {
		a.admitted.SetWithTTL(admissionKey(chatID, userID), struct{}{}, a.admittedTTL)
	}
	return ListenerStanding{CanListen: true, CanPreview: true}, nil
}

// Encryption is what a chat says of encrypted content in it (see SpeakerStanding):
// whether it takes any, and whether it takes anything else.
type Encryption uint8

const (
	// EncryptionNone: the chat takes plaintext alone. A public group.
	EncryptionNone Encryption = iota
	// EncryptionOptional: the chat takes plaintext, and encrypted content
	// under SpeakerStanding.Scheme. A DM, whose pairwise key either member's client
	// derives on its own, so a conversation can mix the two as clients
	// adopt it.
	EncryptionOptional
	// EncryptionRequired: the chat takes encrypted content under
	// SpeakerStanding.Scheme and nothing else. A private group with its key (see
	// Chat.IsPrivate): the key is the group's definition, so plaintext never
	// appears in one.
	EncryptionRequired
)

// SpeakerStanding is a user's standing to speak in a chat, the way ListenerStanding is their
// standing to read it: whether they may send in it at all (see CanSpeak), and
// if so what the chat takes from them as far as encryption goes, which is the
// chat's and the same for every speaker in it. Who may speak and what a chat
// takes are decided together so that nothing outside this package has to know
// what makes a chat take encrypted content — a DM's pairwise key, a private
// group's stored chat key — only that it does. A refused speaker is the zero
// value: CanSpeak false and nothing about the chat.
type SpeakerStanding struct {
	CanSpeak bool

	// Encryption is how the chat takes encrypted content (see Encryption).
	Encryption Encryption

	// Scheme is the scheme encrypted content in the chat must carry, the one
	// thing in it the server reads. Unset when Encryption is EncryptionNone.
	Scheme messagingpb.EncryptedContent_Scheme
}

// takesEncrypted reports whether the chat takes encrypted content from the
// speaker at all: they may speak, and the chat takes some. It is the second
// half of the gate on an end-to-end encrypted blob upload for the chat (see
// NewBlobEncryptedUploadGate), which is sent as encrypted content.
func (s SpeakerStanding) takesEncrypted() bool {
	return s.CanSpeak && s.Encryption != EncryptionNone
}

// CanSpeak reports whether userID may send in chatID (see SpeakerStanding), for a
// caller that sends nothing the chat could judge: a deletion, a typing
// notification, a mention suggestion.
func (a *Access) CanSpeak(ctx context.Context, chatID *commonpb.ChatId, userID *commonpb.UserId) (bool, error) {
	speaker, err := a.SpeakerStanding(ctx, chatID, userID)
	return speaker.CanSpeak, err
}

// SpeakerStanding is userID's standing to speak in chatID (see SpeakerStanding): a
// non-member never speaks, and a member speaks as the chat's governance
// decides (see governance). Membership is checked first, so a chat's rules
// are never evaluated for a send on behalf of a non-member. It is the zero
// value, not an error, for a chat that does not exist.
func (a *Access) SpeakerStanding(ctx context.Context, chatID *commonpb.ChatId, userID *commonpb.UserId) (SpeakerStanding, error) {
	isMember, err := a.chats.IsMember(ctx, chatID, userID)
	if err != nil || !isMember {
		return SpeakerStanding{}, err
	}
	rules, err := a.rules.rulesFor(ctx, chatID, userID)
	if errors.Is(err, ErrChatNotFound) {
		// Membership just confirmed the chat; a not-found here is a chat deleted
		// between the two reads, which admits no one.
		return SpeakerStanding{}, nil
	}
	if err != nil {
		return SpeakerStanding{}, err
	}
	switch gov, ruleSet := governanceOf(chatID, rules); gov {
	case governanceDm:
		return a.dmSpeaker(ctx, ruleSet, userID)
	case governancePublicGroup:
		return a.publicGroupSpeaker(ctx, chatID, ruleSet, userID)
	default:
		return a.privateGroupSpeaker(ctx, chatID, rules)
	}
}

// dmSpeaker is a DM member's standing to speak: they speak when they satisfy
// the DM's derived rules — always, except in a DM with the team account, whose
// Never rule no one satisfies — and the DM takes plaintext or content under
// its pairwise scheme, which either member's client derives on its own.
func (a *Access) dmSpeaker(ctx context.Context, rules RuleSet, userID *commonpb.UserId) (SpeakerStanding, error) {
	ok, err := a.rules.CanSpeakWithRules(ctx, rules, userID)
	if err != nil || !ok {
		return SpeakerStanding{}, err
	}
	return SpeakerStanding{
		CanSpeak:   true,
		Encryption: EncryptionOptional,
		Scheme:     messagingpb.EncryptedContent_X25519_XCHACHA20POLY1305,
	}, nil
}

// publicGroupSpeaker is a public group member's standing to speak: they speak when
// they satisfy its listener and speaker rules, evaluated against their state
// now or remembered from the last speakerAdmissionTTL (see Access), and the
// group takes plaintext alone. The cache is neither consulted nor written for
// a group whose verdict reads nothing beyond its rules (see
// RuleSet.speakingReadsState).
func (a *Access) publicGroupSpeaker(ctx context.Context, chatID *commonpb.ChatId, rules RuleSet, userID *commonpb.UserId) (SpeakerStanding, error) {
	key := admissionKey(chatID, userID)
	remember := a.speakers != nil && rules.speakingReadsState()
	if remember {
		if _, ok := a.speakers.Get(key); ok {
			return SpeakerStanding{CanSpeak: true}, nil
		}
	}
	ok, err := a.rules.CanSpeakWithRules(ctx, rules, userID)
	if err != nil || !ok {
		return SpeakerStanding{}, err
	}
	if remember {
		a.speakers.SetWithTTL(key, struct{}{}, a.speakersTTL)
	}
	return SpeakerStanding{CanSpeak: true}, nil
}

// privateGroupSpeaker is a private group member's standing to speak: they speak
// exactly when the group has its key — its creator has stored a key envelope
// (see KeyEnvelope) — and no rule is evaluated, since none admitted them. A
// keyless group's members are refused, and it is every gate CanSpeak stands
// behind at once that refuses them: a send, an edit, a deletion, a typing
// notification, and mention suggestions. A keyed group's members speak with
// EncryptionRequired under the chat key's scheme, so messaging admits nothing
// but encrypted content there (see messaging.Server.SendMessage). Whether a
// member holds an envelope of their own is not asked: the key is the group's,
// and a member without one cannot read what they would write, which is their
// client's to notice.
//
// The key is found with the rules read every gate makes, which a store caches
// with the rules, and then one strongly consistent point read of the
// creator's envelope. A group found keyed is remembered for the life of the
// process and never read again: the creator's envelope is never deleted (see
// Store.RemoveGroupMember), so a group that has its key has it for good. A
// group found keyless is not remembered, so its first message after the
// creator stores the key is admitted at once, and until then each of its
// members' refused sends costs that one read. A private group with no
// recorded creator can have no key, since the creator's envelope is the only
// possible first one.
func (a *Access) privateGroupSpeaker(ctx context.Context, chatID *commonpb.ChatId, rules ChatRules) (SpeakerStanding, error) {
	keyed, err := a.hasKey(ctx, chatID, rules)
	if err != nil || !keyed {
		return SpeakerStanding{}, err
	}
	return SpeakerStanding{
		CanSpeak:   true,
		Encryption: EncryptionRequired,
		Scheme:     messagingpb.EncryptedContent_CHAT_KEY_XCHACHA20POLY1305,
	}, nil
}

// hasKey reports whether the private group whose rules the caller holds has
// its chat key (see privateGroupSpeaker). It is false for a public group.
func (a *Access) hasKey(ctx context.Context, chatID *commonpb.ChatId, rules ChatRules) (bool, error) {
	if !rules.IsPrivate {
		return false, nil
	}
	key := string(chatID.Value)
	if _, ok := a.keyed.Get(key); ok {
		return true, nil
	}
	if rules.CreatorID == nil {
		return false, nil
	}
	_, err := a.chats.GetKeyEnvelope(ctx, chatID, rules.CreatorID)
	if errors.Is(err, ErrKeyEnvelopeNotFound) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	a.keyed.Set(key, struct{}{})
	return true, nil
}

// admissionKey keys the admission cache by (group, user). Group IDs are fixed
// width (GroupChatIDSize), so concatenating the raw bytes is unambiguous.
func admissionKey(chatID *commonpb.ChatId, userID *commonpb.UserId) string {
	return string(chatID.Value) + string(userID.Value)
}
