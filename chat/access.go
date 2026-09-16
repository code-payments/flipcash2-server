package chat

import (
	"context"
	"errors"
	"time"

	"github.com/ReneKroon/ttlcache"

	chatpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/chat/v1"
	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"
)

// DefaultListenerAdmissionTTL is how long Access remembers that a non-member
// satisfied a group's listener rules (see Access.CanListen). It bounds how long a
// reader whose balance has since dropped, or whose staff flag was since
// revoked, keeps reading before the rules are asked again.
const DefaultListenerAdmissionTTL = 30 * time.Second

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
//     group, and only a group, if they satisfy its listener rules right now.
//   - CanSpeak: may the user produce something other members see — a message,
//     an edit, a deletion, a typing notification. Members only, and only those
//     who satisfy the chat's listener and speaker rules.
//
// Membership is always checked first: it is the cheaper check (a keyed read,
// cached for a DM), and it answers for a member without evaluating a rule — a
// member whose balance has since dipped under the group's requirement is still
// a member and still reads (see messaging/access.go for why membership stands
// for the rules on a member's reads). Rules are evaluated only where they
// decide the answer: for a non-member's read of a group, and for any send.
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
// A DM admits its two members and no one else; its rules are nil, and nil
// rules admit everyone, so a DM must never reach the rules fallback. A group
// without listener rules admits no non-member either: the rules are the only
// thing that can admit a non-member, and an empty set is not taken as
// admitting everyone, even though that is what the RuleEvaluator says of it.
// Every group created through StartChat carries a minimum listener balance
// (see RulesFromProto), but the store does not require one, and a group
// written before that rule existed, or by an operator, may carry none; such a
// group stays its members' alone rather than becoming readable by every
// registered user. If an open group is ever wanted, it is an explicit rule to
// add here, not the absence of one.
//
// A chat that does not exist admits no one: IsMember, CanListen and CanSpeak all
// report false, not ErrChatNotFound, for a chat ID nothing is stored under. A
// caller that must tell NOT_FOUND from DENIED reads the canonical record itself
// and asks with CanListenWithRules or StandingWithRules.
type Access struct {
	chats Store
	rules *RuleEvaluator

	// admitted remembers, by (group, user), that a non-member satisfied the
	// group's listener rules. Positive entries only (see above). A nil cache
	// means admissions are not remembered (see WithListenerAdmissionTTL).
	admitted    *ttlcache.Cache
	admittedTTL time.Duration
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

// Standing is a user's relation to a chat as Access sees it: whether they are
// on its roster, and whether they may read it (see Access.CanListen). A member
// may always read; a non-member with CanListen is a group's qualifying
// non-member, admitted by its listener rules.
type Standing struct {
	IsMember  bool
	CanListen bool
}

// memberStanding is a member's standing, for a caller that has established
// membership by other means — a feed built from the viewer's own memberships, a
// join that just landed.
var memberStanding = Standing{IsMember: true, CanListen: true}

// CanListen reports whether userID may read chatID: they are a member, or chatID
// is a group whose listener rules they satisfy (see Access). It is false, not
// an error, for a chat that does not exist.
func (a *Access) CanListen(ctx context.Context, chatID *commonpb.ChatId, userID *commonpb.UserId) (bool, error) {
	standing, err := a.standing(ctx, chatID, userID, func(ctx context.Context) (*chatpb.Rules, error) {
		return a.chats.GetGroupRules(ctx, chatID)
	})
	return standing.CanListen, err
}

// CanListenWithRules is CanListen for a caller already holding the chat's rules
// (see StandingWithRules).
func (a *Access) CanListenWithRules(ctx context.Context, chatID *commonpb.ChatId, rules *chatpb.Rules, userID *commonpb.UserId) (bool, error) {
	standing, err := a.StandingWithRules(ctx, chatID, rules, userID)
	return standing.CanListen, err
}

// StandingWithRules answers both of CanListen's questions — is userID a member,
// and may they read — in one membership read, for a caller already holding the
// chat's rules (read off a canonical record it loaded for its own purposes,
// see Chat.Rules), so they are not read a second time. The chat is taken as
// existing: a caller that has its rules has already told NOT_FOUND from
// everything else. A nil rules is a chat with none, which admits no
// non-member (see Access). On error the standing is zero.
func (a *Access) StandingWithRules(ctx context.Context, chatID *commonpb.ChatId, rules *chatpb.Rules, userID *commonpb.UserId) (Standing, error) {
	return a.standing(ctx, chatID, userID, func(context.Context) (*chatpb.Rules, error) {
		return rules, nil
	})
}

// standing is the shared shape of CanListen and StandingWithRules: membership,
// then for a non-member of a group only, the remembered or freshly evaluated
// listener rules. loadRules supplies the group's rules — from the store, or
// off a record the caller holds — and is called only when they decide the
// answer; ErrChatNotFound from it is a plain refusal.
func (a *Access) standing(ctx context.Context, chatID *commonpb.ChatId, userID *commonpb.UserId, loadRules func(context.Context) (*chatpb.Rules, error)) (Standing, error) {
	isMember, err := a.chats.IsMember(ctx, chatID, userID)
	if err != nil {
		return Standing{}, err
	}
	if isMember {
		return Standing{IsMember: true, CanListen: true}, nil
	}

	// Only a group admits a non-member. A DM's rules are nil and would admit
	// everyone, so the fallback is never reached for one.
	if !IsGroupChatID(chatID) {
		return Standing{}, nil
	}

	key := admissionKey(chatID, userID)
	if a.admitted != nil {
		if _, ok := a.admitted.Get(key); ok {
			return Standing{CanListen: true}, nil
		}
	}

	rules, err := loadRules(ctx)
	if errors.Is(err, ErrChatNotFound) {
		return Standing{}, nil
	}
	if err != nil {
		return Standing{}, err
	}
	// No listener rules, no admission: only a rule can admit a non-member
	// (see above).
	if len(rules.GetListener()) == 0 {
		return Standing{}, nil
	}
	ok, err := a.rules.CanListenWithRules(ctx, rules, userID)
	if err != nil || !ok {
		return Standing{}, err
	}
	if a.admitted != nil {
		a.admitted.SetWithTTL(key, struct{}{}, a.admittedTTL)
	}
	return Standing{CanListen: true}, nil
}

// CanSpeak reports whether userID may send in chatID: they are a member, and
// they satisfy the chat's listener and speaker rules. Membership is checked
// first, so a chat's rules are never evaluated for a send on behalf of a
// non-member. It is false, not an error, for a chat that does not exist.
func (a *Access) CanSpeak(ctx context.Context, chatID *commonpb.ChatId, userID *commonpb.UserId) (bool, error) {
	isMember, err := a.chats.IsMember(ctx, chatID, userID)
	if err != nil || !isMember {
		return false, err
	}
	ok, err := a.rules.CanSpeak(ctx, chatID, userID)
	if errors.Is(err, ErrChatNotFound) {
		// Membership just confirmed the chat; a not-found here is a chat deleted
		// between the two reads, which admits no one.
		return false, nil
	}
	return ok, err
}

// admissionKey keys the admission cache by (group, user). Group IDs are fixed
// width (GroupChatIDSize), so concatenating the raw bytes is unambiguous.
func admissionKey(chatID *commonpb.ChatId, userID *commonpb.UserId) string {
	return string(chatID.Value) + string(userID.Value)
}
