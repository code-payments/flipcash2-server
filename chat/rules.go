package chat

import (
	"context"
	"errors"
	"fmt"
	"math"

	chatpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/chat/v1"
	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"

	currency_lib "github.com/code-payments/ocp-server/currency"
	ocp_common "github.com/code-payments/ocp-server/ocp/common"

	"github.com/code-payments/flipcash2-server/account"
	"github.com/code-payments/flipcash2-server/balance"
)

// Rules projects the chat's stored participation requirements onto a
// chatpb.Rules, or nil when the chat has none — which is every DM, and every
// group not created with a requirement. The proto is the single vocabulary for
// rules: what the client is shown (Metadata.rules) is exactly what the server
// evaluates (RuleEvaluator), so the two can never disagree about what a chat
// requires.
//
// Every requirement a group carries is a listener rule: a StaffRequirement for
// a staff-only group, a MinimumBalanceRequirement for a group with a minimum
// listener balance, or both. Listener rules gate reading and joining, and a
// member must be able to listen before they can speak, so restricting the
// audience restricts the speakers too without repeating a rule in the speaker
// class — which no group carries yet. The rules are listed cheapest to evaluate
// first, since an evaluator stops at the first one a user fails: a staff check
// is a flag read, a balance check a valuation.
func (c *Chat) Rules() *chatpb.Rules {
	if c.Type != chatpb.ChatType_GROUP {
		return nil
	}
	var listener []*chatpb.ListenerRules
	if c.IsStaffOnly {
		listener = append(listener, &chatpb.ListenerRules{
			Kind: &chatpb.ListenerRules_Staff{Staff: &chatpb.StaffRequirement{}},
		})
	}
	if c.MinimumListenerBalance != nil {
		listener = append(listener, &chatpb.ListenerRules{
			Kind: &chatpb.ListenerRules_MinimumBalance{MinimumBalance: c.MinimumListenerBalance.ToProto()},
		})
	}
	if len(listener) == 0 {
		return nil
	}
	return &chatpb.Rules{Listener: listener}
}

// ErrInvalidRules is returned by RulesFromProto for a rule set a group cannot
// carry (see RulesFromProto for what one can).
var ErrInvalidRules = errors.New("invalid chat rules")

// RulesFromProto validates a rule set a client asked a new group to carry and
// projects it onto the stored fields Rules projects back from, so that a group
// created with rules shows exactly the rules it was asked for.
//
// It accepts what a group can store today, and nothing more, so that a rule
// is never accepted and then silently dropped: listener rules only, since no
// group carries a speaker rule yet; each kind at most once, since the record
// holds one of each; and a minimum balance in USD only, with a positive amount,
// since that is the only balance the evaluator can answer (see
// satisfiesMinimumBalance). The requirement's mints are taken as given: the
// proto bounds how many, and validation bounds their shape. Anything else is
// ErrInvalidRules — a rule the server cannot enforce is refused up front rather
// than stored and failed on every evaluation.
//
// It also requires what every group must carry today: a minimum listener
// balance. A set without one — nil, empty, or staff-only — is ErrInvalidRules,
// so no group is created that a holder of nothing could join.
func RulesFromProto(rules *chatpb.Rules) (isStaffOnly bool, minimumListenerBalance *MinimumBalance, err error) {
	if len(rules.GetSpeaker()) > 0 {
		return false, nil, fmt.Errorf("%w: speaker rules are not supported", ErrInvalidRules)
	}
	for _, rule := range rules.GetListener() {
		switch k := rule.GetKind().(type) {
		case *chatpb.ListenerRules_Staff:
			if isStaffOnly {
				return false, nil, fmt.Errorf("%w: duplicate staff requirement", ErrInvalidRules)
			}
			isStaffOnly = true
		case *chatpb.ListenerRules_MinimumBalance:
			if minimumListenerBalance != nil {
				return false, nil, fmt.Errorf("%w: duplicate minimum balance requirement", ErrInvalidRules)
			}
			req := k.MinimumBalance
			if currency_lib.Code(req.GetAmount().GetCurrency()) != currency_lib.USD {
				return false, nil, fmt.Errorf("%w: unsupported minimum balance currency %q", ErrInvalidRules, req.GetAmount().GetCurrency())
			}
			amount := req.GetAmount().GetNativeAmount()
			if math.IsNaN(amount) || math.IsInf(amount, 0) || amount <= 0 {
				return false, nil, fmt.Errorf("%w: minimum balance amount must be positive", ErrInvalidRules)
			}
			mints := make([]*commonpb.PublicKey, len(req.GetMints()))
			for i, mint := range req.GetMints() {
				mints[i] = &commonpb.PublicKey{Value: append([]byte(nil), mint.GetValue()...)}
			}
			minimumListenerBalance = &MinimumBalance{
				Currency:     string(currency_lib.USD),
				NativeAmount: amount,
				Mints:        mints,
			}
		default:
			return false, nil, fmt.Errorf("%w: unsupported listener rule %T", ErrInvalidRules, k)
		}
	}
	if minimumListenerBalance == nil {
		return false, nil, fmt.Errorf("%w: a minimum listener balance is required", ErrInvalidRules)
	}
	return isStaffOnly, minimumListenerBalance, nil
}

// RuleEvaluator decides whether a user satisfies a chat's participation rules
// (see Chat.Rules). It evaluates rules only: membership is a separate, cheaper
// check the caller makes first, so a chat's rules are never evaluated — and
// its requirement never probed — on behalf of a non-member. Only a group can
// carry rules; a DM's evaluation never touches the store.
//
// Rules are read through Store.GetGroupRules — in production the caching
// store, which holds every group's rules after its first read. A
// StaffRequirement is answered by the account store's staff flag, and a
// MinimumBalanceRequirement by the balance client's valuation of the user's
// holdings (see satisfiesMinimumBalance).
//
// Rules are evaluated against the current state of their subject, not the
// state at join time: a member who no longer satisfies a listener rule (a
// staff member whose flag was revoked, a holder whose balance dropped) keeps
// their membership record but is denied on every path that evaluates the
// rules until they satisfy it again. Not every path does: the messaging
// service evaluates rules on sends only, and gates reads on membership alone,
// so that a read never pays for an evaluation (see messaging.canListen). The
// intended design is for membership itself to track the listener rules — a
// member who stops satisfying one is removed — at which point the membership
// record is the rules' answer everywhere. Enforcing rules on membership
// changes, a join gated by listener rules included, is the job of whatever
// path mutates membership.
type RuleEvaluator struct {
	accounts account.Store
	balances *balance.Client
	chats    Store
}

func NewRuleEvaluator(accounts account.Store, balances *balance.Client, chats Store) *RuleEvaluator {
	return &RuleEvaluator{accounts: accounts, balances: balances, chats: chats}
}

// CanListen reports whether userID satisfies every listener rule of chatID —
// the requirements to read (and join) the chat. A chat with no listener rules
// admits everyone. It returns ErrChatNotFound if a group chat does not exist.
func (e *RuleEvaluator) CanListen(ctx context.Context, chatID *commonpb.ChatId, userID *commonpb.UserId) (bool, error) {
	rules, err := e.rulesFor(ctx, chatID)
	if err != nil {
		return false, err
	}
	return e.satisfiesListener(ctx, rules, userID)
}

// CanListenWithRules is CanListen for a caller that already holds the chat's
// rules — read off a canonical record it loaded for its own purposes, or
// taken from a request for a chat that does not exist yet — so the rules are
// not read a second time. A nil rules admits everyone, as a chat with none
// does.
func (e *RuleEvaluator) CanListenWithRules(ctx context.Context, rules *chatpb.Rules, userID *commonpb.UserId) (bool, error) {
	return e.satisfiesListener(ctx, rules, userID)
}

// CanSpeak reports whether userID satisfies every listener and speaker rule of
// chatID — the requirements to send messages in the chat. Speaker rules apply
// on top of listener rules: a user who cannot listen cannot speak, whatever the
// speaker rules say. It returns ErrChatNotFound if a group chat does not exist.
func (e *RuleEvaluator) CanSpeak(ctx context.Context, chatID *commonpb.ChatId, userID *commonpb.UserId) (bool, error) {
	rules, err := e.rulesFor(ctx, chatID)
	if err != nil {
		return false, err
	}
	return e.satisfiesSpeaker(ctx, rules, userID)
}

// CanSpeakWithRules is CanSpeak for a caller that already holds the chat's
// rules (see CanListenWithRules). A nil rules admits everyone.
func (e *RuleEvaluator) CanSpeakWithRules(ctx context.Context, rules *chatpb.Rules, userID *commonpb.UserId) (bool, error) {
	return e.satisfiesSpeaker(ctx, rules, userID)
}

func (e *RuleEvaluator) satisfiesListener(ctx context.Context, rules *chatpb.Rules, userID *commonpb.UserId) (bool, error) {
	for _, rule := range rules.GetListener() {
		ok, err := e.satisfies(ctx, rule.GetKind(), userID)
		if err != nil || !ok {
			return false, err
		}
	}
	return true, nil
}

// satisfiesSpeaker evaluates the listener rules and then the speaker rules,
// stopping at the first the user fails.
func (e *RuleEvaluator) satisfiesSpeaker(ctx context.Context, rules *chatpb.Rules, userID *commonpb.UserId) (bool, error) {
	if ok, err := e.satisfiesListener(ctx, rules, userID); err != nil || !ok {
		return false, err
	}
	for _, rule := range rules.GetSpeaker() {
		ok, err := e.satisfies(ctx, rule.GetKind(), userID)
		if err != nil || !ok {
			return false, err
		}
	}
	return true, nil
}

// rulesFor returns chatID's rules, nil when it has none. A DM is answered
// without a read: rules are group-only, and a DM's ID says which it is.
func (e *RuleEvaluator) rulesFor(ctx context.Context, chatID *commonpb.ChatId) (*chatpb.Rules, error) {
	if !IsGroupChatID(chatID) {
		return nil, nil
	}
	return e.chats.GetGroupRules(ctx, chatID)
}

// satisfies evaluates one rule, of either class, for userID. The two classes
// share a vocabulary of requirement kinds, so a single switch covers both.
//
// A kind the evaluator does not know how to answer is an error, not a pass:
// a rule is a restriction, and the safe failure for a restriction the server
// cannot evaluate is to admit no one. The Server projects such an error as an
// Internal failure, never as an OK.
func (e *RuleEvaluator) satisfies(ctx context.Context, kind any, userID *commonpb.UserId) (bool, error) {
	switch k := kind.(type) {
	case *chatpb.ListenerRules_Staff, *chatpb.SpeakerRules_Staff:
		return e.accounts.IsStaff(ctx, userID)
	case *chatpb.ListenerRules_MinimumBalance:
		return e.satisfiesMinimumBalance(ctx, k.MinimumBalance, userID)
	case *chatpb.SpeakerRules_MinimumBalance:
		return e.satisfiesMinimumBalance(ctx, k.MinimumBalance, userID)
	default:
		return false, fmt.Errorf("unsupported chat rule %T", k)
	}
}

// satisfiesMinimumBalance reports whether userID holds at least the required
// amount, in the required mints, right now.
//
// Only a USD requirement can be answered: the balance client values holdings in
// USDF, a USD stablecoin, so the requirement compares directly against what it
// returns with no exchange rate in between. A requirement in any other currency
// is, like an unknown rule kind, an error and never a pass. The comparison is
// made in quarks, the requirement rounded to the nearest quark, so a balance
// that is exactly the requirement satisfies it whatever the float arithmetic
// on the way in.
//
// A user with no owner account holds nothing, and fails as a zero balance would;
// a balance that cannot be read is an error, so the gate is never left
// unenforced.
func (e *RuleEvaluator) satisfiesMinimumBalance(ctx context.Context, req *chatpb.MinimumBalanceRequirement, userID *commonpb.UserId) (bool, error) {
	if currency_lib.Code(req.GetAmount().GetCurrency()) != currency_lib.USD {
		return false, fmt.Errorf("unsupported minimum balance currency %q", req.GetAmount().GetCurrency())
	}
	required := uint64(math.Round(req.GetAmount().GetNativeAmount() * float64(ocp_common.CoreMintQuarksPerUnit)))

	held, err := e.balances.GetTotalUsdfBalance(ctx, userID, req.GetMints()...)
	if errors.Is(err, balance.ErrNotFound) {
		held = 0
	} else if err != nil {
		return false, err
	}
	return held >= required, nil
}
