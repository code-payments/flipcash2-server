package chat

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"

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
// A group's listener rules are a StaffRequirement for a staff-only group, a
// MinimumBalanceRequirement for a group with a minimum listener balance, or
// both. Listener rules gate reading and joining, and a member must be able to
// listen before they can speak, so restricting the audience restricts the
// speakers too without repeating a rule in the speaker class. The rules are
// listed cheapest to evaluate first, since an evaluator stops at the first
// one a user fails: a staff check is a flag read, a balance check a
// valuation.
//
// A group's speaker rules are a CreatorRequirement for a group only its
// creator may speak in (see Chat.IsCreatorOnlySpeaker), a
// MinimumBalanceRequirement for a group with a minimum speaker balance (see
// Chat.MinimumSpeakerBalance), or both, again cheapest first. The
// CreatorRequirement names no user: it is evaluated against the creator the
// group recorded, which is read with the rules (see GroupRules).
//
// A DM's record carries none, but a DM with the Flipcash team account carries
// a Never speaker rule, which only the RuleEvaluator, knowing the team, can
// add (see RuleEvaluator.RulesOf): a chat's rules for display or evaluation
// come from there, not from here.
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
	var speaker []*chatpb.SpeakerRules
	if c.IsCreatorOnlySpeaker {
		speaker = append(speaker, &chatpb.SpeakerRules{
			Kind: &chatpb.SpeakerRules_Creator{Creator: &chatpb.CreatorRequirement{}},
		})
	}
	if c.MinimumSpeakerBalance != nil {
		speaker = append(speaker, &chatpb.SpeakerRules{
			Kind: &chatpb.SpeakerRules_MinimumBalance{MinimumBalance: c.MinimumSpeakerBalance.ToProto()},
		})
	}
	if len(listener) == 0 && len(speaker) == 0 {
		return nil
	}
	return &chatpb.Rules{Listener: listener, Speaker: speaker}
}

// GroupRules is what a chat's rules are evaluated against: the rules, the
// creator a CreatorRequirement names, since the rule itself names no one, and
// whether the group is private, which decides who speaks in it before any
// rule does (see Access.CanSpeak). All are fixed at creation and read
// together (see Store.GetGroupRules), so a store that caches one caches the
// others with it, and evaluating a creator-only group, or refusing a send in
// a private one, costs no read beyond its rules.
//
// Everything beside Rules is metadata carried only so that evaluation is
// efficient: it is what a rule is relative to, read with the rules rather
// than looked up per evaluation. It is not part of what a client is shown
// (Metadata.rules is Rules alone), and a field belongs here only if it is
// fixed at creation like the rules, since it is cached with them forever.
type GroupRules struct {
	// Rules are the chat's rules, nil when it has none (see Chat.Rules).
	Rules *chatpb.Rules

	// CreatorID is the group's creator, nil when it has none recorded (see
	// Chat.CreatorID), in which case a CreatorRequirement admits no one.
	CreatorID *commonpb.UserId

	// IsPrivate is whether the group is private (see Chat.IsPrivate). It is
	// not a rule, and it overrides them: no rule admits anyone to a private
	// group, whatever rules its record carries (see RuleEvaluator).
	IsPrivate bool
}

// GroupRules returns the chat's rules, creator and privacy as they are
// evaluated (see GroupRules).
func (c *Chat) GroupRules() GroupRules {
	return GroupRules{Rules: c.Rules(), CreatorID: c.CreatorID, IsPrivate: c.IsPrivate}
}

// isCreator reports whether userID is the recorded creator; false when none
// is recorded.
func (r GroupRules) isCreator(userID *commonpb.UserId) bool {
	return r.CreatorID != nil && bytes.Equal(r.CreatorID.Value, userID.GetValue())
}

// ErrInvalidRules is returned by RulesFromProto for a rule set a group cannot
// carry (see RulesFromProto for what one can).
var ErrInvalidRules = errors.New("invalid chat rules")

// RulesFromProto validates a rule set a client asked a new group to carry and
// projects it onto the stored fields Rules projects back from, so that a group
// created with rules shows exactly the rules it was asked for.
//
// It accepts what a group can be created with today, and nothing more, so
// that a rule is never accepted and then silently dropped: listener rules
// only, since no client sets a speaker rule (a creator-only or speaker-gated
// group is made by writing its record, see Chat.IsCreatorOnlySpeaker and
// Chat.MinimumSpeakerBalance); each kind at most once, since the record
// holds one of each; and a minimum balance of at least the currency's minimum
// transfer value — one unit at its last decimal place, a penny for USD, a yen
// for JPY, the smallest amount OCP lets anyone hold or move in that currency
// (see minimumTransferValue). A requirement below it asks for a balance no one
// can distinguish from nothing, so the rule would admit everyone, or no one,
// on rounding alone. The currency is any ISO 4217 code: the proto bounds its
// shape, and whether OCP can value a balance in it is OCP's to say, which it
// does the first time the rule is evaluated — for a new group, against its
// creator, before anything is written (see Server.StartChat). The requirement's
// mints are taken as given: the proto bounds how many, and validation bounds
// their shape. Anything else is ErrInvalidRules — a rule the server cannot
// enforce is refused up front rather than stored and failed on every
// evaluation.
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
			currency := currency_lib.Code(req.GetAmount().GetCurrency())
			if currency == "" {
				return false, nil, fmt.Errorf("%w: minimum balance currency is required", ErrInvalidRules)
			}
			amount := req.GetAmount().GetNativeAmount()
			if minimum := minimumTransferValue(currency); math.IsNaN(amount) || math.IsInf(amount, 0) || amount < minimum {
				return false, nil, fmt.Errorf("%w: minimum balance amount must be at least %s %s", ErrInvalidRules, strconv.FormatFloat(minimum, 'f', -1, 64), strings.ToUpper(string(currency)))
			}
			mints := make([]*commonpb.PublicKey, len(req.GetMints()))
			for i, mint := range req.GetMints() {
				mints[i] = &commonpb.PublicKey{Value: append([]byte(nil), mint.GetValue()...)}
			}
			minimumListenerBalance = &MinimumBalance{
				Currency:     string(currency),
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

// minimumTransferValue is the smallest amount of a currency OCP transfers: one
// unit at the currency's last decimal place (currency.GetDecimals), 0.01 for
// USD and 1 for a currency with no minor unit. It is the floor a minimum
// balance requirement must meet (see RulesFromProto). OCP's own amount
// validation is the source of the definition; it is not exported, so the
// arithmetic is repeated here.
func minimumTransferValue(code currency_lib.Code) float64 {
	return math.Pow10(-currency_lib.GetDecimals(code))
}

// RuleEvaluator decides whether a user satisfies a chat's participation rules
// (see Chat.Rules). It evaluates rules only: membership is a separate, cheaper
// check the caller makes first, so a chat's rules are evaluated on behalf of a
// non-member only where the rules are what admits them — a join, or a
// qualifying non-member's read of a group (see Access). Only a group stores
// rules; a DM's evaluation never touches the store.
//
// A DM with the Flipcash team account carries one rule no record stores: a
// Never speaker rule, since nobody sends in one (see teamDmRules). The team
// writes through the messaging Sender, which no rule gates, and nobody reads
// as it, so a message to it would be read by no one. The rule is decided per
// read off the team account the evaluator is built with, like use_e2ee, so
// that the rule a client is shown (RulesOf, through Metadata.rules) and the
// rule a send is refused by (CanSpeak) are one decision.
//
// A private group (see Chat.IsPrivate) is not the rules' to open. Its members
// are admitted by its creator, so the evaluator admits no one to one: every
// listen and speak evaluation of a private group is false, before any rule is
// read. StartChat writes a private group with no rules, and an empty rule set
// admits everyone, so without this a caller that asked the evaluator alone
// would find a private group open to all; with it, the answer does not depend
// on what rules the record carries. Who reads and speaks in a private group
// is decided on membership, by Access.
//
// Rules are read through Store.GetGroupRules — in production the caching
// store, which holds every group's rules after its first read. A
// StaffRequirement is answered by the account store's staff flag, a
// MinimumBalanceRequirement by the balance client's valuation of the user's
// holdings (see satisfiesMinimumBalance), and a CreatorRequirement by the
// creator read and cached with the rules (see GroupRules), so it costs no
// read of its own.
//
// Rules are evaluated against the current state of their subject, not the
// state at join time: a member who no longer satisfies a listener rule (a
// staff member whose flag was revoked, a holder whose balance dropped) keeps
// their membership record but is denied on every path that evaluates the
// rules until they satisfy it again. Not every path does: a member's read is
// gated on membership alone, so that it never pays for an evaluation; rules
// are evaluated on sends, and on a non-member's read of a group (see Access).
// The intended design is for membership itself to track the listener rules — a
// member who stops satisfying one is removed — at which point the membership
// record is the rules' answer everywhere. Enforcing rules on membership
// changes, a join gated by listener rules included, is the job of whatever
// path mutates membership.
type RuleEvaluator struct {
	accounts account.Store
	balances *balance.Client
	chats    Store

	// teamUserID is the Flipcash team account, nil when there is none.
	teamUserID *commonpb.UserId
}

// NewRuleEvaluator returns a RuleEvaluator. teamUserID is the Flipcash team
// account (see flipcashteam), nil when there is none; it is an argument rather
// than an option so that a parent cannot build an evaluator without deciding
// it, since an evaluator that does not know the team lets users message it.
func NewRuleEvaluator(accounts account.Store, balances *balance.Client, chats Store, teamUserID *commonpb.UserId) *RuleEvaluator {
	var team *commonpb.UserId
	if teamUserID != nil {
		team = &commonpb.UserId{Value: bytes.Clone(teamUserID.Value)}
	}
	return &RuleEvaluator{accounts: accounts, balances: balances, chats: chats, teamUserID: team}
}

// RulesOf returns c's rules as the evaluator evaluates them, nil when it has
// none: a group's stored rules (see Chat.Rules), and for a DM, the Never
// speaker rule when the team account is one of its members (see
// teamDmRules). It is what a client is shown as Metadata.rules.
func (e *RuleEvaluator) RulesOf(c *Chat) *chatpb.Rules {
	if IsGroupChatID(c.ID) {
		return c.Rules()
	}
	if e.teamUserID != nil && c.HasMember(e.teamUserID) {
		return teamDmRules()
	}
	return nil
}

// CanListen reports whether userID satisfies every listener rule of chatID —
// the requirements to read (and join) the chat. A chat with no listener rules
// admits everyone, except a private group, which admits no one (see
// RuleEvaluator). It returns ErrChatNotFound if a group chat does not exist.
func (e *RuleEvaluator) CanListen(ctx context.Context, chatID *commonpb.ChatId, userID *commonpb.UserId) (bool, error) {
	rules, err := e.rulesFor(ctx, chatID, userID)
	if err != nil {
		return false, err
	}
	return e.satisfiesListener(ctx, rules, userID)
}

// CanListenWithRules is CanListen for a caller that already holds the chat's
// rules — read off a canonical record it loaded for its own purposes, or
// taken from a request for a chat that does not exist yet — so the rules are
// not read a second time. A nil rules.Rules admits everyone, as a chat with
// none does, unless rules.IsPrivate. chatID is the chat the rules are evaluated for; no rule reads it
// today, since what a rule is relative to comes with the rules (see
// GroupRules).
func (e *RuleEvaluator) CanListenWithRules(ctx context.Context, chatID *commonpb.ChatId, rules GroupRules, userID *commonpb.UserId) (bool, error) {
	return e.satisfiesListener(ctx, rules, userID)
}

// CanSpeak reports whether userID satisfies every listener and speaker rule of
// chatID — the requirements to send messages in the chat. Speaker rules apply
// on top of listener rules: a user who cannot listen cannot speak, whatever the
// speaker rules say. It returns ErrChatNotFound if a group chat does not exist.
func (e *RuleEvaluator) CanSpeak(ctx context.Context, chatID *commonpb.ChatId, userID *commonpb.UserId) (bool, error) {
	rules, err := e.rulesFor(ctx, chatID, userID)
	if err != nil {
		return false, err
	}
	return e.satisfiesSpeaker(ctx, rules, userID)
}

// CanSpeakWithRules is CanSpeak for a caller that already holds the chat's
// rules (see CanListenWithRules). A nil rules.Rules admits everyone. Rules
// taken from a request for a chat that does not exist yet carry their would-be
// creator, the caller.
func (e *RuleEvaluator) CanSpeakWithRules(ctx context.Context, chatID *commonpb.ChatId, rules GroupRules, userID *commonpb.UserId) (bool, error) {
	return e.satisfiesSpeaker(ctx, rules, userID)
}

// satisfiesListener evaluates the listener rules, stopping at the first the
// user fails. No one satisfies a private group's (see RuleEvaluator).
func (e *RuleEvaluator) satisfiesListener(ctx context.Context, rules GroupRules, userID *commonpb.UserId) (bool, error) {
	if rules.IsPrivate {
		return false, nil
	}
	for _, rule := range rules.Rules.GetListener() {
		ok, err := e.satisfies(ctx, rules, rule.GetKind(), userID)
		if err != nil || !ok {
			return false, err
		}
	}
	return true, nil
}

// satisfiesSpeaker evaluates the listener rules and then the speaker rules,
// stopping at the first the user fails. A private group fails at the listener
// rules, like any chat the user cannot listen to.
func (e *RuleEvaluator) satisfiesSpeaker(ctx context.Context, rules GroupRules, userID *commonpb.UserId) (bool, error) {
	if ok, err := e.satisfiesListener(ctx, rules, userID); err != nil || !ok {
		return false, err
	}
	for _, rule := range rules.Rules.GetSpeaker() {
		ok, err := e.satisfies(ctx, rules, rule.GetKind(), userID)
		if err != nil || !ok {
			return false, err
		}
	}
	return true, nil
}

// rulesFor returns chatID's rules as userID is evaluated against them, with a
// nil Rules when it has none. A DM is answered without a read: a DM stores no rules, and
// whether it is one with the team account is decided off the IDs (see
// inTeamDm). That is exact for a member, which every caller has established
// first, since a DM's members are userID and one other.
func (e *RuleEvaluator) rulesFor(ctx context.Context, chatID *commonpb.ChatId, userID *commonpb.UserId) (GroupRules, error) {
	if !IsGroupChatID(chatID) {
		if e.inTeamDm(chatID, userID) {
			return GroupRules{Rules: teamDmRules()}, nil
		}
		return GroupRules{}, nil
	}
	return e.chats.GetGroupRules(ctx, chatID)
}

// inTeamDm reports whether chatID is a DM that userID shares with the team
// account, or, when userID is the team account itself, any DM. It is decided
// off the IDs alone, with no read: a DM's ID is derived from its members, so
// it is a DM with the team exactly when it derives from the team and userID
// under one of the DM types. It says nothing of whether the chat exists or
// userID is in it.
func (e *RuleEvaluator) inTeamDm(chatID *commonpb.ChatId, userID *commonpb.UserId) bool {
	if e.teamUserID == nil || IsGroupChatID(chatID) {
		return false
	}
	if bytes.Equal(userID.GetValue(), e.teamUserID.Value) {
		return true
	}
	return DeriveDmChatType(chatID, []*commonpb.UserId{e.teamUserID, userID}) != chatpb.ChatType_UNKNOWN
}

// teamDmRules are the rules of a DM with the Flipcash team account (see
// RuleEvaluator): a Never speaker rule, which no one satisfies, the team
// included. A client shown it leaves the composer out.
func teamDmRules() *chatpb.Rules {
	return &chatpb.Rules{
		Speaker: []*chatpb.SpeakerRules{{
			Kind: &chatpb.SpeakerRules_Never{Never: &chatpb.Never{}},
		}},
	}
}

// satisfies evaluates one rule, of either class, for userID. The two classes
// share a vocabulary of requirement kinds, so a single switch covers both.
// rules are the rules the kind was taken from, whose creator a
// CreatorRequirement is answered by.
//
// A kind the evaluator does not know how to answer is an error, not a pass:
// a rule is a restriction, and the safe failure for a restriction the server
// cannot evaluate is to admit no one. The Server projects such an error as an
// Internal failure, never as an OK.
func (e *RuleEvaluator) satisfies(ctx context.Context, rules GroupRules, kind any, userID *commonpb.UserId) (bool, error) {
	switch k := kind.(type) {
	case *chatpb.ListenerRules_Staff, *chatpb.SpeakerRules_Staff:
		return e.accounts.IsStaff(ctx, userID)
	case *chatpb.ListenerRules_MinimumBalance:
		return e.satisfiesMinimumBalance(ctx, k.MinimumBalance, userID)
	case *chatpb.SpeakerRules_MinimumBalance:
		return e.satisfiesMinimumBalance(ctx, k.MinimumBalance, userID)
	case *chatpb.SpeakerRules_Never:
		// No one satisfies it. Only a DM with the Flipcash team account
		// carries one, never from its record (see teamDmRules), and this is
		// what refuses every send in one.
		return false, nil
	case *chatpb.SpeakerRules_Creator:
		// Only a group written as creator-only carries the rule (see
		// Chat.IsCreatorOnlySpeaker), and one with no recorded creator admits
		// no one. Membership is the caller's check, so a creator who has left
		// speaks no more than any other non-member.
		return rules.isCreator(userID), nil
	default:
		return false, fmt.Errorf("unsupported chat rule %T", k)
	}
}

// satisfiesMinimumBalance reports whether userID holds at least the required
// amount, in the required mints, right now.
//
// A USD requirement is answered in quarks: the balance client's core mint
// total is USDF, a USD stablecoin, so the requirement compares directly
// against it with no exchange rate in between, the requirement rounded to the
// nearest quark, so a balance that is exactly the requirement satisfies it
// whatever the float arithmetic on the way in. The path asks OCP for no
// valuation, so a USD group is unaffected by what OCP can price.
//
// Any other currency is answered by OCP's valuation of the same holdings in
// that currency (see balance.Client.GetTotalFiatValue). The value is a quoted
// rate applied to a quark total, so it can land a fraction of a minor unit
// under a requirement it meets through rounding alone; half of the currency's
// smallest transferable unit of slack is allowed, as intent validation allows
// on a rate-derived payment. A currency OCP cannot value is, like an unknown
// rule kind, an error and never a pass.
//
// A user with no owner account holds nothing, and fails as a zero balance would;
// a balance that cannot be read is an error, so the gate is never left
// unenforced.
func (e *RuleEvaluator) satisfiesMinimumBalance(ctx context.Context, req *chatpb.MinimumBalanceRequirement, userID *commonpb.UserId) (bool, error) {
	currency := currency_lib.Code(req.GetAmount().GetCurrency())
	if currency == currency_lib.USD {
		required := uint64(math.Round(req.GetAmount().GetNativeAmount() * float64(ocp_common.CoreMintQuarksPerUnit)))

		held, err := e.balances.GetTotalUsdfBalance(ctx, userID, req.GetMints()...)
		if errors.Is(err, balance.ErrNotFound) {
			held = 0
		} else if err != nil {
			return false, err
		}
		return held >= required, nil
	}

	held, err := e.balances.GetTotalFiatValue(ctx, userID, currency, req.GetMints()...)
	if errors.Is(err, balance.ErrNotFound) {
		held = 0
	} else if err != nil {
		return false, err
	}
	tolerance := 0.5 * minimumTransferValue(currency)
	return held >= req.GetAmount().GetNativeAmount()-tolerance, nil
}
