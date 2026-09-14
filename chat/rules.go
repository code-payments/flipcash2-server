package chat

import (
	"context"
	"fmt"

	chatpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/chat/v1"
	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"

	"github.com/code-payments/flipcash2-server/account"
)

// Rules projects the chat's stored participation requirements onto a
// chatpb.Rules, or nil when the chat has none — which is every DM, and every
// group not created with a requirement. The proto is the single vocabulary for
// rules: what the client is shown (Metadata.rules) is exactly what the server
// evaluates (RuleEvaluator), so the two can never disagree about what a chat
// requires.
//
// A staff-only group is one listener rule, StaffRequirement: listener rules
// gate reading and joining, and a member must be able to listen before they
// can speak, so restricting the audience to staff restricts the speakers too
// without repeating the rule in the speaker class.
func (c *Chat) Rules() *chatpb.Rules {
	if c.Type != chatpb.ChatType_GROUP || !c.IsStaffOnly {
		return nil
	}
	return &chatpb.Rules{
		Listener: []*chatpb.ListenerRules{{
			Kind: &chatpb.ListenerRules_Staff{Staff: &chatpb.StaffRequirement{}},
		}},
	}
}

// RuleEvaluator decides whether a user satisfies a chat's participation rules
// (see Chat.Rules). It evaluates rules only: membership is a separate, cheaper
// check the caller makes first, so a chat's rules are never evaluated — and
// its requirement never probed — on behalf of a non-member. Only a group can
// carry rules; a DM's evaluation never touches the store.
//
// Rules are read through Store.GetGroupRules — in production the caching
// store, which holds every group's rules after its first read — and a
// StaffRequirement is answered by the account store's staff flag.
//
// Rules are evaluated against the current state of their subject, not the
// state at join time: a member who no longer satisfies a listener rule (a
// staff member whose flag was revoked, say) keeps their membership record but
// is denied on every gated path until they satisfy it again. Enforcing rules on
// membership changes — a join gated by listener rules — is the job of whatever
// path mutates membership.
type RuleEvaluator struct {
	accounts account.Store
	chats    Store
}

func NewRuleEvaluator(accounts account.Store, chats Store) *RuleEvaluator {
	return &RuleEvaluator{accounts: accounts, chats: chats}
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

// CanSpeak reports whether userID satisfies every listener and speaker rule of
// chatID — the requirements to send messages in the chat. Speaker rules apply
// on top of listener rules: a user who cannot listen cannot speak, whatever the
// speaker rules say. It returns ErrChatNotFound if a group chat does not exist.
func (e *RuleEvaluator) CanSpeak(ctx context.Context, chatID *commonpb.ChatId, userID *commonpb.UserId) (bool, error) {
	rules, err := e.rulesFor(ctx, chatID)
	if err != nil {
		return false, err
	}
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

func (e *RuleEvaluator) satisfiesListener(ctx context.Context, rules *chatpb.Rules, userID *commonpb.UserId) (bool, error) {
	for _, rule := range rules.GetListener() {
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
	default:
		return false, fmt.Errorf("unsupported chat rule %T", k)
	}
}
