package messaging

import (
	"bytes"
	"context"
	"encoding/hex"
	"fmt"
	"math/bits"

	"github.com/pkg/errors"
	"go.uber.org/zap"
	"google.golang.org/protobuf/types/known/timestamppb"

	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"
	eventpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/event/v1"
	messagingpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/messaging/v1"

	"github.com/code-payments/flipcash2-server/model"
)

// A boost is a payment made to put money behind a reaction: the payer's
// reaction with an emoji on a group message carries the payment's value, the
// emoji's total grows by it, and the emoji is listed ahead of ones with less
// money behind it (see ReactionLess and Reactor.BoostUsdMicros). The payment
// is divided between the group's creator and the message's sender. This file
// is the rule for that division; the record of one boost is Boost (model.go)
// and what it does to the reaction state is the Store's (AddBoost).
//
// The payer chooses the division. The server enforces only who must be paid
// and how little either may get:
//
//   - The creator is always paid, and the sender is paid too unless they are
//     the creator (one person, paid once) or the payer (who keeps that part
//     by not paying it). A payer who is the creator likewise keeps the
//     creator's part, so the sender takes the whole payment. See
//     BoostPayees.
//   - When two people are paid, neither gets less than
//     MinimumBoostShareBps of the whole: a 90/10 split either way is the
//     least uneven division allowed, 95/5 is refused. When one person is
//     paid, they get the whole payment.
//   - A payment that would pay no one, because the payer is both the creator
//     and the sender, cannot boost at all (ErrBoostNoPayee).
//
// The shares are checked on quarks, the unit the payment is made in, and the
// minimum is rounded up: a share is large enough when it is at least a tenth
// of the total, rounded up to a whole quark.

// MinimumBoostShareBps is the least share of a boost either payee may be
// given when two are paid, in basis points of the whole payment.
const MinimumBoostShareBps uint64 = 1000

var (
	// ErrBoostNoPayer is returned when no payer is named: the division is
	// decided against who pays, so a boost with no payer cannot be judged.
	ErrBoostNoPayer = errors.New("boost requires a payer")

	// ErrBoostNoCreator is returned when the group has no recorded creator:
	// the creator is always paid, so a group with none cannot take a boost.
	ErrBoostNoCreator = errors.New("boost requires the group's creator")

	// ErrBoostNoPayee is returned when the payment would pay no one but the
	// payer: they are both the creator and the sender.
	ErrBoostNoPayee = errors.New("boost would pay no one but the payer")

	// ErrBoostPayoutMismatch is returned when the payouts do not name exactly
	// the people the boost must pay (see BoostPayees): one is missing, one
	// is a stranger to the boost, or one is named twice.
	ErrBoostPayoutMismatch = errors.New("boost payouts do not match the payees")

	// ErrBoostPayoutEmpty is returned when a payout carries nothing: every
	// payee is paid something.
	ErrBoostPayoutEmpty = errors.New("boost payout is empty")

	// ErrBoostShareBelowMinimum is returned when two people are paid and
	// one's share is under MinimumBoostShareBps of the whole.
	ErrBoostShareBelowMinimum = errors.New("boost share is below the minimum")

	// ErrBoostTotalOverflow is returned when the payouts sum past what a
	// payment can carry.
	ErrBoostTotalOverflow = errors.New("boost total overflows")
)

// BoostPayout is one person's part of a boost, as the payment pays them.
type BoostPayout struct {
	UserID *commonpb.UserId
	Quarks uint64
}

// BoostPayees is who a boost by payer must pay, given the group's creator
// and the message's sender: the creator, then the sender, each left out when
// they are the payer, and the sender also when they are the creator. sender
// is nil for a message with none, which then pays the creator alone. It
// returns ErrBoostNoPayer when payer is nil (a nil payer would match no one
// and pay everyone), ErrBoostNoCreator when creator is nil, and
// ErrBoostNoPayee when no one is left to pay. payer and the returned payees
// are the caller's own values, not copies.
func BoostPayees(payer, creator, sender *commonpb.UserId) ([]*commonpb.UserId, error) {
	if payer == nil {
		return nil, ErrBoostNoPayer
	}
	if creator == nil {
		return nil, ErrBoostNoCreator
	}
	var payees []*commonpb.UserId
	if !sameUser(creator, payer) {
		payees = append(payees, creator)
	}
	if sender != nil && !sameUser(sender, payer) && !sameUser(sender, creator) {
		payees = append(payees, sender)
	}
	if len(payees) == 0 {
		return nil, ErrBoostNoPayee
	}
	return payees, nil
}

// ValidateBoostPayouts checks a boost's payouts against the rule above for a
// boost by payer on a message sent by sender in a group created by creator,
// and returns the payment's total, the sum of the payouts. The payouts must
// name exactly BoostPayees (in any order), each paid something, and when
// there are two, each at least MinimumBoostShare of the total. Every
// failure wraps one of the ErrBoost* sentinels.
func ValidateBoostPayouts(payer, creator, sender *commonpb.UserId, payouts []BoostPayout) (total uint64, err error) {
	payees, err := BoostPayees(payer, creator, sender)
	if err != nil {
		return 0, err
	}

	if len(payouts) != len(payees) {
		return 0, fmt.Errorf("%w: %d payouts for %d payees", ErrBoostPayoutMismatch, len(payouts), len(payees))
	}
	matched := make([]bool, len(payees))
	for _, payout := range payouts {
		found := false
		for i, payee := range payees {
			if !matched[i] && sameUser(payout.UserID, payee) {
				matched[i], found = true, true
				break
			}
		}
		if !found {
			return 0, fmt.Errorf("%w: payout to someone the boost does not pay, or paid twice", ErrBoostPayoutMismatch)
		}
		if payout.Quarks == 0 {
			return 0, ErrBoostPayoutEmpty
		}
		var carry uint64
		if total, carry = bits.Add64(total, payout.Quarks, 0); carry != 0 {
			return 0, ErrBoostTotalOverflow
		}
	}

	if len(payouts) == 2 {
		minimum := MinimumBoostShare(total)
		for _, payout := range payouts {
			if payout.Quarks < minimum {
				return 0, fmt.Errorf("%w: %d of %d quarks, at least %d required", ErrBoostShareBelowMinimum, payout.Quarks, total, minimum)
			}
		}
	}

	return total, nil
}

// MinimumBoostShare is the least either payee may be given of a boost of
// total quarks when two are paid: MinimumBoostShareBps of the total, rounded
// up to a whole quark, so a share is never under the minimum by a fraction
// the payment cannot express.
func MinimumBoostShare(total uint64) uint64 {
	// total * bps can exceed 64 bits; the high word is under 10000 since bps
	// is, so the division cannot overflow.
	hi, lo := bits.Mul64(total, MinimumBoostShareBps)
	quotient, remainder := bits.Div64(hi, lo, 10000)
	if remainder != 0 {
		quotient++
	}
	return quotient
}

// sameUser reports whether two user IDs name the same user; nil names no one
// and matches nothing.
func sameUser(a, b *commonpb.UserId) bool {
	return a != nil && b != nil && bytes.Equal(a.Value, b.Value)
}

// RecordBoost records a boost (see boost.go) against userID's reaction with
// emoji on a message and tells the chat: the store write (Store.AddBoost,
// which adds the reaction if absent, grows the totals and advances the
// emoji's version) and then one BOOSTED ReactionUpdate on the chat, carrying
// the count, the version, the emoji's new total and the actor's. It is the
// entry point for the task that will record a paid boost (not built yet; it
// belongs in task.Executor beside the cash-message tasks), so it gates on
// nothing: the payer's standing was judged when the payment
// was validated, and the money has moved. What it refuses is malformed
// input, none of which a validated payment produces: a value that is not a
// single emoji (ErrInvalidEmoji, the reaction RPCs' own ValidateEmoji,
// applied here since no RPC guards this path), and from the store a DM
// (ErrBoostGroupOnly), a boost worth nothing (ErrBoostEmpty), or a missing
// intent ID or amount. No push is sent and no message is posted; the boost
// shows on the reaction itself.
//
// Like the store it is idempotent on the payment's intent ID: a replay
// returns the current aggregate with credited false and broadcasts nothing,
// so an at-least-once task never announces a boost twice. The other side of
// that: the broadcast is best effort, like every broadcast here. A process
// that dies after the store write commits and before the update goes out
// leaves a boost no retry will announce, since the retry is the replay, and
// clients see it on their next read of the reaction.
func (s *Sender) RecordBoost(
	ctx context.Context,
	chatID *commonpb.ChatId,
	messageID *messagingpb.MessageId,
	userID *commonpb.UserId,
	emoji string,
	boost BoostInput,
) (reaction *Reaction, credited bool, err error) {
	if err := ValidateEmoji(emoji); err != nil {
		return nil, false, err
	}
	log := s.log.With(
		zap.String("chat_id", hex.EncodeToString(chatID.Value)),
		zap.String("user_id", model.UserIDString(userID)),
		zap.Uint64("message_id", messageID.Value),
	)

	reaction, credited, _, err = s.messages.AddBoost(ctx, chatID, messageID, userID, emoji, boost)
	if err != nil {
		return nil, false, errors.Wrap(err, "add boost")
	}
	if !credited {
		return reaction, false, nil
	}

	s.publishChatUpdate(ctx, log, chatID, &eventpb.ChatUpdate{
		ReactionUpdates: &messagingpb.ReactionUpdateBatch{
			ReactionUpdates: []*messagingpb.ReactionUpdate{{
				MessageId:     &messagingpb.MessageId{Value: messageID.Value},
				Emoji:         &messagingpb.Emoji{Value: emoji},
				Actor:         &commonpb.UserId{Value: append([]byte(nil), userID.Value...)},
				Action:        messagingpb.ReactionUpdate_BOOSTED,
				Count:         reaction.Count,
				Version:       reaction.Version,
				ReactedTs:     timestamppb.New(reaction.Self.ReactedTs),
				BoostUsd:      UsdFromMicros(reaction.BoostUsdMicros),
				ActorBoostUsd: UsdFromMicros(reaction.Self.BoostUsdMicros),
			}},
		},
	}, nil, nil)
	return reaction, true, nil
}
