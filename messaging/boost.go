package messaging

import (
	"bytes"
	"context"
	"encoding/hex"
	"fmt"
	"math"

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
// is divided between the message's sender and the group's creator. This file
// is the rule for that division; the record of one boost is Boost (model.go)
// and what it does to the reaction state is the Store's (AddBoost).
//
// The division is fixed, not the payer's to choose (see BoostPayees):
//
//   - No one boosts their own message (ErrBoostOwnMessage), the creator
//     included.
//   - A boost of another member's message by anyone but the creator pays the
//     sender and the creator: the creator CreatorBoostShareBps (20%) of the
//     whole, the sender the rest (80%).
//   - A boost of the creator's own message pays the creator the whole
//     payment: they are both payees, and are paid once.
//   - A boost by the creator pays the sender the whole payment: the
//     creator's part would be paid to themself, so it is not taken out.
//
// So a boost is one payment to one person, the whole amount, unless it pays
// two, and only then is anything divided. Either way the reaction is credited
// with the whole payment.
//
// The shares are judged on fiat, each payee's amount in the payment's
// currency, never on quarks. What a payee is owed is a fiat amount, and the
// quarks that are worth it are the payment's to prove: OCP checks every
// payment's quarks against its own amount (what they sell for, for a currency
// on a bonding curve) to within half the currency's smallest unit, so a payee
// is paid quarks worth their share. A rule on quarks could not say the same:
// quarks on a bonding curve are not proportional to value, so an exact 80/20
// of the quarks is not an exact 80/20 of what they sell for, and the quarks
// worth each share are not an exact 80/20 of each other. The fiat shares are
// held to the caller's tolerance, half the currency's smallest unit at
// validation, so a client rounds each share to that unit (e.g. a cent).

// CreatorBoostShareBps is the group creator's share of a boost that pays
// both the message's sender and the creator, in basis points of the whole
// payment. The sender is paid the rest.
const CreatorBoostShareBps uint64 = 2000

var (
	// ErrBoostNoPayer is returned when no payer is named: the division is
	// decided against who pays, so a boost with no payer cannot be judged.
	ErrBoostNoPayer = errors.New("boost requires a payer")

	// ErrBoostNoCreator is returned when the group has no recorded creator:
	// the creator is paid on every boost they do not make, so a group with
	// none cannot take a boost.
	ErrBoostNoCreator = errors.New("boost requires the group's creator")

	// ErrBoostOwnMessage is returned when the payer sent the message: no one
	// boosts their own message.
	ErrBoostOwnMessage = errors.New("boost of one's own message")

	// ErrBoostNoPayee is returned when the payment would pay no one but the
	// payer: the creator boosting a message with no sender. A message with
	// no sender cannot be reacted to, so no validated boost reaches it.
	ErrBoostNoPayee = errors.New("boost would pay no one but the payer")

	// ErrBoostPayoutMismatch is returned when the payouts do not name exactly
	// the people the boost must pay (see BoostPayees): one is missing, one
	// is a stranger to the boost, or one is named twice.
	ErrBoostPayoutMismatch = errors.New("boost payouts do not match the payees")

	// ErrBoostPayoutEmpty is returned when a payout's amount is not a
	// positive, finite number: every payee is paid something.
	ErrBoostPayoutEmpty = errors.New("boost payout is empty")

	// ErrBoostShareMismatch is returned when two people are paid and either's
	// amount is further than the tolerance from their share of the whole.
	ErrBoostShareMismatch = errors.New("boost shares do not match the split")
)

// BoostPayout is one person's part of a boost, as the payment pays them: the
// fiat amount, in the payment's currency, whose worth in quarks the payment
// carries to them.
type BoostPayout struct {
	UserID *commonpb.UserId
	Amount float64
}

// BoostShare is one person a boost must pay and their share of the whole, in
// basis points.
type BoostShare struct {
	UserID *commonpb.UserId
	Bps    uint64
}

// BoostPayees is who a boost by payer must pay, given the group's creator and
// the message's sender, by the rule above: the sender at 80% then the creator
// at 20% when both are paid, or the one payee at the whole. sender is nil for
// a message with none, which then pays the creator alone. It returns
// ErrBoostNoPayer when payer is nil (a nil payer would match no one and pay
// everyone), ErrBoostNoCreator when creator is nil, ErrBoostOwnMessage when
// payer is the sender, and ErrBoostNoPayee when no one is left to pay. The
// user IDs returned are the caller's own values, not copies.
func BoostPayees(payer, creator, sender *commonpb.UserId) ([]BoostShare, error) {
	if payer == nil {
		return nil, ErrBoostNoPayer
	}
	if creator == nil {
		return nil, ErrBoostNoCreator
	}
	if sameUser(sender, payer) {
		return nil, ErrBoostOwnMessage
	}

	switch {
	case sender == nil || sameUser(sender, creator):
		if sameUser(creator, payer) {
			return nil, ErrBoostNoPayee
		}
		return []BoostShare{{UserID: creator, Bps: 10000}}, nil
	case sameUser(creator, payer):
		return []BoostShare{{UserID: sender, Bps: 10000}}, nil
	}

	return []BoostShare{
		{UserID: sender, Bps: 10000 - CreatorBoostShareBps},
		{UserID: creator, Bps: CreatorBoostShareBps},
	}, nil
}

// ValidateBoostPayouts checks a boost's payouts against the rule above for a
// boost by payer on a message sent by sender in a group created by creator,
// and returns the payment's total, the sum of the payouts' amounts. The
// payouts must name exactly the people BoostPayees names, in any order, each
// paid a positive amount, and when two are paid, each amount must be within
// tolerance of their share of the total (a little float error forgiven).
// Every failure wraps one of the ErrBoost* sentinels.
func ValidateBoostPayouts(payer, creator, sender *commonpb.UserId, payouts []BoostPayout, tolerance float64) (total float64, err error) {
	payees, err := BoostPayees(payer, creator, sender)
	if err != nil {
		return 0, err
	}

	for _, payout := range payouts {
		if !(payout.Amount > 0) || math.IsInf(payout.Amount, 0) {
			return 0, fmt.Errorf("%w: amount %v", ErrBoostPayoutEmpty, payout.Amount)
		}
		total += payout.Amount
	}

	if len(payouts) != len(payees) {
		return 0, fmt.Errorf("%w: %d payouts for %d payees", ErrBoostPayoutMismatch, len(payouts), len(payees))
	}
	// Who is paid is judged before how much, so a payout to the wrong person
	// is always a mismatch, never a share that looks wrong.
	shares := make([]uint64, len(payouts))
	matched := make([]bool, len(payees))
	for j, payout := range payouts {
		found := false
		for i, payee := range payees {
			if !matched[i] && sameUser(payout.UserID, payee.UserID) {
				matched[i], found = true, true
				shares[j] = payee.Bps
				break
			}
		}
		if !found {
			return 0, fmt.Errorf("%w: payout to someone the boost does not pay, or paid twice", ErrBoostPayoutMismatch)
		}
	}

	// One payee is paid the total by construction. Two are each held to
	// their share of it.
	if len(payees) > 1 {
		// A share and the amount it is compared with are each a product or
		// sum of a few floats, so they can differ by a few ulps of the total
		// even when they are equal.
		slack := tolerance + 4*(math.Nextafter(total, math.Inf(1))-total)
		for j, payout := range payouts {
			expected := total * float64(shares[j]) / 10000
			if math.Abs(payout.Amount-expected) > slack {
				return 0, fmt.Errorf("%w: paid %v of %v, %v required (within %v)", ErrBoostShareMismatch, payout.Amount, total, expected, tolerance)
			}
		}
	}

	return total, nil
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
