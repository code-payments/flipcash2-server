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
// is divided between the message's sender and the group's creator. This file
// is the rule for that division; the record of one boost is Boost (model.go)
// and what it does to the reaction state is the Store's (AddBoost).
//
// The division is fixed, not the payer's to choose (see BoostSplit):
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
// with the whole payment. The shares are on quarks, the unit the payment is
// made in. BoostSplit gives the creator CreatorBoostShareBps of the total
// rounded down to a whole quark and the sender the remainder, but a payment
// need not round the same way: its creator share passes when it is within
// BoostShareToleranceQuarks of the exact 20%, and since the sender takes the
// rest of the same total, theirs is then as close to the exact 80%.

// CreatorBoostShareBps is the group creator's share of a boost that pays
// both the message's sender and the creator, in basis points of the whole
// payment. The sender is paid the rest.
const CreatorBoostShareBps uint64 = 2000

// BoostShareToleranceQuarks is how far a payment's creator share may be from
// the exact CreatorBoostShareBps of its total, in quarks, either way, so a
// client that rounds the share differently is not refused.
const BoostShareToleranceQuarks uint64 = 1

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
	// the people the boost must pay (see BoostSplit): one is missing, one
	// is a stranger to the boost, or one is named twice.
	ErrBoostPayoutMismatch = errors.New("boost payouts do not match the payees")

	// ErrBoostPayoutEmpty is returned when a payout carries nothing, or the
	// boost is too small for a split to give every payee something (under 5
	// quarks): every payee is paid something.
	ErrBoostPayoutEmpty = errors.New("boost payout is empty")

	// ErrBoostShareMismatch is returned when two people are paid and the
	// creator's share is more than BoostShareToleranceQuarks from the exact
	// split.
	ErrBoostShareMismatch = errors.New("boost shares do not match the split")

	// ErrBoostTotalOverflow is returned when the payouts sum past what a
	// payment can carry.
	ErrBoostTotalOverflow = errors.New("boost total overflows")
)

// BoostPayout is one person's part of a boost, as the payment pays them.
type BoostPayout struct {
	UserID *commonpb.UserId
	Quarks uint64
}

// BoostSplit is what a boost of total quarks by payer must pay, given the
// group's creator and the message's sender, by the rule above: the sender
// then the creator when both are paid, or the one payee and the whole total.
// sender is nil for a message with none, which then pays the creator alone.
// It returns ErrBoostNoPayer when payer is nil (a nil payer would match no
// one and pay everyone), ErrBoostNoCreator when creator is nil,
// ErrBoostOwnMessage when payer is the sender, ErrBoostNoPayee when no one is
// left to pay, and ErrBoostPayoutEmpty when the total is too small for a
// split to pay both. The user IDs returned are the caller's own values, not
// copies.
func BoostSplit(payer, creator, sender *commonpb.UserId, total uint64) ([]BoostPayout, error) {
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
		return []BoostPayout{{UserID: creator, Quarks: total}}, nil
	case sameUser(creator, payer):
		return []BoostPayout{{UserID: sender, Quarks: total}}, nil
	}

	creatorShare := CreatorBoostShare(total)
	if creatorShare == 0 {
		return nil, fmt.Errorf("%w: %d quarks cannot be split", ErrBoostPayoutEmpty, total)
	}
	return []BoostPayout{
		{UserID: sender, Quarks: total - creatorShare},
		{UserID: creator, Quarks: creatorShare},
	}, nil
}

// ValidateBoostPayouts checks a boost's payouts against the rule above for a
// boost by payer on a message sent by sender in a group created by creator,
// and returns the payment's total, the sum of the payouts. The payouts must
// name exactly the people BoostSplit of that total pays, in any order, each
// paid something, and when two are paid, the creator's share must be within
// BoostShareToleranceQuarks of the exact CreatorBoostShareBps of the total
// (the sender's is the rest, so it needs no check of its own). Every failure
// wraps one of the ErrBoost* sentinels.
func ValidateBoostPayouts(payer, creator, sender *commonpb.UserId, payouts []BoostPayout) (total uint64, err error) {
	for _, payout := range payouts {
		if payout.Quarks == 0 {
			return 0, ErrBoostPayoutEmpty
		}
		var carry uint64
		if total, carry = bits.Add64(total, payout.Quarks, 0); carry != 0 {
			return 0, ErrBoostTotalOverflow
		}
	}

	split, err := BoostSplit(payer, creator, sender, total)
	if err != nil {
		return 0, err
	}

	if len(payouts) != len(split) {
		return 0, fmt.Errorf("%w: %d payouts for %d payees", ErrBoostPayoutMismatch, len(payouts), len(split))
	}
	// Who is paid is judged before how much, so a payout to the wrong person
	// is always a mismatch, never a share that looks wrong.
	matched := make([]bool, len(split))
	for _, payout := range payouts {
		found := false
		for i, share := range split {
			if !matched[i] && sameUser(payout.UserID, share.UserID) {
				matched[i], found = true, true
				break
			}
		}
		if !found {
			return 0, fmt.Errorf("%w: payout to someone the boost does not pay, or paid twice", ErrBoostPayoutMismatch)
		}
	}

	// One payee is paid the total by construction. Of two, the creator's
	// share is checked, and the sender's is the rest of the same total.
	if len(split) == 2 {
		for _, payout := range payouts {
			if sameUser(payout.UserID, creator) && !nearCreatorBoostShare(payout.Quarks, total) {
				return 0, fmt.Errorf("%w: creator paid %d of %d quarks, %d required (within %d)", ErrBoostShareMismatch, payout.Quarks, total, CreatorBoostShare(total), BoostShareToleranceQuarks)
			}
		}
	}

	return total, nil
}

// CreatorBoostShare is the group creator's share of a boost of total quarks
// that pays both the sender and the creator: CreatorBoostShareBps of the
// total, rounded down to a whole quark. The sender is paid the rest.
func CreatorBoostShare(total uint64) uint64 {
	// total * bps can exceed 64 bits; the high word is under 10000 since bps
	// is, so the division cannot overflow.
	hi, lo := bits.Mul64(total, CreatorBoostShareBps)
	quotient, _ := bits.Div64(hi, lo, 10000)
	return quotient
}

// nearCreatorBoostShare reports whether quarks is within
// BoostShareToleranceQuarks of the exact CreatorBoostShareBps of total, a
// fraction of a quark that CreatorBoostShare rounds down. It compares
// quarks*10000 with total*bps in 128 bits, so nothing is rounded or
// overflows.
func nearCreatorBoostShare(quarks, total uint64) bool {
	aHi, aLo := bits.Mul64(quarks, 10000)
	bHi, bLo := bits.Mul64(total, CreatorBoostShareBps)
	if aHi < bHi || (aHi == bHi && aLo < bLo) {
		aHi, aLo, bHi, bLo = bHi, bLo, aHi, aLo
	}
	diffLo, borrow := bits.Sub64(aLo, bLo, 0)
	diffHi, _ := bits.Sub64(aHi, bHi, borrow)
	limitHi, limitLo := bits.Mul64(BoostShareToleranceQuarks, 10000)
	return diffHi < limitHi || (diffHi == limitHi && diffLo <= limitLo)
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
