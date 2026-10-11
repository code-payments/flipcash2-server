package poll

import (
	"context"
	"errors"
	"time"

	"google.golang.org/protobuf/proto"

	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"
	messagingpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/messaging/v1"
	pollpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/poll/v1"
)

// ErrNotSendable is returned by PrepareSend for a poll the sender may not send
// in the chat: it does not exist, it is someone else's, it belongs to another
// chat, it is no longer open, or it has been sent already.
var ErrNotSendable = errors.New("poll cannot be sent")

// PrepareSend readies pollID to be sent by senderID in chatID at now, as a
// messagingpb.PollWidget in plain WidgetContent in the message with
// clientMessageID, which only a group that is not private takes. The poll must be the sender's
// and created for this chat, or ErrNotSendable is returned.
//
// A poll is sent at most once: the send that makes it live is the only one.
// An unsent poll that is still open goes live as sent by this message (see
// Store.MarkLive), so the caller sends the message only after this returns:
// everyone who sees the message can then see the poll and vote. Any other
// send of a live poll is ErrNotSendable, except a retry of the one that made
// it live (the same clientMessageID) within SendRetryWindow of the poll's
// creation, which passes whether or not the poll is still open: messaging answers it
// with the original message, or, if the first attempt failed after the poll
// went live, writes the one message that carries it. Of sends racing to make
// a poll live, the one whose MarkLive lands goes through and the rest are
// refused.
//
// That is what keeps a poll to one message: messaging writes at most one
// message per client message ID in a chat, and the poll accepts one client
// message ID. Its message, once deleted, is not replaced. A private group's
// poll is sent inside encrypted content the server never reads, so it never
// reaches this, and nothing limits how often it is sent; it is live from
// creation.
func PrepareSend(ctx context.Context, polls Store, chatID *commonpb.ChatId, senderID *commonpb.UserId, pollID *pollpb.PollId, clientMessageID *messagingpb.ClientMessageId, now time.Time) error {
	p, err := polls.GetPoll(ctx, pollID)
	if errors.Is(err, ErrPollNotFound) {
		return ErrNotSendable
	}
	if err != nil {
		return err
	}

	if !proto.Equal(p.CreatorID, senderID) || !proto.Equal(p.ChatID, chatID) {
		return ErrNotSendable
	}

	if p.Live {
		if p.ClientMessageID != nil && proto.Equal(p.ClientMessageID, clientMessageID) && now.Before(p.CreatedAt.Add(SendRetryWindow)) {
			return nil
		}
		return ErrNotSendable
	}

	// Open, judged as for a vote but before the poll is live: an unsent poll
	// is open until its end, and sending it is what makes it live.
	if p.StatusAt(now) != StatusOpen {
		return ErrNotSendable
	}
	_, err = polls.MarkLive(ctx, pollID, clientMessageID, now)
	switch {
	case errors.Is(err, ErrPollNotFound):
		// It ended unsent since the read above, and so is gone.
		return ErrNotSendable
	case errors.Is(err, ErrPollAlreadyLive):
		// Another send made it live since the read above.
		return ErrNotSendable
	}
	return err
}
