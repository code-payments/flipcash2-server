package poll

import (
	"context"
	"errors"
	"time"

	"google.golang.org/protobuf/proto"

	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"
	pollpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/poll/v1"
)

// ErrNotSendable is returned by PrepareSend for a poll the sender may not send
// in the chat: it does not exist, it is someone else's, it belongs to another
// chat, or it is no longer open.
var ErrNotSendable = errors.New("poll cannot be sent")

// PrepareSend readies pollID to be sent by senderID in chatID at now, as a
// messagingpb.PollWidget in plain WidgetContent, which only a group that is not
// private takes: the poll must be the sender's, created for this chat, and
// open, or ErrNotSendable is returned. A poll that has not been sent before
// goes live (see Store.MarkLive), so the caller sends the message only after
// this returns: everyone who sees the message can then see the poll and vote.
// If the send fails after that, the poll is live without a message carrying
// it, and the creator's retry sends one.
//
// A creator may send their poll any number of times while it is open, and
// every send after the first changes nothing here. A private group's poll is
// sent inside encrypted content the server never reads, so it never reaches
// this; it is live from creation.
func PrepareSend(ctx context.Context, polls Store, chatID *commonpb.ChatId, senderID *commonpb.UserId, pollID *pollpb.PollId, now time.Time) error {
	p, err := polls.GetPoll(ctx, pollID)
	if errors.Is(err, ErrPollNotFound) {
		return ErrNotSendable
	}
	if err != nil {
		return err
	}

	// Open, judged as for a vote but whether or not it is live yet: an unsent
	// poll is open until its end, and sending it is what makes it live.
	if !proto.Equal(p.CreatorID, senderID) ||
		!proto.Equal(p.ChatID, chatID) ||
		p.StatusAt(now) != StatusOpen {
		return ErrNotSendable
	}

	if p.Live {
		return nil
	}
	_, err = polls.MarkLive(ctx, pollID, now)
	if errors.Is(err, ErrPollNotFound) {
		// It ended unsent since the read above, and so is gone.
		return ErrNotSendable
	}
	return err
}
