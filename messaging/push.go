package messaging

import (
	"bytes"
	"context"
	"sync"
	"time"

	"go.uber.org/zap"

	chatpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/chat/v1"
	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"
	messagingpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/messaging/v1"

	"github.com/code-payments/flipcash2-server/chat"
	"github.com/code-payments/flipcash2-server/model"
	"github.com/code-payments/flipcash2-server/profile"
	"github.com/code-payments/flipcash2-server/push"
)

// The push fan-out for a new message walks the chat's members in pages and
// runs the whole pipeline per page — drop the sender, filter blockers, split
// muted, look up tokens, bump badges, send — so what it holds at once is
// bounded by the page size however large the group, and the first push
// leaves after the first page rather than after the whole roster has been
// read. Pages are read one after another, since a read is a small fraction
// of a page's cost, but sent concurrently: each page's pipeline runs on its
// own goroutine under a slot from the Sender's pool (see
// defaultPushPageConcurrency), which the reader takes before launching the
// page, so a walk holds at most one page more than it has in send, and every
// fan-out the Sender runs shares one bound on the pages in flight.
// Everything that is the same for every recipient (the sender's profile, the
// rendered push, the choice of mute read) is resolved once per message
// before the walk. A DM is a walk of exactly one page: its inline pair, sent
// inline and outside the pool so a large group's walk never delays it. The
// walk's cursor is a user ID (see chat.Store.GetGroupMembersPage), which is
// what a durable worker would checkpoint to resume a fan-out without
// re-sending the pages already out.
const (
	// defaultPushPageSize is how many members a page holds. A member has a
	// device or two, so a page's tokens fill every FCM batch the pusher runs
	// at once (see push.FCMPusher): a smaller page leaves send concurrency
	// idle, a larger one sends no faster and only amortizes the reads between
	// pages, while pushing a page's badge rounds and send closer to
	// pushStepTimeout. A page's messages share their payload, so its memory
	// is a few hundred bytes per token whatever the size.
	defaultPushPageSize = 2000

	// defaultPushPageConcurrency is how many group pages a Sender has in send
	// at once, across every fan-out it runs. A page's send is dominated by
	// its FCM batches, which the pusher already runs several at a time, so
	// this multiplies the FCM batches (and badge increments) a process has in
	// flight: pages × the pusher's batch concurrency × its batch size
	// messages at once. Four keeps that in the low thousands. It is a
	// process-wide bound, not a per-walk one, so concurrent sends into large
	// groups queue for slots rather than each running their own pipelines.
	defaultPushPageConcurrency = 4

	// defaultPushMutedWholeSetCap is the recorded-mute count at or below which
	// a group's active muted set is read whole, once per message, and held as
	// a map for every page: that read is billed by the active mutes it
	// returns, so it is the cheaper shape while few have muted. Above the cap
	// each page reads the mutes between its first and last user instead (see
	// chat.Store.GetMutedCount for the choice, and the two reads it chooses
	// between). The count bounds active mutes from above, so a set read under
	// it is never larger than the cap.
	defaultPushMutedWholeSetCap = 1000

	// pushStepTimeout bounds each step of a fan-out on its own: the per-message
	// setup, each roster page read, and each page's filtering and send. A step
	// that stalls costs its page, not the pages after it.
	pushStepTimeout = 15 * time.Second

	// pushBudget bounds one update's whole fan-out, every message and page,
	// including what a page waits for a slot. A page's send is a few seconds
	// of FCM batches, so with defaultPushPageConcurrency pages in flight the
	// budget covers a group on the order of a hundred thousand members; a
	// walk that runs past it abandons the pages it has not yet launched and
	// logs the cursor it stopped at.
	pushBudget = 1 * time.Minute
)

// pushSentMessages pushes each newly sent message of one update to every
// member who should hear about it. It runs detached from the originating RPC
// and is best-effort throughout: failures are logged, never surfaced, since
// the messages themselves are already delivered on the event stream and a
// failed push costs only the notification. members is a DM's inline pair
// (the one page a DM walks) and ignored for a group, whose roster is paged.
func (s *Sender) pushSentMessages(ctx context.Context, log *zap.Logger, chatID *commonpb.ChatId, members []*commonpb.UserId, sent []*messagingpb.Message) {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), pushBudget)
	defer cancel()

	log = log.With(zap.String("chat_id", model.ChatIDString(chatID)))
	isGroup := chat.IsGroupChatID(chatID)

	// Pushes identify the sender differently per chat type — a contact DM push
	// carries the sender's phone number, which is private in every other chat
	// type. A DM's type is recovered from the members already in hand, since a
	// DM's ID commits to its type via the derivation domain — no store read. A
	// group's type cannot be member-derived, so its stored metadata is read
	// instead, which the push needs anyway for the group's title. That read is
	// the canonical record alone — never the membership, which is paged below.
	var chatType chatpb.ChatType
	var chatTitle string
	if isGroup {
		chatType = chatpb.ChatType_GROUP
		err := pushStep(ctx, func(ctx context.Context) error {
			md, err := s.chats.GetChatByID(ctx, chatID)
			if err != nil {
				return err
			}
			chatTitle = md.Title
			return nil
		})
		if err != nil {
			log.With(zap.Error(err)).Warn("Failure loading chat metadata for message pushes")
			return
		}
	} else {
		chatType = chat.DeriveDmChatType(chatID, members)
	}

	for _, message := range sent {
		if message.SenderId == nil {
			continue
		}
		p := &messagePush{
			sender:  s,
			log:     log.With(zap.Uint64("message_id", message.GetMessageId().GetValue())),
			chatID:  chatID,
			message: message,
			now:     time.Now(),
		}
		if err := pushStep(ctx, func(ctx context.Context) error { return p.prepare(ctx, chatType, chatTitle) }); err != nil {
			p.log.With(zap.Error(err)).Warn("Failure preparing message push")
			continue
		}
		if p.push == nil {
			continue
		}
		if isGroup {
			p.walkGroup(ctx)
		} else {
			p.sendPage(ctx, members)
		}
	}
}

// pushStep runs one step of a fan-out under its own deadline.
func pushStep(ctx context.Context, step func(ctx context.Context) error) error {
	ctx, cancel := context.WithTimeout(ctx, pushStepTimeout)
	defer cancel()
	return step(ctx)
}

// mutedShape is how a messagePush answers which recipients have the chat
// muted (see defaultPushMutedWholeSetCap).
type mutedShape int

const (
	// mutedUnknown: the lookup failed, so every recipient is treated as
	// unmuted. The mute lookup fails open where the blocklist lookup fails
	// closed, because the worst outcome is one notification a client's own
	// copy of its mute may still catch, while a suppressed push would lose
	// the message delivery the flag exists to preserve.
	mutedUnknown mutedShape = iota
	// mutedWhole: the chat's active muted set was read once and is held in
	// messagePush.muted.
	mutedWhole
	// mutedPerPage: each page reads the mutes in its own key range.
	mutedPerPage
)

// messagePush is one message's fan-out: what is resolved once per message,
// and the per-page pipeline that consumes it.
type messagePush struct {
	sender *Sender
	log    *zap.Logger

	chatID  *commonpb.ChatId
	message *messagingpb.Message

	// push is the rendered notification, nil when the message earns none.
	push *push.ChatMessagePush

	// now is when the fan-out began; a mute is active or lapsed against it
	// for every page, so a timed mute lapsing mid-walk cannot split a page.
	now time.Time

	shape mutedShape
	muted map[string]struct{}
}

// prepare resolves what every page shares: the sender's profile, the
// rendered push, and how mutes will be read. A sender without a profile, or
// without what the chat type's title needs, earns no push and is not an
// error.
func (p *messagePush) prepare(ctx context.Context, chatType chatpb.ChatType, chatTitle string) error {
	senderProfile, err := p.sender.profiles.GetProfile(ctx, p.message.SenderId, true)
	if err == profile.ErrNotFound {
		return nil
	} else if err != nil {
		return err
	}

	switch chatType {
	case chatpb.ChatType_CONTACT_DM:
		if senderProfile.PhoneNumber == nil {
			return nil
		}
		p.push, err = push.BuildContactDmPush(ctx, p.sender.ocpData, p.chatID, p.message, p.message.SenderId, senderProfile.PhoneNumber)
	case chatpb.ChatType_TIP_DM:
		if senderProfile.DisplayName == "" {
			return nil
		}
		p.push, err = push.BuildTipDmPush(ctx, p.sender.ocpData, p.chatID, p.message, p.message.SenderId, senderProfile.DisplayName)
	case chatpb.ChatType_GROUP:
		if senderProfile.DisplayName == "" {
			return nil
		}
		p.push, err = push.BuildGroupChatPush(ctx, p.sender.ocpData, p.chatID, p.message, p.message.SenderId, senderProfile.DisplayName, chatTitle)
	default:
		return nil
	}
	if err != nil || p.push == nil {
		return err
	}

	p.chooseMutedShape(ctx)
	return nil
}

// chooseMutedShape picks how the pages will learn who has muted the chat. A
// DM's set is at most its pair, so it is read whole without asking the
// count. A lookup failure at any point falls back to mutedUnknown.
func (p *messagePush) chooseMutedShape(ctx context.Context) {
	if chat.IsGroupChatID(p.chatID) {
		count, err := p.sender.chats.GetMutedCount(ctx, p.chatID)
		if err != nil {
			p.log.With(zap.Error(err)).Warn("Failure reading muted count for message push; sending all as unmuted")
			return
		}
		if count > p.sender.pushMutedWholeSetCap {
			p.shape = mutedPerPage
			return
		}
	}

	users, err := p.sender.chats.GetMutedUsers(ctx, p.chatID, p.now, 0)
	if err != nil {
		p.log.With(zap.Error(err)).Warn("Failure reading muted users for message push; sending all as unmuted")
		return
	}
	p.shape = mutedWhole
	p.muted = userSet(users)
}

// walkGroup pages the group's roster and sends each page on its own
// goroutine under a slot from the Sender's pool, taken before the page is
// launched and before the next is read. It returns once every page it
// launched has finished, so the caller's context outlives the sends. A
// failed page read, or a budget that runs out while waiting for a slot, ends
// the walk: the cursor to continue from is what failed to arrive, or the
// page that never launched.
func (p *messagePush) walkGroup(ctx context.Context) {
	var wg sync.WaitGroup
	defer wg.Wait()

	var after *commonpb.UserId
	for {
		var page chat.MembersPage
		err := pushStep(ctx, func(ctx context.Context) error {
			var err error
			page, err = p.sender.chats.GetGroupMembersPage(ctx, p.chatID, after, p.sender.pushPageSize)
			return err
		})
		if err != nil {
			p.log.With(zap.Error(err)).Warn("Failure loading members page for message pushes; abandoning remaining pages")
			return
		}
		if len(page.Users) > 0 {
			select {
			case p.sender.pushPageSlots <- struct{}{}:
			case <-ctx.Done():
				p.log.With(zap.Error(ctx.Err()), zap.String("page_first_user", model.UserIDString(page.Users[0]))).Warn("Push budget exhausted waiting for a page slot; abandoning remaining pages")
				return
			}
			wg.Add(1)
			go func(users []*commonpb.UserId) {
				defer wg.Done()
				defer func() { <-p.sender.pushPageSlots }()
				p.sendPage(ctx, users)
			}(page.Users)
		}
		if page.Next == nil {
			return
		}
		after = page.Next
	}
}

// sendPage runs the per-page pipeline over one page of members, under the
// page's own deadline. Its failures are the page's alone: the walk continues
// with the next page whatever happened to this one.
func (p *messagePush) sendPage(ctx context.Context, members []*commonpb.UserId) {
	_ = pushStep(ctx, func(ctx context.Context) error {
		p.sendPageStep(ctx, members)
		return nil
	})
}

func (p *messagePush) sendPageStep(ctx context.Context, members []*commonpb.UserId) {
	// Push recipients are every member but the sender, minus anyone who has
	// blocked the sender — a user who blocks another must stop receiving
	// pushes for that user's messages. The check is scoped to each
	// recipient's own blocklist (recipient is the owner, sender the blocked
	// candidate), and the page's recipients are resolved in one batched read.
	candidates := make([]*commonpb.UserId, 0, len(members))
	for _, member := range members {
		if bytes.Equal(member.Value, p.message.SenderId.Value) {
			continue
		}
		candidates = append(candidates, member)
	}
	// The sender was the page's only member: nothing to push, and nothing to
	// ask the blocklist about.
	if len(candidates) == 0 {
		return
	}

	// Fails closed: on a lookup failure the page's push is suppressed rather
	// than risk notifying a recipient who has blocked the sender. One batched
	// read has no partial outcome to salvage, so that suppresses the push for
	// every recipient on the page rather than just the one that failed — the
	// message itself is still delivered on the event stream, so a transient
	// blocklist error costs only the notification, never the message, and
	// only for this page.
	blockingSender, err := p.sender.blocklists.GetBlockers(ctx, p.message.SenderId, candidates)
	if err != nil {
		p.log.With(zap.Error(err)).Warn("Failure checking blocklists for message push; suppressing push for page")
		return
	}
	recipients := make([]*commonpb.UserId, 0, len(candidates))
	for _, candidate := range candidates {
		if blockingSender[string(candidate.Value)] {
			continue
		}
		recipients = append(recipients, candidate)
	}
	// Every recipient on the page has blocked the sender: nothing to push.
	if len(recipients) == 0 {
		return
	}

	// A recipient who has the chat muted gets the push in a batch of its own,
	// flagged and sent without moving their badge, and only to their Android
	// devices — the pusher drops a muted push's iOS tokens, so an iOS user
	// hears nothing from a muted chat (see push.ChatRecipients).
	split, err := p.splitMuted(ctx, recipients)
	if err != nil {
		p.log.With(zap.Error(err)).Warn("Failure reading muted users for message push; sending page as unmuted")
		split = push.ChatRecipients{Unmuted: recipients}
	}

	if err := p.push.Send(ctx, p.sender.pusher, p.sender.badges, split); err != nil {
		p.log.With(zap.Error(err)).Warn("Failure sending message push")
	}
}

// splitMuted divides one page's recipients by whether they have the chat
// muted, by the shape chosen at prepare. The per-page shape reads the mutes
// between the page's least and greatest user — every record in that range,
// including users who have since left the chat, which the intersection with
// the page drops.
func (p *messagePush) splitMuted(ctx context.Context, recipients []*commonpb.UserId) (push.ChatRecipients, error) {
	var muted map[string]struct{}
	switch p.shape {
	case mutedWhole:
		muted = p.muted
	case mutedPerPage:
		lo, hi := recipients[0], recipients[0]
		for _, r := range recipients[1:] {
			if bytes.Compare(r.Value, lo.Value) < 0 {
				lo = r
			}
			if bytes.Compare(r.Value, hi.Value) > 0 {
				hi = r
			}
		}
		users, err := p.sender.chats.GetMutedUsersPage(ctx, p.chatID, p.now, lo, hi)
		if err != nil {
			return push.ChatRecipients{}, err
		}
		muted = userSet(users)
	default:
		return push.ChatRecipients{Unmuted: recipients}, nil
	}

	if len(muted) == 0 {
		return push.ChatRecipients{Unmuted: recipients}, nil
	}
	var split push.ChatRecipients
	for _, r := range recipients {
		if _, ok := muted[string(r.Value)]; ok {
			split.Muted = append(split.Muted, r)
		} else {
			split.Unmuted = append(split.Unmuted, r)
		}
	}
	return split, nil
}

func userSet(users []*commonpb.UserId) map[string]struct{} {
	set := make(map[string]struct{}, len(users))
	for _, u := range users {
		set[string(u.Value)] = struct{}{}
	}
	return set
}
