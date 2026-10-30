package messaging

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"time"

	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"
	messagingpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/messaging/v1"

	"github.com/code-payments/flipcash2-server/database"
)

// ErrMessageNotFound indicates that no message exists for the given chat and
// message ID.
var ErrMessageNotFound = errors.New("message not found")

// ErrEventSequenceConflict indicates an optimistic-concurrency failure on a
// message mutation (edit/delete): the message's current event_sequence no longer
// matches the expected value the caller supplied, so the mutation was rejected
// rather than clobbering newer state. The store returns the message's current
// state alongside this error so the caller can surface it.
var ErrEventSequenceConflict = errors.New("message event sequence conflict")

// ErrPartialReplay is returned by Store.PutMessages when some of the batch's
// client message IDs name messages that already exist and others do not. A
// batch lands whole or not at all, so a replay of it finds every marker; a mix
// means the IDs were spent by other sends (a single, or a differently shaped
// batch), and the store neither persists the rest — that would break the
// batch's atomicity — nor guesses which existing messages to hand back. It
// catches a mix only: a batch whose every ID was spent, by one earlier batch
// or by several sends, is a replay (see Store.PutMessages).
var ErrPartialReplay = errors.New("batch client message ids partly reused")

// ErrEmptyBatch, ErrBatchTooLarge, ErrMissingClientMessageID and
// ErrDuplicateClientMessageID are Store.PutMessages' shape errors (see ValidateMessageInputs). Every store
// answers them before touching storage, so a batch a backend could not commit
// fails identically in a unit test against the memory store.
var (
	ErrEmptyBatch               = errors.New("batch holds no messages")
	ErrBatchTooLarge            = errors.New("batch exceeds MaxMessagesPerPut")
	ErrMissingClientMessageID   = errors.New("batch message has no client message id")
	ErrDuplicateClientMessageID = errors.New("batch repeats a client message id")
)

// ErrSelfReactionsGroupOnly is returned by Store.GetSelfReactions for a DM: a
// store keeps no per-viewer reaction rows for a DM (see there), so the read has
// nothing to answer from and refuses rather than report the viewer reacted to
// nothing.
var ErrSelfReactionsGroupOnly = errors.New("self reactions are read for groups only")

// ErrReactionBoosted is returned by Store.RemoveReaction when the reaction is
// boosted (see Reactor.BoostUsdMicros): money has been paid on it, so it is
// permanent, and nothing changes.
var ErrReactionBoosted = errors.New("reaction is boosted and cannot be removed")

// ErrBoostEmpty is returned by Store.AddBoost for a boost worth nothing
// (BoostInput.UsdMicros of 0): a reaction is boosted exactly when its total
// is positive, so a boost of nothing has no state to land in and is refused
// rather than recorded as a boost that looks plain.
var ErrBoostEmpty = errors.New("boost is worth nothing")

// ErrInvalidReturnIntentID is Store.PutMessages' refusal of a batch in which a
// message's ReturnIntentID is set but is not a valid intent ID
// (commonpb.IntentId's validation; see ValidateMessageInputs). An offer stored with an ID no intent can carry
// could never be returned, so it is refused before anything is written.
var ErrInvalidReturnIntentID = errors.New("return intent id has the wrong length")

// ErrInvalidReturnOffer is Store.PutMessages' refusal of a batch in which a
// message's return offer and its ReturnIntentID disagree (see
// ValidateMessageInputs): an ID on anything but a single cash message to one
// receiver carrying CashContent.payment_return.offer, or such an offer with
// no ID. Either would leave clients and the server disagreeing about whether
// the payment can be returned, so it is refused before anything is written.
var ErrInvalidReturnOffer = errors.New("return offer does not match its return intent id")

// ErrNoCashMessageReturn is returned by Store.MarkCashMessageReturned for a
// message that exists but was sent with no return offer (no
// MessageInput.ReturnIntentID): there is nothing to mark, and nothing is
// written. It is told apart from ErrMessageNotFound so that a caller marking
// a return it validated against an offer learns which assumption failed.
var ErrNoCashMessageReturn = errors.New("message carries no return offer")

// ErrWrongReturnIntent is returned by Store.MarkCashMessageReturned when the
// intent named is not the one the message's offer holds
// (CashMessageReturn.IntentID), whether or not the message is marked already:
// only the payment made with the offer's own intent ID returns it, so a path
// that skipped validating the payment against the offer fails loudly rather
// than marking an offer some other payment did not return. Nothing is written.
var ErrWrongReturnIntent = errors.New("intent is not the message's return intent")

// MessageRef identifies a single message within a chat. It is the unit of a
// cross-chat batch read (see Store.GetMessagesByRefs) — e.g. one ref per chat to
// fetch every chat's last message for the feed.
type MessageRef struct {
	ChatID    *commonpb.ChatId
	MessageID *messagingpb.MessageId
}

// MessageInput is one message of a send, as Store.PutMessages takes it: what
// PutMessage takes per message, minus the chat, which a batch shares.
type MessageInput struct {
	SenderID           *commonpb.UserId // nil for a system message
	Content            []*messagingpb.Content
	Timestamp          time.Time
	ClientMessageID    *messagingpb.ClientMessageId
	CountsTowardUnread bool

	// ReturnIntentID, when set, makes the message a cash message offering
	// its payment for returning: it is the intent ID the return must use
	// (see CashMessageReturn), stored on the message's own record in the
	// send's write and read back only by Store.GetCashMessageReturn, never
	// onto Message. It must be a valid intent ID (ErrInvalidReturnIntentID;
	// see NewReturnIntentID), and it is set exactly when the content is
	// a single cash message to one receiver carrying
	// CashContent.payment_return.offer (ErrInvalidReturnOffer). nil for
	// every other message.
	ReturnIntentID *commonpb.IntentId
}

// NewReturnIntentID draws a cash message's return intent ID
// (MessageInput.ReturnIntentID): random bytes of an intent ID's length,
// checked by commonpb.IntentId's own validation, which is the one definition
// of that length every check of an ID defers to. Random rather than derived,
// since an ID anyone could compute could be used up first, blocking the
// return (see CashMessageReturn).
func NewReturnIntentID() (*commonpb.IntentId, error) {
	intentID := &commonpb.IntentId{Value: make([]byte, 32)}
	if _, err := rand.Read(intentID.Value); err != nil {
		return nil, err
	}
	if err := intentID.Validate(); err != nil {
		return nil, err
	}
	return intentID, nil
}

// CashMessageReturn is the server's half of a cash message's return offer
// (CashContent.payment_return.offer), what Store.GetCashMessageReturn reads
// beside the message. The offer clients see is fixed when the message is
// sent and never changes; what can change, and what must not be shown to
// every reader, lives here:
//
//   - IntentID is the intent ID the return payment must use, drawn at random
//     by the sender when the message is sent (MessageInput.ReturnIntentID)
//     and never changed. An intent ID is used at most once, so a return with
//     this ID is the only one that can go through, however often it is
//     retried; the same property means anyone who learned it could use it up
//     first and block the return, so it is handed only to the message's
//     single_receiver, never carried on Message, and so never in any message
//     read, delta or event.
//   - Returned is whether the payment has been returned
//     (Store.MarkCashMessageReturned). It only ever goes from false to true,
//     and moving it is not a change to the message: no event is logged and
//     the message's EventSequence stays as it was, so clients learn of it by
//     asking, or from the return's own cash message.
type CashMessageReturn struct {
	IntentID *commonpb.IntentId
	Returned bool
}

// MaxMessagesPerPut bounds a Store.PutMessages batch. It is set by the
// DynamoDB store, where a send is one transaction of 2n + 2 items (the chat's
// counter row, a message row and an idempotency marker per message, and one
// event-log row covering the whole batch) and a transaction holds at most 100
// items: floor(98 / 2). Every store enforces it (see ValidateMessageInputs) so
// the bound is one contract rather than a production-only surprise.
const MaxMessagesPerPut = 49

// ValidateMessageInputs checks the shape of a PutMessages batch: at least one
// message (ErrEmptyBatch), at most MaxMessagesPerPut (ErrBatchTooLarge), every
// message carrying a non-empty client message ID (ErrMissingClientMessageID —
// it is the idempotency marker's key, so a nil one cannot be keyed and an empty
// one would be a marker every such send shares), and no client message ID used
// twice (ErrDuplicateClientMessageID — two messages cannot share an idempotency
// marker, and a DynamoDB transaction refuses two writes to one item anyway),
// every ReturnIntentID that is set a valid intent ID
// (ErrInvalidReturnIntentID), and every message's return offer matching its
// ReturnIntentID (ErrInvalidReturnOffer, see returnOfferMatches). Stores call
// it before any read or write, so a replay carrying a malformed ID or a
// mismatched offer is refused too rather than answered.
func ValidateMessageInputs(inputs []MessageInput) error {
	if len(inputs) == 0 {
		return ErrEmptyBatch
	}
	if len(inputs) > MaxMessagesPerPut {
		return ErrBatchTooLarge
	}
	seen := make(map[string]struct{}, len(inputs))
	for _, in := range inputs {
		key := string(in.ClientMessageID.GetValue())
		if key == "" {
			return ErrMissingClientMessageID
		}
		if _, dup := seen[key]; dup {
			return ErrDuplicateClientMessageID
		}
		seen[key] = struct{}{}
		if in.ReturnIntentID != nil {
			if err := in.ReturnIntentID.Validate(); err != nil {
				return fmt.Errorf("%w: %w", ErrInvalidReturnIntentID, err)
			}
		}
		if !returnOfferMatches(in) {
			return ErrInvalidReturnOffer
		}
	}
	return nil
}

// returnOfferMatches reports whether a message's return offer as clients see
// it (CashContent.payment_return.offer) and as the server keeps it
// (ReturnIntentID) agree: a message with a ReturnIntentID is a single cash
// message to one receiver carrying an offer, and a message carrying an offer
// has a ReturnIntentID. So no message shows an offer the server holds no
// intent ID for, and no intent ID sits on a message that offers nothing.
func returnOfferMatches(in MessageInput) bool {
	offers := 0
	for _, content := range in.Content {
		if content.GetCash().GetPaymentReturn().GetOffer() != nil {
			offers++
		}
	}
	if in.ReturnIntentID == nil {
		return offers == 0
	}
	return len(in.Content) == 1 && offers == 1 && in.Content[0].GetCash().GetSingleReceiver() != nil
}

// PointerRef requests a chat's stored pointers (StoredPointerTypes) for the
// given members. It is the unit of the batched cross-chat pointer read (see
// Store.GetPointersForChats), mirroring MessageRef on the message path: the
// caller enumerates exactly which pointers to hydrate so the store can address
// them by key rather than scanning each chat's partition.
type PointerRef struct {
	ChatID  *commonpb.ChatId
	Members []*commonpb.UserId
}

// SelfReaction is one reaction a viewer currently holds in a chat, as the
// per-viewer overlay read reports it (see Store.GetSelfReactions): the (message,
// emoji) it sits on, the version that added it and when — the viewer's own
// Reactor.Version and Reactor.ReactedTs, from which the caller builds
// Reaction.Self.
type SelfReaction struct {
	MessageID *messagingpb.MessageId
	Emoji     string
	Version   uint64
	ReactedTs time.Time
	// BoostUsdMicros is the viewer's own total on the emoji
	// (Reactor.BoostUsdMicros), 0 for a plain reaction.
	BoostUsdMicros uint64
}

// BoostInput is the payment a boost records (see Store.AddBoost): what a Boost
// carries beyond the reaction it sits on. IntentID identifies the payment and
// is the write's idempotency key; UsdMicros is the payment's market value in
// millionths of a US dollar (MicrosFromUsd), what the totals grow by; Amount
// is the payment as made, kept exact on the record, and required; Ts is when
// the boost is recorded, and becomes the reaction's ReactedTs when the boost
// adds it.
type BoostInput struct {
	IntentID  *commonpb.IntentId
	Amount    *commonpb.CryptoPaymentAmount
	UsdMicros uint64
	Ts        time.Time
}

// StoredPointerTypes are the only pointer types persisted, for any chat type:
// DELIVERED and READ. SENT is client-side and never stored, so enumerating these
// per member addresses every pointer that can exist for a chat. Treat as
// read-only.
var StoredPointerTypes = []messagingpb.Pointer_Type{
	messagingpb.Pointer_DELIVERED,
	messagingpb.Pointer_READ,
}

// Store persists chat messages and message-history pointers.
//
// Each chat has its own gapless message ID sequence. Sends are made idempotent
// by a client-generated message ID, so a retried send returns the originally
// persisted message rather than assigning a new ID.
type Store interface {
	// PutMessage assigns the next gapless message ID for the chat and persists
	// the message, returning the persisted message with its assigned ID and
	// unread sequence.
	//
	// It is idempotent on (chatID, clientMessageID): a retried send with the
	// same client message ID returns the originally persisted message without
	// assigning a new ID. created reports whether this call persisted the
	// message (false on a retry), so callers can skip one-time side effects
	// like pushes.
	//
	// countsTowardUnread controls the unread sequence: when true the message's
	// unread_seq is the previous value + 1; when false it carries the previous
	// value forward (for messages that shouldn't bump anyone's unread count).
	//
	// senderID may be nil to denote a system message.
	//
	// It is PutMessages with a batch of one.
	PutMessage(
		ctx context.Context,
		chatID *commonpb.ChatId,
		senderID *commonpb.UserId,
		content []*messagingpb.Content,
		ts time.Time,
		clientMessageID *messagingpb.ClientMessageId,
		countsTowardUnread bool,
	) (msg *Message, created bool, err error)

	// PutMessages persists a batch of messages in one chat atomically: either
	// every message lands or none does, and no other send, edit or delete is
	// interleaved with them. The messages take consecutive gapless IDs and
	// consecutive event sequences in batch order, and unread_seq accumulates
	// through the batch as it would over the same sends made one at a time.
	// The result is in batch order, one persisted message per input.
	//
	// Idempotency is the batch's, on its client message IDs: a retry whose
	// every ID already names a message returns those messages (in batch order)
	// with created false, exactly as PutMessage answers a retried send. A
	// replay writes nothing, so what the retry carries is ignored, its
	// ReturnIntentIDs included: a retried send draws a fresh random ID, and
	// the one the first send stored is the one that stands (see
	// CashMessageReturn). The
	// replay is judged per ID, not per original batch: the IDs may have been
	// spent by several earlier sends, so a replay's messages need not be
	// consecutive, nor in ID order when the retry lists its IDs in another
	// order. Only a fresh write promises consecutive IDs. A batch is judged
	// whole, so a retry that finds only some of its IDs spent is
	// ErrPartialReplay (see there) and persists nothing. The batch's shape
	// is checked first (ValidateMessageInputs): an empty batch, one over
	// MaxMessagesPerPut, one with a message missing its client message ID, or
	// one repeating a client message ID is refused with nothing read or
	// written.
	//
	// It is the write behind a send whose messages must be seen together or
	// not at all — a client would otherwise observe, and a crash between two
	// single sends could leave, the first without the second.
	PutMessages(
		ctx context.Context,
		chatID *commonpb.ChatId,
		inputs []MessageInput,
	) (msgs []*Message, created bool, err error)

	// EditMessage replaces a message's content with the given content and stamps
	// editedTs as its last-edited time, advances the chat's event-log head, and
	// re-stamps the message's event_sequence to that new head — the message ID and
	// unread_seq are left untouched, so the per-chat ID sequence stays gapless. Like
	// DeleteMessage it advances the event-log head without minting a message ID, so
	// event_sequence diverges from message ID.
	//
	// It is an optimistic-concurrency operation: the edit is applied only if the
	// message's current event_sequence still equals expectedEventSeq. On a mismatch
	// nothing is modified and it returns the message's current state alongside
	// ErrEventSequenceConflict; there is no last-writer-wins path. It returns
	// ErrMessageNotFound if no such message exists. On success it returns the edited
	// message at its new event_sequence.
	EditMessage(
		ctx context.Context,
		chatID *commonpb.ChatId,
		messageID *messagingpb.MessageId,
		content []*messagingpb.Content,
		editedTs time.Time,
		expectedEventSeq uint64,
	) (*Message, error)

	// DeleteMessage tombstones a message: it replaces the message's content with a
	// single DeletedContent (carrying deletedTs and deletedBy), advances the chat's
	// event-log head, and re-stamps the message's event_sequence to that new head —
	// the message ID and unread_seq are left untouched, so the per-chat ID sequence
	// stays gapless. This is the first operation that advances the event-log head
	// without minting a message ID, so event_sequence diverges from message ID here.
	//
	// It is an optimistic-concurrency operation: the tombstone is applied only if
	// the message's current event_sequence still equals expectedEventSeq. On a
	// mismatch nothing is modified and it returns the message's current state
	// alongside ErrEventSequenceConflict; there is no last-writer-wins path. It
	// returns ErrMessageNotFound if no such message exists. On success it returns
	// the tombstoned message at its new event_sequence.
	//
	// deletedBy may be nil to denote a system-level deletion (e.g. moderation).
	DeleteMessage(
		ctx context.Context,
		chatID *commonpb.ChatId,
		messageID *messagingpb.MessageId,
		deletedBy *commonpb.UserId,
		deletedTs time.Time,
		expectedEventSeq uint64,
	) (*Message, error)

	// GetMessage returns a single message by ID, or ErrMessageNotFound. The read
	// is strongly consistent: it reflects every send, edit and delete that
	// completed before it.
	GetMessage(ctx context.Context, chatID *commonpb.ChatId, messageID *messagingpb.MessageId) (*Message, error)

	// GetCashMessageReturn returns a message, exactly as GetMessage would,
	// beside its return offer's server-side state (see CashMessageReturn),
	// nil when the message was sent with none. It is ErrMessageNotFound when
	// no such message exists. It is one strongly consistent read of the
	// message's record, so the message and its state are one snapshot, and
	// it reflects every send and MarkCashMessageReturned that completed
	// before it: a return validated against it cannot miss one already
	// marked. This is the only read that carries the state.
	//
	// An edit or a delete leaves the state as it was, so a deleted message
	// still answers with it beside its tombstone; whether that still offers
	// a return is the caller's call.
	GetCashMessageReturn(ctx context.Context, chatID *commonpb.ChatId, messageID *messagingpb.MessageId) (*Message, *CashMessageReturn, error)

	// MarkCashMessageReturned records that a message's payment has been
	// returned by the payment with intentID (CashMessageReturn.Returned),
	// which must be the offer's own intent ID. It is idempotent: marking a
	// message already marked, with that ID, succeeds and changes nothing, so
	// the task that records a return can be delivered more than once. It is
	// ErrMessageNotFound when no such message exists,
	// ErrNoCashMessageReturn when the message carries no return offer, and
	// ErrWrongReturnIntent when intentID is not the offer's, marked or not;
	// in every case nothing is written.
	//
	// It changes nothing a client reads: it logs no event, moves neither the
	// event-log head nor the message's EventSequence, and leaves the
	// message's content (and so its CashContent.payment_return) as sent, so
	// it races with no edit, delete or send. A client learns of it through
	// GetCashMessageReturn's reader, or from the return's own message.
	MarkCashMessageReturned(ctx context.Context, chatID *commonpb.ChatId, messageID *messagingpb.MessageId, intentID *commonpb.IntentId) error

	// MessageExists reports whether a message exists in the chat. It is a
	// lightweight existence check that does not read or decode the message body,
	// for callers (e.g. the reaction read paths) that only need to distinguish a
	// missing message and don't need its content. The read is strongly
	// consistent: it reflects every send that completed before it. Its callers
	// gate an action on a message the user is looking at, and a message just
	// delivered on the stream is one another server committed moments ago —
	// the case a lagging read would report as missing.
	MessageExists(ctx context.Context, chatID *commonpb.ChatId, messageID *messagingpb.MessageId) (bool, error)

	// GetMessages returns a page of messages for a chat ordered by message ID
	// (ascending by default), paged via the provided query options. The paging
	// token's value is the message ID of the last message from the previous
	// page (see PageTokenFromID). Returns an empty result (no error) when the
	// chat has no messages.
	GetMessages(ctx context.Context, chatID *commonpb.ChatId, opts ...database.QueryOption) ([]*Message, error)

	// GetMessagesByRefs returns the messages identified by the given refs that
	// exist, across any number of chats. Refs without a matching message are
	// omitted and duplicate refs collapse. Results are ordered by
	// (chatID, message ID), so refs within a single chat come back ascending by
	// ID. It returns an empty result (no error) when refs is empty.
	//
	// It is the batch read behind the DM feed, where it fetches every chat's last
	// message in one call (one ref per chat). For the single-chat case the caller
	// builds refs from a shared chat ID.
	GetMessagesByRefs(ctx context.Context, refs []MessageRef) ([]*Message, error)

	// GetEventDelta reads up to limit event-log entries with event_sequence in
	// (afterEventSeq, headEventSeq], ascending, and returns the current state of the
	// messages they concern — the page primitive behind GetDelta's catch-up. The
	// event log stores only a thin descriptor per event (message_id, type, ts); each
	// referenced message is joined to its current materialized state here.
	//
	// Superseded events are dropped: when a scanned event's message has since changed
	// again (its current event_sequence is greater than that event's sequence), the
	// stale entry is skipped because a newer event — later in this delta, or, if past
	// head, on the live stream — carries the up-to-date state. So a message appears at
	// most once, at its latest in-range event, in its current state.
	//
	// nextCursor is the highest event_sequence scanned (<= headEventSeq), whether or
	// not it survived the drop — the caller advances its cursor to it so a fully
	// superseded page still makes progress rather than re-reading. It equals
	// afterEventSeq when the range is empty. The log is gapless and read consistently,
	// so advancing by nextCursor never skips an event. limit <= 0 uses the store's
	// default page size.
	GetEventDelta(ctx context.Context, chatID *commonpb.ChatId, afterEventSeq, headEventSeq uint64, limit int) (msgs []*Message, nextCursor uint64, err error)

	// GetLatestEventSequence returns the chat's current head event sequence — the
	// highest event_sequence assigned in the chat, or 0 when the chat has no
	// messages. It bounds GetDelta catch-up: a client whose cursor equals this
	// value is at the head.
	//
	// While every event is a new message (no edits or deletes yet) the event
	// sequence advances in lockstep with the message ID, so this equals the
	// chat's highest message ID. The two are distinct concepts: once edits and
	// deletes advance the event sequence without minting a new message ID, they
	// diverge.
	GetLatestEventSequence(ctx context.Context, chatID *commonpb.ChatId) (uint64, error)

	// GetLatestEventSequencesForChats returns the head event sequence of each of
	// the given chats, keyed by string(chatID.Value). It is the cross-chat batch
	// counterpart to GetLatestEventSequence, used to hydrate Metadata.latest_event_sequence
	// for the DM feed in one call rather than one read per chat. A chat with no
	// messages (head 0) is absent from the map and duplicate chat IDs collapse, so
	// callers must treat a missing key as 0. Returns an empty map (no error) when
	// chatIDs is empty.
	GetLatestEventSequencesForChats(ctx context.Context, chatIDs []*commonpb.ChatId) (map[string]uint64, error)

	// GetPointers returns all delivered/read pointers for a chat. Returns an
	// empty result (no error) when the chat has no pointers.
	GetPointers(ctx context.Context, chatID *commonpb.ChatId) ([]*messagingpb.Pointer, error)

	// GetPointersForChats returns the stored pointers (StoredPointerTypes) for the
	// members named in each ref, keyed by string(chatID.Value). It is the
	// cross-chat batch counterpart to GetPointers, used to hydrate member pointers
	// for the DM feed. Unlike GetPointers it addresses pointers by exact key
	// (chat × member × StoredPointerTypes) in a single batched read rather than
	// scanning each chat's partition; since those are the only types ever stored,
	// this is exhaustive. Chats with no matching pointers are absent from the map
	// and duplicate (chat, member) pairs collapse. Returns an empty map (no error)
	// when refs is empty.
	GetPointersForChats(ctx context.Context, refs []PointerRef) (map[string][]*messagingpb.Pointer, error)

	// AdvancePointer moves a member's pointer of the given type forward to
	// newValue. Pointers are monotonic: a request to move a pointer to a value
	// at or before its current value is a no-op. The bool reports whether this
	// call moved the pointer; when it did, the returned pointer is the new state
	// (newValue, carrying this advance's ts). On a no-op the pointer is nil: the
	// stored state is not read back, so an implementation (see messaging/cache)
	// may answer a no-op without reaching storage at all. Callers act on a
	// pointer only when it advanced.
	//
	// It does not verify that newValue references an existing message. Callers
	// with a caller-supplied newValue must check existence first (see
	// MessageExists and the AdvancePointer RPC); callers that already know the
	// message exists — e.g. the sender's own READ pointer right after PutMessage
	// returns the message it just wrote — can advance directly.
	AdvancePointer(
		ctx context.Context,
		chatID *commonpb.ChatId,
		userID *commonpb.UserId,
		pointerType messagingpb.Pointer_Type,
		newValue *messagingpb.MessageId,
	) (*messagingpb.Pointer, bool, error)

	// AddReaction records userID's reaction with emoji on a message and returns
	// the emoji's aggregate after the add. The result goes back to the reactor
	// alone, so unlike the summary reads it is already the caller's view:
	// Self is the caller's own entry, at the version that added their reaction
	// — the new Version on a real add, the earlier one on a re-add — and when:
	// ts on a real add, the original add's time on a re-add (ts is ignored). It
	// is idempotent on (chat, message, emoji, user): a re-add
	// returns the current aggregate with created false and changes nothing.
	// created reports whether this call actually added the reaction (false on a
	// re-add), so callers can skip the broadcast.
	//
	// tooManyTypes is true (with a nil reaction) when adding this emoji would
	// exceed MaxReactionTypesPerMessage distinct emoji on the message; the add is
	// rejected. Re-adding an already-present emoji never trips the cap.
	//
	// It does not verify the message exists or is reactable — the caller checks
	// that first (see Message.IsReactable).
	AddReaction(
		ctx context.Context,
		chatID *commonpb.ChatId,
		messageID *messagingpb.MessageId,
		userID *commonpb.UserId,
		emoji string,
		ts time.Time,
	) (reaction *Reaction, created bool, tooManyTypes bool, err error)

	// AddBoost records a boost: userID's payment to put money behind emoji on
	// a message (see boost.go for what a boost is). In one atomic write it
	// keeps the Boost record (keyed by the payment's intent ID), adds userID's
	// reaction with emoji if they have none, grows the emoji's and userID's
	// totals by boost.UsdMicros, places userID in the retained sample
	// (inserted with their reaction's version and time, or their entry's
	// total raised; the least entry by ReactorLess evicted when that
	// overfills it, so the sample always holds the top boosters), and
	// advances the emoji's version by one. The result is the emoji's
	// aggregate after the write as the booster's own view: Self is their
	// entry with their new total.
	//
	// It is idempotent on the intent ID, scoped to the message: a boost
	// already recorded on the message under that intent ID is a replay, with
	// whatever emoji it names, which returns the current aggregate with
	// credited false and changes nothing, so the task that records a payment
	// can be delivered more than once. credited reports whether this call
	// recorded the boost, so callers can skip the broadcast on a replay;
	// created whether it added the reaction (false when userID had already
	// reacted, and on a replay).
	//
	// It records a boost in a DM as in a group, except that a DM keeps no
	// per-viewer row of it (see GetSelfReactions). boost.UsdMicros must be
	// positive, a boost worth nothing being ErrBoostEmpty, refused before the
	// intent ID is looked at and recording nothing. boost.IntentID and
	// boost.Amount must be set; a boost without either is an error, and
	// nothing is written.
	//
	// A boost adds its reaction whatever the message's distinct-emoji count:
	// the money has been paid, so the cap that AddReaction enforces is not
	// applied here (the caller refuses the payment up front instead). It does
	// not verify the message exists or is reactable, as AddReaction does not;
	// a boost on a message that has since been deleted still lands.
	AddBoost(
		ctx context.Context,
		chatID *commonpb.ChatId,
		messageID *messagingpb.MessageId,
		userID *commonpb.UserId,
		emoji string,
		boost BoostInput,
	) (reaction *Reaction, credited bool, created bool, err error)

	// RemoveReaction removes userID's reaction with emoji from a message. It is
	// idempotent: removing a reaction that isn't present returns removed false and
	// changes nothing. removed reports whether this call actually removed the
	// reaction, so callers can skip the broadcast.
	//
	// A boosted reaction (one with a non-zero Reactor.BoostUsdMicros) is
	// permanent: the call returns ErrReactionBoosted and changes nothing. The
	// refusal is decided by the write itself, conditioned on the reactor's row
	// carrying no boost, so a boost that lands between the store's read and
	// its write still makes the removal lose rather than take the boost's row
	// with it.
	//
	// reaction is the emoji's aggregate after the removal, with Count possibly 0
	// when the last reactor left (it still carries the advanced Version, which
	// the removal broadcast needs). Self is left nil — which is also
	// the correct overlay for the caller, who just removed their reaction. It is
	// nil only when the emoji has no aggregate at all (a pure no-op).
	RemoveReaction(
		ctx context.Context,
		chatID *commonpb.ChatId,
		messageID *messagingpb.MessageId,
		userID *commonpb.UserId,
		emoji string,
	) (reaction *Reaction, removed bool, err error)

	// GetReactionSummary returns the per-emoji aggregates for a single message,
	// one entry per distinct emoji that currently has at least one reactor. The
	// aggregates are shareable: Self is left nil for the caller to
	// overlay (see GetSelfReactions). Returns an empty result (no error) when the
	// message has no reactions.
	//
	// The read is strongly consistent, as every summary read is: it reflects
	// every add and remove that completed before it. A stale count would be
	// harmless — the client keeps the greater version — but a stale *set* is
	// not: a reader who reacts and then refreshes would find their emoji
	// missing, with no version to tell "not yet visible" from "removed", and
	// the strongly consistent self overlay would then have nothing to mark.
	GetReactionSummary(
		ctx context.Context,
		chatID *commonpb.ChatId,
		messageID *messagingpb.MessageId,
	) ([]*Reaction, error)

	// GetReactionSummariesByRefs returns one summary per requested message,
	// deduplicated and ordered by message ID. A message with no reactions (or
	// unknown) is echoed with an empty Reactions slice rather than omitted, so the
	// caller gets an answer for every requested ID. Aggregates are shareable
	// (Self left nil) and the read is strongly consistent (see
	// GetReactionSummary). Returns an empty result (no error) when messageIDs is
	// empty.
	GetReactionSummariesByRefs(
		ctx context.Context,
		chatID *commonpb.ChatId,
		messageIDs []*messagingpb.MessageId,
	) ([]*ReactionSummary, error)

	// GetReactionSummaries returns one summary per message in a page of the chat's
	// messages, ordered by message ID and paged via the query options (the paging
	// token is a message ID, as in GetMessages). The page spans messages, not just
	// reacted ones: a message with no reactions is returned with an empty Reactions
	// slice rather than skipped. Aggregates are shareable (Self left
	// false) and the read is strongly consistent (see GetReactionSummary).
	// Returns an empty result (no error) when the page is empty.
	GetReactionSummaries(
		ctx context.Context,
		chatID *commonpb.ChatId,
		opts ...database.QueryOption,
	) ([]*ReactionSummary, error)

	// GetSelfReactions returns every reaction userID currently holds on the given
	// messages of a group, each with the version that added it, when, and the
	// viewer's boost total on it — the per-viewer data behind Reaction.Self and
	// EmojiReaction.self_reactor. It is
	// the viewer's half of a summary read: the aggregates are shareable and
	// leave Self unset, and this answers it for one viewer across a
	// whole page. The read is addressed by the viewer, not by every (message,
	// emoji) on the page, so its cost follows how much the viewer reacted rather
	// than how reacted the page is, and it depends on nothing but the message
	// IDs, so it can run alongside the aggregate read. Message IDs are
	// deduplicated and an unknown ID contributes nothing; the result is in no
	// particular order. Returns an empty result (no error) when messageIDs is
	// empty. The read is strongly consistent: the rows it reads are written and
	// deleted in the add's and remove's own transaction, so it reflects every
	// transition of the viewer's that completed before it.
	//
	// It is for groups only and returns ErrSelfReactionsGroupOnly for a DM. A
	// DM's two members can never outgrow an emoji's sample, so its overlay is
	// read off the aggregates the caller already holds (see
	// Server.applySelfReactions), and a store keeps no per-viewer rows for a DM
	// — a write per reaction that nothing would read.
	GetSelfReactions(
		ctx context.Context,
		chatID *commonpb.ChatId,
		userID *commonpb.UserId,
		messageIDs []*messagingpb.MessageId,
	) ([]SelfReaction, error)

	// GetReactors returns a page of the users who reacted to a message with emoji,
	// in list order (see ReactorLess): most boosted first, then most recent
	// first, paged via the query options (the paging token is a
	// ReactorPageToken, which resumes strictly after the reactor it names under
	// that order, so a page boundary may fall between the last booster and the
	// first plain reactor). It also returns hasMore, whether further pages
	// remain. Returns an empty result (no error) when the message has no reactors
	// for the emoji.
	//
	// With no limit in the options the whole list is returned and hasMore is
	// false; the RPC always sets one.
	//
	// The plain reactors are read strongly consistent: the page reflects every
	// add and remove that completed before it. So is the boosted head while
	// the emoji has at most MaxStoredSampleReactors boosters, the common
	// case, since the retained sample then holds every booster and serves
	// the head. Past that size the head may be served from an index that
	// trails a boost by a moment, so a reactor whose boost has just landed
	// can be missing from a page: the boost moved their row out of the plain
	// index at once (the strongly consistent read no longer finds it) while
	// the boosted index has yet to list it; they appear on the next read.
	// The booster's own client is the likeliest to refetch, since the
	// BOOSTED update reaches it first, which is why the exact head covers
	// every emoji but the rare one with many boosters.
	//
	// version is the emoji aggregate's version (Reaction.Version), read BEFORE
	// the page, so the plain tail, and a head served from the sample, reflect
	// at least every transition up to it and possibly more; a head served
	// from the index is the exception above, and may lack a boost at or
	// below it. That direction is the safe one: a client holding version may
	// apply live updates for the emoji whose version exceeds it, and any it
	// re-applies are idempotent (an add of a reactor already listed dedupes,
	// a remove of one already absent is a no-op, a boost re-applied is the
	// same total), whereas a version newer than the page would make it drop
	// an update the page is missing. It is 0 when the emoji has no aggregate.
	// A page cannot read the two atomically, so the order of the reads is
	// the contract.
	GetReactors(
		ctx context.Context,
		chatID *commonpb.ChatId,
		messageID *messagingpb.MessageId,
		emoji string,
		opts ...database.QueryOption,
	) (reactors []*Reactor, version uint64, hasMore bool, err error)
}

// PageTokenFromID encodes a message ID as a paging token. The token is the
// identifier of the last message in a page; the next request resumes strictly
// after it. Shared by the store implementations and callers.
func PageTokenFromID(messageID *messagingpb.MessageId) *commonpb.PagingToken {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, messageID.Value)
	return &commonpb.PagingToken{Value: buf}
}

// IDFromPageToken decodes the message ID from a token produced by
// PageTokenFromID. The ok return is false if the token is nil or malformed.
func IDFromPageToken(token *commonpb.PagingToken) (messageID uint64, ok bool) {
	if token == nil || len(token.Value) != 8 {
		return 0, false
	}
	return binary.BigEndian.Uint64(token.Value), true
}

// ReactorCursor is a position in an emoji's reactor list (see ReactorLess):
// the boost total and version of the last reactor returned. Versions are
// unique within an emoji, so the pair names one reactor, and the next page
// resumes strictly after them (see ReactorAfterCursor). A cursor with a boost
// total is in the boosted head of the list; one without is in the plain tail,
// which every boosted reactor precedes.
type ReactorCursor struct {
	BoostUsdMicros uint64
	Version        uint64
}

// ReactorAfterCursor reports whether reactor lies strictly after cursor in
// list order: less boosted, or as boosted and older. It is how every backend
// applies a page token, so a page boundary means the same thing everywhere.
func ReactorAfterCursor(cursor ReactorCursor, reactor *Reactor) bool {
	if reactor.BoostUsdMicros != cursor.BoostUsdMicros {
		return reactor.BoostUsdMicros < cursor.BoostUsdMicros
	}
	return reactor.Version < cursor.Version
}

// ReactorPageToken encodes a reactor as the server-issued cursor returned in
// GetReactorsResponse.paging_token: its ReactorCursor, boost total then
// version, each big-endian. The token is opaque to the client, which echoes it
// back in options.paging_token.
func ReactorPageToken(reactor *Reactor) *commonpb.PagingToken {
	buf := make([]byte, 16)
	binary.BigEndian.PutUint64(buf[:8], reactor.BoostUsdMicros)
	binary.BigEndian.PutUint64(buf[8:], reactor.Version)
	return &commonpb.PagingToken{Value: buf}
}

// ReactorFromPageToken decodes the cursor from a token produced by
// ReactorPageToken. The ok return is false if the token is nil or malformed,
// which every store answers with the first page: that covers the 8-byte,
// version-only token issued before boosts, which has no position in the
// boost-first order and so restarts the list rather than resuming it.
func ReactorFromPageToken(token *commonpb.PagingToken) (cursor ReactorCursor, ok bool) {
	if token == nil || len(token.Value) != 16 {
		return ReactorCursor{}, false
	}
	return ReactorCursor{
		BoostUsdMicros: binary.BigEndian.Uint64(token.Value[:8]),
		Version:        binary.BigEndian.Uint64(token.Value[8:]),
	}, true
}
