package messaging

import (
	"context"
	"encoding/binary"
	"errors"
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

// MessageRef identifies a single message within a chat. It is the unit of a
// cross-chat batch read (see Store.GetMessagesByRefs) — e.g. one ref per chat to
// fetch every chat's last message for the feed.
type MessageRef struct {
	ChatID    *commonpb.ChatId
	MessageID *messagingpb.MessageId
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

// ReactionRef identifies one (message, emoji) reaction within a chat. It is the
// unit of the batched self-reaction lookup (see Store.GetSelfReactions): the
// caller derives refs from a reaction summary it already holds so the store can
// resolve each by exact key rather than scanning.
type ReactionRef struct {
	MessageID *messagingpb.MessageId
	Emoji     string
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
	PutMessage(
		ctx context.Context,
		chatID *commonpb.ChatId,
		senderID *commonpb.UserId,
		content []*messagingpb.Content,
		ts time.Time,
		clientMessageID *messagingpb.ClientMessageId,
		countsTowardUnread bool,
	) (msg *Message, created bool, err error)

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

	// GetMessage returns a single message by ID, or ErrMessageNotFound.
	GetMessage(ctx context.Context, chatID *commonpb.ChatId, messageID *messagingpb.MessageId) (*Message, error)

	// MessageExists reports whether a message exists in the chat. It is a
	// lightweight existence check that does not read or decode the message body,
	// for callers (e.g. the reaction read paths) that only need to distinguish a
	// missing message and don't need its content.
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

	// GetMessagesByEventSequence returns up to limit messages whose current
	// event_sequence is greater than afterEventSeq, ordered by event_sequence
	// ascending — the page primitive behind GetDelta's state delta. Because the
	// ordering is by CURRENT event_sequence, a message edited or deleted after
	// afterEventSeq appears at its new (higher) position, so each message appears at
	// most once. limit <= 0 uses the store's default page size. Returns an empty
	// result (no error) when nothing changed past afterEventSeq.
	GetMessagesByEventSequence(ctx context.Context, chatID *commonpb.ChatId, afterEventSeq uint64, limit int) ([]*Message, error)

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
	// at or before its current value is a no-op. It always returns the pointer's
	// current state (carrying its last-advanced ts), whether or not this call
	// moved it; the bool reports whether it advanced. The pointer is nil only
	// alongside a non-nil error.
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
	// the emoji's aggregate after the add. The aggregate is shareable, so
	// ReactedBySelf is left false for the caller to overlay. It is
	// idempotent on (chat, message, emoji, user): a re-add returns the current
	// aggregate with created false and changes nothing. created reports whether
	// this call actually added the reaction (false on a re-add), so callers can
	// skip the broadcast.
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

	// RemoveReaction removes userID's reaction with emoji from a message. It is
	// idempotent: removing a reaction that isn't present returns removed false and
	// changes nothing. removed reports whether this call actually removed the
	// reaction, so callers can skip the broadcast.
	//
	// reaction is the emoji's aggregate after the removal, with Count possibly 0
	// when the last reactor left (it still carries the advanced Sequence, which
	// the removal broadcast needs). ReactedBySelf is left false — which is also
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
	// aggregates are shareable: ReactedBySelf is left false for the caller to
	// overlay (see GetSelfReactions). Returns an empty result (no error) when the
	// message has no reactions.
	GetReactionSummary(
		ctx context.Context,
		chatID *commonpb.ChatId,
		messageID *messagingpb.MessageId,
	) ([]*Reaction, error)

	// GetReactionSummariesByRefs returns one summary per requested message,
	// deduplicated and ordered by message ID. A message with no reactions (or
	// unknown) is echoed with an empty Reactions slice rather than omitted, so the
	// caller gets an answer for every requested ID. Aggregates are shareable
	// (ReactedBySelf left false). Returns an empty result (no error) when messageIDs
	// is empty.
	GetReactionSummariesByRefs(
		ctx context.Context,
		chatID *commonpb.ChatId,
		messageIDs []*messagingpb.MessageId,
	) ([]*ReactionSummary, error)

	// GetReactionSummaries returns one summary per message in a page of the chat's
	// messages, ordered by message ID and paged via the query options (the paging
	// token is a message ID, as in GetMessages). The page spans messages, not just
	// reacted ones: a message with no reactions is returned with an empty Reactions
	// slice rather than skipped. Aggregates are shareable (ReactedBySelf left
	// false). Returns an empty result (no error) when the page is empty.
	GetReactionSummaries(
		ctx context.Context,
		chatID *commonpb.ChatId,
		opts ...database.QueryOption,
	) ([]*ReactionSummary, error)

	// GetSelfReactions returns the subset of refs that userID has reacted to — the
	// per-viewer data behind EmojiReaction.reacted_by_self. The caller derives refs
	// from a summary it already holds, so the store resolves them by exact key in
	// one batched read. Returns an empty result (no error) when refs is empty.
	GetSelfReactions(
		ctx context.Context,
		chatID *commonpb.ChatId,
		userID *commonpb.UserId,
		refs []ReactionRef,
	) ([]ReactionRef, error)

	// GetReactors returns a page of the users who reacted to a message with emoji,
	// most-recent-first, paged via the query options (the paging token is a
	// ReactorPageToken). It also returns hasMore, whether further pages remain.
	// Returns an empty result (no error) when the message has no reactors for the
	// emoji.
	//
	// When consistent is true the read is strongly consistent — reflecting every
	// preceding add/remove with no propagation lag — at the cost of scaling less
	// well to large, deeply-paged reactor lists. The flag changes only consistency;
	// the ordering and paging semantics are identical either way.
	GetReactors(
		ctx context.Context,
		chatID *commonpb.ChatId,
		messageID *messagingpb.MessageId,
		emoji string,
		consistent bool,
		opts ...database.QueryOption,
	) (reactors []*Reactor, hasMore bool, err error)
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

// ReactorPageToken encodes a reactor as the server-issued cursor returned in
// GetReactorsResponse.paging_token. Reactors are returned most-recent-first, so
// the token carries the last reactor's reaction timestamp (the ordering key)
// followed by its user ID (a tie-breaker for equal timestamps); the next request
// resumes strictly after it. The token is opaque to the client, which echoes it
// back in options.paging_token.
func ReactorPageToken(reactor *Reactor) *commonpb.PagingToken {
	buf := make([]byte, 8, 8+len(reactor.UserID.Value))
	binary.BigEndian.PutUint64(buf, uint64(reactor.ReactedTs.UnixNano()))
	buf = append(buf, reactor.UserID.Value...)
	return &commonpb.PagingToken{Value: buf}
}

// ReactorFromPageToken decodes the reaction timestamp and user ID from a token
// produced by ReactorPageToken. The ok return is false if the token is nil or
// malformed.
func ReactorFromPageToken(token *commonpb.PagingToken) (reactedTs time.Time, userID *commonpb.UserId, ok bool) {
	if token == nil || len(token.Value) <= 8 {
		return time.Time{}, nil, false
	}
	nanos := int64(binary.BigEndian.Uint64(token.Value[:8]))
	return time.Unix(0, nanos).UTC(), &commonpb.UserId{Value: append([]byte(nil), token.Value[8:]...)}, true
}
