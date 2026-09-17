package dynamodb

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"math/rand/v2"
	"slices"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"
	messagingpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/messaging/v1"

	"github.com/code-payments/flipcash2-server/chat"
	"github.com/code-payments/flipcash2-server/database"
	"github.com/code-payments/flipcash2-server/messaging"
)

// The messaging store spans five tables:
//
//	messages           pk = "chat#<id>", sk in { "#counter", "msg#<padded seq>",
//	                   "evt#<padded event_seq>", "cmid#<client id>" }. All of a
//	                   chat's messages, its event log, its sequence counter, and its
//	                   idempotency markers share one partition so a send is one
//	                   single-partition transaction. The #counter row holds last_seq
//	                   (message-ID head) and last_event_seq (event-log head). Each
//	                   msg# row is a message's current materialized state (read by ID
//	                   for history) and carries its current event_seq — the client's
//	                   optimistic-concurrency token. Each evt#<event_seq> row is an
//	                   append-only event-log entry — a thin descriptor (message_id,
//	                   type, ts), not a copy of the message — read as a
//	                   strongly-consistent, gapless, event-ordered range and joined to
//	                   the messages' current state for delta catch-up (see
//	                   GetEventDelta). Unlike a GSI this range can be read consistently
//	                   and never has transient holes, so a catch-up cursor can't skip an
//	                   event. While every event is a new message, event_seq is the
//	                   message's own seq and last_event_seq mirrors last_seq; the two
//	                   heads (and event_seq vs seq) diverge once edits and deletes
//	                   append events without minting a seq.
//
//	message_pointers   pk = "chat#<id>". Pointers are kept out of the messages
//	                   partition so heavy receipt writes don't contend with the
//	                   send path (they share nothing transactional with messages).
//	                   The item shape depends on the chat type. The feed hydrates
//	                   pointers with a keyed batch read that is billed per item
//	                   however small, so fewer items per chat is a cheaper page:
//
//	                   A group keeps sk = "ptr#<user>", one item per (chat,
//	                   member) carrying every stored pointer type as its own
//	                   attribute pair (<type>_value, <type>_ts; see
//	                   memberPointerAttrs). A roster is mutable and unbounded,
//	                   so the item stays per member.
//
//	                   A DM keeps sk = "#ptrs", a single item for the chat
//	                   carrying both members' pointers, each (member, type) as
//	                   its own attribute pair: the group item's attribute names
//	                   with the member appended (delivered_value#<user hex>,
//	                   delivered_ts#<user hex>, read_value#<user hex>,
//	                   read_ts#<user hex>; see dmPointerAttrs), so both layouts
//	                   read as one scheme. A DM's membership is exactly two and
//	                   immutable, so one item per chat halves the feed's pointer
//	                   read with no growth risk. Every write names only its own
//	                   pair and conditions only on it, so the two members never
//	                   interfere: DynamoDB serializes writes per item, and a
//	                   member's write leaves the peer's attributes untouched
//	                   whether it applies or not.
//
//	message_reactions  pk = "chat#<id>", sk in { "agg#<padded seq>#<emoji hex>",
//	                   "meta#<padded seq>" }. The aggregates. One agg# row per
//	                   (message, emoji) holds the reactor count, the emoji's
//	                   version, and a bounded sample of its most recent reactors.
//	                   One meta# row per reacted message holds the message's
//	                   own reaction state, today the count of emoji currently
//	                   active on it, for the type cap. Aggregates are keyed by
//	                   chat so that a page of messages' summaries is one
//	                   sort-key range query (see reactionsForSeqRange), the
//	                   read every history page pays.
//
//	message_reactors   pk = "chat#<id>#<padded seq>", sk = "user#<user hex>#<emoji
//	                   hex>". One row per current reaction, keyed by message so
//	                   that the by_version LSI can be queried with a
//	                   strongly consistent read (a GSI never can), and so that
//	                   the item-collection size cap an LSI imposes applies per
//	                   message rather than per chat. A row is deleted, not
//	                   tombstoned, when the reaction is removed: no reader
//	                   follows a reactor row's transitions, so there is nothing
//	                   for a tombstone to remember (contrast group_members).
//
//	message_self_reactions  pk = "chat#<id>#<user hex>", sk = "<padded seq>#<emoji
//	                   hex>", version. A group's reactions keyed by viewer: one
//	                   row per current reaction, written and deleted in the
//	                   reactor row's transaction, carrying the version that
//	                   added it. A viewer's reactions across a page of messages
//	                   — the reacted_by_self overlay on a summary read — are
//	                   one strongly consistent sort-key range on the viewer's
//	                   own partition, billed by what the viewer reacted rather
//	                   than a key probe per aggregate on the page. It is its
//	                   own table so that this per-reaction write lands on the
//	                   viewer's partition, not the chat partition that already
//	                   takes every aggregate write in the chat. A DM writes no
//	                   row here: its two members can never outgrow an emoji's
//	                   sample, so its overlay is answered from the aggregate
//	                   (see messaging.Server.applySelfReactions) and a row
//	                   would be a transactional write nothing reads.
//
//	                   The design follows the group_members roster summary. The
//	                   agg# row is the emoji's "#meta": every add or remove is
//	                   one transaction that writes the reactor row and
//	                   compare-and-sets the aggregate to the exact next state the
//	                   writer computed (count, version + 1, sample), conditioned
//	                   on the version it read. The writer therefore knows the
//	                   outcome with no read afterward, the sample's eviction and
//	                   the type cap are exact rather than best-effort, and a lost
//	                   race hands back the current row for a read-free retry. The
//	                   reactor row is stamped with the version that added it,
//	                   which is what the LSI orders by: unique per transition
//	                   within an emoji, so the order is total, the paging cursor
//	                   is the bare version, and no wall clock is involved.
const (
	skCounter   = "#counter"
	msgPrefix   = "msg#"
	evtPrefix   = "evt#"
	cmidPrefix  = "cmid#"
	seqPadWidth = 20

	// cmidTTL is how long a cmid# idempotency marker is retained before DynamoDB
	// TTL reaps it. Markers only guard against retried sends, which happen within
	// seconds, so a month of retention is ample; a (wildly implausible) retry past
	// this window would persist a duplicate message rather than dedup.
	cmidTTL = 30 * 24 * time.Hour

	// messages table attributes
	attrPK            = "pk"
	attrSK            = "sk"
	attrSeq           = "seq"
	attrLastSeq       = "last_seq"
	attrLastUnreadSeq = "last_unread_seq"
	attrLastEventSeq  = "last_event_seq" // #counter row: event-log head (maintained == last_seq until edits/deletes diverge it)
	attrSenderID      = "sender_id"
	attrContent       = "content"
	attrTS            = "ts"
	attrUnreadSeq     = "unread_seq"
	attrEventSeq      = "event_seq"      // msg# row: the message's current event-log sequence (== seq while every event is a new message); the client's optimistic-concurrency token
	attrLastEditedTs  = "last_edited_ts" // msg# row: when the message's content was last edited (absent until edited); a delete leaves it untouched
	attrMessageID     = "message_id"     // evt# row: the message this event concerns (msg# rows encode it in the sk instead)
	attrEventType     = "event_type"     // evt# row: the messaging.EventType recorded (create/edit/delete)
	attrExpiresAt     = "expires_at"     // DynamoDB TTL attribute (epoch seconds)

	// message_pointers table, group items. The member is carried by the sk
	// alone (see pointerSK / userIDFromPointerSK), as the chat is by the pk.
	ptrPrefix = "ptr#"
	// One attribute pair per stored pointer type on the ptr# item (see
	// memberPointerAttrs): the pointer's value and its last-advanced timestamp.
	attrDeliveredVal = "delivered_value"
	attrDeliveredTS  = "delivered_ts"
	attrReadVal      = "read_value"
	attrReadTS       = "read_ts"

	// message_pointers table, DM item. One item per chat; each member's pair for
	// a type is keyed by the member's hex ID under these prefixes — the group
	// item's attribute names plus a separator (see dmPointerAttrs). No value
	// prefix may be a prefix of a timestamp prefix, or vice versa, as
	// dmPointersFromItem recovers pairs by prefix.
	skDmPointers         = "#ptrs"
	dmDeliveredValPrefix = attrDeliveredVal + "#"
	dmDeliveredTSPrefix  = attrDeliveredTS + "#"
	dmReadValPrefix      = attrReadVal + "#"
	dmReadTSPrefix       = attrReadTS + "#"

	// message_reactions table (aggregates, keyed by chat)
	aggPrefix  = "agg#"
	metaPrefix = "meta#"

	attrEmoji           = "emoji"
	attrReactorCount    = "reactor_count"
	attrReactionVersion = "version"
	attrSample          = "sample" // agg# row map: user hex -> { version: added at, ts: reacted nanos }; a reserved word, so always named through a placeholder
	attrSampleVersion   = "version"
	attrSampleTs        = "ts"
	attrTypeCount       = "type_count" // meta# row: emoji currently active on the message

	// message_reactors table (one row per current reaction, keyed by message).
	// The member is carried by the sk under the same prefix the chat store uses
	// for its membership rows.
	userPrefix = "user#"

	attrReactedTs    = "reacted_ts"    // display timestamp, projected into the LSI
	attrEmojiVersion = "emoji_version" // "<emoji hex>#<padded version>": the LSI sort key

	// lsiByVersion orders a message's reactor rows by emoji_version (emoji, then
	// the version that added the reactor). Queried under one emoji's prefix it
	// yields that emoji's reactors in reaction order, and being local it can be
	// read strongly consistently. The aggregate rows live in the other table,
	// so nothing but reactor rows can ever carry emoji_version and leak into it.
	lsiByVersion = "by_version"

	// message_self_reactions table (one row per current reaction, keyed by
	// viewer). The version that added the reaction is the row's one attribute
	// beyond its keys.
	attrSelfVersion = "version"

	// maxSummaryRefGap bounds how far apart two requested message IDs may be for
	// GetReactionSummariesByRefs to cover both with one range query. Refs usually
	// arrive as a page of consecutive IDs, which is one query; a scattered batch is
	// split into runs so that no query reads a long stretch of aggregates nobody
	// asked for.
	maxSummaryRefGap = 64

	// DynamoDB transaction cancellation / condition codes
	codeConditionalCheckFailed = "ConditionalCheckFailed"
	codeTransactionConflict    = "TransactionConflict"

	maxPutMessageAttempts = 32
	maxBatchGetKeys       = 100

	// A reaction transition is a transaction of two to four items — the reactor
	// row, the emoji's aggregate, (when an emoji activates or empties) the
	// message's type-count row, and (in a group) the viewer-keyed copy — and
	// every transition on an emoji contends on its aggregate, as membership
	// transitions contend on a group's #meta (see chat/dynamodb). Both failures
	// that contention produces are absorbed with a bounded number of attempts:
	// a lost compare-and-set retries from the value the failure returned, with
	// no extra read, and a transaction conflict after a fresh read (see
	// absorbReactionFailure). Unlike membership, both wait before retrying (short,
	// jittered, doubling): a roster changes rarely, so an immediate retry there
	// wins, but a popular message draws many writers to one row at once, and a
	// writer that retries immediately keeps landing behind the next winner until
	// its budget is gone. The ceiling is what one aggregate's transactional
	// throughput allows; an emoji whose reaction rate exceeds it is the signal to
	// revisit.
	maxReactionAttempts = 16
	reactionBackoffBase = 2 * time.Millisecond
	reactionBackoffMax  = 100 * time.Millisecond
)

type store struct {
	client             *dynamodb.Client
	messagesTable      string
	pointersTable      string
	reactionsTable     string
	reactorsTable      string
	selfReactionsTable string

	// beforeReactionWrite, when set, runs between a reaction write's state read
	// and its transaction. Tests use it to land a concurrent write in that gap;
	// it is nil in production.
	beforeReactionWrite func()
}

// NewInDynamoDB returns a messaging.Store backed by the given DynamoDB tables.
// Use CreateTables to provision them.
func NewInDynamoDB(client *dynamodb.Client, messagesTable, pointersTable, reactionsTable, reactorsTable, selfReactionsTable string) messaging.Store {
	return &store{
		client:             client,
		messagesTable:      messagesTable,
		pointersTable:      pointersTable,
		reactionsTable:     reactionsTable,
		reactorsTable:      reactorsTable,
		selfReactionsTable: selfReactionsTable,
	}
}

func (s *store) PutMessage(
	ctx context.Context,
	chatID *commonpb.ChatId,
	senderID *commonpb.UserId,
	content []*messagingpb.Content,
	ts time.Time,
	clientMessageID *messagingpb.ClientMessageId,
	countsTowardUnread bool,
) (*messaging.Message, bool, error) {
	contentBlobs, err := marshalContent(content)
	if err != nil {
		return nil, false, err
	}

	for attempt := 0; attempt < maxPutMessageAttempts; attempt++ {
		// The idempotency marker and the sequence counter live in the same
		// partition (pk = chat#<id>), so one consistent batch read fetches both.
		markerSeq, lastSeq, lastUnread, lastEventSeq, err := s.readSendState(ctx, chatID, clientMessageID)
		if err != nil {
			return nil, false, err
		}
		// Fast idempotent path: a prior send with this client message ID wins. The
		// marker was just read strongly-consistent, so the message it points at is
		// committed; read it back strongly-consistent too, else a lagging replica
		// could spuriously return ErrMessageNotFound (or stale content) for a message
		// that provably exists.
		if markerSeq != nil {
			msg, err := s.getMessage(ctx, chatID, &messagingpb.MessageId{Value: *markerSeq}, true)
			return msg, false, err
		}

		nextSeq := lastSeq + 1
		nextUnread := lastUnread
		if countsTowardUnread {
			nextUnread++
		}
		// event_seq is assigned from the event-log head. While every event is a new
		// message it advances in lockstep with the message ID (nextEventSeq ==
		// nextSeq); edits and deletes will advance it without minting a seq.
		nextEventSeq := lastEventSeq + 1

		msg := &messaging.Message{
			ChatID:        &commonpb.ChatId{Value: append([]byte(nil), chatID.Value...)},
			ID:            &messagingpb.MessageId{Value: nextSeq},
			SenderID:      senderID,
			Content:       content,
			Timestamp:     ts,
			UnreadSeq:     nextUnread,
			EventSequence: nextEventSeq,
		}

		_, err = s.client.TransactWriteItems(ctx, &dynamodb.TransactWriteItemsInput{
			TransactItems: []types.TransactWriteItem{
				// [0] advance both heads under an optimistic lock on both, so the
				// whole transaction rolls back together — no leaked sequence numbers.
				// Locking on last_event_seq (not just last_seq) is what lets edits and
				// deletes — which advance only the event-log head — serialize against
				// sends. While every event is a new message the two heads stay equal.
				{Update: &types.Update{
					TableName: aws.String(s.messagesTable),
					Key: map[string]types.AttributeValue{
						attrPK: avS(chatPK(chatID)),
						attrSK: avS(skCounter),
					},
					UpdateExpression:    aws.String(fmt.Sprintf("SET %s = :nextSeq, %s = :nextUnread, %s = :nextEventSeq", attrLastSeq, attrLastUnreadSeq, attrLastEventSeq)),
					ConditionExpression: aws.String(fmt.Sprintf("attribute_not_exists(%s) OR (%s = :expectedSeq AND %s = :expectedEventSeq)", attrPK, attrLastSeq, attrLastEventSeq)),
					ExpressionAttributeValues: map[string]types.AttributeValue{
						":nextSeq":          avN(nextSeq),
						":nextUnread":       avN(nextUnread),
						":nextEventSeq":     avN(nextEventSeq),
						":expectedSeq":      avN(lastSeq),
						":expectedEventSeq": avN(lastEventSeq),
					},
				}},
				// [1] the message's current materialized state, read by ID for
				// history and carrying its current event_seq.
				{Put: &types.Put{
					TableName:           aws.String(s.messagesTable),
					Item:                s.messageItem(msg, contentBlobs),
					ConditionExpression: aws.String(fmt.Sprintf("attribute_not_exists(%s)", attrPK)),
				}},
				// [2] the idempotency marker. It is transient — only the message,
				// counter, and event-log entry are permanent — so it carries a TTL
				// for auto-reaping.
				{Put: &types.Put{
					TableName: aws.String(s.messagesTable),
					Item: map[string]types.AttributeValue{
						attrPK:        avS(chatPK(chatID)),
						attrSK:        avS(cmidSK(clientMessageID)),
						attrSeq:       avN(nextSeq),
						attrExpiresAt: avN(uint64(ts.Add(cmidTTL).Unix())),
					},
					ConditionExpression: aws.String(fmt.Sprintf("attribute_not_exists(%s)", attrPK)),
				}},
				// [3] the append-only event-log entry for this send: a thin descriptor
				// keyed by its event_seq, read as a gapless event-ordered range for
				// delta catch-up. Minted under the same counter lock, so its event_seq
				// is unique.
				{Put: &types.Put{
					TableName:           aws.String(s.messagesTable),
					Item:                s.eventItem(chatID, msg.EventSequence, msg.ID.Value, messaging.EventTypeMessageSent, msg.Timestamp),
					ConditionExpression: aws.String(fmt.Sprintf("attribute_not_exists(%s)", attrPK)),
				}},
			},
		})
		if err == nil {
			return msg, true, nil
		}

		reasons, ok := cancellationReasons(err)
		if !ok {
			return nil, false, err
		}
		// reasons index matches TransactItems order: [0]=counter, [1]=message,
		// [2]=idempotency marker, [3]=event-log entry.
		if len(reasons) == 4 && reasons[2] == codeConditionalCheckFailed {
			// A concurrent identical send already persisted; re-read and return it.
			continue
		}
		if isRetryable(reasons) {
			continue
		}
		return nil, false, err
	}
	return nil, false, fmt.Errorf("put message exhausted retries for chat %s", hex.EncodeToString(chatID.Value))
}

func (s *store) EditMessage(
	ctx context.Context,
	chatID *commonpb.ChatId,
	messageID *messagingpb.MessageId,
	content []*messagingpb.Content,
	editedTs time.Time,
	expectedEventSeq uint64,
) (*messaging.Message, error) {
	contentBlobs, err := marshalContent(content)
	if err != nil {
		return nil, err
	}

	for attempt := 0; attempt < maxPutMessageAttempts; attempt++ {
		// The event-log head lives on the counter row. The edit advances it by one
		// without touching last_seq (no new message ID), so event_seq diverges from
		// the message ID here. Read it strongly-consistent, then lock on it below.
		head, err := s.lastEventSeq(ctx, chatID)
		if err != nil {
			return nil, err
		}
		if head == 0 {
			return nil, messaging.ErrMessageNotFound // no counter → no messages
		}
		newEventSeq := head + 1

		_, err = s.client.TransactWriteItems(ctx, &dynamodb.TransactWriteItemsInput{
			TransactItems: []types.TransactWriteItem{
				// [0] advance the event-log head under an optimistic lock, serializing
				// this edit against concurrent sends and other edits/deletes (all of
				// which advance last_event_seq). last_seq is deliberately left alone.
				{Update: &types.Update{
					TableName:           aws.String(s.messagesTable),
					Key:                 map[string]types.AttributeValue{attrPK: avS(chatPK(chatID)), attrSK: avS(skCounter)},
					UpdateExpression:    aws.String(fmt.Sprintf("SET %s = :newEventSeq", attrLastEventSeq)),
					ConditionExpression: aws.String(fmt.Sprintf("%s = :head", attrLastEventSeq)),
					ExpressionAttributeValues: map[string]types.AttributeValue{
						":newEventSeq": avN(newEventSeq),
						":head":        avN(head),
					},
				}},
				// [1] replace the content, stamp the edit time, and re-stamp the
				// message's event_seq to the new head (its current-state token), guarded
				// on the caller's expected event_sequence — a stale expectation is a
				// CONFLICT, never a clobber. ALL_OLD lets us tell a missing message from
				// a stale one without a second read.
				{Update: &types.Update{
					TableName:           aws.String(s.messagesTable),
					Key:                 map[string]types.AttributeValue{attrPK: avS(chatPK(chatID)), attrSK: avS(msgSK(messageID.Value))},
					UpdateExpression:    aws.String(fmt.Sprintf("SET %s = :content, %s = :newEventSeq, %s = :editedTs", attrContent, attrEventSeq, attrLastEditedTs)),
					ConditionExpression: aws.String(fmt.Sprintf("attribute_exists(%s) AND %s = :expected", attrPK, attrEventSeq)),
					ExpressionAttributeValues: map[string]types.AttributeValue{
						":content":     &types.AttributeValueMemberL{Value: contentBlobs},
						":newEventSeq": avN(newEventSeq),
						":editedTs":    avN(uint64(editedTs.UnixNano())),
						":expected":    avN(expectedEventSeq),
					},
					ReturnValuesOnConditionCheckFailure: types.ReturnValuesOnConditionCheckFailureAllOld,
				}},
				// [2] append the edit to the event log (evt#<newEventSeq>) so the
				// catch-up read (GetEventDelta) joins and surfaces it. Minted under the
				// same counter lock as [0], so its event_seq is unique.
				{Put: &types.Put{
					TableName:           aws.String(s.messagesTable),
					Item:                s.eventItem(chatID, newEventSeq, messageID.Value, messaging.EventTypeMessageEdited, editedTs),
					ConditionExpression: aws.String(fmt.Sprintf("attribute_not_exists(%s)", attrPK)),
				}},
			},
		})
		if err == nil {
			// Re-read strongly-consistent: an eventually-consistent read right after
			// the commit can still return the pre-edit content/event_seq, which would
			// surface stale state to the caller and to the broadcast.
			return s.getMessage(ctx, chatID, messageID, true) // canonical edited state
		}

		var tce *types.TransactionCanceledException
		if !errors.As(err, &tce) || len(tce.CancellationReasons) != 3 {
			return nil, err
		}
		// reasons index matches TransactItems order: [0]=counter, [1]=message,
		// [2]=event-log entry.
		counterReason, msgReason, eventReason := tce.CancellationReasons[0], tce.CancellationReasons[1], tce.CancellationReasons[2]

		// A failed message condition is terminal: either the message is gone or the
		// caller's expected event_sequence is stale. Distinguish via the ALL_OLD item
		// carried on the cancellation reason.
		if aws.ToString(msgReason.Code) == codeConditionalCheckFailed {
			if len(msgReason.Item) == 0 {
				return nil, messaging.ErrMessageNotFound
			}
			current, err := messageFromItem(chatID, msgReason.Item)
			if err != nil {
				return nil, err
			}
			return current, messaging.ErrEventSequenceConflict
		}
		// The head moved under us (a concurrent send/edit/delete that didn't touch this
		// message — which fails the counter lock and, in lockstep, the evt# guard), or
		// a transient transaction conflict: re-read the head and retry.
		if isRetryable([]string{aws.ToString(counterReason.Code), aws.ToString(msgReason.Code), aws.ToString(eventReason.Code)}) {
			continue
		}
		return nil, err
	}
	return nil, fmt.Errorf("edit message exhausted retries for chat %s", hex.EncodeToString(chatID.Value))
}

func (s *store) DeleteMessage(
	ctx context.Context,
	chatID *commonpb.ChatId,
	messageID *messagingpb.MessageId,
	deletedBy *commonpb.UserId,
	deletedTs time.Time,
	expectedEventSeq uint64,
) (*messaging.Message, error) {
	// The tombstone is a single DeletedContent that replaces the message's content.
	// message_id, unread_seq and ts are left untouched.
	deleted := &messagingpb.DeletedContent{DeletedTs: timestamppb.New(deletedTs)}
	if deletedBy != nil {
		deleted.DeletedBy = &commonpb.UserId{Value: append([]byte(nil), deletedBy.Value...)}
	}
	contentBlobs, err := marshalContent([]*messagingpb.Content{{
		Type: &messagingpb.Content_Deleted{Deleted: deleted},
	}})
	if err != nil {
		return nil, err
	}

	for attempt := 0; attempt < maxPutMessageAttempts; attempt++ {
		// The event-log head lives on the counter row. The delete advances it by one
		// without touching last_seq (no new message ID), so event_seq diverges from
		// the message ID here. Read it strongly-consistent, then lock on it below.
		head, err := s.lastEventSeq(ctx, chatID)
		if err != nil {
			return nil, err
		}
		if head == 0 {
			return nil, messaging.ErrMessageNotFound // no counter → no messages
		}
		newEventSeq := head + 1

		_, err = s.client.TransactWriteItems(ctx, &dynamodb.TransactWriteItemsInput{
			TransactItems: []types.TransactWriteItem{
				// [0] advance the event-log head under an optimistic lock, serializing
				// this delete against concurrent sends and other edits/deletes (all of
				// which advance last_event_seq). last_seq is deliberately left alone.
				{Update: &types.Update{
					TableName:           aws.String(s.messagesTable),
					Key:                 map[string]types.AttributeValue{attrPK: avS(chatPK(chatID)), attrSK: avS(skCounter)},
					UpdateExpression:    aws.String(fmt.Sprintf("SET %s = :newEventSeq", attrLastEventSeq)),
					ConditionExpression: aws.String(fmt.Sprintf("%s = :head", attrLastEventSeq)),
					ExpressionAttributeValues: map[string]types.AttributeValue{
						":newEventSeq": avN(newEventSeq),
						":head":        avN(head),
					},
				}},
				// [1] tombstone the message and re-stamp its event_seq to the new head
				// (its current-state token), guarded on the caller's expected
				// event_sequence — a stale expectation is a CONFLICT, never a clobber.
				// ALL_OLD lets us tell a missing message from a stale one without a
				// second read.
				{Update: &types.Update{
					TableName:           aws.String(s.messagesTable),
					Key:                 map[string]types.AttributeValue{attrPK: avS(chatPK(chatID)), attrSK: avS(msgSK(messageID.Value))},
					UpdateExpression:    aws.String(fmt.Sprintf("SET %s = :content, %s = :newEventSeq", attrContent, attrEventSeq)),
					ConditionExpression: aws.String(fmt.Sprintf("attribute_exists(%s) AND %s = :expected", attrPK, attrEventSeq)),
					ExpressionAttributeValues: map[string]types.AttributeValue{
						":content":     &types.AttributeValueMemberL{Value: contentBlobs},
						":newEventSeq": avN(newEventSeq),
						":expected":    avN(expectedEventSeq),
					},
					ReturnValuesOnConditionCheckFailure: types.ReturnValuesOnConditionCheckFailureAllOld,
				}},
				// [2] append the delete to the event log (evt#<newEventSeq>) so the
				// catch-up read (GetEventDelta) joins and surfaces it. Minted under the
				// same counter lock as [0], so its event_seq is unique.
				{Put: &types.Put{
					TableName:           aws.String(s.messagesTable),
					Item:                s.eventItem(chatID, newEventSeq, messageID.Value, messaging.EventTypeMessageDeleted, deletedTs),
					ConditionExpression: aws.String(fmt.Sprintf("attribute_not_exists(%s)", attrPK)),
				}},
			},
		})
		if err == nil {
			// Re-read strongly-consistent: an eventually-consistent read right after
			// the commit can still return the pre-delete (non-tombstone) state, which
			// would surface stale content to the caller and make NewMessageDeletedEvent
			// dereference a Deleted that isn't there.
			return s.getMessage(ctx, chatID, messageID, true) // canonical tombstone
		}

		var tce *types.TransactionCanceledException
		if !errors.As(err, &tce) || len(tce.CancellationReasons) != 3 {
			return nil, err
		}
		// reasons index matches TransactItems order: [0]=counter, [1]=message,
		// [2]=event-log entry.
		counterReason, msgReason, eventReason := tce.CancellationReasons[0], tce.CancellationReasons[1], tce.CancellationReasons[2]

		// A failed message condition is terminal: either the message is gone or the
		// caller's expected event_sequence is stale. Distinguish via the ALL_OLD item
		// carried on the cancellation reason.
		if aws.ToString(msgReason.Code) == codeConditionalCheckFailed {
			if len(msgReason.Item) == 0 {
				return nil, messaging.ErrMessageNotFound
			}
			current, err := messageFromItem(chatID, msgReason.Item)
			if err != nil {
				return nil, err
			}
			return current, messaging.ErrEventSequenceConflict
		}
		// The head moved under us (a concurrent send/delete that didn't touch this
		// message — which fails the counter lock and, in lockstep, the evt# guard),
		// or a transient transaction conflict: re-read the head and retry.
		if isRetryable([]string{aws.ToString(counterReason.Code), aws.ToString(msgReason.Code), aws.ToString(eventReason.Code)}) {
			continue
		}
		return nil, err
	}
	return nil, fmt.Errorf("delete message exhausted retries for chat %s", hex.EncodeToString(chatID.Value))
}

func (s *store) GetLatestEventSequence(ctx context.Context, chatID *commonpb.ChatId) (uint64, error) {
	// The event-log head is last_event_seq on the counter row. While every event
	// is a new message it equals the message-ID head; edits and deletes advance it
	// independently.
	return s.lastEventSeq(ctx, chatID)
}

// lastEventSeq returns the chat's event-log head (last_event_seq) from the counter
// row, or 0 when the chat has no messages yet. A counter present but missing
// last_event_seq is a migration gap and surfaces as an error (via parseN).
func (s *store) lastEventSeq(ctx context.Context, chatID *commonpb.ChatId) (uint64, error) {
	out, err := s.client.GetItem(ctx, &dynamodb.GetItemInput{
		TableName:            aws.String(s.messagesTable),
		Key:                  map[string]types.AttributeValue{attrPK: avS(chatPK(chatID)), attrSK: avS(skCounter)},
		ProjectionExpression: aws.String(attrLastEventSeq),
		ConsistentRead:       aws.Bool(true),
	})
	if err != nil {
		return 0, err
	}
	if len(out.Item) == 0 {
		return 0, nil // no counter yet → no messages
	}
	return parseN(out.Item[attrLastEventSeq])
}

func (s *store) GetLatestEventSequencesForChats(ctx context.Context, chatIDs []*commonpb.ChatId) (map[string]uint64, error) {
	// The head lives on each chat's counter row (last_event_seq). The rows are
	// addressable by exact key (chatPK, #counter), so batch-read them in one path,
	// mirroring GetMessagesByRefs — no per-chat round trip. Dedup so a repeated
	// chat ID collapses.
	seen := make(map[string]struct{}, len(chatIDs))
	var keys []map[string]types.AttributeValue
	for _, chatID := range chatIDs {
		k := string(chatID.Value)
		if _, dup := seen[k]; dup {
			continue
		}
		seen[k] = struct{}{}
		keys = append(keys, map[string]types.AttributeValue{
			attrPK: avS(chatPK(chatID)),
			attrSK: avS(skCounter),
		})
	}

	out := make(map[string]uint64)
	for start := 0; start < len(keys); start += maxBatchGetKeys {
		end := start + maxBatchGetKeys
		if end > len(keys) {
			end = len(keys)
		}

		// Retry UnprocessedKeys until the batch drains.
		req := map[string]types.KeysAndAttributes{
			s.messagesTable: {
				Keys:                 keys[start:end],
				ProjectionExpression: aws.String(strings.Join([]string{attrPK, attrLastEventSeq}, ", ")),
			},
		}
		for len(req[s.messagesTable].Keys) > 0 {
			resp, err := s.client.BatchGetItem(ctx, &dynamodb.BatchGetItemInput{RequestItems: req})
			if err != nil {
				return nil, err
			}
			for _, item := range resp.Responses[s.messagesTable] {
				// Items come back unordered and intermixed across chats; the owning
				// chat is recovered from the item's pk.
				seq, err := parseN(item[attrLastEventSeq])
				if err != nil {
					return nil, err
				}
				// A head of 0 is the proto default; leave it out so callers treat a
				// missing key and an explicit 0 identically.
				if seq == 0 {
					continue
				}
				out[string(chatIDFromPK(item).Value)] = seq
			}
			if unprocessed, ok := resp.UnprocessedKeys[s.messagesTable]; ok && len(unprocessed.Keys) > 0 {
				req = map[string]types.KeysAndAttributes{s.messagesTable: unprocessed}
			} else {
				break
			}
		}
	}
	return out, nil
}

// GetEventDelta reads the event log in (afterEventSeq, headEventSeq], joins each
// event to its message's current state, and drops events superseded by a later one
// — see the Store interface for the full contract.
func (s *store) GetEventDelta(ctx context.Context, chatID *commonpb.ChatId, afterEventSeq, headEventSeq uint64, limit int) ([]*messaging.Message, uint64, error) {
	if limit <= 0 {
		limit = database.DefaultQueryOptions().Limit
	}
	if afterEventSeq >= headEventSeq {
		return nil, afterEventSeq, nil
	}

	// 1. Read the thin event descriptors in (after, head], ascending. BETWEEN bounds
	//    the scan to the evt# prefix and stops at head. The sort key encodes event_seq
	//    zero-padded, so lexicographic order is numeric order. The range is in the
	//    chat's own partition, so — unlike the eventually-consistent GSI this replaced
	//    — it is read strongly-consistent and gapless: no transient holes for the
	//    cursor to skip, no out-of-order propagation between events.
	out, err := s.client.Query(ctx, &dynamodb.QueryInput{
		TableName:              aws.String(s.messagesTable),
		KeyConditionExpression: aws.String(fmt.Sprintf("%s = :pk AND %s BETWEEN :from AND :to", attrPK, attrSK)),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":pk":   avS(chatPK(chatID)),
			":from": avS(evtSK(afterEventSeq + 1)),
			":to":   avS(evtSK(headEventSeq)),
		},
		ScanIndexForward: aws.Bool(true), // ascending by event_seq
		Limit:            aws.Int32(int32(limit)),
		ConsistentRead:   aws.Bool(true),
	})
	if err != nil {
		return nil, 0, err
	}
	if len(out.Items) == 0 {
		return nil, afterEventSeq, nil
	}

	type descriptor struct{ eventSeq, messageID uint64 }
	descriptors := make([]descriptor, 0, len(out.Items))
	ids := make(map[uint64]struct{}, len(out.Items))
	for _, item := range out.Items {
		eventSeq, err := eventSeqFromEvtSK(asS(item[attrSK]))
		if err != nil {
			return nil, 0, err
		}
		messageID, err := parseN(item[attrMessageID])
		if err != nil {
			return nil, 0, err
		}
		descriptors = append(descriptors, descriptor{eventSeq, messageID})
		ids[messageID] = struct{}{}
	}
	// The cursor advances over every event scanned, survivor or not, so a wholly
	// superseded page still makes progress rather than re-reading.
	nextCursor := descriptors[len(descriptors)-1].eventSeq

	// 2. Join each referenced message to its current materialized state.
	current, err := s.getMessagesByIDs(ctx, chatID, ids)
	if err != nil {
		return nil, 0, err
	}

	// 3. Emit in event order, dropping a superseded event: when the message's current
	//    event_sequence is past this event's, a newer event (later here, or — if past
	//    head — on the live stream) carries the up-to-date state, so this entry is
	//    stale. The survivor of each message is the one whose event_seq equals the
	//    message's current event_sequence, so each message appears at most once.
	msgs := make([]*messaging.Message, 0, len(descriptors))
	for _, d := range descriptors {
		cur := current[d.messageID]
		if cur == nil || cur.EventSequence > d.eventSeq {
			continue
		}
		msgs = append(msgs, cur)
	}
	return msgs, nextCursor, nil
}

// getMessagesByIDs fetches the current state of the given message IDs within one
// chat in a single strongly-consistent batch read, keyed by message ID. Missing
// IDs are simply absent from the map. It backs GetEventDelta's join.
func (s *store) getMessagesByIDs(ctx context.Context, chatID *commonpb.ChatId, ids map[uint64]struct{}) (map[uint64]*messaging.Message, error) {
	keys := make([]map[string]types.AttributeValue, 0, len(ids))
	for id := range ids {
		keys = append(keys, map[string]types.AttributeValue{
			attrPK: avS(chatPK(chatID)),
			attrSK: avS(msgSK(id)),
		})
	}

	out := make(map[uint64]*messaging.Message, len(ids))
	for start := 0; start < len(keys); start += maxBatchGetKeys {
		end := start + maxBatchGetKeys
		if end > len(keys) {
			end = len(keys)
		}
		req := map[string]types.KeysAndAttributes{
			s.messagesTable: {Keys: keys[start:end], ConsistentRead: aws.Bool(true)},
		}
		for len(req[s.messagesTable].Keys) > 0 {
			resp, err := s.client.BatchGetItem(ctx, &dynamodb.BatchGetItemInput{RequestItems: req})
			if err != nil {
				return nil, err
			}
			for _, item := range resp.Responses[s.messagesTable] {
				msg, err := messageFromItem(chatID, item)
				if err != nil {
					return nil, err
				}
				out[msg.ID.Value] = msg
			}
			if unprocessed, ok := resp.UnprocessedKeys[s.messagesTable]; ok && len(unprocessed.Keys) > 0 {
				req = map[string]types.KeysAndAttributes{s.messagesTable: unprocessed}
			} else {
				break
			}
		}
	}
	return out, nil
}

func (s *store) GetMessage(ctx context.Context, chatID *commonpb.ChatId, messageID *messagingpb.MessageId) (*messaging.Message, error) {
	return s.getMessage(ctx, chatID, messageID, false)
}

// getMessage reads a single message by ID, or ErrMessageNotFound. consistent
// forces a strongly-consistent read; callers reading a message back immediately
// after mutating it (e.g. the delete tombstone) must set it, since an
// eventually-consistent read can still return the pre-mutation state.
func (s *store) getMessage(ctx context.Context, chatID *commonpb.ChatId, messageID *messagingpb.MessageId, consistent bool) (*messaging.Message, error) {
	out, err := s.client.GetItem(ctx, &dynamodb.GetItemInput{
		TableName: aws.String(s.messagesTable),
		Key: map[string]types.AttributeValue{
			attrPK: avS(chatPK(chatID)),
			attrSK: avS(msgSK(messageID.Value)),
		},
		ConsistentRead: aws.Bool(consistent),
	})
	if err != nil {
		return nil, err
	}
	if len(out.Item) == 0 {
		return nil, messaging.ErrMessageNotFound
	}
	return messageFromItem(chatID, out.Item)
}

func (s *store) MessageExists(ctx context.Context, chatID *commonpb.ChatId, messageID *messagingpb.MessageId) (bool, error) {
	// Project to pk only so the content blobs are never read or decoded. The
	// read is strong: the message being asked about was, as often as not, just
	// delivered on the stream (see the Store contract).
	out, err := s.client.GetItem(ctx, &dynamodb.GetItemInput{
		TableName:            aws.String(s.messagesTable),
		Key:                  map[string]types.AttributeValue{attrPK: avS(chatPK(chatID)), attrSK: avS(msgSK(messageID.Value))},
		ProjectionExpression: aws.String(attrPK),
		ConsistentRead:       aws.Bool(true),
	})
	if err != nil {
		return false, err
	}
	return len(out.Item) > 0, nil
}

func (s *store) GetMessages(ctx context.Context, chatID *commonpb.ChatId, opts ...database.QueryOption) ([]*messaging.Message, error) {
	q := database.ApplyQueryOptions(opts...)

	input := &dynamodb.QueryInput{
		TableName:              aws.String(s.messagesTable),
		KeyConditionExpression: aws.String(fmt.Sprintf("%s = :pk AND begins_with(%s, :prefix)", attrPK, attrSK)),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":pk":     avS(chatPK(chatID)),
			":prefix": avS(msgPrefix),
		},
		ScanIndexForward: aws.Bool(q.Order != commonpb.QueryOptions_DESC),
	}
	if q.Limit > 0 {
		input.Limit = aws.Int32(int32(q.Limit))
	}
	if cursor, ok := messaging.IDFromPageToken(q.PagingToken); ok {
		input.ExclusiveStartKey = map[string]types.AttributeValue{
			attrPK: avS(chatPK(chatID)),
			attrSK: avS(msgSK(cursor)),
		}
	}

	out, err := s.client.Query(ctx, input)
	if err != nil {
		return nil, err
	}
	messages := make([]*messaging.Message, 0, len(out.Items))
	for _, item := range out.Items {
		msg, err := messageFromItem(chatID, item)
		if err != nil {
			return nil, err
		}
		messages = append(messages, msg)
	}
	return messages, nil
}

func (s *store) GetMessagesByRefs(ctx context.Context, refs []messaging.MessageRef) ([]*messaging.Message, error) {
	// Dedup and build the batch keys. Keys may span partitions (chats);
	// BatchGetItem handles a mixed set in one request.
	type dedupKey struct {
		chat string
		id   uint64
	}
	seen := make(map[dedupKey]struct{}, len(refs))
	var keys []map[string]types.AttributeValue
	for _, ref := range refs {
		k := dedupKey{chat: string(ref.ChatID.Value), id: ref.MessageID.Value}
		if _, dup := seen[k]; dup {
			continue
		}
		seen[k] = struct{}{}
		keys = append(keys, map[string]types.AttributeValue{
			attrPK: avS(chatPK(ref.ChatID)),
			attrSK: avS(msgSK(ref.MessageID.Value)),
		})
	}

	var out []*messaging.Message
	for start := 0; start < len(keys); start += maxBatchGetKeys {
		end := start + maxBatchGetKeys
		if end > len(keys) {
			end = len(keys)
		}

		// Retry UnprocessedKeys until the batch drains.
		req := map[string]types.KeysAndAttributes{
			s.messagesTable: {Keys: keys[start:end]},
		}
		for len(req[s.messagesTable].Keys) > 0 {
			resp, err := s.client.BatchGetItem(ctx, &dynamodb.BatchGetItemInput{RequestItems: req})
			if err != nil {
				return nil, err
			}
			for _, item := range resp.Responses[s.messagesTable] {
				// Items come back unordered and intermixed across chats, so the
				// owning chat is recovered from each item's pk rather than a param.
				msg, err := messageFromItem(chatIDFromPK(item), item)
				if err != nil {
					return nil, err
				}
				out = append(out, msg)
			}
			if unprocessed, ok := resp.UnprocessedKeys[s.messagesTable]; ok && len(unprocessed.Keys) > 0 {
				req = map[string]types.KeysAndAttributes{s.messagesTable: unprocessed}
			} else {
				break
			}
		}
	}

	// Order by (chatID, message ID): deterministic, and ascending by ID within a
	// single chat to match the single-chat batch contract.
	sort.Slice(out, func(i, j int) bool {
		if c := bytes.Compare(out[i].ChatID.Value, out[j].ChatID.Value); c != 0 {
			return c < 0
		}
		return out[i].ID.Value < out[j].ID.Value
	})
	return out, nil
}

func (s *store) GetPointers(ctx context.Context, chatID *commonpb.ChatId) ([]*messagingpb.Pointer, error) {
	if !chat.IsGroupChatID(chatID) {
		// A DM's pointers are one item: a point read.
		out, err := s.client.GetItem(ctx, &dynamodb.GetItemInput{
			TableName: aws.String(s.pointersTable),
			Key: map[string]types.AttributeValue{
				attrPK: avS(chatPK(chatID)),
				attrSK: avS(skDmPointers),
			},
		})
		if err != nil {
			return nil, err
		}
		return dmPointersFromItem(out.Item), nil
	}

	// Bound the query to the ptr# range: pointers are all a group's partition
	// holds today, and the bound keeps that true of the read if that ever
	// changes. sk order is member order, so the result is deterministic.
	var pointers []*messagingpb.Pointer
	var startKey map[string]types.AttributeValue
	for {
		out, err := s.client.Query(ctx, &dynamodb.QueryInput{
			TableName:              aws.String(s.pointersTable),
			KeyConditionExpression: aws.String(fmt.Sprintf("%s = :pk AND begins_with(%s, :prefix)", attrPK, attrSK)),
			ExpressionAttributeValues: map[string]types.AttributeValue{
				":pk":     avS(chatPK(chatID)),
				":prefix": avS(ptrPrefix),
			},
			ExclusiveStartKey: startKey,
		})
		if err != nil {
			return nil, err
		}
		for _, item := range out.Items {
			pointers = append(pointers, memberPointersFromItem(item)...)
		}
		if len(out.LastEvaluatedKey) == 0 {
			break
		}
		startKey = out.LastEvaluatedKey
	}
	return pointers, nil
}

func (s *store) GetPointersForChats(ctx context.Context, refs []messaging.PointerRef) (map[string][]*messagingpb.Pointer, error) {
	// The refs name the members, so every item's key is known up front:
	// enumerate them and batch-read in one path, mirroring GetMessagesByRefs —
	// no per-chat partition scan. A group costs one key per named member; a DM
	// costs one key per chat, its single #ptrs item carrying both members. Dedup
	// so a repeated (chat, member) pair, or a repeated DM, collapses.
	type pairKey struct {
		chat string
		user string
	}
	seenPairs := make(map[pairKey]struct{})
	// The members each DM ref asked for: the #ptrs item carries both members,
	// but only the named ones are returned, honoring the contract.
	dmMembers := make(map[string]map[string]struct{})
	var keys []map[string]types.AttributeValue
	for _, ref := range refs {
		chatKey := string(ref.ChatID.Value)
		isGroup := chat.IsGroupChatID(ref.ChatID)
		if !isGroup {
			if _, dup := dmMembers[chatKey]; !dup {
				dmMembers[chatKey] = make(map[string]struct{})
				keys = append(keys, map[string]types.AttributeValue{
					attrPK: avS(chatPK(ref.ChatID)),
					attrSK: avS(skDmPointers),
				})
			}
		}
		for _, member := range ref.Members {
			pk := pairKey{chat: chatKey, user: string(member.Value)}
			if _, dup := seenPairs[pk]; dup {
				continue
			}
			seenPairs[pk] = struct{}{}
			if !isGroup {
				dmMembers[chatKey][pk.user] = struct{}{}
				continue
			}
			keys = append(keys, map[string]types.AttributeValue{
				attrPK: avS(chatPK(ref.ChatID)),
				attrSK: avS(pointerSK(member)),
			})
		}
	}

	out := make(map[string][]*messagingpb.Pointer)
	for start := 0; start < len(keys); start += maxBatchGetKeys {
		end := start + maxBatchGetKeys
		if end > len(keys) {
			end = len(keys)
		}

		// Retry UnprocessedKeys until the batch drains.
		req := map[string]types.KeysAndAttributes{
			s.pointersTable: {Keys: keys[start:end]},
		}
		for len(req[s.pointersTable].Keys) > 0 {
			resp, err := s.client.BatchGetItem(ctx, &dynamodb.BatchGetItemInput{RequestItems: req})
			if err != nil {
				return nil, err
			}
			for _, item := range resp.Responses[s.pointersTable] {
				// Items come back unordered and intermixed across chats; the owning
				// chat is recovered from the item's pk (the pointers table stores no
				// chat_id attribute of its own).
				chatKey := string(chatIDFromPK(item).Value)
				if asS(item[attrSK]) != skDmPointers {
					out[chatKey] = append(out[chatKey], memberPointersFromItem(item)...)
					continue
				}
				for _, p := range dmPointersFromItem(item) {
					if _, wanted := dmMembers[chatKey][string(p.UserId.Value)]; wanted {
						out[chatKey] = append(out[chatKey], p)
					}
				}
			}
			if unprocessed, ok := resp.UnprocessedKeys[s.pointersTable]; ok && len(unprocessed.Keys) > 0 {
				req = map[string]types.KeysAndAttributes{s.pointersTable: unprocessed}
			} else {
				break
			}
		}
	}
	return out, nil
}

func (s *store) AdvancePointer(
	ctx context.Context,
	chatID *commonpb.ChatId,
	userID *commonpb.UserId,
	pointerType messagingpb.Pointer_Type,
	newValue *messagingpb.MessageId,
) (*messagingpb.Pointer, bool, error) {
	sk, valAttr, tsAttr, err := pointerKey(chatID, userID, pointerType)
	if err != nil {
		return nil, false, err
	}

	now := time.Now().UTC()
	_, err = s.client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName: aws.String(s.pointersTable),
		Key: map[string]types.AttributeValue{
			attrPK: avS(chatPK(chatID)),
			attrSK: avS(sk),
		},
		// The write names only this member's pair for this type: on a DM's
		// shared item the peer's pairs, and this member's other type, are never
		// touched.
		UpdateExpression: aws.String("SET #val = :v, #ts = :ts"),
		// Monotonic per type: the item may already exist for the member's other
		// type (or, on a DM, for the peer), so the guard is on this pair's value,
		// not the item.
		ConditionExpression: aws.String("attribute_not_exists(#val) OR #val < :v"),
		ExpressionAttributeNames: map[string]string{
			"#val": valAttr,
			"#ts":  tsAttr,
		},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":v":  avN(newValue.Value),
			":ts": avN(uint64(now.UnixNano())),
		},
		// On a no-op (the pointer is already at/past newValue) return the existing
		// item so the caller still gets the current pointer state without a second
		// read.
		ReturnValuesOnConditionCheckFailure: types.ReturnValuesOnConditionCheckFailureAllOld,
	})
	if err != nil {
		var ccf *types.ConditionalCheckFailedException
		if errors.As(err, &ccf) {
			// Not advanced (already at or past newValue); reconstruct from the
			// item that failed the condition. It must carry the pair — the
			// condition can only fail on a stored value — so its absence is an
			// integrity error rather than a nil pointer, which the Store contract
			// returns only alongside an error.
			p := pointerFromAttrs(ccf.Item, userID, pointerType, valAttr, tsAttr)
			if p == nil {
				return nil, false, fmt.Errorf("pointer %s for user %x rejected as a no-op but has no stored value", pointerType, userID.Value)
			}
			return p, false, nil
		}
		return nil, false, err
	}
	return newPointer(pointerType, userID, newValue.Value, now), true, nil
}

// readSendState fetches, in a single consistent batch read, the two partition
// items PutMessage needs before assigning a sequence number: the idempotency
// marker for clientMessageID and the chat's sequence counter. Both share the
// chat's partition (pk = chat#<id>), so one BatchGetItem covers them.
//
// markerSeq is non-nil when a prior send with this client message ID already
// persisted, carrying that message's sequence number; the caller then returns
// the existing message rather than assigning a new one. lastSeq, lastUnread, and
// lastEventSeq are zero when the counter does not yet exist (the chat's first
// send); on an existing counter lastEventSeq is always present (guaranteed by the
// maintain phase plus the one-time backfill).
func (s *store) readSendState(ctx context.Context, chatID *commonpb.ChatId, clientMessageID *messagingpb.ClientMessageId) (markerSeq *uint64, lastSeq, lastUnread, lastEventSeq uint64, err error) {
	cmidSKVal := cmidSK(clientMessageID)
	req := map[string]types.KeysAndAttributes{
		s.messagesTable: {
			Keys: []map[string]types.AttributeValue{
				{attrPK: avS(chatPK(chatID)), attrSK: avS(cmidSKVal)},
				{attrPK: avS(chatPK(chatID)), attrSK: avS(skCounter)},
			},
			ConsistentRead: aws.Bool(true),
		},
	}

	// Drain UnprocessedKeys; values accumulate across iterations, so an item
	// resolved early is retained while a throttled one is retried.
	for len(req[s.messagesTable].Keys) > 0 {
		resp, batchErr := s.client.BatchGetItem(ctx, &dynamodb.BatchGetItemInput{RequestItems: req})
		if batchErr != nil {
			return nil, 0, 0, 0, batchErr
		}
		for _, item := range resp.Responses[s.messagesTable] {
			switch asS(item[attrSK]) {
			case cmidSKVal:
				seq, perr := parseN(item[attrSeq])
				if perr != nil {
					return nil, 0, 0, 0, perr
				}
				markerSeq = &seq
			case skCounter:
				ls, perr := parseN(item[attrLastSeq])
				if perr != nil {
					return nil, 0, 0, 0, perr
				}
				lu, perr := parseN(item[attrLastUnreadSeq])
				if perr != nil {
					return nil, 0, 0, 0, perr
				}
				les, perr := parseN(item[attrLastEventSeq])
				if perr != nil {
					return nil, 0, 0, 0, perr
				}
				lastSeq, lastUnread, lastEventSeq = ls, lu, les
			}
		}
		if unprocessed, ok := resp.UnprocessedKeys[s.messagesTable]; ok && len(unprocessed.Keys) > 0 {
			req = map[string]types.KeysAndAttributes{s.messagesTable: unprocessed}
		} else {
			break
		}
	}
	return markerSeq, lastSeq, lastUnread, lastEventSeq, nil
}

func (s *store) messageItem(msg *messaging.Message, contentBlobs []types.AttributeValue) map[string]types.AttributeValue {
	// The message ID is encoded in the sk (msg#<padded seq>, see seqFromMsgSK),
	// the chat ID is recovered from the pk (see chatIDFromPK), and the client
	// message ID lives on the separate cmid# idempotency marker — so none of the
	// three is duplicated onto the message item.
	item := map[string]types.AttributeValue{
		attrPK:        avS(chatPK(msg.ChatID)),
		attrSK:        avS(msgSK(msg.ID.Value)),
		attrContent:   &types.AttributeValueMemberL{Value: contentBlobs},
		attrTS:        avN(uint64(msg.Timestamp.UnixNano())),
		attrUnreadSeq: avN(msg.UnreadSeq),
		// The event-log sequence at which this message reached its current state,
		// assigned from the chat's event-log head; the client's optimistic-concurrency
		// token. The append-only evt# rows (not this attribute) drive event-ordered
		// reads.
		attrEventSeq: avN(msg.EventSequence),
	}
	if msg.SenderID != nil {
		item[attrSenderID] = avB(msg.SenderID.Value)
	}
	return item
}

// eventItem builds an evt#<event_seq> append-only event-log row: a thin descriptor
// of one event — the message_id it concerns, the event type, and when it happened.
// The event_seq is encoded in the sk (evt#<padded event_seq>, see eventSeqFromEvtSK).
// It deliberately does NOT carry the message body; the read path joins each event to
// the message's current state (see GetEventDelta). The type and ts are recorded so
// the log can later be read or filtered by what happened (e.g. deletions only)
// without that join.
func (s *store) eventItem(chatID *commonpb.ChatId, eventSeq, messageID uint64, eventType messaging.EventType, ts time.Time) map[string]types.AttributeValue {
	return map[string]types.AttributeValue{
		attrPK:        avS(chatPK(chatID)),
		attrSK:        avS(evtSK(eventSeq)),
		attrMessageID: avN(messageID),
		attrEventType: avN(uint64(eventType)),
		attrTS:        avN(uint64(ts.UnixNano())),
	}
}

func messageFromItem(chatID *commonpb.ChatId, item map[string]types.AttributeValue) (*messaging.Message, error) {
	seq, err := seqFromMsgSK(asS(item[attrSK]))
	if err != nil {
		return nil, err
	}
	nanos, err := parseInt(item[attrTS])
	if err != nil {
		return nil, err
	}
	unreadSeq, err := parseN(item[attrUnreadSeq])
	if err != nil {
		return nil, err
	}
	content, err := unmarshalContent(item[attrContent])
	if err != nil {
		return nil, err
	}
	eventSeq, err := parseN(item[attrEventSeq])
	if err != nil {
		return nil, err
	}

	msg := &messaging.Message{
		ChatID:        &commonpb.ChatId{Value: append([]byte(nil), chatID.Value...)},
		ID:            &messagingpb.MessageId{Value: seq},
		Content:       content,
		Timestamp:     time.Unix(0, nanos).UTC(),
		UnreadSeq:     unreadSeq,
		EventSequence: eventSeq,
	}
	if sender := asB(item[attrSenderID]); len(sender) > 0 {
		msg.SenderID = &commonpb.UserId{Value: append([]byte(nil), sender...)}
	}
	// last_edited_ts is absent until the message is edited.
	if _, ok := item[attrLastEditedTs]; ok {
		editedNanos, err := parseInt(item[attrLastEditedTs])
		if err != nil {
			return nil, err
		}
		msg.LastEditedTs = time.Unix(0, editedNanos).UTC()
	}
	return msg, nil
}

// pointerKey locates a member's pointer of type t within the chat's partition:
// the item's sk and the attribute pair on it holding the pointer's value and
// last-advanced timestamp. A group member's pointers are their own ptr#<user>
// item with fixed attribute names; a DM member's share the chat's #ptrs item,
// with the pair keyed by the member.
func pointerKey(chatID *commonpb.ChatId, userID *commonpb.UserId, t messagingpb.Pointer_Type) (sk, valAttr, tsAttr string, err error) {
	if chat.IsGroupChatID(chatID) {
		valAttr, tsAttr, err = memberPointerAttrs(t)
		return pointerSK(userID), valAttr, tsAttr, err
	}
	valAttr, tsAttr, err = dmPointerAttrs(userID, t)
	return skDmPointers, valAttr, tsAttr, err
}

// memberPointerAttrs names the attribute pair on a group's ptr#<user> item that
// holds a type's pointer. Only StoredPointerTypes are persisted; any other type
// is an error rather than a silent write.
func memberPointerAttrs(t messagingpb.Pointer_Type) (valAttr, tsAttr string, err error) {
	switch t {
	case messagingpb.Pointer_DELIVERED:
		return attrDeliveredVal, attrDeliveredTS, nil
	case messagingpb.Pointer_READ:
		return attrReadVal, attrReadTS, nil
	default:
		return "", "", fmt.Errorf("pointer type %s is not stored", t)
	}
}

// dmPointerAttrs names the attribute pair on a DM's #ptrs item that holds a
// member's pointer of a type: the type's value and timestamp prefixes followed
// by the member's hex ID, the inverse of dmPointersFromItem. Only
// StoredPointerTypes are persisted; any other type is an error rather than a
// silent write.
func dmPointerAttrs(userID *commonpb.UserId, t messagingpb.Pointer_Type) (valAttr, tsAttr string, err error) {
	user := hex.EncodeToString(userID.Value)
	switch t {
	case messagingpb.Pointer_DELIVERED:
		return dmDeliveredValPrefix + user, dmDeliveredTSPrefix + user, nil
	case messagingpb.Pointer_READ:
		return dmReadValPrefix + user, dmReadTSPrefix + user, nil
	default:
		return "", "", fmt.Errorf("pointer type %s is not stored", t)
	}
}

// pointerFromAttrs rebuilds userID's pointer of type t from the named value
// and timestamp attributes of item, or nil when item carries no such value.
func pointerFromAttrs(item map[string]types.AttributeValue, userID *commonpb.UserId, t messagingpb.Pointer_Type, valAttr, tsAttr string) *messagingpb.Pointer {
	valAV, ok := item[valAttr]
	if !ok {
		return nil
	}
	value, _ := parseN(valAV)
	return newPointer(t, userID, value, pointerTS(item[tsAttr]))
}

func newPointer(t messagingpb.Pointer_Type, userID *commonpb.UserId, value uint64, ts time.Time) *messagingpb.Pointer {
	return &messagingpb.Pointer{
		Type:   t,
		UserId: &commonpb.UserId{Value: append([]byte(nil), userID.Value...)},
		Value:  &messagingpb.MessageId{Value: value},
		Ts:     timestamppb.New(ts),
	}
}

// userIDFromPointerSK recovers the member from a ptr# item's sk, the inverse
// of pointerSK — the item carries no user attribute of its own, as the
// messages table carries no chat attribute (see chatIDFromPK).
func userIDFromPointerSK(item map[string]types.AttributeValue) *commonpb.UserId {
	id, _ := hex.DecodeString(strings.TrimPrefix(asS(item[attrSK]), ptrPrefix))
	return &commonpb.UserId{Value: id}
}

// memberPointersFromItem rebuilds every pointer a group's ptr#<user> item
// carries.
func memberPointersFromItem(item map[string]types.AttributeValue) []*messagingpb.Pointer {
	userID := userIDFromPointerSK(item)
	pointers := make([]*messagingpb.Pointer, 0, len(messaging.StoredPointerTypes))
	for _, t := range messaging.StoredPointerTypes {
		valAttr, tsAttr, err := memberPointerAttrs(t)
		if err != nil {
			continue
		}
		if p := pointerFromAttrs(item, userID, t, valAttr, tsAttr); p != nil {
			pointers = append(pointers, p)
		}
	}
	return pointers
}

// dmPointersFromItem rebuilds every pointer a DM's #ptrs item carries, for
// every member on it: each value attribute names its member and type by prefix
// (see dmPointerAttrs). The result is ordered by (member, type) so a read is
// deterministic despite the item's attributes being a map. An empty item (the
// DM has no pointers) yields nil.
func dmPointersFromItem(item map[string]types.AttributeValue) []*messagingpb.Pointer {
	var pointers []*messagingpb.Pointer
	for name := range item {
		var t messagingpb.Pointer_Type
		var user, tsPrefix string
		switch {
		case strings.HasPrefix(name, dmDeliveredValPrefix):
			t, user, tsPrefix = messagingpb.Pointer_DELIVERED, strings.TrimPrefix(name, dmDeliveredValPrefix), dmDeliveredTSPrefix
		case strings.HasPrefix(name, dmReadValPrefix):
			t, user, tsPrefix = messagingpb.Pointer_READ, strings.TrimPrefix(name, dmReadValPrefix), dmReadTSPrefix
		default:
			continue
		}
		id, err := hex.DecodeString(user)
		if err != nil {
			continue
		}
		if p := pointerFromAttrs(item, &commonpb.UserId{Value: id}, t, name, tsPrefix+user); p != nil {
			pointers = append(pointers, p)
		}
	}
	sort.Slice(pointers, func(i, j int) bool {
		if c := bytes.Compare(pointers[i].UserId.Value, pointers[j].UserId.Value); c != 0 {
			return c < 0
		}
		return pointers[i].Type < pointers[j].Type
	})
	return pointers
}

// pointerTS decodes a pointer's last-advanced timestamp. Pointers carried over
// from before ts existed have none; default to now() so the required proto
// field is always populated rather than backfilling.
func pointerTS(av types.AttributeValue) time.Time {
	if nanos, err := parseInt(av); err == nil {
		return time.Unix(0, nanos).UTC()
	}
	return time.Now()
}

func marshalContent(content []*messagingpb.Content) ([]types.AttributeValue, error) {
	blobs := make([]types.AttributeValue, len(content))
	for i, c := range content {
		b, err := proto.Marshal(c)
		if err != nil {
			return nil, err
		}
		blobs[i] = avB(b)
	}
	return blobs, nil
}

func unmarshalContent(av types.AttributeValue) ([]*messagingpb.Content, error) {
	list := asL(av)
	content := make([]*messagingpb.Content, len(list))
	for i, blob := range list {
		c := &messagingpb.Content{}
		if err := proto.Unmarshal(asB(blob), c); err != nil {
			return nil, err
		}
		content[i] = c
	}
	return content, nil
}

// chatIDFromPK recovers a chat ID from an item's pk ("chat#<hex>"), the inverse
// of chatPK. Used for items that carry no chat_id attribute (messages fetched
// across partitions, pointers).
func chatIDFromPK(item map[string]types.AttributeValue) *commonpb.ChatId {
	id, _ := hex.DecodeString(strings.TrimPrefix(asS(item[attrPK]), "chat#"))
	return &commonpb.ChatId{Value: id}
}

func chatPK(chatID *commonpb.ChatId) string { return "chat#" + hex.EncodeToString(chatID.Value) }

func msgSK(seq uint64) string { return fmt.Sprintf("%s%0*d", msgPrefix, seqPadWidth, seq) }

func evtSK(eventSeq uint64) string { return fmt.Sprintf("%s%0*d", evtPrefix, seqPadWidth, eventSeq) }

// eventSeqFromEvtSK recovers an event's sequence number from its sk
// ("evt#<padded event_seq>"), the inverse of evtSK. The zero-padding parses cleanly
// as a base-10 integer.
func eventSeqFromEvtSK(sk string) (uint64, error) {
	padded, ok := strings.CutPrefix(sk, evtPrefix)
	if !ok {
		return 0, fmt.Errorf("unexpected event sk %q", sk)
	}
	return strconv.ParseUint(padded, 10, 64)
}

// seqFromMsgSK recovers a message's sequence number from its sk
// ("msg#<padded seq>"), the inverse of msgSK. The zero-padding parses cleanly as
// a base-10 integer.
func seqFromMsgSK(sk string) (uint64, error) {
	padded, ok := strings.CutPrefix(sk, msgPrefix)
	if !ok {
		return 0, fmt.Errorf("unexpected message sk %q", sk)
	}
	return strconv.ParseUint(padded, 10, 64)
}

func cmidSK(clientMessageID *messagingpb.ClientMessageId) string {
	return cmidPrefix + hex.EncodeToString(clientMessageID.Value)
}

// pointerSK keys a group member's pointers item within the chat's partition.
func pointerSK(userID *commonpb.UserId) string {
	return ptrPrefix + hex.EncodeToString(userID.Value)
}

func avS(v string) types.AttributeValue { return &types.AttributeValueMemberS{Value: v} }
func avB(v []byte) types.AttributeValue {
	return &types.AttributeValueMemberB{Value: append([]byte(nil), v...)}
}
func avN(v uint64) types.AttributeValue {
	return &types.AttributeValueMemberN{Value: strconv.FormatUint(v, 10)}
}

func asS(av types.AttributeValue) string {
	if s, ok := av.(*types.AttributeValueMemberS); ok {
		return s.Value
	}
	return ""
}

func asB(av types.AttributeValue) []byte {
	if b, ok := av.(*types.AttributeValueMemberB); ok {
		return b.Value
	}
	return nil
}

func asL(av types.AttributeValue) []types.AttributeValue {
	if l, ok := av.(*types.AttributeValueMemberL); ok {
		return l.Value
	}
	return nil
}

func parseN(av types.AttributeValue) (uint64, error) {
	n, ok := av.(*types.AttributeValueMemberN)
	if !ok {
		return 0, fmt.Errorf("expected number attribute, got %T", av)
	}
	return strconv.ParseUint(n.Value, 10, 64)
}

func parseInt(av types.AttributeValue) (int64, error) {
	n, ok := av.(*types.AttributeValueMemberN)
	if !ok {
		return 0, fmt.Errorf("expected number attribute, got %T", av)
	}
	return strconv.ParseInt(n.Value, 10, 64)
}

// cancellationReasons extracts the per-item cancellation codes from a
// TransactWriteItems error, if it is a TransactionCanceledException.
func cancellationReasons(err error) ([]string, bool) {
	var tce *types.TransactionCanceledException
	if !errors.As(err, &tce) {
		return nil, false
	}
	codes := make([]string, len(tce.CancellationReasons))
	for i, r := range tce.CancellationReasons {
		codes[i] = aws.ToString(r.Code)
	}
	return codes, true
}

func isRetryable(reasons []string) bool {
	for _, code := range reasons {
		if code == codeConditionalCheckFailed || code == codeTransactionConflict {
			return true
		}
	}
	return false
}

// sampleEntry is one retained reactor in an aggregate's sample: the version that
// added them (the ordering key) and when, for display.
type sampleEntry struct {
	version uint64
	ts      time.Time
}

// aggState is an agg# row as read for a write, or the next state a writer
// intends to commit. exists distinguishes a retained count-0 row (which keeps
// its version) from no row at all.
type aggState struct {
	exists  bool
	count   uint64
	version uint64
	sample  map[string]sampleEntry // user hex -> entry, at most MaxStoredSampleReactors
}

// advance is the state after userID reacts: count and version each move by one
// and the reactor joins the sample. The new reactor carries the highest version
// the emoji has seen, so it always belongs in a most-recent sample; when the
// retained set is full the lowest-version entry is evicted to make room.
func (a aggState) advance(userID *commonpb.UserId, ts time.Time) aggState {
	next := aggState{
		exists:  true,
		count:   a.count + 1,
		version: a.version + 1,
		sample:  make(map[string]sampleEntry, len(a.sample)+1),
	}
	for k, v := range a.sample {
		next.sample[k] = v
	}
	if len(next.sample) >= messaging.MaxStoredSampleReactors {
		var evict string
		var lowest uint64
		for k, v := range next.sample {
			if evict == "" || v.version < lowest || (v.version == lowest && k > evict) {
				evict, lowest = k, v.version
			}
		}
		delete(next.sample, evict)
	}
	next.sample[hex.EncodeToString(userID.Value)] = sampleEntry{version: next.version, ts: ts}
	return next
}

// retract is the state after userID's reaction is removed: count down by one,
// version up by one, and the reactor dropped from the sample if present. The
// sample is not backfilled — see messaging.MaxStoredSampleReactors.
func (a aggState) retract(userID *commonpb.UserId) aggState {
	next := aggState{
		exists:  true,
		count:   a.count - 1,
		version: a.version + 1,
		sample:  make(map[string]sampleEntry, len(a.sample)),
	}
	for k, v := range a.sample {
		next.sample[k] = v
	}
	delete(next.sample, hex.EncodeToString(userID.Value))
	return next
}

// metaState is a meta# row as read for a write.
type metaState struct {
	exists bool
	types  uint64
}

// reactionState is everything a reaction write inspects before deciding what to
// commit: the emoji's aggregate, the message's type count, and whether the
// caller already holds a reaction row for the emoji.
type reactionState struct {
	agg     aggState
	meta    metaState
	reacted bool
	// reactedAt is the version that added the caller's reaction, when reacted.
	reactedAt uint64
}

func (s *store) AddReaction(
	ctx context.Context,
	chatID *commonpb.ChatId,
	messageID *messagingpb.MessageId,
	userID *commonpb.UserId,
	emoji string,
	ts time.Time,
) (*messaging.Reaction, bool, bool, error) {
	seq := messageID.Value
	backoff := reactionBackoffBase
	var state reactionState
	needRead := true
	for attempt := 0; ; attempt++ {
		if needRead {
			st, err := s.readReactionState(ctx, chatID, seq, emoji, userID)
			if err != nil {
				return nil, false, false, err
			}
			state, needRead = st, false
		}

		// Idempotent: the user already reacted with this emoji.
		if state.reacted {
			return selfReaction(reactionFromAgg(emoji, state.agg), state.reactedAt), false, false, nil
		}

		// Activating a new or emptied emoji must respect the per-message type cap.
		// The meta row is compare-and-set in the same transaction, so the check is
		// exact: a concurrent activation of a different emoji fails this write's
		// condition and it re-decides against the count that won. Re-adding an
		// active emoji never touches the meta row.
		activating := !state.agg.exists || state.agg.count == 0
		if activating && state.meta.types >= messaging.MaxReactionTypesPerMessage {
			return nil, false, true, nil
		}

		next := state.agg.advance(userID, ts)
		items := []types.TransactWriteItem{
			// [0] the reactor row, conditioned so a concurrent identical add loses
			// and is reported as the idempotent no-op it is.
			{Put: &types.Put{
				TableName: aws.String(s.reactorsTable),
				Item: map[string]types.AttributeValue{
					attrPK:           avS(reactorPK(chatID, seq)),
					attrSK:           avS(userSK(userID, emoji)),
					attrReactedTs:    avN(uint64(ts.UnixNano())),
					attrEmojiVersion: avS(emojiVersionKey(emoji, next.version)),
				},
				ConditionExpression: aws.String(fmt.Sprintf("attribute_not_exists(%s)", attrPK)),
			}},
			// [1] the aggregate, set to exactly next.
			s.aggregateWrite(chatID, seq, emoji, state.agg, next),
		}
		metaIdx := -1
		if activating {
			// The type count, set to exactly one more.
			metaIdx = len(items)
			items = append(items, s.metaWrite(chatID, seq, state.meta, state.meta.types+1))
		}
		if chat.IsGroupChatID(chatID) {
			// The viewer-keyed copy, groups only (see the table comment).
			// Unconditioned: it exists exactly when the reactor row does, and [0]
			// already decides that.
			items = append(items, types.TransactWriteItem{Put: &types.Put{
				TableName: aws.String(s.selfReactionsTable),
				Item: map[string]types.AttributeValue{
					attrPK:          avS(viewerPK(chatID, userID)),
					attrSK:          avS(seqEmojiSK(seq, emoji)),
					attrSelfVersion: avN(next.version),
				},
			}})
		}

		if s.beforeReactionWrite != nil {
			s.beforeReactionWrite()
		}
		_, err := s.client.TransactWriteItems(ctx, &dynamodb.TransactWriteItemsInput{TransactItems: items})
		if err == nil {
			return selfReaction(reactionFromAgg(emoji, next), next.version), true, false, nil
		}

		retry, err := s.absorbReactionFailure(ctx, err, items, metaIdx, &state, attempt, &backoff)
		if err != nil {
			return nil, false, false, fmt.Errorf("add reaction for chat %s: %w", hex.EncodeToString(chatID.Value), err)
		}
		// A non-activating add carries no meta item, so a cancellation refreshes
		// the aggregate but not the meta row. If the refresh shows the emoji emptied
		// under us (its last reactor left), the retry activates it and must
		// decide the cap against a fresh read: the meta row in hand predates that
		// removal and would refuse a slot it just freed.
		if !activating && state.agg.exists && state.agg.count == 0 {
			retry = retryAfterRead
		}
		needRead = retry == retryAfterRead
	}
}

func (s *store) RemoveReaction(
	ctx context.Context,
	chatID *commonpb.ChatId,
	messageID *messagingpb.MessageId,
	userID *commonpb.UserId,
	emoji string,
) (*messaging.Reaction, bool, error) {
	seq := messageID.Value
	backoff := reactionBackoffBase
	var state reactionState
	needRead := true
	for attempt := 0; ; attempt++ {
		if needRead {
			st, err := s.readReactionState(ctx, chatID, seq, emoji, userID)
			if err != nil {
				return nil, false, err
			}
			state, needRead = st, false
		}

		// Idempotent: the user didn't react. Reflect the current state, or report
		// a pure no-op when there's no aggregate at all.
		if !state.reacted {
			if state.agg.exists {
				return reactionFromAgg(emoji, state.agg), false, nil
			}
			return nil, false, nil
		}
		// A reactor row exists only alongside its aggregate, and an active
		// aggregate only alongside its message's meta row counting it: all are
		// written in the reactor's transaction and nothing deletes any of them.
		if !state.agg.exists || state.agg.count == 0 || !state.meta.exists || state.meta.types == 0 {
			return nil, false, fmt.Errorf("chat %s message %d emoji %q has a reactor but no active aggregate", hex.EncodeToString(chatID.Value), seq, emoji)
		}

		next := state.agg.retract(userID)
		items := []types.TransactWriteItem{
			// [0] delete the reactor row; a concurrent identical remove wins the
			// condition and this call reports the no-op.
			{Delete: &types.Delete{
				TableName: aws.String(s.reactorsTable),
				Key: map[string]types.AttributeValue{
					attrPK: avS(reactorPK(chatID, seq)),
					attrSK: avS(userSK(userID, emoji)),
				},
				ConditionExpression: aws.String(fmt.Sprintf("attribute_exists(%s)", attrPK)),
			}},
			// [1] the aggregate, set to exactly next. The row is retained at count
			// 0 so the version survives the emoji being re-added.
			s.aggregateWrite(chatID, seq, emoji, state.agg, next),
		}
		metaIdx := -1
		if next.count == 0 {
			// The last reactor left: the emoji no longer counts toward the cap.
			metaIdx = len(items)
			items = append(items, s.metaWrite(chatID, seq, state.meta, state.meta.types-1))
		}
		if chat.IsGroupChatID(chatID) {
			// The viewer-keyed copy goes with the reactor row (groups only, as
			// on the add).
			items = append(items, types.TransactWriteItem{Delete: &types.Delete{
				TableName: aws.String(s.selfReactionsTable),
				Key: map[string]types.AttributeValue{
					attrPK: avS(viewerPK(chatID, userID)),
					attrSK: avS(seqEmojiSK(seq, emoji)),
				},
			}})
		}

		_, err := s.client.TransactWriteItems(ctx, &dynamodb.TransactWriteItemsInput{TransactItems: items})
		if err == nil {
			return reactionFromAgg(emoji, next), true, nil
		}

		retry, err := s.absorbReactionFailure(ctx, err, items, metaIdx, &state, attempt, &backoff)
		if err != nil {
			return nil, false, fmt.Errorf("remove reaction for chat %s: %w", hex.EncodeToString(chatID.Value), err)
		}
		needRead = retry == retryAfterRead
	}
}

// reactionRetry says how a cancelled reaction transaction is retried.
type reactionRetry int

const (
	// retryFromState retries against the state in hand, refreshed from what the
	// cancellation returned; no read is needed.
	retryFromState reactionRetry = iota
	// retryAfterRead re-reads the state first: either the reactor row's own
	// condition failed, so a concurrent identical write landed and the
	// idempotent answer is whatever it produced, or the transaction collided
	// with another and learned nothing about what it left behind.
	retryAfterRead
)

// absorbReactionFailure classifies a failed reaction transaction and prepares
// the retry, or returns the error when the failure is not one to retry or the
// attempt budget is spent. Cancellation reasons are positional over items: [0]
// is the reactor row, [1] the aggregate, metaIdx (-1 when the write carries
// none) the meta row. Any other item is the viewer-keyed copy, which carries no
// condition and so never fails one.
//
// A failed [0] condition means a concurrent identical add or remove won, so the
// caller re-reads and reports the no-op; that is not a retry of the write, so
// it is taken even when the budget is spent. A failed [1] or meta condition
// means a concurrent writer moved the aggregate or the type count; the failed
// item is returned with the cancellation, so the state is refreshed from it and
// the write retried with no extra read. Losing the write itself to a concurrent
// transaction on the same item surfaces as TransactionConflict: no condition
// was evaluated and no item comes back, and the transaction it collided with
// was all but certainly a write to this emoji's aggregate (every transition on
// the emoji writes it), so the state in hand is stale and a retry from it would
// only fail the version check to learn as much. It re-reads instead, spending
// a batch read rather than a doomed transaction. Every retry waits first (see
// reactionBackoffBase) and they share the attempt budget (see
// maxReactionAttempts).
func (s *store) absorbReactionFailure(ctx context.Context, err error, items []types.TransactWriteItem, metaIdx int, state *reactionState, attempt int, backoff *time.Duration) (reactionRetry, error) {
	reasons, ok := cancellationDetails(err)
	if !ok || len(reasons) != len(items) {
		return 0, err
	}
	codes := make([]string, len(reasons))
	for i, r := range reasons {
		codes[i] = aws.ToString(r.Code)
	}
	metaFailed := metaIdx >= 0 && codes[metaIdx] == codeConditionalCheckFailed

	var retry reactionRetry
	switch {
	case codes[0] == codeConditionalCheckFailed:
		retry = retryAfterRead
	case attempt+1 >= maxReactionAttempts:
		return 0, err
	case codes[1] == codeConditionalCheckFailed || metaFailed:
		if codes[1] == codeConditionalCheckFailed {
			// Nothing deletes an aggregate row, so a failed condition always
			// returns one — whether the loss was a create racing another create
			// (the row now exists) or a stale version.
			if len(reasons[1].Item) == 0 {
				return 0, errors.New("aggregate condition failed without returning the row")
			}
			state.agg = aggStateFromItem(reasons[1].Item)
		}
		if metaFailed {
			if len(reasons[metaIdx].Item) == 0 {
				return 0, errors.New("meta condition failed without returning the row")
			}
			state.meta = metaStateFromItem(reasons[metaIdx].Item)
		}
		retry = retryFromState
	case slices.Contains(codes, codeTransactionConflict):
		retry = retryAfterRead
	default:
		return 0, err
	}

	select {
	case <-ctx.Done():
		return 0, ctx.Err()
	case <-time.After(*backoff + rand.N(*backoff)):
	}
	*backoff = min(2**backoff, reactionBackoffMax)
	return retry, nil
}

// aggregateWrite is the transaction item that sets an emoji's aggregate from
// current to next: a create conditioned on absence when no row exists, else an
// update conditioned on the version read. Either way the row is returned on a
// failed condition so the writer can retry from it.
func (s *store) aggregateWrite(chatID *commonpb.ChatId, seq uint64, emoji string, current, next aggState) types.TransactWriteItem {
	key := map[string]types.AttributeValue{attrPK: avS(chatPK(chatID)), attrSK: avS(aggSK(seq, emoji))}
	if !current.exists {
		return types.TransactWriteItem{Put: &types.Put{
			TableName: aws.String(s.reactionsTable),
			Item: map[string]types.AttributeValue{
				attrPK:              key[attrPK],
				attrSK:              key[attrSK],
				attrEmoji:           avS(emoji),
				attrReactorCount:    avN(next.count),
				attrReactionVersion: avN(next.version),
				attrSample:          sampleAttr(next.sample),
			},
			ConditionExpression:                 aws.String(fmt.Sprintf("attribute_not_exists(%s)", attrPK)),
			ReturnValuesOnConditionCheckFailure: types.ReturnValuesOnConditionCheckFailureAllOld,
		}}
	}
	// The emoji is fixed by the key and written once by the create above, so the
	// update sets only what moves. Attribute names go through placeholders:
	// "sample" is a DynamoDB reserved word, and naming the rest the same way
	// keeps the expression uniform.
	return types.TransactWriteItem{Update: &types.Update{
		TableName:           aws.String(s.reactionsTable),
		Key:                 key,
		UpdateExpression:    aws.String("SET #count = :count, #version = :version, #sample = :sample"),
		ConditionExpression: aws.String("#version = :expected"),
		ExpressionAttributeNames: map[string]string{
			"#count":   attrReactorCount,
			"#version": attrReactionVersion,
			"#sample":  attrSample,
		},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":count":    avN(next.count),
			":version":  avN(next.version),
			":sample":   sampleAttr(next.sample),
			":expected": avN(current.version),
		},
		ReturnValuesOnConditionCheckFailure: types.ReturnValuesOnConditionCheckFailureAllOld,
	}}
}

// metaWrite is the transaction item that sets a message's active type count from
// current to activeTypes, on the same create-or-compare-and-set terms as
// aggregateWrite.
func (s *store) metaWrite(chatID *commonpb.ChatId, seq uint64, current metaState, activeTypes uint64) types.TransactWriteItem {
	key := map[string]types.AttributeValue{attrPK: avS(chatPK(chatID)), attrSK: avS(metaSK(seq))}
	if !current.exists {
		return types.TransactWriteItem{Put: &types.Put{
			TableName: aws.String(s.reactionsTable),
			Item: map[string]types.AttributeValue{
				attrPK:        key[attrPK],
				attrSK:        key[attrSK],
				attrTypeCount: avN(activeTypes),
			},
			ConditionExpression:                 aws.String(fmt.Sprintf("attribute_not_exists(%s)", attrPK)),
			ReturnValuesOnConditionCheckFailure: types.ReturnValuesOnConditionCheckFailureAllOld,
		}}
	}
	return types.TransactWriteItem{Update: &types.Update{
		TableName:                aws.String(s.reactionsTable),
		Key:                      key,
		UpdateExpression:         aws.String("SET #types = :types"),
		ConditionExpression:      aws.String("#types = :expected"),
		ExpressionAttributeNames: map[string]string{"#types": attrTypeCount},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":types":    avN(activeTypes),
			":expected": avN(current.types),
		},
		ReturnValuesOnConditionCheckFailure: types.ReturnValuesOnConditionCheckFailureAllOld,
	}}
}

// readReactionState fetches, in one strongly consistent batch read spanning both
// tables, the three items a reaction write decides from: the emoji's aggregate
// and the message's meta row (message_reactions) and the caller's own reactor row
// (message_reactors, with the version that added it, for a re-add's result).
// Absent keys are simply omitted from the response, so a missing row leaves its
// flag false.
func (s *store) readReactionState(ctx context.Context, chatID *commonpb.ChatId, seq uint64, emoji string, userID *commonpb.UserId) (reactionState, error) {
	aggKey := aggSK(seq, emoji)
	metaKey := metaSK(seq)
	req := map[string]types.KeysAndAttributes{
		s.reactionsTable: {
			Keys: []map[string]types.AttributeValue{
				{attrPK: avS(chatPK(chatID)), attrSK: avS(aggKey)},
				{attrPK: avS(chatPK(chatID)), attrSK: avS(metaKey)},
			},
			ConsistentRead: aws.Bool(true),
		},
		s.reactorsTable: {
			Keys: []map[string]types.AttributeValue{
				{attrPK: avS(reactorPK(chatID, seq)), attrSK: avS(userSK(userID, emoji))},
			},
			ConsistentRead:       aws.Bool(true),
			ProjectionExpression: aws.String(attrSK + ", " + attrEmojiVersion),
		},
	}

	var state reactionState
	// Drain UnprocessedKeys; a resolved item is retained while a throttled one is
	// retried (as in readSendState).
	for len(req) > 0 {
		resp, err := s.client.BatchGetItem(ctx, &dynamodb.BatchGetItemInput{RequestItems: req})
		if err != nil {
			return reactionState{}, err
		}
		for _, item := range resp.Responses[s.reactionsTable] {
			switch asS(item[attrSK]) {
			case aggKey:
				state.agg = aggStateFromItem(item)
			case metaKey:
				state.meta = metaStateFromItem(item)
			}
		}
		for _, item := range resp.Responses[s.reactorsTable] {
			version, err := versionFromEmojiVersionKey(asS(item[attrEmojiVersion]))
			if err != nil {
				return reactionState{}, err
			}
			state.reacted, state.reactedAt = true, version
		}
		req = make(map[string]types.KeysAndAttributes)
		for table, unprocessed := range resp.UnprocessedKeys {
			if len(unprocessed.Keys) > 0 {
				req[table] = unprocessed
			}
		}
	}
	return state, nil
}

func (s *store) GetReactionSummary(
	ctx context.Context,
	chatID *commonpb.ChatId,
	messageID *messagingpb.MessageId,
) ([]*messaging.Reaction, error) {
	groups, err := s.reactionsForSeqRange(ctx, chatID, messageID.Value, messageID.Value)
	if err != nil {
		return nil, err
	}
	return groups[messageID.Value], nil
}

func (s *store) GetReactionSummariesByRefs(
	ctx context.Context,
	chatID *commonpb.ChatId,
	messageIDs []*messagingpb.MessageId,
) ([]*messaging.ReactionSummary, error) {
	if len(messageIDs) == 0 {
		return nil, nil
	}
	seqs := sortedSeqs(messageIDs)

	// Query each run's window once; a message with no reactions (or unknown) has
	// no aggregates in the window and is echoed with an empty summary below.
	groups := make(map[uint64][]*messaging.Reaction)
	for _, run := range seqRuns(seqs) {
		window, err := s.reactionsForSeqRange(ctx, chatID, run[0], run[1])
		if err != nil {
			return nil, err
		}
		for seq, reactions := range window {
			groups[seq] = reactions
		}
	}

	out := make([]*messaging.ReactionSummary, 0, len(seqs))
	for _, seq := range seqs {
		out = append(out, &messaging.ReactionSummary{
			MessageID: &messagingpb.MessageId{Value: seq},
			Reactions: groups[seq],
		})
	}
	return out, nil
}

func (s *store) GetReactionSummaries(
	ctx context.Context,
	chatID *commonpb.ChatId,
	opts ...database.QueryOption,
) ([]*messaging.ReactionSummary, error) {
	q := database.ApplyQueryOptions(opts...)
	forward := q.Order != commonpb.QueryOptions_DESC

	limit := q.Limit
	if limit <= 0 {
		limit = database.DefaultQueryOptions().Limit
	}

	// The page spans messages, not just reacted ones, so a message with no
	// reactions is returned with an empty summary rather than skipped. Message IDs
	// are a gapless per-chat sequence (1..lastSeq), so the page is a contiguous
	// window of IDs; lastSeq (the counter row, one small read) bounds it. The
	// alternative — paging the messages table — would read full message rows just
	// to recover their IDs.
	lastSeq, err := s.lastMessageSeq(ctx, chatID)
	if err != nil {
		return nil, err
	}
	if lastSeq == 0 {
		return nil, nil // no messages in the chat
	}

	// Resolve the page to a contiguous message-ID window [lo, hi], clamped to the
	// sequence and resuming strictly past the cursor in the requested order.
	cursor, hasCursor := messaging.IDFromPageToken(q.PagingToken)
	var lo, hi uint64
	if forward {
		lo = 1
		if hasCursor {
			lo = cursor + 1
		}
		if lo > lastSeq {
			return nil, nil // cursor at or past the head
		}
		hi = lastSeq
		if span := lo + uint64(limit) - 1; span < hi {
			hi = span
		}
	} else {
		hi = lastSeq
		if hasCursor {
			if cursor <= 1 {
				return nil, nil // cursor at or before the tail
			}
			hi = cursor - 1
		}
		lo = 1
		if hi > uint64(limit) {
			lo = hi - uint64(limit) + 1
		}
	}

	// One agg# range query over the window; a message with no reactions simply has
	// no group, and is emitted below as an empty summary.
	groups, err := s.reactionsForSeqRange(ctx, chatID, lo, hi)
	if err != nil {
		return nil, err
	}

	result := make([]*messaging.ReactionSummary, 0, hi-lo+1)
	emit := func(seq uint64) {
		result = append(result, &messaging.ReactionSummary{
			MessageID: &messagingpb.MessageId{Value: seq},
			Reactions: groups[seq], // nil/empty when the message has no reactions
		})
	}
	if forward {
		for seq := lo; seq <= hi; seq++ {
			emit(seq)
		}
	} else {
		for seq := hi; seq >= lo; seq-- {
			emit(seq)
		}
	}
	return result, nil
}

// lastMessageSeq returns the chat's highest assigned message seq from the counter
// row, or 0 when the chat has no messages yet. Message IDs are gapless, so this
// bounds the message-ID space at 1..lastSeq.
func (s *store) lastMessageSeq(ctx context.Context, chatID *commonpb.ChatId) (uint64, error) {
	out, err := s.client.GetItem(ctx, &dynamodb.GetItemInput{
		TableName:            aws.String(s.messagesTable),
		Key:                  map[string]types.AttributeValue{attrPK: avS(chatPK(chatID)), attrSK: avS(skCounter)},
		ProjectionExpression: aws.String(attrLastSeq),
		ConsistentRead:       aws.Bool(true),
	})
	if err != nil {
		return 0, err
	}
	if len(out.Item) == 0 {
		return 0, nil
	}
	return parseN(out.Item[attrLastSeq])
}

// reactionsForSeqRange returns the active emoji aggregates for message seqs in
// the contiguous window [lo, hi], grouped by seq and ordered by emoji within each
// group, via one agg# range query. Every seq in the window is a real message (the
// sequence is gapless), so any aggregate the query returns belongs to the page.
// Messages with no reactions are absent from the returned map; so is any emoji
// retained at count 0 for its version. The meta# rows share the partition but not
// the prefix, so the range never sees them.
//
// The read is strongly consistent (see messaging.Store.GetReactionSummary for
// the client-facing reason). It is the one reaction read whose volume follows
// viewers × pages rather than reactions, and it lands on the chat partition
// that takes every aggregate CAS, so the doubled RCU of a strong read is spent
// here of all places; on a typical page (a few agg# rows) that is a couple of
// units. Should a hot chat's partition ever throttle, the lever is to keep the
// single-message and by-refs reads strong — the refresh after a reaction —
// and let only the paged history walk go eventual, where the version
// watermark already makes a lagging count harmless.
func (s *store) reactionsForSeqRange(ctx context.Context, chatID *commonpb.ChatId, lo, hi uint64) (map[uint64][]*messaging.Reaction, error) {
	groups := make(map[uint64][]*messaging.Reaction)
	from := aggPrefix + seqPad(lo) + "#"
	to := aggPrefix + seqPad(hi) + "#~"
	var startKey map[string]types.AttributeValue
	for {
		out, err := s.client.Query(ctx, &dynamodb.QueryInput{
			TableName:              aws.String(s.reactionsTable),
			KeyConditionExpression: aws.String(fmt.Sprintf("%s = :pk AND %s BETWEEN :from AND :to", attrPK, attrSK)),
			ExpressionAttributeValues: map[string]types.AttributeValue{
				":pk":   avS(chatPK(chatID)),
				":from": avS(from),
				":to":   avS(to),
			},
			ConsistentRead:    aws.Bool(true),
			ExclusiveStartKey: startKey,
		})
		if err != nil {
			return nil, err
		}
		for _, item := range out.Items {
			agg := aggStateFromItem(item)
			if agg.count == 0 {
				continue // inactive emoji aggregate, retained only for its version
			}
			seq, err := seqFromAggSK(asS(item[attrSK]))
			if err != nil {
				return nil, err
			}
			groups[seq] = append(groups[seq], reactionFromAgg(asS(item[attrEmoji]), agg))
		}
		if len(out.LastEvaluatedKey) == 0 {
			break
		}
		startKey = out.LastEvaluatedKey
	}

	for seq := range groups {
		reactions := groups[seq]
		sort.Slice(reactions, func(i, j int) bool { return reactions[i].Emoji < reactions[j].Emoji })
		groups[seq] = reactions
	}
	return groups, nil
}

func (s *store) GetSelfReactions(
	ctx context.Context,
	chatID *commonpb.ChatId,
	userID *commonpb.UserId,
	messageIDs []*messagingpb.MessageId,
) ([]messaging.SelfReaction, error) {
	// A DM writes no viewer-keyed rows (see the table comment), so a DM read
	// here would answer "nothing" for a viewer who did react. Refuse it rather
	// than mislead: the caller owns the DM overlay.
	if !chat.IsGroupChatID(chatID) {
		return nil, messaging.ErrSelfReactionsGroupOnly
	}
	seqs := sortedSeqs(messageIDs)
	if len(seqs) == 0 {
		return nil, nil
	}
	wanted := make(map[uint64]struct{}, len(seqs))
	for _, seq := range seqs {
		wanted[seq] = struct{}{}
	}

	// One strongly consistent range query on the viewer's partition per run of
	// message IDs, the same grouping as the aggregate read (see
	// maxSummaryRefGap). A run's window can cover IDs nobody asked about; the
	// rows are tiny and the viewer's own, so reading past them is cheap, but
	// they are filtered out so the answer is exactly the messages requested.
	var present []messaging.SelfReaction
	for _, run := range seqRuns(seqs) {
		var startKey map[string]types.AttributeValue
		for {
			out, err := s.client.Query(ctx, &dynamodb.QueryInput{
				TableName:              aws.String(s.selfReactionsTable),
				KeyConditionExpression: aws.String(fmt.Sprintf("%s = :pk AND %s BETWEEN :from AND :to", attrPK, attrSK)),
				ExpressionAttributeValues: map[string]types.AttributeValue{
					":pk":   avS(viewerPK(chatID, userID)),
					":from": avS(seqPad(run[0]) + "#"),
					":to":   avS(seqPad(run[1]) + "#~"),
				},
				ConsistentRead:    aws.Bool(true),
				ExclusiveStartKey: startKey,
			})
			if err != nil {
				return nil, err
			}
			for _, item := range out.Items {
				seq, emoji, err := seqEmojiFromSK(asS(item[attrSK]))
				if err != nil {
					return nil, err
				}
				if _, ok := wanted[seq]; !ok {
					continue
				}
				version, err := parseN(item[attrSelfVersion])
				if err != nil {
					return nil, err
				}
				present = append(present, messaging.SelfReaction{
					MessageID: &messagingpb.MessageId{Value: seq},
					Emoji:     emoji,
					Version:   version,
				})
			}
			if len(out.LastEvaluatedKey) == 0 {
				break
			}
			startKey = out.LastEvaluatedKey
		}
	}
	return present, nil
}

// sortedSeqs is the message IDs as sorted, deduplicated seqs.
func sortedSeqs(messageIDs []*messagingpb.MessageId) []uint64 {
	seqs := make([]uint64, 0, len(messageIDs))
	for _, id := range messageIDs {
		seqs = append(seqs, id.Value)
	}
	slices.Sort(seqs)
	return slices.Compact(seqs)
}

// seqRuns groups sorted seqs into [lo, hi] windows in which no two neighbours
// are further apart than maxSummaryRefGap, so each window is one range query
// that reads no long stretch nobody asked for.
func seqRuns(seqs []uint64) [][2]uint64 {
	var runs [][2]uint64
	for start := 0; start < len(seqs); {
		end := start
		for end+1 < len(seqs) && seqs[end+1]-seqs[end] <= maxSummaryRefGap {
			end++
		}
		runs = append(runs, [2]uint64{seqs[start], seqs[end]})
		start = end + 1
	}
	return runs
}

func (s *store) GetReactors(
	ctx context.Context,
	chatID *commonpb.ChatId,
	messageID *messagingpb.MessageId,
	emoji string,
	opts ...database.QueryOption,
) ([]*messaging.Reactor, uint64, bool, error) {
	seq := messageID.Value
	q := database.ApplyQueryOptions(opts...)

	// The version first, then the page, so the page is never older than the
	// version reported with it (see the Store contract). An emoji with no
	// reactors has no rows to page.
	agg, err := s.getAggregate(ctx, chatID, seq, emoji)
	if err != nil {
		return nil, 0, false, err
	}
	if agg.count == 0 {
		return nil, agg.version, false, nil
	}

	// Page the emoji's reactors off the version index, newest first, under a
	// strongly consistent read. The cursor resumes strictly below the last
	// version returned, expressed as a key range rather than an ExclusiveStartKey
	// so the token need carry nothing but the version.
	input := &dynamodb.QueryInput{
		TableName:                aws.String(s.reactorsTable),
		IndexName:                aws.String(lsiByVersion),
		ExpressionAttributeNames: map[string]string{"#pk": attrPK, "#version": attrEmojiVersion},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":pk": avS(reactorPK(chatID, seq)),
		},
		ScanIndexForward: aws.Bool(false),
		ConsistentRead:   aws.Bool(true),
	}
	if cursor, ok := messaging.ReactorFromPageToken(q.PagingToken); ok {
		if cursor <= 1 {
			return nil, agg.version, false, nil // nothing lies below the first version
		}
		input.KeyConditionExpression = aws.String("#pk = :pk AND #version BETWEEN :lo AND :hi")
		input.ExpressionAttributeValues[":lo"] = avS(emojiVersionKey(emoji, 0))
		input.ExpressionAttributeValues[":hi"] = avS(emojiVersionKey(emoji, cursor-1))
	} else {
		input.KeyConditionExpression = aws.String("#pk = :pk AND begins_with(#version, :prefix)")
		input.ExpressionAttributeValues[":prefix"] = avS(emojiHex(emoji) + "#")
	}
	// Over-fetch one row beyond the page so hasMore can be determined exactly.
	// DynamoDB sets LastEvaluatedKey whenever a query stops at Limit — even when it
	// landed on the last matching row — so the key alone can't distinguish "exactly
	// a page" from "more remain". Fetching limit+1 does.
	if q.Limit > 0 {
		input.Limit = aws.Int32(int32(q.Limit) + 1)
	}

	out, err := s.client.Query(ctx, input)
	if err != nil {
		return nil, 0, false, err
	}
	reactors := make([]*messaging.Reactor, 0, len(out.Items))
	for _, item := range out.Items {
		r, err := reactorFromItem(item)
		if err != nil {
			return nil, 0, false, err
		}
		reactors = append(reactors, r)
	}

	// Trim the over-fetched row and report hasMore. The dropped row isn't lost: the
	// caller resumes from the last returned reactor, so it leads the next page. When
	// no limit was set, fall back to LastEvaluatedKey (a real 1 MB page boundary).
	var hasMore bool
	if q.Limit > 0 {
		if hasMore = len(reactors) > q.Limit; hasMore {
			reactors = reactors[:q.Limit]
		}
	} else {
		hasMore = len(out.LastEvaluatedKey) > 0
	}
	return reactors, agg.version, hasMore, nil
}

// getAggregate reads an emoji's agg# row with a strongly consistent read. The
// zero aggState (exists false) is returned when there is no row.
func (s *store) getAggregate(ctx context.Context, chatID *commonpb.ChatId, seq uint64, emoji string) (aggState, error) {
	out, err := s.client.GetItem(ctx, &dynamodb.GetItemInput{
		TableName:      aws.String(s.reactionsTable),
		Key:            map[string]types.AttributeValue{attrPK: avS(chatPK(chatID)), attrSK: avS(aggSK(seq, emoji))},
		ConsistentRead: aws.Bool(true),
	})
	if err != nil {
		return aggState{}, err
	}
	if len(out.Item) == 0 {
		return aggState{}, nil
	}
	return aggStateFromItem(out.Item), nil
}

// aggStateFromItem decodes an agg# row. Malformed numbers decode as zero: the
// store writes every attribute itself, so this is defensive, not a code path.
func aggStateFromItem(item map[string]types.AttributeValue) aggState {
	count, _ := parseN(item[attrReactorCount])
	version, _ := parseN(item[attrReactionVersion])
	return aggState{exists: true, count: count, version: version, sample: parseSampleMap(item[attrSample])}
}

func metaStateFromItem(item map[string]types.AttributeValue) metaState {
	activeTypes, _ := parseN(item[attrTypeCount])
	return metaState{exists: true, types: activeTypes}
}

// reactionFromAgg projects an aggregate onto a Reaction. The surfaced sample is
// the most-recent MaxSampleReactors of the retained set (see
// messaging.SampleFromReactors). The per-viewer ReactedBySelf is left false for
// the server to overlay.
func reactionFromAgg(emoji string, agg aggState) *messaging.Reaction {
	reactors := make([]*messaging.Reactor, 0, len(agg.sample))
	for userHex, entry := range agg.sample {
		uid, err := hex.DecodeString(userHex)
		if err != nil {
			continue // sample keys are store-written hex; skip anything malformed
		}
		reactors = append(reactors, &messaging.Reactor{
			UserID:    &commonpb.UserId{Value: uid},
			ReactedTs: entry.ts,
			Version:   entry.version,
		})
	}
	return &messaging.Reaction{
		Emoji:          emoji,
		Count:          agg.count,
		Version:        agg.version,
		SampleReactors: messaging.SampleFromReactors(reactors),
	}
}

// sampleAttr encodes a sample as the agg# row's map attribute: user hex -> {
// version: added at, ts: reacted nanos }.
func sampleAttr(sample map[string]sampleEntry) types.AttributeValue {
	m := make(map[string]types.AttributeValue, len(sample))
	for userHex, entry := range sample {
		m[userHex] = &types.AttributeValueMemberM{Value: map[string]types.AttributeValue{
			attrSampleVersion: avN(entry.version),
			attrSampleTs:      avN(uint64(entry.ts.UnixNano())),
		}}
	}
	return &types.AttributeValueMemberM{Value: m}
}

// parseSampleMap decodes an agg# row's sample attribute, the inverse of
// sampleAttr. A missing or non-map attribute yields an empty sample.
func parseSampleMap(av types.AttributeValue) map[string]sampleEntry {
	out := make(map[string]sampleEntry)
	m, ok := av.(*types.AttributeValueMemberM)
	if !ok {
		return out
	}
	for userHex, v := range m.Value {
		entry, ok := v.(*types.AttributeValueMemberM)
		if !ok {
			continue
		}
		version, _ := parseN(entry.Value[attrSampleVersion])
		nanos, _ := parseInt(entry.Value[attrSampleTs])
		out[userHex] = sampleEntry{version: version, ts: time.Unix(0, nanos).UTC()}
	}
	return out
}

// reactorFromItem decodes a reactor row (or its LSI projection): the user from
// the sort key, the version from emoji_version, and the display timestamp.
func reactorFromItem(item map[string]types.AttributeValue) (*messaging.Reactor, error) {
	userID, err := userIDFromUserSK(asS(item[attrSK]))
	if err != nil {
		return nil, err
	}
	version, err := versionFromEmojiVersionKey(asS(item[attrEmojiVersion]))
	if err != nil {
		return nil, err
	}
	nanos, _ := parseInt(item[attrReactedTs])
	return &messaging.Reactor{
		UserID:    userID,
		ReactedTs: time.Unix(0, nanos).UTC(),
		Version:   version,
	}, nil
}

// cancellationDetails extracts the per-item cancellation reasons — code and, when
// the write asked for it, the item that failed its condition — from a
// TransactWriteItems error, if it is a TransactionCanceledException.
func cancellationDetails(err error) ([]types.CancellationReason, bool) {
	var tce *types.TransactionCanceledException
	if !errors.As(err, &tce) {
		return nil, false
	}
	return tce.CancellationReasons, true
}

func aggSK(seq uint64, emoji string) string { return aggPrefix + seqPad(seq) + "#" + emojiHex(emoji) }

func metaSK(seq uint64) string { return metaPrefix + seqPad(seq) }

// reactorPK keys a message's partition in message_reactors: the chat's key
// extended by the message seq, so no two messages share an item collection.
func reactorPK(chatID *commonpb.ChatId, seq uint64) string { return chatPK(chatID) + "#" + seqPad(seq) }

func userSK(userID *commonpb.UserId, emoji string) string {
	return userPrefix + hex.EncodeToString(userID.Value) + "#" + emojiHex(emoji)
}

// viewerPK keys a viewer's partition in message_self_reactions: the chat's key
// extended by the user, so one partition holds a user's reactions in a chat.
func viewerPK(chatID *commonpb.ChatId, userID *commonpb.UserId) string {
	return chatPK(chatID) + "#" + hex.EncodeToString(userID.Value)
}

// seqEmojiSK is a self-reaction row's sk: the padded message seq then the
// emoji, so a viewer's rows sort by message and a window of messages is a key
// range.
func seqEmojiSK(seq uint64, emoji string) string {
	return seqPad(seq) + "#" + emojiHex(emoji)
}

// seqEmojiFromSK recovers the message seq and emoji from a self-reaction sk.
func seqEmojiFromSK(sk string) (uint64, string, error) {
	padded, emojiHex, ok := strings.Cut(sk, "#")
	if !ok {
		return 0, "", fmt.Errorf("unexpected self-reaction sk %q", sk)
	}
	seq, err := strconv.ParseUint(padded, 10, 64)
	if err != nil {
		return 0, "", fmt.Errorf("unexpected self-reaction sk %q: %w", sk, err)
	}
	emoji, err := hex.DecodeString(emojiHex)
	if err != nil {
		return 0, "", fmt.Errorf("unexpected self-reaction sk %q: %w", sk, err)
	}
	return seq, string(emoji), nil
}

// selfReaction marks an AddReaction result as the reactor's own view: reacted,
// at the version that added their reaction.
func selfReaction(r *messaging.Reaction, addedAt uint64) *messaging.Reaction {
	r.ReactedBySelf = true
	r.ReactedBySelfVersion = addedAt
	return r
}

// emojiVersionKey is a reactor row's emoji_version: the emoji then the zero-padded version
// that added the reactor, so one emoji's rows sort by version under a common
// prefix in the LSI.
func emojiVersionKey(emoji string, version uint64) string {
	return emojiHex(emoji) + "#" + seqPad(version)
}

func emojiHex(emoji string) string { return hex.EncodeToString([]byte(emoji)) }

func seqPad(seq uint64) string { return fmt.Sprintf("%0*d", seqPadWidth, seq) }

// seqFromAggSK recovers a message's sequence number from an aggregate sk
// ("agg#<padded seq>#<emoji hex>").
func seqFromAggSK(sk string) (uint64, error) {
	rest, ok := strings.CutPrefix(sk, aggPrefix)
	if !ok {
		return 0, fmt.Errorf("unexpected agg sk %q", sk)
	}
	padded, _, ok := strings.Cut(rest, "#")
	if !ok {
		return 0, fmt.Errorf("unexpected agg sk %q", sk)
	}
	return strconv.ParseUint(padded, 10, 64)
}

// userIDFromUserSK recovers the reactor from a reactor row's sk
// ("user#<user hex>#<emoji hex>").
func userIDFromUserSK(sk string) (*commonpb.UserId, error) {
	rest, ok := strings.CutPrefix(sk, userPrefix)
	if !ok {
		return nil, fmt.Errorf("unexpected reactor sk %q", sk)
	}
	userHex, _, ok := strings.Cut(rest, "#")
	if !ok {
		return nil, fmt.Errorf("unexpected reactor sk %q", sk)
	}
	id, err := hex.DecodeString(userHex)
	if err != nil {
		return nil, fmt.Errorf("unexpected reactor sk %q: %w", sk, err)
	}
	return &commonpb.UserId{Value: id}, nil
}

// versionFromEmojiVersionKey recovers the version from a reactor row's emoji_version
// ("<emoji hex>#<padded version>").
func versionFromEmojiVersionKey(key string) (uint64, error) {
	_, padded, ok := strings.Cut(key, "#")
	if !ok {
		return 0, fmt.Errorf("unexpected emoji version key %q", key)
	}
	return strconv.ParseUint(padded, 10, 64)
}
