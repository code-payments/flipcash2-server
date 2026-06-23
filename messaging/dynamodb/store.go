package dynamodb

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"fmt"
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

	"github.com/code-payments/flipcash2-server/database"
	"github.com/code-payments/flipcash2-server/messaging"
)

// The messaging store spans three tables:
//
//	messages           pk = "chat#<id>", sk in { "#counter", "msg#<padded seq>",
//	                   "cmid#<client id>" }. All of a chat's messages, its
//	                   sequence counter, and its idempotency markers share one
//	                   partition so a send is one single-partition transaction.
//	                   The #counter row holds last_seq (message-ID head) and
//	                   last_event_seq (event-log head); each msg# row carries its
//	                   event_seq, indexed by the messages_by_event_seq GSI (pk,
//	                   event_seq) for event-sequence-ordered reads. While every
//	                   event is a new message, event_seq is the message's own seq
//	                   and last_event_seq mirrors last_seq; the two heads (and
//	                   event_seq vs seq) diverge once edits and deletes advance the
//	                   event log without minting a seq.
//
//	message_pointers   pk = "chat#<id>", sk = "<type>#<user>". Delivered/read
//	                   pointers, kept out of the messages partition so heavy
//	                   receipt writes don't contend with the send path (pointers
//	                   share nothing transactional with messages).
//
//	message_reactions  pk = "chat#<id>", sk in { "agg#<padded seq>#<emoji hex>",
//	                   "rct#<padded seq>#<emoji hex>#<user hex>" }. One agg# row
//	                   per (message, emoji) holds the count, a monotonic sequence,
//	                   and a bounded sample map (user hex -> reacted_ts, up to
//	                   MaxStoredSampleReactors of the most-recent reactors, a new
//	                   reactor evicting the least-recent once full) surfaced as the
//	                   reaction's sample. The row is retained at
//	                   count 0 so the sequence survives an emoji being removed and
//	                   re-added. One rct# row per reactor backs idempotency, the
//	                   self-reaction check, and the reactors_by_recency GSI
//	                   (reaction_key = chat#<id>#<padded seq>#<emoji hex>,
//	                   reacted_ts), which orders an emoji's reactors
//	                   most-recent-first for GetReactors (the full paged list).
const (
	skCounter   = "#counter"
	msgPrefix   = "msg#"
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
	attrEventSeq      = "event_seq" // msg# row: per-message event-log sequence (== seq while every event is a new message); messages_by_event_seq GSI sort key
	attrExpiresAt     = "expires_at" // DynamoDB TTL attribute (epoch seconds)

	// message_pointers table attributes
	attrUserID     = "user_id"
	attrPointerVal = "ptr_value" // avoids the reserved word "value"
	attrType       = "type"      // reserved; referenced via ExpressionAttributeNames

	// message_reactions table attributes
	aggPrefix = "agg#"
	rctPrefix = "rct#"

	attrEmoji         = "emoji"
	attrReactionCount = "r_count" // count and sequence are DynamoDB reserved words
	attrReactionSeq   = "seq"
	attrSample        = "sample"       // agg# row map: user hex -> reacted_ts (bounded sample)
	attrReactedTs     = "reacted_ts"   // reactor row attr; reactors_by_recency sort key (nanos)
	attrReactionKey   = "reaction_key" // reactors_by_recency partition key

	reactorsByRecencyGSI  = "reactors_by_recency"
	messagesByEventSeqGSI = "messages_by_event_seq"

	// DynamoDB transaction cancellation / condition codes
	codeConditionalCheckFailed = "ConditionalCheckFailed"
	codeTransactionConflict    = "TransactionConflict"

	maxPutMessageAttempts  = 32
	maxAddReactionAttempts = 32
	maxBatchGetKeys        = 100
)

type store struct {
	client         *dynamodb.Client
	messagesTable  string
	pointersTable  string
	reactionsTable string
}

// NewInDynamoDB returns a messaging.Store backed by the given DynamoDB tables.
// Use CreateTables to provision them.
func NewInDynamoDB(client *dynamodb.Client, messagesTable, pointersTable, reactionsTable string) messaging.Store {
	return &store{
		client:         client,
		messagesTable:  messagesTable,
		pointersTable:  pointersTable,
		reactionsTable: reactionsTable,
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
		markerSeq, lastSeq, lastUnread, err := s.readSendState(ctx, chatID, clientMessageID)
		if err != nil {
			return nil, false, err
		}
		// Fast idempotent path: a prior send with this client message ID wins.
		if markerSeq != nil {
			msg, err := s.GetMessage(ctx, chatID, &messagingpb.MessageId{Value: *markerSeq})
			return msg, false, err
		}

		nextSeq := lastSeq + 1
		nextUnread := lastUnread
		if countsTowardUnread {
			nextUnread++
		}

		msg := &messaging.Message{
			ChatID:    &commonpb.ChatId{Value: append([]byte(nil), chatID.Value...)},
			ID:        &messagingpb.MessageId{Value: nextSeq},
			SenderID:  senderID,
			Content:   content,
			Timestamp: ts,
			UnreadSeq: nextUnread,
		}

		_, err = s.client.TransactWriteItems(ctx, &dynamodb.TransactWriteItemsInput{
			TransactItems: []types.TransactWriteItem{
				// [0] advance the counter under an optimistic lock so the whole
				// transaction rolls back together — no leaked sequence numbers.
				// last_event_seq mirrors last_seq here — the fleet keeps the
				// event-log head current (maintain-only), but nothing depends on it
				// yet; it diverges from last_seq once edits and deletes land.
				{Update: &types.Update{
					TableName: aws.String(s.messagesTable),
					Key: map[string]types.AttributeValue{
						attrPK: avS(chatPK(chatID)),
						attrSK: avS(skCounter),
					},
					UpdateExpression:    aws.String(fmt.Sprintf("SET %s = :next, %s = :nextUnread, %s = :next", attrLastSeq, attrLastUnreadSeq, attrLastEventSeq)),
					ConditionExpression: aws.String(fmt.Sprintf("attribute_not_exists(%s) OR %s = :expected", attrPK, attrLastSeq)),
					ExpressionAttributeValues: map[string]types.AttributeValue{
						":next":       avN(nextSeq),
						":nextUnread": avN(nextUnread),
						":expected":   avN(lastSeq),
					},
				}},
				// [1] the message itself.
				{Put: &types.Put{
					TableName:           aws.String(s.messagesTable),
					Item:                s.messageItem(msg, contentBlobs),
					ConditionExpression: aws.String(fmt.Sprintf("attribute_not_exists(%s)", attrPK)),
				}},
				// [2] the idempotency marker. It is transient — only the message
				// and counter are permanent — so it carries a TTL for auto-reaping.
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
		// [2]=idempotency marker.
		if len(reasons) == 3 && reasons[2] == codeConditionalCheckFailed {
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

func (s *store) GetLatestEventSequence(ctx context.Context, chatID *commonpb.ChatId) (uint64, error) {
	// Every event is a new message, so the head event sequence is the highest
	// assigned message seq, read from the counter row.
	return s.lastMessageSeq(ctx, chatID)
}

func (s *store) GetMessage(ctx context.Context, chatID *commonpb.ChatId, messageID *messagingpb.MessageId) (*messaging.Message, error) {
	out, err := s.client.GetItem(ctx, &dynamodb.GetItemInput{
		TableName: aws.String(s.messagesTable),
		Key: map[string]types.AttributeValue{
			attrPK: avS(chatPK(chatID)),
			attrSK: avS(msgSK(messageID.Value)),
		},
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
	// Project to pk only so the content blobs are never read or decoded.
	out, err := s.client.GetItem(ctx, &dynamodb.GetItemInput{
		TableName:            aws.String(s.messagesTable),
		Key:                  map[string]types.AttributeValue{attrPK: avS(chatPK(chatID)), attrSK: avS(msgSK(messageID.Value))},
		ProjectionExpression: aws.String(attrPK),
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
	var pointers []*messagingpb.Pointer
	var startKey map[string]types.AttributeValue
	for {
		out, err := s.client.Query(ctx, &dynamodb.QueryInput{
			TableName:              aws.String(s.pointersTable),
			KeyConditionExpression: aws.String(fmt.Sprintf("%s = :pk", attrPK)),
			ExpressionAttributeValues: map[string]types.AttributeValue{
				":pk": avS(chatPK(chatID)),
			},
			ExclusiveStartKey: startKey,
		})
		if err != nil {
			return nil, err
		}
		for _, item := range out.Items {
			pointers = append(pointers, pointerFromItem(item))
		}
		if len(out.LastEvaluatedKey) == 0 {
			break
		}
		startKey = out.LastEvaluatedKey
	}
	return pointers, nil
}

func (s *store) GetPointersForChats(ctx context.Context, refs []messaging.PointerRef) (map[string][]*messagingpb.Pointer, error) {
	// DELIVERED and READ are the only pointer types ever stored (StoredPointerTypes),
	// and the refs name the members, so the exact (chat, member, type) keys are
	// known up front. Enumerate them and batch-read in one path, mirroring
	// GetMessagesByRefs — no per-chat partition scan. Dedup so a repeated
	// (chat, member) pair collapses.
	type dedupKey struct {
		chat string
		user string
	}
	seen := make(map[dedupKey]struct{})
	var keys []map[string]types.AttributeValue
	for _, ref := range refs {
		for _, member := range ref.Members {
			dk := dedupKey{chat: string(ref.ChatID.Value), user: string(member.Value)}
			if _, dup := seen[dk]; dup {
				continue
			}
			seen[dk] = struct{}{}
			for _, t := range messaging.StoredPointerTypes {
				keys = append(keys, map[string]types.AttributeValue{
					attrPK: avS(chatPK(ref.ChatID)),
					attrSK: avS(pointerSK(t, member)),
				})
			}
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
				key := string(chatIDFromPK(item).Value)
				out[key] = append(out[key], pointerFromItem(item))
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
	now := time.Now().UTC()
	_, err := s.client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName: aws.String(s.pointersTable),
		Key: map[string]types.AttributeValue{
			attrPK: avS(chatPK(chatID)),
			attrSK: avS(pointerSK(pointerType, userID)),
		},
		UpdateExpression:    aws.String(fmt.Sprintf("SET #t = :t, %s = :u, %s = :v, %s = :ts", attrUserID, attrPointerVal, attrTS)),
		ConditionExpression: aws.String(fmt.Sprintf("attribute_not_exists(%s) OR %s < :v", attrPK, attrPointerVal)),
		ExpressionAttributeNames: map[string]string{
			"#t": attrType,
		},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":t":  avN(uint64(pointerType)),
			":u":  avB(userID.Value),
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
			// item that failed the condition.
			return pointerFromItem(ccf.Item), false, nil
		}
		return nil, false, err
	}
	return &messagingpb.Pointer{
		Type:   pointerType,
		UserId: &commonpb.UserId{Value: append([]byte(nil), userID.Value...)},
		Value:  &messagingpb.MessageId{Value: newValue.Value},
		Ts:     timestamppb.New(now),
	}, true, nil
}

// readSendState fetches, in a single consistent batch read, the two partition
// items PutMessage needs before assigning a sequence number: the idempotency
// marker for clientMessageID and the chat's sequence counter. Both share the
// chat's partition (pk = chat#<id>), so one BatchGetItem covers them.
//
// markerSeq is non-nil when a prior send with this client message ID already
// persisted, carrying that message's sequence number; the caller then returns
// the existing message rather than assigning a new one. lastSeq and lastUnread
// are zero when the counter does not yet exist (the chat's first send).
func (s *store) readSendState(ctx context.Context, chatID *commonpb.ChatId, clientMessageID *messagingpb.ClientMessageId) (markerSeq *uint64, lastSeq, lastUnread uint64, err error) {
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
			return nil, 0, 0, batchErr
		}
		for _, item := range resp.Responses[s.messagesTable] {
			switch asS(item[attrSK]) {
			case cmidSKVal:
				seq, perr := parseN(item[attrSeq])
				if perr != nil {
					return nil, 0, 0, perr
				}
				markerSeq = &seq
			case skCounter:
				ls, perr := parseN(item[attrLastSeq])
				if perr != nil {
					return nil, 0, 0, perr
				}
				lu, perr := parseN(item[attrLastUnreadSeq])
				if perr != nil {
					return nil, 0, 0, perr
				}
				lastSeq, lastUnread = ls, lu
			}
		}
		if unprocessed, ok := resp.UnprocessedKeys[s.messagesTable]; ok && len(unprocessed.Keys) > 0 {
			req = map[string]types.KeysAndAttributes{s.messagesTable: unprocessed}
		} else {
			break
		}
	}
	return markerSeq, lastSeq, lastUnread, nil
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
		// While every event is a new message, a message's event_seq is its own
		// seq. Written (rather than derived on read) so the messages_by_event_seq
		// GSI indexes the row.
		attrEventSeq: avN(msg.ID.Value),
	}
	if msg.SenderID != nil {
		item[attrSenderID] = avB(msg.SenderID.Value)
	}
	return item
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

	msg := &messaging.Message{
		ChatID:    &commonpb.ChatId{Value: append([]byte(nil), chatID.Value...)},
		ID:        &messagingpb.MessageId{Value: seq},
		Content:   content,
		Timestamp: time.Unix(0, nanos).UTC(),
		UnreadSeq: unreadSeq,
	}
	if sender := asB(item[attrSenderID]); len(sender) > 0 {
		msg.SenderID = &commonpb.UserId{Value: append([]byte(nil), sender...)}
	}
	return msg, nil
}

func pointerFromItem(item map[string]types.AttributeValue) *messagingpb.Pointer {
	typeVal, _ := parseN(item[attrType])
	value, _ := parseN(item[attrPointerVal])
	// Pointers written before ts existed have no timestamp; default to now() so
	// the required proto field is always populated rather than backfilling.
	ts := time.Now()
	if nanos, err := parseInt(item[attrTS]); err == nil {
		ts = time.Unix(0, nanos).UTC()
	}
	return &messagingpb.Pointer{
		Type:   messagingpb.Pointer_Type(typeVal),
		UserId: &commonpb.UserId{Value: append([]byte(nil), asB(item[attrUserID])...)},
		Value:  &messagingpb.MessageId{Value: value},
		Ts:     timestamppb.New(ts),
	}
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

func pointerSK(pointerType messagingpb.Pointer_Type, userID *commonpb.UserId) string {
	return strconv.Itoa(int(pointerType)) + "#" + hex.EncodeToString(userID.Value)
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

func (s *store) AddReaction(
	ctx context.Context,
	chatID *commonpb.ChatId,
	messageID *messagingpb.MessageId,
	userID *commonpb.UserId,
	emoji string,
	ts time.Time,
) (*messaging.Reaction, bool, bool, error) {
	seq := messageID.Value

	for attempt := 0; attempt < maxAddReactionAttempts; attempt++ {
		// One batched read fetches both the emoji aggregate and the caller's own
		// reactor row (both share the chat's partition).
		aggExists, count, sequence, sample, exists, err := s.readReactionState(ctx, chatID, seq, emoji, userID)
		if err != nil {
			return nil, false, false, err
		}

		// Idempotent: the user already reacted with this emoji.
		if exists {
			return buildReaction(emoji, count, sequence, sample), false, false, nil
		}

		// Activating a (new or previously-emptied) emoji must respect the
		// per-message distinct-type cap; re-adding never trips it.
		//
		// This is a soft cap: the count is read here, not enforced inside the
		// write transaction below, so concurrent activations of distinct emoji can
		// each pass the check and land a few over MaxReactionTypesPerMessage. The
		// overshoot is rare (it needs simultaneous distinct-emoji adds right at the
		// limit), bounded, self-limiting (further activations are then rejected),
		// and harms nothing — counts and sequences stay correct. Enforcing it
		// exactly would require a per-message counter in the transaction plus an
		// optimistic condition on the aggregate bump; not worth the added
		// contention for an anti-abuse limit.
		if !aggExists || count == 0 {
			active, err := s.countActiveAggregates(ctx, chatID, seq)
			if err != nil {
				return nil, false, false, err
			}
			if active >= messaging.MaxReactionTypesPerMessage {
				return nil, false, true, nil
			}
		}

		// [0] the reactor row, conditioned so a concurrent identical add (same
		// user, same emoji) loses and is treated as idempotent.
		items := []types.TransactWriteItem{
			{Put: &types.Put{
				TableName: aws.String(s.reactionsTable),
				Item: map[string]types.AttributeValue{
					attrPK:          avS(chatPK(chatID)),
					attrSK:          avS(rctSK(seq, emoji, userID)),
					attrUserID:      avB(userID.Value),
					attrReactedTs:   avN(uint64(ts.UnixNano())),
					attrReactionKey: avS(reactionKey(chatID, seq, emoji)),
				},
				ConditionExpression: aws.String(fmt.Sprintf("attribute_not_exists(%s)", attrPK)),
			}},
		}
		// Maintain the recent sample on the aggregate: insert this reactor while the
		// stored set has room, otherwise evict the least-recent entry to make room —
		// but only when this reactor is itself more recent than that entry. This keeps
		// the stored sample the most-recent MaxStoredSampleReactors; reads surface only
		// the most-recent MaxSampleReactors of it (see messaging.SampleFromReactors).
		//
		// The decision uses the sample read above, not a transactional condition, so it
		// is soft. But every sample mutation targets this one agg# item, and DynamoDB
		// serializes transactions on a shared item: a conflicting concurrent add is
		// cancelled and retried against a fresh strongly-consistent read, so eviction
		// targets the true current least-recent entry rather than a stale one.
		newHex := hex.EncodeToString(userID.Value)
		addToSample := false
		evictHex := ""
		if len(sample) < messaging.MaxStoredSampleReactors {
			addToSample = true
		} else if oldHex, oldTs, ok := leastRecentInSample(sample); ok && moreRecent(ts, newHex, oldTs, oldHex) {
			addToSample = true
			evictHex = oldHex
		}

		// [1] the emoji aggregate: bump an existing one, or create it. A create
		// races with a concurrent first-reactor; the loser retries as a bump.
		if aggExists {
			update := &types.Update{
				TableName: aws.String(s.reactionsTable),
				Key:       map[string]types.AttributeValue{attrPK: avS(chatPK(chatID)), attrSK: avS(aggSK(seq, emoji))},
			}
			switch {
			case addToSample && evictHex != "":
				// Atomic evict-and-insert on the sample map alongside the counter bump;
				// no read-modify-write, so it never contends on the aggregate row.
				update.UpdateExpression = aws.String(fmt.Sprintf("SET #s.#new = :ts REMOVE #s.#old ADD %s :one, %s :one", attrReactionCount, attrReactionSeq))
				update.ExpressionAttributeNames = map[string]string{"#s": attrSample, "#new": newHex, "#old": evictHex}
				update.ExpressionAttributeValues = map[string]types.AttributeValue{":one": avN(1), ":ts": avN(uint64(ts.UnixNano()))}
			case addToSample:
				// Room in the sample: single-key map insert alongside the counter bump.
				update.UpdateExpression = aws.String(fmt.Sprintf("SET #s.#new = :ts ADD %s :one, %s :one", attrReactionCount, attrReactionSeq))
				update.ExpressionAttributeNames = map[string]string{"#s": attrSample, "#new": newHex}
				update.ExpressionAttributeValues = map[string]types.AttributeValue{":one": avN(1), ":ts": avN(uint64(ts.UnixNano()))}
			default:
				// Not recent enough to sample: pure counter bump.
				update.UpdateExpression = aws.String(fmt.Sprintf("ADD %s :one, %s :one", attrReactionCount, attrReactionSeq))
				update.ExpressionAttributeValues = map[string]types.AttributeValue{":one": avN(1)}
			}
			items = append(items, types.TransactWriteItem{Update: update})
		} else {
			items = append(items, types.TransactWriteItem{Put: &types.Put{
				TableName: aws.String(s.reactionsTable),
				Item: map[string]types.AttributeValue{
					attrPK:            avS(chatPK(chatID)),
					attrSK:            avS(aggSK(seq, emoji)),
					attrEmoji:         avS(emoji),
					attrReactionCount: avN(1),
					attrReactionSeq:   avN(1),
					// First reactor seeds the sample map (always has room).
					attrSample: &types.AttributeValueMemberM{Value: map[string]types.AttributeValue{
						hex.EncodeToString(userID.Value): avN(uint64(ts.UnixNano())),
					}},
				},
				ConditionExpression: aws.String(fmt.Sprintf("attribute_not_exists(%s)", attrPK)),
			}})
		}

		_, err = s.client.TransactWriteItems(ctx, &dynamodb.TransactWriteItemsInput{TransactItems: items})
		if err == nil {
			// Read back the authoritative count, sequence, and sample (a concurrent
			// add to the same emoji may have advanced them past count+1).
			_, newCount, newSeq, newSample, err := s.getAggregate(ctx, chatID, seq, emoji)
			if err != nil {
				return nil, false, false, err
			}
			return buildReaction(emoji, newCount, newSeq, newSample), true, false, nil
		}

		reasons, ok := cancellationReasons(err)
		if !ok {
			return nil, false, false, err
		}
		// reasons index matches TransactItems order: [0]=reactor, [1]=aggregate.
		// A failed reactor condition means a concurrent identical add won: re-read
		// and return the idempotent result. A failed aggregate create means a
		// concurrent first-reactor created it: retry as a bump.
		if len(reasons) >= 1 && reasons[0] == codeConditionalCheckFailed {
			continue
		}
		if isRetryable(reasons) {
			continue
		}
		return nil, false, false, err
	}
	return nil, false, false, fmt.Errorf("add reaction exhausted retries for chat %s", hex.EncodeToString(chatID.Value))
}

func (s *store) RemoveReaction(
	ctx context.Context,
	chatID *commonpb.ChatId,
	messageID *messagingpb.MessageId,
	userID *commonpb.UserId,
	emoji string,
) (*messaging.Reaction, bool, error) {
	seq := messageID.Value

	for attempt := 0; attempt < maxAddReactionAttempts; attempt++ {
		// One batched read fetches both the emoji aggregate and the caller's own
		// reactor row (both share the chat's partition).
		aggExists, count, sequence, sample, exists, err := s.readReactionState(ctx, chatID, seq, emoji, userID)
		if err != nil {
			return nil, false, err
		}

		// Idempotent: the user didn't react. Reflect the current state, or report a
		// pure no-op when there's no aggregate at all.
		if !exists {
			if aggExists {
				return buildReaction(emoji, count, sequence, sample), false, nil
			}
			return nil, false, nil
		}

		// [0] delete the reactor (lose to a concurrent identical remove), [1]
		// decrement the aggregate, advance its sequence, and drop the reactor from
		// the sample map (a no-op if it wasn't sampled; never backfilled). The
		// aggregate row is retained even at count 0 so sequence survives a re-add.
		_, err = s.client.TransactWriteItems(ctx, &dynamodb.TransactWriteItemsInput{TransactItems: []types.TransactWriteItem{
			{Delete: &types.Delete{
				TableName:           aws.String(s.reactionsTable),
				Key:                 map[string]types.AttributeValue{attrPK: avS(chatPK(chatID)), attrSK: avS(rctSK(seq, emoji, userID))},
				ConditionExpression: aws.String(fmt.Sprintf("attribute_exists(%s)", attrPK)),
			}},
			{Update: &types.Update{
				TableName:           aws.String(s.reactionsTable),
				Key:                 map[string]types.AttributeValue{attrPK: avS(chatPK(chatID)), attrSK: avS(aggSK(seq, emoji))},
				UpdateExpression:    aws.String(fmt.Sprintf("REMOVE #s.#u ADD %s :negone, %s :one", attrReactionCount, attrReactionSeq)),
				ConditionExpression: aws.String(fmt.Sprintf("attribute_exists(%s)", attrPK)),
				ExpressionAttributeNames: map[string]string{
					"#s": attrSample,
					"#u": hex.EncodeToString(userID.Value),
				},
				ExpressionAttributeValues: map[string]types.AttributeValue{
					":negone": &types.AttributeValueMemberN{Value: "-1"},
					":one":    avN(1),
				},
			}},
		}})
		if err == nil {
			// Read back the advanced count, sequence, and sample; Count may now be 0,
			// but the aggregate still carries the sequence the removal broadcast needs.
			_, newCount, newSeq, newSample, err := s.getAggregate(ctx, chatID, seq, emoji)
			if err != nil {
				return nil, false, err
			}
			return buildReaction(emoji, newCount, newSeq, newSample), true, nil
		}

		reasons, ok := cancellationReasons(err)
		if !ok {
			return nil, false, err
		}
		// A failed reactor delete means a concurrent identical remove won: re-read
		// and return the idempotent result.
		if len(reasons) >= 1 && reasons[0] == codeConditionalCheckFailed {
			continue
		}
		if isRetryable(reasons) {
			continue
		}
		return nil, false, err
	}
	return nil, false, fmt.Errorf("remove reaction exhausted retries for chat %s", hex.EncodeToString(chatID.Value))
}

func (s *store) GetReactionSummary(
	ctx context.Context,
	chatID *commonpb.ChatId,
	messageID *messagingpb.MessageId,
) ([]*messaging.Reaction, error) {
	return s.reactionsForMessage(ctx, chatID, messageID.Value)
}

func (s *store) GetReactionSummariesByRefs(
	ctx context.Context,
	chatID *commonpb.ChatId,
	messageIDs []*messagingpb.MessageId,
) ([]*messaging.ReactionSummary, error) {
	seen := make(map[uint64]struct{}, len(messageIDs))
	var out []*messaging.ReactionSummary
	for _, id := range messageIDs {
		if _, dup := seen[id.Value]; dup {
			continue
		}
		seen[id.Value] = struct{}{}
		reactions, err := s.reactionsForMessage(ctx, chatID, id.Value)
		if err != nil {
			return nil, err
		}
		// Every requested message is echoed; one with no reactions (or unknown)
		// comes back with an empty summary rather than being omitted.
		out = append(out, &messaging.ReactionSummary{
			MessageID: &messagingpb.MessageId{Value: id.Value},
			Reactions: reactions,
		})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].MessageID.Value < out[j].MessageID.Value })
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
// Messages with no reactions are absent from the returned map.
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
			ExclusiveStartKey: startKey,
		})
		if err != nil {
			return nil, err
		}
		for _, item := range out.Items {
			count, _ := parseN(item[attrReactionCount])
			if count == 0 {
				continue // inactive emoji aggregate, retained only for its sequence
			}
			seq, err := seqFromAggSK(asS(item[attrSK]))
			if err != nil {
				return nil, err
			}
			// The sample is carried on the agg# item itself — no per-emoji query.
			groups[seq] = append(groups[seq], reactionFromAggItem(item))
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
	refs []messaging.ReactionRef,
) ([]messaging.ReactionRef, error) {
	// Dedup to exact reactor-row keys, remembering which ref each key maps back to.
	byKey := make(map[string]messaging.ReactionRef, len(refs))
	var keys []map[string]types.AttributeValue
	for _, ref := range refs {
		sk := rctSK(ref.MessageID.Value, ref.Emoji, userID)
		if _, dup := byKey[sk]; dup {
			continue
		}
		byKey[sk] = ref
		keys = append(keys, map[string]types.AttributeValue{
			attrPK: avS(chatPK(chatID)),
			attrSK: avS(sk),
		})
	}

	var present []messaging.ReactionRef
	for start := 0; start < len(keys); start += maxBatchGetKeys {
		end := start + maxBatchGetKeys
		if end > len(keys) {
			end = len(keys)
		}
		req := map[string]types.KeysAndAttributes{
			s.reactionsTable: {Keys: keys[start:end], ProjectionExpression: aws.String(attrSK)},
		}
		for len(req[s.reactionsTable].Keys) > 0 {
			resp, err := s.client.BatchGetItem(ctx, &dynamodb.BatchGetItemInput{RequestItems: req})
			if err != nil {
				return nil, err
			}
			for _, item := range resp.Responses[s.reactionsTable] {
				if ref, ok := byKey[asS(item[attrSK])]; ok {
					present = append(present, ref)
				}
			}
			if unprocessed, ok := resp.UnprocessedKeys[s.reactionsTable]; ok && len(unprocessed.Keys) > 0 {
				req = map[string]types.KeysAndAttributes{s.reactionsTable: unprocessed}
			} else {
				break
			}
		}
	}
	return present, nil
}

func (s *store) GetReactors(
	ctx context.Context,
	chatID *commonpb.ChatId,
	messageID *messagingpb.MessageId,
	emoji string,
	consistent bool,
	opts ...database.QueryOption,
) ([]*messaging.Reactor, bool, error) {
	q := database.ApplyQueryOptions(opts...)
	if consistent {
		return s.getReactorsConsistent(ctx, chatID, messageID.Value, emoji, q)
	}
	return s.getReactorsEventuallyConsistent(ctx, chatID, messageID.Value, emoji, q)
}

// getReactorsEventuallyConsistent pages an emoji's reactors most-recent-first
// off the reactors_by_recency index. The index is eventually consistent but
// supports deep paging without reading the whole reactor set.
func (s *store) getReactorsEventuallyConsistent(ctx context.Context, chatID *commonpb.ChatId, seq uint64, emoji string, q database.QueryOptions) ([]*messaging.Reactor, bool, error) {
	input := &dynamodb.QueryInput{
		TableName:              aws.String(s.reactionsTable),
		IndexName:              aws.String(reactorsByRecencyGSI),
		KeyConditionExpression: aws.String("#g = :g"),
		ExpressionAttributeNames: map[string]string{
			"#g": attrReactionKey,
		},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":g": avS(reactionKey(chatID, seq, emoji)),
		},
		ScanIndexForward: aws.Bool(false), // most-recent-first
	}
	// Over-fetch one row beyond the page so hasMore can be determined exactly.
	// DynamoDB sets LastEvaluatedKey whenever a query stops at Limit — even when it
	// landed on the last matching row — so the key alone can't distinguish "exactly
	// a page" from "more remain". Fetching limit+1 does: getting the extra row means
	// a next page exists; getting fewer means the partition is exhausted.
	if q.Limit > 0 {
		input.Limit = aws.Int32(int32(q.Limit) + 1)
	}
	if ts, userID, ok := messaging.ReactorFromPageToken(q.PagingToken); ok {
		input.ExclusiveStartKey = map[string]types.AttributeValue{
			attrReactionKey: avS(reactionKey(chatID, seq, emoji)),
			attrReactedTs:   avN(uint64(ts.UnixNano())),
			attrPK:          avS(chatPK(chatID)),
			attrSK:          avS(rctSK(seq, emoji, userID)),
		}
	}

	out, err := s.client.Query(ctx, input)
	if err != nil {
		return nil, false, err
	}
	reactors := make([]*messaging.Reactor, 0, len(out.Items))
	for _, item := range out.Items {
		reactors = append(reactors, reactorFromItem(item))
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

	return reactors, hasMore, nil
}

// getReactorsConsistent reads an emoji's reactor rows directly from the message
// partition under one strongly consistent query, then orders and pages them in
// memory. It reads the whole reactor set, so it suits small sets (e.g. DM
// reactions); larger lists should page the recency index instead. Ordering and
// paging match getReactorsEventuallyConsistent exactly.
func (s *store) getReactorsConsistent(ctx context.Context, chatID *commonpb.ChatId, seq uint64, emoji string, q database.QueryOptions) ([]*messaging.Reactor, bool, error) {
	prefix := rctPrefix + seqPad(seq) + "#" + emojiHex(emoji) + "#"
	var reactors []*messaging.Reactor
	var startKey map[string]types.AttributeValue
	for {
		out, err := s.client.Query(ctx, &dynamodb.QueryInput{
			TableName:              aws.String(s.reactionsTable),
			KeyConditionExpression: aws.String(fmt.Sprintf("%s = :pk AND begins_with(%s, :prefix)", attrPK, attrSK)),
			ExpressionAttributeValues: map[string]types.AttributeValue{
				":pk":     avS(chatPK(chatID)),
				":prefix": avS(prefix),
			},
			ConsistentRead:    aws.Bool(true),
			ExclusiveStartKey: startKey,
		})
		if err != nil {
			return nil, false, err
		}
		for _, item := range out.Items {
			reactors = append(reactors, reactorFromItem(item))
		}
		if len(out.LastEvaluatedKey) == 0 {
			break
		}
		startKey = out.LastEvaluatedKey
	}

	// Rows come back keyed by user, so order them most-recent-first here.
	sort.Slice(reactors, func(i, j int) bool { return reactorLess(reactors[i], reactors[j]) })

	// Resume strictly after the cursor reactor, in the same order.
	if ts, userID, ok := messaging.ReactorFromPageToken(q.PagingToken); ok {
		cursor := &messaging.Reactor{UserID: userID, ReactedTs: ts}
		filtered := reactors[:0]
		for _, r := range reactors {
			if reactorLess(cursor, r) {
				filtered = append(filtered, r)
			}
		}
		reactors = filtered
	}

	limit := q.Limit
	if limit <= 0 {
		limit = len(reactors)
	}
	hasMore := len(reactors) > limit
	if hasMore {
		reactors = reactors[:limit]
	}
	return reactors, hasMore, nil
}

// reactorLess orders reactors most-recent-first, breaking ties by ascending user
// ID so the ordering is total and stable for paging.
func reactorLess(a, b *messaging.Reactor) bool {
	if !a.ReactedTs.Equal(b.ReactedTs) {
		return a.ReactedTs.After(b.ReactedTs)
	}
	return bytes.Compare(a.UserID.Value, b.UserID.Value) < 0
}

// moreRecent reports whether reactor (tsA, userHexA) ranks ahead of
// (tsB, userHexB) under the most-recent-first ordering used for samples and
// GetReactors: later reaction time first, ties broken by smaller user ID. The keys
// are hex, whose lexicographic order matches the raw-byte order used elsewhere
// (see reactorLess).
func moreRecent(tsA time.Time, userHexA string, tsB time.Time, userHexB string) bool {
	if !tsA.Equal(tsB) {
		return tsA.After(tsB)
	}
	return userHexA < userHexB
}

// leastRecentInSample returns the sample entry that ranks last under the
// most-recent-first ordering (earliest reaction time; ties broken by larger user
// hex) — the entry a recent-sample eviction drops. ok is false for an empty map.
func leastRecentInSample(sample map[string]time.Time) (userHex string, ts time.Time, ok bool) {
	for k, t := range sample {
		if !ok || t.Before(ts) || (t.Equal(ts) && k > userHex) {
			userHex, ts, ok = k, t, true
		}
	}
	return userHex, ts, ok
}

// reactionsForMessage returns a message's active emoji aggregates, ordered by
// emoji. ReactedBySelf is left false for the caller to overlay.
func (s *store) reactionsForMessage(ctx context.Context, chatID *commonpb.ChatId, seq uint64) ([]*messaging.Reaction, error) {
	var reactions []*messaging.Reaction
	var startKey map[string]types.AttributeValue
	prefix := aggPrefix + seqPad(seq) + "#"
	for {
		out, err := s.client.Query(ctx, &dynamodb.QueryInput{
			TableName:              aws.String(s.reactionsTable),
			KeyConditionExpression: aws.String(fmt.Sprintf("%s = :pk AND begins_with(%s, :prefix)", attrPK, attrSK)),
			ExpressionAttributeValues: map[string]types.AttributeValue{
				":pk":     avS(chatPK(chatID)),
				":prefix": avS(prefix),
			},
			ExclusiveStartKey: startKey,
		})
		if err != nil {
			return nil, err
		}
		for _, item := range out.Items {
			count, _ := parseN(item[attrReactionCount])
			if count == 0 {
				continue
			}
			reactions = append(reactions, reactionFromAggItem(item))
		}
		if len(out.LastEvaluatedKey) == 0 {
			break
		}
		startKey = out.LastEvaluatedKey
	}
	sort.Slice(reactions, func(i, j int) bool { return reactions[i].Emoji < reactions[j].Emoji })
	return reactions, nil
}

// getAggregate reads an emoji aggregate's count, sequence, and bounded sample map
// with a strongly consistent read. exists is false when the aggregate row is
// absent; sample is nil in that case.
func (s *store) getAggregate(ctx context.Context, chatID *commonpb.ChatId, seq uint64, emoji string) (exists bool, count, sequence uint64, sample map[string]time.Time, err error) {
	out, err := s.client.GetItem(ctx, &dynamodb.GetItemInput{
		TableName:      aws.String(s.reactionsTable),
		Key:            map[string]types.AttributeValue{attrPK: avS(chatPK(chatID)), attrSK: avS(aggSK(seq, emoji))},
		ConsistentRead: aws.Bool(true),
	})
	if err != nil {
		return false, 0, 0, nil, err
	}
	if len(out.Item) == 0 {
		return false, 0, 0, nil, nil
	}
	count, _ = parseN(out.Item[attrReactionCount])
	sequence, _ = parseN(out.Item[attrReactionSeq])
	return true, count, sequence, parseSampleMap(out.Item[attrSample]), nil
}

// readReactionState fetches, in a single strongly-consistent batch read, the two
// rows the add/remove paths inspect before writing: the emoji's aggregate (count,
// sequence, bounded sample) and the caller's own reactor row. Both share the
// chat's partition (pk = chat#<id>), so one BatchGetItem covers them, mirroring
// readSendState on the message path. aggExists is false when the aggregate row is
// absent (sample is then nil); reacted reports whether userID already has a
// reactor row for the emoji.
func (s *store) readReactionState(ctx context.Context, chatID *commonpb.ChatId, seq uint64, emoji string, userID *commonpb.UserId) (aggExists bool, count, sequence uint64, sample map[string]time.Time, reacted bool, err error) {
	aggSKVal := aggSK(seq, emoji)
	rctSKVal := rctSK(seq, emoji, userID)
	req := map[string]types.KeysAndAttributes{
		s.reactionsTable: {
			Keys: []map[string]types.AttributeValue{
				{attrPK: avS(chatPK(chatID)), attrSK: avS(aggSKVal)},
				{attrPK: avS(chatPK(chatID)), attrSK: avS(rctSKVal)},
			},
			ConsistentRead: aws.Bool(true),
		},
	}

	// Drain UnprocessedKeys; a resolved item is retained while a throttled one is
	// retried (as in readSendState). Absent keys are simply omitted from the
	// response, so a missing aggregate or reactor row leaves its flag false.
	for len(req[s.reactionsTable].Keys) > 0 {
		resp, batchErr := s.client.BatchGetItem(ctx, &dynamodb.BatchGetItemInput{RequestItems: req})
		if batchErr != nil {
			return false, 0, 0, nil, false, batchErr
		}
		for _, item := range resp.Responses[s.reactionsTable] {
			switch asS(item[attrSK]) {
			case aggSKVal:
				aggExists = true
				count, _ = parseN(item[attrReactionCount])
				sequence, _ = parseN(item[attrReactionSeq])
				sample = parseSampleMap(item[attrSample])
			case rctSKVal:
				reacted = true
			}
		}
		if unprocessed, ok := resp.UnprocessedKeys[s.reactionsTable]; ok && len(unprocessed.Keys) > 0 {
			req = map[string]types.KeysAndAttributes{s.reactionsTable: unprocessed}
		} else {
			break
		}
	}
	return aggExists, count, sequence, sample, reacted, nil
}

// countActiveAggregates counts the distinct emoji on a message that currently
// have at least one reactor, for the per-message type cap.
func (s *store) countActiveAggregates(ctx context.Context, chatID *commonpb.ChatId, seq uint64) (int, error) {
	active := 0
	var startKey map[string]types.AttributeValue
	prefix := aggPrefix + seqPad(seq) + "#"
	for {
		out, err := s.client.Query(ctx, &dynamodb.QueryInput{
			TableName:              aws.String(s.reactionsTable),
			KeyConditionExpression: aws.String(fmt.Sprintf("%s = :pk AND begins_with(%s, :prefix)", attrPK, attrSK)),
			ProjectionExpression:   aws.String(attrReactionCount),
			ExpressionAttributeValues: map[string]types.AttributeValue{
				":pk":     avS(chatPK(chatID)),
				":prefix": avS(prefix),
			},
			ConsistentRead:    aws.Bool(true),
			ExclusiveStartKey: startKey,
		})
		if err != nil {
			return 0, err
		}
		for _, item := range out.Items {
			if count, _ := parseN(item[attrReactionCount]); count > 0 {
				active++
			}
		}
		if len(out.LastEvaluatedKey) == 0 {
			break
		}
		startKey = out.LastEvaluatedKey
	}
	return active, nil
}

// reactionFromAggItem assembles a Reaction from a full agg# item — count,
// sequence, and the bounded sample map carried on the row. No query is needed:
// the sample lives on the aggregate itself. The per-viewer ReactedBySelf is left
// false for the server to overlay.
func reactionFromAggItem(item map[string]types.AttributeValue) *messaging.Reaction {
	count, _ := parseN(item[attrReactionCount])
	sequence, _ := parseN(item[attrReactionSeq])
	return buildReaction(asS(item[attrEmoji]), count, sequence, parseSampleMap(item[attrSample]))
}

// buildReaction assembles a Reaction from a known count, sequence, and sample map
// (user hex -> reacted ts). The surfaced sample is the most-recent MaxSampleReactors
// of the retained set (see messaging.SampleFromReactors).
func buildReaction(emoji string, count, sequence uint64, sample map[string]time.Time) *messaging.Reaction {
	reactors := make([]*messaging.Reactor, 0, len(sample))
	for userHex, ts := range sample {
		uid, err := hex.DecodeString(userHex)
		if err != nil {
			continue // sample keys are store-written hex; skip anything malformed
		}
		reactors = append(reactors, &messaging.Reactor{
			UserID:    &commonpb.UserId{Value: uid},
			ReactedTs: ts,
		})
	}
	return &messaging.Reaction{
		Emoji:          emoji,
		Count:          count,
		Sequence:       sequence,
		SampleReactors: messaging.SampleFromReactors(reactors),
	}
}

// parseSampleMap decodes an agg# row's sample map (user hex -> reacted_ts nanos)
// into a map of the same shape buildReaction consumes. A missing or non-map
// attribute yields nil.
func parseSampleMap(av types.AttributeValue) map[string]time.Time {
	m, ok := av.(*types.AttributeValueMemberM)
	if !ok {
		return nil
	}
	out := make(map[string]time.Time, len(m.Value))
	for userHex, v := range m.Value {
		nanos, _ := parseInt(v)
		out[userHex] = time.Unix(0, nanos).UTC()
	}
	return out
}

func reactorFromItem(item map[string]types.AttributeValue) *messaging.Reactor {
	nanos, _ := parseInt(item[attrReactedTs])
	return &messaging.Reactor{
		UserID:    &commonpb.UserId{Value: append([]byte(nil), asB(item[attrUserID])...)},
		ReactedTs: time.Unix(0, nanos).UTC(),
	}
}

func aggSK(seq uint64, emoji string) string {
	return aggPrefix + seqPad(seq) + "#" + emojiHex(emoji)
}

func rctSK(seq uint64, emoji string, userID *commonpb.UserId) string {
	return rctPrefix + seqPad(seq) + "#" + emojiHex(emoji) + "#" + hex.EncodeToString(userID.Value)
}

// reactionKey is the recency GSI partition for one (message, emoji): it includes
// the chat so reactors of identically-keyed messages in different chats never
// share a GSI partition.
func reactionKey(chatID *commonpb.ChatId, seq uint64, emoji string) string {
	return chatPK(chatID) + "#" + seqPad(seq) + "#" + emojiHex(emoji)
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
