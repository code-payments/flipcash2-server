package dynamodb

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"google.golang.org/protobuf/proto"

	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"
	messagingpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/messaging/v1"

	"github.com/code-payments/flipcash2-server/database"
	"github.com/code-payments/flipcash2-server/messaging"
)

// The messaging store spans two tables:
//
//	messages          pk = "chat#<id>", sk in { "#counter", "msg#<padded seq>",
//	                  "cmid#<client id>" }. All of a chat's messages, its
//	                  sequence counter, and its idempotency markers share one
//	                  partition so a send is one single-partition transaction.
//
//	message_pointers  pk = "chat#<id>", sk = "<type>#<user>". Delivered/read
//	                  pointers, kept out of the messages partition so heavy
//	                  receipt writes don't contend with the send path (pointers
//	                  share nothing transactional with messages).
const (
	skCounter   = "#counter"
	msgPrefix   = "msg#"
	cmidPrefix  = "cmid#"
	seqPadWidth = 20

	// messages table attributes
	attrPK              = "pk"
	attrSK              = "sk"
	attrChatID          = "chat_id"
	attrSeq             = "seq"
	attrLastSeq         = "last_seq"
	attrLastUnreadSeq   = "last_unread_seq"
	attrSenderID        = "sender_id"
	attrContent         = "content"
	attrTS              = "ts"
	attrUnreadSeq       = "unread_seq"
	attrClientMessageID = "client_message_id"

	// message_pointers table attributes
	attrUserID     = "user_id"
	attrPointerVal = "ptr_value" // avoids the reserved word "value"
	attrType       = "type"      // reserved; referenced via ExpressionAttributeNames

	// DynamoDB transaction cancellation / condition codes
	codeConditionalCheckFailed = "ConditionalCheckFailed"
	codeTransactionConflict    = "TransactionConflict"

	maxPutMessageAttempts = 16
	maxBatchGetKeys       = 100
)

type store struct {
	client        *dynamodb.Client
	messagesTable string
	pointersTable string
}

// NewInDynamoDB returns a messaging.Store backed by the given DynamoDB tables.
// Use CreateTables to provision them.
func NewInDynamoDB(client *dynamodb.Client, messagesTable, pointersTable string) messaging.Store {
	return &store{
		client:        client,
		messagesTable: messagesTable,
		pointersTable: pointersTable,
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
) (*messaging.Message, error) {
	contentBlobs, err := marshalContent(content)
	if err != nil {
		return nil, err
	}

	for attempt := 0; attempt < maxPutMessageAttempts; attempt++ {
		// Fast idempotent path: a prior send with this client message ID wins.
		if existing, err := s.messageByClientID(ctx, chatID, clientMessageID); err != nil {
			return nil, err
		} else if existing != nil {
			return existing, nil
		}

		lastSeq, lastUnread, err := s.readCounter(ctx, chatID)
		if err != nil {
			return nil, err
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
				{Update: &types.Update{
					TableName: aws.String(s.messagesTable),
					Key: map[string]types.AttributeValue{
						attrPK: avS(chatPK(chatID)),
						attrSK: avS(skCounter),
					},
					UpdateExpression:    aws.String(fmt.Sprintf("SET %s = :next, %s = :nextUnread", attrLastSeq, attrLastUnreadSeq)),
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
					Item:                s.messageItem(msg, clientMessageID, contentBlobs),
					ConditionExpression: aws.String(fmt.Sprintf("attribute_not_exists(%s)", attrPK)),
				}},
				// [2] the idempotency marker.
				{Put: &types.Put{
					TableName: aws.String(s.messagesTable),
					Item: map[string]types.AttributeValue{
						attrPK:  avS(chatPK(chatID)),
						attrSK:  avS(cmidSK(clientMessageID)),
						attrSeq: avN(nextSeq),
					},
					ConditionExpression: aws.String(fmt.Sprintf("attribute_not_exists(%s)", attrPK)),
				}},
			},
		})
		if err == nil {
			return msg, nil
		}

		reasons, ok := cancellationReasons(err)
		if !ok {
			return nil, err
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
		return nil, err
	}
	return nil, fmt.Errorf("put message exhausted retries for chat %s", hex.EncodeToString(chatID.Value))
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
				// owning chat is taken from the item rather than a single param.
				msg, err := messageFromItem(chatIDFromItem(item), item)
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

func (s *store) GetPointersForChats(ctx context.Context, chatIDs []*commonpb.ChatId) (map[string][]*messagingpb.Pointer, error) {
	// message_pointers has no cross-chat index, so this issues one query per
	// distinct chat. Bounded by the feed page size; parallelize if it gets hot.
	out := make(map[string][]*messagingpb.Pointer)
	for _, chatID := range chatIDs {
		key := string(chatID.Value)
		if _, done := out[key]; done {
			continue
		}
		pointers, err := s.GetPointers(ctx, chatID)
		if err != nil {
			return nil, err
		}
		if len(pointers) > 0 {
			out[key] = pointers
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
) (bool, error) {
	// The pointer's target must reference an existing message (in the other table).
	exists, err := s.client.GetItem(ctx, &dynamodb.GetItemInput{
		TableName:            aws.String(s.messagesTable),
		Key:                  map[string]types.AttributeValue{attrPK: avS(chatPK(chatID)), attrSK: avS(msgSK(newValue.Value))},
		ProjectionExpression: aws.String(attrPK),
	})
	if err != nil {
		return false, err
	}
	if len(exists.Item) == 0 {
		return false, messaging.ErrMessageNotFound
	}

	_, err = s.client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName: aws.String(s.pointersTable),
		Key: map[string]types.AttributeValue{
			attrPK: avS(chatPK(chatID)),
			attrSK: avS(pointerSK(pointerType, userID)),
		},
		UpdateExpression:    aws.String(fmt.Sprintf("SET #t = :t, %s = :u, %s = :v", attrUserID, attrPointerVal)),
		ConditionExpression: aws.String(fmt.Sprintf("attribute_not_exists(%s) OR %s < :v", attrPK, attrPointerVal)),
		ExpressionAttributeNames: map[string]string{
			"#t": attrType,
		},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":t": avN(uint64(pointerType)),
			":u": avB(userID.Value),
			":v": avN(newValue.Value),
		},
	})
	if err != nil {
		if isConditionalCheckFailed(err) {
			return false, nil // Not advanced (already at or past newValue).
		}
		return false, err
	}
	return true, nil
}

// messageByClientID returns the message previously persisted for clientMessageID
// in the chat, or nil if none exists.
func (s *store) messageByClientID(ctx context.Context, chatID *commonpb.ChatId, clientMessageID *messagingpb.ClientMessageId) (*messaging.Message, error) {
	out, err := s.client.GetItem(ctx, &dynamodb.GetItemInput{
		TableName: aws.String(s.messagesTable),
		Key: map[string]types.AttributeValue{
			attrPK: avS(chatPK(chatID)),
			attrSK: avS(cmidSK(clientMessageID)),
		},
		ConsistentRead: aws.Bool(true),
	})
	if err != nil {
		return nil, err
	}
	if len(out.Item) == 0 {
		return nil, nil
	}
	seq, err := parseN(out.Item[attrSeq])
	if err != nil {
		return nil, err
	}
	return s.GetMessage(ctx, chatID, &messagingpb.MessageId{Value: seq})
}

func (s *store) readCounter(ctx context.Context, chatID *commonpb.ChatId) (lastSeq, lastUnread uint64, err error) {
	out, err := s.client.GetItem(ctx, &dynamodb.GetItemInput{
		TableName:      aws.String(s.messagesTable),
		Key:            map[string]types.AttributeValue{attrPK: avS(chatPK(chatID)), attrSK: avS(skCounter)},
		ConsistentRead: aws.Bool(true),
	})
	if err != nil {
		return 0, 0, err
	}
	if len(out.Item) == 0 {
		return 0, 0, nil
	}
	lastSeq, err = parseN(out.Item[attrLastSeq])
	if err != nil {
		return 0, 0, err
	}
	lastUnread, err = parseN(out.Item[attrLastUnreadSeq])
	if err != nil {
		return 0, 0, err
	}
	return lastSeq, lastUnread, nil
}

func (s *store) messageItem(msg *messaging.Message, clientMessageID *messagingpb.ClientMessageId, contentBlobs []types.AttributeValue) map[string]types.AttributeValue {
	item := map[string]types.AttributeValue{
		attrPK:              avS(chatPK(msg.ChatID)),
		attrSK:              avS(msgSK(msg.ID.Value)),
		attrChatID:          avB(msg.ChatID.Value),
		attrSeq:             avN(msg.ID.Value),
		attrContent:         &types.AttributeValueMemberL{Value: contentBlobs},
		attrTS:              avN(uint64(msg.Timestamp.UnixNano())),
		attrUnreadSeq:       avN(msg.UnreadSeq),
		attrClientMessageID: avB(clientMessageID.Value),
	}
	if msg.SenderID != nil {
		item[attrSenderID] = avB(msg.SenderID.Value)
	}
	return item
}

func messageFromItem(chatID *commonpb.ChatId, item map[string]types.AttributeValue) (*messaging.Message, error) {
	seq, err := parseN(item[attrSeq])
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
	return &messagingpb.Pointer{
		Type:   messagingpb.Pointer_Type(typeVal),
		UserId: &commonpb.UserId{Value: append([]byte(nil), asB(item[attrUserID])...)},
		Value:  &messagingpb.MessageId{Value: value},
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

func chatIDFromItem(item map[string]types.AttributeValue) *commonpb.ChatId {
	return &commonpb.ChatId{Value: append([]byte(nil), asB(item[attrChatID])...)}
}

func chatPK(chatID *commonpb.ChatId) string { return "chat#" + hex.EncodeToString(chatID.Value) }

func msgSK(seq uint64) string { return fmt.Sprintf("%s%0*d", msgPrefix, seqPadWidth, seq) }

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

func isConditionalCheckFailed(err error) bool {
	var ccf *types.ConditionalCheckFailedException
	return errors.As(err, &ccf)
}
