package dynamodb

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	chatpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/chat/v1"
	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"

	"github.com/code-payments/flipcash2-server/chat"
	"github.com/code-payments/flipcash2-server/database"
)

// The chat store spans two tables:
//
//	chats     pk = "chat#<id>" (one item per chat). Canonical metadata: type,
//	          members (the DM participants), last_activity. GetChat is a point
//	          read and AdvanceLastActivity is an O(1) update of the source of
//	          truth.
//
//	dm_inbox  pk = "user#<id>", sk = "chat#<id>" (one item per (user, DM)). The
//	          per-user DM inbox index. A GSI on (user, last_activity) lets a
//	          user's DMs be listed most-recently-active first with true
//	          server-side pagination. last_activity and the participants are
//	          denormalized so the inbox renders from one query. AdvanceLast-
//	          Activity fans the new last_activity out to each member's row (two
//	          for a DM), re-sorting the GSI.
const (
	gsiByActivity = "by_activity"

	attrPK           = "pk"
	attrSK           = "sk"
	attrChatID       = "chat_id"
	attrType         = "type"
	attrMembers      = "members"
	attrLastActivity = "last_activity"
)

type store struct {
	client       *dynamodb.Client
	chatsTable   string
	dmInboxTable string
}

// NewInDynamoDB returns a chat.Store backed by the given DynamoDB tables. Use
// CreateTables to provision them.
func NewInDynamoDB(client *dynamodb.Client, chatsTable, dmInboxTable string) chat.Store {
	return &store{
		client:       client,
		chatsTable:   chatsTable,
		dmInboxTable: dmInboxTable,
	}
}

func (s *store) PutChat(ctx context.Context, c *chat.Chat) error {
	if len(c.Members) == 0 {
		return fmt.Errorf("chat must have at least one member")
	}

	transactItems := []types.TransactWriteItem{
		// The canonical metadata item; its condition enforces uniqueness.
		{Put: &types.Put{
			TableName:           aws.String(s.chatsTable),
			Item:                s.chatItem(c),
			ConditionExpression: aws.String(fmt.Sprintf("attribute_not_exists(%s)", attrPK)),
		}},
	}
	for _, member := range c.Members {
		transactItems = append(transactItems, types.TransactWriteItem{
			Put: &types.Put{
				TableName: aws.String(s.dmInboxTable),
				Item:      s.dmInboxItem(c, member),
			},
		})
	}

	_, err := s.client.TransactWriteItems(ctx, &dynamodb.TransactWriteItemsInput{
		TransactItems: transactItems,
	})
	if err != nil {
		// The only condition is attribute_not_exists on the metadata item, so a
		// cancelled transaction means the chat already exists.
		if isTransactionCanceled(err) {
			return chat.ErrChatExists
		}
		return err
	}
	return nil
}

func (s *store) GetChatByID(ctx context.Context, chatID *commonpb.ChatId) (*chat.Chat, error) {
	out, err := s.client.GetItem(ctx, &dynamodb.GetItemInput{
		TableName: aws.String(s.chatsTable),
		Key:       map[string]types.AttributeValue{attrPK: avS(chatPK(chatID))},
	})
	if err != nil {
		return nil, err
	}
	if len(out.Item) == 0 {
		return nil, chat.ErrChatNotFound
	}
	return chatFromItem(out.Item)
}

func (s *store) GetDmsForUserByLastActivity(ctx context.Context, userID *commonpb.UserId, opts ...database.QueryOption) ([]*chat.Chat, error) {
	q := database.ApplyQueryOptions(opts...)

	input := &dynamodb.QueryInput{
		TableName:              aws.String(s.dmInboxTable),
		IndexName:              aws.String(gsiByActivity),
		KeyConditionExpression: aws.String(fmt.Sprintf("%s = :u", attrPK)),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":u": avS(userPK(userID)),
		},
		ScanIndexForward: aws.Bool(q.Order != commonpb.QueryOptions_DESC),
	}
	if q.Limit > 0 {
		input.Limit = aws.Int32(int32(q.Limit))
	}

	// Resolve the paging cursor: the token value is the chat ID of the last chat
	// from the previous page. Resume strictly after that dm_inbox row.
	if q.PagingToken != nil {
		startKey, err := s.inboxRowKey(ctx, userID, &commonpb.ChatId{Value: q.PagingToken.Value})
		if err != nil {
			return nil, err
		}
		if startKey == nil {
			return nil, nil // Token references a chat the user is not in.
		}
		input.ExclusiveStartKey = startKey
	}

	out, err := s.client.Query(ctx, input)
	if err != nil {
		return nil, err
	}
	chats := make([]*chat.Chat, 0, len(out.Items))
	for _, item := range out.Items {
		c, err := chatFromItem(item)
		if err != nil {
			return nil, err
		}
		chats = append(chats, c)
	}
	return chats, nil
}

func (s *store) GetMembers(ctx context.Context, chatID *commonpb.ChatId) ([]*commonpb.UserId, error) {
	c, err := s.GetChatByID(ctx, chatID)
	if err != nil {
		return nil, err
	}
	return c.Members, nil
}

func (s *store) IsMember(ctx context.Context, chatID *commonpb.ChatId, userID *commonpb.UserId) (bool, error) {
	out, err := s.client.GetItem(ctx, &dynamodb.GetItemInput{
		TableName:            aws.String(s.dmInboxTable),
		Key:                  map[string]types.AttributeValue{attrPK: avS(userPK(userID)), attrSK: avS(chatSK(chatID))},
		ProjectionExpression: aws.String(attrPK),
	})
	if err != nil {
		return false, err
	}
	return len(out.Item) > 0, nil
}

func (s *store) AdvanceLastActivity(ctx context.Context, chatID *commonpb.ChatId, ts time.Time) (bool, error) {
	// Load the canonical record for the current value and the member set to
	// fan out to.
	out, err := s.client.GetItem(ctx, &dynamodb.GetItemInput{
		TableName:      aws.String(s.chatsTable),
		Key:            map[string]types.AttributeValue{attrPK: avS(chatPK(chatID))},
		ConsistentRead: aws.Bool(true),
	})
	if err != nil {
		return false, err
	}
	if len(out.Item) == 0 {
		return false, chat.ErrChatNotFound
	}
	cur, err := parseInt(out.Item[attrLastActivity])
	if err != nil {
		return false, err
	}
	if ts.UnixNano() <= cur {
		return false, nil // No-op: stored value is already at or after ts.
	}
	members := membersFromItem(out.Item)

	// Bump the canonical value (conditioned so it only moves forward) and
	// mirror it onto each member's inbox row so the GSI re-sorts.
	transactItems := []types.TransactWriteItem{
		{Update: &types.Update{
			TableName:           aws.String(s.chatsTable),
			Key:                 map[string]types.AttributeValue{attrPK: avS(chatPK(chatID))},
			UpdateExpression:    aws.String(fmt.Sprintf("SET %s = :ts", attrLastActivity)),
			ConditionExpression: aws.String(fmt.Sprintf("%s < :ts", attrLastActivity)),
			ExpressionAttributeValues: map[string]types.AttributeValue{
				":ts": avN(uint64(ts.UnixNano())),
			},
		}},
	}
	for _, member := range members {
		transactItems = append(transactItems, types.TransactWriteItem{
			Update: &types.Update{
				TableName:        aws.String(s.dmInboxTable),
				Key:              map[string]types.AttributeValue{attrPK: avS(userPK(member)), attrSK: avS(chatSK(chatID))},
				UpdateExpression: aws.String(fmt.Sprintf("SET %s = :ts", attrLastActivity)),
				// Each inbox row advances only if the new value is strictly
				// newer. Also guards against upserting a malformed row if the
				// member's row were somehow missing.
				ConditionExpression: aws.String(fmt.Sprintf("%s < :ts", attrLastActivity)),
				ExpressionAttributeValues: map[string]types.AttributeValue{
					":ts": avN(uint64(ts.UnixNano())),
				},
			},
		})
	}

	_, err = s.client.TransactWriteItems(ctx, &dynamodb.TransactWriteItemsInput{TransactItems: transactItems})
	if err != nil {
		// A concurrent advance moved last_activity to/past ts; treat as no-op.
		// last_activity is a derived value that self-heals on the next bump.
		if isTransactionCanceled(err) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

// inboxRowKey returns the GSI ExclusiveStartKey for the user's inbox row for
// chatID, or nil if the user has no such row.
func (s *store) inboxRowKey(ctx context.Context, userID *commonpb.UserId, chatID *commonpb.ChatId) (map[string]types.AttributeValue, error) {
	out, err := s.client.GetItem(ctx, &dynamodb.GetItemInput{
		TableName:            aws.String(s.dmInboxTable),
		Key:                  map[string]types.AttributeValue{attrPK: avS(userPK(userID)), attrSK: avS(chatSK(chatID))},
		ProjectionExpression: aws.String(fmt.Sprintf("%s, %s, %s", attrPK, attrSK, attrLastActivity)),
	})
	if err != nil {
		return nil, err
	}
	if len(out.Item) == 0 {
		return nil, nil
	}
	return map[string]types.AttributeValue{
		attrPK:           out.Item[attrPK],
		attrSK:           out.Item[attrSK],
		attrLastActivity: out.Item[attrLastActivity],
	}, nil
}

func (s *store) chatItem(c *chat.Chat) map[string]types.AttributeValue {
	return map[string]types.AttributeValue{
		attrPK:           avS(chatPK(c.ID)),
		attrChatID:       avB(c.ID.Value),
		attrType:         avN(uint64(c.Type)),
		attrMembers:      membersAttr(c.Members),
		attrLastActivity: avN(uint64(c.LastActivity.UnixNano())),
	}
}

func (s *store) dmInboxItem(c *chat.Chat, member *commonpb.UserId) map[string]types.AttributeValue {
	return map[string]types.AttributeValue{
		attrPK:           avS(userPK(member)),
		attrSK:           avS(chatSK(c.ID)),
		attrChatID:       avB(c.ID.Value),
		attrType:         avN(uint64(c.Type)),
		attrMembers:      membersAttr(c.Members),
		attrLastActivity: avN(uint64(c.LastActivity.UnixNano())),
	}
}

func chatFromItem(item map[string]types.AttributeValue) (*chat.Chat, error) {
	typeVal, err := parseN(item[attrType])
	if err != nil {
		return nil, err
	}
	nanos, err := parseInt(item[attrLastActivity])
	if err != nil {
		return nil, err
	}
	return &chat.Chat{
		ID:           &commonpb.ChatId{Value: append([]byte(nil), asB(item[attrChatID])...)},
		Type:         protoChatType(uint64(typeVal)),
		Members:      membersFromItem(item),
		LastActivity: time.Unix(0, nanos).UTC(),
	}, nil
}

func membersFromItem(item map[string]types.AttributeValue) []*commonpb.UserId {
	list := asL(item[attrMembers])
	members := make([]*commonpb.UserId, len(list))
	for i, av := range list {
		members[i] = &commonpb.UserId{Value: append([]byte(nil), asB(av)...)}
	}
	return members
}

func membersAttr(members []*commonpb.UserId) types.AttributeValue {
	values := make([]types.AttributeValue, len(members))
	for i, m := range members {
		values[i] = avB(m.Value)
	}
	return &types.AttributeValueMemberL{Value: values}
}

func chatPK(chatID *commonpb.ChatId) string { return "chat#" + hex.EncodeToString(chatID.Value) }
func chatSK(chatID *commonpb.ChatId) string { return "chat#" + hex.EncodeToString(chatID.Value) }
func userPK(userID *commonpb.UserId) string { return "user#" + hex.EncodeToString(userID.Value) }

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

func isTransactionCanceled(err error) bool {
	var tce *types.TransactionCanceledException
	return errors.As(err, &tce)
}

func protoChatType(v uint64) chatpb.Metadata_ChatType {
	return chatpb.Metadata_ChatType(v)
}
