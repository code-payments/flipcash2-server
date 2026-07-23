package dynamodb

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"

	"github.com/code-payments/flipcash2-server/blocklist"
)

// The blocklist store is a single table:
//
//	blocklist  pk = "user#<owner>", sk = "blocked#<blocked>" (one item per
//	           (owner, blocked user)). blocked_at is denormalized as an attribute.
//	           IsBlocked/Unblock are O(1) point operations on the base key. A GSI
//	           on (pk, blocked_at) lists an owner's blocklist most-recently-blocked
//	           first with true server-side pagination. The blocked user ID is
//	           recovered from the sk, so it is not stored as its own attribute.
const (
	// gsiByBlockedAt orders an owner's blocklist by blocked_at. It reuses the
	// base table's pk as its hash key.
	gsiByBlockedAt = "by_blocked_at"

	// gsiByBlockedUser is the reverse of gsiByBlockedAt: it reuses the base
	// table's sk (the blocked user) as its hash key, so a query for one user
	// returns everyone who has blocked them, ordered by blocked_at. The blocker
	// is recovered from the base table's pk.
	gsiByBlockedUser = "by_blocked_user"

	// userKeyPrefix prefixes an owner ID in the pk; blockedKeyPrefix prefixes a
	// blocked user ID in the sk.
	userKeyPrefix    = "user#"
	blockedKeyPrefix = "blocked#"

	attrPK        = "pk"
	attrSK        = "sk"
	attrBlockedAt = "blocked_at"
)

type store struct {
	client *dynamodb.Client
	table  string
}

// NewInDynamoDB returns a blocklist.Store backed by the given DynamoDB table.
// Use CreateTable to provision it.
func NewInDynamoDB(client *dynamodb.Client, table string) blocklist.Store {
	return &store{
		client: client,
		table:  table,
	}
}

func (s *store) Block(ctx context.Context, ownerID, blockedID *commonpb.UserId, blockedAt time.Time) (bool, error) {
	// Conditioned on the entry not already existing, so a re-block leaves the
	// original entry (and its blocked_at) untouched.
	_, err := s.client.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: aws.String(s.table),
		Item: map[string]types.AttributeValue{
			attrPK:        avS(ownerPK(ownerID)),
			attrSK:        avS(blockedSK(blockedID)),
			attrBlockedAt: avN(uint64(blockedAt.UnixNano())),
		},
		ConditionExpression: aws.String(fmt.Sprintf("attribute_not_exists(%s)", attrPK)),
	})
	if err != nil {
		// The condition failed only because the entry already exists: a no-op.
		if isConditionalCheckFailed(err) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

func (s *store) Unblock(ctx context.Context, ownerID, blockedID *commonpb.UserId) (bool, error) {
	out, err := s.client.DeleteItem(ctx, &dynamodb.DeleteItemInput{
		TableName:    aws.String(s.table),
		Key:          map[string]types.AttributeValue{attrPK: avS(ownerPK(ownerID)), attrSK: avS(blockedSK(blockedID))},
		ReturnValues: types.ReturnValueAllOld,
	})
	if err != nil {
		return false, err
	}
	// A returned old image means an entry was actually removed; an empty one
	// means the user was not blocked (a no-op delete).
	return len(out.Attributes) > 0, nil
}

func (s *store) IsBlocked(ctx context.Context, ownerID, blockedID *commonpb.UserId) (bool, error) {
	out, err := s.client.GetItem(ctx, &dynamodb.GetItemInput{
		TableName:            aws.String(s.table),
		Key:                  map[string]types.AttributeValue{attrPK: avS(ownerPK(ownerID)), attrSK: avS(blockedSK(blockedID))},
		ProjectionExpression: aws.String(attrPK),
	})
	if err != nil {
		return false, err
	}
	return len(out.Item) > 0, nil
}

func (s *store) GetBlocklistPage(ctx context.Context, ownerID *commonpb.UserId, cursor *blocklist.Cursor, limit int) ([]*blocklist.BlockedUser, error) {
	// Query the GSI for one owner's entries, most recent first. blocked_at is
	// immutable, so no watermark bound is needed; the partition holds exactly the
	// owner's blocklist, so pages come back dense with no filter expression.
	input := &dynamodb.QueryInput{
		TableName:                aws.String(s.table),
		IndexName:                aws.String(gsiByBlockedAt),
		KeyConditionExpression:   aws.String("#pk = :owner"),
		ExpressionAttributeNames: map[string]string{"#pk": attrPK},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":owner": avS(ownerPK(ownerID)),
		},
		ScanIndexForward: aws.Bool(false),
	}
	if limit > 0 {
		input.Limit = aws.Int32(int32(limit))
	}

	// The cursor carries (blocked_at, user_id) explicitly, so the GSI start key is
	// built directly without a lookup. A GSI start key must include the GSI key
	// (pk, blocked_at) and the base table key (pk, sk).
	if cursor != nil {
		input.ExclusiveStartKey = map[string]types.AttributeValue{
			attrPK:        avS(ownerPK(ownerID)),
			attrSK:        avS(blockedSK(cursor.UserID)),
			attrBlockedAt: avN(uint64(cursor.BlockedAt.UnixNano())),
		}
	}

	out, err := s.client.Query(ctx, input)
	if err != nil {
		return nil, err
	}
	entries := make([]*blocklist.BlockedUser, 0, len(out.Items))
	for _, item := range out.Items {
		e, err := blockedUserFromItem(item)
		if err != nil {
			return nil, err
		}
		entries = append(entries, e)
	}
	return entries, nil
}

// blockedUserFromItem builds a BlockedUser from a blocklist item. The blocked
// user ID is recovered from the item's sk rather than stored on its own.
func blockedUserFromItem(item map[string]types.AttributeValue) (*blocklist.BlockedUser, error) {
	userID, err := blockedIDFromSK(item)
	if err != nil {
		return nil, err
	}
	nanos, err := parseInt(item[attrBlockedAt])
	if err != nil {
		return nil, err
	}
	return &blocklist.BlockedUser{
		UserID:    userID,
		BlockedAt: time.Unix(0, nanos).UTC(),
	}, nil
}

func ownerPK(userID *commonpb.UserId) string { return userKeyPrefix + hex.EncodeToString(userID.Value) }
func blockedSK(userID *commonpb.UserId) string {
	return blockedKeyPrefix + hex.EncodeToString(userID.Value)
}

// blockedIDFromSK recovers a blocked user ID from an item's sk
// ("blocked#<hex>"), the inverse of blockedSK.
func blockedIDFromSK(item map[string]types.AttributeValue) (*commonpb.UserId, error) {
	sk := asS(item[attrSK])
	encoded, ok := strings.CutPrefix(sk, blockedKeyPrefix)
	if !ok {
		return nil, fmt.Errorf("unexpected sk %q", sk)
	}
	id, err := hex.DecodeString(encoded)
	if err != nil {
		return nil, fmt.Errorf("decoding user id from sk %q: %w", sk, err)
	}
	return &commonpb.UserId{Value: id}, nil
}

func avS(v string) types.AttributeValue { return &types.AttributeValueMemberS{Value: v} }
func avN(v uint64) types.AttributeValue {
	return &types.AttributeValueMemberN{Value: strconv.FormatUint(v, 10)}
}

func asS(av types.AttributeValue) string {
	if s, ok := av.(*types.AttributeValueMemberS); ok {
		return s.Value
	}
	return ""
}

func parseInt(av types.AttributeValue) (int64, error) {
	n, ok := av.(*types.AttributeValueMemberN)
	if !ok {
		return 0, fmt.Errorf("expected number attribute, got %T", av)
	}
	return strconv.ParseInt(n.Value, 10, 64)
}

func isConditionalCheckFailed(err error) bool {
	var ccf *types.ConditionalCheckFailedException
	return errors.As(err, &ccf)
}
