package dynamodb

import (
	"context"
	"encoding/hex"
	"fmt"
	"strconv"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"

	"github.com/code-payments/flipcash2-server/badge"
)

// The badge store uses a single table, one item per user keyed by
// pk = "user#<id>" with the count in attrCount. Mutations use DynamoDB's atomic
// ADD / SET so the concurrent increments from a chat's fan-out serialize on the
// item without a read-modify-write race.
const (
	attrPK    = "pk"
	attrCount = "badge_count" // "count" is a DynamoDB reserved word.
)

type store struct {
	client *dynamodb.Client
	table  string
}

// NewInDynamoDB returns a badge.Store backed by the given DynamoDB table. Use
// CreateTables to provision it.
func NewInDynamoDB(client *dynamodb.Client, table string) badge.Store {
	return &store{
		client: client,
		table:  table,
	}
}

func (s *store) Increment(ctx context.Context, userID *commonpb.UserId, delta uint64) (uint64, error) {
	out, err := s.client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName:                 aws.String(s.table),
		Key:                       map[string]types.AttributeValue{attrPK: avS(userPK(userID))},
		UpdateExpression:          aws.String(fmt.Sprintf("ADD %s :delta", attrCount)),
		ExpressionAttributeValues: map[string]types.AttributeValue{":delta": avN(delta)},
		ReturnValues:              types.ReturnValueUpdatedNew,
	})
	if err != nil {
		return 0, err
	}
	// Read the post-increment value straight off the response — it is always
	// current, unlike a follow-up eventually-consistent GetItem.
	return parseN(out.Attributes[attrCount])
}

func (s *store) Get(ctx context.Context, userID *commonpb.UserId) (uint64, error) {
	out, err := s.client.GetItem(ctx, &dynamodb.GetItemInput{
		TableName: aws.String(s.table),
		Key:       map[string]types.AttributeValue{attrPK: avS(userPK(userID))},
	})
	if err != nil {
		return 0, err
	}
	if len(out.Item) == 0 {
		return 0, nil
	}
	return parseN(out.Item[attrCount])
}

func (s *store) Reset(ctx context.Context, userID *commonpb.UserId) error {
	// SET (rather than delete) keeps the per-user item as a stable anchor for any
	// future attributes (e.g. per-chat applied watermarks) without changing the
	// observable zero.
	_, err := s.client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName:                 aws.String(s.table),
		Key:                       map[string]types.AttributeValue{attrPK: avS(userPK(userID))},
		UpdateExpression:          aws.String(fmt.Sprintf("SET %s = :zero", attrCount)),
		ExpressionAttributeValues: map[string]types.AttributeValue{":zero": avN(0)},
	})
	return err
}

func userPK(userID *commonpb.UserId) string { return "user#" + hex.EncodeToString(userID.Value) }

func avS(v string) types.AttributeValue { return &types.AttributeValueMemberS{Value: v} }
func avN(v uint64) types.AttributeValue {
	return &types.AttributeValueMemberN{Value: strconv.FormatUint(v, 10)}
}

func parseN(av types.AttributeValue) (uint64, error) {
	n, ok := av.(*types.AttributeValueMemberN)
	if !ok {
		return 0, fmt.Errorf("expected number attribute, got %T", av)
	}
	return strconv.ParseUint(n.Value, 10, 64)
}
