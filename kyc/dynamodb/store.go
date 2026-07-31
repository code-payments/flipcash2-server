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
	thirdpartypb "github.com/code-payments/flipcash2-protobuf-api/generated/go/thirdparty/v1"

	"github.com/code-payments/flipcash2-server/kyc"
)

// The kyc_customers table holds one item per (user, partner): pk = "user#<id>",
// sk = "partner#<enum number>", carrying only the partner-side customer ID and
// a creation timestamp. Get is a point read; Create's condition enforces the
// one-customer-per-(user, partner) invariant. The by_customer_id GSI serves the
// reverse lookup when consuming partner events, which reference customers by
// their partner-side ID.
const (
	gsiByCustomerID = "by_customer_id"

	userKeyPrefix    = "user#"
	partnerKeyPrefix = "partner#"

	attrPK         = "pk"
	attrSK         = "sk"
	attrCustomerID = "customer_id"
	attrCreatedAt  = "created_at"
)

type store struct {
	client *dynamodb.Client
	table  string
}

// NewInDynamoDB returns a kyc.Store backed by the given DynamoDB table. Use
// CreateTable to provision it.
func NewInDynamoDB(client *dynamodb.Client, table string) kyc.Store {
	return &store{
		client: client,
		table:  table,
	}
}

func (s *store) Get(ctx context.Context, userID *commonpb.UserId, partner thirdpartypb.Partner) (*kyc.Record, error) {
	out, err := s.client.GetItem(ctx, &dynamodb.GetItemInput{
		TableName: aws.String(s.table),
		Key: map[string]types.AttributeValue{
			attrPK: avS(userPK(userID)),
			attrSK: avS(partnerSK(partner)),
		},
	})
	if err != nil {
		return nil, err
	}
	if len(out.Item) == 0 {
		return nil, kyc.ErrNotFound
	}
	return recordFromItem(userID, partner, out.Item)
}

func (s *store) GetByCustomerID(ctx context.Context, partner thirdpartypb.Partner, customerID string) (*kyc.Record, error) {
	// Customer IDs are unique within a partner, so the query returns at most
	// one item per partner; the sk check scopes the match to the requested
	// partner's ID space.
	out, err := s.client.Query(ctx, &dynamodb.QueryInput{
		TableName:                 aws.String(s.table),
		IndexName:                 aws.String(gsiByCustomerID),
		KeyConditionExpression:    aws.String(fmt.Sprintf("%s = :cid", attrCustomerID)),
		ExpressionAttributeValues: map[string]types.AttributeValue{":cid": avS(customerID)},
	})
	if err != nil {
		return nil, err
	}

	for _, item := range out.Items {
		if asS(item[attrSK]) != partnerSK(partner) {
			continue
		}
		userID, err := userIDFromPK(item)
		if err != nil {
			return nil, err
		}
		return recordFromItem(userID, partner, item)
	}
	return nil, kyc.ErrNotFound
}

func (s *store) Create(ctx context.Context, record *kyc.Record) error {
	_, err := s.client.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: aws.String(s.table),
		Item: map[string]types.AttributeValue{
			attrPK:         avS(userPK(record.UserID)),
			attrSK:         avS(partnerSK(record.Partner)),
			attrCustomerID: avS(record.CustomerID),
			attrCreatedAt:  avN(uint64(record.CreatedAt.UnixNano())),
		},
		ConditionExpression: aws.String(fmt.Sprintf("attribute_not_exists(%s)", attrPK)),
	})
	if err != nil {
		var conditionFailed *types.ConditionalCheckFailedException
		if errors.As(err, &conditionFailed) {
			return kyc.ErrExists
		}
		return err
	}
	return nil
}

func recordFromItem(userID *commonpb.UserId, partner thirdpartypb.Partner, item map[string]types.AttributeValue) (*kyc.Record, error) {
	nanos, err := parseInt(item[attrCreatedAt])
	if err != nil {
		return nil, err
	}
	return &kyc.Record{
		UserID:     &commonpb.UserId{Value: append([]byte(nil), userID.Value...)},
		Partner:    partner,
		CustomerID: asS(item[attrCustomerID]),
		CreatedAt:  time.Unix(0, nanos).UTC(),
	}, nil
}

func userPK(userID *commonpb.UserId) string {
	return userKeyPrefix + hex.EncodeToString(userID.Value)
}

// partnerSK encodes the partner by its stable proto enum number.
func partnerSK(partner thirdpartypb.Partner) string {
	return fmt.Sprintf("%s%d", partnerKeyPrefix, partner)
}

// userIDFromPK recovers a user ID from an item's pk ("user#<hex>"), the
// inverse of userPK.
func userIDFromPK(item map[string]types.AttributeValue) (*commonpb.UserId, error) {
	pk := asS(item[attrPK])
	encoded, ok := strings.CutPrefix(pk, userKeyPrefix)
	if !ok {
		return nil, fmt.Errorf("unexpected pk %q", pk)
	}
	id, err := hex.DecodeString(encoded)
	if err != nil {
		return nil, fmt.Errorf("decoding user id from pk %q: %w", pk, err)
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
