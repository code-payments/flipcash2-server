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

	"github.com/code-payments/flipcash2-server/cdp/customers"
	"github.com/code-payments/flipcash2-server/kyc/coinbase"
)

// The store uses a single table, one item per user keyed by
// pk = "user#<lowercase hex user id>", holding the Coinbase customer ID in
// attrCustomerID and the record's creation time in attrCreatedAt as Unix
// nanoseconds, so a record reads back exactly as written. The by_customer GSI,
// hashed on attrCustomerID, finds the user a customer belongs to.
const (
	attrPK         = "pk"
	attrCustomerID = "customer_id"
	attrCreatedAt  = "created_at"

	gsiByCustomer = "by_customer"

	userPKPrefix = "user#"
)

type store struct {
	client *dynamodb.Client
	table  string
}

// NewInDynamoDB returns a coinbase.Store backed by the given DynamoDB table.
// Use CreateTables to provision it.
func NewInDynamoDB(client *dynamodb.Client, table string) coinbase.Store {
	return &store{
		client: client,
		table:  table,
	}
}

// PutCustomer is one PutItem conditioned on the user having no item. A lost
// condition returns the stored item with the failure, so the loser of a race
// is answered with no second read; only when the failure carries no item is
// the user's record read again, strongly consistent.
func (s *store) PutCustomer(ctx context.Context, record *coinbase.CustomerRecord) (*coinbase.CustomerRecord, bool, error) {
	if err := record.Validate(); err != nil {
		return nil, false, err
	}

	_, err := s.client.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: aws.String(s.table),
		Item: map[string]types.AttributeValue{
			attrPK:         avS(userPK(record.UserID)),
			attrCustomerID: avS(record.CustomerID),
			attrCreatedAt:  avN(strconv.FormatInt(record.CreatedAt.UnixNano(), 10)),
		},
		ConditionExpression:                 aws.String("attribute_not_exists(#pk)"),
		ExpressionAttributeNames:            map[string]string{"#pk": attrPK},
		ReturnValuesOnConditionCheckFailure: types.ReturnValuesOnConditionCheckFailureAllOld,
	})
	if err == nil {
		return record.Clone(), true, nil
	}
	var ccf *types.ConditionalCheckFailedException
	if !errors.As(err, &ccf) {
		return nil, false, err
	}
	if len(ccf.Item) > 0 {
		stored, err := recordFromItem(ccf.Item)
		if err != nil {
			return nil, false, err
		}
		return stored, false, nil
	}
	stored, err := s.GetCustomer(ctx, record.UserID)
	if err != nil {
		return nil, false, fmt.Errorf("reading the record that stands: %w", err)
	}
	return stored, false, nil
}

func (s *store) GetCustomer(ctx context.Context, userID *commonpb.UserId) (*coinbase.CustomerRecord, error) {
	out, err := s.client.GetItem(ctx, &dynamodb.GetItemInput{
		TableName:      aws.String(s.table),
		Key:            map[string]types.AttributeValue{attrPK: avS(userPK(userID))},
		ConsistentRead: aws.Bool(true),
	})
	if err != nil {
		return nil, err
	}
	if len(out.Item) == 0 {
		return nil, coinbase.ErrNotFound
	}
	return recordFromItem(out.Item)
}

// GetCustomerByID queries the by_customer GSI, which is eventually consistent
// (a GSI offers no other read).
func (s *store) GetCustomerByID(ctx context.Context, customerID string) (*coinbase.CustomerRecord, error) {
	if !customers.IsCustomerID(customerID) {
		return nil, coinbase.ErrNotFound
	}

	out, err := s.client.Query(ctx, &dynamodb.QueryInput{
		TableName:                aws.String(s.table),
		IndexName:                aws.String(gsiByCustomer),
		KeyConditionExpression:   aws.String("#customer = :customer"),
		ExpressionAttributeNames: map[string]string{"#customer": attrCustomerID},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":customer": avS(customerID),
		},
		Limit: aws.Int32(1),
	})
	if err != nil {
		return nil, err
	}
	if len(out.Items) == 0 {
		return nil, coinbase.ErrNotFound
	}
	return recordFromItem(out.Items[0])
}

// recordFromItem decodes a stored item and validates it. An item that fails
// either is an error, never skipped.
func recordFromItem(item map[string]types.AttributeValue) (*coinbase.CustomerRecord, error) {
	pk, err := parseS(item[attrPK])
	if err != nil {
		return nil, fmt.Errorf("parsing %s: %w", attrPK, err)
	}
	hexID, ok := strings.CutPrefix(pk, userPKPrefix)
	if !ok {
		return nil, fmt.Errorf("parsing %s: missing %q prefix", attrPK, userPKPrefix)
	}
	userID, err := hex.DecodeString(hexID)
	if err != nil {
		return nil, fmt.Errorf("parsing %s: %w", attrPK, err)
	}

	customerID, err := parseS(item[attrCustomerID])
	if err != nil {
		return nil, fmt.Errorf("parsing %s: %w", attrCustomerID, err)
	}

	createdAtRaw, err := parseN(item[attrCreatedAt])
	if err != nil {
		return nil, fmt.Errorf("parsing %s: %w", attrCreatedAt, err)
	}
	createdAtNanos, err := strconv.ParseInt(createdAtRaw, 10, 64)
	if err != nil {
		return nil, fmt.Errorf("parsing %s: %w", attrCreatedAt, err)
	}

	record := &coinbase.CustomerRecord{
		UserID:     &commonpb.UserId{Value: userID},
		CustomerID: customerID,
		CreatedAt:  time.Unix(0, createdAtNanos).UTC(),
	}
	if err := record.Validate(); err != nil {
		return nil, fmt.Errorf("stored record: %w", err)
	}
	return record, nil
}

func userPK(userID *commonpb.UserId) string {
	return userPKPrefix + hex.EncodeToString(userID.GetValue())
}

func avS(v string) types.AttributeValue { return &types.AttributeValueMemberS{Value: v} }
func avN(v string) types.AttributeValue { return &types.AttributeValueMemberN{Value: v} }

func parseS(av types.AttributeValue) (string, error) {
	s, ok := av.(*types.AttributeValueMemberS)
	if !ok {
		return "", fmt.Errorf("expected string attribute, got %T", av)
	}
	return s.Value, nil
}

func parseN(av types.AttributeValue) (string, error) {
	n, ok := av.(*types.AttributeValueMemberN)
	if !ok {
		return "", fmt.Errorf("expected number attribute, got %T", av)
	}
	return n.Value, nil
}
