package dynamodb

import (
	"context"
	"errors"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// CreateTable provisions the kyc_customers table with on-demand billing. It is
// keyed by (pk, sk) = (user, partner), with a GSI on customer_id for the
// reverse lookup when consuming partner events. It is idempotent and blocks
// until the table is ACTIVE.
func CreateTable(ctx context.Context, client *dynamodb.Client, table string) error {
	input := &dynamodb.CreateTableInput{
		TableName:   aws.String(table),
		BillingMode: types.BillingModePayPerRequest,
		AttributeDefinitions: []types.AttributeDefinition{
			{AttributeName: aws.String(attrPK), AttributeType: types.ScalarAttributeTypeS},
			{AttributeName: aws.String(attrSK), AttributeType: types.ScalarAttributeTypeS},
			{AttributeName: aws.String(attrCustomerID), AttributeType: types.ScalarAttributeTypeS},
		},
		KeySchema: []types.KeySchemaElement{
			{AttributeName: aws.String(attrPK), KeyType: types.KeyTypeHash},
			{AttributeName: aws.String(attrSK), KeyType: types.KeyTypeRange},
		},
		GlobalSecondaryIndexes: []types.GlobalSecondaryIndex{
			{
				IndexName: aws.String(gsiByCustomerID),
				KeySchema: []types.KeySchemaElement{
					{AttributeName: aws.String(attrCustomerID), KeyType: types.KeyTypeHash},
				},
				Projection: &types.Projection{ProjectionType: types.ProjectionTypeAll},
			},
		},
	}

	if _, err := client.CreateTable(ctx, input); err != nil {
		var inUse *types.ResourceInUseException
		if !errors.As(err, &inUse) {
			return err
		}
		// Already exists; fall through to wait for ACTIVE.
	}
	return dynamodb.NewTableExistsWaiter(client).Wait(ctx, &dynamodb.DescribeTableInput{
		TableName: aws.String(table),
	}, 2*time.Minute)
}

// reset deletes every item from the table, for tests.
func (s *store) reset() {
	ctx := context.Background()

	var startKey map[string]types.AttributeValue
	for {
		out, err := s.client.Scan(ctx, &dynamodb.ScanInput{
			TableName:            aws.String(s.table),
			ProjectionExpression: aws.String(attrPK + ", " + attrSK),
			ExclusiveStartKey:    startKey,
		})
		if err != nil {
			panic(err)
		}

		for _, item := range out.Items {
			if _, err := s.client.DeleteItem(ctx, &dynamodb.DeleteItemInput{
				TableName: aws.String(s.table),
				Key: map[string]types.AttributeValue{
					attrPK: item[attrPK],
					attrSK: item[attrSK],
				},
			}); err != nil {
				panic(err)
			}
		}

		if len(out.LastEvaluatedKey) == 0 {
			return
		}
		startKey = out.LastEvaluatedKey
	}
}
