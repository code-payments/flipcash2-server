//go:build integration

package dynamodb

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/code-payments/flipcash2-server/cluster/tests"
)

const (
	membersTable       = "cluster_members_test"
	claimsTable        = "cluster_claims_test"
	subscriptionsTable = "cluster_subscriptions_test"
)

func TestCluster_DynamoDBStore(t *testing.T) {
	require.NoError(t, CreateTables(context.Background(), testEnv.Client, membersTable, claimsTable, subscriptionsTable))

	testStore := NewInDynamoDB(testEnv.Client, membersTable, claimsTable, subscriptionsTable)
	teardown := func() {
		testStore.(*store).reset()
	}
	tests.RunStoreTests(t, testStore, teardown)
}

func TestCluster_DynamoDBRuntime(t *testing.T) {
	require.NoError(t, CreateTables(context.Background(), testEnv.Client, membersTable, claimsTable, subscriptionsTable))

	testStore := NewInDynamoDB(testEnv.Client, membersTable, claimsTable, subscriptionsTable)
	teardown := func() {
		testStore.(*store).reset()
	}
	tests.RunClusterTests(t, testStore, teardown)
}

func TestCluster_DynamoDBProductionTimings(t *testing.T) {
	require.NoError(t, CreateTables(context.Background(), testEnv.Client, membersTable, claimsTable, subscriptionsTable))

	testStore := NewInDynamoDB(testEnv.Client, membersTable, claimsTable, subscriptionsTable)
	teardown := func() {
		testStore.(*store).reset()
	}
	tests.RunProductionTimingTests(t, testStore, teardown)
}

func TestCluster_DynamoDBE2E(t *testing.T) {
	require.NoError(t, CreateTables(context.Background(), testEnv.Client, membersTable, claimsTable, subscriptionsTable))

	testStore := NewInDynamoDB(testEnv.Client, membersTable, claimsTable, subscriptionsTable)
	teardown := func() {
		testStore.(*store).reset()
	}
	tests.RunE2ETests(t, testStore, teardown)
}
