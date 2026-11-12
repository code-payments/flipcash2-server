//go:build integration

package dynamodb

import (
	"os"
	"testing"

	"github.com/sirupsen/logrus"

	dynamotest "github.com/code-payments/flipcash2-server/database/dynamodb/test"
)

var testEnv *dynamotest.TestEnv

func TestMain(m *testing.M) {
	log := logrus.StandardLogger()

	env, err := dynamotest.NewTestEnv()
	if err != nil {
		log.WithError(err).Error("Error creating dynamodb test environment")
		os.Exit(1)
	}

	testEnv = env

	os.Exit(m.Run())
}
