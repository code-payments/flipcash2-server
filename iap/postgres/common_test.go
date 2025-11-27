//go:build integration

package postgres

import (
	"os"
	"testing"

	"github.com/sirupsen/logrus"

	prismatest "github.com/code-payments/flipcash2-server/database/prisma/test"

	_ "github.com/jackc/pgx/v5/stdlib"
)

var testEnv *prismatest.TestEnv

func TestMain(m *testing.M) {
	log := logrus.StandardLogger()

	// Create a new test environment
	env, err := prismatest.NewTestEnv()
	if err != nil {
		log.WithError(err).Error("Error creating test environment")
		os.Exit(1)
	}

	// Set the test environment
	testEnv = env

	// Run tests
	code := m.Run()
	os.Exit(code)
}
