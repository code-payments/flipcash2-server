package test

import (
	"os"
	"os/exec"
	"path/filepath"
	"runtime"

	"github.com/ory/dockertest/v3"

	postgrestest "github.com/code-payments/flipcash2-server/database/postgres/test"
)

type TestEnv struct {
	TestPool    *dockertest.Pool
	DatabaseUrl string
}

func NewTestEnv() (*TestEnv, error) {
	var err error
	testPool, err := dockertest.NewPool("")
	if err != nil {
		return nil, err
	}

	// Start a postgres container
	databaseUrl, err := postgrestest.StartPostgresDB(testPool)
	if err != nil {
		return nil, err
	}

	// Wait for the database to be ready
	_, _, err = postgrestest.WaitForConnection(databaseUrl, true)
	if err != nil {
		return nil, err
	}

	// Apply sql migrations
	err = RunPrismaMigrateDeploy(databaseUrl)
	if err != nil {
		return nil, err
	}

	return &TestEnv{
		TestPool:    testPool,
		DatabaseUrl: databaseUrl,
	}, nil
}

// A bit of a hack, we should call the prisma migration programmatically
func RunPrismaMigrateDeploy(databaseUrl string) error {
	// Get the directory of the current file
	_, filePath, _, ok := runtime.Caller(0)
	if !ok {
		return os.ErrInvalid
	}

	prismaDir := filepath.Join(filepath.Dir(filePath), "../")
	if _, err := os.Stat(prismaDir); os.IsNotExist(err) {
		return err // prisma folder doesn't exist
	}

	cmd := exec.Command("go", "run", "github.com/steebchen/prisma-client-go", "migrate", "deploy")
	cmd.Env = append(os.Environ(), "DATABASE_URL="+databaseUrl)
	cmd.Dir = prismaDir // Set the working directory
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	return cmd.Run()
}
