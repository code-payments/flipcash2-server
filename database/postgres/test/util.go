package test

import (
	"database/sql"
	"fmt"
	"time"

	"github.com/ory/dockertest/v3"
	"github.com/ory/dockertest/v3/docker"

	"github.com/pkg/errors"

	_ "github.com/jackc/pgx/v5/stdlib"

	"github.com/code-payments/ocp-server/retry"
	"github.com/code-payments/ocp-server/retry/backoff"
)

const (
	containerName     = "postgres"
	containerVersion  = "10.4"
	containerAutoKill = 120 // seconds

	port     = 5432
	user     = "localtest"
	password = "localpassword"
	dbname   = "testdb"
)

const (
	postgresUserEnv     = "POSTGRES_USER=" + user
	postgresPasswordEnv = "POSTGRES_PASSWORD=" + password
	postgresDbEnv       = "POSTGRES_DB=" + dbname
)

// StartPostgresDB starts a Docker container using the postgres image and returns a postgres client for testing purposes.
func StartPostgresDB(pool *dockertest.Pool) (url string, err error) {
	// Pulls the image, creates a container based on it and runs it
	resource, err := pool.RunWithOptions(&dockertest.RunOptions{
		Repository: containerName,
		Tag:        containerVersion,
		Env: []string{
			"listen_addresses = '*'",
			postgresUserEnv,
			postgresPasswordEnv,
			postgresDbEnv,
		},
	}, func(config *docker.HostConfig) {
		// set AutoRemove to true so that stopped container goes away by itself
		config.AutoRemove = true
		config.RestartPolicy = docker.RestartPolicy{Name: "no"}
	})

	// Check if the container resource was generated as expected
	if err != nil {
		return "", errors.Wrap(err, "could not start resource")
	}

	// Uncomment this to view docker container logs (note: this will fully consume os.Stdout)
	/*
		opts := docker.LogsOptions{
			Context: context.Background(),

			Stderr:      true,
			Stdout:      true,
			Follow:      true,
			Timestamps:  true,
			RawTerminal: true,

			Container: resource.Container.ID,

			OutputStream: os.Stdout,
		}
		pool.Client.Logs(opts)
	*/

	hostAndPort := resource.GetHostPort(fmt.Sprintf("%d/tcp", port))
	databaseUrl := fmt.Sprintf("postgres://%s:%s@%s/%s?sslmode=disable", user, password, hostAndPort, dbname)

	// logrus.StandardLogger().Println("Connecting to database on url: ", databaseUrl)
	// logrus.StandardLogger().Println("Setting container auto-kill to: ", containerAutoKill, " seconds")

	// You may need to adjust this number if it is too low for your test environment.
	resource.Expire(containerAutoKill) // Tell docker to hard kill the container in 120 seconds

	return databaseUrl, nil
}

// WaitForConnection waits for the postgres container to be ready to accept connections.
func WaitForConnection(databaseUrl string, waitonly bool) (db *sql.DB, closeFunc func(), err error) {

	_, err = retry.Retry(
		func() error {
			db, err = sql.Open("pgx", databaseUrl)
			if err != nil {
				return err
			}
			return db.Ping()
		},
		retry.Limit(50),
		retry.Backoff(backoff.Constant(500*time.Millisecond), 500*time.Second),
	)

	if err != nil {
		return nil, nil, errors.Wrap(err, "timed out waiting for postgres container to become available")
	}

	if waitonly {
		db.Close()
		return nil, nil, nil
	}

	return db, func() { db.Close() }, nil
}
