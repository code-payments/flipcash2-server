package test

import (
	"context"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"

	"github.com/ory/dockertest/v3"
	"github.com/ory/dockertest/v3/docker"

	"github.com/pkg/errors"

	"github.com/code-payments/ocp-server/retry"
	"github.com/code-payments/ocp-server/retry/backoff"
)

const (
	containerRepository = "amazon/dynamodb-local"
	containerVersion    = "2.5.2"
	containerAutoKill   = 120 // seconds

	port = "8000"

	region    = "us-east-1"
	accessKey = "dummy"
	secretKey = "dummy"
)

// TestEnv is a running dynamodb-local container with a connected client.
type TestEnv struct {
	Pool     *dockertest.Pool
	Client   *dynamodb.Client
	Endpoint string
}

// NewTestEnv starts a dynamodb-local Docker container and returns a connected
// client. It mirrors the postgres test harness.
func NewTestEnv() (*TestEnv, error) {
	pool, err := dockertest.NewPool("")
	if err != nil {
		return nil, errors.Wrap(err, "could not connect to docker")
	}

	resource, err := pool.RunWithOptions(&dockertest.RunOptions{
		Repository: containerRepository,
		Tag:        containerVersion,
		Cmd:        []string{"-jar", "DynamoDBLocal.jar", "-inMemory"},
	}, func(config *docker.HostConfig) {
		config.AutoRemove = true
		config.RestartPolicy = docker.RestartPolicy{Name: "no"}
	})
	if err != nil {
		return nil, errors.Wrap(err, "could not start resource")
	}
	resource.Expire(containerAutoKill)

	endpoint := "http://" + resource.GetHostPort(port+"/tcp")
	client := NewClient(endpoint)

	// Wait for the container to accept connections.
	_, err = retry.Retry(
		func() error {
			_, err := client.ListTables(context.Background(), &dynamodb.ListTablesInput{})
			return err
		},
		retry.Limit(60),
		retry.Backoff(backoff.Constant(500*time.Millisecond), 500*time.Millisecond),
	)
	if err != nil {
		return nil, errors.Wrap(err, "timed out waiting for dynamodb-local to become available")
	}

	return &TestEnv{Pool: pool, Client: client, Endpoint: endpoint}, nil
}

// NewClient builds a DynamoDB client pointed at the given local endpoint with
// dummy static credentials.
func NewClient(endpoint string) *dynamodb.Client {
	return dynamodb.New(dynamodb.Options{
		Region:       region,
		BaseEndpoint: aws.String(endpoint),
		Credentials:  credentials.NewStaticCredentialsProvider(accessKey, secretKey, ""),
	})
}
