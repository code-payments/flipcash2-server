//go:build cdpsandbox

// The cdpsandbox tag runs the main path against Coinbase's CDP Sandbox and
// checks that the fake (customerstest) agrees with it step for step. It needs
// a CDP Sandbox secret API key file (the JSON the CDP portal downloads, with
// `id` and `privateKey`), named by CDP_SANDBOX_KEY_FILE:
//
//	CDP_SANDBOX_KEY_FILE=/path/to/cdp_api_key.json go test -tags cdpsandbox -count=1 -run Sandbox ./cdp/customers/
//
// It submits only Coinbase's magic values and obviously fake data, creates
// one customer, and deletes it before it finishes (in cleanup too, should a
// step fail). It logs the customer's ID and nothing it submitted.
package customers_test

import (
	"context"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/code-payments/flipcash2-server/cdp"
	"github.com/code-payments/flipcash2-server/cdp/customers"
)

const sandboxReviewTimeout = 90 * time.Second

func sandboxClient(t *testing.T) *customers.Client {
	path := os.Getenv("CDP_SANDBOX_KEY_FILE")
	if path == "" {
		t.Fatal("CDP_SANDBOX_KEY_FILE is not set: set it to the path of a CDP Sandbox API key file")
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("reading CDP_SANDBOX_KEY_FILE: %v", err)
	}
	key, err := cdp.ParseAPIKeyJSON(data)
	require.NoError(t, err)

	client, err := customers.NewClient(&http.Client{Timeout: 30 * time.Second}, customers.SandboxBaseURL, key.ID, key.PrivateKey)
	require.NoError(t, err)
	return client
}

func TestMainPath_SandboxAgreesWithFake(t *testing.T) {
	client := sandboxClient(t)

	sandbox := mainPath(t, client, func(ctx context.Context, id string) *customers.Customer {
		deadline := time.Now().Add(sandboxReviewTimeout)
		for {
			c, err := client.Get(ctx, id)
			require.NoError(t, err)
			pending := false
			for _, r := range c.Requirements {
				if r.Status == customers.RequirementPending {
					pending = true
				}
			}
			if !pending {
				return c
			}
			if time.Now().After(deadline) {
				t.Fatalf("review still pending after %s", sandboxReviewTimeout)
			}
			time.Sleep(time.Second)
		}
	})

	fake := runMainPathOnFake(t)
	assert.Equal(t, fake, sandbox)
}
