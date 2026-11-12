//go:build cdpsandbox

// The cdpsandbox tag runs the Kyc server against Coinbase's CDP Sandbox. It
// needs a CDP Sandbox secret API key file (the JSON the CDP portal downloads,
// with `id` and `privateKey`), named by CDP_SANDBOX_KEY_FILE:
//
//	CDP_SANDBOX_KEY_FILE=/path/to/cdp_api_key.json go test -tags cdpsandbox -count=1 -v -run Sandbox ./kyc/...
//
// Each path creates one customer and deletes it when it ends, pass or fail.
// It logs customer IDs and nothing it submitted.
package memory

import (
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	account_memory "github.com/code-payments/flipcash2-server/account/memory"
	"github.com/code-payments/flipcash2-server/cdp"
	"github.com/code-payments/flipcash2-server/cdp/customers"
	"github.com/code-payments/flipcash2-server/kyc/coinbase/tests"
)

func TestCoinbaseKyc_MemorySandbox(t *testing.T) {
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

	accounts := account_memory.NewInMemory()
	testStore := NewInMemory()
	teardown := func() {
		testStore.(*memory).reset()
	}
	tests.RunSandboxTests(t, accounts, testStore, client, teardown)
}
