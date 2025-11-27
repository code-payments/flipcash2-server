package memory

import (
	"testing"

	account "github.com/code-payments/flipcash2-server/account/memory"
	"github.com/code-payments/flipcash2-server/iap"
	"github.com/code-payments/flipcash2-server/iap/tests"
)

func TestIAP_MemoryServer(t *testing.T) {
	pub, priv, err := GenerateKeyPair()
	if err != nil {
		t.Fatalf("error generating key pair: %v", err)
	}

	product := iap.CreateAccountProductID
	verifier := NewMemoryVerifier(pub, product)
	validReceiptFunc := func(msg string) (string, string) {
		return GenerateValidReceipt(priv, msg), product
	}

	accounts := account.NewInMemory()
	iaps := NewInMemory()

	teardown := func() {
		iaps.(*InMemoryStore).reset()
	}

	tests.RunServerTests(t, accounts, iaps, verifier, validReceiptFunc, teardown)
}
