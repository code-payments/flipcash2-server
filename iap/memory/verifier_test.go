package memory

import (
	"testing"

	"github.com/code-payments/flipcash2-server/iap/tests"
)

func TestMemoryVerifier(t *testing.T) {
	pub, priv, err := GenerateKeyPair()
	if err != nil {
		t.Fatalf("error generating key pair: %v", err)
	}

	product := "valid_product"

	verifier := NewMemoryVerifier(pub, product)
	messageGenerator := func() string {
		return "paid_feature"
	}
	validReceiptFunc := func(msg string) (string, string) {
		return GenerateValidReceipt(priv, msg), product
	}

	teardown := func() {}

	tests.RunGenericVerifierTests(t,
		verifier, messageGenerator, validReceiptFunc, teardown)
}
