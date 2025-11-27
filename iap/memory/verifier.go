package memory

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"fmt"
	"strings"

	"github.com/code-payments/flipcash2-server/iap"
)

// MemoryVerifier is an in-memory verifier that checks an ed25519 signature on
// the receipt. For testing purposes, the "receipt" is actually a message that,
// when signed by the owner secret, is considered valid.
type MemoryVerifier struct {
	publicKey    ed25519.PublicKey
	validProduct string
}

// NewMemoryVerifier creates a new MemoryVerifier from a given public key.
func NewMemoryVerifier(pubKey ed25519.PublicKey, validProduct string) iap.Verifier {
	return &MemoryVerifier{
		publicKey:    pubKey,
		validProduct: validProduct,
	}
}

func (m *MemoryVerifier) VerifyReceipt(ctx context.Context, receipt, product string) (bool, error) {
	// For simplicity, the receipt format is: base64(signature)|message

	signature, message, err := parseReceipt(receipt)
	if err != nil {
		// Not returning an error here because we're testing the verifier, not the
		// receipt parsing.

		return false, nil
	}

	if product != m.validProduct {
		return false, nil
	}

	// Verify the signature.
	if ed25519.Verify(m.publicKey, message, signature) {
		return true, nil
	}

	return false, nil
}

func (m *MemoryVerifier) GetReceiptIdentifier(ctx context.Context, receipt string) ([]byte, error) {
	signature, _, err := parseReceipt(receipt)
	if err != nil {
		return nil, err
	}

	return signature, nil
}

func GenerateKeyPair() (ed25519.PublicKey, ed25519.PrivateKey, error) {
	return ed25519.GenerateKey(rand.Reader)
}

func GenerateValidReceipt(owner ed25519.PrivateKey, message string) string {
	signature := ed25519.Sign(owner, []byte(message))
	return base64.StdEncoding.EncodeToString(signature) + "|" + message
}

func parseReceipt(receipt string) (signature []byte, message []byte, err error) {
	parts := strings.Split(receipt, "|")
	if len(parts) != 2 {
		return nil, nil, fmt.Errorf("invalid receipt format: %s", receipt)
	}

	signature, err = base64.StdEncoding.DecodeString(parts[0])
	if err != nil {
		return nil, nil, fmt.Errorf("error decoding signature: %w", err)
	}

	message = []byte(parts[1])
	return signature, message, nil
}
