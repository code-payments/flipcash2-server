package tests

import (
	"context"
	"testing"

	"github.com/code-payments/flipcash2-server/iap"
)

type MessageGenerator func() string
type ValidReceiptFromMessage func(message string) (string, string)

func RunGenericVerifierTests(t *testing.T, v iap.Verifier, msgGen MessageGenerator, validReceiptFunc ValidReceiptFromMessage, teardown func()) {
	for _, testFunc := range []func(t *testing.T, v iap.Verifier, msgGen MessageGenerator, validReceiptFunc ValidReceiptFromMessage){
		testValidReceipt,
		testInvalidReceipt,
	} {
		testFunc(t, v, msgGen, validReceiptFunc)
		teardown()
	}
}

func testValidReceipt(t *testing.T, v iap.Verifier, msgGen MessageGenerator, validReceiptFunc ValidReceiptFromMessage) {
	ctx := context.Background()

	message := msgGen()                                     // get the message
	validReceipt, validProduct := validReceiptFunc(message) // create a valid receipt from the message

	//t.Logf("message: %s", message)
	//t.Logf("valid receipt: %s", validReceipt)

	identifier, err := v.GetReceiptIdentifier(ctx, validReceipt)
	if err != nil {
		t.Fatalf("unexpected error getting identifier: %v", err)
	}
	if identifier == nil {
		t.Errorf("expected identifier to be non-nil")
	}

	valid, err := v.VerifyReceipt(ctx, validReceipt, validProduct)
	if err != nil {
		t.Fatalf("unexpected error verifying valid receipt: %v", err)
	}
	if !valid {
		t.Errorf("expected receipt to be valid, got invalid")
	}
}

func testInvalidReceipt(t *testing.T, v iap.Verifier, msgGen MessageGenerator, validReceiptFunc ValidReceiptFromMessage) {
	ctx := context.Background()

	message := msgGen()
	_, validProduct := validReceiptFunc(message) // create a valid receipt from the message

	// Just use the word "invalid" as an invalid receipt or product.
	invalidReceipt := "invalid"

	valid, _ := v.VerifyReceipt(ctx, invalidReceipt, validProduct)
	/*
		if err != nil {
			t.Fatalf("unexpected error verifying invalid receipt: %v", err)
		}
	*/
	if valid {
		t.Errorf("expected receipt to be invalid, got valid")
	}

}
