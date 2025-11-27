package iap

import "context"

type Verifier interface {

	// VerifyReceipt takes a IAP receipt (for iOS/Android usually a
	// base64-encoded string, for memory a signed payload) and determines if it
	// is valid and has purchased the required feature.
	VerifyReceipt(ctx context.Context, receipt, product string) (bool, error)

	// GetReceiptIdentifier takes a IAP receipt and returns the identifier of the
	// purchased feature. This can be used to identify the receipt in the system.
	GetReceiptIdentifier(ctx context.Context, receipt string) ([]byte, error)
}
