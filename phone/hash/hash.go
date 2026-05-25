package hash

import (
	"crypto/hmac"
	"crypto/sha256"

	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"
	phonepb "github.com/code-payments/flipcash2-protobuf-api/generated/go/phone/v1"
)

// Secure returns the HMAC-SHA256 hash of the phone number's E.164 value
// using the provided pepper.
func Secure(phoneNumber *phonepb.PhoneNumber, pepper []byte) *commonpb.Hash {
	mac := hmac.New(sha256.New, pepper)
	mac.Write([]byte(phoneNumber.Value))
	return &commonpb.Hash{Value: mac.Sum(nil)}
}
