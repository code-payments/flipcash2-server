package phone

import (
	"crypto/hmac"
	"crypto/sha256"

	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"
	phonepb "github.com/code-payments/flipcash2-protobuf-api/generated/go/phone/v1"
)

func SecureHash(phoneNumber *phonepb.PhoneNumber, pepper []byte) *commonpb.Hash {
	mac := hmac.New(sha256.New, pepper)
	mac.Write([]byte(phoneNumber.Value))
	return &commonpb.Hash{Value: mac.Sum(nil)}
}
