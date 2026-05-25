package phone

import (
	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"
	phonepb "github.com/code-payments/flipcash2-protobuf-api/generated/go/phone/v1"

	"github.com/code-payments/flipcash2-server/phone/hash"
)

func SecureHash(phoneNumber *phonepb.PhoneNumber, pepper []byte) *commonpb.Hash {
	return hash.Secure(phoneNumber, pepper)
}
