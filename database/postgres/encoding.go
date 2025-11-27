package pg

import (
	"encoding/base64"
	"encoding/hex"
	"errors"
	"strings"

	"github.com/mr-tron/base58"
)

// EncodeType is an enum-like type for encoding types.
type EncodeType string

const (
	Base64            EncodeType = "b64"
	Base58            EncodeType = "b58"
	Hex               EncodeType = "hex"
	DefaultEncodeType            = Base64
)

// Encode encodes the input byte slice into the specified format, prefixed with
// a short encoding type.
// If no encodeType is provided, it defaults to Base64.
func Encode(value []byte, encodeType ...EncodeType) string {
	encType := DefaultEncodeType
	if len(encodeType) > 0 {
		encType = encodeType[0]
	}

	var encodedValue string
	switch encType {
	case Base58:
		encodedValue = base58.Encode(value)
	case Hex:
		encodedValue = hex.EncodeToString(value)
	case Base64:
		fallthrough
	default:
		encodedValue = base64.StdEncoding.EncodeToString(value)
	}

	// Prefix the encoded value with the short encoding type
	return string(encType) + ":" + encodedValue
}

// Decode decodes the input string by automatically determining the encoding
// type from its short prefix.
func Decode(value string) ([]byte, error) {
	// Split the prefix and the encoded value
	parts := strings.SplitN(value, ":", 2)
	if len(parts) != 2 {
		return nil, errors.New("invalid encoded value format")
	}

	encType, encodedValue := EncodeType(parts[0]), parts[1]

	// Decode based on the prefix type
	switch encType {
	case Base58:
		return base58.Decode(encodedValue)
	case Hex:
		return hex.DecodeString(encodedValue)
	case Base64:
		return base64.StdEncoding.DecodeString(encodedValue)
	default:
		return nil, errors.New("unsupported encoding type")
	}
}
