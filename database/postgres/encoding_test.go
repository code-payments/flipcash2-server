package pg

import (
	"testing"
)

func TestEncodeDecode(t *testing.T) {
	data := []byte("Hello, World!")

	tests := []struct {
		name       string
		encodeType EncodeType
	}{
		{"Base64", Base64},
		{"Base58", Base58},
		{"Hex", Hex},
		{"DefaultBase64", DefaultEncodeType},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Encode the data
			encoded := Encode(data, tt.encodeType)

			// Decode the encoded data
			decoded, err := Decode(encoded)
			if err != nil {
				t.Fatalf("failed to decode %s: %v", tt.name, err)
			}

			// Validate the decoded data
			if string(decoded) != string(data) {
				t.Errorf("decoded data does not match original. Got: %s, Expected: %s", string(decoded), string(data))
			}
		})
	}
}

func TestDecodeInvalidFormat(t *testing.T) {
	invalidData := "invalid_format_without_colon"

	_, err := Decode(invalidData)
	if err == nil {
		t.Fatal("expected error for invalid format, got none")
	}
}

func TestDecodeUnsupportedEncoding(t *testing.T) {
	unsupportedData := "unknown:SGVsbG8sIFdvcmxkIQ=="

	_, err := Decode(unsupportedData)
	if err == nil {
		t.Fatal("expected error for unsupported encoding, got none")
	}
}
