package paymentrequest

import (
	"testing"

	"github.com/stretchr/testify/require"

	messagingpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/messaging/v1"
	paymentrequestpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/paymentrequest/v1"

	"github.com/code-payments/flipcash2-server/chat"
)

// TestTextVerdict pins the text a chat takes, including a scheme other than
// the chat's, which the request's validation keeps from reaching the server
// today (the proto admits CHAT_KEY_XCHACHA20POLY1305 alone).
func TestTextVerdict(t *testing.T) {
	plaintext := &paymentrequestpb.Split{Text: &paymentrequestpb.Split_Plaintext{Plaintext: &paymentrequestpb.PaymentRequestText{Title: "Dinner"}}}
	encrypted := func(scheme paymentrequestpb.EncryptedPaymentRequestText_Scheme) *paymentrequestpb.Split {
		return &paymentrequestpb.Split{Text: &paymentrequestpb.Split_Encrypted{Encrypted: &paymentrequestpb.EncryptedPaymentRequestText{Scheme: scheme}}}
	}
	public := chat.SpeakerStanding{CanSpeak: true, Encryption: chat.EncryptionNone}
	private := chat.SpeakerStanding{CanSpeak: true, Encryption: chat.EncryptionRequired, Scheme: messagingpb.EncryptedContent_CHAT_KEY_XCHACHA20POLY1305}
	dm := chat.SpeakerStanding{CanSpeak: true, Encryption: chat.EncryptionRequired, Scheme: messagingpb.EncryptedContent_X25519_XCHACHA20POLY1305}

	for _, tc := range []struct {
		name    string
		speaker chat.SpeakerStanding
		split   *paymentrequestpb.Split
		want    paymentrequestpb.CreatePaymentRequestResponse_Result
	}{
		{"public plaintext", public, plaintext, paymentrequestpb.CreatePaymentRequestResponse_OK},
		{"public encrypted", public, encrypted(paymentrequestpb.EncryptedPaymentRequestText_CHAT_KEY_XCHACHA20POLY1305), paymentrequestpb.CreatePaymentRequestResponse_ENCRYPTION_NOT_ALLOWED},
		{"private encrypted", private, encrypted(paymentrequestpb.EncryptedPaymentRequestText_CHAT_KEY_XCHACHA20POLY1305), paymentrequestpb.CreatePaymentRequestResponse_OK},
		{"private plaintext", private, plaintext, paymentrequestpb.CreatePaymentRequestResponse_ENCRYPTION_REQUIRED},
		{"private wrong scheme", private, encrypted(paymentrequestpb.EncryptedPaymentRequestText_UNKNOWN), paymentrequestpb.CreatePaymentRequestResponse_ENCRYPTION_NOT_ALLOWED},
		{"chat scheme with no request scheme", chat.SpeakerStanding{CanSpeak: true, Encryption: chat.EncryptionRequired}, encrypted(paymentrequestpb.EncryptedPaymentRequestText_CHAT_KEY_XCHACHA20POLY1305), paymentrequestpb.CreatePaymentRequestResponse_ENCRYPTION_NOT_ALLOWED},
		{"dm", dm, encrypted(paymentrequestpb.EncryptedPaymentRequestText_CHAT_KEY_XCHACHA20POLY1305), paymentrequestpb.CreatePaymentRequestResponse_ENCRYPTION_NOT_ALLOWED},
	} {
		result, ok := textVerdict(tc.speaker, tc.split)
		require.Equal(t, tc.want, result, tc.name)
		require.Equal(t, tc.want == paymentrequestpb.CreatePaymentRequestResponse_OK, ok, tc.name)
	}
}
