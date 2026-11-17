package flipcashteam

import (
	"crypto/ed25519"
	"errors"
	"fmt"

	"golang.org/x/crypto/chacha20poly1305"
	"google.golang.org/protobuf/proto"

	chatpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/chat/v1"
	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"
	messagingpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/messaging/v1"

	"github.com/code-payments/flipcash2-server/model"
)

// The recipient's side of the team's private group encryption, for tests that
// play the user: opening the envelope the team wrapped for them and reading
// what the team posted. They follow the same proto specs as the sending side
// in crypto.go, with the roles reversed, and are exported so the package's
// external tests can use them too.

// OpenGroupKey opens env as self, its recipient, wrapped by the account with
// public key wrapper in the group chatID, returning the chat key
// (chat.v1.KeyEnvelope, scheme X25519_XCHACHA20POLY1305).
func OpenGroupKey(self model.KeyPair, wrapper ed25519.PublicKey, chatID *commonpb.ChatId, env *chatpb.KeyEnvelope) ([]byte, error) {
	if env.GetScheme() != chatpb.KeyEnvelope_X25519_XCHACHA20POLY1305 {
		return nil, fmt.Errorf("unsupported envelope scheme %s", env.GetScheme())
	}
	ss, err := sharedSecret(self, wrapper)
	if err != nil {
		return nil, fmt.Errorf("wrapper key: %w", err)
	}
	recipient := self.Public()
	key := wrappingKey(ss, wrapper, recipient, chatID.GetValue())
	groupKey, err := open(key, env.GetNonce(), env.GetCiphertext(), keyWrapAAD(chatID.GetValue(), wrapper, recipient))
	if err != nil {
		return nil, fmt.Errorf("open envelope: %w", err)
	}
	if len(groupKey) != groupKeySize {
		return nil, fmt.Errorf("envelope holds %d bytes, want %d", len(groupKey), groupKeySize)
	}
	return groupKey, nil
}

// DecryptGroupContent decrypts a private group message's content sent by
// senderID (Message.sender_id) under the chat key (messaging.v1.EncryptedContent,
// scheme CHAT_KEY_XCHACHA20POLY1305).
func DecryptGroupContent(groupKey []byte, chatID *commonpb.ChatId, senderID *commonpb.UserId, enc *messagingpb.EncryptedContent) (*messagingpb.Content, error) {
	if enc.GetScheme() != messagingpb.EncryptedContent_CHAT_KEY_XCHACHA20POLY1305 {
		return nil, fmt.Errorf("not a chat-key ciphertext (scheme %s)", enc.GetScheme())
	}
	plaintext, err := open(groupKey, enc.GetNonce(), enc.GetCiphertext(), groupContentAAD(chatID.GetValue(), senderID.GetValue()))
	if err != nil {
		return nil, err
	}
	var content messagingpb.Content
	if err := proto.Unmarshal(plaintext, &content); err != nil {
		return nil, fmt.Errorf("plaintext is not a Content: %w", err)
	}
	if content.GetEncrypted() != nil {
		return nil, errors.New("plaintext holds EncryptedContent")
	}
	return &content, nil
}

func open(key, nonce, ciphertext, aad []byte) ([]byte, error) {
	aead, err := chacha20poly1305.NewX(key)
	if err != nil {
		return nil, err
	}
	if len(nonce) != chacha20poly1305.NonceSizeX {
		return nil, fmt.Errorf("nonce is %d bytes, want %d", len(nonce), chacha20poly1305.NonceSizeX)
	}
	plaintext, err := aead.Open(nil, nonce, ciphertext, aad)
	if err != nil {
		return nil, errors.New("ciphertext does not authenticate")
	}
	return plaintext, nil
}
