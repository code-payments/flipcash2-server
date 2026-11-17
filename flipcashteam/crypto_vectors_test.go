package flipcashteam

import (
	"bytes"
	"crypto/ed25519"
	"encoding/hex"
	"encoding/json"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/crypto/chacha20poly1305"

	chatpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/chat/v1"
	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"
	messagingpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/messaging/v1"

	"github.com/code-payments/flipcash2-server/model"
)

// clientGroupCipherVectors are the iOS and Android apps' own known-answer
// vectors for private group encryption, generated against libsodium rather
// than either side's code: testdata/client_group_cipher.json holds the message
// and key wrap entries of the apps' group_cipher.json (code-payments/
// code-android-app, libs/encryption/chat-cipher/src/commonTest/resources, as
// of fde9c3af5, the change that introduced group encryption, first released in
// 2026.10.3 on both platforms; iOS runs the same cipher through its shared
// core). Matching them pins that what the team wraps and sends opens in the
// apps, which the round trips in crypto_test.go cannot, since their recipient
// side reuses the sending side's helpers.
//
// The vectors use a 32-byte chat ID, which the spec allows but the team's
// entry points refuse (they serve groups alone), so the derivations under
// them are checked directly, with each vector's fixed nonce.
type clientGroupCipherVectors struct {
	Messages []struct {
		Name       string   `json:"name"`
		GroupKey   hexBytes `json:"groupKey"`
		ChatID     hexBytes `json:"chatId"`
		SenderID   hexBytes `json:"senderId"`
		Content    hexBytes `json:"content"`
		Nonce      hexBytes `json:"nonce"`
		AAD        hexBytes `json:"aad"`
		Ciphertext hexBytes `json:"ciphertext"`
	} `json:"messages"`
	Wraps []struct {
		Name               string   `json:"name"`
		WrapperSeed        hexBytes `json:"wrapperSeed"`
		WrapperPublicKey   hexBytes `json:"wrapperPublicKey"`
		RecipientPublicKey hexBytes `json:"recipientPublicKey"`
		RecipientSeed      hexBytes `json:"recipientSeed"`
		ChatID             hexBytes `json:"chatId"`
		GroupKey           hexBytes `json:"groupKey"`
		WrappingKey        hexBytes `json:"wrappingKey"`
		Nonce              hexBytes `json:"nonce"`
		AAD                hexBytes `json:"aad"`
		Ciphertext         hexBytes `json:"ciphertext"`
	} `json:"wraps"`
}

type hexBytes []byte

func (b *hexBytes) UnmarshalJSON(data []byte) error {
	var s string
	if err := json.Unmarshal(data, &s); err != nil {
		return err
	}
	decoded, err := hex.DecodeString(s)
	if err != nil {
		return err
	}
	*b = decoded
	return nil
}

func loadClientGroupCipherVectors(t *testing.T) clientGroupCipherVectors {
	data, err := os.ReadFile("testdata/client_group_cipher.json")
	require.NoError(t, err)
	var v clientGroupCipherVectors
	require.NoError(t, json.Unmarshal(data, &v))
	require.NotEmpty(t, v.Messages)
	require.NotEmpty(t, v.Wraps)
	return v
}

func keyPairFromSeed(t *testing.T, seed []byte) model.KeyPair {
	kp, err := model.FromPrivateKey(ed25519.NewKeyFromSeed(seed))
	require.NoError(t, err)
	return kp
}

// TestClientVectors_KeyWrap: the team's key wrap derives the apps' wrapping
// key and aad, seals the chat key to their ciphertext, from either side of
// the exchange, and the envelope opens as the recipient.
func TestClientVectors_KeyWrap(t *testing.T) {
	for _, v := range loadClientGroupCipherVectors(t).Wraps {
		t.Run(v.Name, func(t *testing.T) {
			wrapper := keyPairFromSeed(t, v.WrapperSeed)
			recipient := keyPairFromSeed(t, v.RecipientSeed)
			require.Equal(t, []byte(v.WrapperPublicKey), []byte(wrapper.Public()))
			require.Equal(t, []byte(v.RecipientPublicKey), []byte(recipient.Public()))

			ss, err := sharedSecret(wrapper, recipient.Public())
			require.NoError(t, err)
			key := wrappingKey(ss, wrapper.Public(), recipient.Public(), v.ChatID)
			assert.Equal(t, []byte(v.WrappingKey), key)

			// The recipient's side of the exchange agrees.
			ss, err = sharedSecret(recipient, wrapper.Public())
			require.NoError(t, err)
			assert.Equal(t, key, wrappingKey(ss, recipient.Public(), wrapper.Public(), v.ChatID))

			aad := keyWrapAAD(v.ChatID, wrapper.Public(), recipient.Public())
			assert.Equal(t, []byte(v.AAD), aad)

			aead, err := chacha20poly1305.NewX(key)
			require.NoError(t, err)
			assert.Equal(t, []byte(v.Ciphertext), aead.Seal(nil, v.Nonce, v.GroupKey, aad))

			opened, err := OpenGroupKey(recipient, wrapper.Public(), &commonpb.ChatId{Value: v.ChatID}, &chatpb.KeyEnvelope{
				Scheme:     chatpb.KeyEnvelope_X25519_XCHACHA20POLY1305,
				Nonce:      v.Nonce,
				Ciphertext: v.Ciphertext,
			})
			require.NoError(t, err)
			assert.Equal(t, []byte(v.GroupKey), opened)
		})
	}
}

// TestClientVectors_GroupContent: the team's group content encryption derives
// the apps' aad and seals their plaintext to their ciphertext.
func TestClientVectors_GroupContent(t *testing.T) {
	for _, v := range loadClientGroupCipherVectors(t).Messages {
		t.Run(v.Name, func(t *testing.T) {
			aad := groupContentAAD(v.ChatID, v.SenderID)
			assert.Equal(t, []byte(v.AAD), aad)

			aead, err := chacha20poly1305.NewX(v.GroupKey)
			require.NoError(t, err)
			assert.Equal(t, []byte(v.Ciphertext), aead.Seal(nil, v.Nonce, v.Content, aad))

			plaintext, err := aead.Open(nil, v.Nonce, v.Ciphertext, aad)
			require.NoError(t, err)
			assert.True(t, bytes.Equal(v.Content, plaintext), "plaintext %x, want %x", plaintext, []byte(v.Content))
		})
	}
}

// TestClientVectors_GroupContentText: the apps' text vector is a serialized
// messaging.v1.Content, and decrypts to it as the team's welcome is read.
func TestClientVectors_GroupContentText(t *testing.T) {
	for _, v := range loadClientGroupCipherVectors(t).Messages {
		if v.Name != "text" {
			continue
		}
		content, err := DecryptGroupContent(v.GroupKey, &commonpb.ChatId{Value: v.ChatID}, &commonpb.UserId{Value: v.SenderID}, &messagingpb.EncryptedContent{
			Scheme:     messagingpb.EncryptedContent_CHAT_KEY_XCHACHA20POLY1305,
			Nonce:      v.Nonce,
			Ciphertext: v.Ciphertext,
		})
		require.NoError(t, err)
		assert.Equal(t, "hello", content.GetText().GetText())
		return
	}
	t.Fatal("no text vector")
}
