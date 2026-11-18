package flipcashteam

import (
	"bytes"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/crypto/chacha20poly1305"
	"google.golang.org/protobuf/proto"

	chatpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/chat/v1"
	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"
	messagingpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/messaging/v1"

	"github.com/code-payments/flipcash2-server/chat"
	"github.com/code-payments/flipcash2-server/model"
)

func groupChatID(t *testing.T) *commonpb.ChatId {
	id := make([]byte, chat.GroupChatIDSize)
	_, err := rand.Read(id)
	require.NoError(t, err)
	return &commonpb.ChatId{Value: id}
}

func secret(fill byte) []byte {
	return bytes.Repeat([]byte{fill}, minGroupKeySecretSize)
}

func mustHex(t *testing.T, s string) []byte {
	b, err := hex.DecodeString(s)
	require.NoError(t, err)
	return b
}

func TestDeriveGroupKey(t *testing.T) {
	chatA, chatB := groupChatID(t), groupChatID(t)

	key, err := deriveGroupKey(secret(1), chatA)
	require.NoError(t, err)
	assert.Len(t, key, groupKeySize)

	again, err := deriveGroupKey(secret(1), chatA)
	require.NoError(t, err)
	assert.Equal(t, key, again)

	otherChat, err := deriveGroupKey(secret(1), chatB)
	require.NoError(t, err)
	assert.NotEqual(t, key, otherChat)

	otherSecret, err := deriveGroupKey(secret(2), chatA)
	require.NoError(t, err)
	assert.NotEqual(t, key, otherSecret)

	// A longer secret is accepted.
	_, err = deriveGroupKey(bytes.Repeat([]byte{1}, 64), chatA)
	require.NoError(t, err)
}

// TestDeriveGroupKey_KnownAnswer pins the derivation: every group the team has
// created depends on it, so it must never change (see deriveGroupKey). The
// expected key was computed independently from RFC 5869 with
// info = "flipcash-team-group-key-v1" || chat_id and no salt.
func TestDeriveGroupKey_KnownAnswer(t *testing.T) {
	s := make([]byte, 32)
	for i := range s {
		s[i] = byte(i)
	}
	chatID := &commonpb.ChatId{Value: mustHex(t, "a0a1a2a3a4a5a6a7a8a9aaabacadaeaf")}

	key, err := deriveGroupKey(s, chatID)
	require.NoError(t, err)
	assert.Equal(t, "8b368c5922d548e7cdf0be6d905f668176fdbb673cc8dd4fc55fc264c0e117b1", hex.EncodeToString(key))
}

func TestDeriveGroupKey_Refused(t *testing.T) {
	_, err := deriveGroupKey(make([]byte, minGroupKeySecretSize-1), groupChatID(t))
	assert.Error(t, err)

	_, err = deriveGroupKey(nil, groupChatID(t))
	assert.Error(t, err)

	// A DM's ID is not a group's.
	_, err = deriveGroupKey(secret(1), &commonpb.ChatId{Value: make([]byte, 32)})
	assert.Error(t, err)
}

type wrapEnv struct {
	team       model.KeyPair
	teamUserID *commonpb.UserId
	user       model.KeyPair
	chatID     *commonpb.ChatId
	groupKey   []byte
}

func newWrapEnv(t *testing.T) wrapEnv {
	chatID := groupChatID(t)
	groupKey, err := deriveGroupKey(secret(7), chatID)
	require.NoError(t, err)
	return wrapEnv{
		team:       model.MustGenerateKeyPair(),
		teamUserID: model.MustGenerateUserID(),
		user:       model.MustGenerateKeyPair(),
		chatID:     chatID,
		groupKey:   groupKey,
	}
}

func TestWrapGroupKey_RoundTrip(t *testing.T) {
	env := newWrapEnv(t)

	envelope, err := wrapGroupKey(env.team, env.teamUserID, env.user.Proto(), env.chatID, env.groupKey)
	require.NoError(t, err)
	assert.Equal(t, chatpb.KeyEnvelope_X25519_XCHACHA20POLY1305, envelope.Scheme)
	assert.Len(t, envelope.Nonce, 24)
	assert.Len(t, envelope.Ciphertext, groupKeySize+16)
	assert.True(t, proto.Equal(env.teamUserID, envelope.WrappedBy))
	assert.Nil(t, envelope.InviteCopy)

	// It is what PutPrivateGroup takes for the user, from a store excluding
	// the team, as production's are.
	c := &chat.Chat{
		ID:        env.chatID,
		Type:      chatpb.ChatType_GROUP,
		IsPrivate: true,
		CreatorID: env.teamUserID,
		Members:   []*commonpb.UserId{env.teamUserID, model.MustGenerateUserID()},
	}
	require.NoError(t, chat.ValidatePrivateGroupCreation(c, []chat.MemberKeyEnvelope{{UserID: c.Members[1], Envelope: envelope}}, chat.NewFeedExclusions([]*commonpb.UserId{env.teamUserID})))

	groupKey, err := OpenGroupKey(env.user, env.team.Public(), env.chatID, envelope.ToProto())
	require.NoError(t, err)
	assert.Equal(t, env.groupKey, groupKey)

	// Fresh nonce each time.
	second, err := wrapGroupKey(env.team, env.teamUserID, env.user.Proto(), env.chatID, env.groupKey)
	require.NoError(t, err)
	assert.NotEqual(t, envelope.Nonce, second.Nonce)
	assert.NotEqual(t, envelope.Ciphertext, second.Ciphertext)
}

func TestWrapGroupKey_Binding(t *testing.T) {
	env := newWrapEnv(t)
	envelope, err := wrapGroupKey(env.team, env.teamUserID, env.user.Proto(), env.chatID, env.groupKey)
	require.NoError(t, err)
	pb := envelope.ToProto()

	t.Run("wrong chat", func(t *testing.T) {
		_, err := OpenGroupKey(env.user, env.team.Public(), groupChatID(t), pb)
		assert.Error(t, err)
	})
	t.Run("wrong wrapper", func(t *testing.T) {
		_, err := OpenGroupKey(env.user, model.MustGenerateKeyPair().Public(), env.chatID, pb)
		assert.Error(t, err)
	})
	t.Run("wrong recipient", func(t *testing.T) {
		_, err := OpenGroupKey(model.MustGenerateKeyPair(), env.team.Public(), env.chatID, pb)
		assert.Error(t, err)
	})
	t.Run("tampered ciphertext", func(t *testing.T) {
		tampered := proto.Clone(pb).(*chatpb.KeyEnvelope)
		tampered.Ciphertext[0] ^= 1
		_, err := OpenGroupKey(env.user, env.team.Public(), env.chatID, tampered)
		assert.Error(t, err)
	})
	t.Run("tampered nonce", func(t *testing.T) {
		tampered := proto.Clone(pb).(*chatpb.KeyEnvelope)
		tampered.Nonce[0] ^= 1
		_, err := OpenGroupKey(env.user, env.team.Public(), env.chatID, tampered)
		assert.Error(t, err)
	})
	t.Run("intact", func(t *testing.T) {
		groupKey, err := OpenGroupKey(env.user, env.team.Public(), env.chatID, pb)
		require.NoError(t, err)
		assert.Equal(t, env.groupKey, groupKey)
	})
}

func TestWrapGroupKey_Refused(t *testing.T) {
	env := newWrapEnv(t)

	// Low-order Ed25519 points: the identity, the point of order 2, the two of
	// order 4 and the two of order 8, plus a non-canonical encoding of the
	// identity.
	lowOrder := []string{
		"0100000000000000000000000000000000000000000000000000000000000000",
		"ecffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff7f",
		"0000000000000000000000000000000000000000000000000000000000000000",
		"0000000000000000000000000000000000000000000000000000000000000080",
		"26e8958fc2b227b045c3f489f2ef98f0d5dfac05d3c63339b13802886d53fc05",
		"c7176a703d4dd84fba3c0b760d10670f2a2053fa2c39ccc64ec7fd7792ac037a",
		"eeffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff7f",
	}
	for _, h := range lowOrder {
		_, err := wrapGroupKey(env.team, env.teamUserID, &commonpb.PublicKey{Value: mustHex(t, h)}, env.chatID, env.groupKey)
		assert.ErrorContains(t, err, "low-order", h)
	}

	// y = 2 is not the y-coordinate of any point on the curve.
	notOnCurve := mustHex(t, "0200000000000000000000000000000000000000000000000000000000000000")
	_, err := wrapGroupKey(env.team, env.teamUserID, &commonpb.PublicKey{Value: notOnCurve}, env.chatID, env.groupKey)
	assert.ErrorContains(t, err, "not a valid Ed25519 point")

	for _, size := range []int{0, 31, 33} {
		_, err := wrapGroupKey(env.team, env.teamUserID, &commonpb.PublicKey{Value: make([]byte, size)}, env.chatID, env.groupKey)
		assert.Error(t, err)
	}
	_, err = wrapGroupKey(env.team, env.teamUserID, nil, env.chatID, env.groupKey)
	assert.Error(t, err)

	// Bad arguments.
	_, err = wrapGroupKey(model.KeyPair{}, env.teamUserID, env.user.Proto(), env.chatID, env.groupKey)
	assert.Error(t, err)
	_, err = wrapGroupKey(env.team, nil, env.user.Proto(), env.chatID, env.groupKey)
	assert.Error(t, err)
	_, err = wrapGroupKey(env.team, env.teamUserID, env.user.Proto(), &commonpb.ChatId{Value: make([]byte, 32)}, env.groupKey)
	assert.Error(t, err)
	_, err = wrapGroupKey(env.team, env.teamUserID, env.user.Proto(), env.chatID, env.groupKey[:31])
	assert.Error(t, err)
}

func TestEncryptGroupContent_RoundTrip(t *testing.T) {
	env := newWrapEnv(t)
	content := &messagingpb.Content{Type: &messagingpb.Content_Text{Text: &messagingpb.TextContent{Text: "Welcome to Flipcash!"}}}

	enc, err := encryptGroupContent(env.groupKey, env.chatID, env.teamUserID, content)
	require.NoError(t, err)
	assert.Equal(t, messagingpb.EncryptedContent_CHAT_KEY_XCHACHA20POLY1305, enc.Scheme)
	assert.Len(t, enc.Nonce, 24)

	decrypted, err := DecryptGroupContent(env.groupKey, env.chatID, env.teamUserID, enc)
	require.NoError(t, err)
	assert.True(t, proto.Equal(content, decrypted))

	// The user reads it with the key from their envelope.
	envelope, err := wrapGroupKey(env.team, env.teamUserID, env.user.Proto(), env.chatID, env.groupKey)
	require.NoError(t, err)
	userKey, err := OpenGroupKey(env.user, env.team.Public(), env.chatID, envelope.ToProto())
	require.NoError(t, err)
	decrypted, err = DecryptGroupContent(userKey, env.chatID, env.teamUserID, enc)
	require.NoError(t, err)
	assert.True(t, proto.Equal(content, decrypted))

	second, err := encryptGroupContent(env.groupKey, env.chatID, env.teamUserID, content)
	require.NoError(t, err)
	assert.NotEqual(t, enc.Nonce, second.Nonce)
}

func TestEncryptGroupContent_Binding(t *testing.T) {
	env := newWrapEnv(t)
	content := &messagingpb.Content{Type: &messagingpb.Content_Text{Text: &messagingpb.TextContent{Text: "hi"}}}
	enc, err := encryptGroupContent(env.groupKey, env.chatID, env.teamUserID, content)
	require.NoError(t, err)

	t.Run("wrong chat", func(t *testing.T) {
		_, err := DecryptGroupContent(env.groupKey, groupChatID(t), env.teamUserID, enc)
		assert.Error(t, err)
	})
	t.Run("wrong sender", func(t *testing.T) {
		_, err := DecryptGroupContent(env.groupKey, env.chatID, model.MustGenerateUserID(), enc)
		assert.Error(t, err)
	})
	t.Run("wrong key", func(t *testing.T) {
		otherKey, err := deriveGroupKey(secret(8), env.chatID)
		require.NoError(t, err)
		_, err = DecryptGroupContent(otherKey, env.chatID, env.teamUserID, enc)
		assert.Error(t, err)
	})
	t.Run("tampered ciphertext", func(t *testing.T) {
		tampered := proto.Clone(enc).(*messagingpb.EncryptedContent)
		tampered.Ciphertext[len(tampered.Ciphertext)-1] ^= 1
		_, err := DecryptGroupContent(env.groupKey, env.chatID, env.teamUserID, tampered)
		assert.Error(t, err)
	})
	t.Run("tampered nonce", func(t *testing.T) {
		tampered := proto.Clone(enc).(*messagingpb.EncryptedContent)
		tampered.Nonce[0] ^= 1
		_, err := DecryptGroupContent(env.groupKey, env.chatID, env.teamUserID, tampered)
		assert.Error(t, err)
	})
}

func TestEncryptGroupContent_Refused(t *testing.T) {
	env := newWrapEnv(t)
	content := &messagingpb.Content{Type: &messagingpb.Content_Text{Text: &messagingpb.TextContent{Text: "hi"}}}

	_, err := encryptGroupContent(env.groupKey[:16], env.chatID, env.teamUserID, content)
	assert.Error(t, err)
	_, err = encryptGroupContent(env.groupKey, &commonpb.ChatId{Value: make([]byte, 32)}, env.teamUserID, content)
	assert.Error(t, err)
	_, err = encryptGroupContent(env.groupKey, env.chatID, nil, content)
	assert.Error(t, err)
	_, err = encryptGroupContent(env.groupKey, env.chatID, env.teamUserID, nil)
	assert.Error(t, err)
	_, err = encryptGroupContent(env.groupKey, env.chatID, env.teamUserID, &messagingpb.Content{})
	assert.Error(t, err)

	encrypted := &messagingpb.Content{Type: &messagingpb.Content_Encrypted{Encrypted: &messagingpb.EncryptedContent{}}}
	_, err = encryptGroupContent(env.groupKey, env.chatID, env.teamUserID, encrypted)
	assert.Error(t, err)
}

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

func TestParseTeamGroupKeySecret(t *testing.T) {
	want := bytes.Repeat([]byte{0xab}, teamGroupKeySecretSize)
	encoded := hex.EncodeToString(want)
	str := func(s string) *string { return &s }

	t.Run("hex string", func(t *testing.T) {
		for _, s := range []string{encoded, strings.ToUpper(encoded), "  " + encoded + "\n"} {
			got, err := parseTeamGroupKeySecret(str(s), nil)
			require.NoError(t, err)
			assert.Equal(t, want, got)
		}
	})

	t.Run("binary", func(t *testing.T) {
		raw := bytes.Clone(want)
		got, err := parseTeamGroupKeySecret(nil, raw)
		require.NoError(t, err)
		assert.Equal(t, want, got)

		// The result does not alias the response's bytes.
		raw[0] = 0
		assert.Equal(t, want, got)
	})

	t.Run("refused", func(t *testing.T) {
		for name, tc := range map[string]struct {
			s *string
			b []byte
		}{
			"no value":            {},
			"empty string":        {s: str("")},
			"short hex":           {s: str(encoded[:62])},
			"long hex":            {s: str(encoded + "ab")},
			"odd-length hex":      {s: str(encoded[:63])},
			"not hex":             {s: str("zz" + encoded[2:])},
			"double encoded":      {s: str(hex.EncodeToString([]byte(encoded)))},
			"short binary":        {b: want[:31]},
			"long binary":         {b: append(bytes.Clone(want), 0)},
			"raw bytes as string": {s: str(string(want))},
		} {
			t.Run(name, func(t *testing.T) {
				got, err := parseTeamGroupKeySecret(tc.s, tc.b)
				require.Error(t, err)
				assert.Nil(t, got)
				// The value never appears in the error.
				assert.NotContains(t, strings.ToLower(err.Error()), encoded[:16])
			})
		}
	})
}
