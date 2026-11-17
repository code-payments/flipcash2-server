package flipcashteam

import (
	"bytes"
	"crypto/rand"
	"encoding/hex"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
