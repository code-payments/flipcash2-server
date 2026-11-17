package flipcashteam

import (
	"bytes"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"crypto/sha512"
	"errors"
	"fmt"
	"io"

	"filippo.io/edwards25519"
	"golang.org/x/crypto/chacha20poly1305"
	"golang.org/x/crypto/curve25519"
	"golang.org/x/crypto/hkdf"
	"google.golang.org/protobuf/proto"

	chatpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/chat/v1"
	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"
	messagingpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/messaging/v1"

	"github.com/code-payments/flipcash2-server/chat"
	"github.com/code-payments/flipcash2-server/model"
)

// Private group encryption as the team account does it.
//
// A private group the team creates for a user (see chat.Store.PutPrivateGroup)
// has a chat key like any other private group, but the team has no client to
// generate one, hold it or wrap it for itself. So the server stands in for the
// team: it derives the group's chat key (deriveGroupKey), wraps the user's copy
// of it as the team (wrapGroupKey), and encrypts what the team posts into the
// group under it (encryptGroupContent). The constructions are the ones the
// protos specify for every client, so the user's client opens and reads them
// exactly as it would a group a person created: chat.v1.KeyEnvelope for the
// envelope (scheme X25519_XCHACHA20POLY1305) and messaging.v1.EncryptedContent
// for the content (scheme CHAT_KEY_XCHACHA20POLY1305, under "Private groups").
//
// The team stores no envelope of its own. The server re-derives the chat key
// whenever it needs it, from a secret of its own and the chat's ID.

// groupKeySize is the size of a private group's chat key.
const groupKeySize = 32

// minGroupKeySecretSize is the smallest secret deriveGroupKey accepts.
const minGroupKeySecretSize = 32

const (
	// groupKeyDomain is the info prefix of deriveGroupKey's HKDF. It versions
	// the derivation: see deriveGroupKey before changing it.
	groupKeyDomain = "flipcash-team-group-key-v1"

	// keyWrapDomain and groupContentDomain are the domain strings the protos
	// fix for chat.v1.KeyEnvelope and messaging.v1.EncryptedContent's private
	// group scheme.
	keyWrapDomain      = "flipcash-group-key-wrap-v1"
	groupContentDomain = "flipcash-group-e2ee-v1"
)

// deriveGroupKey returns the chat key of the private group chatID that the
// team creates, derived from secret with HKDF-SHA256 (RFC 5869):
//
//	salt = none (HKDF's default, 32 zero bytes)
//	ikm  = secret
//	info = "flipcash-team-group-key-v1" || chat_id
//	L    = 32
//
// chat_id is the raw bytes of common.v1.ChatId.value, which must be a group's
// (chat.GroupChatIDSize bytes, so info is fixed width). secret must be at
// least minGroupKeySecretSize bytes of uniformly random data that the server
// keeps for this purpose alone: in particular it must not be the team's
// signing key, so that the team's key and the groups' keys never stand or fall
// together, and nothing that wraps or signs with the team's key can be turned
// into a group key.
//
// The key is derived rather than drawn at random and stored so that the
// server needs no record of it: a retried creation derives the same key, so
// the envelope it wraps holds the key of the group as first written, and every
// later post the team makes into the group derives the key again with no
// read. Anyone holding secret can derive the key of every group the team
// creates, so it is guarded like a private key.
//
// For a group already created, neither secret nor the derivation (its domain
// string, which carries the version) may ever change. The group's members
// read with the envelopes they hold, which do not depend on it, but the
// server would derive a different key and could no longer post into the group,
// since members would fail to decrypt what it sent. A new derivation, or a
// new secret, takes a new version of the domain string and is used for groups
// created from then on only, with the old one kept for the groups before
// them.
func deriveGroupKey(secret []byte, chatID *commonpb.ChatId) ([]byte, error) {
	if len(secret) < minGroupKeySecretSize {
		return nil, fmt.Errorf("group key secret is %d bytes, want at least %d", len(secret), minGroupKeySecretSize)
	}
	if !chat.IsGroupChatID(chatID) {
		return nil, errors.New("not a group chat ID")
	}
	info := append([]byte(groupKeyDomain), chatID.Value...)
	key := make([]byte, groupKeySize)
	if _, err := io.ReadFull(hkdf.New(sha256.New, secret, nil, info), key); err != nil {
		return nil, fmt.Errorf("derive group key: %w", err)
	}
	return key, nil
}

// wrapGroupKey makes the key envelope of groupKey that team, the group's
// creator, wraps for recipient in the private group chatID, as
// chat.v1.KeyEnvelope specifies for scheme X25519_XCHACHA20POLY1305. The
// envelope is recorded as wrapped by teamUserID, team's user ID, so it can be
// handed to chat.Store.PutPrivateGroup as the recipient's
// chat.MemberKeyEnvelope as is. recipient is the Ed25519 public key the
// recipient registered their account with:
//
//  1. team's Ed25519 private key is converted to an X25519 private key, and
//     recipient's Ed25519 public key to an X25519 public key, with the
//     standard birational map. A recipient key that is not a valid point, or
//     that converts to a low-order X25519 point, is refused.
//  2. ss = X25519(team_x25519_priv, recipient_x25519_pub), refused if all
//     zeros.
//  3. The wrapping key is HKDF-SHA256 with salt = the two Ed25519 public keys
//     in bytewise order, ikm = ss, info = "flipcash-group-key-wrap-v1" ||
//     chat_id, L = 32.
//  4. groupKey is sealed with XChaCha20-Poly1305 under the wrapping key with a
//     fresh random 24-byte nonce and aad = "flipcash-group-key-wrap-v1" ||
//     chat_id || team_pk || recipient_pk.
//
// The recipient opens it with team's public key, which they learn as the
// group's creator.
func wrapGroupKey(team model.KeyPair, teamUserID *commonpb.UserId, recipient *commonpb.PublicKey, chatID *commonpb.ChatId, groupKey []byte) (chat.KeyEnvelope, error) {
	if len(team.Private()) != ed25519.PrivateKeySize {
		return chat.KeyEnvelope{}, errors.New("team key pair has no private key")
	}
	if len(teamUserID.GetValue()) == 0 {
		return chat.KeyEnvelope{}, errors.New("missing team user ID")
	}
	if !chat.IsGroupChatID(chatID) {
		return chat.KeyEnvelope{}, errors.New("not a group chat ID")
	}
	if len(groupKey) != groupKeySize {
		return chat.KeyEnvelope{}, fmt.Errorf("group key is %d bytes, want %d", len(groupKey), groupKeySize)
	}

	recipientPK := ed25519.PublicKey(recipient.GetValue())
	ss, err := sharedSecret(team, recipientPK)
	if err != nil {
		return chat.KeyEnvelope{}, fmt.Errorf("recipient key: %w", err)
	}
	teamPK := team.Public()
	key := wrappingKey(ss, teamPK, recipientPK, chatID.Value)
	nonce, ciphertext, err := seal(key, groupKey, keyWrapAAD(chatID.Value, teamPK, recipientPK))
	if err != nil {
		return chat.KeyEnvelope{}, err
	}
	return chat.KeyEnvelope{
		Scheme:     chatpb.KeyEnvelope_X25519_XCHACHA20POLY1305,
		Nonce:      nonce,
		Ciphertext: ciphertext,
		WrappedBy:  proto.Clone(teamUserID).(*commonpb.UserId),
	}, nil
}

// encryptGroupContent encrypts content for the private group chatID under its
// chat key, as senderID, which must be the user the message is sent as (the
// team's user ID for a team post): the aad binds the sender the server records
// as Message.sender_id. It follows messaging.v1.EncryptedContent's scheme
// CHAT_KEY_XCHACHA20POLY1305: XChaCha20-Poly1305 with key = groupKey, a fresh
// random 24-byte nonce, plaintext = the serialized Content, and aad =
// "flipcash-group-e2ee-v1" || chat_id || sender_id. content must be one the
// proto allows as a plaintext; it may not itself be encrypted.
func encryptGroupContent(groupKey []byte, chatID *commonpb.ChatId, senderID *commonpb.UserId, content *messagingpb.Content) (*messagingpb.EncryptedContent, error) {
	if len(groupKey) != groupKeySize {
		return nil, fmt.Errorf("group key is %d bytes, want %d", len(groupKey), groupKeySize)
	}
	if !chat.IsGroupChatID(chatID) {
		return nil, errors.New("not a group chat ID")
	}
	if len(senderID.GetValue()) == 0 {
		return nil, errors.New("missing sender ID")
	}
	if content == nil || content.GetType() == nil {
		return nil, errors.New("missing content")
	}
	if content.GetEncrypted() != nil {
		return nil, errors.New("content is already encrypted")
	}
	plaintext, err := proto.Marshal(content)
	if err != nil {
		return nil, fmt.Errorf("marshal content: %w", err)
	}
	nonce, ciphertext, err := seal(groupKey, plaintext, groupContentAAD(chatID.Value, senderID.Value))
	if err != nil {
		return nil, err
	}
	return &messagingpb.EncryptedContent{
		Scheme:     messagingpb.EncryptedContent_CHAT_KEY_XCHACHA20POLY1305,
		Nonce:      nonce,
		Ciphertext: ciphertext,
	}, nil
}

// x25519Private converts an Ed25519 private key to an X25519 private key as
// libsodium's crypto_sign_ed25519_sk_to_curve25519 does: the first 32 bytes of
// SHA-512 of the seed. curve25519.X25519 clamps it.
func x25519Private(priv ed25519.PrivateKey) []byte {
	h := sha512.Sum512(priv.Seed())
	return h[:32]
}

// x25519Public converts an Ed25519 public key to an X25519 public key with the
// birational map u = (1 + y) / (1 - y) (libsodium's
// crypto_sign_ed25519_pk_to_curve25519), refusing a key that is not 32 bytes,
// does not decode to a point on the curve, or is of low order (multiplying it
// by the cofactor 8 gives the identity).
func x25519Public(pub ed25519.PublicKey) ([]byte, error) {
	if len(pub) != ed25519.PublicKeySize {
		return nil, fmt.Errorf("public key is %d bytes, want %d", len(pub), ed25519.PublicKeySize)
	}
	p, err := new(edwards25519.Point).SetBytes(pub)
	if err != nil {
		return nil, fmt.Errorf("public key is not a valid Ed25519 point: %w", err)
	}
	if new(edwards25519.Point).MultByCofactor(p).Equal(edwards25519.NewIdentityPoint()) == 1 {
		return nil, errors.New("public key is a low-order point")
	}
	return p.BytesMontgomery(), nil
}

// sharedSecret is X25519 between self's private key and peer's public key,
// both converted from Ed25519. An all-zero result is refused (curve25519.X25519
// refuses it too).
func sharedSecret(self model.KeyPair, peer ed25519.PublicKey) ([]byte, error) {
	peerX, err := x25519Public(peer)
	if err != nil {
		return nil, err
	}
	ss, err := curve25519.X25519(x25519Private(self.Private()), peerX)
	if err != nil {
		return nil, fmt.Errorf("x25519: %w", err)
	}
	if bytes.Equal(ss, make([]byte, len(ss))) {
		return nil, errors.New("x25519 shared secret is all zeros")
	}
	return ss, nil
}

// wrappingKey is chat.v1.KeyEnvelope's step 3: HKDF-SHA256 over ss with salt
// = the two Ed25519 public keys in bytewise order and info =
// "flipcash-group-key-wrap-v1" || chat_id.
func wrappingKey(ss []byte, pkA, pkB ed25519.PublicKey, chatID []byte) []byte {
	lo, hi := pkA, pkB
	if bytes.Compare(lo, hi) > 0 {
		lo, hi = hi, lo
	}
	salt := append(bytes.Clone(lo), hi...)
	info := append([]byte(keyWrapDomain), chatID...)
	key := make([]byte, chacha20poly1305.KeySize)
	if _, err := io.ReadFull(hkdf.New(sha256.New, ss, salt, info), key); err != nil {
		panic(err) // HKDF-SHA256 cannot fail for a 32-byte output
	}
	return key
}

// keyWrapAAD is chat.v1.KeyEnvelope's aad: "flipcash-group-key-wrap-v1" ||
// chat_id || wrapper_pk || recipient_pk.
func keyWrapAAD(chatID []byte, wrapper, recipient ed25519.PublicKey) []byte {
	aad := append([]byte(keyWrapDomain), chatID...)
	aad = append(aad, wrapper...)
	return append(aad, recipient...)
}

// groupContentAAD is messaging.v1.EncryptedContent's private group aad:
// "flipcash-group-e2ee-v1" || chat_id || sender_id.
func groupContentAAD(chatID, senderID []byte) []byte {
	return append(append([]byte(groupContentDomain), chatID...), senderID...)
}

// seal is XChaCha20-Poly1305 with a fresh random 24-byte nonce; the
// ciphertext has the 16-byte tag appended.
func seal(key, plaintext, aad []byte) (nonce, ciphertext []byte, err error) {
	aead, err := chacha20poly1305.NewX(key)
	if err != nil {
		return nil, nil, err
	}
	nonce = make([]byte, chacha20poly1305.NonceSizeX)
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return nil, nil, fmt.Errorf("generate nonce: %w", err)
	}
	return nonce, aead.Seal(nil, nonce, plaintext, aad), nil
}
