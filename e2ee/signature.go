package e2ee

import (
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/binary"

	"github.com/code-payments/flipcash2-server/e2ee/xeddsa"
)

// identityKeyDomain is the ASCII prefix the account key's certification of
// an identity key is made over (RegisterDeviceRequest.identity_key_signature).
// It keeps that signature from being confused with any other message the
// account key signs, such as an authenticated request.
const identityKeyDomain = "flipcash-e2ee-identity-key-v1"

// IdentityKeyMessage is the message the account key signs to certify an
// identity key: the domain prefix followed by the serialized key, with no
// separator. Exported so a Go client can make the signature the server
// checks.
func IdentityKeyMessage(identityKey []byte) []byte {
	msg := make([]byte, 0, len(identityKeyDomain)+len(identityKey))
	msg = append(msg, identityKeyDomain...)
	return append(msg, identityKey...)
}

// VerifyIdentityKeySignature reports whether sig is accountKey's Ed25519
// signature certifying identityKey.
func VerifyIdentityKeySignature(accountKey, identityKey, sig []byte) bool {
	if len(accountKey) != ed25519.PublicKeySize || len(sig) != ed25519.SignatureSize {
		return false
	}
	return ed25519.Verify(ed25519.PublicKey(accountKey), IdentityKeyMessage(identityKey), sig)
}

// VerifyPreKeySignature reports whether sig is identityKey's XEdDSA signature
// over prekey, the serialized public key (EncodeEC or EncodeKEM, type byte
// included: what PQXDH has the device sign). identityKey is the serialized
// EcPublicKey; its type byte is stripped to reach the Curve25519 key.
func VerifyPreKeySignature(identityKey, prekey, sig []byte) bool {
	if len(identityKey) != EcPublicKeySize {
		return false
	}
	return xeddsa.Verify(identityKey[1:], prekey, sig)
}

// RepeatedUseDigest is the digest of a device's repeated-use keys that
// GetPreKeyCountsResponse.repeated_use_digest carries:
//
//	SHA-256(identityKey || spk.ID (uint64 BE) || spk.PublicKey
//	        || lastResort.ID (uint64 BE) || lastResort.PublicKey)
//
// over the serialized key forms, with each id as 8 big-endian bytes: the
// layout of Signal-Server's keys/check (a Java long per id), so a client
// built on Signal's code computes it unchanged. A client computes it over
// the keys it believes it uploaded and compares; a mismatch means the
// server's view diverged from the device's. Exported so a Go client can
// make it.
func RepeatedUseDigest(identityKey []byte, spk *SignedPreKey, lastResort *KemSignedPreKey) []byte {
	h := sha256.New()
	h.Write(identityKey)
	var id [8]byte
	binary.BigEndian.PutUint64(id[:], uint64(spk.ID))
	h.Write(id[:])
	h.Write(spk.PublicKey)
	binary.BigEndian.PutUint64(id[:], uint64(lastResort.ID))
	h.Write(id[:])
	h.Write(lastResort.PublicKey)
	return h.Sum(nil)
}
