// Package xeddsa verifies XEdDSA signatures: Ed25519 signatures made with a
// Curve25519 (Montgomery) key, as libsignal produces over prekeys with a
// device's identity key.
//
// Signal's XEdDSA specification (https://signal.org/docs/specifications/xeddsa/)
// defines signing so that the result verifies as an ordinary Ed25519 signature
// under the Edwards form of the signer's Montgomery public key. Verification
// is therefore a key conversion followed by standard Ed25519 verification,
// which is all this package does on the server: the birational map
//
//	y = (u - 1) / (u + 1)
//
// takes the Montgomery u-coordinate to the Edwards y-coordinate, and the
// sign of the Edwards x-coordinate, which the map does not determine, is
// carried in the top bit of the signature's last byte, where libsignal puts
// it. Ed25519 requires that bit clear (S < 2^253), so it is free to carry it;
// it is masked off before the signature is verified. A signer that forces the
// sign to zero by negating its scalar, as the specification's calculate_key_pair
// does, leaves the bit clear, and verifies the same way.
//
// The server never signs. KeyPair below is a signer whose signatures verify
// under this scheme, for tests and for a Go client: it is an Ed25519 key
// presented in Montgomery form, which is what XEdDSA verification accepts,
// but its nonces are Ed25519's deterministic ones rather than the
// specification's randomized construction, so it is not a conforming XEdDSA
// signer and must not be used for a production identity key.
package xeddsa

import (
	"crypto/ed25519"
	"crypto/rand"

	"filippo.io/edwards25519"
	"filippo.io/edwards25519/field"
)

const (
	// PublicKeySize is the size of a Curve25519 public key: the u-coordinate,
	// little-endian.
	PublicKeySize = 32

	// SignatureSize is the size of an XEdDSA signature, the same as Ed25519's.
	SignatureSize = 64
)

// Verify reports whether sig is a valid XEdDSA signature over message by the
// Curve25519 public key pub. It returns false, never an error, for malformed
// input of any kind: a key or signature of the wrong length, a u-coordinate
// with no Edwards image, or a signature Ed25519 rejects.
func Verify(pub, message, sig []byte) bool {
	if len(pub) != PublicKeySize || len(sig) != SignatureSize {
		return false
	}

	edPub, ok := toEdwards(pub, sig[63]>>7)
	if !ok {
		return false
	}

	edSig := make([]byte, SignatureSize)
	copy(edSig, sig)
	edSig[63] &= 0x7f

	return ed25519.Verify(edPub, message, edSig)
}

// toEdwards converts a Montgomery u-coordinate and an x sign bit into a
// compressed Edwards point, as Ed25519 encodes a public key. It fails when
// u = -1, whose image is the point at infinity and has no y.
func toEdwards(u []byte, sign byte) (ed25519.PublicKey, bool) {
	var uElem field.Element
	if _, err := uElem.SetBytes(u); err != nil {
		return nil, false
	}

	one := new(field.Element).One()
	denom := new(field.Element).Add(&uElem, one)
	if denom.Equal(new(field.Element).Zero()) == 1 {
		return nil, false
	}

	y := new(field.Element).Subtract(&uElem, one)
	y.Multiply(y, denom.Invert(denom))

	out := y.Bytes()
	out[31] |= sign << 7
	return ed25519.PublicKey(out), true
}

// KeyPair is a signer whose signatures Verify accepts. See the package
// comment for what it is not.
type KeyPair struct {
	priv ed25519.PrivateKey
	pub  []byte
}

// GenerateKeyPair mints a fresh key pair.
func GenerateKeyPair() (*KeyPair, error) {
	edPub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return nil, err
	}

	point, err := new(edwards25519.Point).SetBytes(edPub)
	if err != nil {
		return nil, err
	}

	return &KeyPair{priv: priv, pub: point.BytesMontgomery()}, nil
}

// MustGenerateKeyPair is GenerateKeyPair for tests.
func MustGenerateKeyPair() *KeyPair {
	k, err := GenerateKeyPair()
	if err != nil {
		panic(err)
	}
	return k
}

// PublicKey returns the Curve25519 public key: the u-coordinate, PublicKeySize
// bytes, little-endian.
func (k *KeyPair) PublicKey() []byte {
	return append([]byte(nil), k.pub...)
}

// Sign signs message. The signature carries the Edwards sign bit of the
// public key in its top bit, as libsignal's do.
func (k *KeyPair) Sign(message []byte) []byte {
	sig := ed25519.Sign(k.priv, message)
	edPub := k.priv.Public().(ed25519.PublicKey)
	sig[63] |= edPub[31] & 0x80
	return sig
}
