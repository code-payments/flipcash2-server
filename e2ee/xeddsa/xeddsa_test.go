package xeddsa

import (
	"crypto/ed25519"
	"crypto/rand"
	"testing"

	"filippo.io/edwards25519"
	"github.com/stretchr/testify/require"
)

func TestVerify_RoundTrip(t *testing.T) {
	// Enough keys that both values of the Edwards sign bit are exercised.
	sawSign := map[byte]bool{}
	for range 32 {
		k := MustGenerateKeyPair()
		msg := []byte("prekey bytes")
		sig := k.Sign(msg)
		sawSign[sig[63]>>7] = true

		require.True(t, Verify(k.PublicKey(), msg, sig))
		require.False(t, Verify(k.PublicKey(), []byte("other"), sig))
		require.False(t, Verify(MustGenerateKeyPair().PublicKey(), msg, sig))

		// Flipping the carried sign bit points at the wrong Edwards key.
		flipped := append([]byte(nil), sig...)
		flipped[63] ^= 0x80
		require.False(t, Verify(k.PublicKey(), msg, flipped))
	}
	require.True(t, sawSign[0])
	require.True(t, sawSign[1])
}

func TestVerify_Malformed(t *testing.T) {
	k := MustGenerateKeyPair()
	msg := []byte("m")
	sig := k.Sign(msg)

	require.False(t, Verify(k.PublicKey()[:31], msg, sig))
	require.False(t, Verify(k.PublicKey(), msg, sig[:63]))
	require.False(t, Verify(k.PublicKey(), msg, nil))

	// u = -1 (p - 1) has no Edwards image.
	minusOne := make([]byte, 32)
	for i := range minusOne {
		minusOne[i] = 0xff
	}
	minusOne[0] = 0xec
	minusOne[31] = 0x7f
	require.False(t, Verify(minusOne, msg, sig))
}

// A plain Ed25519 signature by a key whose Edwards sign bit is 0 is a valid
// XEdDSA signature under the key's Montgomery form: the scheme is the same
// verification, and a signer that forces the sign to zero leaves the carried
// bit clear.
func TestVerify_PlainEd25519(t *testing.T) {
	checked := 0
	for checked < 8 {
		edPub, priv, err := ed25519.GenerateKey(rand.Reader)
		require.NoError(t, err)
		if edPub[31]&0x80 != 0 {
			continue
		}
		checked++

		point, err := new(edwards25519.Point).SetBytes(edPub)
		require.NoError(t, err)

		msg := []byte("m")
		require.True(t, Verify(point.BytesMontgomery(), msg, ed25519.Sign(priv, msg)))
	}
}
