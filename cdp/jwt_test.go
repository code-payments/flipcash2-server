package cdp

import (
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func testKey() ed25519.PrivateKey {
	seed := sha256.Sum256([]byte("cdp jwt test key"))
	return ed25519.NewKeyFromSeed(seed[:])
}

func TestNewJWT_Shape(t *testing.T) {
	key := testKey()
	now := time.Unix(1_800_000_000, 999_000_000)

	token, err := newJWT(now, "PUT", "sandbox.cdp.coinbase.com", "/platform/v2/customers/customer_x", "kid-1", key)
	require.NoError(t, err)

	parts := strings.Split(token, ".")
	require.Len(t, parts, 3)

	header, err := base64.RawURLEncoding.DecodeString(parts[0])
	require.NoError(t, err)
	payload, err := base64.RawURLEncoding.DecodeString(parts[1])
	require.NoError(t, err)
	signature, err := base64.RawURLEncoding.DecodeString(parts[2])
	require.NoError(t, err)

	// The exact bytes, field order included, as the signer produced them
	// when it lived in thirdparty/.
	assert.Regexp(t, regexp.MustCompile(`^\{"typ":"JWT","alg":"EdDSA","kid":"kid-1","nonce":"[0-9a-f]{32}"\}$`), string(header))
	assert.Equal(t,
		`{"iss":"cdp","nbf":1800000000,"exp":1800000120,"sub":"kid-1","uri":"PUT sandbox.cdp.coinbase.com/platform/v2/customers/customer_x"}`,
		string(payload))
	assert.True(t, ed25519.Verify(key.Public().(ed25519.PublicKey), []byte(parts[0]+"."+parts[1]), signature))
}

func TestNewJWT_NonceVaries(t *testing.T) {
	key := testKey()
	a, err := NewJWT("GET", "h", "/p", "k", key)
	require.NoError(t, err)
	b, err := NewJWT("GET", "h", "/p", "k", key)
	require.NoError(t, err)
	assert.NotEqual(t, strings.Split(a, ".")[0], strings.Split(b, ".")[0])
}

func TestNewJWT_LifetimeFromNow(t *testing.T) {
	before := time.Now().Unix()
	token, err := NewJWT("GET", "h", "/p", "k", testKey())
	require.NoError(t, err)
	after := time.Now().Unix()

	payload, err := base64.RawURLEncoding.DecodeString(strings.Split(token, ".")[1])
	require.NoError(t, err)
	var claims struct{ Nbf, Exp int64 }
	require.NoError(t, json.Unmarshal(payload, &claims))
	assert.GreaterOrEqual(t, claims.Nbf, before)
	assert.LessOrEqual(t, claims.Nbf, after)
	assert.Equal(t, int64(120), claims.Exp-claims.Nbf)
}

func TestNewJWT_BadKey(t *testing.T) {
	_, err := NewJWT("GET", "h", "/p", "k", ed25519.PrivateKey(make([]byte, 10)))
	assert.Error(t, err)
}

func TestParseAPIKeyJSON(t *testing.T) {
	key := testKey()
	encoded := base64.StdEncoding.EncodeToString(key)

	parsed, err := ParseAPIKeyJSON([]byte(fmt.Sprintf(`{"id":"key-id","privateKey":%q}`, encoded)))
	require.NoError(t, err)
	assert.Equal(t, "key-id", parsed.ID)
	assert.Equal(t, key, parsed.PrivateKey)

	mismatched := append(append([]byte{}, key.Seed()...), make([]byte, 32)...)

	for name, data := range map[string]string{
		"not json":     `not json ` + encoded,
		"no id":        fmt.Sprintf(`{"privateKey":%q}`, encoded),
		"not base64":   `{"id":"k","privateKey":"%%%"}`,
		"short key":    fmt.Sprintf(`{"id":"k","privateKey":%q}`, base64.StdEncoding.EncodeToString(key[:32])),
		"inconsistent": fmt.Sprintf(`{"id":"k","privateKey":%q}`, base64.StdEncoding.EncodeToString(mismatched)),
	} {
		t.Run(name, func(t *testing.T) {
			_, err := ParseAPIKeyJSON([]byte(data))
			require.Error(t, err)
			assert.NotContains(t, err.Error(), encoded[:16], "an error must never quote the key")
		})
	}
}
