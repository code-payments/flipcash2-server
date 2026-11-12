// Package cdp holds what every Coinbase Developer Platform (CDP) client
// shares: the bearer JWT that authenticates one REST request, and the API key
// that signs it.
//
// CDP authenticates each request with a short-lived JWT signed by the
// caller's Ed25519 API key, bound to that one request by its `uri` claim
// ("METHOD host/path"). See
// https://docs.cdp.coinbase.com/api-reference/v2/authentication. The same
// token is minted for two audiences: ThirdParty.GetJwt hands one to the app
// for a request the app makes itself (thirdparty/), and the server-to-server
// clients under this package (customers/, and later the bank payout leg) sign
// their own requests with it.
package cdp

import (
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

// JWTLifetime is how long a token from NewJWT is valid, from its issue time.
// CDP accepts at most two minutes.
const JWTLifetime = 120 * time.Second

// From https://docs.cdp.coinbase.com/get-started/authentication/jwt-authentication#go
//
// The field order of both structs is the order the JSON is written in, and is
// kept as it was when the signer lived in thirdparty/.

type jwtHeader struct {
	Typ   string `json:"typ"`
	Alg   string `json:"alg"`
	Kid   string `json:"kid"`
	Nonce string `json:"nonce"`
}

type jwtPayload struct {
	Iss string `json:"iss"`
	Nbf int64  `json:"nbf"`
	Exp int64  `json:"exp"`
	Sub string `json:"sub"`
	URI string `json:"uri"`
}

// NewJWT returns a CDP bearer token for one request, signed with the API key
// keyID / privateKey. Its `uri` claim is "METHOD host/path", built exactly as
// given: host is the bare host (with a port only if the request URL has one)
// and path is the URL path with no query string, which CDP leaves out of the
// claim. The token is valid from now for JWTLifetime and carries a random
// nonce, so two tokens for the same request differ.
//
// NewJWT does not validate method, host or path: it signs whatever request it
// is given, so which requests are signed is its caller's to decide.
// ThirdParty.GetJwt, whose caller is an app, signs only the requests on its
// allowlist (thirdparty/coinbase.go). A private key of the wrong length is
// refused rather than signed with (ed25519.Sign would panic).
func NewJWT(method, host, path, keyID string, privateKey ed25519.PrivateKey) (string, error) {
	return newJWT(time.Now(), method, host, path, keyID, privateKey)
}

func newJWT(now time.Time, method, host, path, keyID string, privateKey ed25519.PrivateKey) (string, error) {
	if len(privateKey) != ed25519.PrivateKeySize {
		return "", errors.New("cdp: private key is not an ed25519 private key")
	}

	uri := fmt.Sprintf("%s %s%s", method, host, path)

	issuedAt := now.Unix()
	pl := jwtPayload{"cdp", issuedAt, issuedAt + int64(JWTLifetime/time.Second), keyID, uri}

	nonce, err := randomNonce()
	if err != nil {
		return "", err
	}
	h := jwtHeader{"JWT", "EdDSA", keyID, nonce}

	hBytes, _ := json.Marshal(h)
	plBytes, _ := json.Marshal(pl)

	unsigned := fmt.Sprintf("%s.%s", base64URL(hBytes), base64URL(plBytes))

	signature := ed25519.Sign(privateKey, []byte(unsigned))
	return fmt.Sprintf("%s.%s", unsigned, base64URL(signature)), nil
}

func base64URL(data []byte) string {
	return base64.RawURLEncoding.EncodeToString(data)
}

func randomNonce() (string, error) {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return hex.EncodeToString(b), nil
}

// APIKey is a CDP secret API key: its ID (the JWT's `kid` and `sub`) and its
// Ed25519 private key.
type APIKey struct {
	ID         string
	PrivateKey ed25519.PrivateKey
}

// ParseAPIKeyJSON parses the key file the CDP portal downloads for an Ed25519
// secret API key: a JSON object with `id` and `privateKey`, the latter the
// standard base64 encoding of the 64-byte private key (seed then public key).
// Errors never quote the file, since it is a secret.
func ParseAPIKeyJSON(data []byte) (APIKey, error) {
	var file struct {
		ID         string `json:"id"`
		PrivateKey string `json:"privateKey"`
	}
	if err := json.Unmarshal(data, &file); err != nil {
		return APIKey{}, errors.New("cdp: api key file is not a json object with id and privateKey")
	}
	if file.ID == "" {
		return APIKey{}, errors.New("cdp: api key file has no id")
	}
	raw, err := base64.StdEncoding.DecodeString(file.PrivateKey)
	if err != nil || len(raw) != ed25519.PrivateKeySize {
		return APIKey{}, errors.New("cdp: api key file's privateKey is not a base64 ed25519 private key")
	}
	key := ed25519.PrivateKey(raw)
	// The last 32 bytes must be the public key of the seed in the first 32,
	// or every signature would fail to verify at CDP.
	if !key.Public().(ed25519.PublicKey).Equal(ed25519.NewKeyFromSeed(key.Seed()).Public()) {
		return APIKey{}, errors.New("cdp: api key file's privateKey is inconsistent")
	}
	return APIKey{ID: file.ID, PrivateKey: key}, nil
}
