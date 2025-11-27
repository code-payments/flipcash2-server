package thirdparty

import (
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"time"
)

// From https://docs.cdp.coinbase.com/get-started/authentication/jwt-authentication#go

type header struct {
	Typ   string `json:"typ"`
	Alg   string `json:"alg"`
	Kid   string `json:"kid"`
	Nonce string `json:"nonce"`
}

type payload struct {
	Iss string `json:"iss"`
	Nbf int64  `json:"nbf"`
	Exp int64  `json:"exp"`
	Sub string `json:"sub"`
	URI string `json:"uri"`
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

func getCoinbaseJwt(
	method, host, path string,
	apiKey string,
	privateKey ed25519.PrivateKey,
) (string, error) {
	uri := fmt.Sprintf("%s %s%s", method, host, path)

	now := time.Now().Unix()
	pl := payload{"cdp", now, now + 120, apiKey, uri}

	nonce, err := randomNonce()
	if err != nil {
		return "", err
	}
	h := header{"JWT", "EdDSA", apiKey, nonce}

	hBytes, _ := json.Marshal(h)
	plBytes, _ := json.Marshal(pl)

	unsigned := fmt.Sprintf("%s.%s", base64URL(hBytes), base64URL(plBytes))

	signature := ed25519.Sign(privateKey, []byte(unsigned))
	jwt := fmt.Sprintf("%s.%s", unsigned, base64URL(signature))

	return jwt, nil
}
