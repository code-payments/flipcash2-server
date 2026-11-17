package flipcashteam

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/secretsmanager"
)

// groupKeySecretSize is the size of the secret LoadGroupKeySecret returns:
// the exact size of the stored secret, though deriveGroupKey would take a
// longer one.
const groupKeySecretSize = minGroupKeySecretSize

// LoadGroupKeySecret fetches the welcome groups' key derivation secret (see
// WelcomeGroupConfig.GroupKeySecret) from AWS Secrets Manager. secretID is the
// secret's name or ARN. The caller constructs and configures the
// *secretsmanager.Client, as for blob/s3.LoadCloudFrontPrivateKey.
//
// The secret is exactly groupKeySecretSize bytes, stored either as a string
// holding them hex-encoded (as `openssl rand -hex 32` makes one, surrounding
// whitespace ignored) or as binary holding them raw. Any other value is
// refused rather than used: the stored format is fixed with the first welcome
// group, and a value pasted wrong or encoded twice would derive keys for no
// group that exists. An error never carries the value, or any part of it.
func LoadGroupKeySecret(ctx context.Context, client *secretsmanager.Client, secretID string) ([]byte, error) {
	out, err := client.GetSecretValue(ctx, &secretsmanager.GetSecretValueInput{
		SecretId: aws.String(secretID),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to get secret %q: %w", secretID, err)
	}
	secret, err := parseGroupKeySecret(out.SecretString, out.SecretBinary)
	if err != nil {
		return nil, fmt.Errorf("secret %q: %w", secretID, err)
	}
	return secret, nil
}

// parseGroupKeySecret is LoadGroupKeySecret's decoding of a secret value, a
// string secret's or a binary one's.
func parseGroupKeySecret(secretString *string, secretBinary []byte) ([]byte, error) {
	var secret []byte
	switch {
	case secretString != nil:
		decoded, err := hex.DecodeString(strings.TrimSpace(*secretString))
		if err != nil {
			// hex's error quotes the offending byte, so it is not wrapped.
			return nil, errors.New("value is not hex")
		}
		secret = decoded
	case len(secretBinary) > 0:
		secret = append([]byte(nil), secretBinary...)
	default:
		return nil, errors.New("secret has no value")
	}
	if len(secret) != groupKeySecretSize {
		return nil, fmt.Errorf("value is %d bytes, want %d", len(secret), groupKeySecretSize)
	}
	return secret, nil
}
