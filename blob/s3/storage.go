// Package s3 implements blob.ObjectStorage on top of Amazon S3 for storage and
// a CloudFront CDN for delivery. It spans two buckets: clients POST bytes to a
// presigned policy in the UPLOAD bucket, the server reads them back to validate
// them, and validated bytes are copied into the ORIGIN bucket (fronted by
// CloudFront) and removed from the upload bucket. Downloads are short-lived
// CloudFront signed URLs against the origin bucket. The server never proxies
// blob bytes — it only reads them back (via GetUploaded) to derive metadata
// during finalization.
package s3

import (
	"context"
	"crypto/hmac"
	"crypto/rsa"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/cloudfront/sign"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	smithy "github.com/aws/smithy-go"
	"google.golang.org/protobuf/types/known/timestamppb"

	blobpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/blob/v1"

	"github.com/code-payments/flipcash2-server/blob"
)

const (
	sigV4Algorithm = "AWS4-HMAC-SHA256"
	s3Service      = "s3"
)

// Config configures the two-bucket S3 + CloudFront backend.
type Config struct {
	// UploadBucket is the bucket clients upload to via presigned POST policies. It
	// holds untrusted, not-yet-validated bytes and should carry a lifecycle rule
	// to expire abandoned uploads.
	UploadBucket string

	// OriginBucket is the bucket the CDN serves from. Only validated bytes,
	// promoted out of the upload bucket, ever land here.
	OriginBucket string

	// Region is the AWS region of the buckets. It scopes the SigV4 credential and
	// signing key for upload POST policies and seeds the default upload endpoint.
	Region string

	// UploadEndpointURL is the base URL (scheme + host) the upload POST targets,
	// already resolved for the upload bucket — e.g. a regional endpoint
	// "https://my-bucket.s3.us-east-1.amazonaws.com" or an S3 Transfer
	// Acceleration endpoint "https://my-bucket.s3-accelerate.amazonaws.com". When
	// empty, the regional virtual-hosted-style endpoint is derived from the bucket
	// and Region. It does not affect the POST policy signature, which
	// is always scoped to the bucket's region, so pointing it at an accelerated
	// (or dualstack/custom) endpoint is safe.
	UploadEndpointURL string

	// UploadTTL is how long a presigned upload policy stays valid.
	UploadTTL time.Duration

	// DownloadTTL is how long a minted CloudFront download URL stays valid.
	DownloadTTL time.Duration

	// CDNBaseURL is the CloudFront distribution base URL (scheme + host, e.g.
	// "https://d111111abcdef8.cloudfront.net") fronting the origin bucket.
	// Download URLs are this joined with the object key, then signed.
	CDNBaseURL string

	// CloudFrontKeyID is the CloudFront public key id paired with PrivateKey.
	CloudFrontKeyID string

	// PrivateKey is the RSA private key whose public half is registered with
	// CloudFront; it signs download URLs.
	PrivateKey *rsa.PrivateKey
}

// Storage is the two-bucket S3 + CloudFront implementation of
// blob.ObjectStorage.
type Storage struct {
	cfg    Config
	client *s3.Client
	signer *sign.URLSigner
}

// NewStorage builds a Storage over an existing S3 client. The client is
// constructed and configured (region, credentials) by the caller, mirroring how
// the DynamoDB-backed stores take a ready *dynamodb.Client. The client's
// credentials and the configured Region are used to sign upload POST policies.
func NewStorage(client *s3.Client, cfg Config) *Storage {
	return &Storage{
		cfg:    cfg,
		client: client,
		signer: sign.NewURLSigner(cfg.CloudFrontKeyID, cfg.PrivateKey),
	}
}

func (s *Storage) PresignUpload(ctx context.Context, key, mimeType string, sizeBytes uint64) (*blobpb.UploadTarget, error) {
	creds, err := s.client.Options().Credentials.Retrieve(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to retrieve credentials: %w", err)
	}
	region := s.cfg.Region

	now := time.Now().UTC()
	amzDate := now.Format("20060102T150405Z")
	shortDate := now.Format("20060102")
	expiresAt := now.Add(s.cfg.UploadTTL)
	credential := fmt.Sprintf("%s/%s/%s/%s/aws4_request", creds.AccessKeyID, shortDate, region, s3Service)
	size := int64(sizeBytes)

	// The signed policy is what S3 enforces: it pins the bucket and key, requires
	// the Content-Type to match exactly, and bounds the body to exactly sizeBytes
	// via content-length-range (min == max). Anything else is rejected by S3.
	conditions := []any{
		map[string]string{"bucket": s.cfg.UploadBucket},
		[]any{"eq", "$key", key},
		map[string]string{"Content-Type": mimeType},
		[]any{"content-length-range", size, size},
		map[string]string{"x-amz-algorithm": sigV4Algorithm},
		map[string]string{"x-amz-credential": credential},
		map[string]string{"x-amz-date": amzDate},
	}
	if creds.SessionToken != "" {
		conditions = append(conditions, map[string]string{"x-amz-security-token": creds.SessionToken})
	}

	policy := map[string]any{
		"expiration": expiresAt.Format("2006-01-02T15:04:05.000Z"),
		"conditions": conditions,
	}
	policyJSON, err := json.Marshal(policy)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal upload policy: %w", err)
	}
	encodedPolicy := base64.StdEncoding.EncodeToString(policyJSON)
	signature := hex.EncodeToString(hmacSHA256(sigV4SigningKey(creds.SecretAccessKey, shortDate, region), encodedPolicy))

	formFields := map[string]string{
		"key":              key,
		"Content-Type":     mimeType,
		"x-amz-algorithm":  sigV4Algorithm,
		"x-amz-credential": credential,
		"x-amz-date":       amzDate,
		"policy":           encodedPolicy,
		"x-amz-signature":  signature,
	}
	if creds.SessionToken != "" {
		formFields["x-amz-security-token"] = creds.SessionToken
	}

	return &blobpb.UploadTarget{
		Method:     blobpb.UploadTarget_POST,
		Url:        s.uploadEndpoint(region),
		FormFields: formFields,
		ExpiresAt:  timestamppb.New(expiresAt),
	}, nil
}

// uploadEndpoint is the URL the upload POST is sent to: the configured override
// when set, otherwise the regional virtual-hosted-style endpoint for the bucket.
func (s *Storage) uploadEndpoint(region string) string {
	if s.cfg.UploadEndpointURL != "" {
		return s.cfg.UploadEndpointURL
	}
	return fmt.Sprintf("https://%s.s3.%s.amazonaws.com", s.cfg.UploadBucket, region)
}

func (s *Storage) GetUploaded(ctx context.Context, key string) ([]byte, error) {
	out, err := s.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(s.cfg.UploadBucket),
		Key:    aws.String(key),
	})
	if err != nil {
		if isNotFound(err) {
			return nil, blob.ErrObjectNotFound
		}
		return nil, fmt.Errorf("failed to get uploaded object: %w", err)
	}
	defer out.Body.Close()

	data, err := io.ReadAll(out.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read uploaded object: %w", err)
	}
	return data, nil
}

func (s *Storage) CopyToOrigin(ctx context.Context, key string) error {
	// Copy the validated bytes from the upload bucket into the origin bucket
	// under the same key. CopySource is "<bucket>/<key>"; blob keys are hex, so
	// no escaping is required. CopyObject overwrites the destination, so it is
	// safe to repeat.
	_, err := s.client.CopyObject(ctx, &s3.CopyObjectInput{
		Bucket:     aws.String(s.cfg.OriginBucket),
		Key:        aws.String(key),
		CopySource: aws.String(s.cfg.UploadBucket + "/" + key),
	})
	if err != nil {
		return fmt.Errorf("failed to copy object to origin bucket: %w", err)
	}
	return nil
}

func (s *Storage) DeleteUpload(ctx context.Context, key string) error {
	// S3 DeleteObject is idempotent: deleting an absent key returns success.
	if _, err := s.client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(s.cfg.UploadBucket),
		Key:    aws.String(key),
	}); err != nil {
		return fmt.Errorf("failed to delete object from upload bucket: %w", err)
	}
	return nil
}

func (s *Storage) SignDownloadURL(_ context.Context, key string) (string, error) {
	rawURL := strings.TrimRight(s.cfg.CDNBaseURL, "/") + "/" + key
	signed, err := s.signer.Sign(rawURL, time.Now().Add(s.cfg.DownloadTTL))
	if err != nil {
		return "", fmt.Errorf("failed to sign download url: %w", err)
	}
	return signed, nil
}

// sigV4SigningKey derives the SigV4 signing key for the S3 service.
func sigV4SigningKey(secret, shortDate, region string) []byte {
	kDate := hmacSHA256([]byte("AWS4"+secret), shortDate)
	kRegion := hmacSHA256(kDate, region)
	kService := hmacSHA256(kRegion, s3Service)
	return hmacSHA256(kService, "aws4_request")
}

func hmacSHA256(key []byte, data string) []byte {
	h := hmac.New(sha256.New, key)
	h.Write([]byte(data))
	return h.Sum(nil)
}

// isNotFound reports whether an S3 error means the object does not exist.
func isNotFound(err error) bool {
	var noSuchKey *s3types.NoSuchKey
	if errors.As(err, &noSuchKey) {
		return true
	}
	var apiErr smithy.APIError
	if errors.As(err, &apiErr) {
		switch apiErr.ErrorCode() {
		case "NoSuchKey", "NotFound":
			return true
		}
	}
	return false
}
