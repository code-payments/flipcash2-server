package blob

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sort"
	"time"

	"google.golang.org/protobuf/types/known/durationpb"

	blobpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/blob/v1"
)

// uploadPolicyTTL is how long a client may rely on a fetched policy before
// re-fetching it. The policy is static today, so this is generous; clients are
// also expected to refresh whenever they observe a version mismatch (including
// one echoed on a denied upload), so a stale cache self-corrects well before
// the TTL lapses.
const uploadPolicyTTL = 24 * time.Hour

// currentPolicy is the upload policy advertised to every caller. It is static,
// so it is built once at startup and then shared read-only across requests
// rather than reconstructed (and re-hashed) on each call.
var currentPolicy = buildUploadPolicy()

// currentPolicyVersion is the version token of currentPolicy, echoed on a
// policy-driven upload denial so a client can detect a stale cached policy. It
// aliases currentPolicy.Version, so the advertised policy and the version a
// denial reports can never drift apart.
var currentPolicyVersion = currentPolicy.Version

// buildUploadPolicy assembles the upload policy advertised to clients: one
// constraint entry per supported image MIME type, each pinned to the same byte,
// dimension, and pixel ceilings the server enforces authoritatively when it
// reserves the upload (InitiateExternalUpload) and inspects the stored bytes
// (InspectImage). The policy is advisory — it lets a client validate and resize
// before uploading — but it never advertises a limit the server does not itself
// enforce. It is called once, to initialize currentPolicy.
func buildUploadPolicy() *blobpb.UploadPolicy {
	constraints := buildMimeTypeConstraints()
	return &blobpb.UploadPolicy{
		Version:             &blobpb.PolicyVersion{Value: policyVersion(constraints)},
		Ttl:                 durationpb.New(uploadPolicyTTL),
		MimeTypeConstraints: constraints,
	}
}

// buildMimeTypeConstraints returns the per-MIME-type constraints, one exact-type
// entry for every image type the server accepts. Every entry is an exact type
// (no wildcards), so the "most specific first" ordering the proto asks for is
// trivially satisfied; they are emitted in a stable, sorted order so the derived
// policy version is deterministic. There is deliberately no "image/*" or "*/*"
// fallback: a type with no matching entry is one the server does not accept.
func buildMimeTypeConstraints() []*blobpb.MimeTypeConstraints {
	mimeTypes := make([]string, 0, len(SupportedImageMimeTypes))
	for mimeType := range SupportedImageMimeTypes {
		mimeTypes = append(mimeTypes, mimeType)
	}
	sort.Strings(mimeTypes)

	constraints := make([]*blobpb.MimeTypeConstraints, 0, len(mimeTypes))
	for _, mimeType := range mimeTypes {
		constraints = append(constraints, &blobpb.MimeTypeConstraints{
			MimeTypePattern: mimeType,
			MaxSizeBytes:    MaxOriginalImageSizeBytes,
			Kind: &blobpb.MimeTypeConstraints_Image{
				Image: &blobpb.ImageConstraints{
					MaxWidth:  maxImageDimension,
					MaxHeight: maxImageDimension,
					MaxPixels: maxImagePixels,
				},
			},
		})
	}
	return constraints
}

// policyVersion hashes a canonical rendering of the policy's limits into a short
// hex token. The rendering covers every advertised value, so any change to the
// TTL or a constraint yields a different token; the constraints arrive in a
// stable order, so an unchanged policy always yields the same token.
func policyVersion(constraints []*blobpb.MimeTypeConstraints) string {
	h := sha256.New()
	fmt.Fprintf(h, "ttl=%d;", uploadPolicyTTL)
	for _, c := range constraints {
		img := c.GetImage()
		fmt.Fprintf(h, "type=%s,size=%d,w=%d,h=%d,px=%d;",
			c.MimeTypePattern, c.MaxSizeBytes, img.GetMaxWidth(), img.GetMaxHeight(), img.GetMaxPixels())
	}
	// 16 bytes is ample to make an accidental collision between two distinct
	// policies vanishingly unlikely, and stays well inside the proto's length cap.
	return hex.EncodeToString(h.Sum(nil)[:16])
}
