package blob

import (
	"bytes"
	"fmt"
	"image"
	"image/png"
	"math"

	"github.com/google/uuid"
	"golang.org/x/image/draw"

	blobpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/blob/v1"
)

// Renditions are derived per CONTENT KIND: what variants a piece of media is
// stored as, and how they are produced, depends on whether it is an image, a
// video, audio, and so on. An image ladder is pixel sizes encoded as JPEG/PNG; a
// video's would be resolutions and bitrates plus a poster-frame still. Only images
// are supported today, so this file holds the IMAGE rendition strategy —
// everything named image* — alongside the kind-agnostic machinery every strategy
// shares (the RenditionType roles, the deterministic-id derivation, and the
// geometry helper).
//
// When another kind is added, it brings its own ladder and generation (e.g.
// videoRenditionSpecs and a generateVideoRenditions), selected by kind during
// finalization; it does NOT widen the image ladder. The shared pieces below —
// RenditionID, scaledDimensions, and RenditionType.ToProtoRole — are written to
// serve any raster-or-timed kind, not just images.

// --- kind-agnostic machinery -------------------------------------------------

// renditionNamespace seeds the deterministic (version-5) ids derived for
// renditions of every kind. It is an arbitrary fixed UUID: pairing it with a
// per-rendition fingerprint via uuid.NewSHA1 yields a stable id, and nothing else
// collides into that space.
var renditionNamespace = uuid.MustParse("b7d3f2a1-8c4e-4f6b-9a2d-1e5c7f0a3b64")

// renditionID derives a rendition's blob id deterministically from a fingerprint
// string that uniquely describes its output spec. Only the hashing MECHANISM is
// shared here — the fingerprint itself is built per kind (see imageRenditionID),
// since what identifies a rendition differs by kind (an image is dimensions +
// format + quality; a video would be resolution + bitrate + codec). Keeping the
// namespace and derivation in one place is what lets every kind's ids share a
// collision-free space.
//
// Deriving the id (rather than minting a random one) is what makes generation
// idempotent: a replayed or resumed finalize recomputes the same id, so it
// recreates the same record and overwrites the same object instead of orphaning a
// duplicate. Folding the full output spec into the fingerprint upholds byte
// immutability too: retuning a ladder yields a NEW id and key rather than mutating
// the bytes a live id already points at.
func renditionID(fingerprint string) *blobpb.BlobId {
	id := uuid.NewSHA1(renditionNamespace, []byte(fingerprint))
	value := id
	return &blobpb.BlobId{Value: value[:]}
}

// scaledDimensions returns the pixel dimensions a rendition bounded by
// maxLongestSide takes for a source sized width x height, preserving aspect ratio
// and never upscaling. Each axis is floored at 1 so an extreme aspect ratio still
// yields a valid (if slivered) result rather than a zero dimension. It is pure
// geometry, shared by any kind that scales a visual frame.
func scaledDimensions(width, height, maxLongestSide uint32) (uint32, uint32) {
	longest := max(width, height)
	if longest <= maxLongestSide {
		return width, height
	}
	scale := float64(maxLongestSide) / float64(longest)
	return max(1, uint32(math.Round(float64(width)*scale))), max(1, uint32(math.Round(float64(height)*scale)))
}

// ToProtoRole maps an internal RenditionType onto the wire Rendition.Role a
// hydrated rendition carries. The roles are kind-agnostic — a video, too, has an
// ORIGINAL, a downscaled DISPLAY, and a THUMBNAIL still.
func (r RenditionType) ToProtoRole() blobpb.Rendition_Role {
	switch r {
	case RenditionOriginal:
		return blobpb.Rendition_ORIGINAL
	case RenditionDisplay:
		return blobpb.Rendition_DISPLAY
	case RenditionThumbnail:
		return blobpb.Rendition_THUMBNAIL
	default:
		return blobpb.Rendition_UNKNOWN
	}
}

// --- image rendition strategy ------------------------------------------------

// imageRenditionSpec is one rung of the image rendition ladder: which
// RenditionType the derived blob is, and the bound on its longest side. The bytes'
// actual dimensions are derived from the original's aspect ratio (see
// scaledDimensions), so a rung is a bounding box, not a fixed size.
type imageRenditionSpec struct {
	Rendition      RenditionType
	MaxLongestSide uint32
}

// imageRenditionSpecs is the image ladder derived from every accepted original,
// ordered small to large. It carries two sizes per role — the proto models several
// renditions per role precisely so a client can pick the smallest that covers its
// display size at the device's pixel ratio:
//
//   - THUMBNAIL 160: list avatars and reply previews (bulk-fetched, so kept small)
//   - THUMBNAIL 320: the profile-screen avatar and media-grid cell
//   - DISPLAY   800: the inline chat bubble
//   - DISPLAY  1600: the full-screen, non-zoomed view
//
// A rung whose bound is at or above the original's longest side is skipped rather
// than upscaled (see generateImageRenditions), so a small original simply yields a
// shorter ladder and the client falls back to the ORIGINAL for anything larger.
// The pixel bounds are deliberately not on the wire: role is the semantic handle
// and the concrete dimensions travel as each rendition's image metadata, so the
// ladder can be retuned server-side without a client change — a retune just mints
// new rendition ids (see imageRenditionID) and ages the old ones out.
//
// This ladder is IMAGE-specific; another content kind supplies its own rather than
// reusing these sizes.
var imageRenditionSpecs = []imageRenditionSpec{
	{Rendition: RenditionThumbnail, MaxLongestSide: 160},
	{Rendition: RenditionThumbnail, MaxLongestSide: 320},
	{Rendition: RenditionDisplay, MaxLongestSide: 800},
	{Rendition: RenditionDisplay, MaxLongestSide: 1600},
}

// imageEncoding is how a rendition's pixels are turned into bytes: the output
// format plus the format-specific parameters that determine those bytes. It is
// derived jointly from the role and the source's alpha, and it is the single place
// that decision lives — so a rendition's id fingerprint and its actual encoding can
// never disagree about the format or its knobs.
type imageEncoding struct {
	mimeType string

	// jpegQuality is the JPEG quality (1-100). It is meaningful only for image/jpeg
	// — a lossy format — and is left zero (and ignored) for PNG, which is lossless.
	jpegQuality int
}

// imageEncodingFor picks the encoding for a rendition: a transparent source is kept
// as PNG so the alpha survives (lossless, so it has no quality knob); an opaque one
// — including an opaque PNG such as a screenshot — becomes the far smaller JPEG,
// with quality tuned per role (thumbnails tolerate more compression than the display
// renditions a user actually scrutinizes). It depends only on the role and the
// persisted HasAlpha bit, so a rendition's encoding — and thus its id — is computable
// without decoding.
func imageEncodingFor(rendition RenditionType, hasAlpha bool) imageEncoding {
	if hasAlpha {
		return imageEncoding{mimeType: "image/png"}
	}
	quality := 80
	if rendition == RenditionThumbnail {
		quality = 75
	}
	return imageEncoding{mimeType: "image/jpeg", jpegQuality: quality}
}

// fingerprint is the encoding's contribution to a rendition id: the parameters that
// determine the bytes for THIS format. JPEG folds in its quality, so retuning it
// mints new ids; PNG is lossless at a fixed compression, so its bytes are determined
// by the pixels alone and it contributes only its format — a JPEG-quality retune
// therefore never churns byte-identical PNG renditions. (If PNG compression ever
// became role-dependent, its level would join the fingerprint here.)
func (e imageEncoding) fingerprint() string {
	if e.mimeType == "image/jpeg" {
		return fmt.Sprintf("%s|q%d", e.mimeType, e.jpegQuality)
	}
	return e.mimeType
}

// imageRenditionID derives an image rendition's blob id from its parent and full
// image output spec — role, pixel dimensions, and the encoding fingerprint. These
// fields are image-specific (a different kind identifies a rendition by its own
// parameters), so the id lives in the image strategy; it defers the actual hashing
// to the shared renditionID.
func imageRenditionID(parentID *blobpb.BlobId, rendition RenditionType, width, height uint32, encoding imageEncoding) *blobpb.BlobId {
	return renditionID(fmt.Sprintf("%x|%d|%dx%d|%s", parentID.Value, rendition, width, height, encoding.fingerprint()))
}

// imageRenditionSlug is the storage-key path segment for a rendition type. It is a
// closed set — only the derived types have their own bytes on disk — so an
// unrecognized type yields "" and imageRenditionStorageKey rejects it.
func imageRenditionSlug(rendition RenditionType) string {
	switch rendition {
	case RenditionDisplay:
		return "display"
	case RenditionThumbnail:
		return "thumbnail"
	default:
		return ""
	}
}

// imageRenditionStorageKey derives the object key an image rendition's bytes live
// under. It groups renditions beneath the same per-media prefix as the original
// (images/<uuid>/...) and names them by role and dimensions, so distinct rungs
// never collide and the key is self-describing:
//
//	images/<parent-uuid>/display_1600x900.jpg
//
// The dimensions are in the key — not just the id — because two rungs of the same
// role differ only by size, and a ladder retune must land on a new key rather than
// overwrite the old bytes. Like the original's StorageKey it hardcodes the image
// layout; another kind uses its own prefix and key scheme.
func imageRenditionStorageKey(parentID *blobpb.BlobId, rendition RenditionType, width, height uint32, mimeType string) (string, error) {
	if err := parentID.Validate(); err != nil {
		return "", err
	}
	slug := imageRenditionSlug(rendition)
	if slug == "" {
		return "", fmt.Errorf("unsupported rendition type %d for storage key", rendition)
	}
	ext := extensionForMimeType(mimeType)
	if ext == "" {
		return "", fmt.Errorf("unsupported rendition mime type %q for storage key", mimeType)
	}
	return fmt.Sprintf("images/%s/%s_%dx%d%s", IDString(parentID), slug, width, height, ext), nil
}

// resampleImage returns img scaled to width x height using a Catmull-Rom filter —
// higher quality than the bilinear filter used for the throwaway moderation and
// BlurHash renderings, because a rendition is what the user actually sees. It
// composites onto a fully transparent RGBA canvas, so alpha in a PNG source is
// preserved.
func resampleImage(img image.Image, width, height int) image.Image {
	dst := image.NewRGBA(image.Rect(0, 0, width, height))
	draw.CatmullRom.Scale(dst, dst.Bounds(), img, img.Bounds(), draw.Src, nil)
	return dst
}

// encode renders img in this encoding: a JPEG at the encoding's quality, or a
// lossless PNG (which has no quality knob).
func (e imageEncoding) encode(img image.Image) ([]byte, error) {
	switch e.mimeType {
	case "image/jpeg":
		return encodeJPEG(img, e.jpegQuality)
	case "image/png":
		var buf bytes.Buffer
		encoder := png.Encoder{CompressionLevel: png.DefaultCompression}
		if err := encoder.Encode(&buf, img); err != nil {
			return nil, err
		}
		return buf.Bytes(), nil
	default:
		return nil, fmt.Errorf("unsupported rendition mime type %q", e.mimeType)
	}
}
