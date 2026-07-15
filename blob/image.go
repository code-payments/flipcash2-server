package blob

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"image"
	"image/jpeg"
	"math"

	// Register the standard PNG decoder so image.Decode recognizes it.
	_ "image/png"

	"github.com/buckket/go-blurhash"

	"golang.org/x/image/draw"

	// Register the WebP decoder.
	_ "golang.org/x/image/webp"
)

// InspectImage failure categories. They are wrapped into the descriptive error
// it returns so finalization can classify a rejection into the right
// RejectionReason without re-deriving why the bytes were unacceptable. A failure
// carrying none of these is an internal processing fault.
var (
	// ErrImageCorrupt means the bytes could not be read or decoded as an image.
	ErrImageCorrupt = errors.New("image is corrupt or undecodable")

	// ErrImageUnsupportedType means the bytes are a kind the service does not
	// accept — an unsupported format, or an animated image.
	ErrImageUnsupportedType = errors.New("unsupported image type")

	// ErrImageTooLarge means the image's pixel dimensions exceed the limits.
	ErrImageTooLarge = errors.New("image exceeds dimension limits")

	// ErrImagePrivacyMetadata means the image still carries embedded
	// privacy-sensitive metadata that the client was required to strip.
	ErrImagePrivacyMetadata = errors.New("image carries privacy-sensitive metadata")
)

const (
	// blurhashComponentsX and blurhashComponentsY control the BlurHash detail. A
	// symmetric 4x4 is aspect-agnostic — a sensible neutral default for images of
	// any orientation — and at 36 chars comfortably fits the proto's 64-char
	// budget.
	blurhashComponentsX = 4
	blurhashComponentsY = 4

	// blurhashMaxDimension bounds the longest side of the image actually fed to
	// the BlurHash encoder. The hash is a handful of low-frequency components, so
	// a small downscaled copy yields an essentially identical result far more
	// cheaply than scanning every pixel of a full-resolution image.
	blurhashMaxDimension = 64

	// blurhashMinShortDimension keeps the shortest side from collapsing on an
	// extreme aspect ratio, so the cross-axis still has enough samples per
	// component for a faithful hash. Capping only the longest side would otherwise
	// starve the short axis (e.g. a 10:1 panorama → 64x6).
	blurhashMinShortDimension = 16

	// MaxOriginalImageSizeBytes bounds the declared size of an ORIGINAL image upload.
	// It is pinned into the upload policy, so storage rejects anything larger before a
	// single byte lands.
	MaxOriginalImageSizeBytes = 8 * 1024 * 1024 // 8 MiB

	// maxImageDimension bounds an image's width and height. It is set to the format
	// ceiling — the largest dimension JPEG can even encode (its size fields are
	// 16-bit); PNG permits more but the pixel cap below bounds it, and WebP is lower
	// still at 16,383. So this is only a format-sanity bound, not a
	// real resource limit: the total pixel cap is the actual memory guard, and
	// unlike a per-axis limit it judges an image by its area, not its shape — so an
	// extreme aspect ratio (a wide panorama, a long screenshot) is accepted as long
	// as it fits the pixel budget, never rejected for its shape alone. Checked from
	// the header before the full image is decoded.
	maxImageDimension = 65_535

	// maxImagePixels bounds the total pixel count (width × height) the server will
	// decode. It is checked from the image header via DecodeConfig before the full
	// image is decoded, so a small compressed file cannot expand into an enormous
	// in-memory pixel buffer (a decompression bomb) — a byte-size limit alone does
	// not bound that. The cap is generous enough for high-resolution phone photos
	// (a 48MP shot is 48,000,000 pixels).
	maxImagePixels = 50_000_000
)

// imageFormatToMimeType maps the format names returned by image.Decode to the
// canonical MIME types this service supports.
var imageFormatToMimeType = map[string]string{
	"jpeg": "image/jpeg",
	"png":  "image/png",
	"webp": "image/webp",
}

// mimeTypeToExtension maps a supported MIME type to its canonical file extension
// (with leading dot), used to give stored objects and signed URLs a meaningful
// extension.
var mimeTypeToExtension = map[string]string{
	"image/jpeg": ".jpg",
	"image/png":  ".png",
	"image/webp": ".webp",
}

// extensionForMimeType returns the canonical file extension (with leading dot)
// for a MIME type, or "" if it is not one of the supported types.
func extensionForMimeType(mimeType string) string {
	return mimeTypeToExtension[mimeType]
}

// ImageInspection is the result of decoding image bytes: the MIME type
// authoritatively derived from the bytes plus the intrinsic image metadata.
type ImageInspection struct {
	MimeType string
	Metadata *ImageMetadata

	// Decoded is the decoded image, retained so callers can derive other
	// renderings (e.g. the moderation payload) without decoding the bytes again.
	Decoded image.Image
}

// InspectImage decodes the bytes as an image and derives their authoritative
// MIME type, pixel dimensions, and BlurHash. It returns an error if the bytes are
// not a decodable image of a supported format, if the image's pixel count exceeds
// maxImagePixels, or if it still carries privacy-sensitive metadata; callers treat
// any of these as a rejection.
func InspectImage(data []byte) (*ImageInspection, error) {
	// Read only the header first to bound the pixel count before decoding the
	// full image into memory. int64 math avoids overflow on a hostile header that
	// declares enormous dimensions.
	config, headerFormat, err := image.DecodeConfig(bytes.NewReader(data))
	if err != nil {
		return nil, fmt.Errorf("failed to read image header: %v: %w", err, ErrImageCorrupt)
	}
	if int64(config.Width)*int64(config.Height) > maxImagePixels {
		return nil, fmt.Errorf("image dimensions %dx%d exceed the %d pixel limit: %w", config.Width, config.Height, maxImagePixels, ErrImageTooLarge)
	}
	if config.Width > maxImageDimension || config.Height > maxImageDimension {
		return nil, fmt.Errorf("image dimensions %dx%d exceed the %d per-axis limit: %w", config.Width, config.Height, maxImageDimension, ErrImageTooLarge)
	}
	// Reject animated images: only the first frame would be inspected and
	// moderated, but the whole animation would be served — a moderation bypass.
	// Detected from the container structure, so no extra frames are decoded.
	if isImageAnimated(headerFormat, data) {
		return nil, fmt.Errorf("animated %s images are not supported: %w", headerFormat, ErrImageUnsupportedType)
	}
	// Reject images that still carry the metadata the client is required to strip:
	// the uploaded bytes are served to recipients verbatim, so an unstripped photo
	// hands them the GPS coordinates it was taken at. Structural, so nothing is
	// decoded to check it.
	if hasPrivacyMetadata(headerFormat, data) {
		return nil, fmt.Errorf("%s image carries metadata that must be stripped before upload: %w", headerFormat, ErrImagePrivacyMetadata)
	}

	img, format, err := image.Decode(bytes.NewReader(data))
	if err != nil {
		return nil, fmt.Errorf("failed to decode image: %v: %w", err, ErrImageCorrupt)
	}

	mimeType, ok := imageFormatToMimeType[format]
	if !ok {
		return nil, fmt.Errorf("unsupported image format %q: %w", format, ErrImageUnsupportedType)
	}

	bounds := img.Bounds()
	width, height := bounds.Dx(), bounds.Dy()
	if width <= 0 || height <= 0 {
		return nil, fmt.Errorf("image has invalid dimensions %dx%d: %w", width, height, ErrImageCorrupt)
	}

	hash, err := blurhash.Encode(blurhashComponentsX, blurhashComponentsY, downscaleForBlurhash(img))
	if err != nil {
		return nil, fmt.Errorf("failed to compute blurhash: %w", err)
	}

	return &ImageInspection{
		MimeType: mimeType,
		Metadata: &ImageMetadata{
			Width:    uint32(width),
			Height:   uint32(height),
			Blurhash: hash,
			HasAlpha: hasAlpha(img),
		},
		Decoded: img,
	}, nil
}

const (
	// moderationMaxBytes caps the payload sent to the moderation provider. Sync
	// image-moderation endpoints are tuned for small images (and impose
	// account-configured size limits), and moderation does not need full
	// resolution — so a large original is downscaled and re-encoded to fit within
	// this budget before classification.
	moderationMaxBytes = 1 << 20 // ~1 MiB

	// moderationMaxDimension bounds the longest side fed to moderation. It is set
	// so a typical photo fits moderationMaxBytes at high JPEG quality on the first
	// encode — so quality reduction effectively never engages — while still far
	// exceeding the few-hundred-pixel input resolution moderation models actually
	// consume. Preserving spatial resolution matters more for content detection
	// than JPEG quality, so this is kept generous and the loop trades quality
	// before dimensions.
	moderationMaxDimension = 1280

	// moderationMinDimension is the floor the size-targeting loop shrinks to. It is
	// kept at a moderation-meaningful resolution so even the fallback path (which
	// real photos never reach) cannot produce an image too small to classify.
	moderationMinDimension = 256
)

// moderationQualitySteps are the JPEG qualities tried, highest first, when
// fitting an image into moderationMaxBytes.
var moderationQualitySteps = []int{85, 70, 55, 40}

// moderationPayload returns the bytes to submit to the moderation provider for
// an image: the original data when it already fits moderationMaxBytes, otherwise
// a downscaled JPEG rendering of the decoded image that does. It keeps the image
// as large and detailed as the budget allows.
func moderationPayload(data []byte, decoded image.Image) ([]byte, error) {
	if len(data) <= moderationMaxBytes {
		return data, nil
	}
	return encodeWithinBudget(decoded, moderationMaxBytes)
}

// encodeWithinBudget renders img as a JPEG no larger than maxBytes, lowering the
// quality and, if still over, shrinking the dimensions until it fits.
func encodeWithinBudget(img image.Image, maxBytes int) ([]byte, error) {
	scaled := limitLongestSide(img, moderationMaxDimension)
	for {
		for _, quality := range moderationQualitySteps {
			encoded, err := encodeJPEG(scaled, quality)
			if err != nil {
				return nil, err
			}
			if len(encoded) <= maxBytes {
				return encoded, nil
			}
		}

		// Still over budget at the lowest quality: shrink and retry. Bounded so the
		// loop always terminates; real photos never reach the floor.
		bounds := scaled.Bounds()
		width, height := bounds.Dx()*3/4, bounds.Dy()*3/4
		if width < moderationMinDimension || height < moderationMinDimension {
			return encodeJPEG(scaled, moderationQualitySteps[len(moderationQualitySteps)-1])
		}
		scaled = scaleTo(scaled, width, height)
	}
}

func encodeJPEG(img image.Image, quality int) ([]byte, error) {
	var buf bytes.Buffer
	if err := jpeg.Encode(&buf, img, &jpeg.Options{Quality: quality}); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// limitLongestSide returns img scaled so its longest side is at most maxLongest,
// preserving aspect ratio; images already within the bound are returned as-is.
func limitLongestSide(img image.Image, maxLongest int) image.Image {
	bounds := img.Bounds()
	width, height := bounds.Dx(), bounds.Dy()
	if max(width, height) <= maxLongest {
		return img
	}
	scale := float64(maxLongest) / float64(max(width, height))
	return scaleTo(img, int(math.Round(float64(width)*scale)), int(math.Round(float64(height)*scale)))
}

func scaleTo(img image.Image, width, height int) image.Image {
	width = max(width, 1)
	height = max(height, 1)
	dst := image.NewRGBA(image.Rect(0, 0, width, height))
	draw.ApproxBiLinear.Scale(dst, dst.Bounds(), img, img.Bounds(), draw.Over, nil)
	return dst
}

// downscaleForBlurhash returns a copy of img shrunk for BlurHash encoding,
// preserving aspect ratio. It caps the longest side at blurhashMaxDimension but
// keeps the shortest side at >= blurhashMinShortDimension, so an extreme aspect
// ratio still has enough samples per component. Images already small enough are
// returned unchanged; it never upscales.
//
// It resamples with a bilinear (averaging) filter rather than nearest-neighbor:
// BlurHash integrates the pixels into low-frequency components, so it wants block
// averages — point-sampling would alias fine high-frequency detail into the
// placeholder.
func downscaleForBlurhash(img image.Image) image.Image {
	bounds := img.Bounds()
	width, height := bounds.Dx(), bounds.Dy()
	if max(width, height) <= blurhashMaxDimension {
		return img
	}

	scale := float64(blurhashMaxDimension) / float64(max(width, height))
	if shortest := min(width, height); float64(shortest)*scale < blurhashMinShortDimension {
		// Capping the long side would starve the short axis; floor the short side
		// instead, letting the long side exceed blurhashMaxDimension.
		scale = blurhashMinShortDimension / float64(shortest)
	}
	if scale >= 1 {
		// The short side is already below the floor; upscaling adds no information.
		return img
	}

	dstWidth := max(1, int(math.Round(float64(width)*scale)))
	dstHeight := max(1, int(math.Round(float64(height)*scale)))
	return scaleTo(img, dstWidth, dstHeight)
}

// isImageAnimated reports whether the encoded image holds more than one frame. It
// inspects the container structure directly rather than decoding pixels, so a
// hostile multi-frame file cannot be used to exhaust memory (the pixel-count cap
// in InspectImage bounds a single canvas, not the number of frames).
//
// Animated images are rejected by the service: only their first frame is
// inspected and moderated, while the full animation would be served — a
// moderation bypass. A malformed stream is reported as not-animated; the regular
// decode path rejects it on its own.
func isImageAnimated(format string, data []byte) bool {
	switch format {
	case "png":
		return pngIsAnimated(data)
	case "webp":
		return webpIsAnimated(data)
	default:
		// JPEG (and anything else that decodes) is single-frame.
		return false
	}
}

// pngIsAnimated reports whether a PNG carries an acTL (animation control) chunk
// before its first IDAT — i.e. it is an APNG. The standard decoder renders only
// the default image, so the animation would otherwise be served unmoderated.
func pngIsAnimated(data []byte) bool {
	const signature = "\x89PNG\r\n\x1a\n"
	if len(data) < len(signature) || string(data[:len(signature)]) != signature {
		return false
	}

	for pos := len(signature); pos+8 <= len(data); {
		length := int(binary.BigEndian.Uint32(data[pos:]))
		switch string(data[pos+4 : pos+8]) {
		case "acTL":
			return true
		case "IDAT":
			// Animation chunks must precede the first IDAT, so this is a still PNG.
			return false
		}
		// Advance past length(4) + type(4) + data(length) + crc(4).
		pos += 12 + length
	}
	return false
}

// hasPrivacyMetadata reports whether the encoded image carries an embedded
// metadata container capable of holding personal data. The headline case is EXIF:
// a photo straight off a phone records the GPS coordinates it was taken at, plus
// the capture time and the device model and serial. XMP, IPTC, and the free-form
// comment blocks are included on the same grounds — each is an arbitrary
// key/value carrier that tools routinely fill with location and authorship.
//
// Clients strip this before uploading, so the server only has to detect it: the
// stored bytes are served to recipients verbatim, and rejecting an image keeps
// them that way, where rewriting the file server-side would break the declared
// size the upload is pinned to. It is the same reject-don't-correct posture
// finalization already takes on a mismatched size or type.
//
// The check is structural — it walks the container looking for the segments,
// decoding no pixels — and it reports nothing on a malformed stream, since the
// regular decode path rejects those on its own.
//
// Color-management data (ICC profiles, and PNG's gamma/chromaticity chunks) is
// deliberately not treated as privacy metadata: it carries nothing personal, and
// dropping it visibly shifts the colors of a wide-gamut photo.
//
// Note that stripping EXIF also discards the Orientation tag, which is
// load-bearing for display, so a client must bake the rotation into the pixels as
// it strips. That is the client's side of this contract, and it is what makes the
// dimensions and BlurHash derived here agree with what the image actually looks
// like: the Go decoders ignore Orientation entirely, so a rotated photo that kept
// its tag would be measured sideways.
func hasPrivacyMetadata(format string, data []byte) bool {
	switch format {
	case "jpeg":
		return jpegHasPrivacyMetadata(data)
	case "png":
		return pngHasPrivacyMetadata(data)
	case "webp":
		return webpHasPrivacyMetadata(data)
	default:
		return false
	}
}

// jpegAllowedAppMarkers are the APPn segments a stripped JPEG may still carry.
// APP0 is the JFIF header a baseline JPEG opens with, APP2 carries the ICC color
// profile, and APP14 is the Adobe segment declaring the color transform (without
// it, CMYK/YCCK JPEGs decode with inverted colors). None is a personal-data
// carrier. Every other APPn is: APP1 holds EXIF and XMP, APP13 holds
// IPTC/Photoshop resources, and the rest are vendor maker-note space. So this is
// an allowlist rather than a blocklist — an unrecognized APPn is rejected rather
// than waved through on the assumption that it is harmless.
var jpegAllowedAppMarkers = map[byte]bool{
	0xE0: true, // APP0  — JFIF
	0xE2: true, // APP2  — ICC profile
	0xEE: true, // APP14 — Adobe color transform
}

// jpegHasPrivacyMetadata walks a JPEG's marker segments looking for a
// metadata-carrying one. It stops at the start of scan: everything past SOS is
// entropy-coded pixel data, and the metadata segments all precede it.
func jpegHasPrivacyMetadata(data []byte) bool {
	const (
		markerSOI = 0xD8 // start of image
		markerTEM = 0x01 // temporary — standalone, no payload
		markerSOS = 0xDA // start of scan
		markerEOI = 0xD9 // end of image
		markerCOM = 0xFE // free-form comment
	)

	if len(data) < 2 || data[0] != 0xFF || data[1] != markerSOI {
		return false
	}

	for pos := 2; pos+1 < len(data); {
		if data[pos] != 0xFF {
			return false // malformed; let the decoder reject it
		}
		marker := data[pos+1]

		switch {
		case marker == 0xFF:
			// A marker may be padded with any number of 0xFF fill bytes.
			pos++
			continue
		case marker == markerSOI || marker == markerTEM || (marker >= 0xD0 && marker <= 0xD7):
			// Standalone markers (the RSTn restart markers included): no payload.
			pos += 2
			continue
		case marker == markerSOS || marker == markerEOI:
			return false
		}

		// Every remaining marker carries a big-endian length that counts itself.
		if pos+4 > len(data) {
			return false
		}
		length := int(binary.BigEndian.Uint16(data[pos+2:]))
		if length < 2 {
			return false // malformed
		}
		if marker == markerCOM || (marker >= 0xE0 && marker <= 0xEF && !jpegAllowedAppMarkers[marker]) {
			return true
		}
		pos += 2 + length
	}
	return false
}

// pngMetadataChunks are the PNG chunk types that carry personal data: eXIf holds
// a verbatim EXIF block, and the three text chunks are arbitrary key/value stores
// (iTXt is where XMP lives). The color-management chunks (iCCP, gAMA, cHRM, sRGB)
// are deliberately absent — they are kept.
var pngMetadataChunks = map[string]bool{
	"eXIf": true,
	"tEXt": true,
	"iTXt": true,
	"zTXt": true,
}

// pngHasPrivacyMetadata walks a PNG's chunk structure looking for a metadata
// chunk. Unlike the APNG check it does not stop at the first IDAT: text chunks
// are legal after the pixel data too, so the walk runs to IEND.
func pngHasPrivacyMetadata(data []byte) bool {
	const signature = "\x89PNG\r\n\x1a\n"
	if len(data) < len(signature) || string(data[:len(signature)]) != signature {
		return false
	}

	for pos := len(signature); pos+8 <= len(data); {
		length := int(binary.BigEndian.Uint32(data[pos:]))
		switch chunkType := string(data[pos+4 : pos+8]); {
		case pngMetadataChunks[chunkType]:
			return true
		case chunkType == "IEND":
			return false
		}
		// Advance past length(4) + type(4) + data(length) + crc(4).
		pos += 12 + length
	}
	return false
}

// webpHasPrivacyMetadata reports whether an extended (VP8X) WebP carries an EXIF
// or XMP chunk. The XMP FourCC is trailing-space padded, as the container
// requires all four bytes.
func webpHasPrivacyMetadata(data []byte) bool {
	// RIFF container: "RIFF" <fileSize:4> "WEBP", then FourCC-tagged chunks.
	if len(data) < 12 || string(data[:4]) != "RIFF" || string(data[8:12]) != "WEBP" {
		return false
	}

	for pos := 12; pos+8 <= len(data); {
		switch string(data[pos : pos+4]) {
		case "EXIF", "XMP ":
			return true
		}
		size := int(binary.LittleEndian.Uint32(data[pos+4:]))
		// Chunk payloads are padded to an even length.
		pos += 8 + size + (size & 1)
	}
	return false
}

// webpIsAnimated reports whether a WebP is animated, i.e. an extended (VP8X) file
// carrying ANIM/ANMF chunks. The decoder in use cannot decode animated WebP and
// already rejects it, but this makes the policy independent of that limitation.
func webpIsAnimated(data []byte) bool {
	// RIFF container: "RIFF" <fileSize:4> "WEBP", then FourCC-tagged chunks.
	if len(data) < 12 || string(data[:4]) != "RIFF" || string(data[8:12]) != "WEBP" {
		return false
	}

	for pos := 12; pos+8 <= len(data); {
		switch string(data[pos : pos+4]) {
		case "ANIM", "ANMF":
			return true
		}
		size := int(binary.LittleEndian.Uint32(data[pos+4:]))
		// Chunk payloads are padded to an even length.
		pos += 8 + size + (size & 1)
	}
	return false
}

// hasAlpha reports whether img carries a non-opaque alpha channel. The standard
// library image types implement Opaque(), which scans for any non-opaque pixel;
// a type that does not expose it is treated as potentially transparent, so its
// renditions are encoded losslessly (see imageEncodingFor) rather than risk
// flattening real alpha.
func hasAlpha(img image.Image) bool {
	if o, ok := img.(interface{ Opaque() bool }); ok {
		return !o.Opaque()
	}
	return true
}
