package blob

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"image"
	"image/jpeg"
	"math"

	// Register the standard image decoders so image.Decode recognizes them.
	_ "image/gif"
	_ "image/png"

	"github.com/buckket/go-blurhash"

	"golang.org/x/image/draw"

	// Register the WebP decoder.
	_ "golang.org/x/image/webp"
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
	"gif":  "image/gif",
	"webp": "image/webp",
}

// mimeTypeToExtension maps a supported MIME type to its canonical file extension
// (with leading dot), used to give stored objects and signed URLs a meaningful
// extension.
var mimeTypeToExtension = map[string]string{
	"image/jpeg": ".jpg",
	"image/png":  ".png",
	"image/gif":  ".gif",
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
// MIME type, pixel dimensions, and BlurHash. It returns an error if the bytes
// are not a decodable image of a supported format, or if the image's pixel count
// exceeds maxImagePixels; callers treat either as a rejection.
func InspectImage(data []byte) (*ImageInspection, error) {
	// Read only the header first to bound the pixel count before decoding the
	// full image into memory. int64 math avoids overflow on a hostile header that
	// declares enormous dimensions.
	config, headerFormat, err := image.DecodeConfig(bytes.NewReader(data))
	if err != nil {
		return nil, fmt.Errorf("failed to read image header: %w", err)
	}
	if int64(config.Width)*int64(config.Height) > maxImagePixels {
		return nil, fmt.Errorf("image dimensions %dx%d exceed the %d pixel limit", config.Width, config.Height, maxImagePixels)
	}
	// Reject animated images: only the first frame would be inspected and
	// moderated, but the whole animation would be served — a moderation bypass.
	// Detected from the container structure, so no extra frames are decoded.
	if isImageAnimated(headerFormat, data) {
		return nil, fmt.Errorf("animated %s images are not supported", headerFormat)
	}

	img, format, err := image.Decode(bytes.NewReader(data))
	if err != nil {
		return nil, fmt.Errorf("failed to decode image: %w", err)
	}

	mimeType, ok := imageFormatToMimeType[format]
	if !ok {
		return nil, fmt.Errorf("unsupported image format %q", format)
	}

	bounds := img.Bounds()
	width, height := bounds.Dx(), bounds.Dy()
	if width <= 0 || height <= 0 {
		return nil, fmt.Errorf("image has invalid dimensions %dx%d", width, height)
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
	case "gif":
		return gifIsAnimated(data)
	case "png":
		return pngIsAnimated(data)
	case "webp":
		return webpIsAnimated(data)
	default:
		// JPEG (and anything else that decodes) is single-frame.
		return false
	}
}

// gifIsAnimated walks a GIF's block structure counting image descriptors,
// stopping as soon as a second one is found. It decodes no pixel data.
func gifIsAnimated(data []byte) bool {
	const (
		extensionIntroducer = 0x21
		imageSeparator      = 0x2C
		trailer             = 0x3B
	)

	// Header (6 bytes) + Logical Screen Descriptor (7 bytes); the packed field at
	// offset 10 says whether a Global Color Table follows.
	if len(data) < 13 {
		return false
	}
	pos := 13
	if packed := data[10]; packed&0x80 != 0 {
		pos += colorTableBytes(packed)
	}

	images := 0
	for pos < len(data) {
		switch data[pos] {
		case imageSeparator:
			images++
			if images > 1 {
				return true
			}
			// Image Descriptor is 10 bytes; its packed field (offset +9) flags a
			// Local Color Table, then a 1-byte LZW code size precedes the image data.
			if pos+10 > len(data) {
				return false
			}
			lct := data[pos+9]
			pos += 10
			if lct&0x80 != 0 {
				pos += colorTableBytes(lct)
			}
			pos++ // LZW minimum code size
			pos = gifSkipSubBlocks(data, pos)
		case extensionIntroducer:
			pos += 2 // introducer + label
			pos = gifSkipSubBlocks(data, pos)
		case trailer:
			return false
		default:
			return false // malformed; let the decoder reject it
		}
	}
	return false
}

// colorTableBytes returns the byte size of a GIF color table from its packed
// descriptor field: 3 bytes per entry, 2^(size+1) entries.
func colorTableBytes(packed byte) int {
	return 3 * (1 << ((packed & 0x07) + 1))
}

// gifSkipSubBlocks advances past a GIF sub-block sequence — a run of
// [size][size bytes] blocks terminated by a zero-size block — returning the
// index just past the terminator (or len(data) if the stream is truncated).
func gifSkipSubBlocks(data []byte, pos int) int {
	for pos < len(data) {
		size := int(data[pos])
		pos++
		if size == 0 {
			return pos
		}
		pos += size
	}
	return pos
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
