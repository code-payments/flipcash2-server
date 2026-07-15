package blob

import (
	"bytes"
	"encoding/binary"
	"hash/crc32"
	"image"
	"image/color"
	"image/png"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestModerationPayload_SmallImagePassesThrough(t *testing.T) {
	// Bytes already within the budget are sent verbatim — no re-encode.
	data := make([]byte, moderationMaxBytes/2)
	payload, err := moderationPayload(data, randomImage(16, 16))
	require.NoError(t, err)
	require.Equal(t, len(data), len(payload))
	require.Equal(t, &data[0], &payload[0]) // same backing array: not copied/re-encoded
}

func TestModerationPayload_LargeImageDownscaledUnderBudget(t *testing.T) {
	// A high-entropy image that does not fit the budget at full size is downscaled
	// and re-encoded as a JPEG that does.
	img := randomImage(2200, 2200)
	oversized := make([]byte, moderationMaxBytes+1) // forces the downscale path

	payload, err := moderationPayload(oversized, img)
	require.NoError(t, err)
	require.LessOrEqual(t, len(payload), moderationMaxBytes)

	// The result is a valid JPEG whose longest side is within the cap.
	cfg, format, err := image.DecodeConfig(bytes.NewReader(payload))
	require.NoError(t, err)
	require.Equal(t, "jpeg", format)
	require.Positive(t, cfg.Width)
	require.LessOrEqual(t, max(cfg.Width, cfg.Height), moderationMaxDimension)
}

func TestDownscaleForBlurhash(t *testing.T) {
	dims := func(img image.Image) (int, int) {
		b := img.Bounds()
		return b.Dx(), b.Dy()
	}

	t.Run("normal aspect caps the long side", func(t *testing.T) {
		w, h := dims(downscaleForBlurhash(randomImage(2000, 1500)))
		require.Equal(t, blurhashMaxDimension, max(w, h))
		require.GreaterOrEqual(t, min(w, h), blurhashMinShortDimension)
	})

	t.Run("extreme aspect floors the short side", func(t *testing.T) {
		// A 10:1 image: capping the long side at 64 would give a 6px short side, so
		// the short side is floored instead and the long side exceeds the cap.
		w, h := dims(downscaleForBlurhash(randomImage(2000, 200)))
		require.Equal(t, blurhashMinShortDimension, min(w, h))
		require.Greater(t, max(w, h), blurhashMaxDimension)
		require.Less(t, max(w, h), 2000) // still a downscale
	})

	t.Run("already small is unchanged", func(t *testing.T) {
		src := randomImage(50, 40)
		require.Same(t, src, downscaleForBlurhash(src))
	})

	t.Run("never upscales a thin short side", func(t *testing.T) {
		// The short side is already below the floor; upscaling adds no information.
		src := randomImage(2000, 8)
		require.Same(t, src, downscaleForBlurhash(src))
	})
}

// randomImage returns an RGBA image filled with a deterministic, high-entropy
// pattern, so JPEG encodings of it are large (exercising the size-targeting loop).
func randomImage(width, height int) image.Image {
	img := image.NewRGBA(image.Rect(0, 0, width, height))
	seed := uint32(2166136261)
	for y := range height {
		for x := range width {
			// A cheap xorshift-ish hash per pixel — no global RNG needed.
			seed ^= uint32(x*73856093) ^ uint32(y*19349663)
			seed *= 2654435761
			img.Set(x, y, color.RGBA{
				R: uint8(seed),
				G: uint8(seed >> 8),
				B: uint8(seed >> 16),
				A: 255,
			})
		}
	}
	return img
}

func TestIsImageAnimated(t *testing.T) {
	tests := []struct {
		name   string
		format string
		data   []byte
		want   bool
	}{
		{"static png", "png", encodePNG(t), false},
		{"apng", "png", encodeAPNG(t), true},
		{"static webp", "webp", webpRIFF("VP8 ", 16), false},
		{"animated webp", "webp", webpRIFF("ANIM", 6), true},
		{"jpeg is never animated", "jpeg", nil, false},
		{"unknown format is not inspected", "tiff", []byte("II*\x00unrecognized"), false},
		{"empty", "png", nil, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, isImageAnimated(tc.format, tc.data))
		})
	}
}

func TestInspectImageAcceptsExtremeAspectRatio(t *testing.T) {
	// A wide, panorama-style image: comfortably under the pixel cap but far past
	// any per-axis ceiling tied to a normal photo's dimensions. It must be judged
	// on area, not shape, so it is accepted.
	var buf bytes.Buffer
	require.NoError(t, png.Encode(&buf, image.NewRGBA(image.Rect(0, 0, 13000, 100))))

	inspection, err := InspectImage(buf.Bytes())
	require.NoError(t, err)
	require.EqualValues(t, 13000, inspection.Metadata.Width)
	require.EqualValues(t, 100, inspection.Metadata.Height)
}

func TestInspectImageRejectsOversizedDimensions(t *testing.T) {
	// A 1px-tall strip is cheap in total pixels (so it clears the pixel-count cap)
	// but exceeds the per-axis format ceiling, isolating the dimension check.
	var buf bytes.Buffer
	require.NoError(t, png.Encode(&buf, image.NewRGBA(image.Rect(0, 0, maxImageDimension+1, 1))))

	_, err := InspectImage(buf.Bytes())
	require.ErrorContains(t, err, "exceed")
}

func TestInspectImageRejectsAnimated(t *testing.T) {
	t.Run("apng is rejected", func(t *testing.T) {
		_, err := InspectImage(encodeAPNG(t))
		require.ErrorContains(t, err, "animated")
	})
}

func TestHasPrivacyMetadata(t *testing.T) {
	tests := []struct {
		name   string
		format string
		data   []byte
		want   bool
	}{
		// JPEG: an allowlist of APPn segments, so anything unrecognized is metadata.
		{"clean jpeg", "jpeg", encodeTestJPEG(t), false},
		{"jpeg with exif (APP1)", "jpeg", jpegWithSegment(t, 0xE1, []byte("Exif\x00\x00gps goes here")), true},
		{"jpeg with xmp (APP1)", "jpeg", jpegWithSegment(t, 0xE1, []byte("http://ns.adobe.com/xap/1.0/\x00")), true},
		{"jpeg with iptc (APP13)", "jpeg", jpegWithSegment(t, 0xED, []byte("Photoshop 3.0\x00")), true},
		{"jpeg with maker note (APP4)", "jpeg", jpegWithSegment(t, 0xE4, []byte("vendor")), true},
		{"jpeg with comment (COM)", "jpeg", jpegWithSegment(t, 0xFE, []byte("shot at home")), true},
		{"jpeg keeps jfif (APP0)", "jpeg", jpegWithSegment(t, 0xE0, []byte("JFIF\x00")), false},
		{"jpeg keeps icc (APP2)", "jpeg", jpegWithSegment(t, 0xE2, []byte("ICC_PROFILE\x00")), false},
		{"jpeg keeps adobe (APP14)", "jpeg", jpegWithSegment(t, 0xEE, []byte("Adobe")), false},

		// PNG: a blocklist — the EXIF and free-text chunks, but not color management.
		{"clean png", "png", encodePNG(t), false},
		{"png with exif", "png", pngWithChunk(t, "eXIf", []byte("gps goes here")), true},
		{"png with text", "png", pngWithChunk(t, "tEXt", []byte("Comment\x00shot at home")), true},
		{"png with international text", "png", pngWithChunk(t, "iTXt", []byte("XML:com.adobe.xmp\x00")), true},
		{"png with compressed text", "png", pngWithChunk(t, "zTXt", []byte("Comment\x00")), true},
		{"png keeps icc profile", "png", pngWithChunk(t, "iCCP", []byte("sRGB\x00")), false},

		{"clean webp", "webp", webpRIFF("VP8 ", 16), false},
		{"webp with exif", "webp", webpRIFF("EXIF", 8), true},
		{"webp with xmp", "webp", webpRIFF("XMP ", 8), true},
		{"webp keeps icc profile", "webp", webpRIFF("ICCP", 8), false},

		{"unknown format is not inspected", "tiff", []byte("II*\x00"), false},
		{"truncated jpeg", "jpeg", []byte{0xFF, 0xD8, 0xFF}, false},
		{"empty", "jpeg", nil, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, hasPrivacyMetadata(tc.format, tc.data))
		})
	}
}

func TestGifIsUnsupported(t *testing.T) {
	// GIF support was removed: it is not an accepted upload type, and its decoder is
	// not registered, so bytes that sneak past the declared-type gate do not decode
	// and are rejected as undecodable rather than served.
	require.False(t, SupportedImageMimeTypes["image/gif"])

	rawGIF := []byte("GIF89a\x10\x00\x0c\x00\x00\x00\x00\x3b")
	_, err := InspectImage(rawGIF)
	require.ErrorIs(t, err, ErrImageCorrupt)
}

func TestInspectImageRejectsPrivacyMetadata(t *testing.T) {
	// The uploaded bytes are served verbatim, so an image that kept its metadata is
	// rejected outright rather than rewritten.
	t.Run("jpeg carrying exif is rejected", func(t *testing.T) {
		_, err := InspectImage(jpegWithSegment(t, 0xE1, []byte("Exif\x00\x00gps goes here")))
		require.ErrorIs(t, err, ErrImagePrivacyMetadata)
	})

	t.Run("png carrying exif is rejected", func(t *testing.T) {
		_, err := InspectImage(pngWithChunk(t, "eXIf", []byte("gps goes here")))
		require.ErrorIs(t, err, ErrImagePrivacyMetadata)
	})

	t.Run("stripped jpeg is accepted", func(t *testing.T) {
		inspection, err := InspectImage(encodeTestJPEG(t))
		require.NoError(t, err)
		require.Equal(t, "image/jpeg", inspection.MimeType)
	})

	t.Run("color profile is not privacy metadata", func(t *testing.T) {
		// An ICC profile carries nothing personal, and dropping it would shift the
		// colors of a wide-gamut photo — so it is kept, not treated as a violation.
		inspection, err := InspectImage(jpegWithSegment(t, 0xE2, []byte("ICC_PROFILE\x00")))
		require.NoError(t, err)
		require.Equal(t, "image/jpeg", inspection.MimeType)
	})
}

func TestInspectImageDerivesAlpha(t *testing.T) {
	// The alpha bit drives a rendition's output format, so it must be derived
	// correctly from the source bytes.
	t.Run("opaque png has no alpha", func(t *testing.T) {
		var buf bytes.Buffer
		require.NoError(t, png.Encode(&buf, randomImage(16, 12))) // randomImage is fully opaque
		inspection, err := InspectImage(buf.Bytes())
		require.NoError(t, err)
		require.False(t, inspection.Metadata.HasAlpha)
	})

	t.Run("transparent png has alpha", func(t *testing.T) {
		inspection, err := InspectImage(encodeTransparentPNG(t))
		require.NoError(t, err)
		require.True(t, inspection.Metadata.HasAlpha)
	})

	t.Run("jpeg never has alpha", func(t *testing.T) {
		inspection, err := InspectImage(encodeTestJPEG(t))
		require.NoError(t, err)
		require.False(t, inspection.Metadata.HasAlpha)
	})
}

// encodeTransparentPNG returns a PNG with a non-opaque pixel, so InspectImage
// derives HasAlpha=true.
func encodeTransparentPNG(t *testing.T) []byte {
	t.Helper()
	img := image.NewRGBA(image.Rect(0, 0, 8, 8))
	for y := range 8 {
		for x := range 8 {
			img.Set(x, y, color.RGBA{R: 200, G: 100, B: 50, A: 64})
		}
	}
	var buf bytes.Buffer
	require.NoError(t, png.Encode(&buf, img))
	return buf.Bytes()
}

func encodeTestJPEG(t *testing.T) []byte {
	t.Helper()
	data, err := encodeJPEG(randomImage(16, 12), 85)
	require.NoError(t, err)
	return data
}

// jpegWithSegment splices a marker segment carrying the given payload in just
// after the SOI of an otherwise clean JPEG.
func jpegWithSegment(t *testing.T, marker byte, payload []byte) []byte {
	t.Helper()
	base := encodeTestJPEG(t)

	segment := []byte{0xFF, marker}
	segment = binary.BigEndian.AppendUint16(segment, uint16(len(payload)+2)) // the length counts itself
	segment = append(segment, payload...)

	out := make([]byte, 0, len(base)+len(segment))
	out = append(out, base[:2]...) // SOI
	out = append(out, segment...)
	return append(out, base[2:]...)
}

// pngWithChunk splices a chunk of the given type in just after the IHDR of an
// otherwise clean PNG. The CRC is computed properly, so the result still decodes.
func pngWithChunk(t *testing.T, chunkType string, payload []byte) []byte {
	t.Helper()
	base := encodePNG(t)
	const afterIHDR = 8 + 12 + 13 // signature + IHDR (length+type+crc) + IHDR data

	var chunk bytes.Buffer
	require.NoError(t, binary.Write(&chunk, binary.BigEndian, uint32(len(payload))))
	chunk.WriteString(chunkType)
	chunk.Write(payload)
	crc := crc32.ChecksumIEEE(append([]byte(chunkType), payload...))
	require.NoError(t, binary.Write(&chunk, binary.BigEndian, crc))

	out := make([]byte, 0, len(base)+chunk.Len())
	out = append(out, base[:afterIHDR]...)
	out = append(out, chunk.Bytes()...)
	return append(out, base[afterIHDR:]...)
}

func encodePNG(t *testing.T) []byte {
	t.Helper()
	var buf bytes.Buffer
	require.NoError(t, png.Encode(&buf, image.NewRGBA(image.Rect(0, 0, 16, 12))))
	return buf.Bytes()
}

// encodeAPNG injects a synthetic acTL chunk just after the IHDR of a still PNG,
// making it an animated PNG as far as the chunk structure is concerned. The CRC
// is left bogus — the detector walks chunk headers and does not validate it.
func encodeAPNG(t *testing.T) []byte {
	t.Helper()
	base := encodePNG(t)
	const afterIHDR = 8 + 12 + 13 // signature + IHDR (length+type+crc) + IHDR data

	var actl bytes.Buffer
	require.NoError(t, binary.Write(&actl, binary.BigEndian, uint32(8))) // data length
	actl.WriteString("acTL")
	require.NoError(t, binary.Write(&actl, binary.BigEndian, uint32(2))) // num_frames
	require.NoError(t, binary.Write(&actl, binary.BigEndian, uint32(0))) // num_plays
	require.NoError(t, binary.Write(&actl, binary.BigEndian, uint32(0))) // crc

	out := make([]byte, 0, len(base)+actl.Len())
	out = append(out, base[:afterIHDR]...)
	out = append(out, actl.Bytes()...)
	out = append(out, base[afterIHDR:]...)
	return out
}

// webpRIFF builds a minimal RIFF/WEBP container whose first chunk carries the
// given FourCC, enough to exercise the animated-WebP structural check.
func webpRIFF(fourCC string, payload int) []byte {
	var buf bytes.Buffer
	buf.WriteString("RIFF")
	_ = binary.Write(&buf, binary.LittleEndian, uint32(0)) // file size, unchecked
	buf.WriteString("WEBP")
	buf.WriteString(fourCC)
	_ = binary.Write(&buf, binary.LittleEndian, uint32(payload))
	buf.Write(make([]byte, payload+(payload&1)))
	return buf.Bytes()
}
