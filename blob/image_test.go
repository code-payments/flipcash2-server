package blob

import (
	"bytes"
	"encoding/binary"
	"hash/crc32"
	"image"
	"image/color"
	"image/gif"
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
		{"single-frame gif", "gif", encodeGIF(t, 1), false},
		{"two-frame gif", "gif", encodeGIF(t, 2), true},
		{"many-frame gif", "gif", encodeGIF(t, 24), true},
		{"static png", "png", encodePNG(t), false},
		{"apng", "png", encodeAPNG(t), true},
		{"static webp", "webp", webpRIFF("VP8 ", 16), false},
		{"animated webp", "webp", webpRIFF("ANIM", 6), true},
		{"jpeg is never animated", "jpeg", nil, false},
		{"unknown format is not inspected", "tiff", encodeGIF(t, 5), false},
		{"truncated gif", "gif", []byte("GIF89a\x00\x00"), false},
		{"empty", "gif", nil, false},
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
	t.Run("static gif is accepted", func(t *testing.T) {
		inspection, err := InspectImage(encodeGIF(t, 1))
		require.NoError(t, err)
		require.Equal(t, "image/gif", inspection.MimeType)
	})

	t.Run("animated gif is rejected", func(t *testing.T) {
		_, err := InspectImage(encodeGIF(t, 3))
		require.ErrorContains(t, err, "animated")
	})

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

		{"clean gif", "gif", encodeGIF(t, 1), false},
		{"gif with comment", "gif", gifWithExtension(0xFE, []byte("shot at home")), true},
		{"gif with xmp", "gif", gifWithExtension(0xFF, []byte("XMP DataXMP")), true},
		{"gif keeps netscape loop extension", "gif", gifWithExtension(0xFF, []byte("NETSCAPE2.0")), false},

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

// gifWithExtension builds a minimal GIF carrying a single extension block with
// the given label, its payload written as one sub-block. An application extension
// (0xFF) identifies itself in that first sub-block, which is how XMP rides in.
func gifWithExtension(label byte, payload []byte) []byte {
	var buf bytes.Buffer
	buf.WriteString("GIF89a")
	buf.Write([]byte{16, 0, 12, 0, 0x00, 0, 0}) // logical screen descriptor; no global color table

	buf.Write([]byte{0x21, label})
	buf.WriteByte(byte(len(payload)))
	buf.Write(payload)
	buf.WriteByte(0x00) // sub-block terminator

	buf.WriteByte(0x3B) // trailer
	return buf.Bytes()
}

// encodeGIF builds a GIF with the given number of frames; one frame yields a
// still GIF, more than one an animated GIF.
func encodeGIF(t *testing.T, frames int) []byte {
	t.Helper()
	palette := color.Palette{color.Black, color.White, color.RGBA{R: 255, A: 255}}
	g := &gif.GIF{}
	for i := range frames {
		frame := image.NewPaletted(image.Rect(0, 0, 16, 12), palette)
		fill := uint8(i % len(palette))
		for p := range frame.Pix {
			frame.Pix[p] = fill
		}
		g.Image = append(g.Image, frame)
		g.Delay = append(g.Delay, 10)
	}
	var buf bytes.Buffer
	require.NoError(t, gif.EncodeAll(&buf, g))
	return buf.Bytes()
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
