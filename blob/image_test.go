package blob

import (
	"bytes"
	"encoding/binary"
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
