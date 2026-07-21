package blob

import (
	"bytes"
	"image"
	"image/color"
	"image/draw"
	"testing"

	"github.com/stretchr/testify/require"

	blobpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/blob/v1"
)

func TestScaledDimensions(t *testing.T) {
	tests := []struct {
		name                   string
		w, h, max              uint32
		wantW, wantH           uint32
	}{
		{"already within bound is unchanged", 800, 600, 1600, 800, 600},
		{"bound equal to longest side is unchanged", 1600, 900, 1600, 1600, 900},
		{"landscape scales by the width", 3200, 1800, 1600, 1600, 900},
		{"portrait scales by the height", 1000, 2000, 800, 400, 800},
		{"square", 2000, 2000, 320, 320, 320},
		{"extreme aspect ratio floors the short side at 1", 8000, 100, 160, 160, 2},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			w, h := scaledDimensions(tc.w, tc.h, tc.max)
			require.Equal(t, tc.wantW, w)
			require.Equal(t, tc.wantH, h)
			// It never upscales.
			require.LessOrEqual(t, w, tc.w)
			require.LessOrEqual(t, h, tc.h)
		})
	}
}

func TestImageRenditionID(t *testing.T) {
	parent := MustGenerateID()
	lossy80 := imageEncoding{mimeType: "image/webp", quality: 80}

	base := imageRenditionID(parent, RenditionDisplay, 1600, 900, lossy80)
	require.Len(t, base.Value, 16)

	// Deterministic: the same spec always yields the same id.
	require.Equal(t, base.Value, imageRenditionID(parent, RenditionDisplay, 1600, 900, lossy80).Value)

	// Every fingerprint field participates, so changing any one yields a new id.
	other := MustGenerateID()
	for name, got := range map[string]*blobpb.BlobId{
		"parent":   imageRenditionID(other, RenditionDisplay, 1600, 900, lossy80),
		"role":     imageRenditionID(parent, RenditionThumbnail, 1600, 900, lossy80),
		"width":    imageRenditionID(parent, RenditionDisplay, 1601, 900, lossy80),
		"height":   imageRenditionID(parent, RenditionDisplay, 1600, 901, lossy80),
		"lossless": imageRenditionID(parent, RenditionDisplay, 1600, 900, imageEncoding{mimeType: "image/webp", lossless: true}),
		"quality":  imageRenditionID(parent, RenditionDisplay, 1600, 900, imageEncoding{mimeType: "image/webp", quality: 81}),
	} {
		require.NotEqual(t, base.Value, got.Value, "id should change when %s changes", name)
	}
}

func TestImageRenditionIDLosslessIgnoresQuality(t *testing.T) {
	// Quality is a lossy knob; lossless WebP's bytes — and thus its id — do not
	// depend on a quality value. A quality retune must never churn lossless ids.
	parent := MustGenerateID()
	a := imageRenditionID(parent, RenditionThumbnail, 320, 240, imageEncoding{mimeType: "image/webp", lossless: true, quality: 75})
	b := imageRenditionID(parent, RenditionThumbnail, 320, 240, imageEncoding{mimeType: "image/webp", lossless: true, quality: 80})
	require.Equal(t, a.Value, b.Value)
}

func TestImageRenditionStorageKey(t *testing.T) {
	parent := MustGenerateID()

	key, err := imageRenditionStorageKey(parent, RenditionDisplay, 1600, 900, "image/webp")
	require.NoError(t, err)
	require.Equal(t, "images/"+IDString(parent)+"/display_1600x900.webp", key)

	key, err = imageRenditionStorageKey(parent, RenditionThumbnail, 160, 90, "image/webp")
	require.NoError(t, err)
	require.Equal(t, "images/"+IDString(parent)+"/thumbnail_160x90.webp", key)

	// An ORIGINAL is not a derived rendition, so it has no rendition slug.
	_, err = imageRenditionStorageKey(parent, RenditionOriginal, 100, 100, "image/webp")
	require.Error(t, err)

	// A non-image mime type has no extension in the image scheme.
	_, err = imageRenditionStorageKey(parent, RenditionDisplay, 100, 100, "video/mp4")
	require.Error(t, err)
}

func TestImageEncodingFor(t *testing.T) {
	// A transparent source is lossless WebP (no quality knob).
	lossless := imageEncodingFor(RenditionDisplay, true)
	require.Equal(t, "image/webp", lossless.mimeType)
	require.True(t, lossless.lossless)
	require.Zero(t, lossless.quality)

	// An opaque source is lossy WebP, with quality tuned per role.
	thumb := imageEncodingFor(RenditionThumbnail, false)
	require.Equal(t, "image/webp", thumb.mimeType)
	require.False(t, thumb.lossless)
	require.Equal(t, 75, thumb.quality)

	display := imageEncodingFor(RenditionDisplay, false)
	require.Equal(t, "image/webp", display.mimeType)
	require.False(t, display.lossless)
	require.Equal(t, 80, display.quality)
}

func TestImageRenditionSpecsLadder(t *testing.T) {
	// The ladder is ordered small to large so hydration emits it in that order.
	require.Equal(t, []imageRenditionSpec{
		{Rendition: RenditionThumbnail, MaxLongestSide: 32},
		{Rendition: RenditionThumbnail, MaxLongestSide: 160},
		{Rendition: RenditionThumbnail, MaxLongestSide: 320},
		{Rendition: RenditionDisplay, MaxLongestSide: 800},
		{Rendition: RenditionDisplay, MaxLongestSide: 1600},
	}, imageRenditionSpecs)

	var prev uint32
	for _, spec := range imageRenditionSpecs {
		require.Greater(t, spec.MaxLongestSide, prev, "ladder must increase")
		prev = spec.MaxLongestSide
	}
}

func TestResampleImage(t *testing.T) {
	src := randomImage(100, 50)
	dst := resampleImage(src, 40, 20)
	require.Equal(t, 40, dst.Bounds().Dx())
	require.Equal(t, 20, dst.Bounds().Dy())
}

func TestImageEncodingEncode(t *testing.T) {
	t.Run("lossy webp encodes to decodable bytes of the given size", func(t *testing.T) {
		encoded, err := imageEncoding{mimeType: "image/webp", quality: 80}.encode(randomImage(64, 48))
		require.NoError(t, err)

		decoded, format, err := image.Decode(bytes.NewReader(encoded))
		require.NoError(t, err)
		require.Equal(t, "webp", format)
		require.Equal(t, 64, decoded.Bounds().Dx())
		require.Equal(t, 48, decoded.Bounds().Dy())
	})

	t.Run("lossless webp preserves alpha and semi-transparent color", func(t *testing.T) {
		// A quarter-opaque source whose STRAIGHT color must survive intact. This is
		// built the way the ladder feeds the encoder: a straight-alpha NRGBA composited
		// onto a premultiplied *image.RGBA canvas (exactly what resampleImage yields).
		// The WebP encoder reads *image.RGBA as straight alpha, so unless encode()
		// converts to straight-alpha NRGBA first, the premultiplied pixels are handed
		// over verbatim and every semi-transparent color comes back badly darkened.
		straight := color.NRGBA{R: 200, G: 40, B: 30, A: 64}
		nsrc := image.NewNRGBA(image.Rect(0, 0, 8, 8))
		for y := range 8 {
			for x := range 8 {
				nsrc.SetNRGBA(x, y, straight)
			}
		}
		src := image.NewRGBA(nsrc.Bounds())
		draw.Draw(src, src.Bounds(), nsrc, nsrc.Bounds().Min, draw.Src)

		encoded, err := imageEncoding{mimeType: "image/webp", lossless: true}.encode(src)
		require.NoError(t, err)

		decoded, format, err := image.Decode(bytes.NewReader(encoded))
		require.NoError(t, err)
		require.Equal(t, "webp", format)

		got := color.NRGBAModel.Convert(decoded.At(0, 0)).(color.NRGBA)
		require.InDelta(t, straight.A, got.A, 1, "alpha should be preserved, not flattened")
		// A premultiplied-as-straight bug darkens R from 200 to ~60; a small delta
		// absorbs only the rounding of the premultiplied RGBA intermediate.
		require.InDelta(t, straight.R, got.R, 16, "straight red must survive, not darken")
		require.InDelta(t, straight.G, got.G, 16, "straight green must survive")
		require.InDelta(t, straight.B, got.B, 16, "straight blue must survive")
	})

	t.Run("unsupported mime type errors", func(t *testing.T) {
		_, err := imageEncoding{mimeType: "image/gif"}.encode(randomImage(4, 4))
		require.Error(t, err)
	})
}

func TestRenditionTypeToProtoRole(t *testing.T) {
	require.Equal(t, blobpb.Rendition_ORIGINAL, RenditionOriginal.ToProtoRole())
	require.Equal(t, blobpb.Rendition_DISPLAY, RenditionDisplay.ToProtoRole())
	require.Equal(t, blobpb.Rendition_THUMBNAIL, RenditionThumbnail.ToProtoRole())
	require.Equal(t, blobpb.Rendition_UNKNOWN, RenditionUnknown.ToProtoRole())
}
