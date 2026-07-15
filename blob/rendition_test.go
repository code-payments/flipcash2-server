package blob

import (
	"bytes"
	"image"
	"image/color"
	"image/jpeg"
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
	jpeg80 := imageEncoding{mimeType: "image/jpeg", jpegQuality: 80}

	base := imageRenditionID(parent, RenditionDisplay, 1600, 900, jpeg80)
	require.Len(t, base.Value, 16)

	// Deterministic: the same spec always yields the same id.
	require.Equal(t, base.Value, imageRenditionID(parent, RenditionDisplay, 1600, 900, jpeg80).Value)

	// Every fingerprint field participates, so changing any one yields a new id.
	other := MustGenerateID()
	for name, got := range map[string]*blobpb.BlobId{
		"parent":       imageRenditionID(other, RenditionDisplay, 1600, 900, jpeg80),
		"role":         imageRenditionID(parent, RenditionThumbnail, 1600, 900, jpeg80),
		"width":        imageRenditionID(parent, RenditionDisplay, 1601, 900, jpeg80),
		"height":       imageRenditionID(parent, RenditionDisplay, 1600, 901, jpeg80),
		"format":       imageRenditionID(parent, RenditionDisplay, 1600, 900, imageEncoding{mimeType: "image/png"}),
		"jpeg quality": imageRenditionID(parent, RenditionDisplay, 1600, 900, imageEncoding{mimeType: "image/jpeg", jpegQuality: 81}),
	} {
		require.NotEqual(t, base.Value, got.Value, "id should change when %s changes", name)
	}
}

func TestImageRenditionIDPNGIgnoresQuality(t *testing.T) {
	// Quality is a JPEG knob; PNG is lossless, so its bytes — and thus its id — do
	// not depend on a quality value. A JPEG-quality retune must never churn PNG ids.
	parent := MustGenerateID()
	a := imageRenditionID(parent, RenditionThumbnail, 320, 240, imageEncoding{mimeType: "image/png", jpegQuality: 75})
	b := imageRenditionID(parent, RenditionThumbnail, 320, 240, imageEncoding{mimeType: "image/png", jpegQuality: 80})
	require.Equal(t, a.Value, b.Value)
}

func TestImageRenditionStorageKey(t *testing.T) {
	parent := MustGenerateID()

	key, err := imageRenditionStorageKey(parent, RenditionDisplay, 1600, 900, "image/jpeg")
	require.NoError(t, err)
	require.Equal(t, "images/"+IDString(parent)+"/display_1600x900.jpg", key)

	key, err = imageRenditionStorageKey(parent, RenditionThumbnail, 160, 90, "image/png")
	require.NoError(t, err)
	require.Equal(t, "images/"+IDString(parent)+"/thumbnail_160x90.png", key)

	// An ORIGINAL is not a derived rendition, so it has no rendition slug.
	_, err = imageRenditionStorageKey(parent, RenditionOriginal, 100, 100, "image/jpeg")
	require.Error(t, err)

	// A non-image mime type has no extension in the image scheme.
	_, err = imageRenditionStorageKey(parent, RenditionDisplay, 100, 100, "video/mp4")
	require.Error(t, err)
}

func TestImageEncodingFor(t *testing.T) {
	// A transparent source is PNG (lossless, no quality knob).
	png := imageEncodingFor(RenditionDisplay, true)
	require.Equal(t, "image/png", png.mimeType)
	require.Zero(t, png.jpegQuality)

	// An opaque source is JPEG, with quality tuned per role.
	thumb := imageEncodingFor(RenditionThumbnail, false)
	require.Equal(t, "image/jpeg", thumb.mimeType)
	require.Equal(t, 75, thumb.jpegQuality)

	display := imageEncodingFor(RenditionDisplay, false)
	require.Equal(t, "image/jpeg", display.mimeType)
	require.Equal(t, 80, display.jpegQuality)
}

func TestImageRenditionSpecsLadder(t *testing.T) {
	// The ladder is ordered small to large so hydration emits it in that order.
	require.Equal(t, []imageRenditionSpec{
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
	t.Run("jpeg encodes to decodable bytes of the given size", func(t *testing.T) {
		encoded, err := imageEncoding{mimeType: "image/jpeg", jpegQuality: 80}.encode(randomImage(64, 48))
		require.NoError(t, err)

		decoded, format, err := image.Decode(bytes.NewReader(encoded))
		require.NoError(t, err)
		require.Equal(t, "jpeg", format)
		require.Equal(t, 64, decoded.Bounds().Dx())
		require.Equal(t, 48, decoded.Bounds().Dy())
	})

	t.Run("png preserves alpha", func(t *testing.T) {
		// A half-transparent source, so a format that dropped alpha would be visible.
		src := image.NewRGBA(image.Rect(0, 0, 8, 8))
		for y := range 8 {
			for x := range 8 {
				src.Set(x, y, color.RGBA{R: 200, G: 100, B: 50, A: 64})
			}
		}
		encoded, err := imageEncoding{mimeType: "image/png"}.encode(src)
		require.NoError(t, err)

		decoded, format, err := image.Decode(bytes.NewReader(encoded))
		require.NoError(t, err)
		require.Equal(t, "png", format)
		_, _, _, a := decoded.At(0, 0).RGBA()
		require.Less(t, a, uint32(0xffff), "alpha should be preserved, not flattened")
	})

	t.Run("unsupported mime type errors", func(t *testing.T) {
		_, err := imageEncoding{mimeType: "image/gif"}.encode(randomImage(4, 4))
		require.Error(t, err)
	})
}

func TestImageEncodingJPEGIsOpaque(t *testing.T) {
	// JPEG has no alpha channel; a transparent source flattens to opaque.
	src := image.NewRGBA(image.Rect(0, 0, 4, 4))
	src.Set(0, 0, color.RGBA{R: 10, G: 20, B: 30, A: 0})

	encoded, err := imageEncoding{mimeType: "image/jpeg", jpegQuality: 80}.encode(src)
	require.NoError(t, err)

	decoded, err := jpeg.Decode(bytes.NewReader(encoded))
	require.NoError(t, err)
	_, _, _, a := decoded.At(0, 0).RGBA()
	require.Equal(t, uint32(0xffff), a)
}

func TestRenditionTypeToProtoRole(t *testing.T) {
	require.Equal(t, blobpb.Rendition_ORIGINAL, RenditionOriginal.ToProtoRole())
	require.Equal(t, blobpb.Rendition_DISPLAY, RenditionDisplay.ToProtoRole())
	require.Equal(t, blobpb.Rendition_THUMBNAIL, RenditionThumbnail.ToProtoRole())
	require.Equal(t, blobpb.Rendition_UNKNOWN, RenditionUnknown.ToProtoRole())
}
