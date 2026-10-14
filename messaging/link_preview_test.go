package messaging

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"

	blobpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/blob/v1"
	messagingpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/messaging/v1"
)

func TestLinkInText(t *testing.T) {
	const link = "https://a.example/path?q=1"
	for _, tc := range []struct {
		name string
		text string
		want bool
	}{
		{"whole text", link, true},
		{"at start", link + " is worth a look", true},
		{"at end", "look at " + link, true},
		{"in the middle", "look at " + link + " now", true},
		{"after a newline", "look:\n" + link, true},
		{"sentence stop", "look at " + link + ".", true},
		{"several trailers", "(look at " + link + ")!?", true},
		{"quoted", `"` + link + `"`, true},
		{"bracketed", "<" + link + ">", true},
		{"twice, once whole", link + "x and " + link, true},
		{"absent", "nothing here", false},
		{"empty text", "", false},
		{"prefix of a longer host", link[:len("https://a.example")] + ".evil.example/", false},
		{"prefix of a longer path", link + "/more", false},
		{"prefix with a trailer inside", "see " + link + ".evil", false},
		{"embedded in a query", "https://evil.example/?u=" + link, false},
		{"embedded behind a bracket", "https://evil.example/(" + link + ")", false},
		{"embedded behind a square bracket", "https://evil.example/[" + link + "]", false},
		{"embedded behind a brace", "https://evil.example/{" + link + "}", false},
		{"embedded behind a quote", "https://evil.example/'" + link + "'", false},
		{"embedded behind a double quote", `https://evil.example/?q="` + link + `"`, false},
		{"bracket run into a word", "see x(" + link + ")", false},
		{"bracketed in a sentence", "see (" + link + ").", true},
		{"several openers", `(("` + link + `"))`, true},
		{"openers after a newline", "look:\n(" + link + ")", true},
		{"run into a word", "x" + link, false},
		{"followed by a word", link + "x", false},
		{"different case", "look at HTTPS://A.EXAMPLE/path?q=1", false},
		{"percent form", "look at https://a.example/path%3Fq=1", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, linkInText(tc.text, link))
		})
	}

	// A link spelled with a trailer of its own, like a closing bracket, is
	// found on its own spelling, as is the shorter link it contains when
	// that is what the preview names.
	const bracketed = "https://en.example/wiki/Foo_(bar)"
	assert.True(t, linkInText("read "+bracketed, bracketed))
	assert.True(t, linkInText("read "+bracketed+".", bracketed))
	assert.False(t, linkInText("read "+bracketed, "https://en.example/wiki/Foo_"))

	assert.False(t, linkInText("anything", ""))
}

func TestValidLinkURL(t *testing.T) {
	for _, tc := range []struct {
		link string
		want bool
	}{
		{"https://a.example", true},
		{"http://a.example/", true},
		{"https://a.example:8443/p?q=1#f", true},
		{"https://xn--bcher-kva.example/", true},
		{"HTTPS://A.EXAMPLE/", true},
		{"", false},
		{"a.example", false},
		{"//a.example", false},
		{"ftp://a.example/", false},
		{"javascript:alert(1)", false},
		{"mailto:a@a.example", false},
		{"https://", false},
		{"https:///path", false},
		{"https://bank.example@evil.example/", false},
		{"https://user:pw@a.example/", false},
		{"https://127.0.0.1/", false},
		{"https://[::1]/", false},
		{"https://bücher.example/", false},
		{"https://a.example/a b", false},
		{"https://a.example/\tb", false},
		{"https://a.example/\n", false},
		{"https://a.exa mple/", false},

		// The host is a plain domain name.
		{"https://a.example./", true},
		{"https://localhost/", true},
		{"https://a-b.example/", true},
		{"https://1.example/", true},
		{"https://a.0xg/", true},
		{"https://a.example;x/", false},
		{`https://a."b".example/`, false},
		{"https://a!b$c.example/", false},
		{"https://a_b.example/", false},
		{"https://a..example/", false},
		{"https://.a.example/", false},
		{"https://a.example../", false},
		{"https://b%C3%BCcher.example/", false},
		{"https://[2001:db8::1]/", false},

		// A host whose last label is a number is an IPv4 address to a
		// browser, in any form.
		{"https://127.1/", false},
		{"https://2130706433/", false},
		{"https://0x7f.1/", false},
		{"https://a.0x7f/", false},
		{"https://a.0x/", false},
		{"https://a.example.42/", false},

		// The port, if any, is one.
		{"https://a.example:1/", true},
		{"https://a.example:65535/", true},
		{"https://a.example:/", false},
		{"https://a.example:0/", false},
		{"https://a.example:65536/", false},
		{"https://a.example:99999/", false},

		// No bidirectional control anywhere, marks included.
		{"https://a.example/\u202Egpj.exe", false},
		{"https://a.example/\u2066x\u2069", false},
		{"https://a.example/\u200Fx", false},
		{"https://a.example/caf\u00e9", true},
	} {
		t.Run(tc.link, func(t *testing.T) {
			assert.Equal(t, tc.want, validLinkURL(tc.link))
		})
	}
}

func TestSingleLine(t *testing.T) {
	assert.True(t, singleLine("A title"))
	assert.True(t, singleLine(""))
	assert.False(t, singleLine("two\nlines"))
	assert.False(t, singleLine("trailing\n"))
	assert.False(t, singleLine("line\u2028separator"))
	assert.False(t, singleLine("paragraph\u2029separator"))
}

func TestVisibleText(t *testing.T) {
	assert.True(t, visibleText("A title"))
	assert.True(t, visibleText("  padded  "))
	assert.True(t, visibleText("two\nlines"))
	assert.False(t, visibleText(""))
	assert.False(t, visibleText("   "))
	assert.False(t, visibleText("\n\n"))
	assert.False(t, visibleText("tab\tbed"))
	assert.False(t, visibleText("nul\x00"))
	assert.False(t, visibleText("cr\r\n"))

	// Something visible means more than invisible format characters.
	assert.False(t, visibleText("\u200B"))
	assert.False(t, visibleText("\u200B \u2060\uFEFF"))
	assert.True(t, visibleText("zero\u200Bwidth"))
	assert.True(t, visibleText("\U0001F469\u200D\U0001F4BB"))

	// No bidi embedding, override or isolate; the marks stay.
	assert.False(t, visibleText("\u202Egnp.elcitra"))
	assert.False(t, visibleText("title \u202A"))
	assert.False(t, visibleText("\u2067\u05e9\u05dc\u05d5\u05dd\u2069"))
	assert.True(t, visibleText("\u05e9\u05dc\u05d5\u05dd\u200F!"))
	assert.True(t, visibleText("\u0645\u0631\u062d\u0628\u0627\u061C"))
}

func TestValidLinkPreviews(t *testing.T) {
	const link = "https://a.example/story"
	blobID := &blobpb.BlobId{Value: bytes.Repeat([]byte{1}, 16)}
	image := func() *blobpb.Media {
		return &blobpb.Media{Renditions: []*blobpb.Rendition{{Role: blobpb.Rendition_ORIGINAL, BlobId: blobID}}}
	}
	preview := func() *messagingpb.LinkPreview {
		return &messagingpb.LinkPreview{Url: link, Title: "A story", Description: "About things", Image: image()}
	}
	text := func(previews ...*messagingpb.LinkPreview) *messagingpb.TextContent {
		return &messagingpb.TextContent{Text: "read " + link + " today", LinkPreviews: previews}
	}

	assert.True(t, validLinkPreviews(text()))
	assert.True(t, validLinkPreviews(text(preview())))

	bare := preview()
	bare.Description, bare.Image = "", nil
	assert.True(t, validLinkPreviews(text(bare)))

	notInText := preview()
	notInText.Url = "https://b.example/"
	assert.False(t, validLinkPreviews(text(notInText)))

	partial := preview()
	partial.Url = "https://a.example"
	assert.False(t, validLinkPreviews(text(partial)))

	badScheme := text(preview())
	badScheme.Text = "read ftp://a.example/story today"
	badScheme.LinkPreviews[0].Url = "ftp://a.example/story"
	assert.False(t, validLinkPreviews(badScheme))

	blankTitle := preview()
	blankTitle.Title = " "
	assert.False(t, validLinkPreviews(text(blankTitle)))

	// The title is one line; the description may have several.
	for _, br := range []string{"\n", "\u2028", "\u2029"} {
		multiline := preview()
		multiline.Title = "Your account is locked" + br + "a.example"
		assert.False(t, validLinkPreviews(text(multiline)), "%q", br)
	}
	multilineDescription := preview()
	multilineDescription.Description = "First paragraph.\nSecond paragraph."
	assert.True(t, validLinkPreviews(text(multilineDescription)))

	controlDescription := preview()
	controlDescription.Description = "a\x07b"
	assert.False(t, validLinkPreviews(text(controlDescription)))

	// An image is judged like a media item: one ORIGINAL by blob ID, no
	// metadata.
	derived := preview()
	derived.Image.Renditions[0].Role = blobpb.Rendition_DISPLAY
	assert.False(t, validLinkPreviews(text(derived)))
	withMetadata := preview()
	withMetadata.Image.Renditions[0].Blob = &blobpb.BlobMetadata{MimeType: "image/png"}
	assert.False(t, validLinkPreviews(text(withMetadata)))
	noRenditions := preview()
	noRenditions.Image.Renditions = nil
	assert.False(t, validLinkPreviews(text(noRenditions)))

	// Two previews of one link are refused, whatever the cap; the cap
	// itself is the proto's.
	assert.False(t, validLinkPreviews(text(preview(), preview())))
	second := &messagingpb.LinkPreview{Url: "https://b.example/", Title: "B"}
	two := text(preview(), second)
	two.Text = "read " + link + " and https://b.example/ today"
	assert.True(t, validLinkPreviews(two))

	assert.False(t, validLinkPreviews(text(nil)))
}
