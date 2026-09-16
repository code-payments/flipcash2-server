package redact

import (
	"testing"
	"unicode"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var hueNames = [hueCount]string{"yellow", "red", "orange", "green", "blue", "purple", "pink", "neutral"}

func dominantHue(text string) hue { return dominant(emojiHues(text)) }

// outputHues is the hue of each emoji in a placeholder, in reading order.
func outputHues(out string) []hue {
	var hs []hue
	for _, r := range out {
		if unicode.Is(emojiTable(), r) {
			hs = append(hs, hueOf(r))
		}
	}
	return hs
}

func TestEmojiHues(t *testing.T) {
	assert.Nil(t, emojiHues("no emoji here"))
	assert.Equal(t, []hue{hueRed, hueYellow, hueRed}, emojiHues("❤️😂❤️"))
	assert.Equal(t, []hue{hueRed, hueYellow, hueRed}, emojiHues("❤️ so 😂 good ❤️"))
	assert.Equal(t, []hue{hueRed}, emojiHues("❤️‍🔥"), "zwj sequence is one emoji")
	assert.Equal(t, []hue{hueYellow, hueYellow}, emojiHues("👍🏿👍🏻"), "skin tones collapse")
}

func TestResample(t *testing.T) {
	y, r, g := hueYellow, hueRed, hueGreen
	cases := []struct {
		name string
		in   []hue
		k    int
		want []hue
	}{
		{"none", nil, 4, nil},
		{"to zero", []hue{r}, 0, nil},
		{"identity", []hue{r, y, g}, 3, []hue{r, y, g}},
		{"stretch one", []hue{r}, 4, []hue{r, r, r, r}},
		{"stretch keeps order", []hue{r, y, g}, 4, []hue{r, r, y, g}},
		{"stretch two", []hue{r, y}, 8, []hue{r, r, r, r, y, y, y, y}},
		{"merge runs", []hue{r, r, r, r, r, y, y, y, y, y}, 8, []hue{r, r, r, r, y, y, y, y}},
		{"merge to one", []hue{r, r, y}, 1, []hue{r}},
		{"merge tie takes the earlier class", []hue{r, y}, 1, []hue{y}},
		{"lone emoji in a run may vanish", []hue{r, r, r, y, r, r, r, r, r}, 3, []hue{r, r, r}},
		{"but survives a tie", []hue{r, r, y, r, r, r}, 3, []hue{r, y, r}},
		{"long tail", []hue{y, y, y, y, y, y, y, y, y, y, y, y, y, y, y, y, y, y, y, r}, 8, []hue{y, y, y, y, y, y, y, y}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			assert.Equal(t, c.want, resample(c.in, c.k))
		})
	}

	// Resampling is idempotent at the target length, the property the
	// placeholder's own placeholder depends on.
	for _, in := range [][]hue{{r, y, g}, {r, r, y, r, r, r}, {y}} {
		for _, k := range []int{1, 2, 3, 4, 8} {
			once := resample(in, k)
			assert.Equal(t, once, resample(once, k))
		}
	}
}

func TestDominantHue(t *testing.T) {
	cases := []struct {
		name string
		text string
		want hue
	}{
		{"laughing", "😂😂😂", hueYellow},
		{"thumbs", "👍", hueYellow},
		{"hearts", "❤️❤️❤️", hueRed},
		{"broken heart", "💔", hueRed},
		{"fire", "🔥🔥", hueOrange},
		{"check", "✅", hueGreen},
		{"blue heart", "💙", hueBlue},
		{"grapes", "🍇", huePurple},
		{"sparkling heart", "💖💖", huePink},
		{"flag", "🇺🇸", hueNeutral},
		{"skull", "💀", hueNeutral},
		{"clock block default", "🕒", hueBlue},
		{"animal block default", "🐶", hueNeutral},
		{"unknown recent emoji", "🫠", hueNeutral},
		{"majority", "❤️❤️😂", hueRed},
		{"tie goes to the earlier class", "❤️😂", hueYellow},
		{"tie between later classes", "✅🔥", hueOrange},
		{"skin tone collapses to the base", "👍🏿👍🏿", hueYellow},
		{"zwj sequence votes once", "❤️‍🔥", hueRed},
		{"family votes once as a person", "👨‍👩‍👧‍👦", hueYellow},
		{"spaces and newlines ignored", "❤️ ❤️\n❤️", hueRed},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := dominantHue(c.text)
			assert.Equalf(t, c.want, got, "want %s, got %s", hueNames[c.want], hueNames[got])
		})
	}
}

// The placeholder for an emoji-only message is drawn from its dominant hue's
// palette and nothing else.
func TestText_EmojiHue(t *testing.T) {
	cases := map[string]hue{
		"😂😂😂":    hueYellow,
		"❤️❤️❤️": hueRed,
		"🔥":      hueOrange,
		"✅✅":     hueGreen,
		"💙💙💙💙":   hueBlue,
		"🍇":      huePurple,
		"🌸🌸":     huePink,
		"🇺🇸🇺🇸":   hueNeutral,
	}
	for text, h := range cases {
		t.Run(hueNames[h], func(t *testing.T) {
			out := Text(seed, text)
			require.NotEmpty(t, out)
			allowed := map[rune]bool{}
			for _, r := range palette[h] {
				allowed[r] = true
			}
			for _, r := range out {
				assert.Truef(t, allowed[r], "%q is not in the %s palette", r, hueNames[h])
			}
		})
	}

	// Same shape but for hue gives a different placeholder: hue is part of
	// the shape, and that is the only way two same-length emoji messages
	// differ.
	assert.NotEqual(t, Text(seed, "😂😂"), Text(seed, "❤️❤️"))
	assert.Equal(t, shapeOf("😂😂").units, shapeOf("❤️❤️").units)
}

// Each emoji of an emoji-only message keeps its own hue, in order, resampled
// to the length bucket.
func TestText_EmojiHueOrder(t *testing.T) {
	y, r, g, b, p := hueYellow, hueRed, hueGreen, hueBlue, huePink
	cases := []struct {
		text string
		want []hue
	}{
		{"❤️😂", []hue{r, y}},
		{"😂❤️", []hue{y, r}},
		{"❤️😂❤️", []hue{r, r, y, r}},
		{"🌊🌸🍀", []hue{b, b, p, g}},
		{"🍎🍎 🍊🍊 🍇🍇", []hue{r, r, r, hueOrange, hueOrange, hueOrange, huePurple, huePurple}},
		{"🔴🔴🔴 🟢🟢🟢 ✅", []hue{r, r, r, r, g, g, g, g}},
		{"❤️\n😂\n❤️", []hue{r, r, y, r}},
	}
	for _, c := range cases {
		t.Run(c.text, func(t *testing.T) {
			out := Text(seed, c.text)
			assert.Equal(t, c.want, outputHues(out), out)
			assert.Equal(t, out, Text(seed, out), "idempotent")
		})
	}

	// A message's own hue sequence is what it shows: two messages of one
	// length with different sequences differ, and different emoji of the
	// same hues do not.
	assert.NotEqual(t, Text(seed, "❤️😂"), Text(seed, "😂❤️"))
	assert.Equal(t, Text(seed, "❤️😂"), Text(seed, "🔴👍"))
}

// A text message's hues are its sprinkled emoji's, in order, and nil without
// any, so they never split two emoji-free text shapes.
func TestText_HueForSprinkledEmoji(t *testing.T) {
	assert.Nil(t, shapeOf("great stuff").hues)
	assert.Equal(t, []hue{hueGreen}, shapeOf("great stuff ✅").hues)
	assert.Equal(t, []hue{hueRed}, shapeOf("great stuff ❤️").hues)
	assert.NotEqual(t, Text(seed, "great stuff ✅"), Text(seed, "great stuff ❤️"))
	assert.Equal(t, Text(seed, "great stuff ✅"), Text(seed, "great stuff 💚"))

	y, r, g, b, p := hueYellow, hueRed, hueGreen, hueBlue, huePink
	cases := []struct {
		text string
		want []hue
	}{
		{"see you all saturday 🌊🌸🍀", []hue{b, p, g}},
		{"🌊🌸🍀 see you all saturday", []hue{b, p, g}},
		{"❤️ love this so much ✅", []hue{r, g}},
		{"so ❤️ good 😂 wow ✅ yes", []hue{r, y, g}},
		{"你能❤️再发一次😂地址吗✅", []hue{r, y, g}},
		{"congrats 🎉🎉🎉🎉🎉🎉 ❤️❤️❤️❤️❤️❤️ to all", []hue{hueOrange, r, r}},
		// Three emoji clamped to two: the last slot merges yellow and
		// green, and the tie goes to the earlier class.
		{"❤️a😂b✅c", []hue{r, y}},
	}
	for _, c := range cases {
		t.Run(c.text, func(t *testing.T) {
			out := Text(seed, c.text)
			assert.Equal(t, c.want, outputHues(out), out)
			assert.Equal(t, out, Text(seed, out), "idempotent")
		})
	}
}

// Palette invariants the idempotence property depends on: every hue has
// filler, and every filler emoji is classified back to the hue it stands
// for, so redacting a placeholder lands on the same palette.
func TestPalette(t *testing.T) {
	for h := range hueCount {
		require.NotEmptyf(t, palette[h], "%s palette is empty", hueNames[h])
		for _, r := range palette[h] {
			assert.Equalf(t, h, hueOf(r), "%q sits in the %s palette but classifies as %s", r, hueNames[h], hueNames[hueOf(r)])
			assert.Truef(t, inAny(r, scripts[len(scripts)-1].tables), "%q is not in the emoji table", r)
		}
	}
}

// The table's block defaults are consulted after the overrides, so an
// override inside a block must win, and each override must be a hue the
// block would not have given (otherwise it is dead weight to maintain).
func TestHueTable(t *testing.T) {
	for r, h := range hueOverrides {
		assert.Equalf(t, h, hueOf(r), "override for %q not honoured", r)
		assert.Truef(t, unicode.Is(emojiTable(), r), "override for %q is unreachable: not in the emoji table", r)
	}
	assert.Equal(t, hueRed, hueOf(0x1F621), "😡 override inside the emoticon block")
	assert.Equal(t, hueYellow, hueOf(0x1F600), "😀 block default")
}

// The table below U+1F000 is the Emoji property exactly: the default- and
// text-presentation emoji keyboards send are in, and the symbols Japanese
// and Korean messages use as punctuation are out.
func TestEmojiTable(t *testing.T) {
	for _, r := range "⌚⌛⏩⏰⏳◽◾⬛⬜‼⁉™ℹ↔↩▶◀⬅⬆⬇⏸⏭〰〽㊗㊙☀☎☑☔☕☺♀♂♠♥♻⚠⚡⚽⛄⛔✂✅✈✔✖✨❄❌❓❗❤➕➡⭐⭕Ⓜ" {
		assert.Truef(t, unicode.Is(emojiTable(), r), "%q %U should be an emoji", r, r)
	}
	for _, r := range "♡♪♫★☆✓©®#*0123456789→←↑↓·•‥…※〒" {
		assert.Falsef(t, unicode.Is(emojiTable(), r), "%q %U should be text", r, r)
	}
}
