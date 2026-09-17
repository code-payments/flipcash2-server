package redact

import (
	"strings"
	"testing"
	"unicode"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var placementNames = []string{"none", "leading", "trailing", "interspersed"}

func TestSprinkleOf(t *testing.T) {
	cases := []struct {
		name  string
		text  string
		count int
		want  placement
	}{
		{"no emoji", "great stuff", 0, placementNone},
		{"emoji only", "😂😂", 2, placementNone},
		{"trailing", "great stuff 👍👍", 2, placementTrailing},
		{"trailing without space", "great👍", 1, placementTrailing},
		{"trailing across lines", "great\nstuff\n👍", 1, placementTrailing},
		{"leading", "🔥 new drop", 1, placementLeading},
		{"leading across lines", "🔥🔥\nnew drop", 2, placementLeading},
		{"both ends", "❤️ love ❤️", 2, placementInterspersed},
		{"middle", "so 😂 good", 1, placementInterspersed},
		{"middle and end", "so 😂 good 😂", 2, placementInterspersed},
		{"skin tone is one", "ok 👍🏽", 1, placementTrailing},
		{"zwj family is one", "us 👨‍👩‍👧‍👦", 1, placementTrailing},
		{"digits are other characters", "5 🔥", 1, placementTrailing},
		{"punctuation is an other character", "wow 🔥!", 1, placementInterspersed},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			count, p := sprinkleOf(c.text)
			assert.Equal(t, c.count, count)
			assert.Equalf(t, c.want, p, "want %s, got %s", placementNames[c.want], placementNames[p])
		})
	}
}

// splitEmoji separates a placeholder's displayed characters into emoji and
// the rest, in order, ignoring whitespace.
func splitEmoji(out string) (emoji []rune, isEmoji []bool) {
	for _, r := range out {
		if unicode.IsSpace(r) {
			continue
		}
		e := unicode.Is(emojiTable(), r)
		isEmoji = append(isEmoji, e)
		if e {
			emoji = append(emoji, r)
		}
	}
	return emoji, isEmoji
}

func emojiTable() *unicode.RangeTable { return emojiScript.tables[0] }

func inPalette(t *testing.T, h hue, rs []rune) {
	t.Helper()
	allowed := map[rune]bool{}
	for _, r := range palette[h] {
		allowed[r] = true
	}
	for _, r := range rs {
		assert.Truef(t, allowed[r], "%q is not in the %s palette", r, hueNames[h])
	}
}

func TestText_Sprinkle(t *testing.T) {
	t.Run("trailing", func(t *testing.T) {
		out := Text(seed, "great stuff 👍👍")
		emoji, flags := splitEmoji(out)
		require.Len(t, emoji, 2)
		assert.Equal(t, []bool{true, true}, flags[len(flags)-2:], "at the end")
		assert.False(t, flags[0])
		inPalette(t, hueYellow, emoji)
		assert.True(t, strings.Contains(out, " "+string(emoji)), "emoji token follows a space")
		assert.Equal(t, 16, countUnits(out))
	})

	t.Run("leading", func(t *testing.T) {
		out := Text(seed, "🔥 new drop today")
		emoji, flags := splitEmoji(out)
		require.Len(t, emoji, 1)
		assert.True(t, flags[0], "at the start")
		assert.False(t, flags[len(flags)-1])
		inPalette(t, hueOrange, emoji)
	})

	t.Run("interspersed", func(t *testing.T) {
		out := Text(seed, "❤️ love this so much ❤️")
		emoji, flags := splitEmoji(out)
		require.Len(t, emoji, 2)
		assert.False(t, flags[0], "not at the start")
		assert.False(t, flags[len(flags)-1], "not at the end")
		inPalette(t, hueRed, emoji)
		// Each emoji in its own gap when there are enough words.
		assert.Equal(t, 2, strings.Count(out, " "+string(emoji[0])+" ")+strings.Count(out, " "+string(emoji[1])+" "), out)
	})

	t.Run("count is capped", func(t *testing.T) {
		out := Text(seed, "🎉🎉🎉🎉🎉🎉🎉🎉 congrats to everyone")
		emoji, _ := splitEmoji(out)
		assert.Len(t, emoji, 3)
	})

	t.Run("unspaced trailing", func(t *testing.T) {
		out := Text(seed, "你能再发一次地址吗👍")
		emoji, flags := splitEmoji(out)
		require.Len(t, emoji, 1)
		assert.True(t, flags[len(flags)-1])
		assert.NotContains(t, out, " ")
		assert.Equal(t, 16, countUnits(out))
	})

	t.Run("unspaced interspersed", func(t *testing.T) {
		out := Text(seed, "你能😂再发一次😂地址吗")
		emoji, flags := splitEmoji(out)
		require.Len(t, emoji, 2)
		assert.False(t, flags[0])
		assert.False(t, flags[len(flags)-1])
		assert.NotContains(t, out, " ")
	})

	t.Run("multiline keeps edges", func(t *testing.T) {
		out := Text(seed, "first line\nsecond line\nthird 👍")
		lines := strings.Split(out, "\n")
		require.Len(t, lines, 3)
		e0, _ := splitEmoji(lines[0])
		e2, _ := splitEmoji(lines[2])
		assert.Empty(t, e0)
		assert.Len(t, e2, 1)

		out = Text(seed, "👍 first line\nsecond line\nthird")
		lines = strings.Split(out, "\n")
		e0, _ = splitEmoji(lines[0])
		assert.Len(t, e0, 1)
	})

	t.Run("clamped in the smallest multiline shape", func(t *testing.T) {
		// Eight units over three lines leave six for the emoji line, which
		// fits one interspersed emoji with a word on each side, not three.
		text := "❤️a❤️b❤️c\nd\ne"
		s := shapeOf(text)
		require.Equal(t, 8, s.units)
		require.Equal(t, 3, s.lines)
		assert.Equal(t, 1, s.emoji)
		out := Text(seed, text)
		emoji, _ := splitEmoji(out)
		assert.Len(t, emoji, 1)
		assert.Equal(t, out, Text(seed, out), "idempotent")
	})

	t.Run("casing applies to the words around the emoji", func(t *testing.T) {
		out := Text(seed, "GREAT STUFF 👍")
		for _, r := range out {
			if unicode.IsLetter(r) {
				assert.True(t, unicode.IsUpper(r))
			}
		}
		out = Text(seed, "Great Stuff Here 👍")
		assert.Greater(t, countCapitalizedWords(out), 1)
	})
}

// The exact gaps are the seed's: the same class from differently placed
// emoji gives the same placeholder.
func TestText_SprinkleIgnoresPositions(t *testing.T) {
	assert.Equal(t, Text(seed, "so 😂 good to see you"), Text(seed, "so good to 😂 see you"))
	assert.Equal(t, Text(seed, "❤️ love this ❤️ so"), Text(seed, "so ❤️ love ❤️ this"))
}
