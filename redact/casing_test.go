package redact

import (
	"strings"
	"testing"
	"unicode"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var casingNames = []string{"none", "sentence", "mixed", "upper"}

func TestCasingOf(t *testing.T) {
	cases := []struct {
		name string
		text string
		want casing
	}{
		{"lowercase", "on my way", casingNone},
		{"digits", "12345", casingNone},
		{"camel case", "iPhone", casingNone},
		{"sentence", "Hello there", casingSentence},
		{"two sentences one line", "Hello there. How are you?", casingSentence},
		{"ellipsis", "Well… Maybe", casingSentence},
		{"one capital per line", "hello\nWorld\nagain", casingSentence},
		{"every line capitalized past the cap", "Alpha\nBravo\nCharlie\nDelta\nEcho\nFoxtrot", casingSentence},
		{"single capital letters are shouting", "A\nB\nC\nD", casingUpper},
		{"short acronym", "OK", casingSentence},
		{"three letter acronym", "LOL", casingSentence},
		{"title case", "Meet Sam At Noon", casingMixed},
		{"two capitals", "Hi There", casingMixed},
		{"proper nouns", "Ask Sam or Alex about Paris", casingMixed},
		{"german nouns", "Der Hund und die Katze", casingMixed},
		{"shouting", "WHERE ARE YOU", casingUpper},
		{"four letters shouting", "OMG YES", casingUpper},
		{"shouting with digits", "CALL 911 NOW", casingUpper},
		{"mostly shouting", "GO TEAM go", casingMixed},
		{"cyrillic shouting", "ПРИВЕТ", casingUpper},
		{"cyrillic sentence", "Привет мир", casingSentence},
		{"greek title", "Καλή Μέρα", casingMixed},
		{"punctuation before the capital", "(Hello) world", casingSentence},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			lines := len(strings.Split(c.text, "\n"))
			got := casingOf(c.text, lines)
			assert.Equalf(t, c.want, got, "want %s, got %s", casingNames[c.want], casingNames[got])
		})
	}
}

// Casing is a shape field only for cased scripts.
func TestText_CasingOnlyForCasedScripts(t *testing.T) {
	assert.Equal(t, casingUpper, shapeOf("HELLO").casing)
	assert.Equal(t, casingNone, shapeOf("你好世界").casing)
	assert.Equal(t, casingNone, shapeOf("مرحبا").casing)
	// A message classified as another script keeps casing zero even if it
	// carries some Latin capitals.
	assert.Equal(t, casingNone, shapeOf("你好世界你好世界 OK").casing)
}

func countCapitalizedWords(line string) int {
	n := 0
	for w := range strings.FieldsSeq(line) {
		if first, ok := utf8First(w); ok && unicode.IsUpper(first) {
			n++
		}
	}
	return n
}

func TestText_Casing(t *testing.T) {
	t.Run("none", func(t *testing.T) {
		out := Text(seed, "on my way to the shop, back in twenty minutes\nsee you")
		for _, r := range out {
			assert.False(t, unicode.IsUpper(r), out)
		}
	})

	t.Run("sentence", func(t *testing.T) {
		out := Text(seed, "On my way to the shop, back in twenty minutes.\nSee you there")
		for line := range strings.SplitSeq(out, "\n") {
			assert.Equal(t, 1, countCapitalizedWords(line), line)
			first, _ := utf8First(line)
			assert.True(t, unicode.IsUpper(first), line)
		}
	})

	t.Run("mixed", func(t *testing.T) {
		out := Text(seed, "Meet Sam And Alex At The Old Bank On Main Street Tomorrow")
		lines := strings.Split(out, "\n")
		capitalized := 0
		for _, line := range lines {
			first, _ := utf8First(line)
			assert.True(t, unicode.IsUpper(first), "every line starts capitalized")
			capitalized += countCapitalizedWords(line)
		}
		assert.Greater(t, capitalized, len(lines), "more capitals than lines")
		assert.Less(t, capitalized, len(strings.Fields(out)), "not every word: that would be a different texture")
	})

	t.Run("mixed on the shortest buckets", func(t *testing.T) {
		// The smallest buckets must still hold two words so the second can
		// be capitalized, or the placeholder would re-classify as sentence.
		for _, text := range []string{"A B", "Hi There"} {
			out := Text(seed, text)
			require.Len(t, strings.Fields(out), 2, text)
			assert.Equal(t, 2, countCapitalizedWords(out), text)
		}
		// Across two lines, one line carries both words.
		out := Text(seed, "A B\nC")
		assert.Equal(t, 3, countCapitalizedWords(out), out)
		// With an edge emoji there is no room for a second word in four
		// units, so the casing steps down to sentence rather than the
		// emoji being dropped.
		out = Text(seed, "A B🔥")
		assert.Equal(t, 1, countCapitalizedWords(out), out)
		assert.Equal(t, casingSentence, shapeOf(out).casing)
		assert.Equal(t, 1, shapeOf(out).emoji)
		// Many one-word lines can round up to as many lines as units: a
		// column of lone letters with no room for a second word or a
		// lowercase letter, which reads as shouting.
		tall := "Ab B\n" + strings.Repeat("C\n", 12)
		assert.Equal(t, casingMixed, casingOf(tall, 13))
		assert.Equal(t, casingUpper, shapeOf(tall).casing)
		assert.Equal(t, shapeOf(tall), shapeOf(Text(seed, tall)))
		column := "Ab\n" + strings.Repeat("c\n", 12)
		assert.Equal(t, casingSentence, casingOf(column, 13))
		assert.Equal(t, casingUpper, shapeOf(column).casing)
		assert.Equal(t, shapeOf(column), shapeOf(Text(seed, column)))
		// With room for both, both are kept.
		out = Text(seed, "Hi There 🔥")
		assert.Equal(t, casingMixed, shapeOf(out).casing)
		assert.Equal(t, 1, shapeOf(out).emoji)
	})

	t.Run("upper", func(t *testing.T) {
		for _, text := range []string{"WHERE ARE YOU RIGHT NOW", "ABCD", "AB\nCD", "A🔥B🔥CD"} {
			out := Text(seed, text)
			letters := 0
			for _, r := range out {
				if unicode.IsLetter(r) {
					letters++
					assert.True(t, unicode.IsUpper(r), out)
				}
			}
			// Enough letters that the placeholder is still shouting, not
			// an acronym.
			assert.GreaterOrEqual(t, letters, minUpperLetters, out)
		}
	})
}

// Positions are the seed's, not the message's: the same class from
// differently arranged capitals gives the same placeholder.
func TestText_CasingIgnoresPositions(t *testing.T) {
	assert.Equal(t, Text(seed, "Ask Sam about the trip"), Text(seed, "ask sam About the Trip"))
	assert.Equal(t, Text(seed, "hello world. Bye"), Text(seed, "Hello world. bye"))
}

func utf8First(s string) (rune, bool) {
	for _, r := range s {
		return r, true
	}
	return 0, false
}
