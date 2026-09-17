package redact

import (
	"math/rand/v2"
	"strings"
	"unicode"
)

// casing is how much of a message is capitalized, bucketed to four classes.
// Under blur a sentence-initial capital is invisible, but a run of capitals
// is a distinct texture — tall uniform glyphs with no ascenders or descenders
// — and title case or a message full of proper nouns has a visibly different
// rhythm from plain prose. The class carries that rhythm and nothing about
// which words are capitalized: rendering picks the positions from the seed.
//
// Only a cased script has a casing; every other script's shape leaves it at
// the zero value, so it never distinguishes two shapes in a script without
// capitals.
type casing uint8

const (
	// casingNone has no capital letters.
	casingNone casing = iota
	// casingSentence has at most one capital per line or sentence: ordinary
	// prose.
	casingSentence
	// casingMixed has more capitals than that: title case, headings, names.
	casingMixed
	// casingUpper is every cased letter uppercase, with enough of them that
	// it is shouting rather than an acronym.
	casingUpper
)

// minUpperLetters is the fewest cased letters an all-uppercase message needs
// to count as casingUpper. Below it, "OK" and "LOL" are ordinary words.
const minUpperLetters = 4

// casingOf classifies text. lines is the raw non-blank line count, before the
// line bucket, so that a long message whose every line starts with a capital
// is still ordinary prose. Only displayed characters count: a capital joined
// onto an emoji by a zero width joiner is not rendered, so it must not push
// the message into casingUpper with fewer letters than a casingUpper
// placeholder carries.
func casingOf(text string, lines int) casing {
	var upper, lower, capitalized, terminators int
	for word := range strings.FieldsSeq(text) {
		first := true
		for r := range displayed(word) {
			switch {
			case unicode.IsUpper(r):
				upper++
				if first {
					capitalized++
				}
			case unicode.IsLower(r):
				lower++
			case r == '.' || r == '!' || r == '?' || r == '…':
				terminators++
			}
			if unicode.IsLetter(r) {
				first = false
			}
		}
	}

	switch {
	case lower == 0 && upper >= minUpperLetters:
		return casingUpper
	case capitalized == 0:
		// A capital inside a word (iPhone, McDonald) is as invisible under
		// blur as none at all.
		return casingNone
	case capitalized <= lines+terminators:
		return casingSentence
	default:
		return casingMixed
	}
}

// applyCasing capitalizes a line's lowercase words according to c. For
// casingMixed the first two words are always capitalized, so the line has
// more capitals than lines whatever the seed draws, and the rest are drawn
// at one in two; the caller guarantees at least two words.
func applyCasing(words []string, c casing, rng *rand.Rand) {
	switch c {
	case casingNone:
	case casingSentence:
		words[0] = capitalize(words[0])
	case casingMixed:
		for i := range words {
			if i < 2 || rng.IntN(2) == 0 {
				words[i] = capitalize(words[i])
			}
		}
	case casingUpper:
		for i := range words {
			words[i] = strings.ToUpper(words[i])
		}
	}
}

func capitalize(word string) string {
	for i, r := range word {
		return string(unicode.ToUpper(r)) + word[i+len(string(r)):]
	}
	return word
}
