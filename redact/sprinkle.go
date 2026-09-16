package redact

import (
	"math/rand/v2"
	"slices"
	"unicode"
)

// A text message's emoji are sprinkled into its placeholder: the right
// number, at the right end, in the right colours, at the resolution blur can
// show. The shape carries a count capped at three, a placement, and one hue
// per rendered emoji in reading order (see resample) — about a dozen bits in
// all. It never carries where each emoji sat: rendering picks the exact
// positions from the seed and lays the hues into them in order.

// placement is where a text message's emoji sit, as a whole.
type placement uint8

const (
	// placementNone: no emoji.
	placementNone placement = iota
	// placementLeading: every emoji comes before the first other character.
	placementLeading
	// placementTrailing: every emoji comes after the last other character.
	placementTrailing
	// placementInterspersed: anything else, including both ends.
	placementInterspersed
)

// sprinkleBuckets caps how many emoji a text placeholder carries. One, two
// and "several" is all blur distinguishes.
var sprinkleBuckets = []int{1, 2, 3}

// sprinkleOf counts a message's emoji and classifies their placement,
// looking at displayed characters only: whitespace and newlines do not
// separate an emoji from the ends.
func sprinkleOf(text string) (count int, p placement) {
	var (
		seenOther, seenEmoji bool
		emojiAfterOther      bool // some emoji follows an other character
		otherAfterEmoji      bool // some other character follows an emoji
	)
	for r := range displayed(text) {
		if unicode.IsSpace(r) {
			continue
		}
		if unicode.Is(emoji, r) {
			count++
			seenEmoji = true
			emojiAfterOther = emojiAfterOther || seenOther
			continue
		}
		seenOther = true
		otherAfterEmoji = otherAfterEmoji || seenEmoji
	}

	switch {
	case count == 0 || !seenOther:
		return count, placementNone
	case !emojiAfterOther:
		return count, placementLeading
	case !otherAfterEmoji:
		return count, placementTrailing
	default:
		return count, placementInterspersed
	}
}

// maxSprinkle is the most emoji a placeholder of the given shape can hold
// while still rendering as classified: the largest count whose lineNeed
// fits the room the designated line can be given, every other line keeping
// one unit. The shape clamps its count to this so that redacting a
// placeholder reproduces the count.
func maxSprinkle(s *script, units, lines int, p placement, c casing) int {
	capacity := units - (lines - 1)
	for e := sprinkleBuckets[len(sprinkleBuckets)-1]; e > 0; e-- {
		if lineNeed(s, c, units, lines, e, p) <= capacity {
			return e
		}
	}
	return 0
}

// sprinkleWords inserts one emoji per hue into a line's words according to
// p, hues in reading order. Each interspersed emoji goes in its own interior
// gap while gaps last, leftovers join the first chosen gap. The caller has
// already spent the spaces reserved for tokens that cannot be placed.
func sprinkleWords(ws []string, rng *rand.Rand, hues []hue, p placement) []string {
	token := func(hues []hue) string {
		return string(emojiRun(rng, hues))
	}
	switch p {
	case placementLeading:
		return append([]string{token(hues)}, ws...)
	case placementTrailing:
		return append(ws, token(hues))
	}

	// Gap i is between ws[i-1] and ws[i].
	perGap, k := chooseGaps(rng, len(ws)-1, len(hues))
	out := make([]string, 0, len(ws)+k)
	next := 0
	for i, w := range ws {
		if n := perGap[i]; n > 0 {
			out = append(out, token(hues[next:next+n]))
			next += n
		}
		out = append(out, w)
	}
	return out
}

// sprinkleRunes is sprinkleWords for an unspaced script: the emoji are
// inserted into a run of letters, each interspersed emoji at its own
// interior index while indices last.
func sprinkleRunes(letters []rune, rng *rand.Rand, hues []hue, p placement) []rune {
	switch p {
	case placementLeading:
		return append(emojiRun(rng, hues), letters...)
	case placementTrailing:
		return append(letters, emojiRun(rng, hues)...)
	}

	// Slot i is before letters[i].
	perSlot, _ := chooseGaps(rng, len(letters)-1, len(hues))
	out := make([]rune, 0, len(letters)+len(hues))
	next := 0
	for i, r := range letters {
		if n := perSlot[i]; n > 0 {
			out = append(out, emojiRun(rng, hues[next:next+n])...)
			next += n
		}
		out = append(out, r)
	}
	return out
}

// emojiRun draws one emoji per hue, in order.
func emojiRun(rng *rand.Rand, hues []hue) []rune {
	out := make([]rune, len(hues))
	for i, h := range hues {
		out[i] = pick(rng, palette[h])
	}
	return out
}

// chooseGaps picks min(count, gaps) distinct interior gaps and returns how
// many emoji each holds, keyed by the index of the word or letter the gap
// precedes, plus the number chosen. Leftover emoji beyond the gaps join the
// first chosen gap in reading order, so the hue sequence is laid out
// left to right whichever gaps the seed picks.
func chooseGaps(rng *rand.Rand, gaps, count int) (map[int]int, int) {
	k := min(count, gaps)
	chosen := rng.Perm(gaps)[:k]
	slices.Sort(chosen)
	per := make(map[int]int, k)
	for _, g := range chosen {
		per[g+1] = 1
	}
	per[chosen[0]+1] += count - k
	return per, k
}
