// Package redact produces placeholder text for a viewer who may see that a
// message exists but not what it says: a non-member browsing a group whose
// listener rules they do not satisfy. The client blurs the placeholder the
// way it blurs a blurhash, and like a blurhash the placeholder carries only
// shape — script, rough length, line structure — never content.
//
// The guarantee is structural rather than statistical. Text is a pure
// function of the seed and the message's shape (see shape), so the
// placeholder cannot depend on anything else: two messages of the same shape
// yield byte-identical placeholders under the same seed, and no transform of
// the original characters is ever performed. What a viewer learns is bounded
// by the number of distinct shapes, a few bits per message. Line structure
// is the non-blank line count and the blank lines between them, each capped
// low, so a bubble padded tall with empty lines renders about as tall. The
// one field of a shape that reflects content is the hue of each emoji the
// placeholder renders (see hue), a deliberate widening that mirrors the
// colours a blurhash keeps for an image.
//
// Filler is drawn from an alphabet of the message's own script so the
// platform's text layout does the rest: Arabic filler joins into cursive and
// lays out right to left, CJK filler is wide and unspaced, emoji-only messages
// stay emoji-sized. Alphabets contain only letters, so a placeholder is never
// something a client's link or mention detector fires on, and rendered
// unblurred by a buggy client it still reveals nothing.
package redact

import (
	"crypto/sha256"
	"encoding/binary"
	"math/rand/v2"
	"strings"
	"unicode"
)

// lineBuckets are the counts a message's non-blank lines are rounded up to;
// the last is the cap. Exact up to four, since a client renders those
// distinctly, then growing by about half so a tall message of short lines
// still renders tall: each step costs a viewer well under a bit. Every
// bucket is at most the length bucket of a message that fills it (the text
// buckets are 4, 8, 16, 24, 32, ...), so a placeholder always has a unit
// for each line; shapeOf clamps to be sure.
var lineBuckets = []int{1, 2, 3, 4, 6, 8, 12, 16, 24, 32}

// blankBuckets are the counts a message's interior blank lines are rounded
// up to; the last is the cap. Blank lines make a bubble tall without adding
// text, and a message padded with them should render about as tall, so the
// count is kept at this resolution: one, a couple, some, a lot.
var blankBuckets = []int{1, 2, 4, 8}

// solidUnits is the largest length bucket whose shape says whether the
// text was one unbroken run (see shape.solid). The most common replies are
// a single word — "thanks", "perfect", "congratulations" — and under blur
// one blob looks nothing like the two or three words a bucket otherwise
// holds. Above the cap an unbroken run is a link or a handle more often
// than a word, and a link-shaped placeholder would say that someone shared
// one, so longer runs render as words like everything else. Below the cap
// the bit splits each bucket's shapes in two.
const solidUnits = 16

// domain separates the placeholder generator's seed from any other use of the
// same bytes.
const domain = "flipcash.redact.text.v1"

// Text returns filler that stands in for text: the same script, direction,
// bucketed length, bucketed line count and bucketed blank lines, none of the
// content. The seed is normally the message ID, so the same message renders
// the same placeholder across pages, delta catch-up and devices, and no
// state is stored.
//
// Text that displays nothing (whitespace, format characters, stray
// combining marks) yields "".
func Text(seed []byte, text string) string {
	s := shapeOf(text)
	if s.units == 0 {
		return ""
	}
	return s.render(seed)
}

// shape is everything a placeholder may depend on. Adding a field here widens
// what a viewer can learn, so each one is bucketed to a handful of values.
type shape struct {
	script *script
	// units is the bucketed length in displayed characters: runes less the
	// modifiers that attach to a neighbour (see modifier). Spaces count,
	// since they take room in the bubble, except in an emoji-only message,
	// whose length is its emoji count.
	units int
	// lines is the bucketed non-blank line count (see lineBuckets), never
	// more than units so every line has something on it.
	lines int
	// blanks is the bucketed count of blank lines between the first and
	// last non-blank line (see blankBuckets). Leading and trailing blank
	// lines are not reproduced, as a client trims them. Zero whenever
	// lines is one, so it never distinguishes two single-line shapes.
	blanks int
	// hues is the hue of each emoji the placeholder renders, in reading
	// order: one per unit of an emoji-only message, one per sprinkled emoji
	// of a text message, resampled from the message's own emoji (see
	// resample). Nil when there are none, so it never distinguishes two
	// emoji-free shapes.
	hues []hue
	// casing is the capitalization class of a message in a cased script
	// and zero for every other. casingMixed steps down to casingSentence
	// when keeping it would cost a sprinkled emoji (see shapeOf).
	casing casing
	// emoji is the bucketed, clamped count of emoji sprinkled into a text
	// message (see sprinkle); placement says where they sit as a whole.
	// Both are zero for an emoji-only message, whose emoji are the message.
	emoji     int
	placement placement
	// solid marks a one-line message in a spaced script whose text is one
	// unbroken run — no whitespace between its first and last non-emoji
	// character — of at most solidUnits. Its letters render as one word
	// beside any emoji, since the length bucket would otherwise split them
	// into two or three. Zero for an unspaced script, where no placeholder
	// has a space anyway, so it never distinguishes two shapes there; zero
	// when an interspersed emoji is rendered, which needs a word on each
	// side; and zero past one line, since a rendered line always carries a
	// letter, so an emoji alone on its own line would come back as text
	// beside it and the placeholder would not be its own placeholder.
	solid bool
}

func shapeOf(text string) shape {
	var (
		units   int
		lines   int
		blanks  int
		pending int // blank lines since the last non-blank line
	)
	for line := range strings.SplitSeq(text, "\n") {
		// A line that displays nothing is blank, whether it is empty,
		// whitespace, or a stray modifier or format character: a line
		// counted as non-blank must carry at least one unit, or the
		// placeholder could have more lines than units to put on them.
		n := countUnits(strings.TrimSpace(line))
		if n == 0 {
			pending++
			continue
		}
		if lines > 0 {
			// Only blank lines between two non-blank lines count; leading
			// ones are skipped here and trailing ones are never flushed.
			blanks += pending
		}
		pending = 0
		lines++
		units += n
	}
	if lines == 0 {
		return shape{}
	}

	s := classify(text)
	var hues []hue
	if s.hued {
		// An emoji-only message is its emoji: the spaces a sender puts
		// between them are not rendered (see renderRunes) and would push
		// three spaced emoji into the eight bucket, where a client no
		// longer renders them large.
		hues = emojiHues(text)
		units = len(hues)
	}
	out := shape{
		script: s,
		units:  bucket(units, s.buckets),
	}
	out.lines = min(bucket(lines, lineBuckets), out.units)
	if blanks > 0 {
		out.blanks = bucket(blanks, blankBuckets)
	}
	if s.hued {
		out.hues = resample(hues, out.units)
	}
	if s.cased {
		out.casing = casingOf(text, lines)
	}
	if !s.hued {
		if count, p := sprinkleOf(text); count > 0 {
			// The emoji come first: they are a stronger signal under blur
			// than a capital, so the count is fixed at the room the least
			// demanding casing leaves and the casing settles for what is
			// left (below).
			n := min(bucket(count, sprinkleBuckets), maxSprinkle(s, out.units, out.lines, p, casingNone))
			if n > 0 {
				out.emoji, out.placement = n, p
				out.hues = resample(emojiHues(text), n)
			}
		}
	}
	if s.spaced() && lines == 1 && out.units <= solidUnits && out.placement != placementInterspersed {
		out.solid = solidOf(text)
	}
	// A casing is kept only if its designated line fits (see lineNeed);
	// otherwise it steps down: title case to sentence case, which loses a
	// capital that would be lost among the lines anyway, and sentence case
	// to shouting, which only happens with four or more lines and no room
	// for a lowercase letter beside their capitals — a column of lone
	// capitals, which is what casingOf calls "A\nB\nC\nD" too.
	for out.casing == casingMixed || out.casing == casingSentence {
		if lineNeed(s, out.casing, out.units, out.lines, out.emoji, out.placement) <= out.units-(out.lines-1) {
			break
		}
		if out.casing == casingMixed {
			out.casing = casingSentence
		} else {
			out.casing = casingUpper
		}
	}
	return out
}

// countUnits approximates the grapheme count of one line as its displayed
// runes (see displayed). The bucket absorbs the difference from a true
// segmentation.
func countUnits(line string) int {
	n := 0
	for range displayed(line) {
		n++
	}
	return n
}

// solidOf reports whether text's non-emoji characters form one unbroken
// run: no whitespace falls between the first and the last of them. Emoji
// and the whitespace around them do not break the run, so "thanks 👍" is
// solid and its letters render as one word beside the emoji. Text with no
// such character is not solid.
func solidOf(text string) bool {
	var seenOther, gap bool
	for r := range displayed(text) {
		switch {
		case unicode.IsSpace(r):
			gap = gap || seenOther
		case unicode.Is(emoji, r):
		default:
			if gap {
				return false
			}
			seenOther = true
		}
	}
	return seenOther
}

// bucket rounds n up to the first bucket at or above it, or to the cap.
func bucket(n int, buckets []int) int {
	for _, b := range buckets {
		if n <= b {
			return b
		}
	}
	return buckets[len(buckets)-1]
}

// render generates the placeholder. The generator is seeded from the seed
// alone, so the sequence of draws — and with it the output — is fixed by the
// seed and the shape.
func (s shape) render(seed []byte) string {
	rng := newRNG(seed)

	lines := min(s.lines, s.units)
	split := s.splitUnits(rng, lines)
	gaps := s.splitBlanks(rng, lines)

	// One designated line carries what the shape promises: every
	// sprinkled emoji (the first line for leading, the last for trailing,
	// so they stay at the message's edge, otherwise the longest), the two
	// words title case needs, and the two-letter word that keeps a
	// sentence-case or title-case placeholder from being all capitals. It
	// is topped up to what that takes; the shape only carries what fits.
	designated := 0
	for i, n := range split {
		if n > split[designated] {
			designated = i
		}
	}
	if s.emoji > 0 {
		switch s.placement {
		case placementLeading:
			designated = 0
		case placementTrailing:
			designated = lines - 1
		}
	}
	ensure(split, designated, lineNeed(s.script, s.casing, s.units, s.lines, s.emoji, s.placement))

	// Hues are spent in reading order: an emoji-only message's across its
	// lines, a text message's all on the designated line.
	var b strings.Builder
	next := 0
	for i, n := range split {
		if i > 0 {
			b.WriteByte('\n')
			for range gaps[i-1] {
				b.WriteByte('\n')
			}
		}
		var hues []hue
		switch {
		case s.script.hued:
			hues = s.hues[next : next+n]
			next += n
		case i == designated:
			hues = s.hues
		}
		if s.script.spaced() {
			b.WriteString(s.renderWords(rng, n, hues, i == designated))
		} else {
			b.WriteString(s.renderRunes(rng, n, hues))
		}
	}
	return b.String()
}

// renderWords renders one line of a spaced script spanning n units, with
// one emoji per hue sprinkled in. The designated line is the one lineNeed
// sized, and it delivers what the size was for.
func (s shape) renderWords(rng *rand.Rand, n int, hues []hue, designated bool) string {
	emoji := len(hues)
	interspersed := s.placement == placementInterspersed
	// Each emoji token costs a space: one token at an edge, one per emoji
	// interspersed (sprinkleWords refunds any it cannot place).
	tokens := 0
	if emoji > 0 {
		tokens = 1
		if interspersed {
			tokens = emoji
		}
	}
	span := n - emoji - tokens
	opts := wordOpts{maxSpaces: span}
	// casingMixed needs a second word to capitalize, and an interspersed
	// emoji needs a word on each side. Asking for two words on every line
	// that has room is the simplest guarantee: the designated line has it.
	opts.atLeastTwo = s.casing == casingMixed || (emoji > 0 && interspersed)
	// A sentence-case or title-case placeholder needs a lowercase letter
	// somewhere, or with enough capitals it would re-classify as shouting.
	// The designated line's first word is two letters when it has room:
	// capitalize only touches the first.
	opts.longFirst = designated && s.casing != casingNone
	// casingUpper needs minUpperLetters letters or the placeholder would
	// re-classify as an acronym. Every line keeps that many unless it is
	// shorter, so the message as a whole does; on the smallest bucket that
	// means no spaces at all.
	if s.casing == casingUpper {
		opts.maxSpaces = max(span-minUpperLetters, 0)
	}
	// A solid message is one word beside its emoji: the shape says its
	// text had no space in it.
	if s.solid {
		opts.maxSpaces = 0
	}
	// An interspersed emoji still gets its two words: the space it costs
	// is refunded as a letter below.
	if opts.atLeastTwo {
		opts.maxSpaces = max(opts.maxSpaces, 1)
	}
	ws := words(rng, s.script, span, opts)
	if emoji > 0 && interspersed {
		// Interspersed emoji beyond the interior gaps share a gap, and the
		// spaces reserved for them are spent as letters instead — before
		// casing, so an uppercase line stays uppercase.
		for range emoji - min(emoji, len(ws)-1) {
			ws[len(ws)-1] += string(pick(rng, s.script.alphabet))
		}
	}
	applyCasing(ws, s.casing, rng)
	if emoji > 0 {
		ws = sprinkleWords(ws, rng, hues, s.placement)
	}
	return strings.Join(ws, " ")
}

// renderRunes renders one line of an unspaced script spanning n units. For
// an emoji-only message hues has one entry per unit and the line is those
// emoji; otherwise it is letters with one emoji per hue sprinkled in.
func (s shape) renderRunes(rng *rand.Rand, n int, hues []hue) string {
	if s.script.hued {
		out := make([]rune, n)
		for i := range out {
			out[i] = pick(rng, palette[hues[i]])
		}
		return string(out)
	}
	letters := make([]rune, n-len(hues))
	for i := range letters {
		letters[i] = pick(rng, s.script.alphabet)
	}
	if len(hues) > 0 {
		letters = sprinkleRunes(letters, rng, hues, s.placement)
	}
	return string(letters)
}

// lineNeed is the fewest units the designated line of a spaced or unspaced
// script needs to hold e emoji at placement p under casing c, given the
// message's bucketed units and lines. It is the one place the arithmetic
// lives: maxSprinkle inverts it to clamp the emoji count, shapeOf checks
// the casing against it, and render tops the line up to it.
//
// The line needs two words when the casing is title case (a second capital)
// or an emoji sits between words, and a two-letter word when the casing
// would otherwise render only capitals: sentence case with as many lines as
// minUpperLetters, or title case unless the message is so small that it
// cannot reach that many capitals (four units on two lines: "A B" over
// "C"). Each emoji then costs a unit, plus a space for each token in a
// spaced script.
func lineNeed(s *script, c casing, units, lines, e int, p placement) int {
	interspersed := p == placementInterspersed
	need := 1
	if s.spaced() {
		two := c == casingMixed || (e > 0 && interspersed)
		lower := c == casingMixed && !(units-(lines-1) == 3 && lines <= 2) ||
			c == casingSentence && lines >= minUpperLetters
		switch {
		case two && lower:
			need = 4 // "Ab C"
		case two:
			need = 3 // "A B"
		case lower:
			need = 2 // "Ab"
		}
		switch {
		case e > 0 && interspersed:
			need += 2 * e
		case e > 0:
			need += e + 1
		}
	} else {
		if e > 0 && interspersed {
			need = 2
		}
		need += e
	}
	return need
}

// ensure tops split[idx] up to need by taking units from the other lines,
// each of which keeps at least one. The caller guarantees the total allows
// it.
func ensure(split []int, idx, need int) {
	for j := range split {
		if split[idx] >= need {
			return
		}
		if j == idx {
			continue
		}
		take := min(need-split[idx], split[j]-1)
		split[j] -= take
		split[idx] += take
	}
}

// splitBlanks scatters the shape's blank lines over the lines-1 gaps between
// rendered lines, each blank into a gap drawn from the seed. Blank lines are
// only counted between non-blank lines, so lines >= 2 whenever there are any.
func (s shape) splitBlanks(rng *rand.Rand, lines int) []int {
	gaps := make([]int, max(lines-1, 0))
	for range s.blanks {
		gaps[rng.IntN(len(gaps))]++
	}
	return gaps
}

// splitUnits shares units across lines unevenly, the way real lines are,
// with at least one unit per line and every unit spent.
func (s shape) splitUnits(rng *rand.Rand, lines int) []int {
	if lines <= 1 {
		return []int{s.units}
	}
	weights := make([]int, lines)
	total := 0
	for i := range weights {
		weights[i] = 1 + rng.IntN(3)
		total += weights[i]
	}
	// Every line gets one unit, then the surplus is shared by weight. The
	// shares are rounded down, so the remainder is never negative and the
	// last line, which takes it, keeps its unit.
	out := make([]int, lines)
	surplus := s.units - lines
	spent := 0
	for i := range out {
		out[i] = 1 + surplus*weights[i]/total
		spent += out[i]
	}
	out[lines-1] += s.units - spent
	return out
}

// wordOpts shapes a line's words beyond its length.
type wordOpts struct {
	// atLeastTwo makes the first word leave room for a second whenever n
	// allows.
	atLeastTwo bool
	// longFirst makes the first word at least two letters whenever n
	// allows alongside atLeastTwo.
	longFirst bool
	// maxSpaces caps the spaces spent; once they are, the last word
	// absorbs the rest.
	maxSpaces int
}

// words draws lowercase words that, joined by single spaces, span exactly n
// units. A word is only started when at least one letter fits after the
// space, so no line starts or ends with one and none has two in a row.
func words(rng *rand.Rand, s *script, n int, o wordOpts) []string {
	var (
		out       []string
		remaining = n
	)
	for remaining > 0 {
		if len(out) > 0 {
			if remaining < 2 || len(out) > o.maxSpaces {
				// No room for a space and a letter, or no space to spend:
				// extend the last word.
				var b strings.Builder
				b.WriteString(out[len(out)-1])
				fillRunes(&b, rng, s.alphabet, remaining)
				out[len(out)-1] = b.String()
				return out
			}
			remaining-- // the space
		}
		length := min(drawWordLength(rng, s.wordLengths), remaining)
		if len(out) == 0 {
			// The second word needs a space and a letter after the first.
			room := n
			if o.atLeastTwo {
				room = n - 2
			}
			if o.longFirst && room >= 2 {
				length = max(length, 2)
			}
			if o.atLeastTwo && n >= 3 {
				length = min(length, room)
			}
		}
		var b strings.Builder
		fillRunes(&b, rng, s.alphabet, length)
		out = append(out, b.String())
		remaining -= length
	}
	return out
}

func fillRunes(b *strings.Builder, rng *rand.Rand, alphabet []rune, n int) {
	for range n {
		b.WriteRune(pick(rng, alphabet))
	}
}

func pick(rng *rand.Rand, alphabet []rune) rune {
	return alphabet[rng.IntN(len(alphabet))]
}

// drawWordLength samples a length from the script's weighted distribution.
func drawWordLength(rng *rand.Rand, weights []int) int {
	total := 0
	for _, w := range weights {
		total += w
	}
	x := rng.IntN(total)
	for i, w := range weights {
		if x < w {
			return i + 1
		}
		x -= w
	}
	return len(weights)
}

// newRNG derives a PCG generator from the seed. PCG's output is specified, so
// the same seed produces the same placeholder on every server and Go release.
func newRNG(seed []byte) *rand.Rand {
	h := sha256.New()
	h.Write([]byte(domain))
	h.Write([]byte{0})
	h.Write(seed)
	sum := h.Sum(nil)
	return rand.New(rand.NewPCG(
		binary.LittleEndian.Uint64(sum[:8]),
		binary.LittleEndian.Uint64(sum[8:16]),
	))
}
