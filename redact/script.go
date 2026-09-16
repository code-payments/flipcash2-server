package redact

import (
	"iter"
	"unicode"
)

// script is a visual class of writing system: what a reader can still tell
// apart once text is blurred. Exact scripts that look alike under blur share a
// class (Latin, Cyrillic and Greek are all "latin"), and the class carries
// everything rendering needs — a filler alphabet drawn from the script so the
// platform's own text layout shapes and directs it (Arabic filler joins into
// cursive and lays out right to left without a direction flag), whether words
// are space-separated, and how long its words tend to be.
//
// The order of scripts matters in one place: a message whose runes are split
// evenly between two classes takes the earlier one.
type script struct {
	name string

	// tables identifies the runes that vote for this class.
	tables []*unicode.RangeTable

	// alphabet is the filler drawn from. Letters repeat to weight the draw
	// towards the script's common shapes so the blurred texture reads as
	// prose rather than noise. Every entry must be a spacing character: no
	// combining marks, digits or punctuation, so filler is never something a
	// client's link or mention detector fires on.
	alphabet []rune

	// hued scripts draw from palette, indexed by the message's dominant
	// hue, instead of alphabet.
	hued bool

	// cased scripts have capital letters, so their shapes carry a casing.
	cased bool

	// wordLengths weights a word of length i+1 in a spaced script. Nil means
	// the script does not separate words with spaces.
	wordLengths []int

	// buckets are the ascending unit counts a message's length is rounded up
	// to; the last is the cap. Filler is generated at exactly the bucket, so
	// the bucket — not the true length — is all a viewer learns.
	buckets []int
}

func (s *script) spaced() bool {
	return s.wordLengths != nil
}

// textBuckets start with a four-unit bucket so the shortest replies stay
// short, then step by eight up to a line or two, then grow geometrically so a
// wall of text still renders as one: each doubling costs a viewer one more
// bit. The first bucket equals minUpperLetters, which is what lets an
// all-caps message of that size render without a space (see renderWords).
// The cap matches the messaging proto's 4096-character limit on text, so a
// placeholder is never longer than a real message could be.
var textBuckets = []int{4, 8, 16, 24, 32, 40, 48, 56, 64, 96, 128, 192, 256, 384, 512, 768, 1024, 1536, 2048, 3072, 4096}

// emojiBuckets are finer because a client renders one, two or a handful of
// emoji at a larger size than a run of them.
var emojiBuckets = []int{1, 2, 4, 8}

var (
	// englishWords is the length distribution of an average alphabetic
	// message, long enough in the tail to pass for German or Finnish.
	englishWords = []int{2, 8, 12, 12, 10, 8, 6, 4, 2, 1}
	// semiticWords runs shorter: abjads elide vowels.
	semiticWords = []int{1, 8, 14, 12, 8, 4, 2}
	// hangulWords count syllable blocks, which pack more per block.
	hangulWords = []int{4, 10, 8, 4, 1}
	// indicWords are consonant clusters with their own vowels attached.
	indicWords = []int{1, 6, 12, 10, 6, 3, 1}
)

// scripts is every class in tie-break order, latin first because it is also
// the fallback for a message with no script of its own (digits, punctuation).
var scripts = []*script{
	{
		name:        "latin",
		tables:      []*unicode.RangeTable{unicode.Latin, unicode.Cyrillic, unicode.Greek},
		alphabet:    []rune("aaaeeeeiiioooutttnnsrrhhldcmpgbfvwyk"),
		wordLengths: englishWords,
		buckets:     textBuckets,
		cased:       true,
	},
	{
		// The other right-to-left scripts ride along so their messages
		// keep their direction: Arabic filler is a poor likeness of Thaana
		// or N'Ko under blur, but a placeholder laid out left to right
		// would say more about the message than the wrong letter shapes.
		name: "arabic",
		tables: []*unicode.RangeTable{
			unicode.Arabic,
			unicode.Syriac, unicode.Thaana, unicode.Nko, unicode.Samaritan, unicode.Mandaic, unicode.Adlam,
		},
		alphabet:    []rune("اااللللمممننوويييتتبههررععسكدفقحج"),
		wordLengths: semiticWords,
		buckets:     textBuckets,
	},
	{
		name:        "hebrew",
		tables:      []*unicode.RangeTable{unicode.Hebrew},
		alphabet:    []rune("ייייווואאהההלללמממרררתתששבבננעכדק"),
		wordLengths: semiticWords,
		buckets:     textBuckets,
	},
	{
		name:     "han",
		tables:   []*unicode.RangeTable{unicode.Han},
		alphabet: []rune("的的一是不了人我在有他这中大来上国个到说们为子和你地出道也时年得就那要下以生会自着去之过家"),
		buckets:  textBuckets,
	},
	{
		name:   "japanese",
		tables: []*unicode.RangeTable{unicode.Hiragana, unicode.Katakana},
		// Kana only: a kanji in the filler would vote for han when the
		// placeholder is itself classified, and the blur cannot tell.
		alphabet: []rune("ののににはをたたがででててととししれさあるるいいうかこもんなっく"),
		buckets:  textBuckets,
	},
	{
		name:        "hangul",
		tables:      []*unicode.RangeTable{unicode.Hangul},
		alphabet:    []rune("이이다다는가가에하하고을를은지지의로한것서수도사람그나있어요"),
		wordLengths: hangulWords,
		buckets:     textBuckets,
	},
	{
		name:     "thai",
		tables:   []*unicode.RangeTable{unicode.Thai, unicode.Lao, unicode.Khmer, unicode.Myanmar},
		alphabet: []rune("กกรรนนมมาาาเเแอยวสลดทบปคชตหจขพงฉโ"),
		buckets:  textBuckets,
	},
	{
		name: "indic",
		tables: []*unicode.RangeTable{
			unicode.Devanagari, unicode.Bengali, unicode.Gurmukhi, unicode.Gujarati, unicode.Oriya,
			unicode.Tamil, unicode.Telugu, unicode.Kannada, unicode.Malayalam, unicode.Sinhala,
		},
		alphabet:    []rune("ककखगचजजटततदननपबममयररललवसससहअइउए"),
		wordLengths: indicWords,
		buckets:     textBuckets,
	},
	{
		// No alphabet: emoji filler comes from the palette of the message's
		// dominant hue (see hue).
		name:    "emoji",
		tables:  []*unicode.RangeTable{emoji},
		hued:    true,
		buckets: emojiBuckets,
	},
}

// alphabets lists every pool the script may draw from.
func (s *script) alphabets() [][]rune {
	if s.hued {
		return palette[:]
	}
	return [][]rune{s.alphabet}
}

// fallback classifies a message with no rune in any class.
var fallback = scripts[0]

// emojiScript classifies a message that is nothing but emoji.
var emojiScript = scripts[len(scripts)-1]

// emoji is the Emoji property, which the unicode package does not carry.
// Below the pictograph planes it is the exact list from emoji-data.txt, since
// the symbol blocks it is scattered through are full of text characters that
// Japanese and Korean messages use as punctuation (♡ ♪ ★ ✓): a message
// reading "ありがとう♡" carries no emoji, and "♡♡♡" is text, not three
// emoji-sized glyphs. Keyboards send the text-presentation members of the
// list (‼ ▶ ⬆ ™) with a variation selector, so they count as emoji here
// whether the selector survived or not. Left out are © ® and the keycap
// bases (# * 0-9), which are ordinary text without a selector. Above U+1F000
// whole blocks are taken: a stray chess piece classed as emoji costs
// nothing, and what matters is that a face, a flag or a hand is never
// mistaken for prose.
var emoji = &unicode.RangeTable{
	R16: []unicode.Range16{
		{Lo: 0x203C, Hi: 0x203C, Stride: 1}, // ‼
		{Lo: 0x2049, Hi: 0x2049, Stride: 1}, // ⁉
		{Lo: 0x2122, Hi: 0x2122, Stride: 1}, // ™
		{Lo: 0x2139, Hi: 0x2139, Stride: 1}, // ℹ
		{Lo: 0x2194, Hi: 0x2199, Stride: 1}, // ↔ through ↙
		{Lo: 0x21A9, Hi: 0x21AA, Stride: 1}, // ↩ ↪
		{Lo: 0x231A, Hi: 0x231B, Stride: 1}, // ⌚ ⌛
		{Lo: 0x2328, Hi: 0x2328, Stride: 1}, // ⌨
		{Lo: 0x23CF, Hi: 0x23CF, Stride: 1}, // ⏏
		{Lo: 0x23E9, Hi: 0x23F3, Stride: 1}, // ⏩ through ⏳
		{Lo: 0x23F8, Hi: 0x23FA, Stride: 1}, // ⏸ ⏹ ⏺
		{Lo: 0x24C2, Hi: 0x24C2, Stride: 1}, // Ⓜ
		{Lo: 0x25AA, Hi: 0x25AB, Stride: 1}, // ▪ ▫
		{Lo: 0x25B6, Hi: 0x25B6, Stride: 1}, // ▶
		{Lo: 0x25C0, Hi: 0x25C0, Stride: 1}, // ◀
		{Lo: 0x25FB, Hi: 0x25FE, Stride: 1}, // ◻ ◼ ◽ ◾
		{Lo: 0x2600, Hi: 0x2604, Stride: 1}, // ☀ ☁ ☂ ☃ ☄
		{Lo: 0x260E, Hi: 0x260E, Stride: 1}, // ☎
		{Lo: 0x2611, Hi: 0x2611, Stride: 1}, // ☑
		{Lo: 0x2614, Hi: 0x2615, Stride: 1}, // ☔ ☕
		{Lo: 0x2618, Hi: 0x2618, Stride: 1}, // ☘
		{Lo: 0x261D, Hi: 0x261D, Stride: 1}, // ☝
		{Lo: 0x2620, Hi: 0x2620, Stride: 1}, // ☠
		{Lo: 0x2622, Hi: 0x2623, Stride: 1}, // ☢ ☣
		{Lo: 0x2626, Hi: 0x2626, Stride: 1}, // ☦
		{Lo: 0x262A, Hi: 0x262A, Stride: 1}, // ☪
		{Lo: 0x262E, Hi: 0x262F, Stride: 1}, // ☮ ☯
		{Lo: 0x2638, Hi: 0x263A, Stride: 1}, // ☸ ☹ ☺
		{Lo: 0x2640, Hi: 0x2640, Stride: 1}, // ♀
		{Lo: 0x2642, Hi: 0x2642, Stride: 1}, // ♂
		{Lo: 0x2648, Hi: 0x2653, Stride: 1}, // zodiac
		{Lo: 0x265F, Hi: 0x2660, Stride: 1}, // ♟ ♠
		{Lo: 0x2663, Hi: 0x2663, Stride: 1}, // ♣
		{Lo: 0x2665, Hi: 0x2666, Stride: 1}, // ♥ ♦
		{Lo: 0x2668, Hi: 0x2668, Stride: 1}, // ♨
		{Lo: 0x267B, Hi: 0x267B, Stride: 1}, // ♻
		{Lo: 0x267E, Hi: 0x267F, Stride: 1}, // ♾ ♿
		{Lo: 0x2692, Hi: 0x2697, Stride: 1}, // ⚒ through ⚗
		{Lo: 0x2699, Hi: 0x2699, Stride: 1}, // ⚙
		{Lo: 0x269B, Hi: 0x269C, Stride: 1}, // ⚛ ⚜
		{Lo: 0x26A0, Hi: 0x26A1, Stride: 1}, // ⚠ ⚡
		{Lo: 0x26A7, Hi: 0x26A7, Stride: 1}, // ⚧
		{Lo: 0x26AA, Hi: 0x26AB, Stride: 1}, // ⚪ ⚫
		{Lo: 0x26B0, Hi: 0x26B1, Stride: 1}, // ⚰ ⚱
		{Lo: 0x26BD, Hi: 0x26BE, Stride: 1}, // ⚽ ⚾
		{Lo: 0x26C4, Hi: 0x26C5, Stride: 1}, // ⛄ ⛅
		{Lo: 0x26C8, Hi: 0x26C8, Stride: 1}, // ⛈
		{Lo: 0x26CE, Hi: 0x26CF, Stride: 1}, // ⛎ ⛏
		{Lo: 0x26D1, Hi: 0x26D1, Stride: 1}, // ⛑
		{Lo: 0x26D3, Hi: 0x26D4, Stride: 1}, // ⛓ ⛔
		{Lo: 0x26E9, Hi: 0x26EA, Stride: 1}, // ⛩ ⛪
		{Lo: 0x26F0, Hi: 0x26F5, Stride: 1}, // ⛰ through ⛵
		{Lo: 0x26F7, Hi: 0x26FA, Stride: 1}, // ⛷ through ⛺
		{Lo: 0x26FD, Hi: 0x26FD, Stride: 1}, // ⛽
		{Lo: 0x2702, Hi: 0x2702, Stride: 1}, // ✂
		{Lo: 0x2705, Hi: 0x2705, Stride: 1}, // ✅
		{Lo: 0x2708, Hi: 0x270D, Stride: 1}, // ✈ through ✍
		{Lo: 0x270F, Hi: 0x270F, Stride: 1}, // ✏
		{Lo: 0x2712, Hi: 0x2712, Stride: 1}, // ✒
		{Lo: 0x2714, Hi: 0x2714, Stride: 1}, // ✔
		{Lo: 0x2716, Hi: 0x2716, Stride: 1}, // ✖
		{Lo: 0x271D, Hi: 0x271D, Stride: 1}, // ✝
		{Lo: 0x2721, Hi: 0x2721, Stride: 1}, // ✡
		{Lo: 0x2728, Hi: 0x2728, Stride: 1}, // ✨
		{Lo: 0x2733, Hi: 0x2734, Stride: 1}, // ✳ ✴
		{Lo: 0x2744, Hi: 0x2744, Stride: 1}, // ❄
		{Lo: 0x2747, Hi: 0x2747, Stride: 1}, // ❇
		{Lo: 0x274C, Hi: 0x274C, Stride: 1}, // ❌
		{Lo: 0x274E, Hi: 0x274E, Stride: 1}, // ❎
		{Lo: 0x2753, Hi: 0x2755, Stride: 1}, // ❓ ❔ ❕
		{Lo: 0x2757, Hi: 0x2757, Stride: 1}, // ❗
		{Lo: 0x2763, Hi: 0x2764, Stride: 1}, // ❣ ❤
		{Lo: 0x2795, Hi: 0x2797, Stride: 1}, // ➕ ➖ ➗
		{Lo: 0x27A1, Hi: 0x27A1, Stride: 1}, // ➡
		{Lo: 0x27B0, Hi: 0x27B0, Stride: 1}, // ➰
		{Lo: 0x27BF, Hi: 0x27BF, Stride: 1}, // ➿
		{Lo: 0x2934, Hi: 0x2935, Stride: 1}, // ⤴ ⤵
		{Lo: 0x2B05, Hi: 0x2B07, Stride: 1}, // ⬅ ⬆ ⬇
		{Lo: 0x2B1B, Hi: 0x2B1C, Stride: 1}, // ⬛ ⬜
		{Lo: 0x2B50, Hi: 0x2B50, Stride: 1}, // ⭐
		{Lo: 0x2B55, Hi: 0x2B55, Stride: 1}, // ⭕
		{Lo: 0x3030, Hi: 0x3030, Stride: 1}, // 〰
		{Lo: 0x303D, Hi: 0x303D, Stride: 1}, // 〽
		{Lo: 0x3297, Hi: 0x3297, Stride: 1}, // ㊗
		{Lo: 0x3299, Hi: 0x3299, Stride: 1}, // ㊙
	},
	R32: []unicode.Range32{
		{Lo: 0x1F000, Hi: 0x1FAFF, Stride: 1}, // Mahjong through Symbols and Pictographs Extended-A
	},
}

// modifier reports a rune that attaches to the one before it, or displays
// nothing of its own, and so should neither vote for a class nor count as a
// unit of length: combining marks (which include variation selectors), skin
// tones, and every format character — the joiner in a ZWJ emoji sequence,
// the non-joiner Persian writes inside words, the tag characters that spell
// subdivision flags, and the direction marks, isolates and zero width spaces
// that keyboards and pasted text slip in around anything. Without the last,
// an emoji-only message that arrived with a mark in front of it would render
// as text with an emoji beside it.
func modifier(r rune) bool {
	switch {
	case unicode.In(r, unicode.Mn, unicode.Me, unicode.Cf):
		return true
	case r >= 0x1F3FB && r <= 0x1F3FF:
		return true
	}
	return false
}

// displayed yields the runes of text that render as a character of their
// own, in order: everything modifier skips is dropped, and so is the rune a
// zero width joiner glues onto the one before it, so a ZWJ emoji sequence
// yields its first emoji only. Every walk over a message — counting units,
// voting for a script, placing emoji, classifying casing — goes through
// this, so they all agree on what the message contains. A shape field
// computed over a different set of runes would not survive a round trip
// through its own placeholder.
func displayed(text string) iter.Seq[rune] {
	return func(yield func(rune) bool) {
		joined := false
		for _, r := range text {
			if modifier(r) {
				joined = r == 0x200D
				continue
			}
			if joined {
				joined = false
				continue
			}
			if !yield(r) {
				return
			}
		}
	}
}

// classify returns the script most of text's displayed runes belong to, or
// fallback when none belong to any.
//
// Emoji are special: the emoji class is taken only when every displayed
// character is an emoji, because that is when a client renders the message
// at emoji size. Emoji in a message with anything else do not vote — the
// text decides the script and the emoji are sprinkled into it (see sprinkle).
func classify(text string) *script {
	votes := make([]int, len(scripts))
	emojiUnits, otherUnits := 0, 0
	for r := range displayed(text) {
		if unicode.IsSpace(r) {
			continue
		}
		if unicode.Is(emoji, r) {
			emojiUnits++
			continue
		}
		otherUnits++
		for i, s := range scripts {
			if inAny(r, s.tables) {
				votes[i]++
				break
			}
		}
	}
	if emojiUnits > 0 && otherUnits == 0 {
		return emojiScript
	}

	best, bestVotes := fallback, 0
	for i, s := range scripts {
		if votes[i] > bestVotes {
			best, bestVotes = s, votes[i]
		}
	}
	return best
}

func inAny(r rune, tables []*unicode.RangeTable) bool {
	for _, t := range tables {
		if unicode.Is(t, r) {
			return true
		}
	}
	return false
}
