package redact

import (
	"strings"
	"testing"
	"unicode"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var seed = []byte("01234567-89ab-cdef-0123-456789abcdef")

// fixtures cover one message per script class plus the classifier's edge
// cases, so every table-driven property below runs over each class.
var fixtures = []struct {
	name   string
	text   string
	script string
}{
	{"english", "Can you send me the address again?", "latin"},
	{"shouting", "WHERE ARE YOU", "latin"},
	{"title case", "Meet Sam At The Old Bank", "latin"},
	{"short title case", "Hi There", "latin"},
	{"lowercase", "on my way", "latin"},
	{"one long word", "congratulations", "latin"},
	{"one word with emoji", "thanks 👍", "latin"},
	{"trailing emoji", "great stuff 👍👍", "latin"},
	{"leading emoji", "🔥 new drop today", "latin"},
	{"interspersed emoji", "❤️ love this ❤️", "latin"},
	{"emoji heavy text", "👏👏👏👏👏 yes", "latin"},
	{"cjk with emoji", "你能再发一次地址吗👍", "han"},
	{"thai with emoji", "😂😂 ส่งที่อยู่ให้อีกครั้ง", "thai"},
	{"digit with emoji", "5 🔥", "latin"},
	{"german", "Könntest du mir bitte die Adresse noch einmal schicken?", "latin"},
	{"russian", "Можешь ещё раз прислать адрес?", "latin"},
	{"greek", "Μπορείς να μου στείλεις ξανά τη διεύθυνση;", "latin"},
	{"arabic", "هل يمكنك إرسال العنوان مرة أخرى؟", "arabic"},
	{"persian", "میشه دوباره آدرس رو بفرستی؟", "arabic"},
	{"hebrew", "אפשר לשלוח לי שוב את הכתובת?", "hebrew"},
	{"dhivehi", "އަހަރެން ހަމަ ދެން ބުނަން", "arabic"},
	{"syriac", "ܫܠܡܐ ܥܠܝܟܘܢ ܐܝܟܢܐ ܐܢܬܘܢ", "arabic"},
	{"nko", "ߒߞߏ ߞߊ߬ߙߊ߲ ߞߊ߬", "arabic"},
	{"chinese", "你能再发一次地址吗？", "han"},
	{"japanese", "もう一度住所を送ってもらえますか？", "japanese"},
	{"korean", "주소를 다시 보내 줄 수 있어?", "hangul"},
	{"thai", "ส่งที่อยู่ให้อีกครั้งได้ไหม", "thai"},
	{"hindi", "क्या आप पता फिर से भेज सकते हैं?", "indic"},
	{"emoji", "😂😂😂", "emoji"},
	{"emoji hearts", "❤️❤️❤️", "emoji"},
	{"emoji mixed hues", "✅ 🔥 💙 🍇 🌸 🇺🇸", "emoji"},
	{"emoji zwj family", "👨‍👩‍👧", "emoji"},
	{"digits only", "12345", "latin"},
	{"punctuation only", "?!?!", "latin"},
	{"url", "https://example.com/a/b?c=d", "latin"},
}

func scriptNamed(t *testing.T, name string) *script {
	t.Helper()
	for _, s := range scripts {
		if s.name == name {
			return s
		}
	}
	t.Fatalf("no script %q", name)
	return nil
}

func TestText_Deterministic(t *testing.T) {
	for _, f := range fixtures {
		t.Run(f.name, func(t *testing.T) {
			assert.Equal(t, Text(seed, f.text), Text(seed, f.text))
		})
	}

	// Different seeds give different filler for the same message, so a
	// message's placeholder is not a lookup key across chats or fixtures.
	long := "The quick brown fox jumps over the lazy dog and keeps on running."
	assert.NotEqual(t, Text(seed, long), Text([]byte("another"), long))
}

// The privacy property: the placeholder is a function of shape alone. Two
// messages that share a script, length bucket and line count are
// indistinguishable, whatever they say.
func TestText_DependsOnlyOnShape(t *testing.T) {
	pairs := [][2]string{
		{"hello world", "quick brown"},
		{"Hello world", "Quick brown"},
		{"CALL ME NOW", "WHERE ARE U"},
		{"New York Trip", "Big Sur Hike"},
		{"yes", "lol"},
		{"thanks", "hello"},
		{"congratulations", "unfortunately"},
		{"thanks 👍", "great 😂"},
		{"Thanks", "Hello"},
		{"a b", "1 2"},
		{"OK", "Hi"},
		{"iPhone", "laptop"},
		{"great stuff 👍👍", "happy friday 😂😂"},
		{"🔥 new drop today", "🍊 what a night"},
		{"❤️ love this ❤️", "so 💔 good 💔 wow"},
		{"meet me at the bank at noon sharp", "i think we should just cancel it."},
		{"你能再发一次地址吗", "我明天下午三点到家"},
		{"هل يمكنك إرسال العنوان", "سأصل بعد ساعة تقريبا"},
		{"😂😂", "👍👍"},
		{"❤️❤️", "🔴🔴"},
		{"❤️😂", "🔴👍"},
		{"❤️ love this ✅", "🍎 hate that 🍀"},
		{"a\nb", "1\n2"},
		{"a\n\n\n\nb", "x\n\n\n\n\ny"},
		{"abc\ncd", "yes\nno"},
		{"ok", "no"},
		{"A B", "Hi U"},
		{"ABCD", "WXYZ"},
		{"a\nb\nc\nd\ne", "1\n2\n3\n4\n5\n6"},
	}
	for _, p := range pairs {
		t.Run(p[0], func(t *testing.T) {
			require.Equal(t, shapeOf(p[0]), shapeOf(p[1]), "fixture pair must share a shape")
			assert.Equal(t, Text(seed, p[0]), Text(seed, p[1]))
		})
	}
}

// A placeholder is its own placeholder: redacting it again changes nothing,
// which shows it carries exactly the shape and nothing the classifier or
// length bucket reads differently.
func TestText_Idempotent(t *testing.T) {
	for _, f := range fixtures {
		t.Run(f.name, func(t *testing.T) {
			once := Text(seed, f.text)
			assert.Equal(t, once, Text(seed, once))
		})
	}
}

func TestText_Script(t *testing.T) {
	for _, f := range fixtures {
		t.Run(f.name, func(t *testing.T) {
			s := scriptNamed(t, f.script)
			assert.Same(t, s, classify(f.text))

			out := Text(seed, f.text)
			allowed := make(map[rune]bool)
			for _, alphabet := range s.alphabets() {
				for _, r := range alphabet {
					allowed[r] = true
				}
			}
			// Any script may carry sprinkled emoji from the palettes.
			for _, pool := range palette {
				for _, r := range pool {
					allowed[r] = true
				}
			}
			for _, r := range out {
				if r == ' ' || r == '\n' {
					continue
				}
				// Casing may capitalize filler; the letters are the same.
				assert.Truef(t, allowed[unicode.ToLower(r)], "%q is not in the %s filler alphabet", r, s.name)
			}

			if s.spaced() {
				// A line longer than the longest word must contain a space;
				// a shorter one, or a solid message, may legitimately be a
				// single word.
				if sh := shapeOf(f.text); sh.units > len(s.wordLengths) && !sh.solid {
					assert.Contains(t, out, " ", "spaced scripts separate words")
				}
			} else {
				assert.NotContains(t, out, " ", "unspaced scripts have no word breaks")
			}
		})
	}
}

// The classifier votes by displayed character, so a message keeps the script
// of most of its text; emoji never vote unless they are all there is.
func TestText_MajorityScript(t *testing.T) {
	cases := map[string]string{
		"great stuff 👍👍":               "latin",
		"👍":                            "emoji",
		"ok 👍":                         "latin",
		"👏👏👏👏👏 yes":                    "latin",
		"5 🔥":                          "latin",
		"😂 😂\n😂":                       "emoji",
		"今日は天気がいいですね":                  "japanese",
		"天気予報":                         "han",
		"Meet at 5 — bring $20 (cash)": "latin",
		"محمد said hi":                 "latin",
		"محمد قال مرحبا hi":            "arabic",
		"\u200f😂😂":                     "emoji",
		"\u200b👍":                      "emoji",
		"\ufeff👍":                      "emoji",
		"\u2067👍\u2069":                "emoji",
		"⏰":                            "emoji",
		"1\ufe0f\u20e3":                "latin",
		"♡♡♡":                          "latin",
		"ありがとう♪":                       "japanese",
	}
	for text, want := range cases {
		assert.Equal(t, want, classify(text).name, text)
	}
}

func TestText_LengthBuckets(t *testing.T) {
	cases := []struct {
		length int
		want   int
	}{
		{1, 4}, {4, 4}, {5, 8}, {7, 8}, {8, 8}, {9, 16}, {16, 16}, {17, 24}, {64, 64}, {65, 96},
		{96, 96}, {97, 128}, {200, 256}, {256, 256}, {257, 384}, {600, 768},
		{1025, 1536}, {4096, 4096}, {5000, 4096},
	}
	for _, c := range cases {
		text := strings.Repeat("a", c.length)
		out := Text(seed, text)
		assert.Equalf(t, c.want, countUnits(out), "length %d", c.length)
	}

	emoji := []struct {
		length int
		want   int
	}{{1, 1}, {2, 2}, {3, 4}, {4, 4}, {5, 8}, {8, 8}, {50, 8}}
	for _, c := range emoji {
		text := strings.Repeat("😂", c.length)
		out := Text(seed, text)
		assert.Equalf(t, c.want, countUnits(out), "emoji length %d", c.length)
	}
}

// displayed is the one definition of what a message contains; every walk
// over a message goes through it.
func TestDisplayed(t *testing.T) {
	collect := func(text string) string {
		var out []rune
		for r := range displayed(text) {
			out = append(out, r)
		}
		return string(out)
	}
	assert.Equal(t, "ab c", collect("a\u0301b\u200d\u2764\ufe0f c\u200f"), "marks, joined rune, selector and format characters drop")
	assert.Equal(t, "👍", collect("👍🏽"), "skin tone")
	assert.Equal(t, "👨", collect("👨\u200d👩\u200d👧"), "zwj sequence is its first emoji")
	assert.Equal(t, "🏴", collect("🏴\U000E0067\U000E0062\U000E0065\U000E006E\U000E0067\U000E007F"), "tag sequence")
	assert.Equal(t, "", collect("\u200d\u200dx"), "a joiner after a joiner still hides the next rune")
	n := 0
	for range displayed("abc") {
		n++
		break
	}
	assert.Equal(t, 1, n, "stops when the caller does")
}

// Modifiers ride along with the character before them and do not count.
func TestText_CountsDisplayedCharacters(t *testing.T) {
	assert.Equal(t, 1, countUnits("👍🏽"), "skin tone")
	assert.Equal(t, 1, countUnits("❤️"), "variation selector")
	assert.Equal(t, 1, countUnits("👨‍👩‍👧‍👦"), "zwj sequence")
	assert.Equal(t, 2, countUnits("🇺🇸"), "flags are two regional indicators; the bucket absorbs it")
	assert.Equal(t, 4, countUnits("नमस्ते"), "vowel signs and virama attach; the conjunct's consonants still count apart, and the bucket absorbs it")
	assert.Equal(t, 6, countUnits("héllo!"), "precomposed accent is one rune")
	assert.Equal(t, 6, countUnits("héllo!"), "combining accent is a modifier")
	assert.Equal(t, 11, countUnits("hello world"), "spaces count")
	assert.Equal(t, 7, countUnits("می\u200cخواهم"), "zero width non-joiner is a modifier")
	assert.Equal(t, 4, countUnits("\u200fשלום\u200f"), "direction marks are modifiers")
	assert.Equal(t, 5, countUnits("\u2067مرحبا\u2069"), "bidi isolates are modifiers")
	assert.Equal(t, 2, countUnits("\u200b😂\u200b😂"), "zero width spaces are modifiers")
	assert.Equal(t, 4, countUnits("co\u00adop"), "soft hyphen is a modifier")
}

// An emoji-only message is as long as its emoji count, whatever spacing the
// sender put between them: three emoji render large, three spaced emoji
// must too.
func TestText_EmojiOnlyLengthIgnoresSpaces(t *testing.T) {
	for _, text := range []string{"😂😂😂", "😂 😂 😂", "😂  😂  😂", " 😂 😂 😂 "} {
		s := shapeOf(text)
		assert.Same(t, emojiScript, s.script, text)
		assert.Equalf(t, 4, s.units, "%q", text)
		assert.Equal(t, Text(seed, "😂😂😂"), Text(seed, text), text)
	}
	assert.Equal(t, 1, shapeOf("😂").units)
	assert.Equal(t, 2, shapeOf("😂 😂").units)
	assert.Equal(t, 8, shapeOf(strings.Repeat("😂 ", 10)).units, "still capped")
	assert.Equal(t, 2, shapeOf("😂 😂 😂\n😂 😂 😂").lines, "line structure survives")
}

func TestText_Lines(t *testing.T) {
	cases := []struct {
		name   string
		text   string
		want   int
		blanks int
	}{
		{"one", "hello", 1, 0},
		{"two", "hello\nworld", 2, 0},
		{"three", "a\nb\nc", 3, 0},
		{"four", "a\nb\nc\nd", 4, 0},
		{"five rounds to six", "a\nb\nc\nd\ne", 6, 0},
		{"seven rounds to eight", "a\nb\nc\nd\ne\nf\ng", 8, 0},
		{"ten rounds to twelve", strings.Repeat("a\n", 10), 12, 0},
		{"capped", strings.Repeat("a\n", 60), 32, 0},
		{"one blank line", "hello\n\nworld", 2, 1},
		{"blank lines bucketed", "hello\n\n\n\nworld", 2, 4},
		{"blank lines summed across gaps", "a\n\nb\n\nc", 3, 2},
		{"blank lines capped", "hello" + strings.Repeat("\n", 30) + "world", 2, 8},
		{"whitespace-only lines are blank", "hello\n \t \nworld", 2, 1},
		{"leading and trailing blank lines ignored", "\n\nhello\nworld\n\n", 2, 0},
		{"windows newlines", "hello\r\nworld", 2, 0},
		{"emoji per line", "😂\n😂\n😂", 3, 0},
		{"emoji with blank lines", "😂\n\n😂", 2, 1},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			out := Text(seed, c.text)
			assert.Equal(t, c.want, shapeOf(c.text).lines)
			assert.Equal(t, c.blanks, shapeOf(c.text).blanks)
			lines := strings.Split(out, "\n")
			assert.Len(t, lines, c.want+c.blanks)
			// Blank lines sit between rendered lines, never at either end.
			assert.NotEmpty(t, lines[0])
			assert.NotEmpty(t, lines[len(lines)-1])
			blanks := 0
			for _, line := range lines {
				if line == "" {
					blanks++
					continue
				}
				assert.Equal(t, strings.TrimSpace(line), line, "no leading or trailing space")
				assert.NotContains(t, line, "  ", "no double spaces")
			}
			assert.Equal(t, c.blanks, blanks)
			// Units are spent exactly once across the lines.
			assert.Equal(t, shapeOf(c.text).units, countUnits(out)-(len(lines)-1))
		})
	}
}

func TestSolidOf(t *testing.T) {
	cases := map[string]bool{
		"thanks":          true,
		"  thanks\n":      true,
		"congratulations": true,
		"12345":           true,
		"thanks 👍":        true,
		"👍 thanks":        true,
		"on it":           false,
		"a\nb":            false,
		"so 😂 good":       false,
		"👍":               false,
		"":                false,
		"\u200d":          false,
	}
	for text, want := range cases {
		assert.Equalf(t, want, solidOf(text), "%q", text)
	}
}

// A one-word reply renders as one word, in every bucket up to solidUnits,
// beside any emoji at its edge. The bit is the shape's, so a message with a
// space in it is not one word, whatever the seed would have drawn.
func TestText_Solid(t *testing.T) {
	letters := func(out string) []string {
		var ws []string
		for _, w := range strings.Fields(out) {
			if e, _ := splitEmoji(w); len(e) == 0 {
				ws = append(ws, w)
			}
		}
		return ws
	}

	for _, text := range []string{"ok", "sure", "thanks", "perfect", "seriously", "congratulations", "THANKS", "Thanks", "thanks 👍", "👍 thanks", "안녕하세요", "12345"} {
		t.Run(text, func(t *testing.T) {
			s := shapeOf(text)
			assert.True(t, s.solid)
			out := Text(seed, text)
			assert.Len(t, letters(out), 1, out)
			assert.Equal(t, out, Text(seed, out), "idempotent")
		})
	}

	// Only the shortest words are one word above the cap; a long
	// unbroken run renders as prose so a link never reads as a link.
	for _, text := range []string{"https://example.com/abc", "@" + strings.Repeat("x", 20)} {
		s := shapeOf(text)
		assert.False(t, s.solid, text)
		assert.Greater(t, len(letters(Text(seed, text))), 1, text)
	}

	// A space in the text is a space in the shape. The placeholders may
	// still coincide: solid forbids a space, it does not force one.
	assert.False(t, shapeOf("on it").solid)
	assert.NotEqual(t, shapeOf("thanks"), shapeOf("on it"))
	assert.Equal(t, shapeOf("thanks").units, shapeOf("on it").units)

	// An interspersed emoji needs a word on each side, so the message is
	// not solid even without a space; an unspaced script never is.
	assert.False(t, shapeOf("wow🔥wow").solid)
	assert.Contains(t, Text(seed, "wow🔥wow"), " ")
	assert.False(t, shapeOf("你好世界").solid)
	assert.False(t, shapeOf("😂😂").solid)

	// An emoji on its own line makes a second line, and a rendered line
	// always carries a letter: the placeholder would not be one run, so
	// the shape is not solid and re-redaction agrees with itself.
	for _, text := range []string{"thanks\n👍", "👍👍\nthanks", "aaa❤️\n🇺", "👨\n00000000"} {
		assert.False(t, shapeOf(text).solid, text)
		out := Text(seed, text)
		assert.Equal(t, out, Text(seed, out), text)
	}
}

// A placeholder that renders unblurred must be inert: nothing a client would
// linkify, mention-parse or read as a number.
func TestText_Inert(t *testing.T) {
	for _, f := range fixtures {
		t.Run(f.name, func(t *testing.T) {
			out := Text(seed, f.text)
			for _, r := range out {
				if r == ' ' || r == '\n' {
					continue
				}
				assert.Falsef(t, unicode.IsDigit(r), "digit %q", r)
				assert.Falsef(t, unicode.IsPunct(r), "punctuation %q", r)
				assert.Falsef(t, unicode.IsSpace(r), "unexpected whitespace %q", r)
			}
		})
	}

	// Alphabets are the only source of runes, so check them directly too.
	for _, s := range scripts {
		for _, alphabet := range s.alphabets() {
			require.NotEmptyf(t, alphabet, "%s has an empty alphabet", s.name)
			for _, r := range alphabet {
				assert.Falsef(t, modifier(r), "%s alphabet holds combining rune %q", s.name, r)
				assert.Falsef(t, unicode.IsDigit(r) || unicode.IsPunct(r) || unicode.IsSpace(r), "%s alphabet holds %q", s.name, r)
				assert.Truef(t, inAny(r, s.tables), "%s alphabet rune %q would not classify as %s", s.name, r, s.name)
			}
		}
	}
}

func TestText_Empty(t *testing.T) {
	for _, text := range []string{"", " ", "\n", " \n\t\n ", "\u200d", "\u200f\u200b", "\u0301"} {
		assert.Empty(t, Text(seed, text))
	}
}

// A line that displays nothing is a blank line, so it never becomes a
// rendered line with no unit to put on it.
func TestText_UndisplayedLineIsBlank(t *testing.T) {
	assert.Equal(t, Text(seed, "♍"), Text(seed, "♍\n\n\u200d"))
	assert.Equal(t, Text(seed, "hi\n\nthere"), Text(seed, "hi\n\u200b\nthere"))
	assert.Equal(t, Text(seed, "hi\nthere"), Text(seed, "hi\nthere\n\u200f"))
	s := shapeOf("hi\n\u200b\nthere")
	assert.Equal(t, 2, s.lines)
	assert.Equal(t, 1, s.blanks)
}

func FuzzText(f *testing.F) {
	for _, fx := range fixtures {
		f.Add(fx.text)
	}
	f.Add("")
	f.Add("\n\n")
	f.Add("a‍b")
	f.Add(strings.Repeat("🇺🇸", 20))
	f.Add("😂 😂 😂")
	f.Add("\u200f😂😂")
	f.Add("می\u200cخواهم")
	f.Fuzz(func(t *testing.T, text string) {
		out := Text(seed, text)
		assert.Equal(t, out, Text(seed, text), "deterministic")

		s := shapeOf(text)
		if s.units == 0 {
			assert.Empty(t, out)
			return
		}
		lines := strings.Split(out, "\n")
		assert.Equal(t, min(s.lines, s.units)+s.blanks, len(lines))
		assert.Equal(t, s.units, countUnits(out)-(len(lines)-1), "units spent exactly")
		assert.NotEmpty(t, lines[0])
		assert.NotEmpty(t, lines[len(lines)-1])
		blanks := 0
		for _, line := range lines {
			if line == "" {
				blanks++
				continue
			}
			assert.Equal(t, strings.TrimSpace(line), line)
			assert.NotContains(t, line, "  ")
		}
		assert.Equal(t, s.blanks, blanks, "blank lines reproduced")
		assert.Equal(t, out, Text(seed, out), "idempotent")
	})
}
