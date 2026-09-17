package redact

import (
	"fmt"
	"os"
	"strings"
	"testing"
	"unicode"

	"github.com/stretchr/testify/assert"
	"golang.org/x/text/width"
)

// conversation is a plausible group chat that touches every branch of the
// transform: lowercase and sentence case, shouting and title case, one-word
// replies, multi-sentence lines, multi-line messages past the line cap, a
// paragraph past the length cap, a tall list of short lines, blank lines
// and a run of them, URLs,
// prices and times, camel case, an
// ellipsis, emoji at every placement, emoji-only messages in several hues,
// skin tones and ZWJ sequences. Each message is seeded like production —
// by its own ID — so neighbouring bubbles differ.
var conversation = []message{
	{"Bob", "hey is anyone around this weekend?"},
	{"Alice", "Yes! What did you have in mind?"},
	{"Bob", "Thinking we finally do the Big Sur Hike. Weather looks perfect Saturday."},
	{"Charlie", "ok"},
	{"Charlie", "wait which trail"},
	{"Bob", "https://www.alltrails.com/trail/us/california/ewoldsen-trail"},
	{"Alice", "Ewoldsen Trail is closed since the slide… Try Andrew Molera instead"},
	{"Bob", "Ah good catch. Andrew Molera it is."},
	{"Charlie", "how long is it"},
	{"Alice", "About 8 miles, 1000 ft of gain. 4 hours if we stop for photos."},
	{"Charlie", "4 HOURS"},
	{"Charlie", "I THOUGHT THIS WAS A CASUAL WALK"},
	{"Bob", "lol"},
	{"Bob", "you'll be fine 💪"},
	{"Alice", "🙌🙌🙌"},
	{"Charlie", "fine. What time?"},
	{"Bob", "Plan:\n\nLeave SF at 6:30am\nBreakfast in Monterey around 8:30\n\n\nTrailhead by 10\nBack in the city for dinner"},
	{"Alice", "Perfect, I can drive. My car fits five."},
	{"Charlie", "🚗 shotgun"},
	{"Bob", "Parking is $10 cash at the lot, bring small bills."},
	{"Alice", "Can someone bring the good snacks this time and not just Bob's sad granola bars"},
	{"Bob", "The granola bars were fine"},
	{"Charlie", "they were not fine 💀"},
	{"Alice", "😂😂😂"},
	{"Bob", "ok ok I'll do a proper Trader Joe's run. Chips, fruit, those peanut butter pretzels everyone likes, sparkling water, and something for Charlie's delicate palate."},
	{"Charlie", "❤️"},
	{"Bob", "Shopping list:\nchips\nfruit\npretzels\nsparkling water\ngranola bars\nsunscreen\nice\nnapkins"},
	{"Charlie", "also can I bring Jordan? they just moved here and don't know anyone yet"},
	{"Bob", "Of course! The more the merrier"},
	{"Alice", "Absolutely 👍🏽"},
	{"Charlie", "🎉🎉🎉🎉🎉 they said yes"},
	{"Alice", "Quick reminder for everyone since it's the coast: it is going to be cold in the morning and hot by noon. Layers, sunscreen, a hat, and at least two litres of water each. The trail has almost no shade after the first mile and the creek crossing means shoes you don't mind getting wet. Also phone signal drops out completely past the parking lot so download the map beforehand and tell someone at home where we're going. I sound like a mum but last time two of you got sunburnt and one of you got lost."},
	{"Bob", "yes mum"},
	{"Charlie", "😌 love you mum"},
	{"Alice", "🙄"},
	{"Bob", "iPhone maps or Google Maps for the download? I never remember which one works offline there"},
	{"Alice", "Either. AllTrails offline is best though, worth the 3 day trial"},
	{"Charlie", "😴 6:30 is so early"},
	{"Bob", "Coffee is on me for whoever is in the car by 6:30 ☕"},
	{"Charlie", "deal"},
	{"Alice", "See you all Saturday!! 🌊🌸🍀"},
	{"Bob", "👨‍👩‍👧‍👦 the whole family is coming"},
	{"Charlie", "wait what"},
	{"Charlie", "guys" + strings.Repeat("\n", 14) + "sorry the cat walked on my phone"},
	{"Bob", "kidding 😂 just me"},
	{"Alice", "🍎🍎 🍊🍊 🍇🍇"},
	{"Charlie", "🔴🔴🔴 🟢🟢🟢 ✅"},
	{"Bob", "🤔"},
	{"Alice", "sorry testing something. carry on"},
}

type message struct {
	sender string
	text   string
}

// TestConversation renders each conversation and prints every original
// beside its placeholder, so a reviewer can eyeball the texture with
// `go test -v -run TestConversation ./redact/`. It also asserts the two
// invariants every message must satisfy: the placeholder is its own
// placeholder, and it spends exactly its shape's units.
func TestConversation(t *testing.T) {
	t.Run("hike", func(t *testing.T) { renderConversation(t, conversation) })
}

func renderConversation(t *testing.T, conversation []message) {
	w := os.Stdout
	fmt.Fprintf(w, "\n%-8s  %-44s  %-44s\n", "from", "original", "redacted")
	fmt.Fprintf(w, "%s\n", strings.Repeat("-", 100))

	for i, m := range conversation {
		seed := fmt.Appendf(nil, "msg-%03d", i)
		out := Text(seed, m.text)
		s := shapeOf(m.text)

		assert.Equalf(t, out, Text(seed, out), "message %d is not idempotent: %q -> %q", i, m.text, out)
		lines := strings.Count(out, "\n") + 1
		assert.Equalf(t, s.units, countUnits(out)-(lines-1), "message %d spends the wrong number of units", i)

		printSideBySide(w, m.sender, m.text, out)
	}
}

// columns is the width of each side-by-side column in terminal cells.
const columns = 44

// printSideBySide prints a message's lines beside its placeholder's, wrapping
// long lines so the columns stay aligned.
func printSideBySide(w *os.File, sender, original, redacted string) {
	left := wrap(strings.Split(original, "\n"), columns)
	right := wrap(strings.Split(redacted, "\n"), columns)
	for i := 0; i < max(len(left), len(right)); i++ {
		var l, r, from string
		if i < len(left) {
			l = left[i]
		}
		if i < len(right) {
			r = right[i]
		}
		if i == 0 {
			from = sender
		}
		fmt.Fprintf(w, "%s  %s  %s\n", pad(from, 8), pad(l, columns), pad(r, columns))
	}
}

// pad right-pads s to n terminal cells. fmt's %-44s pads by byte count, which
// drifts on every emoji and every non-ASCII letter.
func pad(s string, n int) string {
	return s + strings.Repeat(" ", max(0, n-cellWidth(s)))
}

// cellWidth approximates how many terminal cells s occupies: modifiers and
// ZWJ-joined runes take none, emoji and East Asian wide characters take two,
// everything else one.
func cellWidth(s string) int {
	n := 0
	joined := false
	for _, r := range s {
		if modifier(r) {
			joined = r == 0x200D
			continue
		}
		if joined {
			joined = false
			continue
		}
		n += runeWidth(r)
	}
	return n
}

func runeWidth(r rune) int {
	if unicode.Is(emoji, r) {
		return 2
	}
	switch width.LookupRune(r).Kind() {
	case width.EastAsianWide, width.EastAsianFullwidth:
		return 2
	}
	return 1
}

// wrap breaks each line at n terminal cells, preferring a space.
func wrap(lines []string, n int) []string {
	var out []string
	for _, line := range lines {
		rs := []rune(line)
		for cellWidth(string(rs)) > n {
			// Index of the first rune that would overflow.
			cut, w := 0, 0
			for cut < len(rs) && w+runeWidth(rs[cut]) <= n {
				w += runeWidth(rs[cut])
				cut++
			}
			for j := cut; j > cut/2; j-- {
				if rs[j] == ' ' {
					cut = j
					break
				}
			}
			out = append(out, string(rs[:cut]))
			rs = rs[cut:]
			for len(rs) > 0 && rs[0] == ' ' {
				rs = rs[1:]
			}
		}
		out = append(out, string(rs))
	}
	return out
}
