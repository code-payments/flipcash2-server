package redact

import "unicode"

// hue is the coarse colour class of an emoji: what is left of it under blur.
// A placeholder's emoji are drawn from the palette of the hue each stands
// for, the way a blurhash keeps an image's colours, so a row of blurred
// hearts still reads as red and a heart between two laughing faces reads as
// yellow, red, yellow.
//
// This is the one field of a shape that reflects content rather than layout.
// Hue is semantic for emoji in a way it is not for photos — red is almost
// always hearts or fire, yellow a face or a hand — so what a viewer learns is
// the emotional register of a message's emoji, about three bits each. Hues
// are carried in order, one per emoji the placeholder renders (see resample),
// which is a deliberate widening over a single hue per message: an ordered
// sequence identifies an emoji-only message far more closely than a colour
// does, and hues in order can spell something (red, white and blue is a
// flag). The count cap bounds it — eight hues for an emoji-only message,
// three sprinkled into text — and the placeholder still says nothing about
// which emoji of a hue was sent.
type hue uint8

// Classes are in tie-break order: a message split evenly takes the earlier.
const (
	hueYellow hue = iota
	hueRed
	hueOrange
	hueGreen
	hueBlue
	huePurple
	huePink
	// hueNeutral is white, grey, brown, black and the multicoloured, and the
	// class of any emoji the table does not know.
	hueNeutral

	hueCount
)

// palette is the filler pool per hue. Entries are fruit, plants, weather and
// shells: meaningless enough that a placeholder rendered unblurred reads as
// filler rather than as a reaction someone sent, and all Emoji 5 or older
// with default emoji presentation, so they render in colour on old devices
// without a variation selector (which the alphabet, a list of single runes,
// could not carry). Every entry must map back to its own hue in the table
// below, or redacting a placeholder would not be a no-op.
var palette = [hueCount][]rune{
	hueYellow:  []rune("🍋🌼🌽🍌"),
	hueRed:     []rune("🍎🍓🎈🍒"),
	hueOrange:  []rune("🍊🥕🍑"),
	hueGreen:   []rune("🍀🥝🌿🍐"),
	hueBlue:    []rune("🌊💧🐟"),
	huePurple:  []rune("🍇🔮"),
	huePink:    []rune("🌸🌷🌺"),
	hueNeutral: []rune("🌰🥔🥚🐚"),
}

// hueOf classifies one emoji. Specific characters are looked up first, then
// block-level defaults, then hueNeutral. Skin tone modifiers never reach here
// (see modifier), so a toned hand takes the hue of its base, and the sender's
// chosen tone does not show through the blur.
func hueOf(r rune) hue {
	if h, ok := hueOverrides[r]; ok {
		return h
	}
	for _, b := range hueBlocks {
		if r >= b.lo && r <= b.hi {
			return b.hue
		}
	}
	return hueNeutral
}

// emojiHues is the hue of each of text's displayed emoji in reading order: a
// heart on fire is one red emoji.
func emojiHues(text string) []hue {
	var out []hue
	for r := range displayed(text) {
		if unicode.Is(emoji, r) {
			out = append(out, hueOf(r))
		}
	}
	return out
}

// dominant is the hue most of hues share, the earlier class on a tie, and
// hueYellow for none.
func dominant(hues []hue) hue {
	var votes [hueCount]int
	for _, h := range hues {
		votes[h]++
	}
	best, bestVotes := hueYellow, 0
	for h, n := range votes {
		if n > bestVotes {
			best, bestVotes = hue(h), n
		}
	}
	return best
}

// resample fits n ordered hues to the k emoji a placeholder renders, keeping
// their order and rough proportions: rendered emoji i takes the dominant hue
// of its share of the originals. With k >= n every original is kept and some
// repeat; with k < n neighbours merge, so a long run keeps its colour and a
// lone emoji inside one may vanish. Resampling k hues to k is the identity,
// which is what makes a placeholder its own placeholder.
func resample(hues []hue, k int) []hue {
	n := len(hues)
	if n == 0 || k == 0 {
		return nil
	}
	out := make([]hue, k)
	for i := range out {
		lo := i * n / k
		hi := max(lo+1, (i+1)*n/k)
		out[i] = dominant(hues[lo:hi])
	}
	return out
}

type hueBlock struct {
	lo, hi rune
	hue    hue
}

// hueBlocks are defaults for whole runs of code points, consulted after
// hueOverrides. Order matters only where blocks overlap: the first match
// wins, so narrower runs come before wider ones.
var hueBlocks = []hueBlock{
	// Faces, cats and gesturing people: yellow skin by default.
	{0x1F600, 0x1F64F, hueYellow},  // Emoticons
	{0x1F910, 0x1F92F, hueYellow},  // Supplemental faces and hands
	{0x1F970, 0x1F97A, hueYellow},  // Smiling face with hearts through pleading
	{0x1F9D0, 0x1F9D2, hueYellow},  // Face with monocle, child, adult
	{0x1F440, 0x1F450, hueYellow},  // Hands and body parts (eyes overridden)
	{0x1F466, 0x1F487, hueYellow},  // People
	{0x1F4AA, 0x1F4AA, hueYellow},  // Flexed biceps
	{0x1F930, 0x1F93E, hueYellow},  // Pregnant woman through handball
	{0x1F1E6, 0x1F1FF, hueNeutral}, // Regional indicators: flags are multicoloured
	{0x1F550, 0x1F567, hueBlue},    // Clock faces
	{0x1F170, 0x1F19A, hueBlue},    // Squared Latin letters and abbreviations (some overridden red)
	{0x1F201, 0x1F251, hueOrange},  // Squared and circled ideographs
	{0x1F400, 0x1F43F, hueNeutral}, // Animals default to fur
	{0x1F680, 0x1F6FF, hueNeutral}, // Transport and map symbols
	{0x1F4F0, 0x1F4FD, hueNeutral}, // Devices
	{0x1F3A0, 0x1F3FF, hueNeutral}, // Activities and sports (several overridden)
}

// hueOverrides pins the common emoji whose colour is not their block's.
var hueOverrides = map[rune]hue{
	// Hearts.
	0x2764:  hueRed,     // ❤
	0x2665:  hueRed,     // ♥
	0x1F494: hueRed,     // 💔
	0x1F9E1: hueOrange,  // 🧡
	0x1F49B: hueYellow,  // 💛
	0x1F49A: hueGreen,   // 💚
	0x1F499: hueBlue,    // 💙
	0x1F49C: huePurple,  // 💜
	0x1F5A4: hueNeutral, // 🖤
	0x1F90D: hueNeutral, // 🤍
	0x1F90E: hueNeutral, // 🤎
	0x1F493: huePink,    // 💓
	0x1F495: huePink,    // 💕
	0x1F496: huePink,    // 💖
	0x1F497: huePink,    // 💗
	0x1F498: huePink,    // 💘
	0x1F49D: huePink,    // 💝
	0x1F49E: huePink,    // 💞
	0x1F49F: huePink,    // 💟
	0x1F48B: hueRed,     // 💋
	0x1F48F: hueYellow,  // 💏
	0x1F491: hueYellow,  // 💑

	// Faces that are not yellow.
	0x1F621: hueRed,     // 😡
	0x1F92C: hueRed,     // 🤬
	0x1F975: hueRed,     // 🥵
	0x1F976: hueBlue,    // 🥶
	0x1F922: hueGreen,   // 🤢
	0x1F92E: hueGreen,   // 🤮
	0x1F608: huePurple,  // 😈
	0x1F47F: hueRed,     // 👿
	0x1F479: hueRed,     // 👹
	0x1F47A: hueRed,     // 👺
	0x1F921: hueRed,     // 🤡
	0x1F480: hueNeutral, // 💀
	0x1F47B: hueNeutral, // 👻
	0x1F47D: hueNeutral, // 👽
	0x1F916: hueNeutral, // 🤖
	0x1F4A9: hueNeutral, // 💩
	0x1F648: hueNeutral, // 🙈
	0x1F649: hueNeutral, // 🙉
	0x1F64A: hueNeutral, // 🙊
	0x1F440: hueNeutral, // 👀
	0x1F444: hueRed,     // 👄
	0x1F445: huePink,    // 👅
	0x1F9E0: huePink,    // 🧠

	// Symbols and marks.
	0x2705:  hueGreen,   // ✅
	0x2714:  hueNeutral, // ✔
	0x274C:  hueRed,     // ❌
	0x274E:  hueGreen,   // ❎
	0x2B55:  hueRed,     // ⭕
	0x2757:  hueRed,     // ❗
	0x2753:  hueRed,     // ❓
	0x2755:  hueNeutral, // ❕
	0x2754:  hueNeutral, // ❔
	0x203C:  hueRed,     // ‼
	0x2049:  hueRed,     // ⁉
	0x1F4AF: hueRed,     // 💯
	0x1F525: hueOrange,  // 🔥
	0x2728:  hueYellow,  // ✨
	0x2B50:  hueYellow,  // ⭐
	0x1F31F: hueYellow,  // 🌟
	0x1F4AB: hueYellow,  // 💫
	0x26A1:  hueYellow,  // ⚡
	0x1F4A5: hueOrange,  // 💥
	0x1F4A2: hueRed,     // 💢
	0x1F4A4: hueBlue,    // 💤
	0x1F4A6: hueBlue,    // 💦
	0x1F4A7: hueBlue,    // 💧
	0x1F4A8: hueNeutral, // 💨
	0x1F4AC: hueNeutral, // 💬
	0x1F4AD: hueNeutral, // 💭
	0x1F6A8: hueRed,     // 🚨
	0x1F6AB: hueRed,     // 🚫
	0x1F6A9: hueRed,     // 🚩
	0x1F6D1: hueRed,     // 🛑
	0x26D4:  hueRed,     // ⛔
	0x26A0:  hueYellow,  // ⚠
	0x1F51E: hueRed,     // 🔞
	0x1F4F5: hueRed,     // 📵
	0x1F534: hueRed,     // 🔴
	0x1F7E0: hueOrange,  // 🟠
	0x1F7E1: hueYellow,  // 🟡
	0x1F7E2: hueGreen,   // 🟢
	0x1F535: hueBlue,    // 🔵
	0x1F7E3: huePurple,  // 🟣
	0x1F7E5: hueRed,     // 🟥
	0x1F7E7: hueOrange,  // 🟧
	0x1F7E8: hueYellow,  // 🟨
	0x1F7E9: hueGreen,   // 🟩
	0x1F7E6: hueBlue,    // 🟦
	0x1F7EA: huePurple,  // 🟪
	0x1F536: hueOrange,  // 🔶
	0x1F538: hueOrange,  // 🔸
	0x1F537: hueBlue,    // 🔷
	0x1F539: hueBlue,    // 🔹
	0x1F4A0: hueBlue,    // 💠
	0x1F52E: huePurple,  // 🔮
	0x1F53A: hueRed,     // 🔺
	0x1F53B: hueRed,     // 🔻
	0x1F18E: hueRed,     // 🆎
	0x1F191: hueRed,     // 🆑
	0x1F198: hueRed,     // 🆘
	0x1F19A: hueOrange,  // 🆚
	0x1F4B0: hueYellow,  // 💰
	0x1F4B5: hueGreen,   // 💵
	0x1F4B8: hueGreen,   // 💸
	0x1F4B3: hueBlue,    // 💳
	0x1F48E: hueBlue,    // 💎
	0x1F451: hueYellow,  // 👑
	0x1F514: hueYellow,  // 🔔
	0x1F512: hueYellow,  // 🔒
	0x1F513: hueYellow,  // 🔓
	0x1F511: hueYellow,  // 🔑
	0x1F4A1: hueYellow,  // 💡
	0x1F4CC: hueRed,     // 📌
	0x1F4CD: hueRed,     // 📍
	0x1F3C6: hueYellow,  // 🏆
	0x1F947: hueYellow,  // 🥇
	0x1F3AF: hueRed,     // 🎯
	0x1F3C0: hueOrange,  // 🏀
	0x1F3BE: hueGreen,   // 🎾
	0x1F389: hueOrange,  // 🎉
	0x1F388: hueRed,     // 🎈
	0x1F381: hueRed,     // 🎁
	0x1F382: huePink,    // 🎂
	0x1F384: hueGreen,   // 🎄
	0x1F383: hueOrange,  // 🎃
	0x1F680: hueNeutral, // 🚀
	0x1F697: hueRed,     // 🚗
	0x1F4DA: hueNeutral, // 📚
	0x1F4DD: hueNeutral, // 📝
	0x1F64C: hueYellow,  // 🙌
	0x1F64F: hueYellow,  // 🙏

	// Weather and sky.
	0x2600:  hueYellow,  // ☀
	0x1F31E: hueYellow,  // 🌞
	0x1F319: hueYellow,  // 🌙
	0x1F31B: hueYellow,  // 🌛
	0x1F31C: hueYellow,  // 🌜
	0x1F31D: hueYellow,  // 🌝
	0x1F315: hueYellow,  // 🌕
	0x1F311: hueNeutral, // 🌑
	0x1F31A: hueNeutral, // 🌚
	0x1F308: hueNeutral, // 🌈
	0x2614:  hueBlue,    // ☔
	0x2744:  hueBlue,    // ❄
	0x26C4:  hueNeutral, // ⛄
	0x2601:  hueNeutral, // ☁
	0x1F300: hueBlue,    // 🌀
	0x1F30A: hueBlue,    // 🌊
	0x1F30D: hueBlue,    // 🌍
	0x1F30E: hueBlue,    // 🌎
	0x1F30F: hueBlue,    // 🌏
	0x1F30C: huePurple,  // 🌌
	0x1F305: hueOrange,  // 🌅
	0x1F307: hueOrange,  // 🌇
	0x1F30B: hueOrange,  // 🌋

	// Plants and flowers.
	0x1F331: hueGreen,  // 🌱
	0x1F332: hueGreen,  // 🌲
	0x1F333: hueGreen,  // 🌳
	0x1F334: hueGreen,  // 🌴
	0x1F335: hueGreen,  // 🌵
	0x1F33F: hueGreen,  // 🌿
	0x1F340: hueGreen,  // 🍀
	0x1F338: huePink,   // 🌸
	0x1F337: huePink,   // 🌷
	0x1F33A: huePink,   // 🌺
	0x1F490: huePink,   // 💐
	0x1F339: hueRed,    // 🌹
	0x1F33B: hueYellow, // 🌻
	0x1F33C: hueYellow, // 🌼
	0x1F33D: hueYellow, // 🌽

	// Food and drink.
	0x1F347: huePurple,  // 🍇
	0x1F346: huePurple,  // 🍆
	0x1F348: hueGreen,   // 🍈
	0x1F349: hueRed,     // 🍉
	0x1F34A: hueOrange,  // 🍊
	0x1F34B: hueYellow,  // 🍋
	0x1F34C: hueYellow,  // 🍌
	0x1F34D: hueYellow,  // 🍍
	0x1F34E: hueRed,     // 🍎
	0x1F34F: hueGreen,   // 🍏
	0x1F350: hueGreen,   // 🍐
	0x1F351: hueOrange,  // 🍑
	0x1F352: hueRed,     // 🍒
	0x1F353: hueRed,     // 🍓
	0x1F345: hueRed,     // 🍅
	0x1F344: hueRed,     // 🍄
	0x1F330: hueNeutral, // 🌰
	0x1F951: hueGreen,   // 🥑
	0x1F952: hueGreen,   // 🥒
	0x1F955: hueOrange,  // 🥕
	0x1F954: hueNeutral, // 🥔
	0x1F95D: hueGreen,   // 🥝
	0x1F95A: hueNeutral, // 🥚
	0x1F957: hueGreen,   // 🥗
	0x1F9C0: hueYellow,  // 🧀
	0x1F354: hueOrange,  // 🍔
	0x1F355: hueOrange,  // 🍕
	0x1F32D: hueOrange,  // 🌭
	0x1F32E: hueYellow,  // 🌮
	0x1F35F: hueYellow,  // 🍟
	0x1F37F: hueYellow,  // 🍿
	0x1F369: huePink,    // 🍩
	0x1F36A: hueOrange,  // 🍪
	0x1F370: hueYellow,  // 🍰
	0x1F36C: huePink,    // 🍬
	0x1F36D: huePink,    // 🍭
	0x1F36F: hueYellow,  // 🍯
	0x1F375: hueGreen,   // 🍵
	0x1F37A: hueYellow,  // 🍺
	0x1F37B: hueYellow,  // 🍻
	0x1F942: hueYellow,  // 🥂
	0x1F377: hueRed,     // 🍷
	0x1F37E: hueGreen,   // 🍾
	0x1F379: hueOrange,  // 🍹
	0x2615:  hueNeutral, // ☕

	// Animals whose colour is not fur.
	0x1F431: hueYellow,  // 🐱
	0x1F438: hueGreen,   // 🐸
	0x1F437: huePink,    // 🐷
	0x1F426: hueBlue,    // 🐦
	0x1F41D: hueYellow,  // 🐝
	0x1F41F: hueBlue,    // 🐟
	0x1F420: hueBlue,    // 🐠
	0x1F421: hueYellow,  // 🐡
	0x1F42C: hueBlue,    // 🐬
	0x1F433: hueBlue,    // 🐳
	0x1F40B: hueBlue,    // 🐋
	0x1F988: hueBlue,    // 🦈
	0x1F98B: hueBlue,    // 🦋
	0x1F422: hueGreen,   // 🐢
	0x1F40D: hueGreen,   // 🐍
	0x1F40A: hueGreen,   // 🐊
	0x1F419: hueOrange,  // 🐙
	0x1F980: hueRed,     // 🦀
	0x1F42F: hueOrange,  // 🐯
	0x1F981: hueOrange,  // 🦁
	0x1F98A: hueOrange,  // 🦊
	0x1F984: huePink,    // 🦄
	0x1F41A: hueNeutral, // 🐚
}
