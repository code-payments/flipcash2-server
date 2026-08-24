package profile

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestIsUsernameReserved(t *testing.T) {
	// The word is reserved wherever it sits in the handle, not just as the whole
	// of it.
	for _, username := range []string{
		"flipcash",
		"flipcash_admin",
		"pay_flipcash",
		"xxflipcashxx",
		"usdf",
		"usdf_support",
	} {
		require.True(t, IsUsernameReserved(username), "username: %q", username)
	}

	// An exact word is reserved as itself.
	for _, username := range []string{
		"api", "app", "login", "me", "settings", "support",
		"seed", "seed_phrase", "seedphrase", "recovery", "claim", "report",
		"deleted", "404",
	} {
		require.True(t, IsUsernameReserved(username), "username: %q", username)
	}

	// A reserved word is reserved through the spellings a handle can hide it in:
	// spaced out with underscores, or with digits standing in for its letters.
	for _, username := range []string{
		"flip_cash",
		"f_l_i_p_c_a_s_h",
		"flipc4sh",
		"f1ipc45h",
		"u_s_d_f",
		"u5df",
		"s_u_p_p_o_r_t",
		"ad_min",
		"supp0rt",
		"4dmin",
		"s33d",
		"r3c0v3ry",
		"n0ne",
		"7357", // test
		"4ll",
	} {
		require.True(t, IsUsernameReserved(username), "username: %q", username)
	}

	// The same digit reads as more than one letter, and a handle is reserved on
	// any reading of it: the 1 in "f1ipcash" stands for an l, and in "fl1pcash"
	// for an i.
	for _, username := range []string{"f1ipcash", "fl1pcash", "f11pcash"} {
		require.True(t, IsUsernameReserved(username), "username: %q", username)
	}

	// An exact word is matched whole, so it does not take every handle spelled
	// around it with it. Only the platform's own name reaches inside a handle.
	for _, username := range []string{
		"apple",     // app
		"happy",     // app
		"rapid",     // api
		"meredith",  // me
		"supported", // support
		"teammate",  // team
		"cashew",    // cash
		"getaway",   // get
		"linked",    // link
		"reported",  // report
		"coined",    // coin
		"seedling",  // seed
		"newer",     // new
		"starting",  // start
	} {
		require.False(t, IsUsernameReserved(username), "username: %q", username)
	}

	// No two letters share a glyph class, so a word spelled in letters alone is
	// only ever caught by the word it actually is. These are every dictionary word
	// that a reserved word would have taken with it had l and i been read as one
	// another: "mall" is not "mail", however alike they can be made to look.
	for _, username := range []string{
		"mall", // mail
		"mali", // mail
		"ail",  // all
		"ami",  // aml
	} {
		require.False(t, IsUsernameReserved(username), "username: %q", username)
	}
}

// TestReservedSpellingsAreNotCapped guards the lists against an entry with so
// many ambiguous letters that its expansion is dropped for the cap — which would
// quietly leave it protected only as the one spelling it is written in.
func TestReservedSpellingsAreNotCapped(t *testing.T) {
	words := []string{}
	for word := range reservedExactWords {
		words = append(words, withoutUnderscores(word))
	}
	words = append(words, reservedUsernameSubstrings...)

	for _, word := range words {
		expected := 1
		for i := 0; i < len(word); i++ {
			if glyphs, ok := reservedGlyphClasses[word[i]]; ok {
				expected *= len(glyphs)
			}
		}

		require.LessOrEqual(t, expected, maxReservedSpellings, "reserved word: %q", word)
		require.Len(t, expandReservedSpellings(word), expected, "reserved word: %q", word)
	}
}

// TestReservedExactWordsAreClaimable guards the list against entries that could
// never have been claimed in the first place — a word with a hyphen or a dot, or
// one too long for a handle — which would sit there implying a protection it is
// not providing.
func TestReservedExactWordsAreClaimable(t *testing.T) {
	for word := range reservedExactWords {
		require.NoError(t, ValidateUsername(word), "reserved word: %q", word)
		require.Equal(t, NormalizeUsername(word), word, "reserved word: %q", word)
	}
}
