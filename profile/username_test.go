package profile

import (
	"context"
	"errors"
	"fmt"
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

func TestDefaultUsernameBase(t *testing.T) {
	for displayName, expected := range map[string]string{
		"Jeff Yanta":          "jeff_yanta",
		"jeff":                "jeff",
		"JEFF":                "jeff",
		"Agent 47":            "agent_47",
		"123":                 "123",
		"  Jeff Yanta  ":      "jeff_yanta",
		"Jeff  Yanta":         "jeff_yanta",
		" Jeff   Q  Yanta ":   "jeff_q_yanta",
		"Christopher Johnson": "christopher_johnson",
	} {
		base, ok := DefaultUsernameBase(displayName)
		require.True(t, ok, "display name: %q", displayName)
		require.Equal(t, expected, base, "display name: %q", displayName)
	}

	for _, displayName := range []string{
		"",
		"   ",
		"Jeff_Yanta",
		"Jeff-Yanta",
		"Jeff.",
		"Jeff\tYanta",
		"José",
		"Jeff 🚀",
		// Reserved, whole or as a substring.
		"Admin",
		"Support",
		"Flipcash Fan",
	} {
		_, ok := DefaultUsernameBase(displayName)
		require.False(t, ok, "display name: %q", displayName)
	}
}

func TestDefaultUsernameCandidate(t *testing.T) {
	for _, tc := range []struct {
		base     string
		n        int
		expected string
	}{
		{"jeff_yanta", 2, "jeff_yanta_2"},
		{"jeff_yanta", 99, "jeff_yanta_99"},
		{"jeff_yanta", 100, "jeff_yanta_100"},
		// The stem gives way to the number so the handle always fits.
		{"christopher_johnson", 2, "christopher_j_2"},
		{"christopher_johnson", 9, "christopher_j_9"},
		{"christopher_johnson", 11, "christopher_11"}, // the cut leaves a dangling underscore, dropped
		{"christopher_johnson", 100, "christopher_100"},
		{"abcdefghijklmnopqrstu", 2, "abcdefghijklm_2"},
		{"a", 2, "a_2"},
	} {
		candidate, ok := defaultUsernameCandidate(tc.base, tc.n)
		require.True(t, ok, "base: %q, n: %d", tc.base, tc.n)
		require.Equal(t, tc.expected, candidate, "base: %q, n: %d", tc.base, tc.n)
		require.NoError(t, ValidateUsername(candidate))
		require.LessOrEqual(t, len(candidate), maxUsernameLength)
	}

	// A cut that leaves nothing but underscores, or a reserved word, has no usable
	// handle at that number.
	_, ok := defaultUsernameCandidate("a_____________b", 2)
	require.True(t, ok)
	_, ok = defaultUsernameCandidate("_____________ab", 2)
	require.False(t, ok)
	_, ok = defaultUsernameCandidate("support_the_team", 10)
	require.True(t, ok)
	_, ok = defaultUsernameCandidate("support_the_team", 100000)
	require.False(t, ok) // "support_the_team" cuts to "support", which is reserved
}

func TestFindFreeDefaultUsernames(t *testing.T) {
	ctx := context.Background()

	heldSet := func(held ...string) (HeldUsernamesFunc, *int) {
		set := make(map[string]struct{}, len(held))
		for _, username := range held {
			set[username] = struct{}{}
		}
		var calls int
		return func(_ context.Context, usernames []string) (map[string]struct{}, error) {
			calls++
			result := make(map[string]struct{})
			for _, username := range usernames {
				if _, ok := set[username]; ok {
					result[username] = struct{}{}
				}
			}
			return result, nil
		}, &calls
	}

	t.Run("Starts at 2 even when the bare handle is free", func(t *testing.T) {
		held, calls := heldSet()
		free, err := findFreeDefaultUsernames(ctx, "jeff_yanta", held, 3)
		require.NoError(t, err)
		require.Equal(t, []string{"jeff_yanta_2", "jeff_yanta_3", "jeff_yanta_4"}, free)
		require.Equal(t, 1, *calls)
	})

	t.Run("Lowest free numbers win, including gaps", func(t *testing.T) {
		held, _ := heldSet("jeff_yanta", "jeff_yanta_2", "jeff_yanta_4", "jeff_yanta_6")
		free, err := findFreeDefaultUsernames(ctx, "jeff_yanta", held, 3)
		require.NoError(t, err)
		require.Equal(t, []string{"jeff_yanta_3", "jeff_yanta_5", "jeff_yanta_7"}, free)
	})

	t.Run("Returns what the first batch with any free has", func(t *testing.T) {
		// Only jeff_100 and jeff_101 are free in the first batch (2..101), so the
		// result is short rather than reaching into the next one.
		var taken []string
		for n := 2; n < 100; n++ {
			taken = append(taken, fmt.Sprintf("jeff_%d", n))
		}
		held, calls := heldSet(taken...)
		free, err := findFreeDefaultUsernames(ctx, "jeff", held, 10)
		require.NoError(t, err)
		require.Equal(t, []string{"jeff_100", "jeff_101"}, free)
		require.Equal(t, 1, *calls)
	})

	t.Run("Moves to the next batch when one is full", func(t *testing.T) {
		var taken []string
		for n := 2; n < 102; n++ {
			taken = append(taken, fmt.Sprintf("jeff_%d", n))
		}
		held, calls := heldSet(taken...)
		free, err := findFreeDefaultUsernames(ctx, "jeff", held, 1)
		require.NoError(t, err)
		require.Equal(t, []string{"jeff_102"}, free)
		require.Equal(t, 2, *calls)
	})

	t.Run("Covers exactly 2 through 1000", func(t *testing.T) {
		var asked []string
		held := func(_ context.Context, usernames []string) (map[string]struct{}, error) {
			asked = append(asked, usernames...)
			result := make(map[string]struct{}, len(usernames))
			for _, username := range usernames {
				result[username] = struct{}{}
			}
			return result, nil
		}
		_, err := findFreeDefaultUsernames(ctx, "jeff", held, 10)
		require.ErrorIs(t, err, ErrNoDefaultUsername)

		require.Len(t, asked, 999)
		require.Equal(t, "jeff_2", asked[0])
		require.Equal(t, "jeff_1000", asked[len(asked)-1])
	})

	t.Run("Asks for exactly the candidates each number yields", func(t *testing.T) {
		// The search works stems out once per digit count; what it asks for must
		// match working each number out on its own, cuts and reserved spellings
		// included.
		for _, base := range []string{"jeff", "christopher_johnson", "developers_x", "password_x", "newsletter_x", "5", "______________a"} {
			var expected []string
			for n := 2; n <= maxLowDefaultUsernameNumber; n++ {
				if candidate, ok := defaultUsernameCandidate(base, n); ok {
					expected = append(expected, candidate)
				}
			}

			var asked []string
			_, _ = findFreeDefaultUsernames(ctx, base, func(_ context.Context, usernames []string) (map[string]struct{}, error) {
				asked = append(asked, usernames...)
				result := make(map[string]struct{}, len(usernames))
				for _, username := range usernames {
					result[username] = struct{}{}
				}
				return result, nil
			}, 10)
			require.Equal(t, expected, asked, base)
		}
	})

	t.Run("Lookup error", func(t *testing.T) {
		expected := errors.New("boom")
		_, err := findFreeDefaultUsernames(ctx, "jeff", func(context.Context, []string) (map[string]struct{}, error) {
			return nil, expected
		}, 10)
		require.ErrorIs(t, err, expected)
	})
}

func TestAssignDefaultUsername(t *testing.T) {
	ctx := context.Background()

	noneHeld := func(context.Context, []string) (map[string]struct{}, error) {
		return map[string]struct{}{}, nil
	}
	allHeld := func(_ context.Context, usernames []string) (map[string]struct{}, error) {
		result := make(map[string]struct{}, len(usernames))
		for _, username := range usernames {
			result[username] = struct{}{}
		}
		return result, nil
	}

	// claimer records every claim and loses each one listed in taken to another
	// holder, the way a concurrent assignment that committed first would.
	claimer := func(taken ...string) (ClaimUsernameFunc, *[]string) {
		lost := make(map[string]struct{}, len(taken))
		for _, username := range taken {
			lost[username] = struct{}{}
		}
		var claims []string
		return func(_ context.Context, username string) (bool, error) {
			claims = append(claims, username)
			if _, ok := lost[username]; ok {
				return false, ErrUsernameTaken
			}
			return true, nil
		}, &claims
	}

	// draws returns a source of randomness that answers with each value in turn,
	// failing the test if a value is out of the range asked for or it runs out.
	draws := func(values ...int) func(n int) int {
		return func(n int) int {
			require.NotEmpty(t, values, "unexpected random draw")
			v := values[0]
			values = values[1:]
			require.True(t, v >= 0 && v < n, "draw %d out of [0, %d)", v, n)
			return v
		}
	}
	// number is the draw the random fallback turns into n.
	number := func(n int) int { return n - minRandomDefaultUsernameNumber }

	t.Run("Uncontended assignment claims the lowest number, with no randomness", func(t *testing.T) {
		claim, claims := claimer()
		username, err := assignDefaultUsername(ctx, "jeff", noneHeld, claim, draws())
		require.NoError(t, err)
		require.Equal(t, "jeff_2", username)
		require.Equal(t, []string{"jeff_2"}, *claims)
	})

	t.Run("A retry picks among the lowest free numbers", func(t *testing.T) {
		// jeff_2 is lost to a concurrent assignment; the retry draws index 3 of the
		// ten lowest free numbers (2..11 in the held view that has not caught up).
		claim, claims := claimer("jeff_2")
		username, err := assignDefaultUsername(ctx, "jeff", noneHeld, claim, draws(3))
		require.NoError(t, err)
		require.Equal(t, "jeff_5", username)
		require.Equal(t, []string{"jeff_2", "jeff_5"}, *claims)
	})

	t.Run("The spread is bounded by the free numbers there are", func(t *testing.T) {
		// Only jeff_100 and jeff_101 are free in the first batch, so the retry
		// draws from two.
		held := func(_ context.Context, usernames []string) (map[string]struct{}, error) {
			result := make(map[string]struct{})
			for _, username := range usernames {
				if username != "jeff_100" && username != "jeff_101" {
					result[username] = struct{}{}
				}
			}
			return result, nil
		}
		var asked []int
		claim, _ := claimer("jeff_100")
		username, err := assignDefaultUsername(ctx, "jeff", held, claim, func(n int) int {
			asked = append(asked, n)
			return 1
		})
		require.NoError(t, err)
		require.Equal(t, "jeff_101", username)
		require.Equal(t, []int{2}, asked)
	})

	t.Run("Falls back to a random number after losing the low numbers repeatedly", func(t *testing.T) {
		// Every low number the retries pick is lost.
		claim, claims := claimer("jeff_2", "jeff_3", "jeff_4", "jeff_5", "jeff_6")
		username, err := assignDefaultUsername(ctx, "jeff", noneHeld, claim, draws(1, 2, 3, 4, number(48213)))
		require.NoError(t, err)
		require.Equal(t, "jeff_48213", username)
		require.Equal(t, []string{"jeff_2", "jeff_3", "jeff_4", "jeff_5", "jeff_6", "jeff_48213"}, *claims)
	})

	t.Run("Falls back to a random number when every low number is held", func(t *testing.T) {
		claim, claims := claimer()
		username, err := assignDefaultUsername(ctx, "jeff", allHeld, claim, draws(number(77777)))
		require.NoError(t, err)
		require.Equal(t, "jeff_77777", username)
		require.Equal(t, []string{"jeff_77777"}, *claims)
	})

	t.Run("Random numbers that collide are drawn again", func(t *testing.T) {
		claim, _ := claimer("jeff_1111", "jeff_2222")
		username, err := assignDefaultUsername(ctx, "jeff", allHeld, claim, draws(number(1111), number(2222), number(3333)))
		require.NoError(t, err)
		require.Equal(t, "jeff_3333", username)
	})

	t.Run("Random numbers are cut to fit like any other", func(t *testing.T) {
		claim, _ := claimer()
		username, err := assignDefaultUsername(ctx, "christopher_johnson", allHeld, claim, draws(number(12345)))
		require.NoError(t, err)
		require.Equal(t, "christoph_12345", username)
	})

	t.Run("Random numbers span 1001 to 99999", func(t *testing.T) {
		for _, tc := range []struct {
			draw     int
			expected string
		}{
			{0, "jeff_1001"},
			{maxRandomDefaultUsernameNumber - minRandomDefaultUsernameNumber, "jeff_99999"},
		} {
			claim, _ := claimer()
			username, err := assignDefaultUsername(ctx, "jeff", allHeld, claim, draws(tc.draw))
			require.NoError(t, err)
			require.Equal(t, tc.expected, username)
		}
	})

	t.Run("Collisions all the way down leave no handle", func(t *testing.T) {
		claim, claims := claimer("jeff_2", "jeff_1001", "jeff_1002", "jeff_1003", "jeff_1004", "jeff_1005")
		_, err := assignDefaultUsername(ctx, "jeff", noneHeld, claim, draws(0, 0, 0, 0, number(1001), number(1002), number(1003), number(1004), number(1005)))
		require.ErrorIs(t, err, ErrNoDefaultUsername)
		require.Len(t, *claims, maxLowestDefaultUsernameAttempts+maxRandomDefaultUsernameAttempts)
	})

	t.Run("Losing the last number with no random fallback leaves no handle", func(t *testing.T) {
		// developers_x_99 is the only free number and has no random fallback
		// ("developers" and "developer" are both reserved). A concurrent assignment
		// commits it first, and the search that follows finds every number held:
		// the same outcome as arriving after that assignment, not an error.
		var lost bool
		held := func(_ context.Context, usernames []string) (map[string]struct{}, error) {
			result := make(map[string]struct{}, len(usernames))
			for _, username := range usernames {
				if username != "developers_x_99" || lost {
					result[username] = struct{}{}
				}
			}
			return result, nil
		}
		var claims []string
		claim := func(_ context.Context, username string) (bool, error) {
			claims = append(claims, username)
			lost = true
			return false, ErrUsernameTaken
		}
		_, err := assignDefaultUsername(ctx, "developers_x", held, claim, draws())
		require.ErrorIs(t, err, ErrNoDefaultUsername)
		require.Equal(t, []string{"developers_x_99"}, claims)
	})

	t.Run("No usable handle at any number", func(t *testing.T) {
		// Every cut of this base is underscores alone.
		claim, claims := claimer()
		_, err := assignDefaultUsername(ctx, "______________a", allHeld, claim, draws())
		require.ErrorIs(t, err, ErrNoDefaultUsername)
		require.Empty(t, *claims)
	})

	t.Run("Random numbers are drawn only from digit counts with a usable stem", func(t *testing.T) {
		// A four-digit number cuts "password_x" to nothing, but a five-digit one to
		// "password", which is reserved: every draw is four digits, and the draw is
		// asked for over exactly 1001-9999.
		for _, tc := range []struct {
			draw     int
			expected string
		}{
			{0, "password_x_1001"},
			{8998, "password_x_9999"},
		} {
			claim, _ := claimer()
			username, err := assignDefaultUsername(ctx, "password_x", allHeld, claim, draws(tc.draw))
			require.NoError(t, err)
			require.Equal(t, tc.expected, username)
		}

		// The other way round: "newsletter" is reserved, "newslette" is not.
		claim, _ := claimer()
		username, err := assignDefaultUsername(ctx, "newsletter_x", allHeld, claim, draws(0))
		require.NoError(t, err)
		require.Equal(t, "newslette_10000", username)
	})

	t.Run("No random number is drawn when no digit count has a usable stem", func(t *testing.T) {
		// "developers" and "developer" are both reserved.
		claim, claims := claimer()
		_, err := assignDefaultUsername(ctx, "developers_x", allHeld, claim, draws())
		require.ErrorIs(t, err, ErrNoDefaultUsername)
		require.Empty(t, *claims)
	})

	t.Run("A number that spells a reserved word with its stem is drawn past", func(t *testing.T) {
		// "5" and 4135 read as "sales".
		_, ok := defaultUsernameCandidate("5", 4135)
		require.False(t, ok)

		claim, claims := claimer()
		username, err := assignDefaultUsername(ctx, "5", allHeld, claim, draws(number(4135), number(4136)))
		require.NoError(t, err)
		require.Equal(t, "5_4136", username)
		require.Equal(t, []string{"5_4136"}, *claims)
	})

	t.Run("User already holds a handle", func(t *testing.T) {
		username, err := assignDefaultUsername(ctx, "jeff", noneHeld, func(context.Context, string) (bool, error) {
			return false, nil
		}, draws())
		require.NoError(t, err)
		require.Empty(t, username)
	})

	t.Run("Claim error", func(t *testing.T) {
		expected := errors.New("boom")
		_, err := assignDefaultUsername(ctx, "jeff", noneHeld, func(context.Context, string) (bool, error) {
			return false, expected
		}, draws())
		require.ErrorIs(t, err, expected)
	})
}

func TestRandomDefaultUsernameRanges(t *testing.T) {
	for base, expected := range map[string][]numberRange{
		"jeff":         {{1001, 9999}, {10000, 99999}},
		"password_x":   {{1001, 9999}},
		"newsletter_x": {{10000, 99999}},
		"developers_x": nil,
	} {
		require.Equal(t, expected, randomDefaultUsernameRanges(base), "base: %q", base)
	}

	ranges := []numberRange{{1001, 9999}, {10000, 99999}}
	require.Equal(t, 1001, nthNumber(ranges, 0))
	require.Equal(t, 9999, nthNumber(ranges, 8998))
	require.Equal(t, 10000, nthNumber(ranges, 8999))
	require.Equal(t, 99999, nthNumber(ranges, 98998))

	gapped := []numberRange{{1, 2}, {10, 11}}
	require.Equal(t, []int{1, 2, 10, 11}, []int{nthNumber(gapped, 0), nthNumber(gapped, 1), nthNumber(gapped, 2), nthNumber(gapped, 3)})
}
