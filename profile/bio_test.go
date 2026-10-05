package profile

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestValidateBio(t *testing.T) {
	for _, valid := range []string{
		"",                                    // clears
		"a",                                   // the shortest
		strings.Repeat("x", MaxBioLength),     // the longest
		strings.Repeat("é", MaxBioLength),     // characters, not bytes
		" padded ",                            // kept as written
		"two\nlines",                          // a line break is the one control character allowed
		"👨‍👩‍👧",                               // a joiner is a format character, not a control one
		"​zero-width",                         // likewise
		"https://example.com — anything else", // free text
	} {
		require.NoError(t, ValidateBio(valid), "bio: %q", valid)
	}

	for _, invalid := range []string{
		" ", "   ", "\t\n", " ", "　", "\n\n", // nothing visible
		strings.Repeat("x", MaxBioLength+1),
		strings.Repeat("é", MaxBioLength+1),     // characters, not bytes
		"tab\tin it", "nul\x00", "\r\n", "\x7f", // control characters
	} {
		require.ErrorIs(t, ValidateBio(invalid), ErrInvalidBio, "bio: %q", invalid)
	}
}
