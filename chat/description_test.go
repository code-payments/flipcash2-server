package chat

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestValidateDescription(t *testing.T) {
	for _, valid := range []string{
		"",  // none
		"a", // the shortest
		strings.Repeat("x", MaxDescriptionLength), // the longest
		strings.Repeat("é", MaxDescriptionLength), // characters, not bytes
		" padded ",                            // kept as written
		"two\nlines",                          // a line break is the one control character allowed
		"👨‍👩‍👧",                               // a joiner is a format character, not a control one
		"https://example.com — anything else", // free text
	} {
		require.NoError(t, ValidateDescription(valid), "description: %q", valid)
	}

	for _, invalid := range []string{
		" ", "   ", "\t\n", " ", "　", "\n\n", // nothing visible
		strings.Repeat("x", MaxDescriptionLength+1),
		strings.Repeat("é", MaxDescriptionLength+1), // characters, not bytes
		"tab\tin it", "nul\x00", "\r\n", "\x7f", // control characters
	} {
		require.ErrorIs(t, ValidateDescription(invalid), ErrInvalidDescription, "description: %q", invalid)
	}
}
