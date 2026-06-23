package messaging

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestValidateEmoji(t *testing.T) {
	valid := []string{
		"👍",       // simple emoji
		"❤️",      // emoji with variation selector
		"🇺🇸",      // regional indicator pair (flag)
		"👨‍👩‍👧‍👦", // ZWJ sequence (family)
		"🏃‍♀️",    // ZWJ sequence with gender + variation selector
		"👍🏽",      // emoji with skin tone modifier
	}
	for _, value := range valid {
		require.NoError(t, ValidateEmoji(value), "expected %q to be valid", value)
	}

	invalid := []string{
		"",                     // empty
		"a",                    // plain text
		"hello",                // word
		"👍👍",                   // two emojis
		" 👍",                   // leading whitespace
		"👍 ",                   // trailing whitespace
		"👍a",                   // emoji followed by text
		"a👍",                   // text followed by emoji
		"5️⃣ ",                 // keycap emoji with trailing space
		"\U0001F600\U0001F600", // two emojis without separator
	}
	for _, value := range invalid {
		require.ErrorIs(t, ValidateEmoji(value), ErrInvalidEmoji, "expected %q to be invalid", value)
	}
}
