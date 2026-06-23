package messaging

import (
	"errors"

	"github.com/forPelevin/gomoji"
)

// ErrInvalidEmoji indicates a reaction value that is not a single emoji.
var ErrInvalidEmoji = errors.New("value is not a single emoji")

// ValidateEmoji enforces that a reaction value is exactly one emoji grapheme and
// nothing else. The protobuf layer only bounds the value's size (code points and
// bytes); true emoji validity is enforced here, against the Unicode emoji set, so
// arbitrary text can't be smuggled in as a reaction.
func ValidateEmoji(value string) error {
	emojis := gomoji.CollectAll(value)
	// Exactly one emoji, and the emoji spans the entire value (no leading or
	// trailing characters, whitespace, or a second grapheme).
	if len(emojis) != 1 || emojis[0].Character != value {
		return ErrInvalidEmoji
	}
	return nil
}
