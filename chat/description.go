package chat

import (
	"errors"
	"strings"
	"unicode"
	"unicode/utf8"
)

// MaxDescriptionLength is the most characters a group's description may run
// to, matching the proto's limit on Metadata.description so a stored
// description is always one the wire accepts.
const MaxDescriptionLength = 160

// ErrInvalidDescription is returned by ValidateDescription for a description
// a group may not carry.
var ErrInvalidDescription = errors.New("invalid description")

// ValidateDescription reports whether description is one a group may carry.
// The empty description is valid: it is how a group has none. Otherwise it
// must have something visible in it, run to at most MaxDescriptionLength
// characters, and carry no control characters beyond a line break — a
// description is shown as a few lines of text, like a user's bio (see
// profile.ValidateBio, whose rules these are), and anything else in it would
// render as nothing or as noise. Returns ErrInvalidDescription otherwise.
//
// Nothing is normalized: the description is stored exactly as written, line
// breaks and surrounding whitespace included.
func ValidateDescription(description string) error {
	if description == "" {
		return nil
	}
	if utf8.RuneCountInString(description) > MaxDescriptionLength {
		return ErrInvalidDescription
	}
	if strings.TrimSpace(description) == "" {
		return ErrInvalidDescription
	}
	for _, r := range description {
		if r == '\n' {
			continue
		}
		if unicode.IsControl(r) {
			return ErrInvalidDescription
		}
	}
	return nil
}
