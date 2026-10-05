package profile

import (
	"strings"
	"unicode"
	"unicode/utf8"
)

// MaxBioLength is the most characters a bio may run to, matching the proto's
// limit on UserProfile.bio so a stored bio is always one the wire accepts.
const MaxBioLength = 160

// ValidateBio reports whether bio is one a user may set. The empty bio is
// valid: it is how a bio is cleared. Otherwise it must have something visible
// in it, run to at most MaxBioLength characters, and carry no control
// characters beyond a line break — a bio is shown as a few lines of text, and
// anything else in it would render as nothing or as noise. Returns
// ErrInvalidBio otherwise.
//
// Nothing is normalized: the bio is stored exactly as the user wrote it, line
// breaks and surrounding whitespace included.
func ValidateBio(bio string) error {
	if bio == "" {
		return nil
	}
	if utf8.RuneCountInString(bio) > MaxBioLength {
		return ErrInvalidBio
	}
	if strings.TrimSpace(bio) == "" {
		return ErrInvalidBio
	}
	for _, r := range bio {
		if r == '\n' {
			continue
		}
		if unicode.IsControl(r) {
			return ErrInvalidBio
		}
	}
	return nil
}
