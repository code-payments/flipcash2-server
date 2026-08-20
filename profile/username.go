package profile

import (
	"regexp"
	"strings"
)

// usernamePattern is the set of handles a user may hold, mirroring what
// profile.v1.Username enforces on the wire: X's character set, minus upper
// case, so a handle has exactly one spelling.
var usernamePattern = regexp.MustCompile(`^[a-z0-9_]{2,15}$`)

// NormalizeUsername puts a handle into the canonical form it is stored and
// compared in, so a lookup finds its holder regardless of the casing it was
// typed in.
func NormalizeUsername(username string) string {
	return strings.ToLower(username)
}

// ValidateUsername returns ErrInvalidUsername unless username is a handle in
// canonical form. Every store checks through here before persisting, so nothing
// but the canonical form is ever held — which is what makes a case-insensitive
// lookup enough to find a holder.
func ValidateUsername(username string) error {
	if !usernamePattern.MatchString(username) {
		return ErrInvalidUsername
	}
	return nil
}
