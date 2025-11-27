package email

import "regexp"

var (
	emailAddressPattern = regexp.MustCompile("^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$")

	// A verification code must be a 4-10 digit string
	verificationCodePattern = regexp.MustCompile("^[0-9]{4,10}$")
)

// IsEmailAddress returns whether a string is an email address
func IsEmailAddress(emailAddress string) bool {
	return emailAddressPattern.Match([]byte(emailAddress))
}

// IsVerificationCode returns whether a string is a 4-10 digit numberical
// verification code.
func IsVerificationCode(code string) bool {
	return verificationCodePattern.Match([]byte(code))
}
