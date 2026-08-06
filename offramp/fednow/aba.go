package fednow

// IsValidRoutingNumber reports whether s is a well-formed ABA routing transit
// number: exactly nine digits with a valid checksum. The checksum catches most
// single-digit typos and transpositions in user-entered routing numbers.
func IsValidRoutingNumber(s string) bool {
	if len(s) != 9 {
		return false
	}

	var digits [9]int
	for i := range 9 {
		c := s[i]
		if c < '0' || c > '9' {
			return false
		}
		digits[i] = int(c - '0')
	}

	checksum := 3*(digits[0]+digits[3]+digits[6]) +
		7*(digits[1]+digits[4]+digits[7]) +
		(digits[2] + digits[5] + digits[8])
	return checksum%10 == 0
}
