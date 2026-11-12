package coinbase

import (
	"regexp"
	"time"

	kyccoinbasepb "github.com/code-payments/flipcash2-protobuf-api/generated/go/kyc/coinbase/v1"
)

// The checks here are the ones the proto's validation rules cannot make. A
// request reaching them has passed those rules, so nothing they enforce
// (lengths, the SSN's and phone number's digits, the enums being defined) is
// checked again. Names, SSN, phone number and the two enums are fully
// constrained by the rules; whether an SSN or a name is right is Coinbase's
// verification, which answers through the state.

var (
	// zoneFurthestBehind and zoneFurthestAhead are the time zones in which
	// the date is earliest (UTC−12) and latest (UTC+14). Between them every
	// place on Earth is on one of at most three dates.
	zoneFurthestBehind = time.FixedZone("UTC-12", -12*60*60)
	zoneFurthestAhead  = time.FixedZone("UTC+14", 14*60*60)
)

const (
	// minimumAge is the youngest a user may be. Coinbase blocks a customer it
	// is told is younger.
	minimumAge = 18

	// maximumAge is the age at which a date of birth is refused as
	// implausible (the proto: "under 110"). Coinbase blocks a customer it is
	// told is impossibly old.
	maximumAge = 110

	// maxEmailBytes is the longest email address Coinbase takes.
	maxEmailBytes = 254
)

// coinbaseEmailPattern is Coinbase's own email pattern, verbatim from its
// OpenAPI spec. It refuses addresses the Flipcash common.v1.EmailAddress
// pattern passes, such as "a@b..com" (an empty domain label), which Coinbase
// would refuse with an unexplained INVALID_SUBMISSION. It holds a backtick,
// so it cannot be one raw string.
var coinbaseEmailPattern = regexp.MustCompile(`^[a-zA-Z0-9.!#$%&'*+/=?^_` + "`" + `{|}/-]+@[a-zA-Z0-9-]+(?:\.[a-zA-Z0-9-]+)*$`)

// usStates are the codes accepted in Address.state: the 50 states, the
// District of Columbia, the inhabited territories (American Samoa, Guam, the
// Northern Mariana Islands, Puerto Rico, the U.S. Virgin Islands), and the
// military codes for the Americas, Europe and the Pacific (AA, AE, AP).
//
// Which codes Coinbase accepts is unconfirmed. A code passed here that
// Coinbase refuses costs the user an unexplained INVALID_SUBMISSION, while a
// code wrongly refused here locks a real resident out for good, so the list
// is the permissive one. The freely associated states (FM, MH, PW) are left
// out: they are not U.S. residence.
var usStates = map[string]bool{
	"AL": true, "AK": true, "AZ": true, "AR": true, "CA": true, "CO": true, "CT": true, "DE": true,
	"FL": true, "GA": true, "HI": true, "ID": true, "IL": true, "IN": true, "IA": true, "KS": true,
	"KY": true, "LA": true, "ME": true, "MD": true, "MA": true, "MI": true, "MN": true, "MS": true,
	"MO": true, "MT": true, "NE": true, "NV": true, "NH": true, "NJ": true, "NM": true, "NY": true,
	"NC": true, "ND": true, "OH": true, "OK": true, "OR": true, "PA": true, "RI": true, "SC": true,
	"SD": true, "TN": true, "TX": true, "UT": true, "VT": true, "VA": true, "WA": true, "WV": true,
	"WI": true, "WY": true,
	"DC": true,
	"AS": true, "GU": true, "MP": true, "PR": true, "VI": true,
	"AA": true, "AE": true, "AP": true,
}

// supportedCountry is the one country whose residents Coinbase verifies.
const supportedCountry = "US"

// checkIndividual checks what a submission says about the user before
// anything is sent to Coinbase, and returns each field it refuses once, in
// enum order. Nil for a nil individual, or one with nothing to refuse.
//
// Only the date of birth, the address and the email are checked; see
// checkDateOfBirth, checkAddress and checkEmail for why each is. The phone
// number is judged too, and the email again where a verified one is required,
// against the user's profile, which is the server's to read
// (Server.checkVerifiedContact): nothing here reads anything.
func checkIndividual(individual *kyccoinbasepb.Individual, now time.Time) []*kyccoinbasepb.SubmitKycResponse_InvalidField {
	if individual == nil {
		return nil
	}
	var invalid []*kyccoinbasepb.SubmitKycResponse_InvalidField
	add := func(field kyccoinbasepb.Field, reason kyccoinbasepb.SubmitKycResponse_InvalidField_Reason) {
		invalid = append(invalid, &kyccoinbasepb.SubmitKycResponse_InvalidField{Field: field, Reason: reason})
	}
	if dob := individual.GetDateOfBirth(); dob != nil {
		if reason, ok := checkDateOfBirth(dob, now); !ok {
			add(kyccoinbasepb.Field_FIELD_DATE_OF_BIRTH, reason)
		}
	}
	if address := individual.GetAddress(); address != nil {
		if reason, ok := checkAddress(address); !ok {
			add(kyccoinbasepb.Field_FIELD_RESIDENTIAL_ADDRESS, reason)
		}
	}
	if email := individual.GetEmail(); email != nil {
		if !checkEmail(email.GetValue()) {
			add(kyccoinbasepb.Field_FIELD_EMAIL, kyccoinbasepb.SubmitKycResponse_InvalidField_MALFORMED)
		}
	}
	return invalid
}

// checkDateOfBirth judges a date of birth. It is checked here at all because
// Coinbase permanently blocks a customer whose date of birth makes them under
// 18 or impossibly old, and a block cannot be undone through the API, so a
// mistyped date must never reach it.
//
// The check is strictly conservative about where the user is. Let E be
// today's date where it is earliest (UTC−12) and L where it is latest
// (UTC+14). The date is MALFORMED when it is not a real calendar date, when
// it is after L (in the future everywhere), or when the user's 110th birthday
// is on or before L (they may be 110 somewhere). It is UNDERAGE unless the
// user's 18th birthday is strictly before E: they must have turned 18 at
// least a day ago everywhere on Earth. A 29 February birthday falls on 1
// March in a year that has none.
func checkDateOfBirth(dob *kyccoinbasepb.DateOfBirth, now time.Time) (kyccoinbasepb.SubmitKycResponse_InvalidField_Reason, bool) {
	year, month, day := int(dob.GetYear()), time.Month(dob.GetMonth()), int(dob.GetDay())

	born := time.Date(year, month, day, 0, 0, 0, 0, time.UTC)
	if born.Year() != year || born.Month() != month || born.Day() != day {
		return kyccoinbasepb.SubmitKycResponse_InvalidField_MALFORMED, false
	}

	earliest := civilDate(now.In(zoneFurthestBehind))
	latest := civilDate(now.In(zoneFurthestAhead))

	if born.After(latest) {
		return kyccoinbasepb.SubmitKycResponse_InvalidField_MALFORMED, false
	}
	if !birthday(year, month, day, maximumAge).After(latest) {
		return kyccoinbasepb.SubmitKycResponse_InvalidField_MALFORMED, false
	}
	if !birthday(year, month, day, minimumAge).Before(earliest) {
		return kyccoinbasepb.SubmitKycResponse_InvalidField_UNDERAGE, false
	}
	return 0, true
}

// civilDate is t's date, as midnight UTC, so that dates from different zones
// compare as dates.
func civilDate(t time.Time) time.Time {
	return time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, time.UTC)
}

// birthday is the date of the age'th birthday of someone born on the given
// date, as midnight UTC. time.Date normalizes 29 February in a year that has
// none to 1 March.
func birthday(year int, month time.Month, day int, age int) time.Time {
	return time.Date(year+age, month, day, 0, 0, 0, 0, time.UTC)
}

// checkAddress judges an address. Coinbase verifies residents of the United
// States alone, so any other country is UNSUPPORTED_REGION, and its state is
// then not judged. A U.S. address whose state is not one of usStates is
// MALFORMED.
func checkAddress(address *kyccoinbasepb.Address) (kyccoinbasepb.SubmitKycResponse_InvalidField_Reason, bool) {
	if address.GetCountryCode() != supportedCountry {
		return kyccoinbasepb.SubmitKycResponse_InvalidField_UNSUPPORTED_REGION, false
	}
	if !usStates[address.GetState()] {
		return kyccoinbasepb.SubmitKycResponse_InvalidField_MALFORMED, false
	}
	return 0, true
}

// checkEmail reports whether Coinbase would take an email address: at most
// 254 bytes, and matching coinbaseEmailPattern.
func checkEmail(email string) bool {
	return len(email) <= maxEmailBytes && coinbaseEmailPattern.MatchString(email)
}
