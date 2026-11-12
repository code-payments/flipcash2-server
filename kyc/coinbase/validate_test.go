package coinbase

import (
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"
	kyccoinbasepb "github.com/code-payments/flipcash2-protobuf-api/generated/go/kyc/coinbase/v1"

	"github.com/code-payments/flipcash2-server/protoutil"
)

const (
	malformed         = kyccoinbasepb.SubmitKycResponse_InvalidField_MALFORMED
	underage          = kyccoinbasepb.SubmitKycResponse_InvalidField_UNDERAGE
	unsupportedRegion = kyccoinbasepb.SubmitKycResponse_InvalidField_UNSUPPORTED_REGION
)

func dob(year, month, day uint32) *kyccoinbasepb.DateOfBirth {
	return &kyccoinbasepb.DateOfBirth{Year: year, Month: month, Day: day}
}

func TestCheckDateOfBirth(t *testing.T) {
	// At 11:00 UTC on 11 November 2026 it is 10 November in UTC−12 (E) and
	// 12 November in UTC+14 (L).
	mid := time.Date(2026, 11, 11, 11, 0, 0, 0, time.UTC)
	// At 11:00 UTC on 1 March 2026 (not a leap year), E is 28 February and L
	// is 2 March.
	march1 := time.Date(2026, 3, 1, 11, 0, 0, 0, time.UTC)

	for _, tc := range []struct {
		name string
		dob  *kyccoinbasepb.DateOfBirth
		now  time.Time
		want []kyccoinbasepb.SubmitKycResponse_InvalidField_Reason // empty for valid
	}{
		{"an ordinary adult", dob(1990, 6, 15), mid, nil},

		{"30 February", dob(1990, 2, 30), mid, []kyccoinbasepb.SubmitKycResponse_InvalidField_Reason{malformed}},
		{"31 April", dob(1990, 4, 31), mid, []kyccoinbasepb.SubmitKycResponse_InvalidField_Reason{malformed}},
		{"29 February in a non-leap year", dob(2001, 2, 29), mid, []kyccoinbasepb.SubmitKycResponse_InvalidField_Reason{malformed}},
		{"29 February 1900, not a leap year", dob(1900, 2, 29), time.Date(1960, 1, 1, 0, 0, 0, 0, time.UTC), []kyccoinbasepb.SubmitKycResponse_InvalidField_Reason{malformed}},
		{"29 February in a leap year", dob(2000, 2, 29), mid, nil},
		{"month zero", dob(1990, 0, 1), mid, []kyccoinbasepb.SubmitKycResponse_InvalidField_Reason{malformed}},
		{"month thirteen", dob(1990, 13, 1), mid, []kyccoinbasepb.SubmitKycResponse_InvalidField_Reason{malformed}},
		{"day zero", dob(1990, 1, 0), mid, []kyccoinbasepb.SubmitKycResponse_InvalidField_Reason{malformed}},

		{"the day after L is in the future everywhere", dob(2026, 11, 13), mid, []kyccoinbasepb.SubmitKycResponse_InvalidField_Reason{malformed}},
		{"far future", dob(9999, 12, 31), mid, []kyccoinbasepb.SubmitKycResponse_InvalidField_Reason{malformed}},
		{"L itself is today somewhere, and underage", dob(2026, 11, 12), mid, []kyccoinbasepb.SubmitKycResponse_InvalidField_Reason{underage}},
		{"today in UTC is underage", dob(2026, 11, 11), mid, []kyccoinbasepb.SubmitKycResponse_InvalidField_Reason{underage}},

		{"110th birthday the day before L", dob(1916, 11, 11), mid, []kyccoinbasepb.SubmitKycResponse_InvalidField_Reason{malformed}},
		{"110th birthday on L", dob(1916, 11, 12), mid, []kyccoinbasepb.SubmitKycResponse_InvalidField_Reason{malformed}},
		{"110th birthday the day after L", dob(1916, 11, 13), mid, nil},
		{"1900, when the bound is far off", dob(1900, 1, 1), time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC), nil},

		{"18th birthday the day before E", dob(2008, 11, 9), mid, nil},
		{"18th birthday on E", dob(2008, 11, 10), mid, []kyccoinbasepb.SubmitKycResponse_InvalidField_Reason{underage}},
		{"18th birthday today in UTC", dob(2008, 11, 11), mid, []kyccoinbasepb.SubmitKycResponse_InvalidField_Reason{underage}},
		{"18th birthday on L", dob(2008, 11, 12), mid, []kyccoinbasepb.SubmitKycResponse_InvalidField_Reason{underage}},

		// 29 February 2008's 18th birthday falls on 1 March 2026.
		{"29 February birthday, 18th on 1 March, E 28 February", dob(2008, 2, 29), march1, []kyccoinbasepb.SubmitKycResponse_InvalidField_Reason{underage}},
		{"29 February birthday, 18th on 1 March, E 1 March", dob(2008, 2, 29), march1.AddDate(0, 0, 1), []kyccoinbasepb.SubmitKycResponse_InvalidField_Reason{underage}},
		{"29 February birthday, 18th on 1 March, E 2 March", dob(2008, 2, 29), march1.AddDate(0, 0, 2), nil},
		{"28 February birthday, 18th on 28 February, E 28 February", dob(2008, 2, 28), march1, []kyccoinbasepb.SubmitKycResponse_InvalidField_Reason{underage}},
		{"28 February birthday, 18th on 28 February, E 1 March", dob(2008, 2, 28), march1.AddDate(0, 0, 1), nil},

		// 29 February 1916's 110th birthday falls on 1 March 2026.
		{"29 February birthday, 110th on 1 March, L 28 February", dob(1916, 2, 29), march1.AddDate(0, 0, -2), nil},
		{"29 February birthday, 110th on 1 March, L 1 March", dob(1916, 2, 29), march1.AddDate(0, 0, -1), []kyccoinbasepb.SubmitKycResponse_InvalidField_Reason{malformed}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := checkIndividual(&kyccoinbasepb.Individual{DateOfBirth: tc.dob}, tc.now)
			var want []*kyccoinbasepb.SubmitKycResponse_InvalidField
			for _, reason := range tc.want {
				want = append(want, &kyccoinbasepb.SubmitKycResponse_InvalidField{Field: kyccoinbasepb.Field_FIELD_DATE_OF_BIRTH, Reason: reason})
			}
			require.NoError(t, protoutil.SliceEqualError(want, got))
		})
	}
}

func TestCheckDateOfBirth_ZonesFallOnDifferentDates(t *testing.T) {
	// The fixture above relies on this.
	now := time.Date(2026, 11, 11, 11, 0, 0, 0, time.UTC)
	require.Equal(t, 10, now.In(zoneFurthestBehind).Day())
	require.Equal(t, 12, now.In(zoneFurthestAhead).Day())
}

func usAddress(state string) *kyccoinbasepb.Address {
	return &kyccoinbasepb.Address{Line1: "1 Test St", City: "Testville", State: state, PostalCode: "00000", CountryCode: "US"}
}

func TestCheckAddress(t *testing.T) {
	accepted := strings.Fields(`
		AL AK AZ AR CA CO CT DE FL GA HI ID IL IN IA KS KY LA ME MD MA MI MN MS MO
		MT NE NV NH NJ NM NY NC ND OH OK OR PA RI SC SD TN TX UT VT VA WA WV WI WY
		DC AS GU MP PR VI AA AE AP`)
	require.Len(t, accepted, 59)
	for _, state := range accepted {
		t.Run("accepts "+state, func(t *testing.T) {
			require.Nil(t, checkIndividual(&kyccoinbasepb.Individual{Address: usAddress(state)}, time.Now()))
		})
	}
	require.Len(t, usStates, len(accepted))

	for _, state := range []string{"FM", "MH", "PW", "XX", "ZZ", "UM", "ca", ""} {
		t.Run("refuses "+state, func(t *testing.T) {
			got := checkIndividual(&kyccoinbasepb.Individual{Address: usAddress(state)}, time.Now())
			require.NoError(t, protoutil.SliceEqualError([]*kyccoinbasepb.SubmitKycResponse_InvalidField{
				{Field: kyccoinbasepb.Field_FIELD_RESIDENTIAL_ADDRESS, Reason: malformed},
			}, got))
		})
	}

	for _, tc := range []struct{ country, state string }{
		{"CA", "ON"},
		{"CA", "XX"},
		{"GB", "CA"},
		{"PR", "PR"},
		{"us", "CA"},
	} {
		t.Run("unsupported region "+tc.country+" "+tc.state, func(t *testing.T) {
			address := usAddress(tc.state)
			address.CountryCode = tc.country
			got := checkIndividual(&kyccoinbasepb.Individual{Address: address}, time.Now())
			require.NoError(t, protoutil.SliceEqualError([]*kyccoinbasepb.SubmitKycResponse_InvalidField{
				{Field: kyccoinbasepb.Field_FIELD_RESIDENTIAL_ADDRESS, Reason: unsupportedRegion},
			}, got))
		})
	}
}

func TestCheckEmail(t *testing.T) {
	local := strings.Repeat("a", 64)
	domain := func(n int) string { // a domain of n bytes
		return strings.Repeat("b", n-len(".com")) + ".com"
	}
	at254 := local + "@" + domain(254-len(local)-1)
	at255 := local + "@" + domain(255-len(local)-1)
	require.Len(t, at254, 254)
	require.Len(t, at255, 255)

	for _, tc := range []struct {
		email string
		ok    bool
	}{
		{"test@example.com", true},
		{"a.b+c@sub.example.com", true},
		{"x!#$%&'*+/=?^_`{|}-@example.com", true},
		{"test@localhost", true},
		{at254, true},
		{at255, false},
		{"a@b..com", false},
		{"a@.com", false},
		{"a@b.com.", false},
		{"a@b_c.com", false},
		{"a b@example.com", false},
		{"@example.com", false},
		{"test@", false},
		{"test", false},
		{"tést@example.com", false},
	} {
		t.Run(tc.email, func(t *testing.T) {
			got := checkIndividual(&kyccoinbasepb.Individual{Email: &commonpb.EmailAddress{Value: tc.email}}, time.Now())
			if tc.ok {
				require.Nil(t, got)
				return
			}
			require.NoError(t, protoutil.SliceEqualError([]*kyccoinbasepb.SubmitKycResponse_InvalidField{
				{Field: kyccoinbasepb.Field_FIELD_EMAIL, Reason: malformed},
			}, got))
		})
	}
}

func TestCheckIndividual(t *testing.T) {
	now := time.Date(2026, 11, 11, 11, 0, 0, 0, time.UTC)

	require.Nil(t, checkIndividual(nil, now))
	require.Nil(t, checkIndividual(&kyccoinbasepb.Individual{}, now))

	// Fields the proto's rules constrain fully are not checked again.
	require.Nil(t, checkIndividual(&kyccoinbasepb.Individual{
		LegalName:        &kyccoinbasepb.LegalName{FirstName: "Test", LastName: "User"},
		DateOfBirth:      dob(1990, 6, 15),
		Address:          usAddress("MA"),
		Ssn:              &kyccoinbasepb.Ssn{Value: "000000000"},
		Email:            &commonpb.EmailAddress{Value: "test@example.com"},
		PhoneNumber:      &commonpb.PhoneNumber{Value: "+15555550100"},
		PurposeOfAccount: kyccoinbasepb.PurposeOfAccount_PURPOSE_OF_ACCOUNT_OTHER,
		SourceOfFunds:    kyccoinbasepb.SourceOfFunds_SOURCE_OF_FUNDS_OTHER,
	}, now))

	// Each refused field once, in enum order.
	address := usAddress("ON")
	address.CountryCode = "CA"
	got := checkIndividual(&kyccoinbasepb.Individual{
		Email:       &commonpb.EmailAddress{Value: "a@b..com"},
		Address:     address,
		DateOfBirth: dob(2010, 1, 1),
	}, now)
	require.NoError(t, protoutil.SliceEqualError([]*kyccoinbasepb.SubmitKycResponse_InvalidField{
		{Field: kyccoinbasepb.Field_FIELD_DATE_OF_BIRTH, Reason: underage},
		{Field: kyccoinbasepb.Field_FIELD_RESIDENTIAL_ADDRESS, Reason: unsupportedRegion},
		{Field: kyccoinbasepb.Field_FIELD_EMAIL, Reason: malformed},
	}, got))
}
