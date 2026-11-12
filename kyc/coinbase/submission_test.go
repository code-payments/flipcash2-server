package coinbase

import (
	"errors"
	"fmt"
	"net/http"
	"net/netip"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"
	kyccoinbasepb "github.com/code-payments/flipcash2-protobuf-api/generated/go/kyc/coinbase/v1"

	"github.com/code-payments/flipcash2-server/cdp/customers"
	"github.com/code-payments/flipcash2-server/protoutil"
)

const (
	notAsked            = kyccoinbasepb.SubmitKycResponse_InvalidTermsOfServiceAcceptance_NOT_ASKED
	unsupportedLanguage = kyccoinbasepb.SubmitKycResponse_InvalidTermsOfServiceAcceptance_UNSUPPORTED_LANGUAGE
)

func acceptance(versionID, language string) *kyccoinbasepb.TermsOfServiceAcceptance {
	return &kyccoinbasepb.TermsOfServiceAcceptance{VersionId: versionID, Language: &commonpb.Locale{Value: language}}
}

func w9Attestation(exempt, edelivery bool) *kyccoinbasepb.TaxAttestation {
	return &kyccoinbasepb.TaxAttestation{
		Form:             &kyccoinbasepb.TaxAttestation_UsW9_{UsW9: &kyccoinbasepb.TaxAttestation_UsW9{IsExemptBackupWithholding: exempt}},
		EdeliveryConsent: edelivery,
	}
}

func invalidTerms(reason kyccoinbasepb.SubmitKycResponse_InvalidTermsOfServiceAcceptance_Reason, versionIDs ...string) []*kyccoinbasepb.SubmitKycResponse_InvalidTermsOfServiceAcceptance {
	var out []*kyccoinbasepb.SubmitKycResponse_InvalidTermsOfServiceAcceptance
	for _, id := range versionIDs {
		out = append(out, &kyccoinbasepb.SubmitKycResponse_InvalidTermsOfServiceAcceptance{VersionId: id, Reason: reason})
	}
	return out
}

func TestValidateSubmission(t *testing.T) {
	for _, tc := range []struct {
		name string
		req  *kyccoinbasepb.SubmitKycRequest
		ok   bool
	}{
		{"empty", &kyccoinbasepb.SubmitKycRequest{}, true},
		{"nil", nil, true},
		{"distinct versions", &kyccoinbasepb.SubmitKycRequest{TermsOfServiceAcceptances: []*kyccoinbasepb.TermsOfServiceAcceptance{
			acceptance("v1", "en"), acceptance("v2", "en"),
		}}, true},
		{"version IDs are opaque, so case matters", &kyccoinbasepb.SubmitKycRequest{TermsOfServiceAcceptances: []*kyccoinbasepb.TermsOfServiceAcceptance{
			acceptance("v1", "en"), acceptance("V1", "en"),
		}}, true},
		{"one version twice", &kyccoinbasepb.SubmitKycRequest{TermsOfServiceAcceptances: []*kyccoinbasepb.TermsOfServiceAcceptance{
			acceptance("v1", "en"), acceptance("v2", "en"), acceptance("v1", "es"),
		}}, false},
		{"one W-9", &kyccoinbasepb.SubmitKycRequest{TaxAttestations: []*kyccoinbasepb.TaxAttestation{w9Attestation(false, true)}}, true},
		{"two W-9s", &kyccoinbasepb.SubmitKycRequest{TaxAttestations: []*kyccoinbasepb.TaxAttestation{w9Attestation(false, true), w9Attestation(true, false)}}, false},
		{"an attestation with no form is left to toCustomerInput", &kyccoinbasepb.SubmitKycRequest{TaxAttestations: []*kyccoinbasepb.TaxAttestation{
			{}, {}, w9Attestation(false, false),
		}}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := validateSubmission(tc.req)
			if tc.ok {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
				require.NotContains(t, err.Error(), "v1")
			}
		})
	}
}

func TestIsEmptySubmission(t *testing.T) {
	require.True(t, isEmptySubmission(&kyccoinbasepb.SubmitKycRequest{}))
	require.True(t, isEmptySubmission(&kyccoinbasepb.SubmitKycRequest{Individual: &kyccoinbasepb.Individual{}}))
	require.True(t, isEmptySubmission(&kyccoinbasepb.SubmitKycRequest{Individual: &kyccoinbasepb.Individual{
		PurposeOfAccount: kyccoinbasepb.PurposeOfAccount_PURPOSE_OF_ACCOUNT_UNKNOWN,
		SourceOfFunds:    kyccoinbasepb.SourceOfFunds_SOURCE_OF_FUNDS_UNKNOWN,
	}}))

	for name, individual := range map[string]*kyccoinbasepb.Individual{
		"legal name":         {LegalName: &kyccoinbasepb.LegalName{}},
		"date of birth":      {DateOfBirth: &kyccoinbasepb.DateOfBirth{}},
		"address":            {Address: &kyccoinbasepb.Address{}},
		"ssn":                {Ssn: &kyccoinbasepb.Ssn{}},
		"email":              {Email: &commonpb.EmailAddress{}},
		"phone number":       {PhoneNumber: &commonpb.PhoneNumber{}},
		"purpose of account": {PurposeOfAccount: kyccoinbasepb.PurposeOfAccount_PURPOSE_OF_ACCOUNT_OTHER},
		"source of funds":    {SourceOfFunds: kyccoinbasepb.SourceOfFunds_SOURCE_OF_FUNDS_OTHER},
	} {
		t.Run(name, func(t *testing.T) {
			require.False(t, isEmptySubmission(&kyccoinbasepb.SubmitKycRequest{Individual: individual}))
		})
	}
	require.False(t, isEmptySubmission(&kyccoinbasepb.SubmitKycRequest{TermsOfServiceAcceptances: []*kyccoinbasepb.TermsOfServiceAcceptance{acceptance("v1", "en")}}))
	require.False(t, isEmptySubmission(&kyccoinbasepb.SubmitKycRequest{TaxAttestations: []*kyccoinbasepb.TaxAttestation{w9Attestation(false, false)}}))
}

func TestSubmitsAcceptancesAlone(t *testing.T) {
	acceptances := []*kyccoinbasepb.TermsOfServiceAcceptance{acceptance("v1", "en")}
	attestations := []*kyccoinbasepb.TaxAttestation{w9Attestation(false, false)}

	require.True(t, submitsAcceptancesAlone(&kyccoinbasepb.SubmitKycRequest{TermsOfServiceAcceptances: acceptances}))
	// An individual that sets nothing is nothing beside them.
	require.True(t, submitsAcceptancesAlone(&kyccoinbasepb.SubmitKycRequest{TermsOfServiceAcceptances: acceptances, Individual: &kyccoinbasepb.Individual{}}))

	require.False(t, submitsAcceptancesAlone(&kyccoinbasepb.SubmitKycRequest{}))
	require.False(t, submitsAcceptancesAlone(&kyccoinbasepb.SubmitKycRequest{TaxAttestations: attestations}))
	require.False(t, submitsAcceptancesAlone(&kyccoinbasepb.SubmitKycRequest{TermsOfServiceAcceptances: acceptances, TaxAttestations: attestations}))
	require.False(t, submitsAcceptancesAlone(&kyccoinbasepb.SubmitKycRequest{
		TermsOfServiceAcceptances: acceptances,
		Individual:                &kyccoinbasepb.Individual{Email: &commonpb.EmailAddress{}},
	}))
	require.False(t, submitsAcceptancesAlone(&kyccoinbasepb.SubmitKycRequest{
		TermsOfServiceAcceptances: acceptances,
		Individual:                &kyccoinbasepb.Individual{SourceOfFunds: kyccoinbasepb.SourceOfFunds_SOURCE_OF_FUNDS_OTHER},
	}))
}

func TestCheckTermsAcceptances(t *testing.T) {
	// Received in a zone other than UTC: the stamp is in UTC.
	receivedAt := time.Date(2026, 11, 11, 6, 0, 30, 500, time.FixedZone("UTC-5", -5*60*60))
	stamp := time.Date(2026, 11, 11, 11, 0, 20, 500, time.UTC)

	listing := func(r customers.Requirement) *customers.Customer {
		return &customers.Customer{Requirements: map[customers.RequirementKey]customers.Requirement{customers.RequirementTos: r}}
	}
	listed := listing(customers.Requirement{Status: customers.RequirementDue, TosVersions: []customers.TermsOfService{
		version("v1", "en", "es-419"),
		version("v2", "EN"),
		version("v1", "fr"),
	}})

	for _, tc := range []struct {
		name        string
		acceptances []*kyccoinbasepb.TermsOfServiceAcceptance
		customer    *customers.Customer
		invalid     []*kyccoinbasepb.SubmitKycResponse_InvalidTermsOfServiceAcceptance
		accepted    []customers.TosAcceptance
		unlisted    []string
	}{
		{
			name:     "none",
			customer: listed,
		},
		{
			name:        "no customer lists nothing",
			acceptances: []*kyccoinbasepb.TermsOfServiceAcceptance{acceptance("v1", "en")},
			accepted:    []customers.TosAcceptance{{VersionID: "v1", Language: "en", AcceptedAt: stamp}},
			unlisted:    []string{"v1"},
		},
		{
			name:        "customer with nil maps lists nothing",
			customer:    &customers.Customer{},
			acceptances: []*kyccoinbasepb.TermsOfServiceAcceptance{acceptance("v1", "EN-us")},
			accepted:    []customers.TosAcceptance{{VersionID: "v1", Language: "EN-us", AcceptedAt: stamp}},
			unlisted:    []string{"v1"},
		},
		{
			name:        "listed, language exactly as listed",
			customer:    listed,
			acceptances: []*kyccoinbasepb.TermsOfServiceAcceptance{acceptance("v1", "es-419"), acceptance("v2", "EN")},
			accepted: []customers.TosAcceptance{
				{VersionID: "v1", Language: "es-419", AcceptedAt: stamp},
				{VersionID: "v2", Language: "EN", AcceptedAt: stamp},
			},
		},
		{
			name:        "listed, language in another case than listed is not published",
			customer:    listed,
			acceptances: []*kyccoinbasepb.TermsOfServiceAcceptance{acceptance("v1", "ES-419"), acceptance("v2", "en")},
			invalid:     invalidTerms(unsupportedLanguage, "v1", "v2"),
		},
		{
			name:        "listed, language not published; the first listing of a version wins",
			customer:    listed,
			acceptances: []*kyccoinbasepb.TermsOfServiceAcceptance{acceptance("v1", "fr"), acceptance("v2", "es")},
			invalid:     invalidTerms(unsupportedLanguage, "v1", "v2"),
		},
		{
			name:     "mixed, in request order",
			customer: listed,
			acceptances: []*kyccoinbasepb.TermsOfServiceAcceptance{
				acceptance("old", "en"), acceptance("v1", "de"), acceptance("v2", "EN"), acceptance("older", "fr"),
			},
			invalid: invalidTerms(unsupportedLanguage, "v1"),
			accepted: []customers.TosAcceptance{
				{VersionID: "old", Language: "en", AcceptedAt: stamp},
				{VersionID: "v2", Language: "EN", AcceptedAt: stamp},
				{VersionID: "older", Language: "fr", AcceptedAt: stamp},
			},
			unlisted: []string{"old", "older"},
		},
		{
			name: "listed whatever the requirement's status or impact",
			customer: listing(customers.Requirement{
				Status:      customers.RequirementPending,
				Impact:      []customers.CapabilityName{customers.CapabilityTradeCrypto},
				TosVersions: []customers.TermsOfService{version("v1", "en")},
			}),
			acceptances: []*kyccoinbasepb.TermsOfServiceAcceptance{acceptance("v1", "fr")},
			invalid:     invalidTerms(unsupportedLanguage, "v1"),
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			invalid, accepted, unlisted := checkTermsAcceptances(tc.acceptances, tc.customer, receivedAt)
			require.NoError(t, protoutil.SliceEqualError(tc.invalid, invalid))
			require.Equal(t, tc.accepted, accepted)
			require.Equal(t, tc.unlisted, unlisted)
			for _, a := range accepted {
				require.Equal(t, time.UTC, a.AcceptedAt.Location())
				require.Equal(t, acceptedAtBackdate, receivedAt.Sub(a.AcceptedAt))
			}
		})
	}
}

var testIP = netip.MustParseAddr("203.0.113.42")

func fullIndividual() *kyccoinbasepb.Individual {
	return &kyccoinbasepb.Individual{
		LegalName:        &kyccoinbasepb.LegalName{FirstName: "Test", LastName: "User"},
		DateOfBirth:      dob(1919, 1, 1),
		Address:          &kyccoinbasepb.Address{Line1: "1 Test St", City: "Testville", State: "MA", PostalCode: "00000", CountryCode: "US"},
		Ssn:              &kyccoinbasepb.Ssn{Value: "000000000"},
		Email:            &commonpb.EmailAddress{Value: "test@example.com"},
		PhoneNumber:      &commonpb.PhoneNumber{Value: "+15555550100"},
		PurposeOfAccount: kyccoinbasepb.PurposeOfAccount_PURPOSE_OF_ACCOUNT_TRADING_ON_OTHER_EXCHANGES,
		SourceOfFunds:    kyccoinbasepb.SourceOfFunds_SOURCE_OF_FUNDS_SALE_OF_COMPANY_SHARES_OR_DIVIDENDS,
	}
}

func TestToCustomerInput(t *testing.T) {
	receivedAt := time.Date(2026, 11, 11, 6, 0, 30, 0, time.FixedZone("UTC-5", -5*60*60))
	stamp := time.Date(2026, 11, 11, 11, 0, 20, 0, time.UTC)
	empty := ""
	line2 := "Apt 2"

	accepted := []customers.TosAcceptance{{VersionID: "v1", Language: "en", AcceptedAt: stamp}}

	for _, tc := range []struct {
		name     string
		req      *kyccoinbasepb.SubmitKycRequest
		accepted []customers.TosAcceptance
		ip       netip.Addr
		want     *customers.CustomerInput
	}{
		{
			name: "everything",
			req: &kyccoinbasepb.SubmitKycRequest{
				Individual:                fullIndividual(),
				TermsOfServiceAcceptances: []*kyccoinbasepb.TermsOfServiceAcceptance{acceptance("v1", "en")},
				TaxAttestations:           []*kyccoinbasepb.TaxAttestation{w9Attestation(true, false)},
			},
			accepted: accepted,
			ip:       testIP,
			want: &customers.CustomerInput{
				Individual: &customers.IndividualInput{
					FirstName:   "Test",
					LastName:    "User",
					DateOfBirth: &customers.DateOfBirth{Year: "1919", Month: "01", Day: "01"},
					Address: &customers.AddressInput{
						Line1: "1 Test St", Line2: &empty, City: "Testville", State: "MA", PostCode: "00000", CountryCode: "US",
					},
					FullSsn:          "000-00-0000",
					Email:            "test@example.com",
					PhoneNumber:      "+15555550100",
					PurposeOfAccount: "trading_on_other_exchanges",
					SourceOfFunds:    "sale_of_company_shares_or_dividends",
				},
				TosAcceptances: accepted,
				TaxAttestations: []customers.TaxAttestation{{UsW9: &customers.UsW9Attestation{
					IsExemptBackupWithholding: true, EdeliveryConsent: false, AcceptedAt: stamp,
				}}},
				Compliance: &customers.Compliance{RequesterIPAddress: "203.0.113.42"},
			},
		},
		{
			name: "a second line, a two-digit month and day, an IPv4-mapped address",
			req: &kyccoinbasepb.SubmitKycRequest{Individual: &kyccoinbasepb.Individual{
				DateOfBirth: dob(1990, 12, 25),
				Address:     &kyccoinbasepb.Address{Line1: "1 Test St", Line2: line2, City: "Testville", State: "PR", PostalCode: "00000", CountryCode: "US"},
				Ssn:         &kyccoinbasepb.Ssn{Value: "123456789"},
			}},
			ip: netip.MustParseAddr("::ffff:203.0.113.42"),
			want: &customers.CustomerInput{
				Individual: &customers.IndividualInput{
					DateOfBirth: &customers.DateOfBirth{Year: "1990", Month: "12", Day: "25"},
					Address: &customers.AddressInput{
						Line1: "1 Test St", Line2: &line2, City: "Testville", State: "PR", PostCode: "00000", CountryCode: "US",
					},
					FullSsn: "123-45-6789",
				},
				Compliance: &customers.Compliance{RequesterIPAddress: "203.0.113.42"},
			},
		},
		{
			name: "only an enum",
			req: &kyccoinbasepb.SubmitKycRequest{Individual: &kyccoinbasepb.Individual{
				SourceOfFunds: kyccoinbasepb.SourceOfFunds_SOURCE_OF_FUNDS_SALARY,
			}},
			ip: netip.MustParseAddr("2001:db8::1"),
			want: &customers.CustomerInput{
				Individual: &customers.IndividualInput{SourceOfFunds: "salary"},
				Compliance: &customers.Compliance{RequesterIPAddress: "2001:db8::1"},
			},
		},
		{
			name: "an individual setting nothing is left out",
			req:  &kyccoinbasepb.SubmitKycRequest{Individual: &kyccoinbasepb.Individual{}},
			ip:   testIP,
			want: &customers.CustomerInput{Compliance: &customers.Compliance{RequesterIPAddress: "203.0.113.42"}},
		},
		{
			name:     "acceptances alone; an empty accepted list is nil",
			req:      &kyccoinbasepb.SubmitKycRequest{},
			accepted: []customers.TosAcceptance{},
			ip:       testIP,
			want:     &customers.CustomerInput{Compliance: &customers.Compliance{RequesterIPAddress: "203.0.113.42"}},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := toCustomerInput(tc.req, tc.accepted, receivedAt, tc.ip)
			require.NoError(t, err)
			require.Equal(t, tc.want, got)
			require.Nil(t, got.Capabilities)
			for _, a := range got.TaxAttestations {
				require.Equal(t, time.UTC, a.UsW9.AcceptedAt.Location())
				require.Equal(t, acceptedAtBackdate, receivedAt.Sub(a.UsW9.AcceptedAt))
			}
		})
	}
}

func TestToCustomerInput_Errors(t *testing.T) {
	now := time.Now()
	for _, tc := range []struct {
		name string
		req  *kyccoinbasepb.SubmitKycRequest
		ip   netip.Addr
	}{
		{"invalid ip", &kyccoinbasepb.SubmitKycRequest{Individual: fullIndividual()}, netip.Addr{}},
		{"attestation with no form", &kyccoinbasepb.SubmitKycRequest{TaxAttestations: []*kyccoinbasepb.TaxAttestation{w9Attestation(false, false), {}}}, testIP},
		{"undefined purpose of account", &kyccoinbasepb.SubmitKycRequest{Individual: &kyccoinbasepb.Individual{PurposeOfAccount: 99}}, testIP},
		{"undefined source of funds", &kyccoinbasepb.SubmitKycRequest{Individual: &kyccoinbasepb.Individual{SourceOfFunds: 99}}, testIP},
		{"ssn not nine digits", &kyccoinbasepb.SubmitKycRequest{Individual: &kyccoinbasepb.Individual{Ssn: &kyccoinbasepb.Ssn{Value: "12345678"}}}, testIP},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := toCustomerInput(tc.req, nil, now, tc.ip)
			require.Error(t, err)
			require.Nil(t, got)
			require.NotContains(t, err.Error(), "12345678")
		})
	}
}

func TestEnumTables(t *testing.T) {
	require.Len(t, purposesOfAccount, len(kyccoinbasepb.PurposeOfAccount_name)-1)
	for number, name := range kyccoinbasepb.PurposeOfAccount_name {
		value, ok := purposesOfAccount[kyccoinbasepb.PurposeOfAccount(number)]
		if number == 0 {
			require.False(t, ok, name)
			continue
		}
		require.True(t, ok, name)
		require.Equal(t, strings.ToLower(strings.TrimPrefix(name, "PURPOSE_OF_ACCOUNT_")), value)
	}

	require.Len(t, sourcesOfFunds, len(kyccoinbasepb.SourceOfFunds_name)-1)
	for number, name := range kyccoinbasepb.SourceOfFunds_name {
		value, ok := sourcesOfFunds[kyccoinbasepb.SourceOfFunds(number)]
		if number == 0 {
			require.False(t, ok, name)
			continue
		}
		require.True(t, ok, name)
		require.Equal(t, strings.ToLower(strings.TrimPrefix(name, "SOURCE_OF_FUNDS_")), value)
	}
}

func TestSubmissionRefusal(t *testing.T) {
	coinbaseError := func(status int, errorType customers.ErrorType) error {
		return &customers.Error{StatusCode: status, Type: errorType, Message: "quotes 000-00-0000", CorrelationID: "c"}
	}
	stamp := time.Now()
	acceptances := func(ids ...string) []customers.TosAcceptance {
		var out []customers.TosAcceptance
		for _, id := range ids {
			out = append(out, customers.TosAcceptance{VersionID: id, Language: "en", AcceptedAt: stamp})
		}
		return out
	}

	for _, tc := range []struct {
		name     string
		err      error
		accepted []customers.TosAcceptance
		unlisted []string
		// alone: the update carried acceptances and nothing else.
		alone bool
		ok    bool
		want  []*kyccoinbasepb.SubmitKycResponse_InvalidTermsOfServiceAcceptance
	}{
		{name: "nil", err: nil},
		{name: "not a Coinbase error", err: errors.New("dial tcp: refused"), unlisted: []string{"v1"}},
		{name: "a request the client refused to send", err: fmt.Errorf("%w: tax attestation sets no form", customers.ErrInvalidArgument)},
		{name: "401", err: coinbaseError(http.StatusUnauthorized, customers.ErrorTypeUnauthorized), unlisted: []string{"v1"}},
		{name: "404", err: coinbaseError(http.StatusNotFound, customers.ErrorTypeNotFound)},
		{name: "422", err: coinbaseError(http.StatusUnprocessableEntity, customers.ErrorTypeIdempotency)},
		{name: "429", err: coinbaseError(http.StatusTooManyRequests, customers.ErrorTypeRateLimitExceeded)},
		{name: "500", err: coinbaseError(http.StatusInternalServerError, "internal_server_error")},
		{name: "502 with no body", err: coinbaseError(http.StatusBadGateway, "")},
		{name: "unsupported_tos_language as a 422 is not a refusal", err: coinbaseError(http.StatusUnprocessableEntity, customers.ErrorTypeUnsupportedTosLanguage), unlisted: []string{"v1"}},

		{
			name:     "unsupported language, attributed to the unlisted",
			err:      coinbaseError(http.StatusBadRequest, customers.ErrorTypeUnsupportedTosLanguage),
			accepted: acceptances("v1", "old", "older"),
			unlisted: []string{"old", "older"},
			ok:       true,
			want:     invalidTerms(unsupportedLanguage, "old", "older"),
		},
		{
			name:     "unsupported language with none unlisted, attributed to every accepted",
			err:      coinbaseError(http.StatusBadRequest, customers.ErrorTypeUnsupportedTosLanguage),
			accepted: acceptances("v1", "v2"),
			ok:       true,
			want:     invalidTerms(unsupportedLanguage, "v1", "v2"),
		},
		{
			name: "unsupported language with no acceptances names nothing",
			err:  coinbaseError(http.StatusBadRequest, customers.ErrorTypeUnsupportedTosLanguage),
			ok:   true,
		},
		{
			name:     "invalid request with unlisted acceptances alone: NOT_ASKED",
			err:      coinbaseError(http.StatusBadRequest, customers.ErrorTypeInvalidRequest),
			accepted: acceptances("v1", "old"),
			unlisted: []string{"old"},
			alone:    true,
			ok:       true,
			want:     invalidTerms(notAsked, "old"),
		},
		{
			name:     "invalid request with unlisted acceptances beside other things names nothing",
			err:      coinbaseError(http.StatusBadRequest, customers.ErrorTypeInvalidRequest),
			accepted: acceptances("v1", "old"),
			unlisted: []string{"old"},
			ok:       true,
		},
		{
			name:     "unsupported language is attributed beside other things too",
			err:      coinbaseError(http.StatusBadRequest, customers.ErrorTypeUnsupportedTosLanguage),
			accepted: acceptances("v1", "old"),
			unlisted: []string{"old"},
			ok:       true,
			want:     invalidTerms(unsupportedLanguage, "old"),
		},
		{
			name:     "invalid request with only listed acceptances names nothing",
			err:      coinbaseError(http.StatusBadRequest, customers.ErrorTypeInvalidRequest),
			accepted: acceptances("v1"),
			alone:    true,
			ok:       true,
		},
		{
			name:     "a 400 of another type or none reads the same",
			err:      coinbaseError(http.StatusBadRequest, ""),
			accepted: acceptances("old"),
			unlisted: []string{"old"},
			alone:    true,
			ok:       true,
			want:     invalidTerms(notAsked, "old"),
		},
		{
			name:     "wrapped",
			err:      fmt.Errorf("updating customer: %w", coinbaseError(http.StatusBadRequest, customers.ErrorTypeInvalidRequest)),
			accepted: acceptances("old"),
			unlisted: []string{"old"},
			alone:    true,
			ok:       true,
			want:     invalidTerms(notAsked, "old"),
		},
		{
			name:     "wrapped unsupported language",
			err:      fmt.Errorf("updating customer: %w", coinbaseError(http.StatusBadRequest, customers.ErrorTypeUnsupportedTosLanguage)),
			accepted: acceptances("v1"),
			ok:       true,
			want:     invalidTerms(unsupportedLanguage, "v1"),
		},
		{
			name:     "a version repeated is named once",
			err:      coinbaseError(http.StatusBadRequest, customers.ErrorTypeInvalidRequest),
			unlisted: []string{"old", "old"},
			alone:    true,
			ok:       true,
			want:     invalidTerms(notAsked, "old"),
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := submissionRefusal(tc.err, tc.accepted, tc.unlisted, tc.alone)
			require.Equal(t, tc.ok, ok)
			require.NoError(t, protoutil.SliceEqualError(tc.want, got))
			for _, invalid := range got {
				require.NoError(t, invalid.ValidateAll())
			}
		})
	}
}
