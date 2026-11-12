package coinbase

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/timestamppb"

	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"
	kyccoinbasepb "github.com/code-payments/flipcash2-protobuf-api/generated/go/kyc/coinbase/v1"

	"github.com/code-payments/flipcash2-server/cdp/customers"
	"github.com/code-payments/flipcash2-server/cdp/customers/customerstest"
	"github.com/code-payments/flipcash2-server/protoutil"
)

const offramp = kyccoinbasepb.Product_PRODUCT_OFFRAMP

var (
	deadline1 = time.Date(2027, 1, 1, 0, 0, 0, 0, time.UTC)
	deadline2 = time.Date(2027, 2, 1, 0, 0, 0, 0, time.UTC)
	deadline3 = time.Date(2027, 3, 1, 0, 0, 0, 0, time.UTC)
)

func at(t time.Time) *time.Time { return &t }

func offrampSet() []customers.CapabilityName {
	set, _ := productCapabilities(offramp)
	return set
}

// started is a customer with every offramp capability requested at status,
// plus tradeCrypto unrequested, and the given requirements.
func started(status customers.CapabilityStatus, requirements map[customers.RequirementKey]customers.Requirement) *customers.Customer {
	caps := map[customers.CapabilityName]customers.Capability{
		customers.CapabilityTradeCrypto: {Requested: false, Status: customers.CapabilityUnrequested},
	}
	for _, name := range offrampSet() {
		caps[name] = customers.Capability{Requested: true, Status: status}
	}
	return &customers.Customer{ID: "customer_00000000-0000-4000-8000-000000000001", Capabilities: caps, Requirements: requirements}
}

func withCapability(c *customers.Customer, name customers.CapabilityName, capability customers.Capability) *customers.Customer {
	c.Capabilities[name] = capability
	return c
}

func due(impact ...customers.CapabilityName) customers.Requirement {
	return customers.Requirement{Status: customers.RequirementDue, Impact: impact}
}

func withStatus(status customers.RequirementStatus, deadline *time.Time) customers.Requirement {
	return customers.Requirement{Status: status, Deadline: deadline}
}

func ts(t *time.Time) *timestamppb.Timestamp {
	if t == nil {
		return nil
	}
	return timestamppb.New(*t)
}

func fieldReq(field kyccoinbasepb.Field, reason kyccoinbasepb.Requirement_Reason, deadline *time.Time) *kyccoinbasepb.Requirement {
	return &kyccoinbasepb.Requirement{Kind: &kyccoinbasepb.Requirement_Field{Field: field}, Reason: reason, Deadline: ts(deadline)}
}

func formReq(form kyccoinbasepb.TaxForm, reason kyccoinbasepb.Requirement_Reason, deadline *time.Time) *kyccoinbasepb.Requirement {
	return &kyccoinbasepb.Requirement{Kind: &kyccoinbasepb.Requirement_TaxForm{TaxForm: form}, Reason: reason, Deadline: ts(deadline)}
}

func termsReq(versionID, url string, languages []string, reason kyccoinbasepb.Requirement_Reason, deadline *time.Time) *kyccoinbasepb.Requirement {
	var locales []*commonpb.Locale
	for _, l := range languages {
		locales = append(locales, &commonpb.Locale{Value: l})
	}
	return &kyccoinbasepb.Requirement{
		Kind:     &kyccoinbasepb.Requirement_TermsOfService{TermsOfService: &kyccoinbasepb.TermsOfService{VersionId: versionID, Url: url, Languages: locales}},
		Reason:   reason,
		Deadline: ts(deadline),
	}
}

const tosURL = "https://example.com/terms"

func version(id string, languages ...string) customers.TermsOfService {
	return customers.TermsOfService{VersionID: id, URL: tosURL, Languages: languages}
}

const (
	missing  = kyccoinbasepb.Requirement_MISSING
	rejected = kyccoinbasepb.Requirement_REJECTED
	unknown  = kyccoinbasepb.Requirement_UNKNOWN
)

func TestProductCapabilities(t *testing.T) {
	set, ok := productCapabilities(offramp)
	require.True(t, ok)
	require.Equal(t, []customers.CapabilityName{
		customers.CapabilityCustodyCrypto,
		customers.CapabilityCustodyFiat,
		customers.CapabilityCustodyStablecoin,
		customers.CapabilityTransferStablecoin,
		customers.CapabilityTradeStablecoin,
		customers.CapabilityTransferFiat,
	}, set)

	// A copy: changing it leaves the table as it was.
	set[0] = "changed"
	again, _ := productCapabilities(offramp)
	require.Equal(t, customers.CapabilityCustodyCrypto, again[0])

	_, ok = productCapabilities(kyccoinbasepb.Product_PRODUCT_UNKNOWN)
	require.False(t, ok)
	_, ok = productCapabilities(kyccoinbasepb.Product(99))
	require.False(t, ok)
}

func TestDeriveState_UnknownProduct(t *testing.T) {
	state, notes, err := deriveState(kyccoinbasepb.Product_PRODUCT_UNKNOWN, started(customers.CapabilityActive, nil))
	require.Error(t, err)
	require.Nil(t, state)
	require.Nil(t, notes)
}

func TestDeriveState(t *testing.T) {
	allRequirements := map[customers.RequirementKey]customers.Requirement{
		customers.RequirementFirstName:      due(),
		customers.RequirementTos:            {Status: customers.RequirementDue, TosVersions: []customers.TermsOfService{version("v1", "en")}},
		customers.RequirementTaxAttestation: {Status: customers.RequirementDue, TaxForms: []customers.TaxForm{customers.TaxFormUsW9}},
	}

	for _, tc := range []struct {
		name     string
		customer *customers.Customer
		status   kyccoinbasepb.KycState_Status
		want     []*kyccoinbasepb.Requirement
		notes    []string
	}{
		{
			name:   "no customer",
			status: kyccoinbasepb.KycState_NOT_STARTED,
		},
		{
			name:     "customer with nil maps",
			customer: &customers.Customer{ID: "customer_00000000-0000-4000-8000-000000000001"},
			status:   kyccoinbasepb.KycState_NOT_STARTED,
		},
		{
			name: "customer with requirements but no capabilities map",
			customer: &customers.Customer{Requirements: map[customers.RequirementKey]customers.Requirement{
				customers.RequirementFirstName: due(),
			}},
			status: kyccoinbasepb.KycState_NOT_STARTED,
		},
		{
			name: "one capability of the set missing from the map",
			customer: func() *customers.Customer {
				c := started(customers.CapabilityPending, allRequirements)
				delete(c.Capabilities, customers.CapabilityTransferFiat)
				return c
			}(),
			status: kyccoinbasepb.KycState_NOT_STARTED,
		},
		{
			name: "one capability of the set not requested",
			customer: withCapability(started(customers.CapabilityActive, allRequirements),
				customers.CapabilityCustodyCrypto, customers.Capability{Requested: false, Status: customers.CapabilityUnrequested}),
			status: kyccoinbasepb.KycState_NOT_STARTED,
		},
		{
			name: "none of the set requested, another capability unrequested too",
			customer: func() *customers.Customer {
				c := started(customers.CapabilityUnrequested, allRequirements)
				for name := range c.Capabilities {
					c.Capabilities[name] = customers.Capability{Requested: false, Status: customers.CapabilityUnrequested}
				}
				return c
			}(),
			status: kyccoinbasepb.KycState_NOT_STARTED,
		},
		{
			// A blocked customer as the Sandbox returns one whose set was
			// only partly requested: what was requested is inactive, the
			// rest stays unrequested, and tos and taxAttestation stay listed.
			name: "blocked with part of the set never requested",
			customer: func() *customers.Customer {
				c := started(customers.CapabilityInactive, allRequirements)
				for _, name := range []customers.CapabilityName{customers.CapabilityTransferFiat, customers.CapabilityTradeStablecoin} {
					c.Capabilities[name] = customers.Capability{Requested: false, Status: customers.CapabilityUnrequested}
				}
				return c
			}(),
			status: kyccoinbasepb.KycState_BLOCKED,
		},
		{
			name: "blocked with part of the set missing from the map",
			customer: func() *customers.Customer {
				c := started(customers.CapabilityInactive, allRequirements)
				delete(c.Capabilities, customers.CapabilityTransferFiat)
				return c
			}(),
			status: kyccoinbasepb.KycState_BLOCKED,
		},
		{
			name: "one capability of the set inactive, the rest never requested",
			customer: func() *customers.Customer {
				c := started(customers.CapabilityUnrequested, nil)
				for name := range c.Capabilities {
					c.Capabilities[name] = customers.Capability{Requested: false, Status: customers.CapabilityUnrequested}
				}
				c.Capabilities[customers.CapabilityCustodyCrypto] = customers.Capability{Requested: true, Status: customers.CapabilityInactive}
				return c
			}(),
			status: kyccoinbasepb.KycState_BLOCKED,
		},
		{
			name: "one capability of the set inactive though not requested",
			customer: withCapability(started(customers.CapabilityPending, nil),
				customers.CapabilityCustodyCrypto, customers.Capability{Requested: false, Status: customers.CapabilityInactive}),
			status: kyccoinbasepb.KycState_BLOCKED,
		},
		{
			name: "an inactive capability outside the set, none of the set requested",
			customer: func() *customers.Customer {
				c := started(customers.CapabilityUnrequested, nil)
				for name := range c.Capabilities {
					c.Capabilities[name] = customers.Capability{Requested: false, Status: customers.CapabilityUnrequested}
				}
				c.Capabilities[customers.CapabilityTradeCrypto] = customers.Capability{Requested: true, Status: customers.CapabilityInactive}
				return c
			}(),
			status: kyccoinbasepb.KycState_NOT_STARTED,
		},
		{
			name:     "started, nothing listed, pending",
			customer: started(customers.CapabilityPending, nil),
			status:   kyccoinbasepb.KycState_IN_REVIEW,
		},
		{
			name: "started, only pending requirements",
			customer: started(customers.CapabilityPending, map[customers.RequirementKey]customers.Requirement{
				customers.RequirementFirstName:   withStatus(customers.RequirementPending, nil),
				customers.RequirementDateOfBirth: withStatus(customers.RequirementPending, at(deadline1)),
				customers.RequirementTos:         {Status: customers.RequirementPending, TosVersions: []customers.TermsOfService{version("v1", "en")}},
			}),
			status: kyccoinbasepb.KycState_IN_REVIEW,
		},
		{
			name:     "started, requirements listed",
			customer: started(customers.CapabilityPending, allRequirements),
			status:   kyccoinbasepb.KycState_ACTION_REQUIRED,
			want: []*kyccoinbasepb.Requirement{
				fieldReq(kyccoinbasepb.Field_FIELD_LEGAL_NAME, missing, nil),
				termsReq("v1", tosURL, []string{"en"}, missing, nil),
				formReq(kyccoinbasepb.TaxForm_TAX_FORM_US_W9, missing, nil),
			},
		},
		{
			name:     "blocked, Coinbase still listing tos and taxAttestation",
			customer: started(customers.CapabilityInactive, allRequirements),
			status:   kyccoinbasepb.KycState_BLOCKED,
		},
		{
			name: "one capability inactive, the rest active",
			customer: withCapability(started(customers.CapabilityActive, allRequirements),
				customers.CapabilityTransferFiat, customers.Capability{Requested: true, Status: customers.CapabilityInactive}),
			status: kyccoinbasepb.KycState_BLOCKED,
		},
		{
			name: "blocked with another capability in an unknown status",
			customer: withCapability(started(customers.CapabilityInactive, nil),
				customers.CapabilityCustodyFiat, customers.Capability{Requested: true, Status: "suspended"}),
			status: kyccoinbasepb.KycState_BLOCKED,
			notes:  []string{`capability "custodyFiat" has status "suspended", which this server does not know; read as pending`},
		},
		{
			name: "an inactive capability outside the set does not block",
			customer: withCapability(started(customers.CapabilityPending, nil),
				customers.CapabilityTradeCrypto, customers.Capability{Requested: true, Status: customers.CapabilityInactive}),
			status: kyccoinbasepb.KycState_IN_REVIEW,
		},
		{
			name:     "verified, nothing listed",
			customer: started(customers.CapabilityActive, nil),
			status:   kyccoinbasepb.KycState_VERIFIED,
		},
		{
			name: "verified with newer terms and a deadline",
			customer: started(customers.CapabilityActive, map[customers.RequirementKey]customers.Requirement{
				customers.RequirementTos: {Status: customers.RequirementDue, Deadline: at(deadline1), TosVersions: []customers.TermsOfService{
					{VersionID: "v2", URL: tosURL, Languages: []string{"en", "es"}, Deadline: at(deadline1)},
				}},
			}),
			status: kyccoinbasepb.KycState_VERIFIED,
			want:   []*kyccoinbasepb.Requirement{termsReq("v2", tosURL, []string{"en", "es"}, missing, at(deadline1))},
		},
		{
			name: "one capability in an unknown status, the rest active, reads as pending",
			customer: withCapability(started(customers.CapabilityActive, nil),
				customers.CapabilityCustodyStablecoin, customers.Capability{Requested: true, Status: "paused"}),
			status: kyccoinbasepb.KycState_IN_REVIEW,
			notes:  []string{`capability "custodyStablecoin" has status "paused", which this server does not know; read as pending`},
		},
		{
			name: "requested capability reported unrequested reads as pending, unnoted",
			customer: withCapability(started(customers.CapabilityActive, nil),
				customers.CapabilityCustodyStablecoin, customers.Capability{Requested: true, Status: customers.CapabilityUnrequested}),
			status: kyccoinbasepb.KycState_IN_REVIEW,
		},
		{
			name: "a capability outside the set in an unknown status is not noted",
			customer: withCapability(started(customers.CapabilityActive, nil),
				customers.CapabilityTradeCrypto, customers.Capability{Requested: true, Status: "paused"}),
			status: kyccoinbasepb.KycState_VERIFIED,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			state, notes, err := deriveState(offramp, tc.customer)
			require.NoError(t, err)
			require.NoError(t, state.ValidateAll())
			require.Equal(t, offramp, state.Product)
			require.Equal(t, tc.status, state.Status)
			require.NoError(t, protoutil.SliceEqualError(tc.want, state.Requirements))
			require.Equal(t, tc.notes, notes)
		})
	}
}

func TestDeriveState_Requirements(t *testing.T) {
	custodyFiat := customers.CapabilityCustodyFiat
	tradeCrypto := customers.CapabilityTradeCrypto
	transferCrypto := customers.CapabilityTransferCrypto

	for _, tc := range []struct {
		name         string
		requirements map[customers.RequirementKey]customers.Requirement
		want         []*kyccoinbasepb.Requirement
		notes        []string
	}{
		{
			name: "impact inside, outside, empty and mixed",
			requirements: map[customers.RequirementKey]customers.Requirement{
				customers.RequirementFirstName:      due(custodyFiat),
				customers.RequirementDateOfBirth:    due(tradeCrypto),
				customers.RequirementFullSsn:        due(tradeCrypto, transferCrypto),
				customers.RequirementEmail:          due(),
				customers.RequirementPhoneNumber:    due(custodyFiat, tradeCrypto),
				customers.RequirementTaxAttestation: {Status: customers.RequirementDue, Impact: []customers.CapabilityName{tradeCrypto}, TaxForms: []customers.TaxForm{customers.TaxFormUsW9}},
				customers.RequirementTos:            {Status: customers.RequirementDue, Impact: []customers.CapabilityName{tradeCrypto}, TosVersions: []customers.TermsOfService{version("v1", "en")}},
			},
			want: []*kyccoinbasepb.Requirement{
				fieldReq(kyccoinbasepb.Field_FIELD_LEGAL_NAME, missing, nil),
				fieldReq(kyccoinbasepb.Field_FIELD_EMAIL, missing, nil),
				fieldReq(kyccoinbasepb.Field_FIELD_PHONE_NUMBER, missing, nil),
			},
		},
		{
			name: "statuses",
			requirements: map[customers.RequirementKey]customers.Requirement{
				customers.RequirementFirstName:        withStatus(customers.RequirementDue, nil),
				customers.RequirementDateOfBirth:      withStatus(customers.RequirementPastDue, at(deadline1)),
				customers.RequirementFullSsn:          withStatus(customers.RequirementRejected, nil),
				customers.RequirementEmail:            withStatus(customers.RequirementPending, nil),
				customers.RequirementPhoneNumber:      withStatus("under_consideration", nil),
				customers.RequirementPurposeOfAccount: withStatus("", nil),
			},
			want: []*kyccoinbasepb.Requirement{
				fieldReq(kyccoinbasepb.Field_FIELD_LEGAL_NAME, missing, nil),
				fieldReq(kyccoinbasepb.Field_FIELD_DATE_OF_BIRTH, missing, at(deadline1)),
				fieldReq(kyccoinbasepb.Field_FIELD_SSN, rejected, nil),
				fieldReq(kyccoinbasepb.Field_FIELD_PHONE_NUMBER, unknown, nil),
				fieldReq(kyccoinbasepb.Field_FIELD_PURPOSE_OF_ACCOUNT, unknown, nil),
			},
			notes: []string{
				`requirement "phoneNumber" has status "under_consideration", which this server does not know; listed with reason UNKNOWN`,
				`requirement "purposeOfAccount" has status "", which this server does not know; listed with reason UNKNOWN`,
			},
		},
		{
			name: "parts merge: rejected wins, earliest deadline",
			requirements: map[customers.RequirementKey]customers.Requirement{
				customers.RequirementFirstName:         withStatus(customers.RequirementDue, at(deadline2)),
				customers.RequirementLastName:          withStatus(customers.RequirementRejected, at(deadline3)),
				customers.RequirementAddressLine1:      withStatus(customers.RequirementDue, nil),
				customers.RequirementAddressCity:       withStatus(customers.RequirementPastDue, at(deadline3)),
				customers.RequirementAddressPostalCode: withStatus(customers.RequirementPending, at(deadline1)),
				customers.RequirementAddressLine2:      withStatus("odd", at(deadline2)),
			},
			want: []*kyccoinbasepb.Requirement{
				fieldReq(kyccoinbasepb.Field_FIELD_LEGAL_NAME, rejected, at(deadline2)),
				fieldReq(kyccoinbasepb.Field_FIELD_RESIDENTIAL_ADDRESS, missing, at(deadline2)),
			},
			notes: []string{`requirement "addressLine2" has status "odd", which this server does not know; listed with reason UNKNOWN`},
		},
		{
			name: "parts all of unknown status stay unknown",
			requirements: map[customers.RequirementKey]customers.Requirement{
				customers.RequirementFirstName: withStatus("odd", nil),
				customers.RequirementLastName:  withStatus("odd", nil),
			},
			want: []*kyccoinbasepb.Requirement{fieldReq(kyccoinbasepb.Field_FIELD_LEGAL_NAME, unknown, nil)},
			notes: []string{
				`requirement "firstName" has status "odd", which this server does not know; listed with reason UNKNOWN`,
				`requirement "lastName" has status "odd", which this server does not know; listed with reason UNKNOWN`,
			},
		},
		{
			name: "keys this API cannot collect are one FIELD_UNKNOWN",
			requirements: map[customers.RequirementKey]customers.Requirement{
				customers.RequirementSsnLast4:         withStatus(customers.RequirementDue, at(deadline3)),
				customers.RequirementCitizenship:      withStatus(customers.RequirementDue, nil),
				customers.RequirementEmploymentStatus: withStatus(customers.RequirementRejected, at(deadline2)),
				customers.RequirementOccupation:       withStatus(customers.RequirementDue, nil),
				customers.RequirementExpectedVolume:   withStatus(customers.RequirementPending, at(deadline1)),
				"somethingNew":                        withStatus(customers.RequirementDue, nil),
				customers.RequirementEmail:            withStatus(customers.RequirementDue, nil),
			},
			want: []*kyccoinbasepb.Requirement{
				fieldReq(kyccoinbasepb.Field_FIELD_EMAIL, missing, nil),
				fieldReq(kyccoinbasepb.Field_FIELD_UNKNOWN, rejected, at(deadline2)),
			},
			notes: []string{
				`requirement "citizenship" is not one this API can collect; listed as FIELD_UNKNOWN`,
				`requirement "employmentStatus" is not one this API can collect; listed as FIELD_UNKNOWN`,
				`requirement "occupation" is not one this API can collect; listed as FIELD_UNKNOWN`,
				`requirement "somethingNew" is not one this API can collect; listed as FIELD_UNKNOWN`,
				`requirement "ssnLast4" is not one this API can collect; listed as FIELD_UNKNOWN`,
			},
		},
		{
			name: "terms: each version once, first wins, deadline its own and never the requirement's",
			requirements: map[customers.RequirementKey]customers.Requirement{
				customers.RequirementTos: {Status: customers.RequirementPastDue, Deadline: at(deadline1), TosVersions: []customers.TermsOfService{
					{VersionID: "v2", URL: tosURL + "/2", Languages: []string{"en", "es-419"}, Deadline: at(deadline3)},
					{VersionID: "v1", URL: tosURL + "/1", Languages: []string{"en"}},
					{VersionID: "v2", URL: tosURL + "/other", Languages: []string{"fr"}},
				}},
			},
			want: []*kyccoinbasepb.Requirement{
				termsReq("v2", tosURL+"/2", []string{"en", "es-419"}, missing, at(deadline3)),
				termsReq("v1", tosURL+"/1", []string{"en"}, missing, nil),
			},
		},
		{
			name: "terms rejected",
			requirements: map[customers.RequirementKey]customers.Requirement{
				customers.RequirementTos: {Status: customers.RequirementRejected, TosVersions: []customers.TermsOfService{version("v1", "en")}},
			},
			want: []*kyccoinbasepb.Requirement{termsReq("v1", tosURL, []string{"en"}, rejected, nil)},
		},
		{
			name: "terms in an unknown status",
			requirements: map[customers.RequirementKey]customers.Requirement{
				customers.RequirementTos: {Status: "odd", TosVersions: []customers.TermsOfService{version("v1", "en")}},
			},
			want:  []*kyccoinbasepb.Requirement{termsReq("v1", tosURL, []string{"en"}, unknown, nil)},
			notes: []string{`requirement "tos" has status "odd", which this server does not know; listed with reason UNKNOWN`},
		},
		{
			name: "terms languages that are not locales are dropped",
			requirements: map[customers.RequirementKey]customers.Requirement{
				customers.RequirementTos: {Status: customers.RequirementDue, TosVersions: []customers.TermsOfService{
					version("v1", "en", "e", "en_US", "", "pt-BR"),
				}},
			},
			want: []*kyccoinbasepb.Requirement{termsReq("v1", tosURL, []string{"en", "pt-BR"}, missing, nil)},
			notes: []string{
				`terms of service version "v1" lists language "e", which is not a locale; left out`,
				`terms of service version "v1" lists language "en_US", which is not a locale; left out`,
				`terms of service version "v1" lists language "", which is not a locale; left out`,
			},
		},
		{
			name: "terms that cannot be represented are FIELD_UNKNOWN",
			requirements: map[customers.RequirementKey]customers.Requirement{
				customers.RequirementFirstName: withStatus(customers.RequirementDue, nil),
				customers.RequirementTos: {Status: customers.RequirementDue, Deadline: at(deadline2), TosVersions: []customers.TermsOfService{
					version("v1", "en"),
					version("no-languages"),
					version("bad-languages", "e"),
					version("", "en"),
					version(strings.Repeat("x", 65), "en"),
					{VersionID: "no-url", Languages: []string{"en"}},
					{VersionID: "long-url", URL: "https://example.com/" + strings.Repeat("x", 2048), Languages: []string{"en"}},
				}},
			},
			want: []*kyccoinbasepb.Requirement{
				fieldReq(kyccoinbasepb.Field_FIELD_LEGAL_NAME, missing, nil),
				fieldReq(kyccoinbasepb.Field_FIELD_UNKNOWN, missing, at(deadline2)),
				termsReq("v1", tosURL, []string{"en"}, missing, nil),
			},
			notes: []string{
				`terms of service version "no-languages" cannot be represented; listed as FIELD_UNKNOWN`,
				`terms of service version "bad-languages" lists language "e", which is not a locale; left out`,
				`terms of service version "bad-languages" cannot be represented; listed as FIELD_UNKNOWN`,
				`terms of service version "" cannot be represented; listed as FIELD_UNKNOWN`,
				`terms of service version "` + strings.Repeat("x", 64) + `"... cannot be represented; listed as FIELD_UNKNOWN`,
				`terms of service version "no-url" cannot be represented; listed as FIELD_UNKNOWN`,
				`terms of service version "long-url" cannot be represented; listed as FIELD_UNKNOWN`,
			},
		},
		{
			name: "terms with no versions are FIELD_UNKNOWN, merged with an unknown key",
			requirements: map[customers.RequirementKey]customers.Requirement{
				customers.RequirementOccupation: withStatus(customers.RequirementDue, at(deadline3)),
				customers.RequirementTos:        {Status: customers.RequirementRejected, Deadline: at(deadline1)},
			},
			want: []*kyccoinbasepb.Requirement{fieldReq(kyccoinbasepb.Field_FIELD_UNKNOWN, rejected, at(deadline1))},
			notes: []string{
				`requirement "occupation" is not one this API can collect; listed as FIELD_UNKNOWN`,
				`requirement "tos" names no terms of service version; listed as FIELD_UNKNOWN`,
			},
		},
		{
			name: "pending terms are not listed",
			requirements: map[customers.RequirementKey]customers.Requirement{
				customers.RequirementTos: {Status: customers.RequirementPending},
			},
		},
		{
			name: "tax forms: W-9 once, unknown forms once",
			requirements: map[customers.RequirementKey]customers.Requirement{
				customers.RequirementTaxAttestation: {Status: customers.RequirementDue, Deadline: at(deadline1), TaxForms: []customers.TaxForm{
					"us_w8ben", customers.TaxFormUsW9, "other", customers.TaxFormUsW9,
				}},
			},
			want: []*kyccoinbasepb.Requirement{
				formReq(kyccoinbasepb.TaxForm_TAX_FORM_US_W9, missing, at(deadline1)),
				formReq(kyccoinbasepb.TaxForm_TAX_FORM_UNKNOWN, missing, at(deadline1)),
			},
			notes: []string{
				`tax form "us_w8ben" is not one this server knows; listed as TAX_FORM_UNKNOWN`,
				`tax form "other" is not one this server knows; listed as TAX_FORM_UNKNOWN`,
			},
		},
		{
			name: "tax attestation naming no form",
			requirements: map[customers.RequirementKey]customers.Requirement{
				customers.RequirementTaxAttestation: {Status: customers.RequirementRejected},
			},
			want:  []*kyccoinbasepb.Requirement{formReq(kyccoinbasepb.TaxForm_TAX_FORM_UNKNOWN, rejected, nil)},
			notes: []string{`requirement "taxAttestation" names no tax form; listed as TAX_FORM_UNKNOWN`},
		},
		{
			name: "order: fields by number, FIELD_UNKNOWN last, terms in Coinbase's order, then forms",
			requirements: map[customers.RequirementKey]customers.Requirement{
				customers.RequirementTaxAttestation: {Status: customers.RequirementDue, TaxForms: []customers.TaxForm{"other", customers.TaxFormUsW9}},
				customers.RequirementTos:            {Status: customers.RequirementDue, TosVersions: []customers.TermsOfService{version("b", "en"), version("a", "en")}},
				customers.RequirementSourceOfFunds:  due(),
				customers.RequirementCitizenship:    due(),
				customers.RequirementFirstName:      due(),
				customers.RequirementAddressCity:    due(),
				customers.RequirementFullSsn:        due(),
			},
			want: []*kyccoinbasepb.Requirement{
				fieldReq(kyccoinbasepb.Field_FIELD_LEGAL_NAME, missing, nil),
				fieldReq(kyccoinbasepb.Field_FIELD_RESIDENTIAL_ADDRESS, missing, nil),
				fieldReq(kyccoinbasepb.Field_FIELD_SSN, missing, nil),
				fieldReq(kyccoinbasepb.Field_FIELD_SOURCE_OF_FUNDS, missing, nil),
				fieldReq(kyccoinbasepb.Field_FIELD_UNKNOWN, missing, nil),
				termsReq("b", tosURL, []string{"en"}, missing, nil),
				termsReq("a", tosURL, []string{"en"}, missing, nil),
				formReq(kyccoinbasepb.TaxForm_TAX_FORM_US_W9, missing, nil),
				formReq(kyccoinbasepb.TaxForm_TAX_FORM_UNKNOWN, missing, nil),
			},
			notes: []string{
				`requirement "citizenship" is not one this API can collect; listed as FIELD_UNKNOWN`,
				`tax form "other" is not one this server knows; listed as TAX_FORM_UNKNOWN`,
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for i := 0; i < 5; i++ { // map order must not show
				state, notes, err := deriveState(offramp, started(customers.CapabilityPending, tc.requirements))
				require.NoError(t, err)
				require.NoError(t, state.ValidateAll())
				if len(tc.want) > 0 {
					require.Equal(t, kyccoinbasepb.KycState_ACTION_REQUIRED, state.Status)
				} else {
					require.Equal(t, kyccoinbasepb.KycState_IN_REVIEW, state.Status)
				}
				require.NoError(t, protoutil.SliceEqualError(tc.want, state.Requirements))
				require.Equal(t, tc.notes, notes)
			}
		})
	}
}

func TestDeriveState_EveryIdentityKey(t *testing.T) {
	for key, field := range map[customers.RequirementKey]kyccoinbasepb.Field{
		customers.RequirementFirstName:          kyccoinbasepb.Field_FIELD_LEGAL_NAME,
		customers.RequirementLastName:           kyccoinbasepb.Field_FIELD_LEGAL_NAME,
		customers.RequirementDateOfBirth:        kyccoinbasepb.Field_FIELD_DATE_OF_BIRTH,
		customers.RequirementAddressLine1:       kyccoinbasepb.Field_FIELD_RESIDENTIAL_ADDRESS,
		customers.RequirementAddressLine2:       kyccoinbasepb.Field_FIELD_RESIDENTIAL_ADDRESS,
		customers.RequirementAddressCity:        kyccoinbasepb.Field_FIELD_RESIDENTIAL_ADDRESS,
		customers.RequirementAddressSubdivision: kyccoinbasepb.Field_FIELD_RESIDENTIAL_ADDRESS,
		customers.RequirementAddressPostalCode:  kyccoinbasepb.Field_FIELD_RESIDENTIAL_ADDRESS,
		customers.RequirementAddressCountryCode: kyccoinbasepb.Field_FIELD_RESIDENTIAL_ADDRESS,
		customers.RequirementFullSsn:            kyccoinbasepb.Field_FIELD_SSN,
		customers.RequirementEmail:              kyccoinbasepb.Field_FIELD_EMAIL,
		customers.RequirementPhoneNumber:        kyccoinbasepb.Field_FIELD_PHONE_NUMBER,
		customers.RequirementPurposeOfAccount:   kyccoinbasepb.Field_FIELD_PURPOSE_OF_ACCOUNT,
		customers.RequirementSourceOfFunds:      kyccoinbasepb.Field_FIELD_SOURCE_OF_FUNDS,
	} {
		t.Run(string(key), func(t *testing.T) {
			state, notes, err := deriveState(offramp, started(customers.CapabilityPending, map[customers.RequirementKey]customers.Requirement{key: due()}))
			require.NoError(t, err)
			require.NoError(t, state.ValidateAll())
			require.Empty(t, notes)
			require.NoError(t, protoutil.SliceEqualError([]*kyccoinbasepb.Requirement{fieldReq(field, missing, nil)}, state.Requirements))
		})
	}
}

func TestDeriveState_TermsLanguageCap(t *testing.T) {
	var languages []string
	for i := 0; i < 66; i++ {
		languages = append(languages, fmt.Sprintf("en-%d", i))
	}
	state, notes, err := deriveState(offramp, started(customers.CapabilityPending, map[customers.RequirementKey]customers.Requirement{
		customers.RequirementTos: {Status: customers.RequirementDue, TosVersions: []customers.TermsOfService{version("v1", languages...)}},
	}))
	require.NoError(t, err)
	require.NoError(t, state.ValidateAll())
	require.NoError(t, protoutil.SliceEqualError([]*kyccoinbasepb.Requirement{termsReq("v1", tosURL, languages[:64], missing, nil)}, state.Requirements))
	require.Equal(t, []string{
		`terms of service version "v1" lists language "en-64" past the first 64; left out`,
		`terms of service version "v1" lists language "en-65" past the first 64; left out`,
	}, notes)
}

func TestDeriveState_RequirementCap(t *testing.T) {
	var versions []customers.TermsOfService
	for i := 0; i < 70; i++ {
		versions = append(versions, version(fmt.Sprintf("v%02d", i), "en"))
	}
	requirements := map[customers.RequirementKey]customers.Requirement{
		customers.RequirementTos:            {Status: customers.RequirementDue, TosVersions: versions},
		customers.RequirementTaxAttestation: {Status: customers.RequirementDue, TaxForms: []customers.TaxForm{customers.TaxFormUsW9}},
	}
	for key := range fieldOfKey {
		requirements[key] = due()
	}
	state, notes, err := deriveState(offramp, started(customers.CapabilityPending, requirements))
	require.NoError(t, err)
	require.NoError(t, state.ValidateAll())
	require.Len(t, state.Requirements, 64)

	// Eight fields, then the first 55 versions in order, then the form.
	require.Equal(t, kyccoinbasepb.Field_FIELD_SOURCE_OF_FUNDS, state.Requirements[7].GetField())
	for i := 0; i < 55; i++ {
		require.Equal(t, fmt.Sprintf("v%02d", i), state.Requirements[8+i].GetTermsOfService().GetVersionId())
	}
	require.Equal(t, kyccoinbasepb.TaxForm_TAX_FORM_US_W9, state.Requirements[63].GetTaxForm())
	require.Equal(t, []string{"70 terms of service versions listed, more than fit; the last 15 are left out"}, notes)
}

// TestDeriveState_Fake walks the in-process fake of Coinbase with a real
// client, building every request with this package's functions, and reads the
// state at each point.
func TestDeriveState_Fake(t *testing.T) {
	now := time.Date(2026, 11, 11, 11, 0, 0, 0, time.UTC)
	fake := customerstest.NewServer(customerstest.WithClock(func() time.Time { return now }))
	defer fake.Close()
	client := fake.Client()
	ctx := context.Background()
	set := offrampSet()

	stateOf := func(c *customers.Customer) *kyccoinbasepb.KycState {
		t.Helper()
		state, notes, err := deriveState(offramp, c)
		require.NoError(t, err)
		require.NoError(t, state.ValidateAll())
		require.Empty(t, notes)
		return state
	}
	requireState := func(c *customers.Customer, status kyccoinbasepb.KycState_Status, want ...*kyccoinbasepb.Requirement) {
		t.Helper()
		state := stateOf(c)
		require.Equal(t, status, state.Status)
		require.NoError(t, protoutil.SliceEqualError(want, state.Requirements))
	}
	submit := func(customerID string, current *customers.Customer, req *kyccoinbasepb.SubmitKycRequest) (*customers.Customer, []*kyccoinbasepb.SubmitKycResponse_InvalidTermsOfServiceAcceptance, error) {
		t.Helper()
		require.NoError(t, validateSubmission(req))
		require.False(t, isEmptySubmission(req))
		require.Empty(t, checkIndividual(req.Individual, now))
		invalid, accepted, unlisted := checkTermsAcceptances(req.TermsOfServiceAcceptances, current, now)
		require.Empty(t, invalid)
		input, err := toCustomerInput(req, accepted, now, testIP)
		require.NoError(t, err)
		updated, err := client.Update(ctx, customerID, "", input)
		if err != nil {
			refusal, ok := submissionRefusal(err, accepted, unlisted, submitsAcceptancesAlone(req))
			require.True(t, ok, "%v", err)
			return nil, refusal, err
		}
		return updated, nil, nil
	}
	start := func(userID *commonpb.UserId) *customers.Customer {
		t.Helper()
		c, err := client.Create(ctx, customerIdempotencyKey(userID), nil)
		require.NoError(t, err)
		requireState(c, kyccoinbasepb.KycState_NOT_STARTED)
		input, err := startInput(set, testIP)
		require.NoError(t, err)
		c, err = client.Update(ctx, c.ID, "", input)
		require.NoError(t, err)
		return c
	}
	identity := func(ssn string) *kyccoinbasepb.Individual {
		individual := fullIndividual()
		individual.Ssn = &kyccoinbasepb.Ssn{Value: strings.ReplaceAll(ssn, "-", "")}
		return individual
	}
	w9 := []*kyccoinbasepb.TaxAttestation{w9Attestation(false, true)}
	terms := termsReq(customerstest.TosVersionID, customerstest.TosURL, customerstest.TosLanguages, missing, nil)

	// No customer, then a bare one: not started.
	requireState(nil, kyccoinbasepb.KycState_NOT_STARTED)
	c := start(testUserID(1))
	id := c.ID

	// Started: every field and the W-9, no terms before the address.
	requireState(c, kyccoinbasepb.KycState_ACTION_REQUIRED,
		fieldReq(kyccoinbasepb.Field_FIELD_LEGAL_NAME, missing, nil),
		fieldReq(kyccoinbasepb.Field_FIELD_DATE_OF_BIRTH, missing, nil),
		fieldReq(kyccoinbasepb.Field_FIELD_RESIDENTIAL_ADDRESS, missing, nil),
		fieldReq(kyccoinbasepb.Field_FIELD_SSN, missing, nil),
		fieldReq(kyccoinbasepb.Field_FIELD_EMAIL, missing, nil),
		fieldReq(kyccoinbasepb.Field_FIELD_PHONE_NUMBER, missing, nil),
		fieldReq(kyccoinbasepb.Field_FIELD_PURPOSE_OF_ACCOUNT, missing, nil),
		fieldReq(kyccoinbasepb.Field_FIELD_SOURCE_OF_FUNDS, missing, nil),
		formReq(kyccoinbasepb.TaxForm_TAX_FORM_US_W9, missing, nil),
	)

	// An acceptance before the address is not asked for.
	_, refusal, err := submit(id, c, &kyccoinbasepb.SubmitKycRequest{
		TermsOfServiceAcceptances: []*kyccoinbasepb.TermsOfServiceAcceptance{acceptance(customerstest.TosVersionID, "en")},
	})
	require.Error(t, err)
	require.NoError(t, protoutil.SliceEqualError(invalidTerms(notAsked, customerstest.TosVersionID), refusal))

	// Identity and the W-9: the terms are listed, the rest is in review.
	c, _, err = submit(id, c, &kyccoinbasepb.SubmitKycRequest{Individual: identity(customerstest.SsnApprove), TaxAttestations: w9})
	require.NoError(t, err)
	requireState(c, kyccoinbasepb.KycState_ACTION_REQUIRED, terms)

	// The acceptance in another case than Coinbase lists is refused unsent.
	invalid, _, _ := checkTermsAcceptances([]*kyccoinbasepb.TermsOfServiceAcceptance{acceptance(customerstest.TosVersionID, "EN")}, c, now)
	require.NoError(t, protoutil.SliceEqualError(invalidTerms(unsupportedLanguage, customerstest.TosVersionID), invalid))

	// The acceptance, as listed: in review.
	c, _, err = submit(id, c, &kyccoinbasepb.SubmitKycRequest{
		TermsOfServiceAcceptances: []*kyccoinbasepb.TermsOfServiceAcceptance{acceptance(customerstest.TosVersionID, "en")},
	})
	require.NoError(t, err)
	requireState(c, kyccoinbasepb.KycState_IN_REVIEW)

	// Accepted, the version is no longer listed, so a retry is sent as given:
	// as listed it changes nothing, and in another case Coinbase refuses it.
	again, _, err := submit(id, c, &kyccoinbasepb.SubmitKycRequest{
		TermsOfServiceAcceptances: []*kyccoinbasepb.TermsOfServiceAcceptance{acceptance(customerstest.TosVersionID, "en")},
	})
	require.NoError(t, err)
	requireState(again, kyccoinbasepb.KycState_IN_REVIEW)
	_, refusal, err = submit(id, c, &kyccoinbasepb.SubmitKycRequest{
		TermsOfServiceAcceptances: []*kyccoinbasepb.TermsOfServiceAcceptance{acceptance(customerstest.TosVersionID, "EN")},
	})
	require.Error(t, err)
	require.NoError(t, protoutil.SliceEqualError(invalidTerms(unsupportedLanguage, customerstest.TosVersionID), refusal))

	snapshot, ok := fake.Inspect(id)
	require.True(t, ok)
	require.Equal(t, customers.TosAcceptance{VersionID: customerstest.TosVersionID, Language: "en", AcceptedAt: now.Add(-acceptedAtBackdate)},
		snapshot.TosAccepted[customerstest.TosVersionID])
	require.Equal(t, now.Add(-acceptedAtBackdate), snapshot.W9.AcceptedAt)
	require.Equal(t, "203.0.113.42", snapshot.RequesterIPAddress)
	require.Equal(t, "000-00-0000", snapshot.Individual.FullSsn)
	require.Equal(t, &customers.DateOfBirth{Year: "1919", Month: "01", Day: "01"}, snapshot.Individual.DateOfBirth)

	// Reviewed: verified.
	require.True(t, fake.CompleteReview(id))
	c, err = client.Get(ctx, id)
	require.NoError(t, err)
	requireState(c, kyccoinbasepb.KycState_VERIFIED)

	// A declined customer: every identity field rejected.
	declined := start(testUserID(2))
	declined, _, err = submit(declined.ID, declined, &kyccoinbasepb.SubmitKycRequest{Individual: identity(customerstest.SsnDecline), TaxAttestations: w9})
	require.NoError(t, err)
	declined, _, err = submit(declined.ID, declined, &kyccoinbasepb.SubmitKycRequest{
		TermsOfServiceAcceptances: []*kyccoinbasepb.TermsOfServiceAcceptance{acceptance(customerstest.TosVersionID, "en")},
	})
	require.NoError(t, err)
	requireState(declined, kyccoinbasepb.KycState_IN_REVIEW)
	require.True(t, fake.CompleteReview(declined.ID))
	declined, err = client.Get(ctx, declined.ID)
	require.NoError(t, err)
	requireState(declined, kyccoinbasepb.KycState_ACTION_REQUIRED,
		fieldReq(kyccoinbasepb.Field_FIELD_LEGAL_NAME, rejected, nil),
		fieldReq(kyccoinbasepb.Field_FIELD_DATE_OF_BIRTH, rejected, nil),
		fieldReq(kyccoinbasepb.Field_FIELD_RESIDENTIAL_ADDRESS, rejected, nil),
		fieldReq(kyccoinbasepb.Field_FIELD_SSN, rejected, nil),
		fieldReq(kyccoinbasepb.Field_FIELD_EMAIL, rejected, nil),
		fieldReq(kyccoinbasepb.Field_FIELD_PHONE_NUMBER, rejected, nil),
	)

	// A blocked customer: blocked on the submission, with nothing listed
	// although Coinbase still lists the terms and the W-9.
	blocked := start(testUserID(3))
	blocked, _, err = submit(blocked.ID, blocked, &kyccoinbasepb.SubmitKycRequest{Individual: identity(customerstest.SsnBlock)})
	require.NoError(t, err)
	require.Contains(t, blocked.Requirements, customers.RequirementTos)
	require.Contains(t, blocked.Requirements, customers.RequirementTaxAttestation)
	requireState(blocked, kyccoinbasepb.KycState_BLOCKED)

	require.Len(t, fake.CustomerIDs(), 3)
}
