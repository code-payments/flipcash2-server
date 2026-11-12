package coinbase

import (
	"errors"
	"fmt"
	"net/http"
	"net/netip"
	"slices"
	"time"

	kyccoinbasepb "github.com/code-payments/flipcash2-protobuf-api/generated/go/kyc/coinbase/v1"

	"github.com/code-payments/flipcash2-server/cdp/customers"
)

// acceptedAtBackdate is how far before the time a submission was received its
// acceptances and attestations are stamped.
//
// Coinbase refuses an acceptedAt in its future with a generic 400, so a server
// clock even slightly ahead of Coinbase's would fail every acceptance and
// attestation. The proto says the server records when it received an
// acceptance as the time the user accepted; the user agreed before the
// request was sent, so a stamp a few seconds earlier than its receipt is
// small against that and still after the user saw the document.
const acceptedAtBackdate = 10 * time.Second

// validateSubmission refuses a request no client should send: two acceptances
// of one terms version, or two attestations to one tax form. The proto asks
// for one per version and one per form, and what Coinbase would make of two is
// unknown. The error names neither the version nor anything submitted.
func validateSubmission(req *kyccoinbasepb.SubmitKycRequest) error {
	versions := map[string]bool{}
	for _, a := range req.GetTermsOfServiceAcceptances() {
		if versions[a.GetVersionId()] {
			return errors.New("two acceptances of one terms of service version")
		}
		versions[a.GetVersionId()] = true
	}
	forms := map[kyccoinbasepb.TaxForm]bool{}
	for _, a := range req.GetTaxAttestations() {
		form, ok := taxFormOf(a)
		if !ok {
			continue
		}
		if forms[form] {
			return errors.New("two attestations to one tax form")
		}
		forms[form] = true
	}
	return nil
}

// taxFormOf names the form an attestation is to. False for one with no form,
// which the proto's validation refuses and toCustomerInput reports.
func taxFormOf(a *kyccoinbasepb.TaxAttestation) (kyccoinbasepb.TaxForm, bool) {
	switch a.GetForm().(type) {
	case *kyccoinbasepb.TaxAttestation_UsW9_:
		return kyccoinbasepb.TaxForm_TAX_FORM_US_W9, true
	}
	return kyccoinbasepb.TaxForm_TAX_FORM_UNKNOWN, false
}

// isEmptySubmission reports whether req submits nothing: no field of
// Individual set, no acceptance and no attestation.
func isEmptySubmission(req *kyccoinbasepb.SubmitKycRequest) bool {
	return !setsIndividual(req.GetIndividual()) &&
		len(req.GetTermsOfServiceAcceptances()) == 0 &&
		len(req.GetTaxAttestations()) == 0
}

// submitsAcceptancesAlone reports whether req submits terms acceptances and
// nothing else: no field of Individual set and no attestation.
func submitsAcceptancesAlone(req *kyccoinbasepb.SubmitKycRequest) bool {
	return len(req.GetTermsOfServiceAcceptances()) > 0 &&
		!setsIndividual(req.GetIndividual()) &&
		len(req.GetTaxAttestations()) == 0
}

// setsIndividual reports whether individual sets at least one field. An enum
// left at its UNKNOWN (zero) value is unset.
func setsIndividual(individual *kyccoinbasepb.Individual) bool {
	return individual.GetLegalName() != nil ||
		individual.GetDateOfBirth() != nil ||
		individual.GetAddress() != nil ||
		individual.GetSsn() != nil ||
		individual.GetEmail() != nil ||
		individual.GetPhoneNumber() != nil ||
		individual.GetPurposeOfAccount() != kyccoinbasepb.PurposeOfAccount_PURPOSE_OF_ACCOUNT_UNKNOWN ||
		individual.GetSourceOfFunds() != kyccoinbasepb.SourceOfFunds_SOURCE_OF_FUNDS_UNKNOWN
}

// stampedAt is the time a submission received at receivedAt stamps on its
// acceptances and attestations (see acceptedAtBackdate).
func stampedAt(receivedAt time.Time) time.Time {
	return receivedAt.Add(-acceptedAtBackdate).UTC()
}

// checkTermsAcceptances judges each acceptance against the terms the
// customer's current state lists (its tos requirement's versions, whatever
// that requirement's impact or status; a nil customer or one listing none
// lists nothing).
//
// invalid: acceptances refused without asking Coinbase. accepted: what to
// send, in request order. unlisted: version IDs sent although the customer
// lists no such version.
//
// An acceptance of a listed version must name one of the version's languages
// exactly as Coinbase lists it, which is how the state gives it to the client.
// Any other is UNSUPPORTED_LANGUAGE, one that differs only in case included.
// Coinbase compares the tag as written (the Sandbox refuses "EN" for an
// accepted version published in "en"), and an acceptance of a version no
// longer listed is sent as given, so a tag matched loosely here would pass
// the first time and be refused on the retry of an acceptance that landed.
//
// An acceptance of a version not listed cannot be judged here, because
// Coinbase does not return what a customer has accepted: it is either a retry
// of an acceptance already made, which Coinbase answers as a no-op when it
// names a language the version is published in, or one never asked for,
// which Coinbase refuses with a 400. So it is sent as given
// and its ID returned in unlisted, for submissionRefusal to read a refusal by.
// Coinbase settles it either way, which is why the store keeps no list of the
// versions a user has accepted: one would only repeat Coinbase's answer, and
// could drift from it (an acceptance can land at Coinbase and its response be
// lost).
//
// Every acceptance sent is stamped receivedAt − acceptedAtBackdate, in UTC.
func checkTermsAcceptances(acceptances []*kyccoinbasepb.TermsOfServiceAcceptance, customer *customers.Customer, receivedAt time.Time) (invalid []*kyccoinbasepb.SubmitKycResponse_InvalidTermsOfServiceAcceptance, accepted []customers.TosAcceptance, unlisted []string) {
	var listed []customers.TermsOfService
	if customer != nil {
		listed = customer.Requirements[customers.RequirementTos].TosVersions
	}
	acceptedAt := stampedAt(receivedAt)

	for _, a := range acceptances {
		versionID := a.GetVersionId()
		language := a.GetLanguage().GetValue()

		version, ok := findVersion(listed, versionID)
		if !ok {
			accepted = append(accepted, customers.TosAcceptance{VersionID: versionID, Language: language, AcceptedAt: acceptedAt})
			unlisted = append(unlisted, versionID)
			continue
		}

		if !slices.Contains(version.Languages, language) {
			invalid = append(invalid, &kyccoinbasepb.SubmitKycResponse_InvalidTermsOfServiceAcceptance{
				VersionId: versionID,
				Reason:    kyccoinbasepb.SubmitKycResponse_InvalidTermsOfServiceAcceptance_UNSUPPORTED_LANGUAGE,
			})
			continue
		}
		accepted = append(accepted, customers.TosAcceptance{VersionID: versionID, Language: language, AcceptedAt: acceptedAt})
	}
	return invalid, accepted, unlisted
}

// findVersion finds a listed version by ID, the first if Coinbase lists one
// twice.
func findVersion(listed []customers.TermsOfService, versionID string) (customers.TermsOfService, bool) {
	for _, v := range listed {
		if v.VersionID == versionID {
			return v, true
		}
	}
	return customers.TermsOfService{}, false
}

// purposesOfAccount are Coinbase's purposeOfAccount values, each the proto
// name lowercased with its prefix removed. Every defined value but UNKNOWN,
// which is the field left unset, has one.
var purposesOfAccount = map[kyccoinbasepb.PurposeOfAccount]string{
	kyccoinbasepb.PurposeOfAccount_PURPOSE_OF_ACCOUNT_INVESTING:                    "investing",
	kyccoinbasepb.PurposeOfAccount_PURPOSE_OF_ACCOUNT_LONG_TERM_INVESTMENT:         "long_term_investment",
	kyccoinbasepb.PurposeOfAccount_PURPOSE_OF_ACCOUNT_SHORT_TERM_INVESTMENT:        "short_term_investment",
	kyccoinbasepb.PurposeOfAccount_PURPOSE_OF_ACCOUNT_TRADING:                      "trading",
	kyccoinbasepb.PurposeOfAccount_PURPOSE_OF_ACCOUNT_TRADING_ON_COINBASE:          "trading_on_coinbase",
	kyccoinbasepb.PurposeOfAccount_PURPOSE_OF_ACCOUNT_TRADING_ON_OTHER_EXCHANGES:   "trading_on_other_exchanges",
	kyccoinbasepb.PurposeOfAccount_PURPOSE_OF_ACCOUNT_DAY_TRADING:                  "day_trading",
	kyccoinbasepb.PurposeOfAccount_PURPOSE_OF_ACCOUNT_ARBITRAGE_TRADING:            "arbitrage_trading",
	kyccoinbasepb.PurposeOfAccount_PURPOSE_OF_ACCOUNT_ONLINE_PURCHASES:             "online_purchases",
	kyccoinbasepb.PurposeOfAccount_PURPOSE_OF_ACCOUNT_ONLINE_PAYMENTS:              "online_payments",
	kyccoinbasepb.PurposeOfAccount_PURPOSE_OF_ACCOUNT_PAYMENTS_OR_GIFTS:            "payments_or_gifts",
	kyccoinbasepb.PurposeOfAccount_PURPOSE_OF_ACCOUNT_PAYMENTS_TO_FRIENDS:          "payments_to_friends",
	kyccoinbasepb.PurposeOfAccount_PURPOSE_OF_ACCOUNT_TRANSFER_CRYPTO:              "transfer_crypto",
	kyccoinbasepb.PurposeOfAccount_PURPOSE_OF_ACCOUNT_INVESTING_ON_OTHER_EXCHANGES: "investing_on_other_exchanges",
	kyccoinbasepb.PurposeOfAccount_PURPOSE_OF_ACCOUNT_BUSINESS:                     "business",
	kyccoinbasepb.PurposeOfAccount_PURPOSE_OF_ACCOUNT_OTHER:                        "other",
}

// sourcesOfFunds are Coinbase's sourceOfFunds values, as purposesOfAccount.
var sourcesOfFunds = map[kyccoinbasepb.SourceOfFunds]string{
	kyccoinbasepb.SourceOfFunds_SOURCE_OF_FUNDS_SALARY:                              "salary",
	kyccoinbasepb.SourceOfFunds_SOURCE_OF_FUNDS_EMPLOYMENT_BUSINESS_INCOME:          "employment_business_income",
	kyccoinbasepb.SourceOfFunds_SOURCE_OF_FUNDS_INVESTMENTS:                         "investments",
	kyccoinbasepb.SourceOfFunds_SOURCE_OF_FUNDS_SAVINGS:                             "savings",
	kyccoinbasepb.SourceOfFunds_SOURCE_OF_FUNDS_PENSION:                             "pension",
	kyccoinbasepb.SourceOfFunds_SOURCE_OF_FUNDS_BUSINESS_EARNINGS:                   "business_earnings",
	kyccoinbasepb.SourceOfFunds_SOURCE_OF_FUNDS_CRYPTO_ASSETS:                       "crypto_assets",
	kyccoinbasepb.SourceOfFunds_SOURCE_OF_FUNDS_INHERITANCE:                         "inheritance",
	kyccoinbasepb.SourceOfFunds_SOURCE_OF_FUNDS_SALE_OF_ASSETS:                      "sale_of_assets",
	kyccoinbasepb.SourceOfFunds_SOURCE_OF_FUNDS_SALE_OF_COMPANY_SHARES_OR_DIVIDENDS: "sale_of_company_shares_or_dividends",
	kyccoinbasepb.SourceOfFunds_SOURCE_OF_FUNDS_SALE_OF_PROPERTY:                    "sale_of_property",
	kyccoinbasepb.SourceOfFunds_SOURCE_OF_FUNDS_SALE_OF_OTHER_ASSETS:                "sale_of_other_assets",
	kyccoinbasepb.SourceOfFunds_SOURCE_OF_FUNDS_CREDIT_OR_LOAN:                      "credit_or_loan",
	kyccoinbasepb.SourceOfFunds_SOURCE_OF_FUNDS_GOVERNMENT_BENEFITS_OR_CREDITS:      "government_benefits_or_credits",
	kyccoinbasepb.SourceOfFunds_SOURCE_OF_FUNDS_LEGAL_SETTLEMENT:                    "legal_settlement",
	kyccoinbasepb.SourceOfFunds_SOURCE_OF_FUNDS_FAMILY_OR_THIRD_PARTY:               "family_or_third_party",
	kyccoinbasepb.SourceOfFunds_SOURCE_OF_FUNDS_LOTTERY_OR_GAMBLING:                 "lottery_or_gambling",
	kyccoinbasepb.SourceOfFunds_SOURCE_OF_FUNDS_CRYPTO_MINING:                       "crypto_mining",
	kyccoinbasepb.SourceOfFunds_SOURCE_OF_FUNDS_OTHER:                               "other",
}

// toCustomerInput converts a submission to the update sent to Coinbase.
//
// It carries the requester's IP, which Coinbase requires on every write to a
// customer who has requested a capability; the individual only when the
// request sets a field of it; accepted as the acceptances (see
// checkTermsAcceptances); and each attestation, stamped like the
// acceptances. It requests no capability: a submission never does.
//
// The address is sent whole, with its second line always set, to "" when the
// request has none: the proto's submission replaces the whole address, so a
// second line submitted before must not survive, and how Coinbase clears a
// second line otherwise is unconfirmed.
//
// An error is a request that cannot be converted (an invalid IP, an enum
// value or attestation form with no Coinbase equivalent) and names nothing
// submitted.
func toCustomerInput(req *kyccoinbasepb.SubmitKycRequest, accepted []customers.TosAcceptance, receivedAt time.Time, requesterIP netip.Addr) (*customers.CustomerInput, error) {
	if !requesterIP.IsValid() {
		return nil, errors.New("invalid requester ip address")
	}
	input := &customers.CustomerInput{
		Compliance: &customers.Compliance{RequesterIPAddress: requesterIP.Unmap().String()},
	}

	if individual := req.GetIndividual(); setsIndividual(individual) {
		converted, err := toIndividualInput(individual)
		if err != nil {
			return nil, err
		}
		input.Individual = converted
	}

	if len(accepted) > 0 {
		input.TosAcceptances = accepted
	}

	acceptedAt := stampedAt(receivedAt)
	for _, a := range req.GetTaxAttestations() {
		switch form := a.GetForm().(type) {
		case *kyccoinbasepb.TaxAttestation_UsW9_:
			input.TaxAttestations = append(input.TaxAttestations, customers.TaxAttestation{UsW9: &customers.UsW9Attestation{
				IsExemptBackupWithholding: form.UsW9.GetIsExemptBackupWithholding(),
				EdeliveryConsent:          a.GetEdeliveryConsent(),
				AcceptedAt:                acceptedAt,
			}})
		default:
			return nil, errors.New("tax attestation names no form")
		}
	}
	return input, nil
}

func toIndividualInput(individual *kyccoinbasepb.Individual) (*customers.IndividualInput, error) {
	out := &customers.IndividualInput{}
	if name := individual.GetLegalName(); name != nil {
		out.FirstName = name.GetFirstName()
		out.LastName = name.GetLastName()
	}
	if dob := individual.GetDateOfBirth(); dob != nil {
		out.DateOfBirth = &customers.DateOfBirth{
			Year:  fmt.Sprintf("%04d", dob.GetYear()),
			Month: fmt.Sprintf("%02d", dob.GetMonth()),
			Day:   fmt.Sprintf("%02d", dob.GetDay()),
		}
	}
	if address := individual.GetAddress(); address != nil {
		line2 := address.GetLine2()
		out.Address = &customers.AddressInput{
			Line1:       address.GetLine1(),
			Line2:       &line2,
			City:        address.GetCity(),
			State:       address.GetState(),
			PostCode:    address.GetPostalCode(),
			CountryCode: address.GetCountryCode(),
		}
	}
	if ssn := individual.GetSsn(); ssn != nil {
		digits := ssn.GetValue()
		if len(digits) != 9 {
			return nil, errors.New("ssn is not nine digits")
		}
		out.FullSsn = digits[:3] + "-" + digits[3:5] + "-" + digits[5:]
	}
	if email := individual.GetEmail(); email != nil {
		out.Email = email.GetValue()
	}
	if phone := individual.GetPhoneNumber(); phone != nil {
		out.PhoneNumber = phone.GetValue()
	}
	if purpose := individual.GetPurposeOfAccount(); purpose != kyccoinbasepb.PurposeOfAccount_PURPOSE_OF_ACCOUNT_UNKNOWN {
		value, ok := purposesOfAccount[purpose]
		if !ok {
			return nil, errors.New("purpose of account has no coinbase value")
		}
		out.PurposeOfAccount = value
	}
	if source := individual.GetSourceOfFunds(); source != kyccoinbasepb.SourceOfFunds_SOURCE_OF_FUNDS_UNKNOWN {
		value, ok := sourcesOfFunds[source]
		if !ok {
			return nil, errors.New("source of funds has no coinbase value")
		}
		out.SourceOfFunds = value
	}
	return out, nil
}

// submissionRefusal reads Coinbase's refusal of an update as an
// INVALID_SUBMISSION. ok is false when err is not a refusal of the submission
// (a failure to return as a gRPC error): anything but a Coinbase 400, so a
// 401, 404, 422, 429, 5xx, a transport error and a request the client refused
// to send are all failures, not verdicts on what the user submitted.
//
// accepted and unlisted are what checkTermsAcceptances returned for the
// update. Acceptances of listed versions had their language checked before
// sending, so an unsupported_tos_language refusal is attributed to each
// unlisted acceptance, or, when there is none, to every acceptance sent.
//
// Any other 400 names nothing in a structured way: Coinbase's only detail is
// a free-text message that may quote submitted values, which is never read,
// logged or returned. So it is attributed only where one thing is left to
// refuse. acceptancesAlone says the update carried acceptances and nothing
// else (submitsAcceptancesAlone), and then each acceptance Coinbase did not
// list is named NOT_ASKED: the listed ones were asked for and had their
// language checked. Beside a field or an attestation the refusal names
// nothing, as the proto's INVALID_SUBMISSION allows, since it may as well be
// theirs, and a version the user has already accepted, which is unlisted too,
// would be blamed for it. With no unlisted acceptance it names nothing
// either.
//
// A version already accepted is still named in one case, which the proto
// states: sent in a submission of acceptances alone that Coinbase refuses,
// beside one never asked for or for any other reason. It is unlisted like a
// version never asked for, Coinbase returns nothing that tells the two apart,
// and so every unlisted version of the submission is named. A client gets the
// state again either way, which lists what is still to accept.
//
// This gives up telling a client that sends a version its state never listed
// beside other things that the version was at fault. The proto has a client
// take each version from its state, so that client is already outside it.
//
// It never returns invalid fields, since Coinbase does not say which field it
// refused.
func submissionRefusal(err error, accepted []customers.TosAcceptance, unlisted []string, acceptancesAlone bool) (invalidTerms []*kyccoinbasepb.SubmitKycResponse_InvalidTermsOfServiceAcceptance, ok bool) {
	e, isCoinbase := customers.AsError(err)
	if !isCoinbase || e.StatusCode != http.StatusBadRequest {
		return nil, false
	}

	reason := kyccoinbasepb.SubmitKycResponse_InvalidTermsOfServiceAcceptance_NOT_ASKED
	var versions []string
	switch {
	case e.Type == customers.ErrorTypeUnsupportedTosLanguage:
		reason = kyccoinbasepb.SubmitKycResponse_InvalidTermsOfServiceAcceptance_UNSUPPORTED_LANGUAGE
		versions = unlisted
		if len(unlisted) == 0 {
			for _, a := range accepted {
				versions = append(versions, a.VersionID)
			}
		}
	case acceptancesAlone:
		versions = unlisted
	}

	seen := map[string]bool{}
	for _, versionID := range versions {
		if seen[versionID] {
			continue
		}
		seen[versionID] = true
		invalidTerms = append(invalidTerms, &kyccoinbasepb.SubmitKycResponse_InvalidTermsOfServiceAcceptance{
			VersionId: versionID,
			Reason:    reason,
		})
	}
	return invalidTerms, true
}
