package customerstest

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/netip"
	"regexp"
	"slices"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/code-payments/flipcash2-server/cdp/customers"
)

// The state model, and where it knowingly differs from Coinbase.
//
// What follows Coinbase (docs, and the Sandbox on 2026-11-11):
//
//   - A customer created with nothing requested has neither a capabilities
//     nor a requirements map. Once any capability is named, all eight are
//     listed, unrequested ones as such.
//   - Each requested capability's requirements are the eleven identity
//     fields Coinbase verifies (firstName, lastName, dateOfBirth, fullSsn,
//     addressLine1, addressCity, addressSubdivision, addressPostalCode,
//     addressCountryCode, email, phoneNumber), the two due-diligence answers
//     (purposeOfAccount, sourceOfFunds), taxAttestation (taxForms [us_w9]),
//     and tos once the address country is set (one version, TosVersionID).
//     Every requirement's impact is every requested capability, sorted.
//     Requesting capabilities, or writing to a customer that has, without
//     compliance.requesterIpAddress is a 400.
//   - A submitted identity field goes from due to pending and stays listed;
//     a due-diligence answer is cleared as soon as it is submitted. A review
//     (CompleteReview) clears every pending field, or rejects them all when
//     a decline magic value is among the stored values. Submitting any
//     identity field while some are rejected puts every rejected one back
//     to pending. A field already verified stays verified when changed.
//   - A block magic value, or a date of birth under 18 or 120 and over,
//     makes every requested capability inactive in the same response,
//     permanently, and drops the identity and due-diligence requirements;
//     tos and taxAttestation stay listed while outstanding (as the Sandbox
//     does on a block).
//   - Terms of Service: re-accepting an accepted version changes nothing; a
//     version the customer is not required to accept (unknown, or any
//     version before the address country is set or a capability requested)
//     is a 400 invalid_request; a language the version is not published in
//     is unsupported_tos_language, compared as written ("EN" is not "en")
//     and for a version already accepted too (as the Sandbox does); an
//     acceptedAt in the future is a 400.
//     Acceptances are judged against the customer as the same request
//     leaves it, so one write can set the address and accept.
//   - A W-9 on file is voided when firstName, lastName or fullSsn changes to
//     a different value, unless the same write carries a new W-9; an
//     identical value voids nothing.
//   - An empty tosAcceptances or taxAttestations array is a 400.
//   - Reads return only firstName, lastName and email under individual.
//   - DELETE is 204, again 204 once deleted; reads of a deleted customer
//     are 404 not_found.
//   - A request sent again under its idempotency key with the same body
//     gets the first response again, a refusal included, as Coinbase
//     documents. The key with a different body is a 422 idempotency_error,
//     after a refusal too: the Sandbox answered so to a create refused 400
//     and then sent with another body under the same key.
//
// Where the fake simplifies:
//
//   - Data with no magic value is approved; Coinbase verifies it.
//   - A review completes only when the test says so, and only once every
//     identity field has been submitted; the Sandbox completes one within
//     seconds, and what it does with a partial submission is unknown.
//   - Setting a field that was unset before is never a change that voids a
//     W-9 (the fake does not model a W-9 attested before the name or SSN it
//     certifies was submitted).
//   - Schema validation is a subset: lengths, the patterns of state, SSN,
//     phone, email and date of birth parts, the US-only country code, and
//     the purposeOfAccount and sourceOfFunds enums. A date of birth must
//     have all three parts and is not checked as a calendar date. Fields
//     the client never sends (ssnLast4, citizenship, employmentStatus,
//     occupation, expectedVolume, projectIds) are refused as unknown, as is
//     any field Coinbase does not define. A refusal of a field's value
//     quotes the value, as Coinbase's free-text messages can; the wording is
//     the fake's own.
//   - No requirement ever has a deadline, and there is one Terms of Service
//     version, published in English alone.
//   - Withdrawing a capability (requested false) just marks it unrequested.
//   - An update to a blocked customer is applied, lifting nothing. Coinbase's
//     spec says an update may be refused 403 forbidden when a capability is
//     permanently inactive; the fake answers that only when
//     BlockOnNextUpdate injects it.
//   - Idempotency keys are scoped to method and path, are not required to
//     be UUIDs (nor, in the Sandbox, are they), and are remembered for the
//     fake's lifetime, where Coinbase documents 24 hours.
//   - Injected failures (FailNext and the rest), which stand for a proxy or
//     an overloaded Coinbase in front of the idempotency layer, are not
//     recorded under a key, so a request retried after one is handled
//     afresh. Whether Coinbase replays a 5xx under a key is not known.

// TosVersionID, TosURL and TosLanguages are the one Terms of Service version
// the fake requires, as the Sandbox lists it.
const (
	TosVersionID = "us_individual_2026-05-29"
	TosURL       = "https://www.coinbase.com/legal/user-agreements-third-party-provider"
)

// TosLanguages are the languages TosVersionID is published in.
var TosLanguages = []string{"en"}

// Coinbase's Sandbox magic values for fullSsn. See magicOutcome for the
// others the fake recognizes.
const (
	SsnApprove = "000-00-0000"
	SsnDecline = "000-00-0001"
	SsnBlock   = "000-00-0002"
)

var allCapabilities = []customers.CapabilityName{
	customers.CapabilityCustodyCrypto,
	customers.CapabilityCustodyFiat,
	customers.CapabilityCustodyStablecoin,
	customers.CapabilityTradeCrypto,
	customers.CapabilityTradeStablecoin,
	customers.CapabilityTransferCrypto,
	customers.CapabilityTransferFiat,
	customers.CapabilityTransferStablecoin,
}

// identityKeys are the requirements Coinbase verifies.
var identityKeys = []customers.RequirementKey{
	customers.RequirementFirstName,
	customers.RequirementLastName,
	customers.RequirementDateOfBirth,
	customers.RequirementFullSsn,
	customers.RequirementAddressLine1,
	customers.RequirementAddressCity,
	customers.RequirementAddressSubdivision,
	customers.RequirementAddressPostalCode,
	customers.RequirementAddressCountryCode,
	customers.RequirementEmail,
	customers.RequirementPhoneNumber,
}

// diligenceKeys are cleared as soon as they are submitted.
var diligenceKeys = []customers.RequirementKey{
	customers.RequirementPurposeOfAccount,
	customers.RequirementSourceOfFunds,
}

var (
	customerIDPattern = regexp.MustCompile(`^customer_[a-f0-9\-]{36}$`)
	statePattern      = regexp.MustCompile(`^[A-Z]{2}$`)
	ssnPattern        = regexp.MustCompile(`^([0-9]{3}-[0-9]{2}-[0-9]{4}|\*\*\*-\*\*-[0-9]{4})$`)
	phonePattern      = regexp.MustCompile(`^\+[1-9]\d{1,14}$`)
	emailPattern      = regexp.MustCompile("^[a-zA-Z0-9.!#$%&'*+/=?^_`{|}/-]+@[a-zA-Z0-9-]+(?:\\.[a-zA-Z0-9-]+)*$")
	dayPattern        = regexp.MustCompile(`^(?:0?[1-9]|[12][0-9]|3[01])$`)
	monthPattern      = regexp.MustCompile(`^(?:0?[1-9]|1[0-2])$`)
	yearPattern       = regexp.MustCompile(`^[0-9]{4}$`)
)

var purposesOfAccount = []string{
	"investing", "long_term_investment", "short_term_investment", "trading", "trading_on_coinbase",
	"trading_on_other_exchanges", "day_trading", "arbitrage_trading", "online_purchases", "online_payments",
	"payments_or_gifts", "payments_to_friends", "transfer_crypto", "investing_on_other_exchanges", "business", "other",
}

var sourcesOfFunds = []string{
	"salary", "employment_business_income", "investments", "savings", "pension", "business_earnings",
	"crypto_assets", "inheritance", "sale_of_assets", "sale_of_company_shares_or_dividends", "sale_of_property",
	"sale_of_other_assets", "credit_or_loan", "government_benefits_or_credits", "legal_settlement",
	"family_or_third_party", "lottery_or_gambling", "crypto_mining", "other",
}

const invalidCustomerData = "Invalid customer data provided."

// The request as it arrives on the wire. These are the fake's own types, not
// the client's, so that a client encoding mistake fails here rather than
// being mirrored. Pointers tell absent from empty.

type wireRequest struct {
	Type            *string                   `json:"type"`
	Capabilities    map[string]wireCapability `json:"capabilities"`
	Individual      *wireIndividual           `json:"individual"`
	TosAcceptances  *[]wireTosAcceptance      `json:"tosAcceptances"`
	TaxAttestations *[]wireTaxAttestation     `json:"taxAttestations"`
	Compliance      *wireCompliance           `json:"compliance"`
}

type wireCapability struct {
	Requested *bool `json:"requested"`
}

type wireIndividual struct {
	FirstName        *string      `json:"firstName"`
	LastName         *string      `json:"lastName"`
	DateOfBirth      *wireDob     `json:"dateOfBirth"`
	Address          *wireAddress `json:"address"`
	FullSsn          *string      `json:"fullSsn"`
	Email            *string      `json:"email"`
	PhoneNumber      *string      `json:"phoneNumber"`
	PurposeOfAccount *string      `json:"purposeOfAccount"`
	SourceOfFunds    *string      `json:"sourceOfFunds"`
}

type wireDob struct {
	Day   *string `json:"day"`
	Month *string `json:"month"`
	Year  *string `json:"year"`
}

type wireAddress struct {
	Line1       *string `json:"line1"`
	Line2       *string `json:"line2"`
	City        *string `json:"city"`
	State       *string `json:"state"`
	PostCode    *string `json:"postCode"`
	CountryCode *string `json:"countryCode"`
}

type wireTosAcceptance struct {
	VersionID  *string `json:"versionId"`
	Language   *string `json:"language"`
	AcceptedAt *string `json:"acceptedAt"`
}

type wireTaxAttestation struct {
	Form                      *string `json:"form"`
	IsExemptBackupWithholding *bool   `json:"isExemptBackupWithholding"`
	EdeliveryConsent          *bool   `json:"edeliveryConsent"`
	AcceptedAt                *string `json:"acceptedAt"`
}

type wireCompliance struct {
	RequesterIPAddress *string `json:"requesterIpAddress"`
}

// decodeRequest decodes and schema-validates a create or update body.
func (s *Server) decodeRequest(body []byte) (*wireRequest, *response) {
	var req wireRequest
	dec := json.NewDecoder(bytes.NewReader(body))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&req); err != nil || dec.More() {
		r := s.errorResponse(400, "invalid_request", "Invalid request. Please check the request body and parameters.")
		return nil, &r
	}
	if msg := validateRequest(&req); msg != "" {
		r := s.errorResponse(400, "invalid_request", msg)
		return nil, &r
	}
	return &req, nil
}

func validateRequest(req *wireRequest) string {
	for name, c := range req.Capabilities {
		if !slices.Contains(allCapabilities, customers.CapabilityName(name)) {
			return fmt.Sprintf("capabilities: unknown capability %q", name)
		}
		if c.Requested == nil {
			return fmt.Sprintf("capabilities.%s.requested: value is required", name)
		}
	}
	if ind := req.Individual; ind != nil {
		checks := []string{
			maxLength("individual.firstName", ind.FirstName, 60),
			maxLength("individual.lastName", ind.LastName, 60),
			matches("individual.fullSsn", ind.FullSsn, ssnPattern),
			maxLength("individual.email", ind.Email, 254),
			matches("individual.email", ind.Email, emailPattern),
			matches("individual.phoneNumber", ind.PhoneNumber, phonePattern),
			oneOf("individual.purposeOfAccount", ind.PurposeOfAccount, purposesOfAccount),
			oneOf("individual.sourceOfFunds", ind.SourceOfFunds, sourcesOfFunds),
		}
		if dob := ind.DateOfBirth; dob != nil {
			if dob.Day == nil || dob.Month == nil || dob.Year == nil {
				return "individual.dateOfBirth: day, month and year are required"
			}
			checks = append(checks,
				matches("individual.dateOfBirth.day", dob.Day, dayPattern),
				matches("individual.dateOfBirth.month", dob.Month, monthPattern),
				matches("individual.dateOfBirth.year", dob.Year, yearPattern),
			)
		}
		if a := ind.Address; a != nil {
			checks = append(checks,
				maxLength("individual.address.line1", a.Line1, 35),
				maxLength("individual.address.line2", a.Line2, 35),
				maxLength("individual.address.city", a.City, 40),
				maxLength("individual.address.postCode", a.PostCode, 16),
				matches("individual.address.state", a.State, statePattern),
				oneOf("individual.address.countryCode", a.CountryCode, []string{"US"}),
			)
		}
		for _, msg := range checks {
			if msg != "" {
				return msg
			}
		}
	}
	if req.TosAcceptances != nil {
		if len(*req.TosAcceptances) == 0 {
			return "tosAcceptances must not be an empty array; omit the field if you are not submitting any acceptances"
		}
		for i, a := range *req.TosAcceptances {
			if a.VersionID == nil || *a.VersionID == "" || len(*a.VersionID) > 64 || a.Language == nil || *a.Language == "" || !validTime(a.AcceptedAt) {
				return fmt.Sprintf("tosAcceptances[%d]: versionId, language and acceptedAt are required", i)
			}
		}
	}
	if req.TaxAttestations != nil {
		if len(*req.TaxAttestations) == 0 {
			return "taxAttestations must not be an empty array; omit the field if you are not submitting any attestations"
		}
		for i, a := range *req.TaxAttestations {
			if a.Form == nil || *a.Form != string(customers.TaxFormUsW9) {
				return fmt.Sprintf(`taxAttestations[%d].form: value is not one of the allowed values ["us_w9"]`, i)
			}
			if a.IsExemptBackupWithholding == nil || a.EdeliveryConsent == nil || !validTime(a.AcceptedAt) {
				return fmt.Sprintf("taxAttestations[%d]: isExemptBackupWithholding, edeliveryConsent and acceptedAt are required", i)
			}
		}
	}
	if c := req.Compliance; c != nil && c.RequesterIPAddress != nil {
		if _, err := netip.ParseAddr(*c.RequesterIPAddress); err != nil {
			return fmt.Sprintf("compliance.requesterIpAddress: %q is not a valid ip address", *c.RequesterIPAddress)
		}
	}
	return ""
}

// maxLength, matches and oneOf refuse a field's value with a message that
// names the field first and quotes the value.

func maxLength(field string, v *string, n int) string {
	if v != nil && utf8.RuneCountInString(*v) > n {
		return fmt.Sprintf("%s: string %q is longer than the maximum string length %d", field, *v, n)
	}
	return ""
}

func matches(field string, v *string, re *regexp.Regexp) string {
	if v != nil && !re.MatchString(*v) {
		return fmt.Sprintf("%s: string %q doesn't match the regular expression %q", field, *v, re.String())
	}
	return ""
}

func oneOf(field string, v *string, allowed []string) string {
	if v != nil && !slices.Contains(allowed, *v) {
		quoted, _ := json.Marshal(allowed)
		return fmt.Sprintf("%s: value %q is not one of the allowed values %s", field, *v, quoted)
	}
	return ""
}

func validTime(v *string) bool {
	if v == nil {
		return false
	}
	_, err := time.Parse(time.RFC3339Nano, *v)
	return err == nil
}

type keyState int

const (
	keyDue keyState = iota
	keyPending
	keyRejected
	keyCleared
)

type customer struct {
	id        string
	createdAt time.Time
	updatedAt time.Time

	// requested is nil until a capability is first named.
	requested map[customers.CapabilityName]bool

	individual customers.IndividualInput
	address    customers.AddressInput
	hasAddress bool

	identity  map[customers.RequirementKey]keyState
	diligence map[customers.RequirementKey]bool

	tosAccepted map[string]customers.TosAcceptance
	w9          *customers.UsW9Attestation
	lastIP      string
	blocked     bool
}

func newCustomer(id string, now time.Time) *customer {
	return &customer{
		id:          id,
		createdAt:   now,
		updatedAt:   now,
		identity:    make(map[customers.RequirementKey]keyState),
		diligence:   make(map[customers.RequirementKey]bool),
		tosAccepted: make(map[string]customers.TosAcceptance),
	}
}

func (c *customer) clone() *customer {
	next := *c
	if c.requested != nil {
		next.requested = make(map[customers.CapabilityName]bool, len(c.requested))
		for k, v := range c.requested {
			next.requested[k] = v
		}
	}
	if c.individual.DateOfBirth != nil {
		dob := *c.individual.DateOfBirth
		next.individual.DateOfBirth = &dob
	}
	if c.address.Line2 != nil {
		line2 := *c.address.Line2
		next.address.Line2 = &line2
	}
	next.identity = make(map[customers.RequirementKey]keyState, len(c.identity))
	for k, v := range c.identity {
		next.identity[k] = v
	}
	next.diligence = make(map[customers.RequirementKey]bool, len(c.diligence))
	for k, v := range c.diligence {
		next.diligence[k] = v
	}
	next.tosAccepted = make(map[string]customers.TosAcceptance, len(c.tosAccepted))
	for k, v := range c.tosAccepted {
		next.tosAccepted[k] = v
	}
	if c.w9 != nil {
		w9 := *c.w9
		next.w9 = &w9
	}
	return &next
}

func (c *customer) anyRequested() bool {
	for _, v := range c.requested {
		if v {
			return true
		}
	}
	return false
}

// apply applies a validated request to c, which the caller discards on an
// error response.
func (s *Server) apply(c *customer, req *wireRequest, now time.Time) *response {
	fail := func(errorType, msg string) *response {
		r := s.errorResponse(400, errorType, msg)
		return &r
	}

	for name, capability := range req.Capabilities {
		if c.requested == nil {
			c.requested = make(map[customers.CapabilityName]bool)
		}
		c.requested[customers.CapabilityName(name)] = *capability.Requested
	}

	if c.anyRequested() && (req.Compliance == nil || req.Compliance.RequesterIPAddress == nil) {
		return fail("invalid_request", invalidCustomerData)
	}
	if req.Compliance != nil && req.Compliance.RequesterIPAddress != nil {
		c.lastIP = *req.Compliance.RequesterIPAddress
	}

	voidsW9 := false
	if ind := req.Individual; ind != nil {
		var submitted []customers.RequirementKey
		set := func(key customers.RequirementKey, dst *string, v *string) {
			if v == nil {
				return
			}
			if *dst != "" && *dst != *v && (key == customers.RequirementFirstName ||
				key == customers.RequirementLastName || key == customers.RequirementFullSsn) {
				voidsW9 = true
			}
			*dst = *v
			submitted = append(submitted, key)
		}
		set(customers.RequirementFirstName, &c.individual.FirstName, ind.FirstName)
		set(customers.RequirementLastName, &c.individual.LastName, ind.LastName)
		set(customers.RequirementFullSsn, &c.individual.FullSsn, ind.FullSsn)
		set(customers.RequirementEmail, &c.individual.Email, ind.Email)
		set(customers.RequirementPhoneNumber, &c.individual.PhoneNumber, ind.PhoneNumber)
		set(customers.RequirementPurposeOfAccount, &c.individual.PurposeOfAccount, ind.PurposeOfAccount)
		set(customers.RequirementSourceOfFunds, &c.individual.SourceOfFunds, ind.SourceOfFunds)
		if dob := ind.DateOfBirth; dob != nil {
			c.individual.DateOfBirth = &customers.DateOfBirth{Day: *dob.Day, Month: *dob.Month, Year: *dob.Year}
			submitted = append(submitted, customers.RequirementDateOfBirth)
		}
		if a := ind.Address; a != nil {
			c.hasAddress = true
			set(customers.RequirementAddressLine1, &c.address.Line1, a.Line1)
			set(customers.RequirementAddressCity, &c.address.City, a.City)
			set(customers.RequirementAddressSubdivision, &c.address.State, a.State)
			set(customers.RequirementAddressPostalCode, &c.address.PostCode, a.PostCode)
			set(customers.RequirementAddressCountryCode, &c.address.CountryCode, a.CountryCode)
			if a.Line2 != nil {
				line2 := *a.Line2
				c.address.Line2 = &line2
			}
		}

		resubmitted := false
		for _, key := range submitted {
			if slices.Contains(diligenceKeys, key) {
				c.diligence[key] = true
				continue
			}
			resubmitted = true
			if c.identity[key] != keyCleared {
				c.identity[key] = keyPending
			}
		}
		if resubmitted {
			for key, state := range c.identity {
				if state == keyRejected {
					c.identity[key] = keyPending
				}
			}
		}
	}

	if req.TaxAttestations != nil {
		for _, a := range *req.TaxAttestations {
			acceptedAt, _ := time.Parse(time.RFC3339Nano, *a.AcceptedAt)
			if acceptedAt.After(now) {
				return fail("invalid_request", invalidCustomerData)
			}
			c.w9 = &customers.UsW9Attestation{
				IsExemptBackupWithholding: *a.IsExemptBackupWithholding,
				EdeliveryConsent:          *a.EdeliveryConsent,
				AcceptedAt:                acceptedAt,
			}
		}
	} else if voidsW9 {
		c.w9 = nil
	}

	if req.TosAcceptances != nil {
		for _, a := range *req.TosAcceptances {
			acceptedAt, _ := time.Parse(time.RFC3339Nano, *a.AcceptedAt)
			if acceptedAt.After(now) {
				return fail("invalid_request", invalidCustomerData)
			}
			_, alreadyAccepted := c.tosAccepted[*a.VersionID]
			required := c.requiredTosVersions()
			if !alreadyAccepted && !slices.Contains(required, *a.VersionID) {
				return fail("invalid_request", fmt.Sprintf(
					"validation failed: terms of service version is not required for this customer: version %q is not required for this customer's jurisdiction, customer type, and requested capabilities (required: [%s])",
					*a.VersionID, strings.Join(required, " ")))
			}
			// The language is judged for a version already accepted too: the
			// Sandbox refuses a re-acceptance in a language the version is not
			// published in, another case of one ("EN" for "en") included.
			if !slices.Contains(TosLanguages, *a.Language) {
				return fail("unsupported_tos_language", fmt.Sprintf(
					"validation failed: acceptance language is not supported for the matching Terms of Service version: version %q is not published in language %q; supported languages: [%s]",
					*a.VersionID, *a.Language, strings.Join(TosLanguages, " ")))
			}
			if alreadyAccepted {
				continue
			}
			c.tosAccepted[*a.VersionID] = customers.TosAcceptance{VersionID: *a.VersionID, Language: *a.Language, AcceptedAt: acceptedAt}
		}
	}

	if c.magicOutcome() == outcomeBlock || c.ageOutOfRange(now) {
		c.blocked = true
	}

	c.updatedAt = now
	return nil
}

// requiredTosVersions are the versions the customer must accept: the one
// version, once a capability is requested and the address country is set.
func (c *customer) requiredTosVersions() []string {
	if !c.anyRequested() || c.address.CountryCode == "" {
		return nil
	}
	return []string{TosVersionID}
}

func (c *customer) ageOutOfRange(now time.Time) bool {
	dob := c.individual.DateOfBirth
	if dob == nil {
		return false
	}
	year, _ := strconv.Atoi(dob.Year)
	month, _ := strconv.Atoi(dob.Month)
	day, _ := strconv.Atoi(dob.Day)
	age := now.Year() - year
	if now.Month() < time.Month(month) || (now.Month() == time.Month(month) && now.Day() < day) {
		age--
	}
	return age < 18 || age >= 120
}

type outcome int

const (
	outcomeApprove outcome = iota
	outcomeDecline
	outcomeBlock
)

// magicOutcome is the most stringent outcome among the stored values'
// Sandbox magic values, approve when there are none.
func (c *customer) magicOutcome() outcome {
	worst := outcomeApprove
	consider := func(o outcome) {
		if o > worst {
			worst = o
		}
	}
	exact := func(v, decline, block string) {
		switch v {
		case decline:
			consider(outcomeDecline)
		case block:
			consider(outcomeBlock)
		}
	}
	word := func(v string) {
		switch strings.ToLower(v) {
		case "magicdecline":
			consider(outcomeDecline)
		case "magicblock":
			consider(outcomeBlock)
		}
	}

	ind := c.individual
	exact(ind.FullSsn, SsnDecline, SsnBlock)
	exact(ind.PhoneNumber, "+15555550101", "+15555550102")
	exact(c.address.PostCode, "00001", "00002")
	word(ind.FirstName)
	word(ind.LastName)
	word(c.address.Line1)
	word(c.address.City)
	if c.address.Line2 != nil {
		word(*c.address.Line2)
	}
	if local, domain, ok := strings.Cut(strings.ToLower(ind.Email), "@"); ok && domain == "example.com" {
		switch {
		case local == "magic-decline" || strings.HasSuffix(local, "+magic-decline"):
			consider(outcomeDecline)
		case local == "magic-block" || strings.HasSuffix(local, "+magic-block"):
			consider(outcomeBlock)
		}
	}
	if dob := ind.DateOfBirth; dob != nil {
		year, _ := strconv.Atoi(dob.Year)
		month, _ := strconv.Atoi(dob.Month)
		day, _ := strconv.Atoi(dob.Day)
		if year == 1919 && month == 1 {
			switch day {
			case 2:
				consider(outcomeDecline)
			case 3:
				consider(outcomeBlock)
			}
		}
	}
	return worst
}

// completeReview resolves pending identity fields once all are submitted.
func (c *customer) completeReview() bool {
	if c.blocked {
		return false
	}
	pending := false
	for _, key := range identityKeys {
		switch c.identity[key] {
		case keyDue:
			return false
		case keyPending:
			pending = true
		}
	}
	if !pending {
		return false
	}
	next := keyCleared
	if c.magicOutcome() == outcomeDecline {
		next = keyRejected
	}
	for _, key := range identityKeys {
		if c.identity[key] == keyPending {
			c.identity[key] = next
		}
	}
	return true
}

type wireCustomer struct {
	CustomerID   string                     `json:"customerId"`
	Type         string                     `json:"type"`
	Individual   map[string]string          `json:"individual,omitempty"`
	Capabilities map[string]wireCapState    `json:"capabilities,omitempty"`
	Requirements map[string]wireRequirement `json:"requirements,omitempty"`
	CreatedAt    string                     `json:"createdAt"`
	UpdatedAt    string                     `json:"updatedAt"`
}

type wireCapState struct {
	Requested bool   `json:"requested"`
	Status    string `json:"status"`
}

type wireRequirement struct {
	Impact      []string      `json:"impact,omitempty"`
	Status      string        `json:"status"`
	TaxForms    []string      `json:"taxForms,omitempty"`
	TosVersions []wireVersion `json:"tosVersions,omitempty"`
}

type wireVersion struct {
	Languages []string `json:"languages"`
	URL       string   `json:"url"`
	VersionID string   `json:"versionId"`
}

const timestampLayout = "2006-01-02T15:04:05.000Z"

func (c *customer) requirements() map[string]wireRequirement {
	if !c.anyRequested() {
		return nil
	}
	var impact []string
	for name, requested := range c.requested {
		if requested {
			impact = append(impact, string(name))
		}
	}
	sort.Strings(impact)

	reqs := make(map[string]wireRequirement)
	if !c.blocked {
		for _, key := range identityKeys {
			var status customers.RequirementStatus
			switch c.identity[key] {
			case keyDue:
				status = customers.RequirementDue
			case keyPending:
				status = customers.RequirementPending
			case keyRejected:
				status = customers.RequirementRejected
			default:
				continue
			}
			reqs[string(key)] = wireRequirement{Status: string(status), Impact: impact}
		}
		for _, key := range diligenceKeys {
			if !c.diligence[key] {
				reqs[string(key)] = wireRequirement{Status: string(customers.RequirementDue), Impact: impact}
			}
		}
	}
	if c.w9 == nil {
		reqs[string(customers.RequirementTaxAttestation)] = wireRequirement{
			Status:   string(customers.RequirementDue),
			Impact:   impact,
			TaxForms: []string{string(customers.TaxFormUsW9)},
		}
	}
	var versions []wireVersion
	for _, v := range c.requiredTosVersions() {
		if _, ok := c.tosAccepted[v]; !ok {
			versions = append(versions, wireVersion{VersionID: v, Languages: TosLanguages, URL: TosURL})
		}
	}
	if len(versions) > 0 {
		reqs[string(customers.RequirementTos)] = wireRequirement{
			Status:      string(customers.RequirementDue),
			Impact:      impact,
			TosVersions: versions,
		}
	}
	return reqs
}

func (c *customer) toWire() wireCustomer {
	w := wireCustomer{
		CustomerID: c.id,
		Type:       "individual",
		CreatedAt:  c.createdAt.UTC().Format(timestampLayout),
		UpdatedAt:  c.updatedAt.UTC().Format(timestampLayout),
	}
	ind := map[string]string{}
	if c.individual.FirstName != "" {
		ind["firstName"] = c.individual.FirstName
	}
	if c.individual.LastName != "" {
		ind["lastName"] = c.individual.LastName
	}
	if c.individual.Email != "" {
		ind["email"] = c.individual.Email
	}
	if len(ind) > 0 {
		w.Individual = ind
	}
	if c.requested == nil {
		return w
	}
	reqs := c.requirements()
	if len(reqs) > 0 {
		w.Requirements = reqs
	}
	w.Capabilities = make(map[string]wireCapState, len(allCapabilities))
	for _, name := range allCapabilities {
		state := wireCapState{Requested: c.requested[name]}
		switch {
		case !state.Requested:
			state.Status = string(customers.CapabilityUnrequested)
		case c.blocked:
			state.Status = string(customers.CapabilityInactive)
		case len(reqs) == 0:
			state.Status = string(customers.CapabilityActive)
		default:
			state.Status = string(customers.CapabilityPending)
		}
		w.Capabilities[string(name)] = state
	}
	return w
}

func (c *customer) snapshot() Snapshot {
	clone := c.clone()
	snap := Snapshot{
		Individual:         clone.individual,
		TosAccepted:        clone.tosAccepted,
		W9:                 clone.w9,
		RequesterIPAddress: c.lastIP,
		Blocked:            c.blocked,
	}
	if c.hasAddress {
		address := clone.address
		snap.Individual.Address = &address
	}
	return snap
}
