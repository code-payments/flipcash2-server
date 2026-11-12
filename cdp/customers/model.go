package customers

import (
	"encoding/json"
	"errors"
	"time"
)

// CapabilityName names a capability, something Coinbase authorizes a customer
// to do. The constants are the names Coinbase documents today; it may add
// more, and a read keeps any name it returns.
type CapabilityName string

const (
	CapabilityCustodyCrypto      CapabilityName = "custodyCrypto"
	CapabilityCustodyFiat        CapabilityName = "custodyFiat"
	CapabilityCustodyStablecoin  CapabilityName = "custodyStablecoin"
	CapabilityTradeCrypto        CapabilityName = "tradeCrypto"
	CapabilityTradeStablecoin    CapabilityName = "tradeStablecoin"
	CapabilityTransferCrypto     CapabilityName = "transferCrypto"
	CapabilityTransferFiat       CapabilityName = "transferFiat"
	CapabilityTransferStablecoin CapabilityName = "transferStablecoin"
)

// CapabilityStatus is where a capability stands. A read keeps any status
// Coinbase returns, documented or not; a caller must treat one it does not
// know as not active.
type CapabilityStatus string

const (
	// CapabilityUnrequested: never requested; does nothing.
	CapabilityUnrequested CapabilityStatus = "unrequested"
	// CapabilityPending: requested, and either requirements are outstanding
	// or verification is running. A recoverable decline also stays here.
	CapabilityPending CapabilityStatus = "pending"
	// CapabilityActive: granted.
	CapabilityActive CapabilityStatus = "active"
	// CapabilityInactive: blocked by a terminal compliance decision, which
	// nothing submitted through the API can lift.
	CapabilityInactive CapabilityStatus = "inactive"
)

// Capability is one entry of a customer's capabilities map.
type Capability struct {
	Requested bool             `json:"requested"`
	Status    CapabilityStatus `json:"status"`
}

// RequirementKey names an outstanding requirement: an identity field
// (firstName, addressCity, ...), or one of the two non-field requirements,
// RequirementTos and RequirementTaxAttestation. The constants are the keys
// Coinbase documents today; a read keeps any key it returns.
type RequirementKey string

const (
	RequirementFirstName          RequirementKey = "firstName"
	RequirementLastName           RequirementKey = "lastName"
	RequirementDateOfBirth        RequirementKey = "dateOfBirth"
	RequirementSsnLast4           RequirementKey = "ssnLast4"
	RequirementFullSsn            RequirementKey = "fullSsn"
	RequirementAddressLine1       RequirementKey = "addressLine1"
	RequirementAddressLine2       RequirementKey = "addressLine2"
	RequirementAddressCity        RequirementKey = "addressCity"
	RequirementAddressSubdivision RequirementKey = "addressSubdivision"
	RequirementAddressPostalCode  RequirementKey = "addressPostalCode"
	RequirementAddressCountryCode RequirementKey = "addressCountryCode"
	RequirementEmail              RequirementKey = "email"
	RequirementPhoneNumber        RequirementKey = "phoneNumber"
	RequirementCitizenship        RequirementKey = "citizenship"
	RequirementPurposeOfAccount   RequirementKey = "purposeOfAccount"
	RequirementSourceOfFunds      RequirementKey = "sourceOfFunds"
	RequirementEmploymentStatus   RequirementKey = "employmentStatus"
	RequirementOccupation         RequirementKey = "occupation"
	RequirementExpectedVolume     RequirementKey = "expectedVolume"
	RequirementTos                RequirementKey = "tos"
	RequirementTaxAttestation     RequirementKey = "taxAttestation"
)

// RequirementStatus is where a requirement stands. A satisfied requirement is
// not listed at all. A read keeps any status Coinbase returns.
type RequirementStatus string

const (
	// RequirementDue: must be submitted (no deadline, or one still ahead).
	RequirementDue RequirementStatus = "due"
	// RequirementPastDue: must be submitted and its deadline has passed.
	RequirementPastDue RequirementStatus = "past_due"
	// RequirementPending: submitted and being verified.
	RequirementPending RequirementStatus = "pending"
	// RequirementRejected: the submitted value failed verification; collect
	// it again and resubmit.
	RequirementRejected RequirementStatus = "rejected"
)

// TaxForm names a tax form a customer may have to attest to.
type TaxForm string

const (
	// TaxFormUsW9 is IRS Form W-9.
	TaxFormUsW9 TaxForm = "us_w9"
)

// Requirement is one entry of a customer's requirements map.
type Requirement struct {
	Status RequirementStatus `json:"status"`
	// Deadline is when the requirement must be met, if Coinbase set one. For
	// RequirementTos it is the earliest deadline among TosVersions; read
	// each version's own.
	Deadline *time.Time `json:"deadline,omitempty"`
	// Impact lists the capabilities this requirement blocks, sorted.
	Impact []CapabilityName `json:"impact,omitempty"`
	// TosVersions is set on RequirementTos alone: the Terms of Service
	// versions the customer has yet to accept.
	TosVersions []TermsOfService `json:"tosVersions,omitempty"`
	// TaxForms is set on RequirementTaxAttestation alone: the forms the
	// customer has yet to attest to.
	TaxForms []TaxForm `json:"taxForms,omitempty"`
}

// TermsOfService is one Terms of Service version a customer must accept.
type TermsOfService struct {
	// VersionID is opaque; accept it verbatim.
	VersionID string `json:"versionId"`
	// Languages are the BCP 47 tags the version is published in. An
	// acceptance must name one of them.
	Languages []string `json:"languages"`
	// URL is the document, language-agnostic; append ?lang=<tag> for a
	// translation.
	URL string `json:"url"`
	// Deadline, when set, ends a grace period: before it the requirement is
	// due but not blocking, after it past_due.
	Deadline *time.Time `json:"deadline,omitempty"`
}

// CustomerType is the kind of customer. Coinbase supports individuals alone,
// and so does this client: Create always sends CustomerTypeIndividual.
type CustomerType string

const (
	CustomerTypeIndividual CustomerType = "individual"
)

// Customer is a customer as Coinbase returns it on every read and write.
//
// There is no overall status: a customer is where its capabilities and
// requirements say it is. Coinbase returns neither map on a customer that
// has never requested a capability (observed in the Sandbox, though its spec
// marks both required), and no requirements map once nothing is
// outstanding, so either may be nil.
type Customer struct {
	ID   string       `json:"customerId"`
	Type CustomerType `json:"type"`
	// Individual is the part of the identity Coinbase echoes back: names and
	// email alone. Nil when none of them is set.
	Individual   *Individual                    `json:"individual,omitempty"`
	Capabilities map[CapabilityName]Capability  `json:"capabilities,omitempty"`
	Requirements map[RequirementKey]Requirement `json:"requirements,omitempty"`
	CreatedAt    time.Time                      `json:"createdAt"`
	UpdatedAt    time.Time                      `json:"updatedAt"`
}

// Individual is the identity Coinbase returns on a read. Everything else
// submitted (SSN, date of birth, address, phone, due-diligence answers, terms
// acceptances, tax attestations, the requester's IP) is write-only.
type Individual struct {
	FirstName string `json:"firstName,omitempty"`
	LastName  string `json:"lastName,omitempty"`
	Email     string `json:"email,omitempty"`
}

// CustomerInput is what a create or update submits. Every field is optional:
// an update changes what it sets and leaves the rest as it was.
//
// Values are sent as given. The client does not check them against
// Coinbase's schema (formats, lengths, enums); Coinbase does, with a 400
// invalid_request whose message names the field.
type CustomerInput struct {
	// Capabilities to request (or, with Requested false, to stop
	// requesting). Requesting any needs Compliance.
	Capabilities map[CapabilityName]CapabilityRequest `json:"capabilities,omitempty"`
	Individual   *IndividualInput                     `json:"individual,omitempty"`
	// TosAcceptances records accepted Terms of Service versions. An empty
	// list is never sent (Coinbase refuses one), so empty and nil both mean
	// "none submitted".
	TosAcceptances []TosAcceptance `json:"tosAcceptances,omitempty"`
	// TaxAttestations records tax attestations, as TosAcceptances: an empty
	// list is never sent.
	TaxAttestations []TaxAttestation `json:"taxAttestations,omitempty"`
	// Compliance carries the end user's IP address, required on every write
	// to a customer who has requested a capability.
	Compliance *Compliance `json:"compliance,omitempty"`
}

// CapabilityRequest requests a capability (or withdraws a request).
type CapabilityRequest struct {
	Requested bool `json:"requested"`
}

// IndividualInput is the identity a write submits. Unset fields (the empty
// string, a nil pointer) are left out of the request and so left unchanged.
type IndividualInput struct {
	FirstName   string        `json:"firstName,omitempty"`
	LastName    string        `json:"lastName,omitempty"`
	DateOfBirth *DateOfBirth  `json:"dateOfBirth,omitempty"`
	Address     *AddressInput `json:"address,omitempty"`
	// FullSsn is the full SSN, "123-45-6789".
	FullSsn string `json:"fullSsn,omitempty"`
	Email   string `json:"email,omitempty"`
	// PhoneNumber is E.164, "+15551234567".
	PhoneNumber      string `json:"phoneNumber,omitempty"`
	PurposeOfAccount string `json:"purposeOfAccount,omitempty"`
	SourceOfFunds    string `json:"sourceOfFunds,omitempty"`
}

// DateOfBirth is Coinbase's date of birth: three decimal strings. Day and
// month may be zero-padded or not; year is four digits.
type DateOfBirth struct {
	Day   string `json:"day"`
	Month string `json:"month"`
	Year  string `json:"year"`
}

// AddressInput is a US address. Unset fields are left out, except Line2,
// which is a pointer so that a pointer to "" sends an explicit empty second
// line (clearing one submitted before) while nil leaves it out.
type AddressInput struct {
	Line1 string  `json:"line1,omitempty"`
	Line2 *string `json:"line2,omitempty"`
	City  string  `json:"city,omitempty"`
	// State is the two-letter code, "MA".
	State    string `json:"state,omitempty"`
	PostCode string `json:"postCode,omitempty"`
	// CountryCode must be "US".
	CountryCode string `json:"countryCode,omitempty"`
}

// TosAcceptance records that the customer accepted a Terms of Service
// version, shown to them in Language, at AcceptedAt (which Coinbase refuses
// if it lies in the future).
type TosAcceptance struct {
	VersionID  string    `json:"versionId"`
	Language   string    `json:"language"`
	AcceptedAt time.Time `json:"acceptedAt"`
}

// TaxAttestation is one tax attestation. Coinbase defines one form today, so
// exactly one variant must be set (W-9); a write with none or several is
// refused before it is sent.
type TaxAttestation struct {
	UsW9 *UsW9Attestation
}

// UsW9Attestation is the customer's certification of IRS Form W-9 as of
// AcceptedAt. It certifies their name and SSN: changing either afterwards,
// without a new W-9 in the same write, voids it.
type UsW9Attestation struct {
	IsExemptBackupWithholding bool
	EdeliveryConsent          bool
	AcceptedAt                time.Time
}

var errTaxAttestationVariant = errors.New("customers: a tax attestation must set exactly one form")

// MarshalJSON writes the attestation in Coinbase's shape, its form named by
// the `form` discriminator.
func (a TaxAttestation) MarshalJSON() ([]byte, error) {
	if a.UsW9 == nil {
		return nil, errTaxAttestationVariant
	}
	return json.Marshal(struct {
		Form                      TaxForm   `json:"form"`
		IsExemptBackupWithholding bool      `json:"isExemptBackupWithholding"`
		EdeliveryConsent          bool      `json:"edeliveryConsent"`
		AcceptedAt                time.Time `json:"acceptedAt"`
	}{
		Form:                      TaxFormUsW9,
		IsExemptBackupWithholding: a.UsW9.IsExemptBackupWithholding,
		EdeliveryConsent:          a.UsW9.EdeliveryConsent,
		AcceptedAt:                a.UsW9.AcceptedAt,
	})
}

// Compliance is the per-request compliance context.
type Compliance struct {
	// RequesterIPAddress is the end user's IPv4 or IPv6 address (never the
	// server's): the live session's, or the last one stored for them.
	RequesterIPAddress string `json:"requesterIpAddress"`
}
