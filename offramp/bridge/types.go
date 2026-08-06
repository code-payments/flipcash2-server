package bridge

import (
	"bytes"
	"encoding/json"
)

// list decodes Bridge list responses, which are either a bare JSON array or
// wrapped as {"count": n, "data": [...]} depending on the endpoint.
type list[T any] struct {
	Data []T
}

func (l *list[T]) UnmarshalJSON(b []byte) error {
	if trimmed := bytes.TrimSpace(b); len(trimmed) > 0 && trimmed[0] == '[' {
		return json.Unmarshal(trimmed, &l.Data)
	}
	var wrapped struct {
		Data []T `json:"data"`
	}
	if err := json.Unmarshal(b, &wrapped); err != nil {
		return err
	}
	l.Data = wrapped.Data
	return nil
}

// Customer statuses.
const (
	CustomerStatusActive                = "active"
	CustomerStatusAwaitingQuestionnaire = "awaiting_questionnaire"
	CustomerStatusAwaitingUBO           = "awaiting_ubo"
	CustomerStatusIncomplete            = "incomplete"
	CustomerStatusNotStarted            = "not_started"
	CustomerStatusUnderReview           = "under_review"
	CustomerStatusRejected              = "rejected"
	CustomerStatusOffboarded            = "offboarded"
	CustomerStatusPaused                = "paused"
)

// Endorsement names and statuses.
const (
	EndorsementBase = "base"

	EndorsementStatusApproved   = "approved"
	EndorsementStatusIncomplete = "incomplete"
	EndorsementStatusRevoked    = "revoked"
)

// Customer types.
const (
	CustomerTypeIndividual = "individual"
)

// Identifying information types.
const (
	IDTypeSSN            = "ssn"
	IDTypeITIN           = "itin"
	IDTypeDriversLicense = "drivers_license"
	IDTypePassport       = "passport"
	IDTypeStateID        = "state_or_provincial_id"
)

// Expected monthly payment volumes.
const (
	ExpectedMonthlyPaymentsUnder5K  = "0_4999"
	ExpectedMonthlyPayments5KTo10K  = "5000_9999"
	ExpectedMonthlyPayments10KTo50K = "10000_49999"
	ExpectedMonthlyPaymentsOver50K  = "50000_plus"
)

// Document purposes.
const (
	DocumentPurposeProofOfAddress = "proof_of_address"
)

// Payment rails.
const (
	PaymentRailACH        = "ach"
	PaymentRailACHSameDay = "ach_same_day"
	PaymentRailWire       = "wire"
	PaymentRailFedNow     = "fednow"
)

// Chains and currencies.
const (
	ChainSolana = "solana"

	CurrencyUSDC = "usdc"
	CurrencyUSD  = "usd"
)

// External account types.
const (
	ExternalAccountTypeUS = "us"

	AccountOwnerTypeIndividual = "individual"

	CheckingAccount = "checking"
	SavingsAccount  = "savings"
)

// Liquidation address states.
const (
	LiquidationAddressStateActive      = "active"
	LiquidationAddressStateDeactivated = "deactivated"
)

// Drain states. The happy path progresses funds_received →
// payment_submitted → payment_processed and never goes backwards.
const (
	DrainStateAwaitingFunds       = "awaiting_funds"
	DrainStateInReview            = "in_review"
	DrainStateFundsReceived       = "funds_received"
	DrainStatePaymentSubmitted    = "payment_submitted"
	DrainStatePaymentProcessed    = "payment_processed"
	DrainStateUndeliverable       = "undeliverable"
	DrainStateReturned            = "returned"
	DrainStateRefundInFlight      = "refund_in_flight"
	DrainStateRefundFailed        = "refund_failed"
	DrainStateMissingReturnPolicy = "missing_return_policy"
	DrainStateRefunded            = "refunded"
	DrainStateError               = "error"
	DrainStateCanceled            = "canceled"
)

// Address is a postal address in Bridge's format. Country is ISO 3166-1
// alpha-3 (e.g. "USA"); Subdivision is an ISO 3166-2 code (e.g. "CA").
type Address struct {
	StreetLine1 string `json:"street_line_1"`
	StreetLine2 string `json:"street_line_2,omitempty"`
	City        string `json:"city"`
	Subdivision string `json:"subdivision,omitempty"`
	PostalCode  string `json:"postal_code,omitempty"`
	Country     string `json:"country"`
}

// IdentifyingInformation is a government identifier or ID document. Image
// fields are base64 data URIs (e.g. "data:image/jpeg;base64,...").
type IdentifyingInformation struct {
	Type           string `json:"type"`
	IssuingCountry string `json:"issuing_country"`
	Number         string `json:"number,omitempty"`
	Expiration     string `json:"expiration,omitempty"`
	ImageFront     string `json:"image_front,omitempty"`
	ImageBack      string `json:"image_back,omitempty"`
}

// Document is a supporting compliance document (e.g. proof of address). File
// is a base64 data URI.
type Document struct {
	Purposes    []string `json:"purposes"`
	File        string   `json:"file"`
	Description string   `json:"description,omitempty"`
}

// complianceAnswers are the enhanced-onboarding questionnaire fields shared by
// customer creation and update requests.
type complianceAnswers struct {
	AccountPurpose             string `json:"account_purpose,omitempty"`
	AccountPurposeOther        string `json:"account_purpose_other,omitempty"`
	EmploymentStatus           string `json:"employment_status,omitempty"`
	ExpectedMonthlyPaymentsUSD string `json:"expected_monthly_payments_usd,omitempty"`
	SourceOfFunds              string `json:"source_of_funds,omitempty"`
	ActingAsIntermediary       *bool  `json:"acting_as_intermediary,omitempty"`
	MostRecentOccupation       string `json:"most_recent_occupation,omitempty"`
}

// CreateCustomerRequest creates an individual customer. All fields are proxied
// directly to Bridge and must not be persisted or logged.
type CreateCustomerRequest struct {
	Type              string `json:"type"`
	FirstName         string `json:"first_name"`
	MiddleName        string `json:"middle_name,omitempty"`
	LastName          string `json:"last_name"`
	Email             string `json:"email"`
	Phone             string `json:"phone,omitempty"`
	BirthDate         string `json:"birth_date"`
	ClientReferenceID string `json:"client_reference_id,omitempty"`

	ResidentialAddress     Address                  `json:"residential_address"`
	IdentifyingInformation []IdentifyingInformation `json:"identifying_information"`
	Documents              []Document               `json:"documents,omitempty"`

	complianceAnswers
	Nationalities []string `json:"nationalities,omitempty"`

	SignedAgreementID string   `json:"signed_agreement_id"`
	Endorsements      []string `json:"endorsements,omitempty"`
}

// UpdateCustomerRequest updates an existing customer. Any subset of fields may
// be provided (e.g. only identifying_information for a document retake), and
// only provided fields change.
type UpdateCustomerRequest struct {
	FirstName  string `json:"first_name,omitempty"`
	MiddleName string `json:"middle_name,omitempty"`
	LastName   string `json:"last_name,omitempty"`
	Email      string `json:"email,omitempty"`
	Phone      string `json:"phone,omitempty"`
	BirthDate  string `json:"birth_date,omitempty"`

	ResidentialAddress     *Address                 `json:"residential_address,omitempty"`
	IdentifyingInformation []IdentifyingInformation `json:"identifying_information,omitempty"`
	Documents              []Document               `json:"documents,omitempty"`

	complianceAnswers
	Nationalities []string `json:"nationalities,omitempty"`

	// A fresh signed agreement ID from re-accepting updated terms.
	SignedAgreementID string `json:"signed_agreement_id,omitempty"`
}

// EndorsementRequirements tracks progress toward an endorsement approval.
// Missing and Issues have provider-defined structure and are surfaced to
// drive re-collection UX.
type EndorsementRequirements struct {
	Complete []string        `json:"complete"`
	Pending  []string        `json:"pending"`
	Missing  json.RawMessage `json:"missing"`
	Issues   json.RawMessage `json:"issues"`
}

// Endorsement enables a customer to transact on a set of payment rails.
type Endorsement struct {
	Name         string                   `json:"name"`
	Status       string                   `json:"status"`
	Requirements *EndorsementRequirements `json:"requirements,omitempty"`
}

// Customer is Bridge's customer object. PII response fields are intentionally
// not modelled unless the server acts on them; only those are decoded.
type Customer struct {
	ID   string `json:"id"`
	Type string `json:"type"`

	// FirstName and LastName are PII, decoded solely to derive the account
	// owner name on external account creation from the verified identity.
	// They must never be logged or persisted, as with the rest of the
	// customer's identity data.
	FirstName string `json:"first_name"`
	LastName  string `json:"last_name"`

	Status                    string            `json:"status"`
	ClientReferenceID         string            `json:"client_reference_id"`
	HasAcceptedTermsOfService bool              `json:"has_accepted_terms_of_service"`
	Endorsements              []Endorsement     `json:"endorsements"`
	RejectionReasons          []RejectionReason `json:"rejection_reasons"`
	CreatedAt                 string            `json:"created_at"`
	UpdatedAt                 string            `json:"updated_at"`
}

// RejectionReason explains a KYC rejection. Reason is customer-facing;
// DeveloperReason is internal-only and must never be surfaced to users.
type RejectionReason struct {
	Reason          string `json:"reason"`
	DeveloperReason string `json:"developer_reason"`
	CreatedAt       string `json:"created_at"`
}

// BaseEndorsement returns the "base" endorsement, if present.
func (c *Customer) BaseEndorsement() *Endorsement {
	for i := range c.Endorsements {
		if c.Endorsements[i].Name == EndorsementBase {
			return &c.Endorsements[i]
		}
	}
	return nil
}

// TOSLink is a hosted terms-of-service acceptance session.
type TOSLink struct {
	URL string `json:"url"`
}

// USBankAccount are US bank account details for external account creation.
type USBankAccount struct {
	RoutingNumber     string `json:"routing_number"`
	AccountNumber     string `json:"account_number,omitempty"`
	CheckingOrSavings string `json:"checking_or_savings,omitempty"`
	Last4             string `json:"last_4,omitempty"` // response only
}

// ExternalAccountAddress is the beneficiary address on an external account.
// Unlike customer addresses, the region field is named "state", and Bridge
// bounds street_line_1 to 4-35 characters.
type ExternalAccountAddress struct {
	StreetLine1 string `json:"street_line_1"`
	StreetLine2 string `json:"street_line_2,omitempty"`
	City        string `json:"city"`
	State       string `json:"state,omitempty"`
	PostalCode  string `json:"postal_code,omitempty"`
	Country     string `json:"country"`
}

// CreateExternalAccountRequest registers a fiat bank account for a customer.
// The account number is proxied to Bridge and must not be persisted or logged.
// For US accounts, currency, account type, account owner name, the bank
// account details, and the beneficiary address are all required.
type CreateExternalAccountRequest struct {
	Currency         string                 `json:"currency"`
	AccountType      string                 `json:"account_type"`
	BankName         string                 `json:"bank_name,omitempty"`
	AccountOwnerName string                 `json:"account_owner_name"`
	FirstName        string                 `json:"first_name,omitempty"`
	LastName         string                 `json:"last_name,omitempty"`
	AccountOwnerType string                 `json:"account_owner_type"`
	Account          USBankAccount          `json:"account"`
	Address          ExternalAccountAddress `json:"address"`
}

// AccountVerification is Bridge's ownership verification result for a linked
// account (populated for aggregator-linked accounts).
type AccountVerification struct {
	MatchLevel                string `json:"match_level"`
	ReasonCode                string `json:"reason_code"`
	ValidatedAccountOwnerName string `json:"validated_account_owner_name"`
}

// ExternalAccount is Bridge's external (fiat) account object.
type ExternalAccount struct {
	ID                  string               `json:"id"`
	CustomerID          string               `json:"customer_id"`
	Currency            string               `json:"currency"`
	AccountType         string               `json:"account_type"`
	AccountOwnerName    string               `json:"account_owner_name"`
	BankName            string               `json:"bank_name"`
	Last4               string               `json:"last_4"`
	Active              bool                 `json:"active"`
	Account             *USBankAccount       `json:"account,omitempty"`
	AccountVerification *AccountVerification `json:"account_verification,omitempty"`
	CreatedAt           string               `json:"created_at"`
}

// PlaidLinkRequest is a Bridge-hosted Plaid Link session. LinkToken
// initializes the Plaid SDK on the client and also identifies the session
// when exchanging the resulting public token; CallbackURL is the full
// exchange endpoint for this session.
type PlaidLinkRequest struct {
	LinkToken          string `json:"link_token"`
	LinkTokenExpiresAt string `json:"link_token_expires_at"`
	CallbackURL        string `json:"callback_url"`
}

// ReturnInstructions describe where Bridge returns funds on a failed
// transaction. Memo is required for Stellar only.
type ReturnInstructions struct {
	Address string `json:"address"`
	Memo    string `json:"memo,omitempty"`
}

// CreateLiquidationAddressRequest creates a permanent on-chain address that
// automatically converts deposits and pays out to an external account.
type CreateLiquidationAddressRequest struct {
	Chain                  string              `json:"chain"`
	Currency               string              `json:"currency"`
	ExternalAccountID      string              `json:"external_account_id"`
	DestinationPaymentRail string              `json:"destination_payment_rail"`
	DestinationCurrency    string              `json:"destination_currency"`
	ReturnInstructions     *ReturnInstructions `json:"return_instructions,omitempty"`
	DestinationWireMessage string              `json:"destination_wire_message,omitempty"`

	// Base-100 percentage as a decimal string (10.2% is "10.2"); empty uses
	// the default fee configured with Bridge.
	CustomDeveloperFeePercent string `json:"custom_developer_fee_percent,omitempty"`
}

// LiquidationAddress is Bridge's liquidation address object. Return
// instructions submitted on creation come back flattened as the (deprecated
// but still populated) return_address plus return_memo.
type LiquidationAddress struct {
	ID                        string `json:"id"`
	Address                   string `json:"address"`
	Chain                     string `json:"chain"`
	Currency                  string `json:"currency"`
	ExternalAccountID         string `json:"external_account_id"`
	DestinationPaymentRail    string `json:"destination_payment_rail"`
	DestinationCurrency       string `json:"destination_currency"`
	ReturnAddress             string `json:"return_address"`
	ReturnMemo                string `json:"return_memo"`
	CustomDeveloperFeePercent string `json:"custom_developer_fee_percent"`
	State                     string `json:"state"`
	CreatedAt                 string `json:"created_at"`
}

// DrainDestination describes where a drain paid out.
type DrainDestination struct {
	PaymentRail string `json:"payment_rail"`
	Currency    string `json:"currency"`
}

// DrainReceipt is the amount breakdown for a drain. All amounts are decimal
// strings. OutgoingAmount is what the user actually receives: converted_amount
// minus any gas fees.
type DrainReceipt struct {
	InitialAmount       string `json:"initial_amount"`
	DeveloperFee        string `json:"developer_fee"`
	ExchangeRate        string `json:"exchange_rate"`
	SubtotalAmount      string `json:"subtotal_amount"`
	ConvertedAmount     string `json:"converted_amount"`
	OutgoingAmount      string `json:"outgoing_amount"`
	DestinationCurrency string `json:"destination_currency"`
	GasFee              string `json:"gas_fee,omitempty"`
	URL                 string `json:"url,omitempty"`
}

// Drain is one deposit to a liquidation address, tracked through payout.
// Amounts are decimal strings. States only ever progress forward.
type Drain struct {
	ID                string            `json:"id"`
	Amount            string            `json:"amount"`
	Currency          string            `json:"currency"`
	State             string            `json:"state"`
	Destination       *DrainDestination `json:"destination,omitempty"`
	Receipt           *DrainReceipt     `json:"receipt,omitempty"`
	DepositTxHash     string            `json:"deposit_tx_hash"`
	DestinationTxHash string            `json:"destination_tx_hash"`
	CreatedAt         string            `json:"created_at"`
}

// IsTerminal returns whether the drain has reached a final state. States
// requiring intervention (refund_failed, missing_return_policy) are not
// terminal: they can still progress to refunded once resolved, so trackers
// should keep polling them (and alert on them) rather than stop. Unknown
// states are treated as non-terminal for the same reason.
func (d *Drain) IsTerminal() bool {
	switch d.State {
	case DrainStatePaymentProcessed, DrainStateRefunded, DrainStateCanceled, DrainStateUndeliverable, DrainStateError:
		return true
	default:
		return false
	}
}
