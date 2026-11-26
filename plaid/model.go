package plaid

import "time"

// Product names, as Plaid spells them.
const (
	ProductAuth     = "auth"
	ProductIdentity = "identity"
)

// AccountType is Plaid's account type. Only depository accounts are linked.
type AccountType string

const (
	AccountTypeDepository AccountType = "depository"
	AccountTypeCredit     AccountType = "credit"
	AccountTypeLoan       AccountType = "loan"
	AccountTypeInvestment AccountType = "investment"
	AccountTypeOther      AccountType = "other"
)

// Depository subtypes Flipcash links.
const (
	AccountSubtypeChecking = "checking"
	AccountSubtypeSavings  = "savings"
)

// LinkToken is a token for opening Link, from /link/token/create. Token is a
// credential (see the package doc).
type LinkToken struct {
	Token      string
	Expiration time.Time
	RequestID  string
}

// PublicTokenExchange is the result of /item/public_token/exchange.
// AccessToken is a credential for the Item's lifetime (see the package doc).
type PublicTokenExchange struct {
	AccessToken string `json:"access_token"`
	ItemID      string `json:"item_id"`
	RequestID   string `json:"request_id"`
}

// Item is Plaid's Item: one user's connection to one bank. Fields Plaid
// sends as null decode to their zero value.
type Item struct {
	ItemID string `json:"item_id"`
	// InstitutionID is the bank, as Plaid identifies it.
	InstitutionID   string `json:"institution_id"`
	InstitutionName string `json:"institution_name"`
	Webhook         string `json:"webhook"`
	// AuthMethod is how the Item's Auth data was obtained, e.g.
	// INSTANT_AUTH or INSTANT_MATCH.
	AuthMethod string `json:"auth_method"`
	// Error is set when the Item needs attention (see Item.Error.Err).
	Error             *ItemError `json:"error"`
	AvailableProducts []string   `json:"available_products"`
	BilledProducts    []string   `json:"billed_products"`
	Products          []string   `json:"products"`
	ConsentedProducts []string   `json:"consented_products"`
	// ConsentExpirationTime is when the user's consent at the bank
	// expires; nil when it does not.
	ConsentExpirationTime *time.Time `json:"consent_expiration_time"`
	// UpdateType is "background" or "user_present_required".
	UpdateType string `json:"update_type"`
}

// ItemError is the error an Item is in, as /item/get reports it. Plaid's
// free-text messages are not kept.
type ItemError struct {
	ErrorType ErrorType `json:"error_type"`
	ErrorCode string    `json:"error_code"`
	// ErrorCodeReason refines some OAuth errors (OAUTH_CONSENT_EXPIRED,
	// OAUTH_USER_REVOKED, ...); usually empty.
	ErrorCodeReason string `json:"error_code_reason"`
}

// Err returns e as an *Error (StatusCode 0), so the predicates in errors.go
// apply to it; nil when e is nil.
func (e *ItemError) Err() error {
	if e == nil {
		return nil
	}
	return &Error{Type: e.ErrorType, Code: e.ErrorCode}
}

// Account is one of an Item's accounts, as /accounts/get reports it. Only
// identifying fields are decoded; balances are not.
type Account struct {
	// AccountID changes when Plaid cannot reconcile the account with the
	// bank, and differs on every Item (see the package doc).
	AccountID string `json:"account_id"`
	// PersistentAccountID identifies the account across Items, at banks
	// with tokenized account numbers only; empty elsewhere.
	PersistentAccountID string      `json:"persistent_account_id"`
	Name                string      `json:"name"`
	OfficialName        string      `json:"official_name"`
	Mask                string      `json:"mask"`
	Type                AccountType `json:"type"`
	Subtype             string      `json:"subtype"`
}

// Accounts is the result of /accounts/get: the accounts the user shares,
// and the Item.
type Accounts struct {
	Accounts  []Account `json:"accounts"`
	Item      Item      `json:"item"`
	RequestID string    `json:"request_id"`
}

// Institution is a bank, from /institutions/get_by_id.
type Institution struct {
	InstitutionID string `json:"institution_id"`
	Name          string `json:"name"`
	// OAuth reports whether the bank signs users in through its own app or
	// website.
	OAuth bool `json:"oauth"`
}
