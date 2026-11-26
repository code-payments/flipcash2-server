// Package plaidtest is an in-process fake of Plaid's API for tests: an
// httptest server speaking the same JSON API as Plaid for the endpoints
// package plaid calls, so tests drive the real plaid.Client against it (it
// satisfies plaid.API) rather than a mock.
//
// The fake is stateful. A test creates an Item as Link would
// (CreatePublicToken), the code under test exchanges the public token and
// reads, updates and removes the Item through the API, and the test changes
// the Item underneath it as the bank, the user or Plaid would: SetItemError,
// ClearItemError, SetConsentExpirationTime, ChangeAccountID, AddAccount,
// RemoveAccount and RemoveItem. FailNext and FailNextStatus make the next
// request fail. LinkTokenRequests and ProcessorTokens show what was asked
// for. It is deterministic: IDs, tokens and request IDs come from counters,
// time from an injectable clock.
//
// What follows Plaid's docs:
//
//   - Every request is a POST whose JSON body carries client_id and secret
//     (ClientID, Secret); wrong ones are INVALID_API_KEYS. A field the fake
//     does not know is UNKNOWN_FIELDS, a missing required one
//     MISSING_FIELDS, an unknown endpoint 404 NOT_FOUND.
//   - A public token expires PublicTokenLifetime after it was made. An
//     unknown or expired one is INVALID_PUBLIC_TOKEN.
//   - A link token expires after 4 hours for a new link and 30 minutes in
//     update mode. A link token names products for a new link only; in
//     update mode account_filters needs update.account_selection_enabled.
//     redirect_uri and android_package_name are never both set.
//   - /item/get answers 200 with item.error set when the Item is in error;
//     /accounts/get fails with that error.
//   - A removed Item (by /item/remove or RemoveItem) answers ITEM_NOT_FOUND
//     on every endpoint that takes its access token, /item/remove included;
//     an access token Plaid never issued is INVALID_ACCESS_TOKEN.
//   - /processor/token/create for an account not on the Item is
//     INVALID_ACCOUNT_ID; /institutions/get_by_id for an unknown
//     institution is INVALID_INSTITUTION.
//   - persistent_account_id is sent only for accounts that have one.
//
// Where the fake decides what Plaid's docs leave open:
//
//   - A public token is single-use: exchanging it again is
//     INVALID_PUBLIC_TOKEN.
//   - /accounts/get on an Item with no accounts left, and no error set, is
//     NO_ACCOUNTS.
//   - In update mode, products or an auth object are refused
//     INVALID_FIELD (Plaid's docs say not to send them).
//   - /processor/token/create ignores the Item's error.
//   - /link/token/create in update mode does not care whether the Item is
//     in error; it refuses only a removed or unknown Item.
//   - Link itself is not modelled: CreatePublicToken stands for a user
//     finishing Link, and the account hooks stand for an update.
package plaidtest

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"sort"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/code-payments/flipcash2-server/plaid"
)

const (
	// ClientID and Secret are the fake's API keys.
	ClientID = "plaidtest-client-id"
	Secret   = "plaidtest-secret"

	// DefaultInstitutionID is a known institution with no persistent
	// account IDs, Sandbox's First Platypus Bank.
	DefaultInstitutionID   = "ins_109508"
	DefaultInstitutionName = "First Platypus Bank"
	// TANInstitutionID is a known institution whose accounts get
	// persistent account IDs by default, as Sandbox's Chase does.
	TANInstitutionID   = "ins_56"
	TANInstitutionName = "Chase"

	PublicTokenLifetime     = 30 * time.Minute
	NewLinkTokenLifetime    = 4 * time.Hour
	UpdateLinkTokenLifetime = 30 * time.Minute
)

// Server is a fake Plaid API. All methods are safe for concurrent use,
// including alongside requests in flight.
type Server struct {
	httpServer *httptest.Server
	now        func() time.Time

	mu              sync.Mutex
	counter         int
	institutions    map[string]institution
	items           map[string]*item
	accessTokens    map[string]string // access token -> item ID
	publicTokens    map[string]*publicToken
	processorTokens map[string]ProcessorToken
	linkTokens      []LinkTokenRequest
	failures        []failure
}

type institution struct {
	name string
	tan  bool
}

type item struct {
	id                string
	institutionID     string
	webhook           string
	accounts          []*account
	err               *errorSpec
	consentExpiration *time.Time
	removed           bool
	accessToken       string
}

type account struct {
	id, persistentID         string
	name, officialName, mask string
	accountType              plaid.AccountType
	subtype                  string
}

type publicToken struct {
	itemID  string
	expires time.Time
	used    bool
}

// errorSpec is a Plaid error as the fake answers it.
type errorSpec struct {
	status    int
	errorType plaid.ErrorType
	code      string
}

type failure struct {
	spec    *errorSpec
	rawBody bool
}

// Option configures a Server.
type Option func(*Server)

// WithClock sets the fake's clock, which judges token expiry.
func WithClock(now func() time.Time) Option {
	return func(s *Server) { s.now = now }
}

// NewServer starts a fake knowing DefaultInstitutionID and TANInstitutionID.
// Close it when done.
func NewServer(opts ...Option) *Server {
	s := &Server{
		now: time.Now,
		institutions: map[string]institution{
			DefaultInstitutionID: {name: DefaultInstitutionName},
			TANInstitutionID:     {name: TANInstitutionName, tan: true},
		},
		items:           make(map[string]*item),
		accessTokens:    make(map[string]string),
		publicTokens:    make(map[string]*publicToken),
		processorTokens: make(map[string]ProcessorToken),
	}
	for _, opt := range opts {
		opt(s)
	}
	s.httpServer = httptest.NewServer(http.HandlerFunc(s.serveHTTP))
	return s
}

// Close shuts the server down.
func (s *Server) Close() {
	s.httpServer.Close()
}

// BaseURL is the base URL to give plaid.NewClient, the equivalent of
// plaid.SandboxBaseURL.
func (s *Server) BaseURL() string {
	return s.httpServer.URL
}

// Config is a plaid.Config for the fake, with a Link config suitable for
// tests; change what a test needs before calling plaid.NewClient.
func (s *Server) Config() plaid.Config {
	return plaid.Config{
		BaseURL:    s.BaseURL(),
		ClientID:   ClientID,
		Secret:     Secret,
		HTTPClient: s.httpServer.Client(),
		Link: plaid.LinkConfig{
			ClientName:         "Flipcash",
			RedirectURI:        "https://app.example.com/plaid/oauth",
			AndroidPackageName: "com.example.app",
		},
	}
}

// NewClient returns a plaid.Client for the fake, with Config.
func (s *Server) NewClient(log *zap.Logger) *plaid.Client {
	c, err := plaid.NewClient(log, s.Config())
	if err != nil {
		panic(err)
	}
	return c
}

// AccountSpec describes an account on an Item.
type AccountSpec struct {
	// Name defaults to "Plaid Checking" or "Plaid Saving".
	Name         string
	OfficialName string
	// Mask defaults to the next four digits of a counter.
	Mask string
	// Type defaults to depository.
	Type plaid.AccountType
	// Subtype defaults to checking.
	Subtype string
	// PersistentAccountID is the account's persistent ID; empty for one
	// made up at a TAN institution, and none elsewhere. NoPersistentID
	// gives none at a TAN institution too.
	PersistentAccountID string
	NoPersistentID      bool
}

// ItemSpec describes an Item a user links.
type ItemSpec struct {
	// InstitutionID defaults to DefaultInstitutionID. It must be known
	// (see AddInstitution).
	InstitutionID string
	// Accounts default to one checking account.
	Accounts []AccountSpec
	// Webhook is the Item's webhook URL.
	Webhook string
}

// AddInstitution makes an institution known. tan gives its accounts
// persistent account IDs by default.
func (s *Server) AddInstitution(institutionID, name string, tan bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.institutions[institutionID] = institution{name: name, tan: tan}
}

// CreatePublicToken creates an Item as a user finishing Link would, and
// returns its public token (valid for PublicTokenLifetime) and Item ID.
func (s *Server) CreatePublicToken(spec ItemSpec) (publicTokenValue string, itemID string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if spec.InstitutionID == "" {
		spec.InstitutionID = DefaultInstitutionID
	}
	inst, ok := s.institutions[spec.InstitutionID]
	if !ok {
		panic(fmt.Sprintf("plaidtest: unknown institution %q", spec.InstitutionID))
	}
	if len(spec.Accounts) == 0 {
		spec.Accounts = []AccountSpec{{}}
	}

	it := &item{
		id:            s.nextID("item"),
		institutionID: spec.InstitutionID,
		webhook:       spec.Webhook,
	}
	for _, a := range spec.Accounts {
		it.accounts = append(it.accounts, s.newAccount(a, inst.tan))
	}
	s.items[it.id] = it

	token := s.nextID("public-sandbox")
	s.publicTokens[token] = &publicToken{itemID: it.id, expires: s.now().Add(PublicTokenLifetime)}
	return token, it.id
}

func (s *Server) newAccount(spec AccountSpec, tan bool) *account {
	a := &account{
		id:           s.nextID("acct"),
		persistentID: spec.PersistentAccountID,
		name:         spec.Name,
		officialName: spec.OfficialName,
		mask:         spec.Mask,
		accountType:  spec.Type,
		subtype:      spec.Subtype,
	}
	if a.accountType == "" {
		a.accountType = plaid.AccountTypeDepository
	}
	if a.subtype == "" {
		a.subtype = plaid.AccountSubtypeChecking
	}
	if a.name == "" {
		a.name = "Plaid Checking"
		if a.subtype == plaid.AccountSubtypeSavings {
			a.name = "Plaid Saving"
		}
	}
	if a.mask == "" {
		a.mask = fmt.Sprintf("%04d", s.counter%10000)
	}
	if a.persistentID == "" && tan && !spec.NoPersistentID {
		a.persistentID = s.nextID("persistent")
	}
	if spec.NoPersistentID {
		a.persistentID = ""
	}
	return a
}

// SetItemError puts an Item into the error code (ITEM_LOGIN_REQUIRED,
// NO_ACCOUNTS, INSTITUTION_DOWN, ...): /item/get reports it and
// /accounts/get fails with it. It panics on a code the fake does not know
// (see FailNext).
func (s *Server) SetItemError(itemID, code string) {
	spec := mustErrorSpec(code)
	s.mu.Lock()
	defer s.mu.Unlock()
	s.mustItem(itemID).err = spec
}

// ClearItemError puts an Item back in good health, as an update in Link
// would.
func (s *Server) ClearItemError(itemID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.mustItem(itemID).err = nil
}

// SetConsentExpirationTime sets when the user's consent expires; nil for
// never.
func (s *Server) SetConsentExpirationTime(itemID string, t *time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if t != nil {
		v := t.UTC()
		t = &v
	}
	s.mustItem(itemID).consentExpiration = t
}

// ChangeAccountID gives an account a new account_id, as Plaid does when it
// cannot reconcile the account with the bank, and returns it. The persistent
// account ID, if any, is kept; processor tokens for the old ID stop
// working.
func (s *Server) ChangeAccountID(itemID, accountID string) string {
	s.mu.Lock()
	defer s.mu.Unlock()
	a := s.mustAccount(s.mustItem(itemID), accountID)
	a.id = s.nextID("acct")
	s.dropProcessorTokens(func(p ProcessorToken) bool { return p.ItemID == itemID && p.AccountID == accountID })
	return a.id
}

// AddAccount adds an account the user now shares (or the bank opened), and
// returns its account ID.
func (s *Server) AddAccount(itemID string, spec AccountSpec) string {
	s.mu.Lock()
	defer s.mu.Unlock()
	it := s.mustItem(itemID)
	a := s.newAccount(spec, s.institutions[it.institutionID].tan)
	it.accounts = append(it.accounts, a)
	return a.id
}

// RemoveAccount removes an account the user no longer shares (or closed).
// Processor tokens for it stop working.
func (s *Server) RemoveAccount(itemID, accountID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	it := s.mustItem(itemID)
	s.mustAccount(it, accountID)
	kept := it.accounts[:0]
	for _, a := range it.accounts {
		if a.id != accountID {
			kept = append(kept, a)
		}
	}
	it.accounts = kept
	s.dropProcessorTokens(func(p ProcessorToken) bool { return p.ItemID == itemID && p.AccountID == accountID })
}

// RemoveItem removes an Item as the user deleting it at my.plaid.com would:
// it is gone, and answers ITEM_NOT_FOUND from then on.
func (s *Server) RemoveItem(itemID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.removeItem(s.mustItem(itemID))
}

// IsItemRemoved reports whether an Item was removed, through the API or
// RemoveItem.
func (s *Server) IsItemRemoved(itemID string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.mustItem(itemID).removed
}

// AccountIDs lists an Item's account IDs, in order.
func (s *Server) AccountIDs(itemID string) []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	var ids []string
	for _, a := range s.mustItem(itemID).accounts {
		ids = append(ids, a.id)
	}
	return ids
}

// FailNext makes the next request, whatever its endpoint, fail with the
// Plaid error code, with the error_type and HTTP status Plaid documents for
// it. Calls queue. It panics on a code the fake does not know.
func (s *Server) FailNext(code string) {
	spec := mustErrorSpec(code)
	s.mu.Lock()
	defer s.mu.Unlock()
	s.failures = append(s.failures, failure{spec: spec})
}

// FailNextStatus makes the next request fail with HTTP status and a body
// that is not Plaid's, as a proxy in front of Plaid would. Calls queue
// with FailNext's.
func (s *Server) FailNextStatus(status int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.failures = append(s.failures, failure{spec: &errorSpec{status: status}, rawBody: true})
}

// LinkTokenRequest is what a /link/token/create asked for.
type LinkTokenRequest struct {
	LinkToken  string
	Expiration time.Time

	ClientName                  string
	Language                    string
	CountryCodes                []string
	ClientUserID                string
	Products                    []string
	AdditionalConsentedProducts []string
	Webhook                     string
	RedirectURI                 string
	AndroidPackageName          string
	// AccountSubtypes is account_filters.depository.account_subtypes.
	AccountSubtypes []string
	// AccessToken and ItemID are set in update mode.
	AccessToken             string
	ItemID                  string
	AccountSelectionEnabled bool
	// Auth is the auth object as sent, nil when absent.
	Auth map[string]any
}

// LinkTokenRequests lists the link tokens created, oldest first.
func (s *Server) LinkTokenRequests() []LinkTokenRequest {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]LinkTokenRequest(nil), s.linkTokens...)
}

// ProcessorToken is a processor token the fake issued and still honours.
type ProcessorToken struct {
	Token     string
	ItemID    string
	AccountID string
	Processor string
}

// ProcessorTokens lists the live processor tokens for an Item, by token.
func (s *Server) ProcessorTokens(itemID string) []ProcessorToken {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []ProcessorToken
	for _, p := range s.processorTokens {
		if p.ItemID == itemID {
			out = append(out, p)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Token < out[j].Token })
	return out
}

func (s *Server) removeItem(it *item) {
	it.removed = true
	s.dropProcessorTokens(func(p ProcessorToken) bool { return p.ItemID == it.id })
}

func (s *Server) dropProcessorTokens(match func(ProcessorToken) bool) {
	for token, p := range s.processorTokens {
		if match(p) {
			delete(s.processorTokens, token)
		}
	}
}

func (s *Server) mustItem(itemID string) *item {
	it, ok := s.items[itemID]
	if !ok {
		panic(fmt.Sprintf("plaidtest: unknown item %q", itemID))
	}
	return it
}

func (s *Server) mustAccount(it *item, accountID string) *account {
	for _, a := range it.accounts {
		if a.id == accountID {
			return a
		}
	}
	panic(fmt.Sprintf("plaidtest: item %q has no account %q", it.id, accountID))
}

// nextID returns prefix-NNNN, unique for the fake's lifetime. Callers hold
// mu.
func (s *Server) nextID(prefix string) string {
	s.counter++
	return fmt.Sprintf("%s-%04d", prefix, s.counter)
}

// knownErrors are the codes FailNext and SetItemError take, with the
// error_type and HTTP status Plaid's error reference gives each.
var knownErrors = map[string]errorSpec{
	plaid.CodeItemNotFound:             {http.StatusBadRequest, plaid.ErrorTypeItemError, ""},
	plaid.CodeItemLoginRequired:        {http.StatusBadRequest, plaid.ErrorTypeItemError, ""},
	plaid.CodeItemLocked:               {http.StatusBadRequest, plaid.ErrorTypeItemError, ""},
	plaid.CodeNoAccounts:               {http.StatusBadRequest, plaid.ErrorTypeItemError, ""},
	plaid.CodeAccessNotGranted:         {http.StatusBadRequest, plaid.ErrorTypeItemError, ""},
	"NO_AUTH_ACCOUNTS":                 {http.StatusBadRequest, plaid.ErrorTypeItemError, ""},
	plaid.CodeInvalidPublicToken:       {http.StatusBadRequest, plaid.ErrorTypeInvalidInput, ""},
	plaid.CodeInvalidAccessToken:       {http.StatusBadRequest, plaid.ErrorTypeInvalidInput, ""},
	plaid.CodeInvalidAccountID:         {http.StatusBadRequest, plaid.ErrorTypeInvalidInput, ""},
	plaid.CodeInvalidInstitution:       {http.StatusBadRequest, plaid.ErrorTypeInvalidInput, ""},
	plaid.CodeInvalidAPIKeys:           {http.StatusBadRequest, plaid.ErrorTypeInvalidInput, ""},
	plaid.CodeInvalidProduct:           {http.StatusBadRequest, plaid.ErrorTypeInvalidInput, ""},
	plaid.CodeInstitutionDown:          {http.StatusBadRequest, plaid.ErrorTypeInstitutionError, ""},
	plaid.CodeInstitutionNotResponding: {http.StatusBadRequest, plaid.ErrorTypeInstitutionError, ""},
	plaid.CodeInstitutionNotAvailable:  {http.StatusBadRequest, plaid.ErrorTypeInstitutionError, ""},
	"RATE_LIMIT":                       {http.StatusTooManyRequests, plaid.ErrorTypeRateLimitExceeded, ""},
	"ITEM_GET_LIMIT":                   {http.StatusTooManyRequests, plaid.ErrorTypeRateLimitExceeded, ""},
	"ACCOUNTS_LIMIT":                   {http.StatusTooManyRequests, plaid.ErrorTypeRateLimitExceeded, ""},
	"INVALID_FIELD":                    {http.StatusBadRequest, plaid.ErrorTypeInvalidRequest, ""},
	"MISSING_FIELDS":                   {http.StatusBadRequest, plaid.ErrorTypeInvalidRequest, ""},
	"UNKNOWN_FIELDS":                   {http.StatusBadRequest, plaid.ErrorTypeInvalidRequest, ""},
	"NOT_FOUND":                        {http.StatusNotFound, plaid.ErrorTypeInvalidRequest, ""},
	plaid.CodeInternalServerError:      {http.StatusInternalServerError, plaid.ErrorTypeAPIError, ""},
	plaid.CodePlannedMaintenance:       {http.StatusServiceUnavailable, plaid.ErrorTypeAPIError, ""},
}

func mustErrorSpec(code string) *errorSpec {
	spec, ok := knownErrors[code]
	if !ok {
		panic(fmt.Sprintf("plaidtest: unknown error code %q", code))
	}
	spec.code = code
	return &spec
}
