package plaidtest

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"time"

	"github.com/code-payments/flipcash2-server/plaid"
)

// serveHTTP answers one request. Handlers run under mu.
func (s *Server) serveHTTP(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	defer s.mu.Unlock()

	requestID := s.nextID("req")

	if len(s.failures) > 0 {
		f := s.failures[0]
		s.failures = s.failures[1:]
		if f.rawBody {
			w.WriteHeader(f.spec.status)
			_, _ = io.WriteString(w, "<html><body>upstream failure</body></html>")
			return
		}
		writeError(w, requestID, f.spec)
		return
	}

	handler, ok := map[string]func([]byte, string) (any, *errorSpec){
		"/link/token/create":          s.linkTokenCreate,
		"/item/public_token/exchange": s.publicTokenExchange,
		"/item/get":                   s.itemGet,
		"/accounts/get":               s.accountsGet,
		"/item/remove":                s.itemRemove,
		"/institutions/get_by_id":     s.institutionsGetByID,
		"/processor/token/create":     s.processorTokenCreate,
	}[r.URL.Path]
	if !ok || r.Method != http.MethodPost {
		writeError(w, requestID, mustErrorSpec("NOT_FOUND"))
		return
	}

	body, err := io.ReadAll(r.Body)
	if err != nil {
		writeError(w, requestID, mustErrorSpec("INVALID_FIELD"))
		return
	}
	var creds struct {
		ClientID string `json:"client_id"`
		Secret   string `json:"secret"`
	}
	if err := json.Unmarshal(body, &creds); err != nil {
		writeError(w, requestID, mustErrorSpec("INVALID_FIELD"))
		return
	}
	if creds.ClientID == "" || creds.Secret == "" {
		writeError(w, requestID, mustErrorSpec("MISSING_FIELDS"))
		return
	}
	if creds.ClientID != ClientID || creds.Secret != Secret {
		writeError(w, requestID, mustErrorSpec(plaid.CodeInvalidAPIKeys))
		return
	}

	res, spec := handler(body, requestID)
	if spec != nil {
		writeError(w, requestID, spec)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(res)
}

func writeError(w http.ResponseWriter, requestID string, spec *errorSpec) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(spec.status)
	_ = json.NewEncoder(w).Encode(map[string]any{
		"error_type":      spec.errorType,
		"error_code":      spec.code,
		"error_message":   "plaidtest: " + spec.code,
		"display_message": nil,
		"request_id":      requestID,
	})
}

// decodeStrict decodes body into v, refusing fields v does not name, as
// Plaid refuses fields it does not define.
func decodeStrict(body []byte, v any) *errorSpec {
	dec := json.NewDecoder(bytes.NewReader(body))
	dec.DisallowUnknownFields()
	if err := dec.Decode(v); err != nil {
		return mustErrorSpec("UNKNOWN_FIELDS")
	}
	return nil
}

type credentialFields struct {
	ClientID string `json:"client_id"`
	Secret   string `json:"secret"`
}

// itemByAccessToken finds the live Item for an access token.
func (s *Server) itemByAccessToken(token string) (*item, *errorSpec) {
	if token == "" {
		return nil, mustErrorSpec("MISSING_FIELDS")
	}
	itemID, ok := s.accessTokens[token]
	if !ok {
		return nil, mustErrorSpec(plaid.CodeInvalidAccessToken)
	}
	it := s.items[itemID]
	if it.removed {
		return nil, mustErrorSpec(plaid.CodeItemNotFound)
	}
	return it, nil
}

func (s *Server) linkTokenCreate(body []byte, requestID string) (any, *errorSpec) {
	var req struct {
		credentialFields
		ClientName   string   `json:"client_name"`
		Language     string   `json:"language"`
		CountryCodes []string `json:"country_codes"`
		User         *struct {
			ClientUserID string `json:"client_user_id"`
		} `json:"user"`
		Products                    []string `json:"products"`
		AdditionalConsentedProducts []string `json:"additional_consented_products"`
		Webhook                     string   `json:"webhook"`
		AccessToken                 string   `json:"access_token"`
		RedirectURI                 string   `json:"redirect_uri"`
		AndroidPackageName          string   `json:"android_package_name"`
		AccountFilters              *struct {
			Depository *struct {
				AccountSubtypes []string `json:"account_subtypes"`
			} `json:"depository"`
		} `json:"account_filters"`
		Auth   map[string]any `json:"auth"`
		Update *struct {
			AccountSelectionEnabled bool `json:"account_selection_enabled"`
		} `json:"update"`
	}
	if spec := decodeStrict(body, &req); spec != nil {
		return nil, spec
	}
	if req.ClientName == "" || req.Language == "" || len(req.CountryCodes) == 0 || req.User == nil || req.User.ClientUserID == "" {
		return nil, mustErrorSpec("MISSING_FIELDS")
	}
	if len(req.ClientName) > 30 || (req.RedirectURI != "" && req.AndroidPackageName != "") {
		return nil, mustErrorSpec("INVALID_FIELD")
	}

	record := LinkTokenRequest{
		ClientName:                  req.ClientName,
		Language:                    req.Language,
		CountryCodes:                req.CountryCodes,
		ClientUserID:                req.User.ClientUserID,
		Products:                    req.Products,
		AdditionalConsentedProducts: req.AdditionalConsentedProducts,
		Webhook:                     req.Webhook,
		RedirectURI:                 req.RedirectURI,
		AndroidPackageName:          req.AndroidPackageName,
		AccessToken:                 req.AccessToken,
		Auth:                        req.Auth,
	}
	if req.AccountFilters != nil {
		if req.AccountFilters.Depository == nil || len(req.AccountFilters.Depository.AccountSubtypes) == 0 {
			return nil, mustErrorSpec("INVALID_FIELD")
		}
		record.AccountSubtypes = req.AccountFilters.Depository.AccountSubtypes
	}
	if req.Update != nil {
		record.AccountSelectionEnabled = req.Update.AccountSelectionEnabled
	}

	lifetime := NewLinkTokenLifetime
	if req.AccessToken != "" {
		it, spec := s.itemByAccessToken(req.AccessToken)
		if spec != nil {
			return nil, spec
		}
		if len(req.Products) > 0 || req.Auth != nil {
			return nil, mustErrorSpec("INVALID_FIELD")
		}
		if req.AccountFilters != nil && !record.AccountSelectionEnabled {
			return nil, mustErrorSpec("INVALID_FIELD")
		}
		record.ItemID = it.id
		lifetime = UpdateLinkTokenLifetime
	} else {
		if len(req.Products) == 0 {
			return nil, mustErrorSpec("MISSING_FIELDS")
		}
		if req.Update != nil {
			return nil, mustErrorSpec("INVALID_FIELD")
		}
	}

	record.LinkToken = s.nextID("link-sandbox")
	record.Expiration = s.now().Add(lifetime).UTC().Truncate(time.Second)
	s.linkTokens = append(s.linkTokens, record)

	return map[string]any{
		"link_token": record.LinkToken,
		"expiration": record.Expiration.Format(time.RFC3339),
		"request_id": requestID,
	}, nil
}

func (s *Server) publicTokenExchange(body []byte, requestID string) (any, *errorSpec) {
	var req struct {
		credentialFields
		PublicToken string `json:"public_token"`
	}
	if spec := decodeStrict(body, &req); spec != nil {
		return nil, spec
	}
	if req.PublicToken == "" {
		return nil, mustErrorSpec("MISSING_FIELDS")
	}
	pt, ok := s.publicTokens[req.PublicToken]
	if !ok || pt.used || !s.now().Before(pt.expires) {
		return nil, mustErrorSpec(plaid.CodeInvalidPublicToken)
	}
	pt.used = true

	it := s.items[pt.itemID]
	it.accessToken = s.nextID("access-sandbox")
	s.accessTokens[it.accessToken] = it.id

	return map[string]any{
		"access_token": it.accessToken,
		"item_id":      it.id,
		"request_id":   requestID,
	}, nil
}

func (s *Server) itemGet(body []byte, requestID string) (any, *errorSpec) {
	var req struct {
		credentialFields
		AccessToken string `json:"access_token"`
	}
	if spec := decodeStrict(body, &req); spec != nil {
		return nil, spec
	}
	it, spec := s.itemByAccessToken(req.AccessToken)
	if spec != nil {
		return nil, spec
	}
	return map[string]any{
		"item":       s.itemJSON(it),
		"status":     map[string]any{"last_webhook": nil},
		"request_id": requestID,
	}, nil
}

func (s *Server) accountsGet(body []byte, requestID string) (any, *errorSpec) {
	var req struct {
		credentialFields
		AccessToken string `json:"access_token"`
	}
	if spec := decodeStrict(body, &req); spec != nil {
		return nil, spec
	}
	it, spec := s.itemByAccessToken(req.AccessToken)
	if spec != nil {
		return nil, spec
	}
	if it.err != nil {
		return nil, it.err
	}
	if len(it.accounts) == 0 {
		return nil, mustErrorSpec(plaid.CodeNoAccounts)
	}
	accounts := make([]any, 0, len(it.accounts))
	for _, a := range it.accounts {
		accounts = append(accounts, accountJSON(a))
	}
	return map[string]any{
		"accounts":   accounts,
		"item":       s.itemJSON(it),
		"request_id": requestID,
	}, nil
}

func (s *Server) itemRemove(body []byte, requestID string) (any, *errorSpec) {
	var req struct {
		credentialFields
		AccessToken string `json:"access_token"`
		ReasonCode  string `json:"reason_code"`
		ReasonNote  string `json:"reason_note"`
	}
	if spec := decodeStrict(body, &req); spec != nil {
		return nil, spec
	}
	it, spec := s.itemByAccessToken(req.AccessToken)
	if spec != nil {
		return nil, spec
	}
	s.removeItem(it)
	return map[string]any{"request_id": requestID}, nil
}

func (s *Server) institutionsGetByID(body []byte, requestID string) (any, *errorSpec) {
	var req struct {
		credentialFields
		InstitutionID string   `json:"institution_id"`
		CountryCodes  []string `json:"country_codes"`
	}
	if spec := decodeStrict(body, &req); spec != nil {
		return nil, spec
	}
	if req.InstitutionID == "" || len(req.CountryCodes) == 0 {
		return nil, mustErrorSpec("MISSING_FIELDS")
	}
	inst, ok := s.institutions[req.InstitutionID]
	if !ok {
		return nil, mustErrorSpec(plaid.CodeInvalidInstitution)
	}
	return map[string]any{
		"institution": map[string]any{
			"institution_id": req.InstitutionID,
			"name":           inst.name,
			"products":       []string{plaid.ProductAuth, plaid.ProductIdentity},
			"country_codes":  []string{"US"},
			"oauth":          inst.tan,
		},
		"request_id": requestID,
	}, nil
}

func (s *Server) processorTokenCreate(body []byte, requestID string) (any, *errorSpec) {
	var req struct {
		credentialFields
		AccessToken string `json:"access_token"`
		AccountID   string `json:"account_id"`
		Processor   string `json:"processor"`
	}
	if spec := decodeStrict(body, &req); spec != nil {
		return nil, spec
	}
	if req.AccountID == "" || req.Processor == "" {
		return nil, mustErrorSpec("MISSING_FIELDS")
	}
	it, spec := s.itemByAccessToken(req.AccessToken)
	if spec != nil {
		return nil, spec
	}
	found := false
	for _, a := range it.accounts {
		if a.id == req.AccountID {
			found = true
		}
	}
	if !found {
		return nil, mustErrorSpec(plaid.CodeInvalidAccountID)
	}
	token := s.nextID("processor-sandbox")
	s.processorTokens[token] = ProcessorToken{Token: token, ItemID: it.id, AccountID: req.AccountID, Processor: req.Processor}
	return map[string]any{"processor_token": token, "request_id": requestID}, nil
}

func (s *Server) itemJSON(it *item) map[string]any {
	var itemErr any
	if it.err != nil {
		itemErr = map[string]any{
			"error_type":        it.err.errorType,
			"error_code":        it.err.code,
			"error_code_reason": nil,
			"error_message":     "plaidtest: " + it.err.code,
			"display_message":   nil,
		}
	}
	var consent any
	if it.consentExpiration != nil {
		consent = it.consentExpiration.Format(time.RFC3339)
	}
	return map[string]any{
		"item_id":                 it.id,
		"institution_id":          it.institutionID,
		"institution_name":        s.institutions[it.institutionID].name,
		"webhook":                 it.webhook,
		"auth_method":             "INSTANT_AUTH",
		"error":                   itemErr,
		"available_products":      []string{plaid.ProductIdentity},
		"billed_products":         []string{plaid.ProductAuth},
		"products":                []string{plaid.ProductAuth},
		"consented_products":      []string{plaid.ProductAuth, plaid.ProductIdentity},
		"consent_expiration_time": consent,
		"update_type":             "background",
	}
}

func accountJSON(a *account) map[string]any {
	out := map[string]any{
		"account_id": a.id,
		"balances": map[string]any{
			"available":         nil,
			"current":           100,
			"iso_currency_code": "USD",
		},
		"mask":          a.mask,
		"name":          a.name,
		"official_name": nilIfEmpty(a.officialName),
		"type":          a.accountType,
		"subtype":       a.subtype,
	}
	if a.persistentID != "" {
		out["persistent_account_id"] = a.persistentID
	}
	return out
}

func nilIfEmpty(s string) any {
	if s == "" {
		return nil
	}
	return s
}
