package plaid

import (
	"context"
	"fmt"
)

type accessTokenRequest struct {
	credentials
	AccessToken string `json:"access_token"`
}

type publicTokenExchangeRequest struct {
	credentials
	PublicToken string `json:"public_token"`
}

type itemGetResponse struct {
	Item      Item   `json:"item"`
	RequestID string `json:"request_id"`
}

type institutionsGetByIDRequest struct {
	credentials
	InstitutionID string   `json:"institution_id"`
	CountryCodes  []string `json:"country_codes"`
}

type institutionsGetByIDResponse struct {
	Institution Institution `json:"institution"`
	RequestID   string      `json:"request_id"`
}

type processorTokenCreateRequest struct {
	credentials
	AccessToken string `json:"access_token"`
	AccountID   string `json:"account_id"`
	Processor   string `json:"processor"`
}

type processorTokenCreateResponse struct {
	ProcessorToken string `json:"processor_token"`
	RequestID      string `json:"request_id"`
}

// ExchangePublicToken exchanges the public token Link's onSuccess returned
// for the new Item's access token and ID. A public token can be exchanged
// once, within 30 minutes; otherwise Plaid answers INVALID_PUBLIC_TOKEN
// (IsInvalidPublicToken).
func (c *Client) ExchangePublicToken(ctx context.Context, publicToken string) (*PublicTokenExchange, error) {
	return trace(ctx, "ExchangePublicToken", func() (*PublicTokenExchange, error) {
		if err := requireNonEmpty("public token", publicToken); err != nil {
			return nil, err
		}
		var res PublicTokenExchange
		req := publicTokenExchangeRequest{credentials: c.credentials(), PublicToken: publicToken}
		if err := c.do(ctx, pathPublicTokenExchange, req, &res); err != nil {
			return nil, err
		}
		if res.AccessToken == "" || res.ItemID == "" {
			return nil, fmt.Errorf("%w: %s returned no access token or item id (request id %s)", ErrMalformedResponse, pathPublicTokenExchange, res.RequestID)
		}
		return &res, nil
	})
}

// GetItem reads an Item. An Item in error is not a failed call: its error
// is on Item.Error. A gone Item is ITEM_NOT_FOUND.
func (c *Client) GetItem(ctx context.Context, accessToken string) (*Item, error) {
	return trace(ctx, "GetItem", func() (*Item, error) {
		if err := requireNonEmpty("access token", accessToken); err != nil {
			return nil, err
		}
		var res itemGetResponse
		req := accessTokenRequest{credentials: c.credentials(), AccessToken: accessToken}
		if err := c.do(ctx, pathItemGet, req, &res); err != nil {
			return nil, err
		}
		if res.Item.ItemID == "" {
			return nil, fmt.Errorf("%w: %s returned no item (request id %s)", ErrMalformedResponse, pathItemGet, res.RequestID)
		}
		return &res.Item, nil
	})
}

// GetAccounts reads the accounts the user shares on an Item, from Plaid's
// cache (it does not ask the bank), and the Item. It fails with the Item's
// error when the Item is in one.
func (c *Client) GetAccounts(ctx context.Context, accessToken string) (*Accounts, error) {
	return trace(ctx, "GetAccounts", func() (*Accounts, error) {
		if err := requireNonEmpty("access token", accessToken); err != nil {
			return nil, err
		}
		var res Accounts
		req := accessTokenRequest{credentials: c.credentials(), AccessToken: accessToken}
		if err := c.do(ctx, pathAccountsGet, req, &res); err != nil {
			return nil, err
		}
		if res.Item.ItemID == "" {
			return nil, fmt.Errorf("%w: %s returned no item (request id %s)", ErrMalformedResponse, pathAccountsGet, res.RequestID)
		}
		return &res, nil
	})
}

// RemoveItem removes an Item at Plaid. Its access token, and every processor
// token made from it, stop working. Removing an Item already gone is
// ITEM_NOT_FOUND, which a caller removing it treats as done.
func (c *Client) RemoveItem(ctx context.Context, accessToken string) error {
	_, err := trace(ctx, "RemoveItem", func() (struct{}, error) {
		if err := requireNonEmpty("access token", accessToken); err != nil {
			return struct{}{}, err
		}
		req := accessTokenRequest{credentials: c.credentials(), AccessToken: accessToken}
		return struct{}{}, c.do(ctx, pathItemRemove, req, nil)
	})
	return err
}

// GetInstitution reads a US bank by Plaid's institution ID. An unknown ID is
// INVALID_INSTITUTION.
func (c *Client) GetInstitution(ctx context.Context, institutionID string) (*Institution, error) {
	return trace(ctx, "GetInstitution", func() (*Institution, error) {
		if err := requireNonEmpty("institution id", institutionID); err != nil {
			return nil, err
		}
		var res institutionsGetByIDResponse
		req := institutionsGetByIDRequest{
			credentials:   c.credentials(),
			InstitutionID: institutionID,
			CountryCodes:  linkCountryCodes,
		}
		if err := c.do(ctx, pathInstitutionsGetByID, req, &res); err != nil {
			return nil, err
		}
		if res.Institution.InstitutionID == "" {
			return nil, fmt.Errorf("%w: %s returned no institution (request id %s)", ErrMalformedResponse, pathInstitutionsGetByID, res.RequestID)
		}
		return &res.Institution, nil
	})
}

// CreateProcessorToken creates a processor token for one account on an
// Item, for the payment processor processor (Plaid's name for it, e.g.
// "dwolla"). An account no longer on the Item is INVALID_ACCOUNT_ID.
func (c *Client) CreateProcessorToken(ctx context.Context, accessToken, accountID, processor string) (string, error) {
	return trace(ctx, "CreateProcessorToken", func() (string, error) {
		if err := requireNonEmpty("access token", accessToken); err != nil {
			return "", err
		}
		if err := requireNonEmpty("account id", accountID); err != nil {
			return "", err
		}
		if err := requireNonEmpty("processor", processor); err != nil {
			return "", err
		}
		var res processorTokenCreateResponse
		req := processorTokenCreateRequest{
			credentials: c.credentials(),
			AccessToken: accessToken,
			AccountID:   accountID,
			Processor:   processor,
		}
		if err := c.do(ctx, pathProcessorTokenCreate, req, &res); err != nil {
			return "", err
		}
		if res.ProcessorToken == "" {
			return "", fmt.Errorf("%w: %s returned no processor token (request id %s)", ErrMalformedResponse, pathProcessorTokenCreate, res.RequestID)
		}
		return res.ProcessorToken, nil
	})
}
