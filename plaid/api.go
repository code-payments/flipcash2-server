package plaid

import "context"

// API is the Plaid API as the bank service uses it. Client implements it;
// tests run a Client against plaidtest's fake.
type API interface {
	// CreateLinkToken creates a link token for linking a new bank.
	CreateLinkToken(ctx context.Context, clientUserID string, platform Platform) (*LinkToken, error)
	// CreateUpdateLinkToken creates a link token for update mode, with
	// account selection on, for an existing Item.
	CreateUpdateLinkToken(ctx context.Context, clientUserID string, accessToken string, platform Platform) (*LinkToken, error)
	// ExchangePublicToken exchanges Link's public token for an Item.
	ExchangePublicToken(ctx context.Context, publicToken string) (*PublicTokenExchange, error)
	// GetItem reads an Item, with its error if it is in one.
	GetItem(ctx context.Context, accessToken string) (*Item, error)
	// GetAccounts reads the accounts the user shares on an Item.
	GetAccounts(ctx context.Context, accessToken string) (*Accounts, error)
	// RemoveItem removes an Item at Plaid.
	RemoveItem(ctx context.Context, accessToken string) error
	// GetInstitution reads a US bank.
	GetInstitution(ctx context.Context, institutionID string) (*Institution, error)
	// CreateProcessorToken creates a processor token for one account.
	CreateProcessorToken(ctx context.Context, accessToken, accountID, processor string) (string, error)
}

var _ API = (*Client)(nil)
