//go:build plaidsandbox

// The plaidsandbox tag runs the client against Plaid's real Sandbox, and
// checks there the behaviours the fake (plaidtest) assumes. It needs Sandbox
// API keys, and never runs against Production (NewSandbox refuses it):
//
//	PLAID_CLIENT_ID=... PLAID_SECRET=... go test -tags plaidsandbox -count=1 -run Sandbox ./plaid/
//
// PLAID_REDIRECT_URI and PLAID_ANDROID_PACKAGE_NAME are optional; when set
// they must be registered in Plaid's Dashboard, and link tokens carry them.
//
// It creates Items with Plaid's Sandbox test user at Sandbox institutions
// only, and removes every Item it creates (in cleanup too, should a step
// fail). Nothing it logs carries a token.
package plaid_test

import (
	"context"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/code-payments/flipcash2-server/plaid"
)

const (
	// sandboxInstitutionID is First Platypus Bank, Plaid's non-OAuth
	// Sandbox institution.
	sandboxInstitutionID = "ins_109508"
	// sandboxTANInstitutionID is Sandbox's Chase, whose accounts carry
	// persistent account IDs.
	sandboxTANInstitutionID = "ins_56"
	// sandboxProcessor is a processor from /processor/token/create's
	// documented list, which (as of 2026-11) does not name Coinbase.
	sandboxProcessor = "dwolla"
)

func sandboxClient(t *testing.T) (*plaid.Client, *plaid.Sandbox) {
	clientID, secret := os.Getenv("PLAID_CLIENT_ID"), os.Getenv("PLAID_SECRET")
	if clientID == "" || secret == "" {
		t.Fatal("PLAID_CLIENT_ID and PLAID_SECRET must be set to Plaid Sandbox API keys")
	}
	c, err := plaid.NewClient(zap.NewNop(), plaid.Config{
		BaseURL:    plaid.SandboxBaseURL,
		ClientID:   clientID,
		Secret:     secret,
		HTTPClient: &http.Client{Timeout: 30 * time.Second},
		Link: plaid.LinkConfig{
			ClientName:         "Flipcash",
			RedirectURI:        os.Getenv("PLAID_REDIRECT_URI"),
			AndroidPackageName: os.Getenv("PLAID_ANDROID_PACKAGE_NAME"),
		},
	})
	require.NoError(t, err)
	sandbox, err := plaid.NewSandbox(c)
	require.NoError(t, err)
	return c, sandbox
}

// sandboxItem creates and exchanges a Sandbox Item, removing it at cleanup
// unless the test already did. It returns the public token too.
func sandboxItem(t *testing.T, c *plaid.Client, sandbox *plaid.Sandbox, institutionID string) (string, *plaid.PublicTokenExchange) {
	ctx := context.Background()
	publicToken, err := sandbox.CreatePublicToken(ctx, institutionID)
	require.NoError(t, err)
	exchange, err := c.ExchangePublicToken(ctx, publicToken)
	require.NoError(t, err)
	t.Logf("created sandbox item %s", exchange.ItemID)
	t.Cleanup(func() {
		err := c.RemoveItem(context.Background(), exchange.AccessToken)
		if err != nil && !plaid.IsItemNotFound(err) {
			t.Errorf("removing sandbox item %s: %v", exchange.ItemID, err)
		}
	})
	return publicToken, exchange
}

func TestSandbox_ItemLifecycle(t *testing.T) {
	c, sandbox := sandboxClient(t)
	ctx := context.Background()

	// Both link token shapes are accepted, auth options included.
	for _, platform := range []plaid.Platform{plaid.PlatformIOS, plaid.PlatformAndroid} {
		token, err := c.CreateLinkToken(ctx, "plaid-sandbox-test-user", platform)
		require.NoError(t, err)
		assert.NotEmpty(t, token.Token)
		assert.WithinDuration(t, time.Now().Add(4*time.Hour), token.Expiration, 5*time.Minute)
	}

	publicToken, exchange := sandboxItem(t, c, sandbox, sandboxInstitutionID)

	// A public token is single-use.
	_, err := c.ExchangePublicToken(ctx, publicToken)
	assert.True(t, plaid.IsInvalidPublicToken(err), "exchanging a public token again: %v", err)

	item, err := c.GetItem(ctx, exchange.AccessToken)
	require.NoError(t, err)
	assert.Equal(t, exchange.ItemID, item.ItemID)
	assert.Equal(t, sandboxInstitutionID, item.InstitutionID)
	assert.Nil(t, item.Error)

	inst, err := c.GetInstitution(ctx, item.InstitutionID)
	require.NoError(t, err)
	assert.NotEmpty(t, inst.Name)

	accounts, err := c.GetAccounts(ctx, exchange.AccessToken)
	require.NoError(t, err)
	assert.Equal(t, exchange.ItemID, accounts.Item.ItemID)
	var checking *plaid.Account
	for i, a := range accounts.Accounts {
		assert.NotEmpty(t, a.AccountID)
		if a.Type == plaid.AccountTypeDepository && a.Subtype == plaid.AccountSubtypeChecking && checking == nil {
			checking = &accounts.Accounts[i]
		}
	}
	require.NotNil(t, checking, "the Sandbox user has a checking account")

	processorToken, err := c.CreateProcessorToken(ctx, exchange.AccessToken, checking.AccountID, sandboxProcessor)
	require.NoError(t, err)
	assert.NotEmpty(t, processorToken)

	_, err = c.CreateProcessorToken(ctx, exchange.AccessToken, "not-an-account", sandboxProcessor)
	assert.True(t, plaid.IsCode(err, plaid.CodeInvalidAccountID), "unknown account: %v", err)

	update, err := c.CreateUpdateLinkToken(ctx, "plaid-sandbox-test-user", exchange.AccessToken, plaid.PlatformIOS)
	require.NoError(t, err)
	assert.WithinDuration(t, time.Now().Add(30*time.Minute), update.Expiration, 5*time.Minute)

	// ITEM_LOGIN_REQUIRED is on /item/get's item, and fails /accounts/get.
	require.NoError(t, sandbox.ResetLogin(ctx, exchange.AccessToken))
	item, err = c.GetItem(ctx, exchange.AccessToken)
	require.NoError(t, err)
	require.NotNil(t, item.Error)
	assert.True(t, plaid.IsItemLoginRequired(item.Error.Err()), "item error %s", item.Error.ErrorCode)
	_, err = c.GetAccounts(ctx, exchange.AccessToken)
	assert.True(t, plaid.IsItemLoginRequired(err), "/accounts/get on an Item needing login: %v", err)

	// Update mode still opens for it.
	_, err = c.CreateUpdateLinkToken(ctx, "plaid-sandbox-test-user", exchange.AccessToken, plaid.PlatformAndroid)
	require.NoError(t, err)

	// Removed: ITEM_NOT_FOUND everywhere, removal included.
	require.NoError(t, c.RemoveItem(ctx, exchange.AccessToken))
	_, err = c.GetItem(ctx, exchange.AccessToken)
	assert.True(t, plaid.IsItemNotFound(err), "/item/get after removal: %v", err)
	_, err = c.GetAccounts(ctx, exchange.AccessToken)
	assert.True(t, plaid.IsItemNotFound(err), "/accounts/get after removal: %v", err)
	err = c.RemoveItem(ctx, exchange.AccessToken)
	assert.True(t, plaid.IsItemNotFound(err), "/item/remove after removal: %v", err)
	_, err = c.CreateUpdateLinkToken(ctx, "plaid-sandbox-test-user", exchange.AccessToken, plaid.PlatformIOS)
	assert.True(t, plaid.IsItemNotFound(err), "update link token after removal: %v", err)

	_, err = c.GetInstitution(ctx, "ins_does_not_exist")
	assert.True(t, plaid.IsCode(err, plaid.CodeInvalidInstitution), "unknown institution: %v", err)
}

func TestSandbox_PersistentAccountID(t *testing.T) {
	c, sandbox := sandboxClient(t)
	_, exchange := sandboxItem(t, c, sandbox, sandboxTANInstitutionID)

	accounts, err := c.GetAccounts(context.Background(), exchange.AccessToken)
	require.NoError(t, err)
	sawPersistent := false
	for _, a := range accounts.Accounts {
		if a.Type == plaid.AccountTypeDepository && a.PersistentAccountID != "" {
			sawPersistent = true
		}
	}
	assert.True(t, sawPersistent, "a TAN institution's depository accounts carry persistent account ids")
}
