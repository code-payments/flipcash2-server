package plaidtest_test

import (
	"context"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/code-payments/flipcash2-server/plaid"
	"github.com/code-payments/flipcash2-server/plaid/plaidtest"
)

const userID = "0ba20545-0587-44a4-a43e-725937f5f15c"

// clock is a settable clock for the fake.
type clock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *clock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *clock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

func newFake(t *testing.T) (*plaidtest.Server, plaid.API, *clock) {
	clk := &clock{now: time.Date(2026, 11, 25, 12, 0, 0, 0, time.UTC)}
	s := plaidtest.NewServer(plaidtest.WithClock(clk.Now))
	t.Cleanup(s.Close)
	return s, s.NewClient(zap.NewNop()), clk
}

// link creates an Item through the fake and exchanges it, as the service
// would after Link.
func link(t *testing.T, s *plaidtest.Server, api plaid.API, spec plaidtest.ItemSpec) *plaid.PublicTokenExchange {
	publicToken, itemID := s.CreatePublicToken(spec)
	res, err := api.ExchangePublicToken(context.Background(), publicToken)
	require.NoError(t, err)
	require.Equal(t, itemID, res.ItemID)
	return res
}

func TestFake_LinkFlow(t *testing.T) {
	s, api, _ := newFake(t)
	ctx := context.Background()

	token, err := api.CreateLinkToken(ctx, userID, plaid.PlatformIOS)
	require.NoError(t, err)
	assert.NotEmpty(t, token.Token)
	assert.Equal(t, time.Date(2026, 11, 25, 16, 0, 0, 0, time.UTC), token.Expiration.UTC())

	reqs := s.LinkTokenRequests()
	require.Len(t, reqs, 1)
	assert.Equal(t, userID, reqs[0].ClientUserID)
	assert.Equal(t, []string{"auth"}, reqs[0].Products)
	assert.Equal(t, []string{"identity"}, reqs[0].AdditionalConsentedProducts)
	assert.Equal(t, []string{"US"}, reqs[0].CountryCodes)
	assert.Equal(t, "en", reqs[0].Language)
	assert.Equal(t, []string{"checking", "savings"}, reqs[0].AccountSubtypes)
	assert.Equal(t, "https://app.example.com/plaid/oauth", reqs[0].RedirectURI)
	assert.Empty(t, reqs[0].AndroidPackageName)
	assert.Empty(t, reqs[0].AccessToken)
	assert.Equal(t, true, reqs[0].Auth["instant_match_enabled"])
	assert.Equal(t, false, reqs[0].Auth["same_day_microdeposits_enabled"])

	exchange := link(t, s, api, plaidtest.ItemSpec{
		Accounts: []plaidtest.AccountSpec{
			{Subtype: plaid.AccountSubtypeChecking, Mask: "0000"},
			{Subtype: plaid.AccountSubtypeSavings, Mask: "1111"},
		},
	})

	item, err := api.GetItem(ctx, exchange.AccessToken)
	require.NoError(t, err)
	assert.Equal(t, exchange.ItemID, item.ItemID)
	assert.Equal(t, plaidtest.DefaultInstitutionID, item.InstitutionID)
	assert.Equal(t, plaidtest.DefaultInstitutionName, item.InstitutionName)
	assert.Nil(t, item.Error)
	assert.Nil(t, item.ConsentExpirationTime)

	accounts, err := api.GetAccounts(ctx, exchange.AccessToken)
	require.NoError(t, err)
	assert.Equal(t, exchange.ItemID, accounts.Item.ItemID)
	require.Len(t, accounts.Accounts, 2)
	assert.Equal(t, s.AccountIDs(exchange.ItemID), []string{accounts.Accounts[0].AccountID, accounts.Accounts[1].AccountID})
	assert.Equal(t, "Plaid Checking", accounts.Accounts[0].Name)
	assert.Equal(t, "0000", accounts.Accounts[0].Mask)
	assert.Equal(t, plaid.AccountTypeDepository, accounts.Accounts[0].Type)
	assert.Equal(t, "savings", accounts.Accounts[1].Subtype)
	assert.Empty(t, accounts.Accounts[0].PersistentAccountID, "no persistent ids outside TAN banks")

	inst, err := api.GetInstitution(ctx, item.InstitutionID)
	require.NoError(t, err)
	assert.Equal(t, plaidtest.DefaultInstitutionName, inst.Name)

	_, err = api.GetInstitution(ctx, "ins_unknown")
	assert.True(t, plaid.IsCode(err, plaid.CodeInvalidInstitution))
}

func TestFake_UpdateLinkToken(t *testing.T) {
	s, api, _ := newFake(t)
	ctx := context.Background()
	exchange := link(t, s, api, plaidtest.ItemSpec{})

	token, err := api.CreateUpdateLinkToken(ctx, userID, exchange.AccessToken, plaid.PlatformAndroid)
	require.NoError(t, err)
	assert.Equal(t, time.Date(2026, 11, 25, 12, 30, 0, 0, time.UTC), token.Expiration.UTC())

	reqs := s.LinkTokenRequests()
	require.Len(t, reqs, 1)
	assert.Equal(t, exchange.ItemID, reqs[0].ItemID)
	assert.True(t, reqs[0].AccountSelectionEnabled)
	assert.Empty(t, reqs[0].Products)
	assert.Nil(t, reqs[0].Auth)
	assert.Equal(t, "com.example.app", reqs[0].AndroidPackageName)
	assert.Empty(t, reqs[0].RedirectURI)

	// Update mode is how an Item in error is repaired, so an error does not
	// block it.
	s.SetItemError(exchange.ItemID, plaid.CodeItemLoginRequired)
	_, err = api.CreateUpdateLinkToken(ctx, userID, exchange.AccessToken, plaid.PlatformIOS)
	require.NoError(t, err)

	_, err = api.CreateUpdateLinkToken(ctx, userID, "access-sandbox-unknown", plaid.PlatformIOS)
	assert.True(t, plaid.IsInvalidAccessToken(err))

	s.RemoveItem(exchange.ItemID)
	_, err = api.CreateUpdateLinkToken(ctx, userID, exchange.AccessToken, plaid.PlatformIOS)
	assert.True(t, plaid.IsItemNotFound(err))
}

func TestFake_PublicTokenSingleUseAndExpiry(t *testing.T) {
	s, api, clk := newFake(t)
	ctx := context.Background()

	publicToken, _ := s.CreatePublicToken(plaidtest.ItemSpec{})
	_, err := api.ExchangePublicToken(ctx, publicToken)
	require.NoError(t, err)
	_, err = api.ExchangePublicToken(ctx, publicToken)
	assert.True(t, plaid.IsInvalidPublicToken(err), "a public token is single-use")

	publicToken, _ = s.CreatePublicToken(plaidtest.ItemSpec{})
	clk.Advance(plaidtest.PublicTokenLifetime)
	_, err = api.ExchangePublicToken(ctx, publicToken)
	assert.True(t, plaid.IsInvalidPublicToken(err), "a public token expires")

	_, err = api.ExchangePublicToken(ctx, "public-sandbox-unknown")
	assert.True(t, plaid.IsInvalidPublicToken(err))
}

func TestFake_ItemErrors(t *testing.T) {
	s, api, _ := newFake(t)
	ctx := context.Background()
	exchange := link(t, s, api, plaidtest.ItemSpec{})

	for _, code := range []string{plaid.CodeItemLoginRequired, plaid.CodeNoAccounts, plaid.CodeInstitutionDown, plaid.CodeItemLocked} {
		s.SetItemError(exchange.ItemID, code)

		item, err := api.GetItem(ctx, exchange.AccessToken)
		require.NoError(t, err, "an Item in error is not a failed /item/get")
		require.NotNil(t, item.Error)
		assert.Equal(t, code, item.Error.ErrorCode)
		assert.True(t, plaid.IsCode(item.Error.Err(), code))

		_, err = api.GetAccounts(ctx, exchange.AccessToken)
		assert.True(t, plaid.IsCode(err, code), "/accounts/get fails with the Item's error")
	}
	s.SetItemError(exchange.ItemID, plaid.CodeInstitutionNotResponding)
	_, err := api.GetAccounts(ctx, exchange.AccessToken)
	assert.True(t, plaid.IsInstitutionDown(err))
	assert.True(t, plaid.IsRetryable(err))

	s.ClearItemError(exchange.ItemID)
	item, err := api.GetItem(ctx, exchange.AccessToken)
	require.NoError(t, err)
	assert.Nil(t, item.Error)
	_, err = api.GetAccounts(ctx, exchange.AccessToken)
	require.NoError(t, err)
}

func TestFake_ConsentExpiration(t *testing.T) {
	s, api, _ := newFake(t)
	ctx := context.Background()
	exchange := link(t, s, api, plaidtest.ItemSpec{})

	expires := time.Date(2027, 2, 1, 0, 0, 0, 0, time.UTC)
	s.SetConsentExpirationTime(exchange.ItemID, &expires)
	item, err := api.GetItem(ctx, exchange.AccessToken)
	require.NoError(t, err)
	require.NotNil(t, item.ConsentExpirationTime)
	assert.True(t, expires.Equal(*item.ConsentExpirationTime))

	s.SetConsentExpirationTime(exchange.ItemID, nil)
	item, err = api.GetItem(ctx, exchange.AccessToken)
	require.NoError(t, err)
	assert.Nil(t, item.ConsentExpirationTime)
}

func TestFake_Accounts(t *testing.T) {
	s, api, _ := newFake(t)
	ctx := context.Background()

	// A TAN bank gives persistent ids, unless an account is told not to have one.
	exchange := link(t, s, api, plaidtest.ItemSpec{
		InstitutionID: plaidtest.TANInstitutionID,
		Accounts: []plaidtest.AccountSpec{
			{},
			{Subtype: plaid.AccountSubtypeSavings, NoPersistentID: true},
		},
	})
	accounts, err := api.GetAccounts(ctx, exchange.AccessToken)
	require.NoError(t, err)
	require.Len(t, accounts.Accounts, 2)
	checking, savings := accounts.Accounts[0], accounts.Accounts[1]
	assert.NotEmpty(t, checking.PersistentAccountID)
	assert.Empty(t, savings.PersistentAccountID)

	// Plaid fails to reconcile an account: new account_id, same persistent id.
	newID := s.ChangeAccountID(exchange.ItemID, checking.AccountID)
	assert.NotEqual(t, checking.AccountID, newID)
	accounts, err = api.GetAccounts(ctx, exchange.AccessToken)
	require.NoError(t, err)
	assert.Equal(t, newID, accounts.Accounts[0].AccountID)
	assert.Equal(t, checking.PersistentAccountID, accounts.Accounts[0].PersistentAccountID)

	// The user shares another account, then stops sharing one.
	added := s.AddAccount(exchange.ItemID, plaidtest.AccountSpec{PersistentAccountID: "persistent-given", Name: "Joint"})
	s.RemoveAccount(exchange.ItemID, savings.AccountID)
	accounts, err = api.GetAccounts(ctx, exchange.AccessToken)
	require.NoError(t, err)
	require.Len(t, accounts.Accounts, 2)
	assert.Equal(t, newID, accounts.Accounts[0].AccountID)
	assert.Equal(t, added, accounts.Accounts[1].AccountID)
	assert.Equal(t, "persistent-given", accounts.Accounts[1].PersistentAccountID)
	assert.Equal(t, "Joint", accounts.Accounts[1].Name)

	// No accounts left: the Item stays, /accounts/get is NO_ACCOUNTS.
	s.RemoveAccount(exchange.ItemID, newID)
	s.RemoveAccount(exchange.ItemID, added)
	_, err = api.GetAccounts(ctx, exchange.AccessToken)
	assert.True(t, plaid.IsNoAccounts(err))
	_, err = api.GetItem(ctx, exchange.AccessToken)
	require.NoError(t, err)
}

func TestFake_ProcessorTokens(t *testing.T) {
	s, api, _ := newFake(t)
	ctx := context.Background()
	exchange := link(t, s, api, plaidtest.ItemSpec{Accounts: []plaidtest.AccountSpec{{}, {}}})
	ids := s.AccountIDs(exchange.ItemID)

	token, err := api.CreateProcessorToken(ctx, exchange.AccessToken, ids[0], "dwolla")
	require.NoError(t, err)
	assert.Equal(t, []plaidtest.ProcessorToken{
		{Token: token, ItemID: exchange.ItemID, AccountID: ids[0], Processor: "dwolla"},
	}, s.ProcessorTokens(exchange.ItemID))

	_, err = api.CreateProcessorToken(ctx, exchange.AccessToken, "acct-unknown", "dwolla")
	assert.True(t, plaid.IsCode(err, plaid.CodeInvalidAccountID))

	// A changed account id leaves the old one unusable, and its tokens dead.
	newID := s.ChangeAccountID(exchange.ItemID, ids[0])
	assert.Empty(t, s.ProcessorTokens(exchange.ItemID))
	_, err = api.CreateProcessorToken(ctx, exchange.AccessToken, ids[0], "dwolla")
	assert.True(t, plaid.IsCode(err, plaid.CodeInvalidAccountID))
	_, err = api.CreateProcessorToken(ctx, exchange.AccessToken, newID, "dwolla")
	require.NoError(t, err)

	// Removing the Item ends every token.
	require.NoError(t, api.RemoveItem(ctx, exchange.AccessToken))
	assert.Empty(t, s.ProcessorTokens(exchange.ItemID))
}

func TestFake_RemoveItem(t *testing.T) {
	s, api, _ := newFake(t)
	ctx := context.Background()

	exchange := link(t, s, api, plaidtest.ItemSpec{})
	require.NoError(t, api.RemoveItem(ctx, exchange.AccessToken))
	assert.True(t, s.IsItemRemoved(exchange.ItemID))

	_, err := api.GetItem(ctx, exchange.AccessToken)
	assert.True(t, plaid.IsItemNotFound(err))
	_, err = api.GetAccounts(ctx, exchange.AccessToken)
	assert.True(t, plaid.IsItemNotFound(err))
	_, err = api.CreateProcessorToken(ctx, exchange.AccessToken, s.AccountIDs(exchange.ItemID)[0], "dwolla")
	assert.True(t, plaid.IsItemNotFound(err))
	assert.True(t, plaid.IsItemNotFound(api.RemoveItem(ctx, exchange.AccessToken)), "removing again is ITEM_NOT_FOUND")

	// Gone at Plaid without /item/remove (the user, at my.plaid.com).
	exchange = link(t, s, api, plaidtest.ItemSpec{})
	s.RemoveItem(exchange.ItemID)
	_, err = api.GetItem(ctx, exchange.AccessToken)
	assert.True(t, plaid.IsItemNotFound(err))

	err = api.RemoveItem(ctx, "access-sandbox-never-issued")
	assert.True(t, plaid.IsInvalidAccessToken(err))
}

func TestFake_FailNext(t *testing.T) {
	s, api, _ := newFake(t)
	ctx := context.Background()
	exchange := link(t, s, api, plaidtest.ItemSpec{})

	s.FailNext("ITEM_GET_LIMIT")
	s.FailNext(plaid.CodeInternalServerError)
	s.FailNextStatus(http.StatusBadGateway)

	_, err := api.GetItem(ctx, exchange.AccessToken)
	assert.True(t, plaid.IsRateLimited(err))
	perr, ok := plaid.AsError(err)
	require.True(t, ok)
	assert.Equal(t, http.StatusTooManyRequests, perr.StatusCode)
	assert.Equal(t, plaid.ErrorTypeRateLimitExceeded, perr.Type)
	assert.NotEmpty(t, perr.RequestID)

	_, err = api.GetAccounts(ctx, exchange.AccessToken)
	perr, ok = plaid.AsError(err)
	require.True(t, ok)
	assert.Equal(t, http.StatusInternalServerError, perr.StatusCode)
	assert.True(t, plaid.IsRetryable(err))

	err = api.RemoveItem(ctx, exchange.AccessToken)
	perr, ok = plaid.AsError(err)
	require.True(t, ok)
	assert.Equal(t, &plaid.Error{StatusCode: http.StatusBadGateway}, perr)
	assert.False(t, s.IsItemRemoved(exchange.ItemID), "a failed request changes nothing")

	// The queue is drained.
	_, err = api.GetItem(ctx, exchange.AccessToken)
	require.NoError(t, err)

	// An injected failure applies to a link token too.
	s.FailNext(plaid.CodeInvalidAPIKeys)
	_, err = api.CreateLinkToken(ctx, userID, plaid.PlatformIOS)
	assert.True(t, plaid.IsConfigError(err))
}

func TestFake_RefusesWrongCredentials(t *testing.T) {
	s, _, _ := newFake(t)
	cfg := s.Config()
	cfg.Secret = "wrong"
	c, err := plaid.NewClient(zap.NewNop(), cfg)
	require.NoError(t, err)

	_, err = c.GetInstitution(context.Background(), plaidtest.DefaultInstitutionID)
	assert.True(t, plaid.IsCode(err, plaid.CodeInvalidAPIKeys))
	assert.True(t, plaid.IsConfigError(err))
}

func TestFake_PanicsOnUnknownCode(t *testing.T) {
	s, _, _ := newFake(t)
	assert.Panics(t, func() { s.FailNext("NOT_A_PLAID_CODE") })
}
