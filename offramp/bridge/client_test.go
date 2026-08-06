package bridge

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type capturedRequest struct {
	Method         string
	Path           string
	APIKey         string
	IdempotencyKey string
	ContentType    string
	Body           []byte
}

func newTestClient(t *testing.T, statusCode int, respBody string) (*Client, *capturedRequest) {
	captured := &capturedRequest{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		captured.Method = r.Method
		captured.Path = r.URL.Path
		captured.APIKey = r.Header.Get("Api-Key")
		captured.IdempotencyKey = r.Header.Get("Idempotency-Key")
		captured.ContentType = r.Header.Get("Content-Type")
		captured.Body, _ = io.ReadAll(r.Body)

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(statusCode)
		_, _ = w.Write([]byte(respBody))
	}))
	t.Cleanup(server.Close)

	return NewClient("test-api-key", WithBaseURL(server.URL)), captured
}

func TestClient_CreateCustomer(t *testing.T) {
	client, captured := newTestClient(t, http.StatusCreated, `{
		"id": "customer-1",
		"first_name": "Ada",
		"last_name": "Lovelace",
		"status": "under_review",
		"has_accepted_terms_of_service": true,
		"endorsements": [{
			"name": "base",
			"status": "incomplete",
			"requirements": {"complete": ["first_name"], "pending": ["id_verification"], "missing": null, "issues": []}
		}]
	}`)

	customer, err := client.CreateCustomer(context.Background(), "idem-1", &CreateCustomerRequest{
		Type:      "individual",
		FirstName: "Ada",
		LastName:  "Lovelace",
		Email:     "ada@example.com",
		BirthDate: "1990-05-15",
		ResidentialAddress: Address{
			StreetLine1: "123 Main Street",
			City:        "San Francisco",
			Subdivision: "CA",
			PostalCode:  "94107",
			Country:     "USA",
		},
		IdentifyingInformation: []IdentifyingInformation{
			{Type: IDTypeSSN, IssuingCountry: "USA", Number: "123-45-6789"},
			{Type: IDTypeDriversLicense, IssuingCountry: "USA", ImageFront: "data:image/jpeg;base64,AAAA"},
		},
		SignedAgreementID: "agreement-1",
		Endorsements:      []string{EndorsementBase},
	})
	require.NoError(t, err)

	assert.Equal(t, http.MethodPost, captured.Method)
	assert.Equal(t, "/v0/customers", captured.Path)
	assert.Equal(t, "test-api-key", captured.APIKey)
	assert.Equal(t, "idem-1", captured.IdempotencyKey)
	assert.Equal(t, "application/json", captured.ContentType)

	var sent map[string]any
	require.NoError(t, json.Unmarshal(captured.Body, &sent))
	assert.Equal(t, "individual", sent["type"])
	assert.Equal(t, "agreement-1", sent["signed_agreement_id"])
	assert.Len(t, sent["identifying_information"], 2)

	assert.Equal(t, "customer-1", customer.ID)
	assert.Equal(t, "Ada", customer.FirstName)
	assert.Equal(t, "Lovelace", customer.LastName)
	assert.Equal(t, CustomerStatusUnderReview, customer.Status)
	assert.True(t, customer.HasAcceptedTermsOfService)

	base := customer.BaseEndorsement()
	require.NotNil(t, base)
	assert.Equal(t, EndorsementStatusIncomplete, base.Status)
	assert.Equal(t, []string{"id_verification"}, base.Requirements.Pending)
}

func TestClient_CreateTOSLink(t *testing.T) {
	client, captured := newTestClient(t, http.StatusOK, `{"url": "https://dashboard.bridge.xyz/accept-terms-of-service?session_token=abc"}`)

	link, err := client.CreateTOSLink(context.Background(), "idem-tos")
	require.NoError(t, err)

	assert.Equal(t, http.MethodPost, captured.Method)
	assert.Equal(t, "/v0/customers/tos_links", captured.Path)
	assert.Equal(t, "idem-tos", captured.IdempotencyKey)
	assert.Contains(t, link.URL, "accept-terms-of-service")
}

func TestClient_CreateExternalAccount(t *testing.T) {
	client, captured := newTestClient(t, http.StatusCreated, `{
		"id": "ea-1",
		"customer_id": "customer-1",
		"currency": "usd",
		"account_type": "us",
		"bank_name": "Lead Bank",
		"last_4": "9123",
		"active": true,
		"account": {"routing_number": "101019644", "last_4": "9123"}
	}`)

	account, err := client.CreateExternalAccount(context.Background(), "idem-ea", "customer-1", &CreateExternalAccountRequest{
		Currency:         CurrencyUSD,
		AccountType:      ExternalAccountTypeUS,
		AccountOwnerName: "Ada Lovelace",
		FirstName:        "Ada",
		LastName:         "Lovelace",
		AccountOwnerType: AccountOwnerTypeIndividual,
		Account: USBankAccount{
			RoutingNumber:     "101019644",
			AccountNumber:     "215268129123",
			CheckingOrSavings: CheckingAccount,
		},
		Address: ExternalAccountAddress{
			StreetLine1: "123 Main Street",
			City:        "San Francisco",
			State:       "CA",
			PostalCode:  "94107",
			Country:     "USA",
		},
	})
	require.NoError(t, err)

	assert.Equal(t, "/v0/customers/customer-1/external_accounts", captured.Path)
	assert.Equal(t, "ea-1", account.ID)
	assert.Equal(t, "9123", account.Last4)
	require.NotNil(t, account.Account)
	assert.Equal(t, "101019644", account.Account.RoutingNumber)

	var sent map[string]any
	require.NoError(t, json.Unmarshal(captured.Body, &sent))
	assert.Equal(t, "Ada Lovelace", sent["account_owner_name"])
	address := sent["address"].(map[string]any)
	assert.Equal(t, "CA", address["state"])
	assert.NotContains(t, address, "subdivision")
}

func TestClient_GetExternalAccounts_WrappedList(t *testing.T) {
	client, captured := newTestClient(t, http.StatusOK, `{"count": 1, "data": [{"id": "ea-1", "bank_name": "Chase", "last_4": "1234", "active": true}]}`)

	accounts, err := client.GetExternalAccounts(context.Background(), "customer-1")
	require.NoError(t, err)

	assert.Equal(t, http.MethodGet, captured.Method)
	assert.Empty(t, captured.IdempotencyKey)
	require.Len(t, accounts, 1)
	assert.Equal(t, "ea-1", accounts[0].ID)
}

func TestClient_CreateLiquidationAddress(t *testing.T) {
	client, captured := newTestClient(t, http.StatusCreated, `{
		"id": "la-1",
		"address": "So1anaAddre55",
		"chain": "solana",
		"currency": "usdc",
		"external_account_id": "ea-1",
		"destination_payment_rail": "ach",
		"destination_currency": "usd",
		"state": "active"
	}`)

	address, err := client.CreateLiquidationAddress(context.Background(), "idem-la", "customer-1", &CreateLiquidationAddressRequest{
		Chain:                     ChainSolana,
		Currency:                  CurrencyUSDC,
		ExternalAccountID:         "ea-1",
		DestinationPaymentRail:    PaymentRailACH,
		DestinationCurrency:       CurrencyUSD,
		ReturnInstructions:        &ReturnInstructions{Address: "UserWa11etAddre55"},
		CustomDeveloperFeePercent: "1.5",
	})
	require.NoError(t, err)

	assert.Equal(t, "/v0/customers/customer-1/liquidation_addresses", captured.Path)
	assert.Equal(t, "la-1", address.ID)
	assert.Equal(t, "So1anaAddre55", address.Address)
	assert.Equal(t, "active", address.State)

	var sent map[string]any
	require.NoError(t, json.Unmarshal(captured.Body, &sent))
	assert.Equal(t, map[string]any{"address": "UserWa11etAddre55"}, sent["return_instructions"])
	assert.Equal(t, "1.5", sent["custom_developer_fee_percent"])
	assert.NotContains(t, sent, "return_address")
	assert.NotContains(t, sent, "developer_fee_percent")
}

func TestClient_ListPagination(t *testing.T) {
	captured := &capturedRequest{}
	var query string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		captured.Path = r.URL.Path
		query = r.URL.RawQuery
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"data": []}`))
	}))
	t.Cleanup(server.Close)
	client := NewClient("test-api-key", WithBaseURL(server.URL))

	_, err := client.GetDrains(context.Background(), "customer-1", "la-1",
		WithLimit(100),
		WithStartingAfter("drain-50"),
		WithUpdatedAfter(1753700000000),
	)
	require.NoError(t, err)

	values, err := url.ParseQuery(query)
	require.NoError(t, err)
	assert.Equal(t, "100", values.Get("limit"))
	assert.Equal(t, "drain-50", values.Get("starting_after"))
	assert.Equal(t, "1753700000000", values.Get("updated_after_ms"))

	// No options → no query string at all.
	_, err = client.GetExternalAccounts(context.Background(), "customer-1")
	require.NoError(t, err)
	assert.Empty(t, query)
}

func TestClient_GetDrains_BareArrayList(t *testing.T) {
	client, captured := newTestClient(t, http.StatusOK, `[{
		"id": "drain-1",
		"amount": "100.0",
		"currency": "usd",
		"state": "payment_processed",
		"destination": {"payment_rail": "ach", "currency": "usd"},
		"deposit_tx_hash": "deposit-hash",
		"destination_tx_hash": "dest-hash"
	}]`)

	drains, err := client.GetDrains(context.Background(), "customer-1", "la-1")
	require.NoError(t, err)

	assert.Equal(t, "/v0/customers/customer-1/liquidation_addresses/la-1/drains", captured.Path)
	require.Len(t, drains, 1)
	assert.Equal(t, DrainStatePaymentProcessed, drains[0].State)
	assert.True(t, drains[0].IsTerminal())
}

func TestClient_DrainIsTerminal(t *testing.T) {
	for state, expected := range map[string]bool{
		DrainStateAwaitingFunds:       false,
		DrainStateInReview:            false,
		DrainStateFundsReceived:       false,
		DrainStatePaymentSubmitted:    false,
		DrainStateReturned:            false,
		DrainStateRefundInFlight:      false,
		DrainStateRefundFailed:        false,
		DrainStateMissingReturnPolicy: false,
		"some_future_state":           false,
		DrainStatePaymentProcessed:    true,
		DrainStateRefunded:            true,
		DrainStateCanceled:            true,
		DrainStateUndeliverable:       true,
		DrainStateError:               true,
	} {
		drain := &Drain{State: state}
		assert.Equal(t, expected, drain.IsTerminal(), "state %s", state)
	}
}

func TestClient_ErrorMapping(t *testing.T) {
	client, _ := newTestClient(t, http.StatusConflict, `{"code": "duplicate_record", "message": "a liquidation address already exists for this destination"}`)

	_, err := client.CreateLiquidationAddress(context.Background(), "idem-dup", "customer-1", &CreateLiquidationAddressRequest{})
	require.Error(t, err)

	assert.True(t, IsConflict(err))
	assert.False(t, IsNotFound(err))

	var apiErr *Error
	require.ErrorAs(t, err, &apiErr)
	assert.Equal(t, http.StatusConflict, apiErr.StatusCode)
	assert.Equal(t, "duplicate_record", apiErr.Code)
	assert.Contains(t, apiErr.Error(), "duplicate_record")
}

func TestClient_ErrorMapping_NonJSONBody(t *testing.T) {
	client, _ := newTestClient(t, http.StatusInternalServerError, `upstream timeout`)

	_, err := client.GetCustomer(context.Background(), "customer-1")
	require.Error(t, err)

	var apiErr *Error
	require.ErrorAs(t, err, &apiErr)
	assert.Equal(t, http.StatusInternalServerError, apiErr.StatusCode)
	assert.Empty(t, apiErr.Code)
}
