package customers_test

import (
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/code-payments/flipcash2-server/cdp/customers"
)

const (
	testKeyID      = "organizations/test/apiKeys/test"
	testCustomerID = "customer_0ba20545-0587-44a4-a43e-725937f5f15c"
	testIdemKey    = "8e03978e-40d5-43e8-bc93-6894a57f9324"
)

func testPrivateKey() ed25519.PrivateKey {
	seed := sha256.Sum256([]byte("customers client test key"))
	return ed25519.NewKeyFromSeed(seed[:])
}

type recorded struct {
	method string
	path   string
	header http.Header
	body   string
}

// recorder is a server that records each request and answers with a canned
// status and body.
type recorder struct {
	*httptest.Server
	mu       sync.Mutex
	requests []recorded
	status   int
	body     string
}

func newRecorder(t *testing.T, status int, body string) *recorder {
	r := &recorder{status: status, body: body}
	r.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		data, _ := io.ReadAll(req.Body)
		r.mu.Lock()
		r.requests = append(r.requests, recorded{req.Method, req.URL.Path, req.Header.Clone(), string(data)})
		status, body := r.status, r.body
		r.mu.Unlock()
		w.WriteHeader(status)
		_, _ = io.WriteString(w, body)
	}))
	t.Cleanup(r.Close)
	return r
}

func (r *recorder) client(t *testing.T) *customers.Client {
	c, err := customers.NewClient(r.Client(), r.URL+"/platform", testKeyID, testPrivateKey())
	require.NoError(t, err)
	return c
}

func (r *recorder) last(t *testing.T) recorded {
	r.mu.Lock()
	defer r.mu.Unlock()
	require.NotEmpty(t, r.requests)
	return r.requests[len(r.requests)-1]
}

func (r *recorder) count() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.requests)
}

const minimalCustomer = `{"customerId":"` + testCustomerID + `","type":"individual","createdAt":"2026-11-11T18:21:15.949Z","updatedAt":"2026-11-11T18:21:15.949Z"}`

func ptr[T any](v T) *T { return &v }

var acceptedAt = time.Date(2026, 11, 11, 18, 0, 0, 0, time.UTC)

func TestEncoding_Golden(t *testing.T) {
	for _, tc := range []struct {
		name   string
		create bool
		input  *customers.CustomerInput
		want   string
	}{
		{
			name:   "create with nothing",
			create: true,
			want:   `{"type":"individual"}`,
		},
		{
			name: "update keeps an explicit empty line2 and omits every unset field",
			input: &customers.CustomerInput{
				Individual: &customers.IndividualInput{
					FirstName:   "Jane",
					DateOfBirth: &customers.DateOfBirth{Day: "5", Month: "01", Year: "1990"},
					FullSsn:     "000-00-0000",
					Address: &customers.AddressInput{
						Line1:       "500 Main St",
						Line2:       ptr(""),
						City:        "Boston",
						State:       "MA",
						PostCode:    "02108",
						CountryCode: "US",
					},
				},
				Compliance: &customers.Compliance{RequesterIPAddress: "203.0.113.42"},
			},
			want: `{"individual":{"firstName":"Jane","dateOfBirth":{"day":"5","month":"01","year":"1990"},"address":{"line1":"500 Main St","line2":"","city":"Boston","state":"MA","postCode":"02108","countryCode":"US"},"fullSsn":"000-00-0000"},"compliance":{"requesterIpAddress":"203.0.113.42"}}`,
		},
		{
			name: "a nil line2 is left out",
			input: &customers.CustomerInput{
				Individual: &customers.IndividualInput{Address: &customers.AddressInput{City: "Boston"}},
			},
			want: `{"individual":{"address":{"city":"Boston"}}}`,
		},
		{
			name: "empty acceptance and attestation lists are never sent",
			input: &customers.CustomerInput{
				Capabilities:    map[customers.CapabilityName]customers.CapabilityRequest{customers.CapabilityTransferFiat: {Requested: true}},
				TosAcceptances:  []customers.TosAcceptance{},
				TaxAttestations: []customers.TaxAttestation{},
				Compliance:      &customers.Compliance{RequesterIPAddress: "2001:db8::1"},
			},
			want: `{"capabilities":{"transferFiat":{"requested":true}},"compliance":{"requesterIpAddress":"2001:db8::1"}}`,
		},
		{
			name:   "create with everything",
			create: true,
			input: &customers.CustomerInput{
				Capabilities: map[customers.CapabilityName]customers.CapabilityRequest{
					customers.CapabilityTransferFiat: {Requested: true},
					customers.CapabilityCustodyFiat:  {Requested: false},
				},
				Individual: &customers.IndividualInput{
					FirstName:        "Jane",
					LastName:         "Doe",
					Email:            "jane.doe@example.com",
					PhoneNumber:      "+15555550100",
					PurposeOfAccount: "payments_to_friends",
					SourceOfFunds:    "salary",
				},
				TosAcceptances: []customers.TosAcceptance{{VersionID: "us_individual_2026-05-29", Language: "en", AcceptedAt: acceptedAt}},
				TaxAttestations: []customers.TaxAttestation{{UsW9: &customers.UsW9Attestation{
					IsExemptBackupWithholding: false,
					EdeliveryConsent:          true,
					AcceptedAt:                acceptedAt.Add(1500 * time.Millisecond),
				}}},
				Compliance: &customers.Compliance{RequesterIPAddress: "203.0.113.42"},
			},
			want: `{"type":"individual","capabilities":{"custodyFiat":{"requested":false},"transferFiat":{"requested":true}},"individual":{"firstName":"Jane","lastName":"Doe","email":"jane.doe@example.com","phoneNumber":"+15555550100","purposeOfAccount":"payments_to_friends","sourceOfFunds":"salary"},"tosAcceptances":[{"versionId":"us_individual_2026-05-29","language":"en","acceptedAt":"2026-11-11T18:00:00Z"}],"taxAttestations":[{"form":"us_w9","isExemptBackupWithholding":false,"edeliveryConsent":true,"acceptedAt":"2026-11-11T18:00:01.5Z"}],"compliance":{"requesterIpAddress":"203.0.113.42"}}`,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			status := http.StatusOK
			if tc.create {
				status = http.StatusCreated
			}
			rec := newRecorder(t, status, minimalCustomer)
			client := rec.client(t)

			var err error
			if tc.create {
				_, err = client.Create(context.Background(), testIdemKey, tc.input)
			} else {
				_, err = client.Update(context.Background(), testCustomerID, "", tc.input)
			}
			require.NoError(t, err)
			assert.Equal(t, tc.want, rec.last(t).body)
		})
	}
}

func TestRequests_HeadersAndSigning(t *testing.T) {
	rec := newRecorder(t, http.StatusCreated, minimalCustomer)
	client := rec.client(t)
	host := strings.TrimPrefix(rec.URL, "http://")

	_, err := client.Create(context.Background(), testIdemKey, nil)
	require.NoError(t, err)
	r := rec.last(t)
	assert.Equal(t, "POST", r.method)
	assert.Equal(t, "/platform/v2/customers", r.path)
	assert.Equal(t, testIdemKey, r.header.Get("X-Idempotency-Key"))
	assert.Equal(t, "application/json", r.header.Get("Content-Type"))
	assert.Equal(t, "application/json", r.header.Get("Accept"))
	assertSigned(t, r, "POST "+host+"/platform/v2/customers")

	rec.status = http.StatusOK
	_, err = client.Get(context.Background(), testCustomerID)
	require.NoError(t, err)
	r = rec.last(t)
	assert.Equal(t, "GET", r.method)
	assert.Equal(t, "/platform/v2/customers/"+testCustomerID, r.path)
	assert.Empty(t, r.body)
	assert.Empty(t, r.header.Get("Content-Type"))
	assert.Empty(t, r.header.Get("X-Idempotency-Key"))
	assertSigned(t, r, "GET "+host+"/platform/v2/customers/"+testCustomerID)

	_, err = client.Update(context.Background(), testCustomerID, testIdemKey, &customers.CustomerInput{})
	require.NoError(t, err)
	r = rec.last(t)
	assert.Equal(t, "PUT", r.method)
	assert.Equal(t, testIdemKey, r.header.Get("X-Idempotency-Key"))
	assert.Equal(t, "{}", r.body)
	assertSigned(t, r, "PUT "+host+"/platform/v2/customers/"+testCustomerID)

	rec.status, rec.body = http.StatusNoContent, ""
	require.NoError(t, client.Delete(context.Background(), testCustomerID))
	r = rec.last(t)
	assert.Equal(t, "DELETE", r.method)
	assertSigned(t, r, "DELETE "+host+"/platform/v2/customers/"+testCustomerID)
}

func assertSigned(t *testing.T, r recorded, wantURI string) {
	t.Helper()
	token, ok := strings.CutPrefix(r.header.Get("Authorization"), "Bearer ")
	require.True(t, ok)
	parts := strings.Split(token, ".")
	require.Len(t, parts, 3)
	sig, err := base64.RawURLEncoding.DecodeString(parts[2])
	require.NoError(t, err)
	assert.True(t, ed25519.Verify(testPrivateKey().Public().(ed25519.PublicKey), []byte(parts[0]+"."+parts[1]), sig))
	payload, err := base64.RawURLEncoding.DecodeString(parts[1])
	require.NoError(t, err)
	var claims struct {
		URI string `json:"uri"`
		Sub string `json:"sub"`
	}
	require.NoError(t, json.Unmarshal(payload, &claims))
	assert.Equal(t, wantURI, claims.URI)
	assert.Equal(t, testKeyID, claims.Sub)
}

func TestDecoding_RealShapeWithUnknownValues(t *testing.T) {
	// The shape the Sandbox returned on 2026-11-11, plus values Coinbase may
	// add later: a capability, a capability status, a requirement key and
	// a requirement status this client does not know, and deadlines.
	body := `{
	  "capabilities": {
	    "custodyFiat": {"requested": true, "status": "pending"},
	    "transferFiat": {"requested": true, "status": "suspended"},
	    "holdGold": {"requested": false, "status": "unrequested"}
	  },
	  "createdAt": "2026-11-11T18:21:15.949Z",
	  "customerId": "` + testCustomerID + `",
	  "individual": {"email": "jane.doe@example.com", "firstName": "Jane", "lastName": "Doe"},
	  "projectIds": [],
	  "requirements": {
	    "fullSsn": {"impact": ["custodyFiat", "transferFiat"], "status": "rejected"},
	    "livenessCheck": {"impact": ["holdGold"], "status": "escalated"},
	    "taxAttestation": {"impact": ["custodyFiat"], "status": "due", "taxForms": ["us_w9", "us_w8ben"]},
	    "tos": {
	      "deadline": "2026-12-01T00:00:00Z",
	      "impact": ["custodyFiat", "transferFiat"],
	      "status": "past_due",
	      "tosVersions": [{"languages": ["en", "es"], "url": "https://www.coinbase.com/legal/user-agreements-third-party-provider", "versionId": "us_individual_2026-05-29", "deadline": "2026-12-01T00:00:00Z"}]
	    }
	  },
	  "someNewField": {"nested": true},
	  "type": "individual",
	  "updatedAt": "2026-11-11T18:21:39.556Z"
	}`
	rec := newRecorder(t, http.StatusOK, body)
	c, err := rec.client(t).Get(context.Background(), testCustomerID)
	require.NoError(t, err)

	deadline := time.Date(2026, 12, 1, 0, 0, 0, 0, time.UTC)
	assert.Equal(t, testCustomerID, c.ID)
	assert.Equal(t, customers.CustomerTypeIndividual, c.Type)
	assert.Equal(t, &customers.Individual{FirstName: "Jane", LastName: "Doe", Email: "jane.doe@example.com"}, c.Individual)
	assert.Equal(t, time.Date(2026, 11, 11, 18, 21, 15, 949_000_000, time.UTC), c.CreatedAt)
	assert.Equal(t, map[customers.CapabilityName]customers.Capability{
		customers.CapabilityCustodyFiat:  {Requested: true, Status: customers.CapabilityPending},
		customers.CapabilityTransferFiat: {Requested: true, Status: "suspended"},
		"holdGold":                       {Requested: false, Status: customers.CapabilityUnrequested},
	}, c.Capabilities)
	assert.Equal(t, map[customers.RequirementKey]customers.Requirement{
		customers.RequirementFullSsn: {Status: customers.RequirementRejected, Impact: []customers.CapabilityName{"custodyFiat", "transferFiat"}},
		"livenessCheck":              {Status: "escalated", Impact: []customers.CapabilityName{"holdGold"}},
		customers.RequirementTaxAttestation: {
			Status:   customers.RequirementDue,
			Impact:   []customers.CapabilityName{"custodyFiat"},
			TaxForms: []customers.TaxForm{customers.TaxFormUsW9, "us_w8ben"},
		},
		customers.RequirementTos: {
			Status:   customers.RequirementPastDue,
			Deadline: &deadline,
			Impact:   []customers.CapabilityName{"custodyFiat", "transferFiat"},
			TosVersions: []customers.TermsOfService{{
				VersionID: "us_individual_2026-05-29",
				Languages: []string{"en", "es"},
				URL:       "https://www.coinbase.com/legal/user-agreements-third-party-provider",
				Deadline:  &deadline,
			}},
		},
	}, c.Requirements)
}

func TestDecoding_NothingRequested(t *testing.T) {
	rec := newRecorder(t, http.StatusOK, minimalCustomer)
	c, err := rec.client(t).Get(context.Background(), testCustomerID)
	require.NoError(t, err)
	assert.Nil(t, c.Individual)
	assert.Nil(t, c.Capabilities)
	assert.Nil(t, c.Requirements)
}

func TestErrors_Mapping(t *testing.T) {
	const sensitive = "000-00-0001"
	for _, tc := range []struct {
		name            string
		status          int
		body            string
		wantType        customers.ErrorType
		notFound        bool
		invalid         bool
		tosLanguage     bool
		rateLimited     bool
		client          bool
		retryable       bool
		wantCorrelation string
	}{
		{
			name:            "invalid request",
			status:          400,
			body:            `{"errorType":"invalid_request","errorMessage":"individual.fullSsn ` + sensitive + ` is wrong","correlationId":"a58f-IAD","errorLink":"https://docs.cdp.coinbase.com/api-reference/v2/errors#invalid-request"}`,
			wantType:        customers.ErrorTypeInvalidRequest,
			invalid:         true,
			client:          true,
			wantCorrelation: "a58f-IAD",
		},
		{
			name:        "unsupported tos language",
			status:      400,
			body:        `{"errorType":"unsupported_tos_language","errorMessage":"version \"us_individual_2026-05-29\" is not published in language \"fr\"","correlationId":"c"}`,
			wantType:    customers.ErrorTypeUnsupportedTosLanguage,
			tosLanguage: true,
			client:      true,
		},
		{
			name:     "not found",
			status:   404,
			body:     `{"errorType":"not_found","errorMessage":"Customer not found.","correlationId":"c"}`,
			wantType: customers.ErrorTypeNotFound,
			notFound: true,
			client:   true,
		},
		{
			name:     "idempotency conflict",
			status:   422,
			body:     `{"errorType":"idempotency_error","errorMessage":"Idempotency key was already used with a different request payload.","correlationId":"c"}`,
			wantType: customers.ErrorTypeIdempotency,
			client:   true,
		},
		{
			name:     "unauthorized",
			status:   401,
			body:     `{"errorType":"unauthorized","errorMessage":"Unauthorized.","correlationId":"c"}`,
			wantType: customers.ErrorTypeUnauthorized,
			client:   true,
		},
		{
			name:        "rate limited",
			status:      429,
			body:        `{"errorType":"rate_limit_exceeded","errorMessage":"Rate limit exceeded.","correlationId":"c"}`,
			wantType:    customers.ErrorTypeRateLimitExceeded,
			rateLimited: true,
			retryable:   true,
		},
		{
			name:      "internal",
			status:    500,
			body:      `{"errorType":"internal_server_error","errorMessage":"An internal server error occurred.","correlationId":"c"}`,
			wantType:  "internal_server_error",
			retryable: true,
		},
		{
			name:      "proxy page",
			status:    502,
			body:      `<html><body>502 Bad Gateway</body></html>`,
			retryable: true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rec := newRecorder(t, tc.status, tc.body)
			_, err := rec.client(t).Get(context.Background(), testCustomerID)
			require.Error(t, err)

			e, ok := customers.AsError(err)
			require.True(t, ok)
			assert.Equal(t, tc.status, e.StatusCode)
			assert.Equal(t, tc.wantType, e.Type)
			if tc.wantCorrelation != "" {
				assert.Equal(t, tc.wantCorrelation, e.CorrelationID)
				assert.Contains(t, err.Error(), tc.wantCorrelation)
			}

			assert.Equal(t, tc.notFound, customers.IsNotFound(err))
			assert.Equal(t, tc.invalid, customers.IsInvalidRequest(err))
			assert.Equal(t, tc.tosLanguage, customers.IsUnsupportedTosLanguage(err))
			assert.Equal(t, tc.rateLimited, customers.IsRateLimited(err))
			assert.Equal(t, tc.client, customers.IsClientError(err))
			assert.Equal(t, tc.retryable, customers.IsRetryable(err))

			// Coinbase's message can echo what was submitted: it is a field,
			// never part of the error string.
			if e.Message != "" {
				assert.NotContains(t, err.Error(), e.Message)
			}
			assert.NotContains(t, err.Error(), sensitive)
		})
	}
}

func TestErrors_MessageKeptForCaller(t *testing.T) {
	rec := newRecorder(t, 400, `{"errorType":"invalid_request","errorMessage":"individual.address.countryCode: value is not one of the allowed values [\"US\"]"}`)
	_, err := rec.client(t).Get(context.Background(), testCustomerID)
	e, ok := customers.AsError(err)
	require.True(t, ok)
	assert.Equal(t, `individual.address.countryCode: value is not one of the allowed values ["US"]`, e.Message)
	assert.Equal(t, "cdp customers: http 400 invalid_request", err.Error())
}

func TestErrors_MalformedSuccess(t *testing.T) {
	rec := newRecorder(t, http.StatusOK, `{"customerId": "Jane Doe 000-00-0000`)
	_, err := rec.client(t).Get(context.Background(), testCustomerID)
	require.ErrorIs(t, err, customers.ErrMalformedResponse)
	assert.True(t, customers.IsRetryable(err))
	assert.False(t, customers.IsClientError(err))
	assert.NotContains(t, err.Error(), "Jane")
	assert.NotContains(t, err.Error(), "000-00-0000")
}

func TestErrors_Transport(t *testing.T) {
	rec := newRecorder(t, http.StatusOK, minimalCustomer)
	client := rec.client(t)
	rec.Close()

	_, err := client.Get(context.Background(), testCustomerID)
	require.Error(t, err)
	_, isCoinbase := customers.AsError(err)
	assert.False(t, isCoinbase)
	assert.True(t, customers.IsRetryable(err))
	assert.False(t, customers.IsClientError(err))
}

func TestErrors_CallerCanceled(t *testing.T) {
	rec := newRecorder(t, http.StatusOK, minimalCustomer)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := rec.client(t).Get(ctx, testCustomerID)
	require.ErrorIs(t, err, context.Canceled)
	assert.False(t, customers.IsRetryable(err))
}

func TestErrors_RefusedBeforeSending(t *testing.T) {
	rec := newRecorder(t, http.StatusOK, minimalCustomer)
	client := rec.client(t)
	ctx := context.Background()

	for name, call := range map[string]func() error{
		"get malformed id": func() error { _, err := client.Get(ctx, "customer_../../v2/other"); return err },
		"delete empty id":  func() error { return client.Delete(ctx, "") },
		"create no key":    func() error { _, err := client.Create(ctx, "", nil); return err },
		"create not uuid":  func() error { _, err := client.Create(ctx, "not-a-uuid", nil); return err },
		"create uuid v1": func() error {
			_, err := client.Create(ctx, "6ba7b810-9dad-11d1-80b4-00c04fd430c8", nil)
			return err
		},
		"create uppercase uuid": func() error {
			_, err := client.Create(ctx, strings.ToUpper(testIdemKey), nil)
			return err
		},
		"update bad key": func() error {
			_, err := client.Update(ctx, testCustomerID, "nope", &customers.CustomerInput{})
			return err
		},
		"update nil": func() error { _, err := client.Update(ctx, testCustomerID, "", nil); return err },
		"attestation with no form": func() error {
			_, err := client.Update(ctx, testCustomerID, "", &customers.CustomerInput{TaxAttestations: []customers.TaxAttestation{{}}})
			return err
		},
	} {
		t.Run(name, func(t *testing.T) {
			err := call()
			require.ErrorIs(t, err, customers.ErrInvalidArgument)
			assert.True(t, customers.IsClientError(err))
			assert.False(t, customers.IsRetryable(err))
		})
	}
	assert.Zero(t, rec.count(), "nothing refused may reach Coinbase")
}

func TestNewClient_Validation(t *testing.T) {
	key := testPrivateKey()
	for name, baseURL := range map[string]string{
		"relative":       "/platform",
		"trailing slash": "https://sandbox.cdp.coinbase.com/platform/",
		"query":          "https://sandbox.cdp.coinbase.com/platform?x=1",
		"fragment":       "https://sandbox.cdp.coinbase.com/platform#x",
		"scheme":         "ftp://sandbox.cdp.coinbase.com/platform",
		"userinfo":       "https://u:p@sandbox.cdp.coinbase.com/platform",
	} {
		t.Run(name, func(t *testing.T) {
			_, err := customers.NewClient(http.DefaultClient, baseURL, testKeyID, key)
			assert.ErrorIs(t, err, customers.ErrInvalidArgument)
		})
	}

	_, err := customers.NewClient(nil, customers.SandboxBaseURL, testKeyID, key)
	assert.ErrorIs(t, err, customers.ErrInvalidArgument)
	_, err = customers.NewClient(http.DefaultClient, customers.SandboxBaseURL, "", key)
	assert.ErrorIs(t, err, customers.ErrInvalidArgument)
	_, err = customers.NewClient(http.DefaultClient, customers.SandboxBaseURL, testKeyID, key[:32])
	assert.ErrorIs(t, err, customers.ErrInvalidArgument)

	for _, baseURL := range []string{customers.SandboxBaseURL, customers.ProductionBaseURL} {
		_, err := customers.NewClient(http.DefaultClient, baseURL, testKeyID, key)
		assert.NoError(t, err)
	}
}

func TestIsCustomerID(t *testing.T) {
	assert.True(t, customers.IsCustomerID(testCustomerID))
	for _, id := range []string{
		"",
		"customer_",
		"0ba20545-0587-44a4-a43e-725937f5f15c",
		strings.ToUpper(testCustomerID),
		"customer_0BA20545-0587-44A4-A43E-725937F5F15C",
		testCustomerID + "0",
		testCustomerID[:len(testCustomerID)-1],
		"customer_../../v2/other",
		" " + testCustomerID,
	} {
		assert.False(t, customers.IsCustomerID(id), "%q", id)
	}
}
