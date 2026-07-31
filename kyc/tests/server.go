package tests

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"
	emailpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/email/v1"
	kycpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/kyc/v1"
	phonepb "github.com/code-payments/flipcash2-protobuf-api/generated/go/phone/v1"
	thirdpartypb "github.com/code-payments/flipcash2-protobuf-api/generated/go/thirdparty/v1"

	"github.com/code-payments/flipcash2-server/account"
	"github.com/code-payments/flipcash2-server/auth"
	"github.com/code-payments/flipcash2-server/kyc"
	"github.com/code-payments/flipcash2-server/model"
	"github.com/code-payments/flipcash2-server/offramp/bridge"
	"github.com/code-payments/flipcash2-server/testutil"
)

const testTOSLinkURL = "https://dashboard.bridge.xyz/accept-terms-of-service?session_token=test"

// fakeBridge is a minimal Bridge API backend for server tests. It records the
// idempotency keys of TOS link creations and serves seeded customers.
type fakeBridge struct {
	sync.Mutex

	tosLinkIdempotencyKeys []string
	customers              map[string]*bridge.Customer

	// createdCustomer is returned by customer creation when set; a nil value
	// fails the request with createError.
	createdCustomer          *bridge.Customer
	createError              *bridge.Error
	createCalls              int
	lastCreateBody           map[string]any
	lastCreateIdempotencyKey string

	// updatedCustomer is returned by customer updates when set; a nil value
	// fails the request with updateError.
	updatedCustomer      *bridge.Customer
	updateError          *bridge.Error
	updateCalls          int
	lastUpdateBody       map[string]any
	lastUpdateCustomerID string
}

func (f *fakeBridge) setCustomer(customer *bridge.Customer) {
	f.Lock()
	defer f.Unlock()

	if f.customers == nil {
		f.customers = make(map[string]*bridge.Customer)
	}
	f.customers[customer.ID] = customer
}

func (f *fakeBridge) handler(t *testing.T) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v0/customers/tos_links", func(w http.ResponseWriter, r *http.Request) {
		require.NotEmpty(t, r.Header.Get("Api-Key"))

		key := r.Header.Get("Idempotency-Key")
		require.NotEmpty(t, key)
		f.Lock()
		f.tosLinkIdempotencyKeys = append(f.tosLinkIdempotencyKeys, key)
		f.Unlock()

		require.NoError(t, json.NewEncoder(w).Encode(bridge.TOSLink{URL: testTOSLinkURL}))
	})
	mux.HandleFunc("POST /v0/customers", func(w http.ResponseWriter, r *http.Request) {
		require.NotEmpty(t, r.Header.Get("Api-Key"))

		f.Lock()
		defer f.Unlock()

		f.createCalls++
		f.lastCreateIdempotencyKey = r.Header.Get("Idempotency-Key")
		require.NotEmpty(t, f.lastCreateIdempotencyKey)

		f.lastCreateBody = nil
		require.NoError(t, json.NewDecoder(r.Body).Decode(&f.lastCreateBody))

		if f.createdCustomer == nil {
			apiErr := f.createError
			if apiErr == nil {
				apiErr = &bridge.Error{StatusCode: http.StatusInternalServerError, Message: "no customer seeded"}
			}
			w.WriteHeader(apiErr.StatusCode)
			require.NoError(t, json.NewEncoder(w).Encode(map[string]string{"code": apiErr.Code, "message": apiErr.Message}))
			return
		}

		if f.customers == nil {
			f.customers = make(map[string]*bridge.Customer)
		}
		f.customers[f.createdCustomer.ID] = f.createdCustomer
		require.NoError(t, json.NewEncoder(w).Encode(f.createdCustomer))
	})
	mux.HandleFunc("PUT /v0/customers/{id}", func(w http.ResponseWriter, r *http.Request) {
		require.NotEmpty(t, r.Header.Get("Api-Key"))

		f.Lock()
		defer f.Unlock()

		f.updateCalls++
		f.lastUpdateCustomerID = r.PathValue("id")
		// Bridge honors idempotency keys on POST only; PUTs must not carry one.
		require.Empty(t, r.Header.Get("Idempotency-Key"))

		f.lastUpdateBody = nil
		require.NoError(t, json.NewDecoder(r.Body).Decode(&f.lastUpdateBody))

		if f.updatedCustomer == nil {
			apiErr := f.updateError
			if apiErr == nil {
				apiErr = &bridge.Error{StatusCode: http.StatusInternalServerError, Message: "no customer seeded"}
			}
			w.WriteHeader(apiErr.StatusCode)
			require.NoError(t, json.NewEncoder(w).Encode(map[string]string{"code": apiErr.Code, "message": apiErr.Message}))
			return
		}

		if f.customers == nil {
			f.customers = make(map[string]*bridge.Customer)
		}
		f.customers[f.updatedCustomer.ID] = f.updatedCustomer
		require.NoError(t, json.NewEncoder(w).Encode(f.updatedCustomer))
	})
	mux.HandleFunc("GET /v0/customers/{id}", func(w http.ResponseWriter, r *http.Request) {
		require.NotEmpty(t, r.Header.Get("Api-Key"))

		f.Lock()
		customer, ok := f.customers[r.PathValue("id")]
		f.Unlock()
		if !ok {
			w.WriteHeader(http.StatusNotFound)
			require.NoError(t, json.NewEncoder(w).Encode(map[string]string{"code": "not_found", "message": "customer not found"}))
			return
		}
		require.NoError(t, json.NewEncoder(w).Encode(customer))
	})
	return mux
}

// newRegisteredUser binds a fresh keypair to a new registered user.
func newRegisteredUser(t *testing.T, ctx context.Context, accounts account.Store) (*commonpb.UserId, model.KeyPair) {
	userID := model.MustGenerateUserID()
	keyPair := model.MustGenerateKeyPair()
	_, err := accounts.Bind(ctx, userID, keyPair.Proto())
	require.NoError(t, err)
	require.NoError(t, accounts.SetRegistrationFlag(ctx, userID, true))
	return userID, keyPair
}

func RunServerTests(t *testing.T, accounts account.Store, store kyc.Store, teardown func()) {
	for _, tf := range []func(t *testing.T, accounts account.Store, store kyc.Store){
		testServer_GetAgreementLinks,
		testServer_SubmitKyc,
		testServer_GetKycStatus,
		testServer_UpdateKyc,
	} {
		tf(t, accounts, store)
		teardown()
	}
}

func testServer_GetAgreementLinks(t *testing.T, accounts account.Store, store kyc.Store) {
	ctx := context.Background()
	log := zaptest.NewLogger(t)

	fake := &fakeBridge{}
	backend := httptest.NewServer(fake.handler(t))
	defer backend.Close()
	bridgeClient := bridge.NewClient("test-api-key", bridge.WithBaseURL(backend.URL))

	authz := account.NewAuthorizer(log, accounts, auth.NewKeyPairAuthenticator(log))

	serv := kyc.NewServer(log, authz, accounts, store, bridgeClient)
	cc := testutil.RunGRPCServer(t, log, testutil.WithService(func(s *grpc.Server) {
		kycpb.RegisterKycServiceServer(s, serv)
	}))

	client := kycpb.NewKycServiceClient(cc)

	t.Run("No user", func(t *testing.T) {
		keyPair := model.MustGenerateKeyPair()
		req := &kycpb.GetAgreementLinksRequest{
			Partner:        thirdpartypb.Partner_BRIDGE,
			SubmissionType: kycpb.SubmissionType_INDIVIDUAL,
		}
		require.NoError(t, keyPair.Auth(req, &req.Auth))
		_, err := client.GetAgreementLinks(ctx, req)
		require.Equal(t, codes.PermissionDenied, status.Code(err))
	})

	t.Run("Unregistered user", func(t *testing.T) {
		userID := model.MustGenerateUserID()
		keyPair := model.MustGenerateKeyPair()
		_, err := accounts.Bind(ctx, userID, keyPair.Proto())
		require.NoError(t, err)

		req := &kycpb.GetAgreementLinksRequest{
			Partner:        thirdpartypb.Partner_BRIDGE,
			SubmissionType: kycpb.SubmissionType_INDIVIDUAL,
		}
		require.NoError(t, keyPair.Auth(req, &req.Auth))
		resp, err := client.GetAgreementLinks(ctx, req)
		require.NoError(t, err)
		require.Equal(t, kycpb.GetAgreementLinksResponse_DENIED, resp.Result)
		require.Empty(t, resp.Links)
	})

	t.Run("Registered user", func(t *testing.T) {
		userID := model.MustGenerateUserID()
		keyPair := model.MustGenerateKeyPair()
		_, err := accounts.Bind(ctx, userID, keyPair.Proto())
		require.NoError(t, err)
		require.NoError(t, accounts.SetRegistrationFlag(ctx, userID, true))

		req := &kycpb.GetAgreementLinksRequest{
			Partner:        thirdpartypb.Partner_BRIDGE,
			SubmissionType: kycpb.SubmissionType_INDIVIDUAL,
		}
		require.NoError(t, keyPair.Auth(req, &req.Auth))
		resp, err := client.GetAgreementLinks(ctx, req)
		require.NoError(t, err)
		require.Equal(t, kycpb.GetAgreementLinksResponse_OK, resp.Result)

		require.Len(t, resp.Links, 1)
		require.Equal(t, kycpb.AgreementLink_TOS, resp.Links[0].Type)
		require.Equal(t, testTOSLinkURL, resp.Links[0].Url)
	})

	t.Run("Unsupported partner", func(t *testing.T) {
		userID := model.MustGenerateUserID()
		keyPair := model.MustGenerateKeyPair()
		_, err := accounts.Bind(ctx, userID, keyPair.Proto())
		require.NoError(t, err)
		require.NoError(t, accounts.SetRegistrationFlag(ctx, userID, true))

		req := &kycpb.GetAgreementLinksRequest{
			Partner:        thirdpartypb.Partner_COINBASE,
			SubmissionType: kycpb.SubmissionType_INDIVIDUAL,
		}
		require.NoError(t, keyPair.Auth(req, &req.Auth))
		_, err = client.GetAgreementLinks(ctx, req)
		require.Equal(t, codes.InvalidArgument, status.Code(err))
	})

	t.Run("Unsupported submission type", func(t *testing.T) {
		userID := model.MustGenerateUserID()
		keyPair := model.MustGenerateKeyPair()
		_, err := accounts.Bind(ctx, userID, keyPair.Proto())
		require.NoError(t, err)
		require.NoError(t, accounts.SetRegistrationFlag(ctx, userID, true))

		req := &kycpb.GetAgreementLinksRequest{
			Partner: thirdpartypb.Partner_BRIDGE,
		}
		require.NoError(t, keyPair.Auth(req, &req.Auth))
		_, err = client.GetAgreementLinks(ctx, req)
		require.Equal(t, codes.InvalidArgument, status.Code(err))
	})
}

func testServer_GetKycStatus(t *testing.T, accounts account.Store, store kyc.Store) {
	ctx := context.Background()
	log := zaptest.NewLogger(t)

	fake := &fakeBridge{}
	backend := httptest.NewServer(fake.handler(t))
	defer backend.Close()
	bridgeClient := bridge.NewClient("test-api-key", bridge.WithBaseURL(backend.URL))

	authz := account.NewAuthorizer(log, accounts, auth.NewKeyPairAuthenticator(log))

	serv := kyc.NewServer(log, authz, accounts, store, bridgeClient)
	cc := testutil.RunGRPCServer(t, log, testutil.WithService(func(s *grpc.Server) {
		kycpb.RegisterKycServiceServer(s, serv)
	}))

	client := kycpb.NewKycServiceClient(cc)

	statusReq := func(t *testing.T, keyPair model.KeyPair) *kycpb.GetKycStatusRequest {
		req := &kycpb.GetKycStatusRequest{
			Partner:        thirdpartypb.Partner_BRIDGE,
			SubmissionType: kycpb.SubmissionType_INDIVIDUAL,
		}
		require.NoError(t, keyPair.Auth(req, &req.Auth))
		return req
	}

	t.Run("Unregistered user", func(t *testing.T) {
		userID := model.MustGenerateUserID()
		keyPair := model.MustGenerateKeyPair()
		_, err := accounts.Bind(ctx, userID, keyPair.Proto())
		require.NoError(t, err)

		resp, err := client.GetKycStatus(ctx, statusReq(t, keyPair))
		require.NoError(t, err)
		require.Equal(t, kycpb.GetKycStatusResponse_DENIED, resp.Result)
		require.Nil(t, resp.KycState)
	})

	t.Run("Not started", func(t *testing.T) {
		_, keyPair := newRegisteredUser(t, ctx, accounts)

		resp, err := client.GetKycStatus(ctx, statusReq(t, keyPair))
		require.NoError(t, err)
		require.Equal(t, kycpb.GetKycStatusResponse_NOT_STARTED, resp.Result)
		require.Nil(t, resp.KycState)
	})

	t.Run("Verified", func(t *testing.T) {
		userID, keyPair := newRegisteredUser(t, ctx, accounts)

		fake.setCustomer(&bridge.Customer{
			ID:                        "cust_verified",
			Status:                    bridge.CustomerStatusActive,
			HasAcceptedTermsOfService: true,
			Endorsements: []bridge.Endorsement{{
				Name:   bridge.EndorsementBase,
				Status: bridge.EndorsementStatusApproved,
			}},
		})
		require.NoError(t, store.Create(ctx, &kyc.Record{
			UserID:     userID,
			Partner:    thirdpartypb.Partner_BRIDGE,
			CustomerID: "cust_verified",
			CreatedAt:  time.Now().UTC(),
		}))

		resp, err := client.GetKycStatus(ctx, statusReq(t, keyPair))
		require.NoError(t, err)
		require.Equal(t, kycpb.GetKycStatusResponse_OK, resp.Result)

		state := resp.KycState
		require.NotNil(t, state)
		require.Equal(t, kycpb.KycStatus_ACTIVE, state.Status)
		require.Equal(t, kycpb.KycState_NONE, state.NextStep)
		require.Empty(t, state.Requirements)
		require.Equal(t, thirdpartypb.Partner_BRIDGE, state.Partner)
		require.Equal(t, kycpb.SubmissionType_INDIVIDUAL, state.SubmissionType)
		require.True(t, state.GetBridgeFeatures().CanOfframp)
	})

	t.Run("Action needed", func(t *testing.T) {
		userID, keyPair := newRegisteredUser(t, ctx, accounts)

		fake.setCustomer(&bridge.Customer{
			ID:                        "cust_incomplete",
			Status:                    bridge.CustomerStatusIncomplete,
			HasAcceptedTermsOfService: true,
			Endorsements: []bridge.Endorsement{{
				Name:   bridge.EndorsementBase,
				Status: bridge.EndorsementStatusIncomplete,
				Requirements: &bridge.EndorsementRequirements{
					Missing: json.RawMessage(`{"all_of": ["tax_identification_number"]}`),
					Issues:  json.RawMessage(`["government_id_verification_failed"]`),
				},
			}},
		})
		require.NoError(t, store.Create(ctx, &kyc.Record{
			UserID:     userID,
			Partner:    thirdpartypb.Partner_BRIDGE,
			CustomerID: "cust_incomplete",
			CreatedAt:  time.Now().UTC(),
		}))

		resp, err := client.GetKycStatus(ctx, statusReq(t, keyPair))
		require.NoError(t, err)
		require.Equal(t, kycpb.GetKycStatusResponse_OK, resp.Result)

		state := resp.KycState
		require.NotNil(t, state)
		require.Equal(t, kycpb.KycStatus_INCOMPLETE, state.Status)
		require.Equal(t, kycpb.KycState_PROVIDE_REQUIREMENTS, state.NextStep)
		require.False(t, state.GetBridgeFeatures().CanOfframp)
		require.Len(t, state.Requirements, 2)
	})
}

func testServer_SubmitKyc(t *testing.T, accounts account.Store, store kyc.Store) {
	ctx := context.Background()
	log := zaptest.NewLogger(t)

	fake := &fakeBridge{}
	backend := httptest.NewServer(fake.handler(t))
	defer backend.Close()
	bridgeClient := bridge.NewClient("test-api-key", bridge.WithBaseURL(backend.URL))

	authz := account.NewAuthorizer(log, accounts, auth.NewKeyPairAuthenticator(log))

	serv := kyc.NewServer(log, authz, accounts, store, bridgeClient)
	cc := testutil.RunGRPCServer(t, log, testutil.WithService(func(s *grpc.Server) {
		kycpb.RegisterKycServiceServer(s, serv)
	}))

	client := kycpb.NewKycServiceClient(cc)

	validSubmission := func() *kycpb.IndividualKycSubmission {
		return &kycpb.IndividualKycSubmission{
			Name:      &kycpb.LegalName{FirstName: "Jane", LastName: "Doe"},
			BirthDate: &commonpb.Date{Year: 1990, Month: 4, Day: 2},
			Address: &kycpb.Address{
				StreetLine_1: "123 Main St",
				City:         "San Francisco",
				Subdivision:  "CA",
				PostalCode:   "94105",
				Country:      &commonpb.CountryCode{Value: "US"},
			},
			TaxId: &kycpb.TaxId{Type: kycpb.TaxId_US_SSN, Value: "123456789"},
			IdentityDocuments: []*kycpb.IdentityDocument{{
				Type:           kycpb.IdentityDocument_DRIVERS_LICENSE,
				FrontImage:     []byte("front-image-bytes"),
				BackImage:      []byte("back-image-bytes"),
				MimeType:       "image/jpeg",
				IssuingCountry: &commonpb.CountryCode{Value: "US"},
			}},
			Email:                 &emailpb.EmailAddress{Value: "jane@example.com"},
			Phone:                 &phonepb.PhoneNumber{Value: "+12025550123"},
			SignedAgreementTokens: []*kycpb.SignedAgreementToken{{Value: "signed-agreement-id"}},
			Compliance: &kycpb.IndividualKycSubmission_BridgeCompliance{
				BridgeCompliance: &kycpb.BridgeComplianceProfile{
					AccountPurpose:        kycpb.BridgeComplianceProfile_PERSONAL_OR_LIVING_EXPENSES,
					SourceOfFunds:         kycpb.BridgeComplianceProfile_SALARY,
					EmploymentStatus:      kycpb.BridgeComplianceProfile_EMPLOYED,
					ExpectedMonthlyVolume: kycpb.BridgeComplianceProfile_UNDER_5K_USD,
					Occupation:            "1234",
				},
			},
		}
	}

	submit := func(t *testing.T, keyPair model.KeyPair, submission *kycpb.IndividualKycSubmission) *kycpb.SubmitKycResponse {
		req := &kycpb.SubmitKycRequest{
			Partner:    thirdpartypb.Partner_BRIDGE,
			Submission: &kycpb.SubmitKycRequest_Individual{Individual: submission},
		}
		require.NoError(t, keyPair.Auth(req, &req.Auth))
		resp, err := client.SubmitKyc(ctx, req)
		require.NoError(t, err)
		return resp
	}

	underReviewCustomer := func(id string) *bridge.Customer {
		return &bridge.Customer{
			ID:                        id,
			Status:                    bridge.CustomerStatusUnderReview,
			HasAcceptedTermsOfService: true,
			Endorsements: []bridge.Endorsement{{
				Name:   bridge.EndorsementBase,
				Status: bridge.EndorsementStatusIncomplete,
				Requirements: &bridge.EndorsementRequirements{
					Pending: []string{"id_verification"},
				},
			}},
		}
	}

	t.Run("Unregistered user", func(t *testing.T) {
		userID := model.MustGenerateUserID()
		keyPair := model.MustGenerateKeyPair()
		_, err := accounts.Bind(ctx, userID, keyPair.Proto())
		require.NoError(t, err)

		resp := submit(t, keyPair, validSubmission())
		require.Equal(t, kycpb.SubmitKycResponse_DENIED, resp.Result)
	})

	t.Run("Happy path", func(t *testing.T) {
		userID, keyPair := newRegisteredUser(t, ctx, accounts)
		fake.createdCustomer = underReviewCustomer("cust_created")

		resp := submit(t, keyPair, validSubmission())
		require.Equal(t, kycpb.SubmitKycResponse_OK, resp.Result)

		state := resp.KycState
		require.NotNil(t, state)
		require.Equal(t, kycpb.KycStatus_UNDER_REVIEW, state.Status)
		require.Equal(t, kycpb.KycState_WAIT, state.NextStep)
		require.False(t, state.GetBridgeFeatures().CanOfframp)

		record, err := store.Get(ctx, userID, thirdpartypb.Partner_BRIDGE)
		require.NoError(t, err)
		require.Equal(t, "cust_created", record.CustomerID)

		body := fake.lastCreateBody
		require.Equal(t, "individual", body["type"])
		require.Equal(t, "Jane", body["first_name"])
		require.Equal(t, "Doe", body["last_name"])
		require.Equal(t, "jane@example.com", body["email"])
		require.Equal(t, "+12025550123", body["phone"])
		require.Equal(t, "1990-04-02", body["birth_date"])
		require.Equal(t, model.UserIDString(userID), body["client_reference_id"])
		require.Equal(t, "signed-agreement-id", body["signed_agreement_id"])
		require.Equal(t, []any{"base"}, body["endorsements"])

		address := body["residential_address"].(map[string]any)
		require.Equal(t, "123 Main St", address["street_line_1"])
		require.Equal(t, "San Francisco", address["city"])
		require.Equal(t, "CA", address["subdivision"])
		require.Equal(t, "94105", address["postal_code"])
		require.Equal(t, "USA", address["country"])

		identifying := body["identifying_information"].([]any)
		require.Len(t, identifying, 2)
		taxID := identifying[0].(map[string]any)
		require.Equal(t, "ssn", taxID["type"])
		require.Equal(t, "usa", taxID["issuing_country"])
		require.Equal(t, "123456789", taxID["number"])
		document := identifying[1].(map[string]any)
		require.Equal(t, "drivers_license", document["type"])
		require.Equal(t, "usa", document["issuing_country"])
		require.Equal(t, "data:image/jpeg;base64,"+base64.StdEncoding.EncodeToString([]byte("front-image-bytes")), document["image_front"])
		require.Equal(t, "data:image/jpeg;base64,"+base64.StdEncoding.EncodeToString([]byte("back-image-bytes")), document["image_back"])

		require.Equal(t, "personal_or_living_expenses", body["account_purpose"])
		require.Equal(t, "salary", body["source_of_funds"])
		require.Equal(t, "employed", body["employment_status"])
		require.Equal(t, "0_4999", body["expected_monthly_payments_usd"])
		require.Equal(t, false, body["acting_as_intermediary"])
		require.Equal(t, "1234", body["most_recent_occupation"])

		t.Run("Resubmit reports existing state", func(t *testing.T) {
			createCalls := fake.createCalls
			resp := submit(t, keyPair, validSubmission())
			require.Equal(t, kycpb.SubmitKycResponse_EXISTING_SUBMISSION, resp.Result)
			require.Equal(t, kycpb.KycStatus_UNDER_REVIEW, resp.KycState.Status)
			require.Equal(t, createCalls, fake.createCalls)
		})
	})

	t.Run("Already verified", func(t *testing.T) {
		userID, keyPair := newRegisteredUser(t, ctx, accounts)
		fake.setCustomer(&bridge.Customer{
			ID:                        "cust_active",
			Status:                    bridge.CustomerStatusActive,
			HasAcceptedTermsOfService: true,
			Endorsements: []bridge.Endorsement{{
				Name:   bridge.EndorsementBase,
				Status: bridge.EndorsementStatusApproved,
			}},
		})
		require.NoError(t, store.Create(ctx, &kyc.Record{
			UserID:     userID,
			Partner:    thirdpartypb.Partner_BRIDGE,
			CustomerID: "cust_active",
			CreatedAt:  time.Now().UTC(),
		}))

		resp := submit(t, keyPair, validSubmission())
		require.Equal(t, kycpb.SubmitKycResponse_EXISTING_SUBMISSION, resp.Result)
		require.Equal(t, kycpb.KycStatus_ACTIVE, resp.KycState.Status)
		require.True(t, resp.KycState.GetBridgeFeatures().CanOfframp)
	})

	t.Run("Instant rejection", func(t *testing.T) {
		// Bridge validated the submission asynchronously and rejected it: the
		// customer exists, the mapping is persisted, and the state directs the
		// user to correct and resubmit with the customer-facing reasons.
		userID, keyPair := newRegisteredUser(t, ctx, accounts)
		fake.createdCustomer = &bridge.Customer{
			ID:                        "cust_rejected",
			Status:                    bridge.CustomerStatusRejected,
			HasAcceptedTermsOfService: true,
			Endorsements: []bridge.Endorsement{{
				Name:   bridge.EndorsementBase,
				Status: bridge.EndorsementStatusIncomplete,
				Requirements: &bridge.EndorsementRequirements{
					Issues: json.RawMessage(`["government_id_verification_failed"]`),
				},
			}},
			RejectionReasons: []bridge.RejectionReason{{
				Reason:          "Your information could not be verified",
				DeveloperReason: "ID cannot be verified against third-party databases",
				CreatedAt:       "2026-07-31T12:00:00.000Z",
			}},
		}

		resp := submit(t, keyPair, validSubmission())
		require.Equal(t, kycpb.SubmitKycResponse_OK, resp.Result)

		state := resp.KycState
		require.NotNil(t, state)
		require.Equal(t, kycpb.KycStatus_REJECTED, state.Status)
		require.Equal(t, kycpb.KycState_PROVIDE_REQUIREMENTS, state.NextStep)
		require.False(t, state.GetBridgeFeatures().CanOfframp)
		require.Len(t, state.Requirements, 1)
		require.Equal(t, kycpb.KycState_Requirement_ID_DOCUMENT, state.Requirements[0].Field)
		require.Len(t, state.RejectionReasons, 1)
		require.Equal(t, "Your information could not be verified", state.RejectionReasons[0].Message)

		record, err := store.Get(ctx, userID, thirdpartypb.Partner_BRIDGE)
		require.NoError(t, err)
		require.Equal(t, "cust_rejected", record.CustomerID)
	})

	t.Run("Agreement expired", func(t *testing.T) {
		userID, keyPair := newRegisteredUser(t, ctx, accounts)
		fake.createdCustomer = nil
		fake.createError = &bridge.Error{
			StatusCode: http.StatusUnprocessableEntity,
			Code:       "invalid_signed_agreement_id",
			Message:    "signed agreement id is invalid or expired",
		}

		resp := submit(t, keyPair, validSubmission())
		require.Equal(t, kycpb.SubmitKycResponse_AGREEMENT_EXPIRED, resp.Result)

		_, err := store.Get(ctx, userID, thirdpartypb.Partner_BRIDGE)
		require.ErrorIs(t, err, kyc.ErrNotFound)
	})

	t.Run("Idempotency conflict", func(t *testing.T) {
		// An idempotency-key conflict is a retryable internal failure, not an
		// identity problem.
		userID, keyPair := newRegisteredUser(t, ctx, accounts)
		fake.createdCustomer = nil
		fake.createError = &bridge.Error{
			StatusCode: http.StatusUnprocessableEntity,
			Code:       "idempotency_key_conflict",
			Message:    "idempotency key has already been used",
		}

		req := &kycpb.SubmitKycRequest{
			Partner:    thirdpartypb.Partner_BRIDGE,
			Submission: &kycpb.SubmitKycRequest_Individual{Individual: validSubmission()},
		}
		require.NoError(t, keyPair.Auth(req, &req.Auth))
		_, err := client.SubmitKyc(ctx, req)
		require.Equal(t, codes.Internal, status.Code(err))

		_, err = store.Get(ctx, userID, thirdpartypb.Partner_BRIDGE)
		require.ErrorIs(t, err, kyc.ErrNotFound)
	})
}

func testServer_UpdateKyc(t *testing.T, accounts account.Store, store kyc.Store) {
	ctx := context.Background()
	log := zaptest.NewLogger(t)

	fake := &fakeBridge{}
	backend := httptest.NewServer(fake.handler(t))
	defer backend.Close()
	bridgeClient := bridge.NewClient("test-api-key", bridge.WithBaseURL(backend.URL))

	authz := account.NewAuthorizer(log, accounts, auth.NewKeyPairAuthenticator(log))

	serv := kyc.NewServer(log, authz, accounts, store, bridgeClient)
	cc := testutil.RunGRPCServer(t, log, testutil.WithService(func(s *grpc.Server) {
		kycpb.RegisterKycServiceServer(s, serv)
	}))

	client := kycpb.NewKycServiceClient(cc)

	update := func(t *testing.T, keyPair model.KeyPair, individual *kycpb.IndividualKycUpdate) *kycpb.UpdateKycResponse {
		req := &kycpb.UpdateKycRequest{
			Partner: thirdpartypb.Partner_BRIDGE,
			Update:  &kycpb.UpdateKycRequest_Individual{Individual: individual},
		}
		require.NoError(t, keyPair.Auth(req, &req.Auth))
		resp, err := client.UpdateKyc(ctx, req)
		require.NoError(t, err)
		return resp
	}

	newUserWithRecord := func(t *testing.T, customerID string) (*commonpb.UserId, model.KeyPair) {
		userID, keyPair := newRegisteredUser(t, ctx, accounts)
		require.NoError(t, store.Create(ctx, &kyc.Record{
			UserID:     userID,
			Partner:    thirdpartypb.Partner_BRIDGE,
			CustomerID: customerID,
			CreatedAt:  time.Now().UTC(),
		}))
		return userID, keyPair
	}

	t.Run("Unregistered user", func(t *testing.T) {
		userID := model.MustGenerateUserID()
		keyPair := model.MustGenerateKeyPair()
		_, err := accounts.Bind(ctx, userID, keyPair.Proto())
		require.NoError(t, err)

		resp := update(t, keyPair, &kycpb.IndividualKycUpdate{})
		require.Equal(t, kycpb.UpdateKycResponse_DENIED, resp.Result)
	})

	t.Run("Not started", func(t *testing.T) {
		_, keyPair := newRegisteredUser(t, ctx, accounts)

		resp := update(t, keyPair, &kycpb.IndividualKycUpdate{
			Name: &kycpb.LegalName{FirstName: "Jane", LastName: "Doe"},
		})
		require.Equal(t, kycpb.UpdateKycResponse_NOT_STARTED, resp.Result)
	})

	t.Run("Nothing to update", func(t *testing.T) {
		_, keyPair := newUserWithRecord(t, "cust_no_update")
		updateCalls := fake.updateCalls

		resp := update(t, keyPair, &kycpb.IndividualKycUpdate{})
		require.Equal(t, kycpb.UpdateKycResponse_NOTHING_TO_UPDATE, resp.Result)
		require.Equal(t, updateCalls, fake.updateCalls)
	})

	t.Run("Document retake", func(t *testing.T) {
		_, keyPair := newUserWithRecord(t, "cust_retake")
		fake.updatedCustomer = &bridge.Customer{
			ID:                        "cust_retake",
			Status:                    bridge.CustomerStatusUnderReview,
			HasAcceptedTermsOfService: true,
			Endorsements: []bridge.Endorsement{{
				Name:   bridge.EndorsementBase,
				Status: bridge.EndorsementStatusIncomplete,
				Requirements: &bridge.EndorsementRequirements{
					Pending: []string{"id_verification"},
				},
			}},
		}

		resp := update(t, keyPair, &kycpb.IndividualKycUpdate{
			IdentityDocuments: []*kycpb.IdentityDocument{{
				Type:           kycpb.IdentityDocument_DRIVERS_LICENSE,
				FrontImage:     []byte("new-front"),
				BackImage:      []byte("new-back"),
				MimeType:       "image/jpeg",
				IssuingCountry: &commonpb.CountryCode{Value: "US"},
			}},
		})
		require.Equal(t, kycpb.UpdateKycResponse_OK, resp.Result)
		require.Equal(t, kycpb.KycStatus_UNDER_REVIEW, resp.KycState.Status)
		require.Equal(t, kycpb.KycState_WAIT, resp.KycState.NextStep)

		require.Equal(t, "cust_retake", fake.lastUpdateCustomerID)
		body := fake.lastUpdateBody
		identifying := body["identifying_information"].([]any)
		require.Len(t, identifying, 1)
		document := identifying[0].(map[string]any)
		require.Equal(t, "drivers_license", document["type"])
		require.Equal(t, "usa", document["issuing_country"])
		require.Equal(t, "data:image/jpeg;base64,"+base64.StdEncoding.EncodeToString([]byte("new-front")), document["image_front"])

		// Only the provided fields are sent.
		require.NotContains(t, body, "first_name")
		require.NotContains(t, body, "birth_date")
		require.NotContains(t, body, "residential_address")
		require.NotContains(t, body, "signed_agreement_id")
	})

	t.Run("Agreement re-acceptance only", func(t *testing.T) {
		_, keyPair := newUserWithRecord(t, "cust_reaccept")
		fake.updatedCustomer = &bridge.Customer{
			ID:                        "cust_reaccept",
			Status:                    bridge.CustomerStatusActive,
			HasAcceptedTermsOfService: true,
			Endorsements: []bridge.Endorsement{{
				Name:   bridge.EndorsementBase,
				Status: bridge.EndorsementStatusApproved,
			}},
		}

		resp := update(t, keyPair, &kycpb.IndividualKycUpdate{
			SignedAgreementTokens: []*kycpb.SignedAgreementToken{{Value: "new-agreement-id"}},
		})
		require.Equal(t, kycpb.UpdateKycResponse_OK, resp.Result)
		require.Equal(t, kycpb.KycState_NONE, resp.KycState.NextStep)
		require.True(t, resp.KycState.GetBridgeFeatures().CanOfframp)

		body := fake.lastUpdateBody
		require.Equal(t, "new-agreement-id", body["signed_agreement_id"])
		require.NotContains(t, body, "identifying_information")
	})
}
