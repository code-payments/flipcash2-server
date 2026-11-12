//go:build cdpsandbox

package tests

import (
	"context"
	"errors"
	"fmt"
	"net/netip"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"
	kyccoinbasepb "github.com/code-payments/flipcash2-protobuf-api/generated/go/kyc/coinbase/v1"

	"github.com/code-payments/flipcash2-server/account"
	"github.com/code-payments/flipcash2-server/cdp/customers"
	"github.com/code-payments/flipcash2-server/cdp/customers/customerstest"
	"github.com/code-payments/flipcash2-server/kyc/coinbase"
)

// RunSandboxTests runs the Kyc server against Coinbase's CDP Sandbox through
// cdpCustomers, a client on customers.SandboxBaseURL, driving it only through
// its three RPCs as a client would. Each path is a subtest with a fresh user
// whose Coinbase customer is deleted when the subtest ends, pass or fail;
// teardown resets the store after each.
//
// It submits Coinbase's Sandbox magic values and the placeholders the
// customers package's own Sandbox test uses, nothing resembling real personal
// data. The Sandbox reviews on its own, so where the fake suite completes a
// review, these poll GetKycState.
func RunSandboxTests(t *testing.T, accounts account.Store, store coinbase.Store, cdpCustomers *customers.Client, teardown func()) {
	for _, path := range []struct {
		name string
		run  func(t *testing.T, env *sandboxEnv)
	}{
		{"approve", testSandbox_Approve},
		{"decline then correct", testSandbox_DeclineThenCorrect},
		{"block", testSandbox_Block},
		{"pre-checks never reach coinbase", testSandbox_PreChecks},
	} {
		t.Run(path.name, func(t *testing.T) {
			env := newServerEnvOn(t, accounts, store, cdpCustomers)
			env.resolver.set(netip.MustParseAddr(sandboxIP), nil)
			env.phone = sandboxPhone
			path.run(t, &sandboxEnv{serverEnv: env, cdp: cdpCustomers})
		})
		teardown()
	}
}

// sandboxReviewTimeout bounds a wait for the Sandbox's review.
const sandboxReviewTimeout = 60 * time.Second

// sandboxIP is the requester address the customers package's Sandbox test
// sends, from a documentation range.
const sandboxIP = "203.0.113.42"

// The placeholders the customers package's Sandbox test submits.
const (
	sandboxFirstName  = "Jane"
	sandboxLastName   = "Doe"
	sandboxLine1      = "500 Main St"
	sandboxCity       = "Boston"
	sandboxState      = "MA"
	sandboxPostalCode = "02108"
	sandboxEmail      = "jane.doe@example.com"
	sandboxPhone      = "+15551234567"
)

type sandboxEnv struct {
	*serverEnv
	cdp *customers.Client
}

// ssnDigits is a magic SSN as the proto takes it, with no dashes.
func ssnDigits(dashed string) string {
	return strings.ReplaceAll(dashed, "-", "")
}

func sandboxIndividual(ssn string) *kyccoinbasepb.Individual {
	return &kyccoinbasepb.Individual{
		LegalName:   &kyccoinbasepb.LegalName{FirstName: sandboxFirstName, LastName: sandboxLastName},
		DateOfBirth: &kyccoinbasepb.DateOfBirth{Year: 1990, Month: 1, Day: 15},
		Address: &kyccoinbasepb.Address{
			Line1:       sandboxLine1,
			City:        sandboxCity,
			State:       sandboxState,
			PostalCode:  sandboxPostalCode,
			CountryCode: "US",
		},
		Ssn:              &kyccoinbasepb.Ssn{Value: ssnDigits(ssn)},
		Email:            &commonpb.EmailAddress{Value: sandboxEmail},
		PhoneNumber:      &commonpb.PhoneNumber{Value: sandboxPhone},
		PurposeOfAccount: kyccoinbasepb.PurposeOfAccount_PURPOSE_OF_ACCOUNT_PAYMENTS_TO_FRIENDS,
		SourceOfFunds:    kyccoinbasepb.SourceOfFunds_SOURCE_OF_FUNDS_SALARY,
	}
}

// start starts u, after arranging for the customer it creates to be deleted
// when the subtest ends, whether it passes or fails. The customer is looked
// up from the store at cleanup, so one created by a start that then failed is
// deleted too, once recorded.
func (e *sandboxEnv) start(t *testing.T, u testUser) *kyccoinbasepb.KycState {
	t.Helper()
	t.Cleanup(func() {
		ctx := context.Background()
		record, err := e.store.GetCustomer(ctx, u.id)
		if errors.Is(err, coinbase.ErrNotFound) {
			return
		}
		if err != nil {
			t.Errorf("cleanup: reading the customer record: %v", err)
			return
		}
		id := record.CustomerID
		if err := e.cdp.Delete(ctx, id); err != nil && !customers.IsNotFound(err) {
			t.Errorf("cleanup: deleting customer %s: %v", id, err)
			return
		}
		if _, err := e.cdp.Get(ctx, id); customers.IsNotFound(err) {
			t.Logf("deleted customer %s; a read answers not_found", id)
		} else {
			t.Errorf("cleanup: customer %s still readable after delete: %v", id, err)
		}
	})

	state := e.mustStart(u)
	t.Logf("created customer %s", e.customerID(u))
	return state
}

// poll polls GetKycState once a second until done holds, failing after
// sandboxReviewTimeout, and logs how long it took.
func (e *sandboxEnv) poll(t *testing.T, u testUser, what string, done func(*kyccoinbasepb.KycState) bool) *kyccoinbasepb.KycState {
	t.Helper()
	began := time.Now()
	for {
		state := e.mustGet(u)
		if done(state) {
			t.Logf("%s after %s", what, time.Since(began).Round(100*time.Millisecond))
			return state
		}
		if time.Since(began) > sandboxReviewTimeout {
			t.Fatalf("%s: still %s with %d requirements after %s", what, state.Status, len(state.Requirements), sandboxReviewTimeout)
		}
		time.Sleep(time.Second)
	}
}

// pollReview polls until the state is no longer IN_REVIEW.
func (e *sandboxEnv) pollReview(t *testing.T, u testUser) *kyccoinbasepb.KycState {
	t.Helper()
	return e.poll(t, u, "review over", func(s *kyccoinbasepb.KycState) bool {
		return s.Status != kyccoinbasepb.KycState_IN_REVIEW
	})
}

// listedTerms returns the one terms version state lists alongside nothing
// else, failing otherwise.
func listedTerms(t *testing.T, state *kyccoinbasepb.KycState) *kyccoinbasepb.TermsOfService {
	t.Helper()
	requireStatus(t, kyccoinbasepb.KycState_ACTION_REQUIRED, state)
	r := requirementsOf(t, state)
	require.Empty(t, r.fields, "fields listed beside the terms")
	require.Empty(t, r.forms, "tax forms listed beside the terms")
	require.Len(t, r.terms, 1)
	for _, req := range r.terms {
		require.Equal(t, kyccoinbasepb.Requirement_MISSING, req.Reason)
		tos := req.GetTermsOfService()
		// The fake models the one version the Sandbox listed when it was
		// written; a new one means the fake should follow.
		require.Equal(t, customerstest.TosVersionID, tos.VersionId, "the Sandbox lists another terms version than the fake")
		return tos
	}
	return nil
}

func languagesOf(tos *kyccoinbasepb.TermsOfService) []string {
	var out []string
	for _, l := range tos.Languages {
		out = append(out, strings.ToLower(l.Value))
	}
	return out
}

func testSandbox_Approve(t *testing.T, e *sandboxEnv) {
	u := e.newUser(true)

	state := e.mustGet(u)
	requireStatus(t, kyccoinbasepb.KycState_NOT_STARTED, state)
	require.Empty(t, state.Requirements)
	e.requireNoRecord(u)

	started := e.start(t, u)
	requireStartedState(t, started)

	// Starting again changes nothing.
	require.True(t, proto.Equal(started, e.mustStart(u)), "a second start changed the state")

	// Coinbase puts no terms to a customer whose address it does not have,
	// and refuses the acceptance, which the server reads as NOT_ASKED.
	resp, err := e.submit(u, &kyccoinbasepb.SubmitKycRequest{
		TermsOfServiceAcceptances: []*kyccoinbasepb.TermsOfServiceAcceptance{acceptance(customerstest.TosVersionID, "en")},
	})
	require.NoError(t, err)
	require.Equal(t, kyccoinbasepb.SubmitKycResponse_INVALID_SUBMISSION, resp.Result)
	require.Empty(t, resp.InvalidFields)
	require.Len(t, resp.InvalidTermsOfServiceAcceptances, 1)
	require.Equal(t, customerstest.TosVersionID, resp.InvalidTermsOfServiceAcceptances[0].VersionId)
	require.Equal(t, kyccoinbasepb.SubmitKycResponse_InvalidTermsOfServiceAcceptance_NOT_ASKED, resp.InvalidTermsOfServiceAcceptances[0].Reason)
	require.True(t, proto.Equal(started, e.mustGet(u)), "a refused submission changed the state")

	// Identity and the W-9: the terms are listed now that the address is in.
	// Whether the identity is still being verified or already verified, it is
	// not listed either way, so the state is the terms alone.
	state = e.mustSubmit(u, &kyccoinbasepb.SubmitKycRequest{
		Individual:      sandboxIndividual(customerstest.SsnApprove),
		TaxAttestations: []*kyccoinbasepb.TaxAttestation{w9()},
	})
	tos := listedTerms(t, state)
	language := tos.Languages[0].Value

	// A language the version is not published in is refused before sending.
	require.NotContains(t, languagesOf(tos), "fr")
	resp, err = e.submit(u, &kyccoinbasepb.SubmitKycRequest{
		TermsOfServiceAcceptances: []*kyccoinbasepb.TermsOfServiceAcceptance{acceptance(tos.VersionId, "fr")},
	})
	require.NoError(t, err)
	require.Equal(t, kyccoinbasepb.SubmitKycResponse_INVALID_SUBMISSION, resp.Result)
	require.Len(t, resp.InvalidTermsOfServiceAcceptances, 1)
	require.Equal(t, kyccoinbasepb.SubmitKycResponse_InvalidTermsOfServiceAcceptance_UNSUPPORTED_LANGUAGE, resp.InvalidTermsOfServiceAcceptances[0].Reason)

	// The acceptance: IN_REVIEW while the Sandbox is still verifying the
	// identity, or VERIFIED at once if it has already finished, which it
	// may do within seconds. Either is what the proto allows.
	state = e.mustSubmit(u, &kyccoinbasepb.SubmitKycRequest{
		TermsOfServiceAcceptances: []*kyccoinbasepb.TermsOfServiceAcceptance{acceptance(tos.VersionId, language)},
	})
	require.Contains(t, []kyccoinbasepb.KycState_Status{kyccoinbasepb.KycState_IN_REVIEW, kyccoinbasepb.KycState_VERIFIED}, state.Status,
		"after the acceptance: %s", state.Status)
	if state.Status == kyccoinbasepb.KycState_IN_REVIEW {
		require.Empty(t, state.Requirements)
	}

	state = e.pollReview(t, u)
	requireStatus(t, kyccoinbasepb.KycState_VERIFIED, state)
	require.Empty(t, state.Requirements)

	// Accepting the version already accepted changes nothing.
	state = e.mustSubmit(u, &kyccoinbasepb.SubmitKycRequest{
		TermsOfServiceAcceptances: []*kyccoinbasepb.TermsOfServiceAcceptance{acceptance(tos.VersionId, language)},
	})
	requireStatus(t, kyccoinbasepb.KycState_VERIFIED, state)

	// The same legal name again, with no new W-9: an identical value voids
	// nothing, so the user stays verified.
	state = e.mustSubmit(u, &kyccoinbasepb.SubmitKycRequest{
		Individual: &kyccoinbasepb.Individual{LegalName: &kyccoinbasepb.LegalName{FirstName: sandboxFirstName, LastName: sandboxLastName}},
	})
	requireStatus(t, kyccoinbasepb.KycState_VERIFIED, state)
	requireStatus(t, kyccoinbasepb.KycState_VERIFIED, e.mustGet(u))

	requireSandboxValuesNotLogged(t, e)
}

func testSandbox_DeclineThenCorrect(t *testing.T, e *sandboxEnv) {
	u := e.newUser(true)
	requireStartedState(t, e.start(t, u))

	state := e.mustSubmit(u, &kyccoinbasepb.SubmitKycRequest{
		Individual:      sandboxIndividual(customerstest.SsnDecline),
		TaxAttestations: []*kyccoinbasepb.TaxAttestation{w9()},
	})
	// The terms are listed; the identity may already be rejected beside them
	// if the Sandbox reviewed it before answering.
	requireStatus(t, kyccoinbasepb.KycState_ACTION_REQUIRED, state)
	require.Contains(t, requirementsOf(t, state).terms, customerstest.TosVersionID)

	state = e.mustSubmit(u, &kyccoinbasepb.SubmitKycRequest{
		TermsOfServiceAcceptances: []*kyccoinbasepb.TermsOfServiceAcceptance{acceptance(customerstest.TosVersionID, "en")},
	})
	require.Contains(t, []kyccoinbasepb.KycState_Status{kyccoinbasepb.KycState_IN_REVIEW, kyccoinbasepb.KycState_ACTION_REQUIRED}, state.Status)

	// Rejected fields, listed until corrected.
	state = e.poll(t, u, "decline", func(s *kyccoinbasepb.KycState) bool {
		return s.Status != kyccoinbasepb.KycState_IN_REVIEW
	})
	requireStatus(t, kyccoinbasepb.KycState_ACTION_REQUIRED, state)
	r := requirementsOf(t, state)
	t.Logf("after the decline: fields %v, terms %d, forms %v", r.fields, len(r.terms), r.forms)
	require.Equal(t, kyccoinbasepb.Requirement_REJECTED, r.fields[kyccoinbasepb.Field_FIELD_SSN], "the declined SSN is not listed as rejected")
	for field, reason := range r.fields {
		require.Equal(t, kyccoinbasepb.Requirement_REJECTED, reason, "field %s", field)
	}
	require.Empty(t, r.terms)
	require.Empty(t, r.forms)

	// Only the SSN corrected, with a new W-9 since the W-9 certifies it. The
	// other fields are verified again along with it.
	state = e.mustSubmit(u, &kyccoinbasepb.SubmitKycRequest{
		Individual:      &kyccoinbasepb.Individual{Ssn: &kyccoinbasepb.Ssn{Value: ssnDigits(customerstest.SsnApprove)}},
		TaxAttestations: []*kyccoinbasepb.TaxAttestation{w9()},
	})
	require.Contains(t, []kyccoinbasepb.KycState_Status{kyccoinbasepb.KycState_IN_REVIEW, kyccoinbasepb.KycState_VERIFIED}, state.Status,
		"after the correction: %s with %v", state.Status, requirementsOf(t, state).fields)

	state = e.pollReview(t, u)
	requireStatus(t, kyccoinbasepb.KycState_VERIFIED, state)

	requireSandboxValuesNotLogged(t, e)
}

func testSandbox_Block(t *testing.T, e *sandboxEnv) {
	u := e.newUser(true)
	requireStartedState(t, e.start(t, u))

	// The block may come in the submission's own response or after a
	// review; either way the user ends BLOCKED.
	resp, err := e.submit(u, &kyccoinbasepb.SubmitKycRequest{
		Individual:      sandboxIndividual(customerstest.SsnBlock),
		TaxAttestations: []*kyccoinbasepb.TaxAttestation{w9()},
	})
	require.NoError(t, err)
	switch resp.Result {
	case kyccoinbasepb.SubmitKycResponse_BLOCKED:
		require.Nil(t, resp.KycState)
		t.Logf("blocked in the submission's response")
	case kyccoinbasepb.SubmitKycResponse_OK:
		require.NotEqual(t, kyccoinbasepb.KycState_BLOCKED, resp.KycState.Status)
		e.poll(t, u, "block", func(s *kyccoinbasepb.KycState) bool {
			return s.Status == kyccoinbasepb.KycState_BLOCKED
		})
	default:
		t.Fatalf("submission answered %s", resp.Result)
	}

	state := e.mustGet(u)
	requireStatus(t, kyccoinbasepb.KycState_BLOCKED, state)
	require.Empty(t, state.Requirements)

	state = e.mustStart(u)
	requireStatus(t, kyccoinbasepb.KycState_BLOCKED, state)
	require.Empty(t, state.Requirements)

	resp, err = e.submit(u, &kyccoinbasepb.SubmitKycRequest{Individual: sandboxIndividual(customerstest.SsnApprove)})
	require.NoError(t, err)
	require.Equal(t, kyccoinbasepb.SubmitKycResponse_BLOCKED, resp.Result)
	require.Nil(t, resp.KycState)

	requireSandboxValuesNotLogged(t, e)
}

func testSandbox_PreChecks(t *testing.T, e *sandboxEnv) {
	u := e.newUser(true)
	started := e.start(t, u)
	requireStartedState(t, started)

	underage := &kyccoinbasepb.DateOfBirth{Year: uint32(e.now.Year() - 17), Month: 1, Day: 15}
	resp, err := e.submit(u, &kyccoinbasepb.SubmitKycRequest{Individual: &kyccoinbasepb.Individual{DateOfBirth: underage}})
	require.NoError(t, err)
	require.Equal(t, kyccoinbasepb.SubmitKycResponse_INVALID_SUBMISSION, resp.Result)
	require.Len(t, resp.InvalidFields, 1)
	require.Equal(t, kyccoinbasepb.Field_FIELD_DATE_OF_BIRTH, resp.InvalidFields[0].Field)
	require.Equal(t, kyccoinbasepb.SubmitKycResponse_InvalidField_UNDERAGE, resp.InvalidFields[0].Reason)

	resp, err = e.submit(u, &kyccoinbasepb.SubmitKycRequest{Individual: &kyccoinbasepb.Individual{Address: &kyccoinbasepb.Address{
		Line1: sandboxLine1, City: "Toronto", State: "ON", PostalCode: "12345", CountryCode: "CA",
	}}})
	require.NoError(t, err)
	require.Equal(t, kyccoinbasepb.SubmitKycResponse_INVALID_SUBMISSION, resp.Result)
	require.Len(t, resp.InvalidFields, 1)
	require.Equal(t, kyccoinbasepb.Field_FIELD_RESIDENTIAL_ADDRESS, resp.InvalidFields[0].Field)
	require.Equal(t, kyccoinbasepb.SubmitKycResponse_InvalidField_UNSUPPORTED_REGION, resp.InvalidFields[0].Reason)

	// Neither reached Coinbase: the customer is not blocked, and still lists
	// the date of birth and the address as missing.
	state := e.mustGet(u)
	require.NotEqual(t, kyccoinbasepb.KycState_BLOCKED, state.Status, "the customer was blocked")
	requireStartedState(t, state)
	r := requirementsOf(t, state)
	require.Equal(t, kyccoinbasepb.Requirement_MISSING, r.fields[kyccoinbasepb.Field_FIELD_DATE_OF_BIRTH])
	require.Equal(t, kyccoinbasepb.Requirement_MISSING, r.fields[kyccoinbasepb.Field_FIELD_RESIDENTIAL_ADDRESS])
	require.True(t, proto.Equal(started, state), "the state changed")
}

// requireSandboxValuesNotLogged requires that the server logged none of the
// values the paths submit.
func requireSandboxValuesNotLogged(t *testing.T, e *sandboxEnv) {
	t.Helper()
	values := []string{
		sandboxFirstName, sandboxLastName, sandboxLine1, sandboxCity, sandboxEmail, sandboxPhone, sandboxIP,
		customerstest.SsnApprove, customerstest.SsnDecline, customerstest.SsnBlock,
		ssnDigits(customerstest.SsnApprove), ssnDigits(customerstest.SsnDecline), ssnDigits(customerstest.SsnBlock),
		fmt.Sprintf("%d-%02d-%02d", 1990, 1, 15),
	}
	requireNotLogged(t, e.logs, values...)
}
