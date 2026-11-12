package customers_test

import (
	"context"
	"net/http"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/code-payments/flipcash2-server/cdp/customers"
	"github.com/code-payments/flipcash2-server/cdp/customers/customerstest"
)

// These tests drive the real client against the fake, pinning the behaviour
// the fake reproduces from Coinbase (see customerstest/customer.go).

var fakeNow = time.Date(2026, 11, 11, 18, 0, 0, 0, time.UTC)

func newFake(t *testing.T) (*customerstest.Server, *customers.Client) {
	fake := customerstest.NewServer(customerstest.WithClock(func() time.Time { return fakeNow }))
	t.Cleanup(fake.Close)
	return fake, fake.Client()
}

var requestedCaps = map[customers.CapabilityName]customers.CapabilityRequest{
	customers.CapabilityCustodyFiat:  {Requested: true},
	customers.CapabilityTransferFiat: {Requested: true},
}

var ip = &customers.Compliance{RequesterIPAddress: "203.0.113.42"}

func identity(ssn string) *customers.IndividualInput {
	return &customers.IndividualInput{
		FirstName:   "Jane",
		LastName:    "Doe",
		DateOfBirth: &customers.DateOfBirth{Day: "15", Month: "1", Year: "1990"},
		FullSsn:     ssn,
		Address: &customers.AddressInput{
			Line1: "500 Main St", Line2: ptr(""), City: "Boston", State: "MA", PostCode: "02108", CountryCode: "US",
		},
		Email:            "jane.doe@example.com",
		PhoneNumber:      "+15551234567",
		PurposeOfAccount: "payments_to_friends",
		SourceOfFunds:    "salary",
	}
}

func w9() []customers.TaxAttestation {
	return []customers.TaxAttestation{{UsW9: &customers.UsW9Attestation{EdeliveryConsent: true, AcceptedAt: fakeNow.Add(-time.Minute)}}}
}

func tos(language string) []customers.TosAcceptance {
	return []customers.TosAcceptance{{VersionID: customerstest.TosVersionID, Language: language, AcceptedAt: fakeNow.Add(-time.Minute)}}
}

var identityKeys = []customers.RequirementKey{
	"firstName", "lastName", "dateOfBirth", "fullSsn", "addressLine1", "addressCity",
	"addressSubdivision", "addressPostalCode", "addressCountryCode", "email", "phoneNumber",
}

// statuses flattens a customer's requirements to key → status.
func statuses(c *customers.Customer) map[customers.RequirementKey]customers.RequirementStatus {
	out := map[customers.RequirementKey]customers.RequirementStatus{}
	for k, r := range c.Requirements {
		out[k] = r.Status
	}
	return out
}

func identityWith(status customers.RequirementStatus) map[customers.RequirementKey]customers.RequirementStatus {
	out := map[customers.RequirementKey]customers.RequirementStatus{}
	for _, k := range identityKeys {
		out[k] = status
	}
	return out
}

func with(m map[customers.RequirementKey]customers.RequirementStatus, extra map[customers.RequirementKey]customers.RequirementStatus) map[customers.RequirementKey]customers.RequirementStatus {
	out := map[customers.RequirementKey]customers.RequirementStatus{}
	for k, v := range m {
		out[k] = v
	}
	for k, v := range extra {
		out[k] = v
	}
	return out
}

func assertCaps(t *testing.T, c *customers.Customer, want customers.CapabilityStatus) {
	t.Helper()
	require.Len(t, c.Capabilities, 8)
	for name, capability := range c.Capabilities {
		if _, requested := requestedCaps[name]; requested {
			assert.Equal(t, customers.Capability{Requested: true, Status: want}, capability, name)
		} else {
			assert.Equal(t, customers.Capability{Requested: false, Status: customers.CapabilityUnrequested}, capability, name)
		}
	}
}

// requested creates a customer and requests the two fiat capabilities.
func requested(t *testing.T, client *customers.Client) string {
	t.Helper()
	ctx := context.Background()
	c, err := client.Create(ctx, uuid.NewString(), &customers.CustomerInput{Capabilities: requestedCaps, Compliance: ip})
	require.NoError(t, err)
	return c.ID
}

func TestFake_CreateMinimal(t *testing.T) {
	_, client := newFake(t)
	c, err := client.Create(context.Background(), uuid.NewString(), nil)
	require.NoError(t, err)
	assert.Regexp(t, `^customer_[a-f0-9\-]{36}$`, c.ID)
	assert.Equal(t, customers.CustomerTypeIndividual, c.Type)
	assert.Nil(t, c.Individual)
	assert.Nil(t, c.Capabilities)
	assert.Nil(t, c.Requirements)
	assert.Equal(t, fakeNow, c.CreatedAt)
}

func TestFake_CreateIdempotency(t *testing.T) {
	fake, client := newFake(t)
	ctx := context.Background()
	key := uuid.NewString()

	first, err := client.Create(ctx, key, nil)
	require.NoError(t, err)
	again, err := client.Create(ctx, key, nil)
	require.NoError(t, err)
	assert.Equal(t, first.ID, again.ID)

	_, err = client.Create(ctx, key, &customers.CustomerInput{Individual: &customers.IndividualInput{FirstName: "A"}})
	require.True(t, customers.HasType(err, customers.ErrorTypeIdempotency), "%v", err)
	e, _ := customers.AsError(err)
	assert.Equal(t, http.StatusUnprocessableEntity, e.StatusCode)
	assert.True(t, customers.IsClientError(err))

	assert.Equal(t, []string{first.ID}, fake.CustomerIDs())
}

func TestFake_IdempotencyRecordsRefusals(t *testing.T) {
	fake, client := newFake(t)
	ctx := context.Background()

	// A create refused for its content (capabilities with no requester IP).
	key := uuid.NewString()
	refused := &customers.CustomerInput{Capabilities: requestedCaps}
	_, err := client.Create(ctx, key, refused)
	require.True(t, customers.IsInvalidRequest(err), "%v", err)
	first, _ := customers.AsError(err)

	// The same request again gets the same response, correlation ID and all.
	_, err = client.Create(ctx, key, refused)
	require.True(t, customers.IsInvalidRequest(err), "%v", err)
	again, _ := customers.AsError(err)
	assert.Equal(t, first, again)

	// Corrected under the same key, it is refused as a reuse of the key.
	_, err = client.Create(ctx, key, &customers.CustomerInput{Capabilities: requestedCaps, Compliance: ip})
	require.True(t, customers.HasType(err, customers.ErrorTypeIdempotency), "%v", err)
	assert.Empty(t, fake.CustomerIDs())

	// Under a new key it goes through.
	_, err = client.Create(ctx, uuid.NewString(), &customers.CustomerInput{Capabilities: requestedCaps, Compliance: ip})
	require.NoError(t, err)

	// An update refused under a key is replayed the same way.
	id := requested(t, client)
	updateKey := uuid.NewString()
	badSsn := &customers.CustomerInput{Individual: &customers.IndividualInput{FullSsn: "000000000"}, Compliance: ip}
	_, err = client.Update(ctx, id, updateKey, badSsn)
	require.True(t, customers.IsInvalidRequest(err), "%v", err)
	first, _ = customers.AsError(err)
	_, err = client.Update(ctx, id, updateKey, badSsn)
	again, _ = customers.AsError(err)
	assert.Equal(t, first, again)
	_, err = client.Update(ctx, id, updateKey, &customers.CustomerInput{Individual: &customers.IndividualInput{FullSsn: customerstest.SsnApprove}, Compliance: ip})
	require.True(t, customers.HasType(err, customers.ErrorTypeIdempotency), "%v", err)

	// An injected failure is not recorded: the same request afterwards is
	// handled afresh.
	failedKey := uuid.NewString()
	fake.FailNext(1, http.StatusServiceUnavailable)
	_, err = client.Create(ctx, failedKey, nil)
	require.True(t, customers.IsRetryable(err), "%v", err)
	created, err := client.Create(ctx, failedKey, nil)
	require.NoError(t, err)
	assert.Contains(t, fake.CustomerIDs(), created.ID)

	// Nor is a queued refusal: an update refused by RefuseNextUpdate under a
	// key goes through when sent again under it.
	refusedKey := uuid.NewString()
	fake.RefuseNextUpdate()
	_, err = client.Update(ctx, id, refusedKey, &customers.CustomerInput{Individual: &customers.IndividualInput{FirstName: "Jane"}, Compliance: ip})
	require.True(t, customers.IsInvalidRequest(err), "%v", err)
	_, err = client.Update(ctx, id, refusedKey, &customers.CustomerInput{Individual: &customers.IndividualInput{FirstName: "Jane"}, Compliance: ip})
	require.NoError(t, err)
}

func TestFake_UpdateInjections(t *testing.T) {
	fake, client := newFake(t)
	ctx := context.Background()
	id := requested(t, client)
	name := func(first string) *customers.CustomerInput {
		return &customers.CustomerInput{Individual: &customers.IndividualInput{FirstName: first}, Compliance: ip}
	}

	// An update failure leaves reads, creates and deletes untouched and
	// fails the next update before it is applied.
	fake.FailNextUpdate(1, http.StatusServiceUnavailable)
	_, err := client.Get(ctx, id)
	require.NoError(t, err)
	other, err := client.Create(ctx, uuid.NewString(), nil)
	require.NoError(t, err)
	require.NoError(t, client.Delete(ctx, other.ID))
	_, err = client.Update(ctx, id, "", name("Janet"))
	require.True(t, customers.HasType(err, "service_unavailable"), "%v", err)
	snap, _ := fake.Inspect(id)
	assert.Empty(t, snap.Individual.FirstName)

	// After apply: the update lands and its response is lost.
	fake.FailNextUpdateAfterApply(1, http.StatusInternalServerError)
	_, err = client.Get(ctx, id)
	require.NoError(t, err)
	_, err = client.Update(ctx, id, "", name("Janet"))
	require.True(t, customers.IsRetryable(err), "%v", err)
	snap, _ = fake.Inspect(id)
	assert.Equal(t, "Janet", snap.Individual.FirstName)

	// The queue keeps its order: a failure of every request at the head is
	// taken by the next request, whatever it is, and an update failure
	// behind it waits for an update.
	fake.FailNext(1, http.StatusBadGateway)
	fake.FailNextUpdate(1, http.StatusTooManyRequests)
	_, err = client.Get(ctx, id)
	require.True(t, customers.HasType(err, "bad_gateway"), "%v", err)
	_, err = client.Get(ctx, id)
	require.NoError(t, err)
	_, err = client.Update(ctx, id, "", name("Jane"))
	require.True(t, customers.IsRateLimited(err), "%v", err)
	_, err = client.Update(ctx, id, "", name("Jane"))
	require.NoError(t, err)

	// A refusal applies nothing and quotes what the update submitted.
	fake.RefuseNextUpdate()
	_, err = client.Update(ctx, id, "", &customers.CustomerInput{
		Individual: &customers.IndividualInput{FirstName: "Quexley", FullSsn: "912-34-5678"},
		Compliance: ip,
	})
	require.True(t, customers.IsInvalidRequest(err), "%v", err)
	e, _ := customers.AsError(err)
	assert.Equal(t, http.StatusBadRequest, e.StatusCode)
	for _, quoted := range []string{"Quexley", "912-34-5678", "203.0.113.42"} {
		assert.Contains(t, e.Message, quoted)
	}
	snap, _ = fake.Inspect(id)
	assert.Equal(t, "Jane", snap.Individual.FirstName)
	assert.Empty(t, snap.Individual.FullSsn)

	// A block refuses the update 403 forbidden, applies nothing, and leaves
	// the customer blocked.
	fake.BlockOnNextUpdate()
	c, err := client.Get(ctx, id)
	require.NoError(t, err)
	assertCaps(t, c, customers.CapabilityPending)
	_, err = client.Update(ctx, id, "", name("Janet"))
	require.True(t, customers.HasType(err, "forbidden"), "%v", err)
	e, _ = customers.AsError(err)
	assert.Equal(t, http.StatusForbidden, e.StatusCode)
	assert.True(t, customers.IsClientError(err))
	snap, _ = fake.Inspect(id)
	assert.True(t, snap.Blocked)
	assert.Equal(t, "Jane", snap.Individual.FirstName)
	c, err = client.Get(ctx, id)
	require.NoError(t, err)
	assertCaps(t, c, customers.CapabilityInactive)
}

func TestFake_CapabilitiesNeedRequesterIP(t *testing.T) {
	fake, client := newFake(t)
	ctx := context.Background()

	_, err := client.Create(ctx, uuid.NewString(), &customers.CustomerInput{Capabilities: requestedCaps})
	require.True(t, customers.IsInvalidRequest(err), "%v", err)
	assert.Empty(t, fake.CustomerIDs())

	c, err := client.Create(ctx, uuid.NewString(), nil)
	require.NoError(t, err)
	_, err = client.Update(ctx, c.ID, "", &customers.CustomerInput{Capabilities: requestedCaps})
	require.True(t, customers.IsInvalidRequest(err), "%v", err)

	id := requested(t, client)
	_, err = client.Update(ctx, id, "", &customers.CustomerInput{Individual: &customers.IndividualInput{FirstName: "Jane"}})
	require.True(t, customers.IsInvalidRequest(err), "a customer with capabilities needs the IP on every write: %v", err)
}

func TestFake_RequirementsBeforeAddress(t *testing.T) {
	_, client := newFake(t)
	ctx := context.Background()

	c, err := client.Create(ctx, uuid.NewString(), &customers.CustomerInput{Capabilities: requestedCaps, Compliance: ip})
	require.NoError(t, err)
	assertCaps(t, c, customers.CapabilityPending)

	assert.Equal(t, with(identityWith(customers.RequirementDue), map[customers.RequirementKey]customers.RequirementStatus{
		"purposeOfAccount": customers.RequirementDue,
		"sourceOfFunds":    customers.RequirementDue,
		"taxAttestation":   customers.RequirementDue,
	}), statuses(c))
	require.Len(t, c.Requirements, 14)
	for key, r := range c.Requirements {
		assert.Equal(t, []customers.CapabilityName{customers.CapabilityCustodyFiat, customers.CapabilityTransferFiat}, r.Impact, key)
	}
	assert.Equal(t, []customers.TaxForm{customers.TaxFormUsW9}, c.Requirements[customers.RequirementTaxAttestation].TaxForms)
	assert.NotContains(t, c.Requirements, customers.RequirementTos)

	// No version is required before the address country is in.
	_, err = client.Update(ctx, c.ID, "", &customers.CustomerInput{TosAcceptances: tos("en"), Compliance: ip})
	require.True(t, customers.IsInvalidRequest(err), "%v", err)
	e, _ := customers.AsError(err)
	assert.Contains(t, e.Message, "(required: [])")
}

func TestFake_ApprovePath(t *testing.T) {
	fake, client := newFake(t)
	ctx := context.Background()
	id := requested(t, client)

	c, err := client.Update(ctx, id, "", &customers.CustomerInput{Individual: identity(customerstest.SsnApprove), TaxAttestations: w9(), Compliance: ip})
	require.NoError(t, err)
	assertCaps(t, c, customers.CapabilityPending)
	assert.Equal(t, with(identityWith(customers.RequirementPending), map[customers.RequirementKey]customers.RequirementStatus{
		"tos": customers.RequirementDue,
	}), statuses(c))
	assert.Equal(t, []customers.TermsOfService{{
		VersionID: customerstest.TosVersionID,
		Languages: []string{"en"},
		URL:       "https://www.coinbase.com/legal/user-agreements-third-party-provider",
	}}, c.Requirements[customers.RequirementTos].TosVersions)
	assert.Equal(t, &customers.Individual{FirstName: "Jane", LastName: "Doe", Email: "jane.doe@example.com"}, c.Individual)

	// Nothing completes until the test says so.
	c, err = client.Get(ctx, id)
	require.NoError(t, err)
	assert.Equal(t, customers.RequirementPending, c.Requirements[customers.RequirementFullSsn].Status)

	assert.Equal(t, 1, fake.CompleteReviews())
	assert.Equal(t, 0, fake.CompleteReviews())
	c, err = client.Get(ctx, id)
	require.NoError(t, err)
	assertCaps(t, c, customers.CapabilityPending)
	assert.Equal(t, map[customers.RequirementKey]customers.RequirementStatus{"tos": customers.RequirementDue}, statuses(c))

	c, err = client.Update(ctx, id, "", &customers.CustomerInput{TosAcceptances: tos("en"), Compliance: ip})
	require.NoError(t, err)
	assertCaps(t, c, customers.CapabilityActive)
	assert.Nil(t, c.Requirements)

	// Re-accepting an accepted version changes nothing.
	c, err = client.Update(ctx, id, "", &customers.CustomerInput{TosAcceptances: tos("en"), Compliance: ip})
	require.NoError(t, err)
	assertCaps(t, c, customers.CapabilityActive)

	// In a language it is not published in, another case of one included, the
	// re-acceptance is refused like a first one.
	for _, language := range []string{"EN", "fr"} {
		_, err = client.Update(ctx, id, "", &customers.CustomerInput{TosAcceptances: tos(language), Compliance: ip})
		require.True(t, customers.IsUnsupportedTosLanguage(err), "%s: %v", language, err)
	}

	// The write-only values are held, never returned.
	snap, ok := fake.Inspect(id)
	require.True(t, ok)
	assert.Equal(t, customerstest.SsnApprove, snap.Individual.FullSsn)
	require.NotNil(t, snap.Individual.Address)
	require.NotNil(t, snap.Individual.Address.Line2)
	assert.Equal(t, "", *snap.Individual.Address.Line2)
	assert.NotNil(t, snap.W9)
	assert.Contains(t, snap.TosAccepted, customerstest.TosVersionID)
	assert.Equal(t, "203.0.113.42", snap.RequesterIPAddress)
}

func TestFake_TosRefusals(t *testing.T) {
	_, client := newFake(t)
	ctx := context.Background()
	id := requested(t, client)
	_, err := client.Update(ctx, id, "", &customers.CustomerInput{Individual: identity(customerstest.SsnApprove), Compliance: ip})
	require.NoError(t, err)

	_, err = client.Update(ctx, id, "", &customers.CustomerInput{
		TosAcceptances: []customers.TosAcceptance{{VersionID: "us_individual_2099-01-01", Language: "en", AcceptedAt: fakeNow}},
		Compliance:     ip,
	})
	require.True(t, customers.IsInvalidRequest(err), "%v", err)
	e, _ := customers.AsError(err)
	assert.Equal(t, `validation failed: terms of service version is not required for this customer: version "us_individual_2099-01-01" is not required for this customer's jurisdiction, customer type, and requested capabilities (required: [us_individual_2026-05-29])`, e.Message)

	_, err = client.Update(ctx, id, "", &customers.CustomerInput{TosAcceptances: tos("fr"), Compliance: ip})
	require.True(t, customers.IsUnsupportedTosLanguage(err), "%v", err)
	assert.True(t, customers.IsClientError(err))

	_, err = client.Update(ctx, id, "", &customers.CustomerInput{
		TosAcceptances: []customers.TosAcceptance{{VersionID: customerstest.TosVersionID, Language: "en", AcceptedAt: fakeNow.Add(time.Hour)}},
		Compliance:     ip,
	})
	require.True(t, customers.IsInvalidRequest(err), "%v", err)

	c, err := client.Get(ctx, id)
	require.NoError(t, err)
	assert.Contains(t, c.Requirements, customers.RequirementTos, "a refused write changes nothing")
}

func TestFake_SetAddressAndAcceptInOneWrite(t *testing.T) {
	fake, client := newFake(t)
	ctx := context.Background()
	id := requested(t, client)

	c, err := client.Update(ctx, id, "", &customers.CustomerInput{
		Individual: identity(customerstest.SsnApprove), TaxAttestations: w9(), TosAcceptances: tos("en"), Compliance: ip,
	})
	require.NoError(t, err)
	assert.Equal(t, identityWith(customers.RequirementPending), statuses(c))

	fake.CompleteReviews()
	c, err = client.Get(ctx, id)
	require.NoError(t, err)
	assertCaps(t, c, customers.CapabilityActive)
}

func TestFake_DeclineThenResubmit(t *testing.T) {
	fake, client := newFake(t)
	ctx := context.Background()
	id := requested(t, client)

	_, err := client.Update(ctx, id, "", &customers.CustomerInput{
		Individual: identity(customerstest.SsnDecline), TaxAttestations: w9(), TosAcceptances: tos("en"), Compliance: ip,
	})
	require.NoError(t, err)
	assert.True(t, fake.CompleteReview(id))

	c, err := client.Get(ctx, id)
	require.NoError(t, err)
	assertCaps(t, c, customers.CapabilityPending)
	assert.Equal(t, identityWith(customers.RequirementRejected), statuses(c))

	// A corrected SSN voids the W-9 unless a new one comes with it.
	c, err = client.Update(ctx, id, "", &customers.CustomerInput{Individual: &customers.IndividualInput{FullSsn: customerstest.SsnApprove}, TaxAttestations: w9(), Compliance: ip})
	require.NoError(t, err)
	assert.Equal(t, identityWith(customers.RequirementPending), statuses(c))

	assert.True(t, fake.CompleteReview(id))
	c, err = client.Get(ctx, id)
	require.NoError(t, err)
	assertCaps(t, c, customers.CapabilityActive)
}

func TestFake_Block(t *testing.T) {
	fake, client := newFake(t)
	ctx := context.Background()
	id := requested(t, client)

	c, err := client.Update(ctx, id, "", &customers.CustomerInput{Individual: identity(customerstest.SsnBlock), Compliance: ip})
	require.NoError(t, err)
	assertCaps(t, c, customers.CapabilityInactive)
	// Identity requirements go; terms and the W-9 stay listed while
	// outstanding, as the Sandbox shows them.
	assert.Equal(t, map[customers.RequirementKey]customers.RequirementStatus{
		"tos": customers.RequirementDue, "taxAttestation": customers.RequirementDue,
	}, statuses(c))

	assert.False(t, fake.CompleteReview(id))

	// Terminal: an approve value afterwards lifts nothing.
	c, err = client.Update(ctx, id, "", &customers.CustomerInput{
		Individual: &customers.IndividualInput{FullSsn: customerstest.SsnApprove}, TaxAttestations: w9(), TosAcceptances: tos("en"), Compliance: ip,
	})
	require.NoError(t, err)
	assertCaps(t, c, customers.CapabilityInactive)
	assert.Nil(t, c.Requirements)
}

func TestFake_AgeOutOfRange(t *testing.T) {
	for name, dob := range map[string]customers.DateOfBirth{
		"17":  {Day: "12", Month: "11", Year: "2008"},
		"120": {Day: "11", Month: "11", Year: "1906"},
	} {
		t.Run(name, func(t *testing.T) {
			_, client := newFake(t)
			id := requested(t, client)
			c, err := client.Update(context.Background(), id, "", &customers.CustomerInput{
				Individual: &customers.IndividualInput{DateOfBirth: &dob}, Compliance: ip,
			})
			require.NoError(t, err)
			assertCaps(t, c, customers.CapabilityInactive)
		})
	}

	_, client := newFake(t)
	id := requested(t, client)
	c, err := client.Update(context.Background(), id, "", &customers.CustomerInput{
		Individual: &customers.IndividualInput{DateOfBirth: &customers.DateOfBirth{Day: "11", Month: "11", Year: "2008"}}, Compliance: ip,
	})
	require.NoError(t, err)
	assertCaps(t, c, customers.CapabilityPending)
}

func TestFake_W9Voiding(t *testing.T) {
	fake, client := newFake(t)
	ctx := context.Background()
	id := requested(t, client)

	_, err := client.Update(ctx, id, "", &customers.CustomerInput{
		Individual: identity(customerstest.SsnApprove), TaxAttestations: w9(), TosAcceptances: tos("en"), Compliance: ip,
	})
	require.NoError(t, err)
	fake.CompleteReviews()

	// Identical values void nothing.
	c, err := client.Update(ctx, id, "", &customers.CustomerInput{Individual: &customers.IndividualInput{FirstName: "Jane", FullSsn: customerstest.SsnApprove}, Compliance: ip})
	require.NoError(t, err)
	assertCaps(t, c, customers.CapabilityActive)

	// A change with a new W-9 in the same write voids nothing.
	c, err = client.Update(ctx, id, "", &customers.CustomerInput{Individual: &customers.IndividualInput{LastName: "Doer"}, TaxAttestations: w9(), Compliance: ip})
	require.NoError(t, err)
	assertCaps(t, c, customers.CapabilityActive)

	// A change alone voids it.
	for _, change := range []*customers.IndividualInput{{FirstName: "Janet"}, {LastName: "Doe"}, {FullSsn: "000-00-0009"}} {
		c, err = client.Update(ctx, id, "", &customers.CustomerInput{Individual: change, Compliance: ip})
		require.NoError(t, err)
		assertCaps(t, c, customers.CapabilityPending)
		assert.Equal(t, map[customers.RequirementKey]customers.RequirementStatus{"taxAttestation": customers.RequirementDue}, statuses(c))

		// A W-9 alone restores it.
		c, err = client.Update(ctx, id, "", &customers.CustomerInput{TaxAttestations: w9(), Compliance: ip})
		require.NoError(t, err)
		assertCaps(t, c, customers.CapabilityActive)
	}

	// Fields the W-9 does not certify void nothing.
	c, err = client.Update(ctx, id, "", &customers.CustomerInput{Individual: &customers.IndividualInput{Email: "other@example.com"}, Compliance: ip})
	require.NoError(t, err)
	assertCaps(t, c, customers.CapabilityActive)
}

func TestFake_SchemaValidation(t *testing.T) {
	_, client := newFake(t)
	ctx := context.Background()
	id := requested(t, client)

	for _, tc := range []struct {
		individual *customers.IndividualInput
		wantField  string
		// wantQuoted is the refused value, which the message quotes.
		wantQuoted string
	}{
		{&customers.IndividualInput{Address: &customers.AddressInput{CountryCode: "CA"}}, "individual.address.countryCode", "CA"},
		{&customers.IndividualInput{Address: &customers.AddressInput{State: "ma"}}, "individual.address.state", "ma"},
		{&customers.IndividualInput{Address: &customers.AddressInput{Line1: strings.Repeat("a", 36)}}, "individual.address.line1", strings.Repeat("a", 36)},
		{&customers.IndividualInput{Address: &customers.AddressInput{Line2: ptr(strings.Repeat("a", 36))}}, "individual.address.line2", strings.Repeat("a", 36)},
		{&customers.IndividualInput{Address: &customers.AddressInput{City: strings.Repeat("a", 41)}}, "individual.address.city", strings.Repeat("a", 41)},
		{&customers.IndividualInput{FirstName: strings.Repeat("a", 61)}, "individual.firstName", strings.Repeat("a", 61)},
		{&customers.IndividualInput{LastName: strings.Repeat("a", 61)}, "individual.lastName", strings.Repeat("a", 61)},
		{&customers.IndividualInput{Email: strings.Repeat("a", 243) + "@example.com"}, "individual.email", strings.Repeat("a", 243) + "@example.com"},
		{&customers.IndividualInput{FullSsn: "000000000"}, "individual.fullSsn", "000000000"},
		{&customers.IndividualInput{PhoneNumber: "555-1234"}, "individual.phoneNumber", "555-1234"},
		{&customers.IndividualInput{PurposeOfAccount: "laundering"}, "individual.purposeOfAccount", "laundering"},
		{&customers.IndividualInput{DateOfBirth: &customers.DateOfBirth{Day: "32", Month: "1", Year: "1990"}}, "individual.dateOfBirth.day", "32"},
	} {
		t.Run(tc.wantField, func(t *testing.T) {
			_, err := client.Update(ctx, id, "", &customers.CustomerInput{Individual: tc.individual, Compliance: ip})
			require.True(t, customers.IsInvalidRequest(err), "%v", err)
			e, _ := customers.AsError(err)
			assert.True(t, strings.HasPrefix(e.Message, tc.wantField+":"), e.Message)
			assert.Contains(t, e.Message, `"`+tc.wantQuoted+`"`)
		})
	}

	// At the limits passes.
	_, err := client.Update(ctx, id, "", &customers.CustomerInput{
		Individual: &customers.IndividualInput{
			FirstName: strings.Repeat("a", 60),
			Email:     strings.Repeat("a", 242) + "@example.com",
			Address:   &customers.AddressInput{Line1: strings.Repeat("a", 35), City: strings.Repeat("a", 40), State: "MA"},
		},
		Compliance: ip,
	})
	require.NoError(t, err)
}

func TestFake_Delete(t *testing.T) {
	fake, client := newFake(t)
	ctx := context.Background()
	c, err := client.Create(ctx, uuid.NewString(), nil)
	require.NoError(t, err)

	require.NoError(t, client.Delete(ctx, c.ID))
	_, err = client.Get(ctx, c.ID)
	require.True(t, customers.IsNotFound(err), "%v", err)
	_, err = client.Update(ctx, c.ID, "", &customers.CustomerInput{})
	require.True(t, customers.IsNotFound(err), "%v", err)
	require.NoError(t, client.Delete(ctx, c.ID), "deleting again succeeds")
	assert.Empty(t, fake.CustomerIDs())

	err = client.Delete(ctx, "customer_11111111-1111-4111-8111-111111111111")
	require.True(t, customers.IsNotFound(err), "%v", err)
}

func TestFake_FailureInjection(t *testing.T) {
	fake, client := newFake(t)
	ctx := context.Background()

	fake.FailNext(2, http.StatusBadGateway)
	for i := 0; i < 2; i++ {
		_, err := client.Create(ctx, uuid.NewString(), nil)
		require.True(t, customers.IsRetryable(err), "%v", err)
		assert.True(t, customers.HasType(err, "bad_gateway"))
	}
	assert.Empty(t, fake.CustomerIDs(), "a failure before apply changes nothing")

	fake.FailNext(1, http.StatusTooManyRequests)
	_, err := client.Create(ctx, uuid.NewString(), nil)
	require.True(t, customers.IsRateLimited(err), "%v", err)

	// A create that lands but loses its response, retried under its key,
	// returns the customer it made.
	key := uuid.NewString()
	fake.FailNextAfterApply(1, http.StatusServiceUnavailable)
	_, err = client.Create(ctx, key, nil)
	require.True(t, customers.IsRetryable(err), "%v", err)
	require.Len(t, fake.CustomerIDs(), 1)

	c, err := client.Create(ctx, key, nil)
	require.NoError(t, err)
	assert.Equal(t, fake.CustomerIDs(), []string{c.ID})
	assert.Equal(t, 5, fake.Requests())
}

func TestFake_RefusesBadSignature(t *testing.T) {
	fake, _ := newFake(t)
	other, err := customers.NewClient(fake.HTTPClient(), fake.BaseURL(), fake.KeyID(), testPrivateKey())
	require.NoError(t, err)
	_, err = other.Create(context.Background(), uuid.NewString(), nil)
	require.True(t, customers.HasType(err, customers.ErrorTypeUnauthorized), "%v", err)
}

func TestFake_Deterministic(t *testing.T) {
	ids := func() []string {
		fake, client := newFake(t)
		for i := 0; i < 3; i++ {
			_, err := client.Create(context.Background(), uuid.NewString(), nil)
			require.NoError(t, err)
		}
		ids := fake.CustomerIDs()
		sort.Strings(ids)
		return ids
	}
	assert.Equal(t, ids(), ids())
}
