package customers_test

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/code-payments/flipcash2-server/cdp/customers"
	"github.com/code-payments/flipcash2-server/cdp/customers/customerstest"
)

// observation is what one step of the main path saw, in a form that the fake
// and the Sandbox can be compared on: capability statuses, requirement
// statuses (identity fields still pending dropped, since the Sandbox may
// finish reviewing them before a write's response returns), the Terms of
// Service versions listed, and the error type of a refused step.
type observation struct {
	Step         string
	Capabilities map[customers.CapabilityName]customers.CapabilityStatus
	Requirements map[customers.RequirementKey]customers.RequirementStatus
	TosVersions  []string
	ErrorType    customers.ErrorType
}

func observe(step string, c *customers.Customer) observation {
	o := observation{Step: step}
	if c.Capabilities != nil {
		o.Capabilities = map[customers.CapabilityName]customers.CapabilityStatus{}
		for name, capability := range c.Capabilities {
			o.Capabilities[name] = capability.Status
		}
	}
	for key, r := range c.Requirements {
		if r.Status == customers.RequirementPending && isIdentityKey(key) {
			continue
		}
		if o.Requirements == nil {
			o.Requirements = map[customers.RequirementKey]customers.RequirementStatus{}
		}
		o.Requirements[key] = r.Status
		for _, v := range r.TosVersions {
			o.TosVersions = append(o.TosVersions, v.VersionID)
		}
	}
	return o
}

func observeError(t *testing.T, step string, err error) observation {
	t.Helper()
	e, ok := customers.AsError(err)
	require.True(t, ok, "%s: want a Coinbase error, got %v", step, err)
	return observation{Step: step, ErrorType: e.Type}
}

func isIdentityKey(key customers.RequirementKey) bool {
	for _, k := range identityKeys {
		if k == key {
			return true
		}
	}
	return false
}

// mainPath runs the path the server will take a user down, asserting what
// it expects at each step, and returns what it observed. awaitReview waits
// for (or, against the fake, completes) the identity review and returns the
// customer after it. The customer is deleted at the end, and again in
// cleanup should the path stop early.
func mainPath(t *testing.T, client *customers.Client, awaitReview func(ctx context.Context, customerID string) *customers.Customer) []observation {
	ctx := context.Background()
	var out []observation
	ip := &customers.Compliance{RequesterIPAddress: "203.0.113.42"}
	acceptedAt := time.Now().Add(-5 * time.Second)

	c, err := client.Create(ctx, uuid.NewString(), nil)
	require.NoError(t, err)
	id := c.ID
	t.Logf("created customer %s", id)
	t.Cleanup(func() {
		if err := client.Delete(context.Background(), id); err != nil && !customers.IsNotFound(err) {
			t.Errorf("cleanup: deleting %s: %v", id, err)
		}
	})
	assert.Nil(t, c.Capabilities)
	assert.Nil(t, c.Requirements)
	out = append(out, observe("create", c))

	_, err = client.Update(ctx, id, "", &customers.CustomerInput{Capabilities: requestedCaps})
	require.True(t, customers.IsInvalidRequest(err), "%v", err)
	out = append(out, observeError(t, "request without ip", err))

	c, err = client.Update(ctx, id, "", &customers.CustomerInput{Capabilities: requestedCaps, Compliance: ip})
	require.NoError(t, err)
	assertCaps(t, c, customers.CapabilityPending)
	assert.Len(t, c.Requirements, 14)
	assert.NotContains(t, c.Requirements, customers.RequirementTos)
	out = append(out, observe("request", c))

	tosEn := []customers.TosAcceptance{{VersionID: customerstest.TosVersionID, Language: "en", AcceptedAt: acceptedAt}}
	_, err = client.Update(ctx, id, "", &customers.CustomerInput{TosAcceptances: tosEn, Compliance: ip})
	require.True(t, customers.IsInvalidRequest(err), "%v", err)
	out = append(out, observeError(t, "accept before address", err))

	c, err = client.Update(ctx, id, "", &customers.CustomerInput{
		Individual: identity(customerstest.SsnApprove),
		TaxAttestations: []customers.TaxAttestation{{UsW9: &customers.UsW9Attestation{
			IsExemptBackupWithholding: false,
			EdeliveryConsent:          true,
			AcceptedAt:                acceptedAt,
		}}},
		Compliance: ip,
	})
	require.NoError(t, err)
	assertCaps(t, c, customers.CapabilityPending)
	require.Contains(t, c.Requirements, customers.RequirementTos)
	assert.Equal(t, customers.RequirementDue, c.Requirements[customers.RequirementTos].Status)
	assert.Equal(t, &customers.Individual{FirstName: "Jane", LastName: "Doe", Email: "jane.doe@example.com"}, c.Individual)
	out = append(out, observe("submit", c))

	_, err = client.Update(ctx, id, "", &customers.CustomerInput{
		TosAcceptances: []customers.TosAcceptance{{VersionID: customerstest.TosVersionID, Language: "fr", AcceptedAt: acceptedAt}},
		Compliance:     ip,
	})
	require.True(t, customers.IsUnsupportedTosLanguage(err), "%v", err)
	out = append(out, observeError(t, "accept in an unpublished language", err))

	c = awaitReview(ctx, id)
	assertCaps(t, c, customers.CapabilityPending)
	assert.Equal(t, map[customers.RequirementKey]customers.RequirementStatus{"tos": customers.RequirementDue}, statuses(c))
	out = append(out, observe("review", c))

	c, err = client.Update(ctx, id, "", &customers.CustomerInput{TosAcceptances: tosEn, Compliance: ip})
	require.NoError(t, err)
	assertCaps(t, c, customers.CapabilityActive)
	assert.Nil(t, c.Requirements)
	out = append(out, observe("accept", c))

	// Re-accepting the accepted version: as listed it changes nothing, and in
	// another case of its language it is refused like a first acceptance.
	c, err = client.Update(ctx, id, "", &customers.CustomerInput{TosAcceptances: tosEn, Compliance: ip})
	require.NoError(t, err)
	assertCaps(t, c, customers.CapabilityActive)
	assert.Nil(t, c.Requirements)
	out = append(out, observe("accept again", c))

	_, err = client.Update(ctx, id, "", &customers.CustomerInput{
		TosAcceptances: []customers.TosAcceptance{{VersionID: customerstest.TosVersionID, Language: "EN", AcceptedAt: acceptedAt}},
		Compliance:     ip,
	})
	require.True(t, customers.IsUnsupportedTosLanguage(err), "%v", err)
	out = append(out, observeError(t, "accept again in another case", err))

	require.NoError(t, client.Delete(ctx, id))
	_, err = client.Get(ctx, id)
	require.True(t, customers.IsNotFound(err), "%v", err)
	require.NoError(t, client.Delete(ctx, id))
	out = append(out, observeError(t, "read after delete", err))

	return out
}

func runMainPathOnFake(t *testing.T) []observation {
	fake := customerstest.NewServer()
	t.Cleanup(fake.Close)
	client := fake.Client()
	out := mainPath(t, client, func(ctx context.Context, id string) *customers.Customer {
		require.True(t, fake.CompleteReview(id))
		c, err := client.Get(ctx, id)
		require.NoError(t, err)
		return c
	})
	assert.Empty(t, fake.CustomerIDs())
	return out
}

func TestMainPath_Fake(t *testing.T) {
	runMainPathOnFake(t)
}
