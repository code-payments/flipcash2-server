package tests

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/code-payments/flipcash2-server/cluster"
)

// RunProductionTimingTests exercises the cluster at true production defaults —
// 5s heartbeats, 2s polls, 15s liveness, the 7s suspicion floor — rather than
// the scaled-down configs the fast suites use. The point is the production
// *relationships* between the knobs: suspicion patience before displacement,
// failover well inside the liveness window, drains bounded by DrainDeadline,
// and no spurious session shedding under real cadences. Wall clock is tens of
// seconds; gate it behind the integration tag.
func RunProductionTimingTests(t *testing.T, s cluster.Store, teardown func()) {
	testProductionTimings(t, s)
	teardown()
}

func memberCounter(t *testing.T, s cluster.Store, instanceID string) (uint64, bool) {
	records, err := s.GetMembers(context.Background())
	require.NoError(t, err)
	for _, r := range records {
		if r.InstanceID == instanceID {
			return r.HeartbeatCounter, true
		}
	}
	return 0, false
}

func testProductionTimings(t *testing.T, s cluster.Store) {
	t.Run("testProductionTimings", func(t *testing.T) {
		ctx := context.Background()

		// Zero-valued configs resolve to the production defaults.
		a := startNode(t, s, "instance-a", cluster.MembershipConfig{}, cluster.OwnershipConfig{})
		b := startNode(t, s, "instance-b", cluster.MembershipConfig{}, cluster.OwnershipConfig{})
		require.Eventually(t, func() bool {
			return len(a.membership.Live()) == 2 && len(b.membership.Live()) == 2
		}, 15*time.Second, 100*time.Millisecond)

		key := keyRoutedTo(t, b, "instance-a")
		require.NoError(t, a.ownership.Do(ctx, testNamespace, key, doNoop))

		// Sync the crash to just after a heartbeat lands, so the takeover
		// delay measured from the crash reflects the suspicion floor rather
		// than however much staleness had already accrued.
		c0, ok := memberCounter(t, s, "instance-a")
		require.True(t, ok)
		require.Eventually(t, func() bool {
			c, ok := memberCounter(t, s, "instance-a")
			return ok && c != c0
		}, 15*time.Second, 100*time.Millisecond)
		final, ok := memberCounter(t, s, "instance-a")
		require.True(t, ok)

		a.ownership.Stop()
		a.membership.Stop()
		crashedAt := time.Now()

		// File the unreachable report only after b has OBSERVED the corpse's
		// final beat: a beat observed after the report invalidates it (by
		// design), which would silently push this test onto the slow
		// liveness-window path instead of the suspicion path under test.
		require.Eventually(t, func() bool {
			counter, _, seen := b.membership.LivenessInfo("instance-a")
			return seen && counter == final
		}, 10*time.Second, 100*time.Millisecond)
		b.ownership.NoteUnreachable("instance-a")

		// Inside the suspicion floor the survivor stays patient: the holder
		// is presumed alive-but-jittery, so requests still redirect rather
		// than displace.
		time.Sleep(3 * time.Second)
		err := b.ownership.Do(ctx, testNamespace, key, doNoop)
		var notOwner *cluster.NotOwnerError
		require.ErrorAs(t, err, &notOwner)

		// Displacement lands via suspicion — comfortably before the plain
		// 15s liveness window could have produced it.
		var claim *cluster.Claim
		require.Eventually(t, func() bool {
			claim = nil
			err := b.ownership.Do(ctx, testNamespace, key, func(_ context.Context, c *cluster.Claim) error {
				claim = c
				return nil
			})
			return err == nil
		}, 13*time.Second, 250*time.Millisecond)
		elapsed := time.Since(crashedAt)
		require.Greater(t, elapsed, 4*time.Second, "displaced before the suspicion floor allows")
		require.Less(t, elapsed, 13*time.Second, "took the liveness window, not the suspicion path")
		require.Equal(t, "instance-b", claim.OwnerInstanceID)
		require.EqualValues(t, 2, claim.Fence)

		// No spurious churn under production cadences: exactly the acquires
		// and releases this scenario calls for, nothing shed by jitter.
		require.Equal(t, []string{string(key)}, a.acquiredKeys())
		require.Empty(t, a.releasedKeys())
		require.Equal(t, []string{string(key)}, b.acquiredKeys())
		require.Empty(t, b.releasedKeys())

		// The corpse's record would mislead a fresh node's first sight; a
		// peer's GC handles this in production (MemberGCAfter = 150s, too
		// slow for a test) — emulate it so the successor converges promptly.
		require.NoError(t, s.DeleteMember(ctx, "instance-a"))

		// Graceful drain at the production DrainDeadline: prompt, eager, and
		// the claim is vacated for the next owner rather than left to expiry.
		drainStart := time.Now()
		require.NoError(t, b.ownership.Drain(ctx))
		require.Less(t, time.Since(drainStart), 10*time.Second)
		require.Equal(t, []string{string(key)}, b.releasedKeys())
		_, err = s.GetClaim(ctx, testNamespace, key)
		require.ErrorIs(t, err, cluster.ErrClaimNotFound)

		c := startNode(t, s, "instance-c", cluster.MembershipConfig{}, cluster.OwnershipConfig{})
		require.Eventually(t, func() bool {
			claim = nil
			err := c.ownership.Do(ctx, testNamespace, key, func(_ context.Context, cl *cluster.Claim) error {
				claim = cl
				return nil
			})
			return err == nil
		}, 15*time.Second, 250*time.Millisecond)
		require.Equal(t, "instance-c", claim.OwnerInstanceID)
		require.EqualValues(t, 3, claim.Fence)
	})
}
