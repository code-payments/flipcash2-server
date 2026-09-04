package tests

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/code-payments/flipcash2-server/cluster"
)

const testNamespace = "chat"

// RunClusterTests runs the membership/routing/ownership runtime suite against
// a cluster.Store implementation, calling teardown between tests.
func RunClusterTests(t *testing.T, s cluster.Store, teardown func()) {
	for _, tf := range []func(t *testing.T, s cluster.Store){
		testRoutingAgreement,
		testOwnershipAcquireAndRedirect,
		testFailover,
		testSuspicionTakeover,
		testGracefulDrain,
		testParallelDrain,
		testDrainUnderAcquisition,
		testDrainWithConcurrentRelease,
		testDrainAbortAndResume,
		testReleaseAcquireRace,
		testAcquireHookPanic,
		testWarmBeforeServe,
		testRedirectCache,
		testMemberGC,
		testMemberGCDeleteFailure,
		testSessionLostPurge,
		testSessionGapPurge,
		testRebalanceOnJoin,
		testIdleRelease,
		testSubscribeRefcounting,
		testSubscriberResolutionAndLiveness,
		testSubscriberCache,
		testSubscriptionsDrainAndResume,
		testSubscriptionStaleHandleAfterResume,
		testSubscriptionReassertDrainRace,
		testSubscriptionSessionReassert,
		testSubscriptionCorpseRowGC,
	} {
		tf(t, s)
		teardown()
	}
}

// node bundles one simulated server's cluster runtime with hook recorders.
type node struct {
	member        *cluster.Member
	membership    *cluster.Membership
	router        *cluster.Router
	ownership     *cluster.Ownership
	subscriptions *cluster.Subscriptions

	mu       sync.Mutex
	acquired []string
	released []string
}

func (n *node) onAcquired(_ context.Context, key []byte) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.acquired = append(n.acquired, string(key))
}

func (n *node) onReleased(_ context.Context, key []byte) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.released = append(n.released, string(key))
}

func (n *node) acquiredKeys() []string {
	n.mu.Lock()
	defer n.mu.Unlock()
	return append([]string(nil), n.acquired...)
}

func (n *node) releasedKeys() []string {
	n.mu.Lock()
	defer n.mu.Unlock()
	return append([]string(nil), n.released...)
}

// fastMembershipConfig keeps liveness convergence fast enough for tests while
// leaving comfortable margin over scheduler jitter.
func fastMembershipConfig() cluster.MembershipConfig {
	return cluster.MembershipConfig{
		HeartbeatInterval: 25 * time.Millisecond,
		PollInterval:      25 * time.Millisecond,
		LivenessWindow:    400 * time.Millisecond,
		// Pinned above scheduler jitter: at the default (heartbeat + poll =
		// 50ms) a busy CI machine fires spurious session purges that break
		// the suite's exact hook-order assertions. testSessionGapPurge
		// exercises the tight default deliberately.
		SessionGapThreshold: 400 * time.Millisecond,
	}
}

func fastOwnershipConfig() cluster.OwnershipConfig {
	return cluster.OwnershipConfig{
		IdleTTL:         time.Hour, // Idle release is exercised explicitly.
		ReapInterval:    25 * time.Millisecond,
		DrainDeadline:   time.Second,
		SuspicionWindow: 100 * time.Millisecond,
	}
}

func fastSubscriptionsConfig() cluster.SubscriptionsConfig {
	return cluster.SubscriptionsConfig{
		CacheTTL: 25 * time.Millisecond,
		// RowGCAfter is resolved against the liveness window at construction;
		// the corpse-sweep test builds its own runtime with a tight value.
	}
}

func startNode(t *testing.T, s cluster.Store, name string, mCfg cluster.MembershipConfig, oCfg cluster.OwnershipConfig) *node {
	log := zap.NewNop()

	n := &node{
		member: &cluster.Member{
			InstanceID: name,
			Address:    name + ".local:8085",
			Labels:     map[string]string{"role": "all"},
		},
	}
	n.membership = cluster.NewMembership(log, s, n.member, mCfg)
	n.router = cluster.NewRouter(n.membership, nil)
	n.ownership = cluster.NewOwnership(log, n.membership, n.router, s, oCfg)
	n.subscriptions = cluster.NewSubscriptions(log, n.membership, s, fastSubscriptionsConfig())
	n.ownership.RegisterNamespace(testNamespace, cluster.NamespaceHooks{
		OnAcquired: n.onAcquired,
		OnReleased: n.onReleased,
	})

	ctx := context.Background()
	require.NoError(t, n.membership.Start(ctx))
	n.ownership.Start(ctx)

	t.Cleanup(func() {
		n.ownership.Stop()
		n.membership.Stop()
	})

	return n
}

// waitForLiveMembers blocks until the node's live view reaches the expected
// size.
func waitForLiveMembers(t *testing.T, n *node, count int) {
	require.Eventually(t, func() bool {
		return len(n.membership.Live()) == count
	}, 5*time.Second, 10*time.Millisecond)
}

// keyRoutedTo finds a key the node's router assigns to the wanted owner.
func keyRoutedTo(t *testing.T, n *node, ownerInstanceID string) []byte {
	for i := 0; i < 10_000; i++ {
		key := fmt.Appendf(nil, "key-%d", i)
		owner, err := n.router.Owner(context.Background(), testNamespace, key)
		require.NoError(t, err)
		if owner.InstanceID == ownerInstanceID {
			return key
		}
	}
	t.Fatalf("no key routed to %s", ownerInstanceID)
	return nil
}

func doNoop(ctx context.Context, _ *cluster.Claim) error { return nil }

func testRoutingAgreement(t *testing.T, s cluster.Store) {
	t.Run("testRoutingAgreement", func(t *testing.T) {
		ctx := context.Background()

		a := startNode(t, s, "instance-a", fastMembershipConfig(), fastOwnershipConfig())
		b := startNode(t, s, "instance-b", fastMembershipConfig(), fastOwnershipConfig())
		waitForLiveMembers(t, a, 2)
		waitForLiveMembers(t, b, 2)

		perOwner := make(map[string]int)
		for i := 0; i < 128; i++ {
			key := fmt.Appendf(nil, "key-%d", i)

			ownerA, err := a.router.Owner(ctx, testNamespace, key)
			require.NoError(t, err)
			ownerB, err := b.router.Owner(ctx, testNamespace, key)
			require.NoError(t, err)

			// Every server elects the same owner with zero coordination.
			require.Equal(t, ownerA.InstanceID, ownerB.InstanceID)
			perOwner[ownerA.InstanceID]++
		}

		// Both members win a share of the keyspace (all-on-one-side has
		// probability 2^-127).
		require.Positive(t, perOwner["instance-a"])
		require.Positive(t, perOwner["instance-b"])
	})
}

func testOwnershipAcquireAndRedirect(t *testing.T, s cluster.Store) {
	t.Run("testOwnershipAcquireAndRedirect", func(t *testing.T) {
		ctx := context.Background()

		a := startNode(t, s, "instance-a", fastMembershipConfig(), fastOwnershipConfig())
		b := startNode(t, s, "instance-b", fastMembershipConfig(), fastOwnershipConfig())
		waitForLiveMembers(t, a, 2)
		waitForLiveMembers(t, b, 2)

		key := keyRoutedTo(t, a, "instance-a")

		var observed *cluster.Claim
		require.NoError(t, a.ownership.Do(ctx, testNamespace, key, func(_ context.Context, claim *cluster.Claim) error {
			observed = claim
			return nil
		}))
		require.Equal(t, "instance-a", observed.OwnerInstanceID)
		require.EqualValues(t, 1, observed.Fence)
		require.Equal(t, []string{string(key)}, a.acquiredKeys())

		// Repeat Do calls ride the owned fast path without re-firing hooks.
		require.NoError(t, a.ownership.Do(ctx, testNamespace, key, doNoop))
		require.Equal(t, []string{string(key)}, a.acquiredKeys())

		// The non-owner is redirected to the claim holder.
		err := b.ownership.Do(ctx, testNamespace, key, doNoop)
		var notOwner *cluster.NotOwnerError
		require.ErrorAs(t, err, &notOwner)
		require.NotNil(t, notOwner.Redirect)
		require.Equal(t, "instance-a", notOwner.Redirect.InstanceID)
		require.Equal(t, "instance-a.local:8085", notOwner.Redirect.Address)
		require.Empty(t, b.acquiredKeys())
	})
}

func testFailover(t *testing.T, s cluster.Store) {
	t.Run("testFailover", func(t *testing.T) {
		ctx := context.Background()

		a := startNode(t, s, "instance-a", fastMembershipConfig(), fastOwnershipConfig())
		b := startNode(t, s, "instance-b", fastMembershipConfig(), fastOwnershipConfig())
		waitForLiveMembers(t, a, 2)
		waitForLiveMembers(t, b, 2)

		key := keyRoutedTo(t, a, "instance-a")
		require.NoError(t, a.ownership.Do(ctx, testNamespace, key, doNoop))

		// A crashes: heartbeats stop, claims are left behind.
		a.ownership.Stop()
		a.membership.Stop()

		// The zombie must stop trusting its owned claims once its own
		// heartbeats have gone stale.
		require.Eventually(t, func() bool {
			err := a.ownership.Do(ctx, testNamespace, key, doNoop)
			var notOwner *cluster.NotOwnerError
			return errors.As(err, &notOwner)
		}, 5*time.Second, 10*time.Millisecond)

		// B observes the death and takes over on demand, with the fence
		// advanced past the corpse's.
		require.Eventually(t, func() bool {
			var claim *cluster.Claim
			err := b.ownership.Do(ctx, testNamespace, key, func(_ context.Context, c *cluster.Claim) error {
				claim = c
				return nil
			})
			return err == nil && claim.OwnerInstanceID == "instance-b" && claim.Fence == 2
		}, 5*time.Second, 10*time.Millisecond)
		require.Equal(t, []string{string(key)}, b.acquiredKeys())
	})
}

func testSuspicionTakeover(t *testing.T, s cluster.Store) {
	t.Run("testSuspicionTakeover", func(t *testing.T) {
		ctx := context.Background()

		// A long liveness window: suspicion is what must move the claim.
		mCfg := fastMembershipConfig()
		mCfg.LivenessWindow = time.Minute
		mCfg.SelfUnhealthyAfter = time.Minute

		a := startNode(t, s, "instance-a", mCfg, fastOwnershipConfig())
		b := startNode(t, s, "instance-b", mCfg, fastOwnershipConfig())
		waitForLiveMembers(t, a, 2)
		waitForLiveMembers(t, b, 2)

		key := keyRoutedTo(t, a, "instance-a")
		require.NoError(t, a.ownership.Do(ctx, testNamespace, key, doNoop))

		// A crashes but stays inside the liveness window: B still routes to it
		// and redirects there.
		a.ownership.Stop()
		a.membership.Stop()

		err := b.ownership.Do(ctx, testNamespace, key, doNoop)
		var notOwner *cluster.NotOwnerError
		require.ErrorAs(t, err, &notOwner)
		require.NotNil(t, notOwner.Redirect)
		require.Equal(t, "instance-a", notOwner.Redirect.InstanceID)

		// File the unreachable report only after B has OBSERVED the corpse's
		// final beat (as production.go does): a beat observed after the report
		// invalidates it (by design), which would silently push this test onto
		// the one-minute liveness-window path and past the Eventually budget.
		final, ok := memberCounter(t, s, "instance-a")
		require.True(t, ok)
		require.Eventually(t, func() bool {
			counter, _, seen := b.membership.LivenessInfo("instance-a")
			return seen && counter == final
		}, 5*time.Second, 10*time.Millisecond)

		// The caller's forward fails and it reports the member unreachable.
		// Corroborated by a heartbeat stale past the suspicion window, B may
		// now route around A and displace the claim without waiting out the
		// full liveness window.
		b.ownership.NoteUnreachable("instance-a")

		require.Eventually(t, func() bool {
			var claim *cluster.Claim
			err := b.ownership.Do(ctx, testNamespace, key, func(_ context.Context, c *cluster.Claim) error {
				claim = c
				return nil
			})
			return err == nil && claim.OwnerInstanceID == "instance-b" && claim.Fence == 2
		}, 5*time.Second, 10*time.Millisecond)
	})
}

func testGracefulDrain(t *testing.T, s cluster.Store) {
	t.Run("testGracefulDrain", func(t *testing.T) {
		ctx := context.Background()

		a := startNode(t, s, "instance-a", fastMembershipConfig(), fastOwnershipConfig())
		b := startNode(t, s, "instance-b", fastMembershipConfig(), fastOwnershipConfig())
		waitForLiveMembers(t, a, 2)
		waitForLiveMembers(t, b, 2)

		key := keyRoutedTo(t, a, "instance-a")
		require.NoError(t, a.ownership.Do(ctx, testNamespace, key, doNoop))

		require.NoError(t, a.ownership.Drain(ctx))
		require.Equal(t, []string{string(key)}, a.releasedKeys())

		// The claim was released eagerly — no liveness wait for the successor.
		_, err := s.GetClaim(ctx, testNamespace, key)
		require.ErrorIs(t, err, cluster.ErrClaimNotFound)

		// Draining members leave routing candidacy once peers observe the
		// flag, so the key re-routes to B, which acquires vacantly on demand
		// (fence advances without a takeover).
		require.Eventually(t, func() bool {
			var claim *cluster.Claim
			err := b.ownership.Do(ctx, testNamespace, key, func(_ context.Context, c *cluster.Claim) error {
				claim = c
				return nil
			})
			return err == nil && claim.OwnerInstanceID == "instance-b" && claim.Fence == 2
		}, 5*time.Second, 10*time.Millisecond)

		// Deregistration completes the shutdown.
		require.NoError(t, a.membership.Deregister(ctx))
		require.Eventually(t, func() bool {
			return len(b.membership.Live()) == 1
		}, 5*time.Second, 10*time.Millisecond)
	})
}

func testDrainUnderAcquisition(t *testing.T, s cluster.Store) {
	t.Run("testDrainUnderAcquisition", func(t *testing.T) {
		ctx := context.Background()

		a := startNode(t, s, "instance-a", fastMembershipConfig(), fastOwnershipConfig())
		waitForLiveMembers(t, a, 1)

		// Hammer fresh keys (first acquisitions, never the owned fast path)
		// across the drain — the shape of a busy node at shutdown. Racing
		// acquisitions must either be swept by the drain or refused and handed
		// back; none may survive it.
		var (
			mu        sync.Mutex
			attempted [][]byte
		)
		var successes atomic.Uint64
		var stop atomic.Bool
		var wg sync.WaitGroup
		for g := range 8 {
			wg.Go(func() {
				for i := 0; !stop.Load(); i++ {
					key := fmt.Appendf(nil, "drain-key-%d-%d", g, i)
					mu.Lock()
					attempted = append(attempted, key)
					mu.Unlock()

					err := a.ownership.Do(ctx, testNamespace, key, doNoop)
					if err == nil {
						successes.Add(1)
						continue
					}
					var notOwner *cluster.NotOwnerError
					require.ErrorAs(t, err, &notOwner)
				}
			})
		}

		time.Sleep(30 * time.Millisecond)
		require.NoError(t, a.ownership.Drain(ctx))
		stop.Store(true)
		wg.Wait()

		require.Positive(t, successes.Load())
		require.Empty(t, a.ownership.OwnedKeys(testNamespace))

		// The eager-release property Drain promises: every claim vacated
		// before deregistration, none left for liveness expiry.
		for _, key := range attempted {
			_, err := s.GetClaim(ctx, testNamespace, key)
			require.ErrorIs(t, err, cluster.ErrClaimNotFound, "leaked claim for %s", key)
		}
	})
}

func testDrainWithConcurrentRelease(t *testing.T, s cluster.Store) {
	t.Run("testDrainWithConcurrentRelease", func(t *testing.T) {
		ctx := context.Background()

		// Slow releases plus an aggressive idle reaper: the reaper is
		// mid-release of keys while Drain sweeps the same set. Drain must
		// complete (yielding to the concurrent releaser, not spinning), and
		// the release idempotency guard must fire each key's flush exactly
		// once.
		slow := &slowReleaseStore{Store: s, delay: 50 * time.Millisecond}
		oCfg := fastOwnershipConfig()
		oCfg.IdleTTL = time.Millisecond
		oCfg.ReapInterval = 10 * time.Millisecond

		a := startNode(t, slow, "instance-a", fastMembershipConfig(), oCfg)
		waitForLiveMembers(t, a, 1)

		const keyCount = 6
		expected := make([]string, 0, keyCount)
		for i := range keyCount {
			key := fmt.Appendf(nil, "key-%d", i)
			expected = append(expected, string(key))
			require.NoError(t, a.ownership.Do(ctx, testNamespace, key, doNoop))
		}

		// Give the reaper time to be mid-release when the drain starts.
		time.Sleep(20 * time.Millisecond)
		require.NoError(t, a.ownership.Drain(ctx))

		require.Empty(t, a.ownership.OwnedKeys(testNamespace))
		require.ElementsMatch(t, expected, a.releasedKeys())
		for _, key := range expected {
			_, err := s.GetClaim(ctx, testNamespace, []byte(key))
			require.ErrorIs(t, err, cluster.ErrClaimNotFound)
		}
	})
}

func testDrainAbortAndResume(t *testing.T, s cluster.Store) {
	t.Run("testDrainAbortAndResume", func(t *testing.T) {
		ctx := context.Background()

		a := startNode(t, s, "instance-a", fastMembershipConfig(), fastOwnershipConfig())
		waitForLiveMembers(t, a, 1)

		key := []byte("held-key")
		require.NoError(t, a.ownership.Do(ctx, testNamespace, key, doNoop))

		// A drain whose budget is already exhausted aborts with the latch
		// deliberately left in place: still-owned keys keep serving, but no
		// new ownership may be taken.
		canceled, cancel := context.WithCancel(ctx)
		cancel()
		require.Error(t, a.ownership.Drain(canceled))

		require.NoError(t, a.ownership.Do(ctx, testNamespace, key, doNoop))
		err := a.ownership.Do(ctx, testNamespace, []byte("new-key"), doNoop)
		var notOwner *cluster.NotOwnerError
		require.ErrorAs(t, err, &notOwner)

		// Resume is the explicit opt back into service for an aborted
		// shutdown: new acquisitions work again once routing recovers.
		require.NoError(t, a.ownership.Resume(ctx))
		require.Eventually(t, func() bool {
			return a.ownership.Do(ctx, testNamespace, []byte("new-key"), doNoop) == nil
		}, 10*time.Second, 10*time.Millisecond)
	})
}

// gatedAcquireStore lets the test hold an AcquireClaim response after it has
// been evaluated — the network-jitter shape where an acquire computed against
// a not-yet-vacated claim surfaces only after the release completed.
type gatedAcquireStore struct {
	cluster.Store
	gate             atomic.Bool
	acquireEvaluated chan struct{}
	acquireRelease   chan struct{}
}

func (s *gatedAcquireStore) AcquireClaim(ctx context.Context, namespace string, key []byte, self *cluster.Member, takeover *cluster.TakeoverTarget) (*cluster.Claim, error) {
	claim, err := s.Store.AcquireClaim(ctx, namespace, key, self, takeover)
	if s.gate.Load() {
		select {
		case s.acquireEvaluated <- struct{}{}:
		default:
		}
		<-s.acquireRelease
	}
	return claim, err
}

func testReleaseAcquireRace(t *testing.T, s cluster.Store) {
	t.Run("testReleaseAcquireRace", func(t *testing.T) {
		ctx := context.Background()

		// Deterministic orchestration of the phantom-ownership race: an idle
		// release is paused mid-flight (in OnReleased, before the claim is
		// vacated) while a concurrent Do's re-acquire is evaluated against
		// the still-held claim; the release then completes fully before the
		// acquire's response is allowed to surface and register. Local
		// ownership and the claim record must never diverge.
		gated := &gatedAcquireStore{
			Store:            s,
			acquireEvaluated: make(chan struct{}, 1),
			acquireRelease:   make(chan struct{}),
		}
		oCfg := fastOwnershipConfig()
		oCfg.IdleTTL = 50 * time.Millisecond
		oCfg.ReapInterval = 10 * time.Millisecond

		a := startNode(t, gated, "instance-a", fastMembershipConfig(), oCfg)
		waitForLiveMembers(t, a, 1)

		var releaseStarted sync.Once
		started := make(chan struct{})
		hookGate := make(chan struct{})
		a.ownership.RegisterNamespace("race", cluster.NamespaceHooks{
			OnReleased: func(context.Context, []byte) {
				releaseStarted.Do(func() { close(started) })
				<-hookGate
			},
		})

		key := []byte("contested-key")
		require.NoError(t, a.ownership.Do(ctx, "race", key, doNoop))
		gated.gate.Store(true)

		// The idle reaper begins releasing and parks in OnReleased with the
		// claim still held.
		<-started

		resultCh := make(chan error, 1)
		go func() {
			resultCh <- a.ownership.Do(ctx, "race", key, doNoop)
		}()

		// Pre-fix, the racing Do reaches the store while the claim is still
		// held (its evaluation signals here); post-fix it blocks on the key
		// lock until the release completes and only then acquires — either
		// way, let the release finish fully before the acquire's response
		// surfaces.
		select {
		case <-gated.acquireEvaluated:
			close(hookGate)
			require.Eventually(t, func() bool {
				return len(a.ownership.OwnedKeys("race")) == 0
			}, 5*time.Second, time.Millisecond)
		case <-time.After(200 * time.Millisecond):
			close(hookGate)
			select {
			case <-gated.acquireEvaluated:
			case <-time.After(5 * time.Second):
				t.Fatal("racing Do never reached the store")
			}
		}
		close(gated.acquireRelease)

		err := <-resultCh
		if err != nil {
			var notOwner *cluster.NotOwnerError
			require.ErrorAs(t, err, &notOwner)
		}

		// The invariant, checked before any later release can paper over it:
		// a locally owned entry must have a claim behind it. An entry whose
		// claim is vacant is phantom ownership — a peer could win the claim
		// and both would serve.
		owned := len(a.ownership.OwnedKeys("race")) > 0
		_, getErr := s.GetClaim(ctx, "race", key)
		claimHeld := getErr == nil
		if !claimHeld {
			require.ErrorIs(t, getErr, cluster.ErrClaimNotFound)
		}
		require.False(t, owned && !claimHeld, "local ownership with no claim behind it (phantom)")
	})
}

func testAcquireHookPanic(t *testing.T, s cluster.Store) {
	t.Run("testAcquireHookPanic", func(t *testing.T) {
		ctx := context.Background()

		a := startNode(t, s, "instance-a", fastMembershipConfig(), fastOwnershipConfig())
		waitForLiveMembers(t, a, 1)

		// The first OnAcquired parks (so a concurrent Do can pile up behind
		// the warm) and then panics — the recovered-by-interceptor shape.
		// The key must not wedge: waiters unblock, the claim is handed back,
		// and the next demand re-acquires and re-warms cleanly.
		var calls atomic.Int32
		warmStarted := make(chan struct{})
		warmGate := make(chan struct{})
		a.ownership.RegisterNamespace("panicky", cluster.NamespaceHooks{
			OnAcquired: func(context.Context, []byte) {
				if calls.Add(1) == 1 {
					close(warmStarted)
					<-warmGate
					panic("hook boom")
				}
			},
		})

		key := []byte("panic-key")

		firstDone := make(chan any, 1)
		go func() {
			defer func() { firstDone <- recover() }()
			_ = a.ownership.Do(ctx, "panicky", key, doNoop)
		}()
		<-warmStarted

		// A second Do arrives mid-warm and parks behind it.
		secondDone := make(chan error, 1)
		go func() {
			secondDone <- a.ownership.Do(ctx, "panicky", key, doNoop)
		}()

		close(warmGate)

		// The panic must reach the first caller (interceptors recover it in
		// production), and the parked caller must unblock — not hang forever
		// on a warm that will never complete.
		select {
		case recovered := <-firstDone:
			require.NotNil(t, recovered)
		case <-time.After(5 * time.Second):
			t.Fatal("panicking Do never returned")
		}
		select {
		case err := <-secondDone:
			if err != nil {
				var notOwner *cluster.NotOwnerError
				require.ErrorAs(t, err, &notOwner)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("Do parked behind a panicked warm never unblocked (wedged key)")
		}

		// The half-warmed entry was retired and its claim handed back; the
		// parked caller may then have legitimately re-acquired (its hook call
		// no longer panics). Either way, local ownership and the claim record
		// must agree — a retained entry with a vacant claim (or vice versa)
		// would be the wedge's quieter sibling.
		require.Eventually(t, func() bool {
			owned := len(a.ownership.OwnedKeys("panicky")) > 0
			_, err := s.GetClaim(ctx, "panicky", key)
			return owned == (err == nil)
		}, 5*time.Second, 10*time.Millisecond)

		// Demand re-acquires and re-warms cleanly (hook no longer panics).
		require.Eventually(t, func() bool {
			return a.ownership.Do(ctx, "panicky", key, doNoop) == nil
		}, 10*time.Second, 10*time.Millisecond)
		claim, err := s.GetClaim(ctx, "panicky", key)
		require.NoError(t, err)
		require.Equal(t, "instance-a", claim.OwnerInstanceID)
	})
}

func testWarmBeforeServe(t *testing.T, s cluster.Store) {
	t.Run("testWarmBeforeServe", func(t *testing.T) {
		ctx := context.Background()

		a := startNode(t, s, "instance-a", fastMembershipConfig(), fastOwnershipConfig())
		waitForLiveMembers(t, a, 1)

		// A deliberately slow OnAcquired: every Do for the key — including
		// ones racing the first — must observe the warm completed before fn
		// runs, per the NamespaceHooks contract.
		var warmed atomic.Bool
		a.ownership.RegisterNamespace("warm", cluster.NamespaceHooks{
			OnAcquired: func(context.Context, []byte) {
				time.Sleep(150 * time.Millisecond)
				warmed.Store(true)
			},
		})

		key := []byte("warm-key")
		var violations atomic.Int32
		var wg sync.WaitGroup
		for range 4 {
			wg.Go(func() {
				err := a.ownership.Do(ctx, "warm", key, func(context.Context, *cluster.Claim) error {
					if !warmed.Load() {
						violations.Add(1)
					}
					return nil
				})
				require.NoError(t, err)
			})
		}
		wg.Wait()

		require.True(t, warmed.Load())
		require.Zero(t, violations.Load())
	})
}

// claimReadCountingStore counts GetClaim calls, to observe the redirect
// cache's effect on the forward path.
type claimReadCountingStore struct {
	cluster.Store
	claimReads atomic.Uint64
}

func (s *claimReadCountingStore) GetClaim(ctx context.Context, namespace string, key []byte) (*cluster.Claim, error) {
	s.claimReads.Add(1)
	return s.Store.GetClaim(ctx, namespace, key)
}

func testRedirectCache(t *testing.T, s cluster.Store) {
	t.Run("testRedirectCache", func(t *testing.T) {
		ctx := context.Background()

		counting := &claimReadCountingStore{Store: s}
		a := startNode(t, counting, "instance-a", fastMembershipConfig(), fastOwnershipConfig())
		b := startNode(t, counting, "instance-b", fastMembershipConfig(), fastOwnershipConfig())
		waitForLiveMembers(t, a, 2)
		waitForLiveMembers(t, b, 2)

		key := keyRoutedTo(t, b, "instance-a")
		require.NoError(t, a.ownership.Do(ctx, testNamespace, key, doNoop))

		// The first non-owner request resolves the redirect from the claim
		// record; repeats within the cache TTL must not read the store again —
		// the forward path pays at most one claim read per key per TTL, not
		// one per request.
		assertRedirect := func() {
			err := b.ownership.Do(ctx, testNamespace, key, doNoop)
			var notOwner *cluster.NotOwnerError
			require.ErrorAs(t, err, &notOwner)
			require.NotNil(t, notOwner.Redirect)
			require.Equal(t, "instance-a", notOwner.Redirect.InstanceID)
		}

		assertRedirect()
		reads := counting.claimReads.Load()
		for range 10 {
			assertRedirect()
		}
		require.Equal(t, reads, counting.claimReads.Load())
	})
}

func testMemberGC(t *testing.T, s cluster.Store) {
	t.Run("testMemberGC", func(t *testing.T) {
		ctx := context.Background()

		// Tight liveness so the GC threshold (10× the window by default) is
		// reachable in test time.
		mCfg := fastMembershipConfig()
		mCfg.LivenessWindow = 100 * time.Millisecond

		a := startNode(t, s, "instance-a", mCfg, fastOwnershipConfig())
		waitForLiveMembers(t, a, 1)

		// A corpse record no process backs: registered, never heartbeated.
		// Store-level TTLs are lazy, so an observer must actively delete it —
		// otherwise freshly started nodes presume it live for a full window
		// and route to it.
		require.NoError(t, s.PutMember(ctx, &cluster.Member{
			InstanceID: "instance-corpse",
			Address:    "corpse.local:8085",
		}, 1))

		require.Eventually(t, func() bool {
			records, err := s.GetMembers(ctx)
			require.NoError(t, err)
			for _, r := range records {
				if r.InstanceID == "instance-corpse" {
					return false
				}
			}
			return true
		}, 10*time.Second, 25*time.Millisecond)

		// The live member's own record must survive its peers' GC.
		records, err := s.GetMembers(ctx)
		require.NoError(t, err)
		require.Len(t, records, 1)
		require.Equal(t, "instance-a", records[0].InstanceID)
	})
}

// failingDeleteStore fails DeleteMember for chosen instance IDs while armed,
// simulating throttling or a permissions gap on the GC path.
type failingDeleteStore struct {
	cluster.Store
	failFor  atomic.Value // string: instance ID whose deletes fail
	attempts atomic.Uint64
}

func (s *failingDeleteStore) DeleteMember(ctx context.Context, instanceID string) error {
	if fail, _ := s.failFor.Load().(string); fail != "" && fail == instanceID {
		s.attempts.Add(1)
		return errors.New("injected delete failure")
	}
	return s.Store.DeleteMember(ctx, instanceID)
}

func testMemberGCDeleteFailure(t *testing.T, s cluster.Store) {
	t.Run("testMemberGCDeleteFailure", func(t *testing.T) {
		ctx := context.Background()

		mCfg := fastMembershipConfig()
		mCfg.LivenessWindow = 100 * time.Millisecond // GC threshold: 1s.

		failing := &failingDeleteStore{Store: s}
		failing.failFor.Store("instance-corpse")

		a := startNode(t, failing, "instance-a", mCfg, fastOwnershipConfig())
		waitForLiveMembers(t, a, 1)

		require.NoError(t, s.PutMember(ctx, &cluster.Member{
			InstanceID: "instance-corpse",
			Address:    "corpse.local:8085",
		}, 1))

		// The corpse enjoys the unavoidable first-sight grace...
		corpseLive := func() bool {
			for _, m := range a.membership.Live() {
				if m.InstanceID == "instance-corpse" {
					return true
				}
			}
			return false
		}
		require.Eventually(t, corpseLive, 5*time.Second, 5*time.Millisecond)
		// ...then its unmoving heartbeat ages it out.
		require.Eventually(t, func() bool { return !corpseLive() }, 5*time.Second, 5*time.Millisecond)

		// With its GC delete persistently failing, the corpse must never be
		// presumed live again: dropping its observation before the delete is
		// confirmed would make the next poll first-sight it — routed to for a
		// full liveness window out of every MemberGCAfter, indefinitely.
		deadline := time.Now().Add(2500 * time.Millisecond)
		for time.Now().Before(deadline) {
			require.False(t, corpseLive(),
				"corpse resurrected as presumed-live after a failed GC delete")
			time.Sleep(5 * time.Millisecond)
		}

		// The delete keeps being retried while it fails, and once the failure
		// clears the record is finally collected.
		require.GreaterOrEqual(t, failing.attempts.Load(), uint64(2))
		failing.failFor.Store("")
		require.Eventually(t, func() bool {
			records, err := s.GetMembers(ctx)
			require.NoError(t, err)
			for _, r := range records {
				if r.InstanceID == "instance-corpse" {
					return false
				}
			}
			return true
		}, 10*time.Second, 25*time.Millisecond)
	})
}

func testSessionLostPurge(t *testing.T, s cluster.Store) {
	t.Run("testSessionLostPurge", func(t *testing.T) {
		ctx := context.Background()

		a := startNode(t, s, "instance-a", fastMembershipConfig(), fastOwnershipConfig())
		waitForLiveMembers(t, a, 1)

		key := []byte("session-key")
		require.NoError(t, a.ownership.Do(ctx, testNamespace, key, doNoop))

		records, err := s.GetMembers(ctx)
		require.NoError(t, err)
		require.Len(t, records, 1)
		preDeletionCounter := records[0].HeartbeatCounter

		// Simulate a peer garbage-collecting our record out from under us: the
		// next heartbeat discovers it, re-registers, and local ownership state
		// is shed (flushed and released) rather than served stale.
		require.NoError(t, s.DeleteMember(ctx, "instance-a"))

		require.Eventually(t, func() bool {
			return len(a.releasedKeys()) == 1 && len(a.ownership.OwnedKeys(testNamespace)) == 0
		}, 10*time.Second, 10*time.Millisecond)
		require.Equal(t, []string{string(key)}, a.releasedKeys())

		// The re-registered record's counter resumes strictly above the
		// previous epoch — a reset would let peers' stale prior-epoch takeover
		// evidence displace this live member. Every observed post-recovery
		// value must clear the old epoch: merely growing past it eventually is
		// not enough, since the window where old values recur is exactly where
		// stale evidence commits.
		reusedOldEpochValue := false
		require.Eventually(t, func() bool {
			records, err := s.GetMembers(ctx)
			require.NoError(t, err)
			if len(records) != 1 {
				return false
			}
			if records[0].HeartbeatCounter <= preDeletionCounter {
				reusedOldEpochValue = true
			}
			return records[0].HeartbeatCounter > preDeletionCounter
		}, 10*time.Second, 5*time.Millisecond)
		require.False(t, reusedOldEpochValue, "re-registration reused a prior-epoch heartbeat counter")

		// The member re-registered itself and serves again: demand re-acquires
		// the shed key with a fresh warm.
		require.Eventually(t, func() bool {
			var claim *cluster.Claim
			err := a.ownership.Do(ctx, testNamespace, key, func(_ context.Context, c *cluster.Claim) error {
				claim = c
				return nil
			})
			return err == nil && claim.OwnerInstanceID == "instance-a"
		}, 10*time.Second, 10*time.Millisecond)
		require.Equal(t, []string{string(key), string(key)}, a.acquiredKeys())
	})
}

// slowReleaseStore injects latency into ReleaseClaim, standing in for
// per-write network RTT so a serial drain's O(N × RTT) floor is measurable.
type slowReleaseStore struct {
	cluster.Store
	delay time.Duration
}

func (s *slowReleaseStore) ReleaseClaim(ctx context.Context, namespace string, key []byte, instanceID string) error {
	time.Sleep(s.delay)
	return s.Store.ReleaseClaim(ctx, namespace, key, instanceID)
}

func testParallelDrain(t *testing.T, s cluster.Store) {
	t.Run("testParallelDrain", func(t *testing.T) {
		ctx := context.Background()

		const keyCount = 200
		const releaseDelay = 10 * time.Millisecond

		slow := &slowReleaseStore{Store: s, delay: releaseDelay}
		a := startNode(t, slow, "instance-a", fastMembershipConfig(), fastOwnershipConfig())
		waitForLiveMembers(t, a, 1)

		for i := range keyCount {
			require.NoError(t, a.ownership.Do(ctx, testNamespace, fmt.Appendf(nil, "key-%d", i), doNoop))
		}
		require.Len(t, a.ownership.OwnedKeys(testNamespace), keyCount)

		// A serial drain's floor is keyCount × releaseDelay (2s here); the
		// bounded worker pool must land far under it — a large owned set may
		// never eat the shutdown grace period.
		start := time.Now()
		require.NoError(t, a.ownership.Drain(ctx))
		elapsed := time.Since(start)
		require.Less(t, elapsed, keyCount*releaseDelay/2)

		// Parallelism must not compromise the handoff: every claim released,
		// every flush hook fired.
		require.Empty(t, a.ownership.OwnedKeys(testNamespace))
		require.Len(t, a.releasedKeys(), keyCount)
		for i := range keyCount {
			_, err := s.GetClaim(ctx, testNamespace, fmt.Appendf(nil, "key-%d", i))
			require.ErrorIs(t, err, cluster.ErrClaimNotFound)
		}
	})
}

// delayableHeartbeatStore lets the test stall one heartbeat write, simulating
// a slow store write or scheduling hiccup.
type delayableHeartbeatStore struct {
	cluster.Store
	delayOnce atomic.Bool
	delay     time.Duration
}

func (s *delayableHeartbeatStore) Heartbeat(ctx context.Context, instanceID string) (uint64, error) {
	if s.delayOnce.CompareAndSwap(true, false) {
		time.Sleep(s.delay)
	}
	return s.Store.Heartbeat(ctx, instanceID)
}

func testSessionGapPurge(t *testing.T, s cluster.Store) {
	t.Run("testSessionGapPurge", func(t *testing.T) {
		ctx := context.Background()

		// A heartbeat gap at the suspicion floor is exactly the window in
		// which a suspicious peer could have displaced this member's claims
		// while it still considered itself healthy. Crossing it must shed
		// local ownership so any such dual serving ends immediately — not at
		// IdleTTL.
		mCfg := fastMembershipConfig()
		mCfg.SessionGapThreshold = 100 * time.Millisecond

		gated := &delayableHeartbeatStore{Store: s, delay: 200 * time.Millisecond}
		a := startNode(t, gated, "instance-a", mCfg, fastOwnershipConfig())
		waitForLiveMembers(t, a, 1)

		key := []byte("gap-key")
		require.NoError(t, a.ownership.Do(ctx, testNamespace, key, doNoop))

		gated.delayOnce.Store(true)

		// The delayed beat lands, the gap is noticed, ownership is shed.
		require.Eventually(t, func() bool {
			return len(a.releasedKeys()) == 1 && len(a.ownership.OwnedKeys(testNamespace)) == 0
		}, 10*time.Second, 5*time.Millisecond)
		require.Equal(t, []string{string(key)}, a.releasedKeys())

		// Demand re-acquires cleanly afterwards.
		require.Eventually(t, func() bool {
			return a.ownership.Do(ctx, testNamespace, key, doNoop) == nil
		}, 10*time.Second, 10*time.Millisecond)
	})
}

func testRebalanceOnJoin(t *testing.T, s cluster.Store) {
	t.Run("testRebalanceOnJoin", func(t *testing.T) {
		ctx := context.Background()

		a := startNode(t, s, "instance-a", fastMembershipConfig(), fastOwnershipConfig())
		waitForLiveMembers(t, a, 1)

		// Alone, A owns everything it touches.
		keys := make([][]byte, 32)
		for i := range keys {
			keys[i] = fmt.Appendf(nil, "key-%d", i)
			require.NoError(t, a.ownership.Do(ctx, testNamespace, keys[i], doNoop))
		}
		require.Len(t, a.ownership.OwnedKeys(testNamespace), len(keys))

		// A second member joins: A must drain exactly the keys that now
		// prefer B — HRW moves ~1/N of the keyspace, never all of it.
		b := startNode(t, s, "instance-b", fastMembershipConfig(), fastOwnershipConfig())
		waitForLiveMembers(t, a, 2)
		waitForLiveMembers(t, b, 2)

		expectKept := 0
		for _, key := range keys {
			owner, err := a.router.Owner(ctx, testNamespace, key)
			require.NoError(t, err)
			if owner.InstanceID == "instance-a" {
				expectKept++
			}
		}
		require.Positive(t, expectKept)
		require.Less(t, expectKept, len(keys))

		require.Eventually(t, func() bool {
			return len(a.ownership.OwnedKeys(testNamespace)) == expectKept
		}, 5*time.Second, 10*time.Millisecond)

		// Moved keys were released (flushed), not abandoned, and B can
		// acquire them on demand immediately.
		require.Len(t, a.releasedKeys(), len(keys)-expectKept)
		for _, key := range keys {
			owner, err := b.router.Owner(ctx, testNamespace, key)
			require.NoError(t, err)
			if owner.InstanceID != "instance-b" {
				continue
			}
			var claim *cluster.Claim
			require.NoError(t, b.ownership.Do(ctx, testNamespace, key, func(_ context.Context, c *cluster.Claim) error {
				claim = c
				return nil
			}))
			require.Equal(t, "instance-b", claim.OwnerInstanceID)
		}
	})
}

func testIdleRelease(t *testing.T, s cluster.Store) {
	t.Run("testIdleRelease", func(t *testing.T) {
		ctx := context.Background()

		oCfg := fastOwnershipConfig()
		oCfg.IdleTTL = 100 * time.Millisecond

		a := startNode(t, s, "instance-a", fastMembershipConfig(), oCfg)
		waitForLiveMembers(t, a, 1)

		key := []byte("idle-key")
		require.NoError(t, a.ownership.Do(ctx, testNamespace, key, doNoop))
		require.Len(t, a.ownership.OwnedKeys(testNamespace), 1)

		// The unused claim is flushed and released, keeping the claim table to
		// the active working set.
		require.Eventually(t, func() bool {
			_, err := s.GetClaim(ctx, testNamespace, key)
			return errors.Is(err, cluster.ErrClaimNotFound)
		}, 5*time.Second, 10*time.Millisecond)
		require.Equal(t, []string{string(key)}, a.releasedKeys())
		require.Empty(t, a.ownership.OwnedKeys(testNamespace))

		// The next demand re-acquires transparently.
		var claim *cluster.Claim
		require.NoError(t, a.ownership.Do(ctx, testNamespace, key, func(_ context.Context, c *cluster.Claim) error {
			claim = c
			return nil
		}))
		require.EqualValues(t, 2, claim.Fence)
	})
}
