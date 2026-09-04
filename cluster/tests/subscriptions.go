package tests

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/code-payments/flipcash2-server/cluster"
)

const subsNamespace = "events"

// testSubscriptionRegistry is the store contract for subscription rows:
// idempotent upserts and deletes, multi-subscriber topics, and isolation
// across namespaces and keys.
func testSubscriptionRegistry(t *testing.T, s cluster.Store) {
	t.Run("testSubscriptionRegistry", func(t *testing.T) {
		ctx := context.Background()

		a := member("instance-a", "10.0.0.1:8085")
		b := member("instance-b", "10.0.0.2:8085")
		key := []byte("group-1")

		subs, err := s.GetSubscribers(ctx, subsNamespace, key)
		require.NoError(t, err)
		require.Empty(t, subs)

		require.NoError(t, s.PutSubscription(ctx, subsNamespace, key, a))
		require.NoError(t, s.PutSubscription(ctx, subsNamespace, key, b))
		require.NoError(t, s.PutSubscription(ctx, subsNamespace, key, a)) // Idempotent upsert.

		subs, err = s.GetSubscribers(ctx, subsNamespace, key)
		require.NoError(t, err)
		require.Len(t, subs, 2)
		byID := make(map[string]*cluster.Subscription)
		for _, sub := range subs {
			byID[sub.InstanceID] = sub
		}
		require.Equal(t, subsNamespace, byID["instance-a"].Namespace)
		require.Equal(t, key, byID["instance-a"].Key)
		require.Equal(t, "10.0.0.1:8085", byID["instance-a"].Address)
		require.Equal(t, "10.0.0.2:8085", byID["instance-b"].Address)

		// Same key in another namespace, and another key in the same
		// namespace, are different topics.
		subs, err = s.GetSubscribers(ctx, "other", key)
		require.NoError(t, err)
		require.Empty(t, subs)
		subs, err = s.GetSubscribers(ctx, subsNamespace, []byte("group-2"))
		require.NoError(t, err)
		require.Empty(t, subs)

		require.NoError(t, s.PutSubscription(ctx, "other", key, a))
		require.NoError(t, s.DeleteSubscription(ctx, subsNamespace, key, "instance-a"))
		require.NoError(t, s.DeleteSubscription(ctx, subsNamespace, key, "instance-a")) // Idempotent.

		subs, err = s.GetSubscribers(ctx, subsNamespace, key)
		require.NoError(t, err)
		require.Len(t, subs, 1)
		require.Equal(t, "instance-b", subs[0].InstanceID)

		// The delete didn't leak across namespaces.
		subs, err = s.GetSubscribers(ctx, "other", key)
		require.NoError(t, err)
		require.Len(t, subs, 1)
		require.Equal(t, "instance-a", subs[0].InstanceID)
	})
}

func subscriberIDs(subs []*cluster.Subscription) []string {
	out := make([]string, len(subs))
	for i, sub := range subs {
		out[i] = sub.InstanceID
	}
	return out
}

func testSubscribeRefcounting(t *testing.T, s cluster.Store) {
	t.Run("testSubscribeRefcounting", func(t *testing.T) {
		ctx := context.Background()

		a := startNode(t, s, "instance-a", fastMembershipConfig(), fastOwnershipConfig())
		waitForLiveMembers(t, a, 1)

		key := []byte("group-1")

		// The first local handle writes the row; the second rides it.
		h1, err := a.subscriptions.Subscribe(ctx, subsNamespace, key)
		require.NoError(t, err)
		h2, err := a.subscriptions.Subscribe(ctx, subsNamespace, key)
		require.NoError(t, err)

		rows, err := s.GetSubscribers(ctx, subsNamespace, key)
		require.NoError(t, err)
		require.Len(t, rows, 1)
		require.Equal(t, "instance-a", rows[0].InstanceID)
		require.Equal(t, "instance-a.local:8085", rows[0].Address)

		// The row survives until the last handle closes.
		require.NoError(t, h1.Close(ctx))
		rows, err = s.GetSubscribers(ctx, subsNamespace, key)
		require.NoError(t, err)
		require.Len(t, rows, 1)

		require.NoError(t, h2.Close(ctx))
		rows, err = s.GetSubscribers(ctx, subsNamespace, key)
		require.NoError(t, err)
		require.Empty(t, rows)

		// Close is idempotent.
		require.NoError(t, h2.Close(ctx))
	})
}

func testSubscriberResolutionAndLiveness(t *testing.T, s cluster.Store) {
	t.Run("testSubscriberResolutionAndLiveness", func(t *testing.T) {
		ctx := context.Background()

		a := startNode(t, s, "instance-a", fastMembershipConfig(), fastOwnershipConfig())
		b := startNode(t, s, "instance-b", fastMembershipConfig(), fastOwnershipConfig())
		waitForLiveMembers(t, a, 2)
		waitForLiveMembers(t, b, 2)

		key := []byte("group-1")

		_, err := a.subscriptions.Subscribe(ctx, subsNamespace, key)
		require.NoError(t, err)

		// A peer resolves the subscriber (past its cache TTL).
		require.Eventually(t, func() bool {
			subs, err := b.subscriptions.Subscribers(ctx, subsNamespace, key)
			require.NoError(t, err)
			return len(subs) == 1 && subs[0].InstanceID == "instance-a"
		}, 5*time.Second, 10*time.Millisecond)

		// Both subscribed: both resolve both.
		_, err = b.subscriptions.Subscribe(ctx, subsNamespace, key)
		require.NoError(t, err)
		require.Eventually(t, func() bool {
			fromA, err := a.subscriptions.Subscribers(ctx, subsNamespace, key)
			require.NoError(t, err)
			fromB, err := b.subscriptions.Subscribers(ctx, subsNamespace, key)
			require.NoError(t, err)
			return len(fromA) == 2 && len(fromB) == 2
		}, 5*time.Second, 10*time.Millisecond)

		// A crashes without cleanup: its row stops counting the moment its
		// liveness lapses — no row deletion required for correctness.
		a.ownership.Stop()
		a.membership.Stop()
		require.Eventually(t, func() bool {
			subs, err := b.subscriptions.Subscribers(ctx, subsNamespace, key)
			require.NoError(t, err)
			return len(subs) == 1 && subs[0].InstanceID == "instance-b"
		}, 5*time.Second, 10*time.Millisecond)
	})
}

// subscriberReadCountingStore counts GetSubscribers store reads, to observe
// the resolution cache's effect on the publish path.
type subscriberReadCountingStore struct {
	cluster.Store
	reads atomic.Uint64
}

func (s *subscriberReadCountingStore) GetSubscribers(ctx context.Context, namespace string, key []byte) ([]*cluster.Subscription, error) {
	s.reads.Add(1)
	return s.Store.GetSubscribers(ctx, namespace, key)
}

func testSubscriberCache(t *testing.T, s cluster.Store) {
	t.Run("testSubscriberCache", func(t *testing.T) {
		ctx := context.Background()

		a := startNode(t, s, "instance-a", fastMembershipConfig(), fastOwnershipConfig())
		waitForLiveMembers(t, a, 1)

		// A dedicated runtime with an effectively-infinite cache TTL, so the
		// read-amortization property is asserted without a timing race.
		counting := &subscriberReadCountingStore{Store: s}
		subs := cluster.NewSubscriptions(zap.NewNop(), a.membership, counting, cluster.SubscriptionsConfig{
			CacheTTL: time.Hour,
		})

		key := []byte("group-1")
		_, err := subs.Subscribe(ctx, subsNamespace, key)
		require.NoError(t, err)

		// One store read resolves the topic; repeats ride the cache — the
		// publish path pays per TTL, never per event.
		resolved, err := subs.Subscribers(ctx, subsNamespace, key)
		require.NoError(t, err)
		require.Equal(t, []string{"instance-a"}, subscriberIDs(resolved))
		reads := counting.reads.Load()
		for range 10 {
			resolved, err = subs.Subscribers(ctx, subsNamespace, key)
			require.NoError(t, err)
			require.Equal(t, []string{"instance-a"}, subscriberIDs(resolved))
		}
		require.Equal(t, reads, counting.reads.Load())

		// Empty results are cached too: publishes toward topics with no
		// streams anywhere must not read the store per event.
		missing := []byte("nobody-streams-this")
		resolved, err = subs.Subscribers(ctx, subsNamespace, missing)
		require.NoError(t, err)
		require.Empty(t, resolved)
		reads = counting.reads.Load()
		for range 10 {
			_, err = subs.Subscribers(ctx, subsNamespace, missing)
			require.NoError(t, err)
		}
		require.Equal(t, reads, counting.reads.Load())
	})
}

func testSubscriptionsDrainAndResume(t *testing.T, s cluster.Store) {
	t.Run("testSubscriptionsDrainAndResume", func(t *testing.T) {
		ctx := context.Background()

		a := startNode(t, s, "instance-a", fastMembershipConfig(), fastOwnershipConfig())
		waitForLiveMembers(t, a, 1)

		keys := [][]byte{[]byte("group-1"), []byte("group-2"), []byte("group-3")}
		handles := make([]*cluster.SubscriptionHandle, len(keys))
		for i, key := range keys {
			h, err := a.subscriptions.Subscribe(ctx, subsNamespace, key)
			require.NoError(t, err)
			handles[i] = h
		}

		// Drain removes every row eagerly — successors' publishers stop
		// forwarding here without waiting out liveness.
		require.NoError(t, a.subscriptions.Drain(ctx))
		for _, key := range keys {
			rows, err := s.GetSubscribers(ctx, subsNamespace, key)
			require.NoError(t, err)
			require.Empty(t, rows)
		}

		// The latch refuses new registrations; outstanding handles close as
		// no-ops.
		_, err := a.subscriptions.Subscribe(ctx, subsNamespace, []byte("late"))
		require.ErrorIs(t, err, cluster.ErrSubscriptionsDraining)
		for _, h := range handles {
			require.NoError(t, h.Close(ctx))
		}

		// Resume is the explicit opt back into service for an aborted
		// shutdown.
		a.subscriptions.Resume()
		h, err := a.subscriptions.Subscribe(ctx, subsNamespace, keys[0])
		require.NoError(t, err)
		rows, err := s.GetSubscribers(ctx, subsNamespace, keys[0])
		require.NoError(t, err)
		require.Len(t, rows, 1)
		require.NoError(t, h.Close(ctx))
	})
}

func testSubscriptionStaleHandleAfterResume(t *testing.T, s cluster.Store) {
	t.Run("testSubscriptionStaleHandleAfterResume", func(t *testing.T) {
		ctx := context.Background()

		a := startNode(t, s, "instance-a", fastMembershipConfig(), fastOwnershipConfig())
		waitForLiveMembers(t, a, 1)

		key := []byte("group-1")

		// A handle survives a Drain+Resume cycle, and a fresh Subscribe then
		// re-creates the topic registration. The stale handle's Close belongs
		// to the drained-out registration: it must not decrement — let alone
		// delete the row of — the live successor.
		stale, err := a.subscriptions.Subscribe(ctx, subsNamespace, key)
		require.NoError(t, err)
		require.NoError(t, a.subscriptions.Drain(ctx))
		a.subscriptions.Resume()

		live, err := a.subscriptions.Subscribe(ctx, subsNamespace, key)
		require.NoError(t, err)

		require.NoError(t, stale.Close(ctx))
		rows, err := s.GetSubscribers(ctx, subsNamespace, key)
		require.NoError(t, err)
		require.Len(t, rows, 1)
		require.Equal(t, "instance-a", rows[0].InstanceID)

		// The live handle's Close is still the one that removes the row.
		require.NoError(t, live.Close(ctx))
		rows, err = s.GetSubscribers(ctx, subsNamespace, key)
		require.NoError(t, err)
		require.Empty(t, rows)
	})
}

// subscriptionPutGateStore parks PutSubscription calls while the gate is up,
// signaling entry and waiting for release — to hold a row write in flight
// across a concurrent Drain.
type subscriptionPutGateStore struct {
	cluster.Store
	gate    atomic.Bool
	entered chan struct{}
	release chan struct{}
	putDone chan struct{}
}

func (s *subscriptionPutGateStore) PutSubscription(ctx context.Context, namespace string, key []byte, m *cluster.Member) error {
	gated := s.gate.Load()
	if gated {
		s.entered <- struct{}{}
		<-s.release
	}
	err := s.Store.PutSubscription(ctx, namespace, key, m)
	if gated {
		s.putDone <- struct{}{}
	}
	return err
}

func testSubscriptionReassertDrainRace(t *testing.T, s cluster.Store) {
	t.Run("testSubscriptionReassertDrainRace", func(t *testing.T) {
		ctx := context.Background()

		a := startNode(t, s, "instance-a", fastMembershipConfig(), fastOwnershipConfig())
		waitForLiveMembers(t, a, 1)

		gated := &subscriptionPutGateStore{
			Store:   s,
			entered: make(chan struct{}, 1),
			release: make(chan struct{}),
			putDone: make(chan struct{}, 1),
		}
		subs := cluster.NewSubscriptions(zap.NewNop(), a.membership, gated, cluster.SubscriptionsConfig{
			CacheTTL: 25 * time.Millisecond,
		})

		key := []byte("group-1")
		_, err := subs.Subscribe(ctx, subsNamespace, key)
		require.NoError(t, err)

		// The row vanishes (a peer's sweep); the next resolution notices and
		// re-asserts it in the background. Park that put in flight — after its
		// pre-drain check passed — and Drain in the gap. The put lands after
		// Drain's bulk delete already ran; the re-assertion must notice and
		// hand the row back rather than leave it orphaned until RowGCAfter.
		require.NoError(t, s.DeleteSubscription(ctx, subsNamespace, key, "instance-a"))
		gated.gate.Store(true)
		_, err = subs.Subscribers(ctx, subsNamespace, key)
		require.NoError(t, err)

		select {
		case <-gated.entered:
		case <-time.After(5 * time.Second):
			require.Fail(t, "re-assert put never started")
		}
		require.NoError(t, subs.Drain(ctx))
		close(gated.release)

		// Only assert once the parked put has actually landed — before that,
		// an empty registry proves nothing.
		select {
		case <-gated.putDone:
		case <-time.After(5 * time.Second):
			require.Fail(t, "re-assert put never completed")
		}
		require.Eventually(t, func() bool {
			rows, err := s.GetSubscribers(ctx, subsNamespace, key)
			require.NoError(t, err)
			return len(rows) == 0
		}, 10*time.Second, 25*time.Millisecond)
	})
}

func testSubscriptionSessionReassert(t *testing.T, s cluster.Store) {
	t.Run("testSubscriptionSessionReassert", func(t *testing.T) {
		ctx := context.Background()

		a := startNode(t, s, "instance-a", fastMembershipConfig(), fastOwnershipConfig())
		waitForLiveMembers(t, a, 1)

		key := []byte("group-1")
		_, err := a.subscriptions.Subscribe(ctx, subsNamespace, key)
		require.NoError(t, err)

		// The row vanishes out from under the live subscriber (the shape a
		// peer's corpse sweep takes when our heartbeats gapped), and then the
		// session interruption is discovered: re-registration must re-assert
		// the row — the inverse of ownership's shedding, since interest rows
		// cannot conflict, only go missing.
		require.NoError(t, s.DeleteSubscription(ctx, subsNamespace, key, "instance-a"))
		require.NoError(t, s.DeleteMember(ctx, "instance-a"))

		require.Eventually(t, func() bool {
			rows, err := s.GetSubscribers(ctx, subsNamespace, key)
			require.NoError(t, err)
			return len(rows) == 1 && rows[0].InstanceID == "instance-a"
		}, 10*time.Second, 10*time.Millisecond)
	})
}

func testSubscriptionCorpseRowGC(t *testing.T, s cluster.Store) {
	t.Run("testSubscriptionCorpseRowGC", func(t *testing.T) {
		ctx := context.Background()

		a := startNode(t, s, "instance-a", fastMembershipConfig(), fastOwnershipConfig())
		waitForLiveMembers(t, a, 1)

		// A dedicated runtime with a reachable sweep threshold (the
		// constructor floors it against the liveness window).
		subs := cluster.NewSubscriptions(zap.NewNop(), a.membership, s, cluster.SubscriptionsConfig{
			CacheTTL:   25 * time.Millisecond,
			RowGCAfter: time.Second,
		})

		key := []byte("group-1")
		_, err := subs.Subscribe(ctx, subsNamespace, key)
		require.NoError(t, err)

		// A crashed instance's leftover row: no member record backs it.
		require.NoError(t, s.PutSubscription(ctx, subsNamespace, key, &cluster.Member{
			InstanceID: "instance-corpse",
			Address:    "corpse.local:8085",
		}))

		// The corpse is excluded from resolution immediately — correctness
		// never waits on the sweep.
		resolved, err := subs.Subscribers(ctx, subsNamespace, key)
		require.NoError(t, err)
		require.Equal(t, []string{"instance-a"}, subscriberIDs(resolved))

		// Once the instance has stayed unknown past RowGCAfter, resolution
		// sweeps the row itself; the live subscriber's row must survive.
		require.Eventually(t, func() bool {
			_, err := subs.Subscribers(ctx, subsNamespace, key)
			require.NoError(t, err)
			rows, err := s.GetSubscribers(ctx, subsNamespace, key)
			require.NoError(t, err)
			return len(rows) == 1 && rows[0].InstanceID == "instance-a"
		}, 10*time.Second, 25*time.Millisecond)
	})
}
