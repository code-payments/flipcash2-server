package tests

import (
	"context"
	"fmt"
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

		key := []byte("group-1")

		subs, err := s.GetSubscribers(ctx, subsNamespace, key)
		require.NoError(t, err)
		require.Empty(t, subs)

		require.NoError(t, s.PutSubscription(ctx, subsNamespace, key, "instance-a"))
		require.NoError(t, s.PutSubscription(ctx, subsNamespace, key, "instance-b"))
		require.NoError(t, s.PutSubscription(ctx, subsNamespace, key, "instance-a")) // Idempotent upsert.

		subs, err = s.GetSubscribers(ctx, subsNamespace, key)
		require.NoError(t, err)
		require.Len(t, subs, 2)
		byID := make(map[string]*cluster.Subscription)
		for _, sub := range subs {
			byID[sub.InstanceID] = sub
		}
		require.Equal(t, subsNamespace, byID["instance-a"].Namespace)
		require.Equal(t, key, byID["instance-a"].Key)
		// Rows carry identity only; addresses are the runtime's job to resolve.
		require.Empty(t, byID["instance-a"].Address)

		// Same key in another namespace, and another key in the same
		// namespace, are different topics.
		subs, err = s.GetSubscribers(ctx, "other", key)
		require.NoError(t, err)
		require.Empty(t, subs)
		subs, err = s.GetSubscribers(ctx, subsNamespace, []byte("group-2"))
		require.NoError(t, err)
		require.Empty(t, subs)

		require.NoError(t, s.PutSubscription(ctx, "other", key, "instance-a"))
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

// testSubscriptionBatchPut is the store contract for PutSubscriptions: one
// call registers every listed topic (duplicates collapsed, re-puts idempotent),
// across enough topics to force chunking in backends with a per-request cap.
func testSubscriptionBatchPut(t *testing.T, s cluster.Store) {
	t.Run("testSubscriptionBatchPut", func(t *testing.T) {
		ctx := context.Background()

		const topicCount = 60 // Comfortably above DynamoDB's 25-item batch cap.
		topics := make([]cluster.SubscriptionTopic, 0, topicCount+1)
		for i := range topicCount {
			topics = append(topics, cluster.SubscriptionTopic{
				Namespace: subsNamespace,
				Key:       fmt.Appendf(nil, "group-%d", i),
			})
		}
		// A duplicated topic collapses instead of failing the batch.
		topics = append(topics, topics[0])

		require.NoError(t, s.PutSubscriptions(ctx, topics, "instance-a"))
		for i := range topicCount {
			subs, err := s.GetSubscribers(ctx, subsNamespace, fmt.Appendf(nil, "group-%d", i))
			require.NoError(t, err)
			require.Len(t, subs, 1)
			require.Equal(t, "instance-a", subs[0].InstanceID)
		}

		// Re-put is an idempotent upsert, exactly as for single rows, and
		// another instance's batch joins the same topics.
		require.NoError(t, s.PutSubscriptions(ctx, topics[:2], "instance-a"))
		require.NoError(t, s.PutSubscriptions(ctx, topics[:1], "instance-b"))
		subs, err := s.GetSubscribers(ctx, subsNamespace, topics[0].Key)
		require.NoError(t, err)
		require.ElementsMatch(t, []string{"instance-a", "instance-b"}, subscriberIDs(subs))

		// An empty batch is a no-op.
		require.NoError(t, s.PutSubscriptions(ctx, nil, "instance-a"))
	})
}

// testSubscriptionBatchDelete is the store contract for DeleteSubscriptions:
// one call removes every listed topic's row for the instance (duplicates
// collapsed, absent rows a no-op), across enough topics to force chunking,
// without disturbing other instances' rows.
func testSubscriptionBatchDelete(t *testing.T, s cluster.Store) {
	t.Run("testSubscriptionBatchDelete", func(t *testing.T) {
		ctx := context.Background()

		const topicCount = 60 // Comfortably above DynamoDB's 25-item batch cap.
		topics := make([]cluster.SubscriptionTopic, 0, topicCount)
		for i := range topicCount {
			topics = append(topics, cluster.SubscriptionTopic{
				Namespace: subsNamespace,
				Key:       fmt.Appendf(nil, "group-%d", i),
			})
		}
		require.NoError(t, s.PutSubscriptions(ctx, topics, "instance-a"))
		require.NoError(t, s.PutSubscription(ctx, subsNamespace, topics[0].Key, "instance-b"))

		// Duplicates collapse; a topic with no row is a no-op, as for single
		// deletes.
		batch := append(append([]cluster.SubscriptionTopic{}, topics...),
			topics[0],
			cluster.SubscriptionTopic{Namespace: subsNamespace, Key: []byte("group-absent")},
		)
		require.NoError(t, s.DeleteSubscriptions(ctx, batch, "instance-a"))

		for i := range topicCount {
			subs, err := s.GetSubscribers(ctx, subsNamespace, topics[i].Key)
			require.NoError(t, err)
			if i == 0 {
				// Another instance's row on the same topic is untouched.
				require.Equal(t, []string{"instance-b"}, subscriberIDs(subs))
			} else {
				require.Empty(t, subs)
			}
		}

		// An empty batch is a no-op.
		require.NoError(t, s.DeleteSubscriptions(ctx, nil, "instance-a"))
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

// testSubscribeAll pins the batch registration path: one call yields a handle
// per entry, refcounts interoperate with single Subscribe/Close (including a
// duplicated topic within the batch), and a draining runtime refuses the batch
// with nothing registered.
func testSubscribeAll(t *testing.T, s cluster.Store) {
	t.Run("testSubscribeAll", func(t *testing.T) {
		ctx := context.Background()

		a := startNode(t, s, "instance-a", fastMembershipConfig(), fastOwnershipConfig())
		waitForLiveMembers(t, a, 1)

		keyShared := []byte("group-shared")
		keyNew1 := []byte("group-new-1")
		keyNew2 := []byte("group-new-2")

		// A single Subscribe already holds the shared topic; the batch must
		// ride its row rather than re-write it.
		hSingle, err := a.subscriptions.Subscribe(ctx, subsNamespace, keyShared)
		require.NoError(t, err)

		handles, err := a.subscriptions.SubscribeAll(ctx, []cluster.SubscriptionTopic{
			{Namespace: subsNamespace, Key: keyShared},
			{Namespace: subsNamespace, Key: keyNew1},
			{Namespace: subsNamespace, Key: keyNew2},
			{Namespace: subsNamespace, Key: keyNew1}, // Duplicate: its own handle, same registration.
		})
		require.NoError(t, err)
		require.Len(t, handles, 4)

		for _, key := range [][]byte{keyShared, keyNew1, keyNew2} {
			rows, err := s.GetSubscribers(ctx, subsNamespace, key)
			require.NoError(t, err)
			require.Len(t, rows, 1)
			require.Equal(t, "instance-a", rows[0].InstanceID)
		}

		// The shared topic's row is refcounted across both paths: the single
		// handle's close must not remove it while the batch handle lives.
		require.NoError(t, hSingle.Close(ctx))
		rows, err := s.GetSubscribers(ctx, subsNamespace, keyShared)
		require.NoError(t, err)
		require.Len(t, rows, 1)

		// The duplicated topic's handles are two refs: one close keeps the row.
		require.NoError(t, handles[1].Close(ctx))
		rows, err = s.GetSubscribers(ctx, subsNamespace, keyNew1)
		require.NoError(t, err)
		require.Len(t, rows, 1)

		// Closing everything (handles[1] a second time — idempotent) removes
		// every row.
		for _, h := range handles {
			require.NoError(t, h.Close(ctx))
		}
		for _, key := range [][]byte{keyShared, keyNew1, keyNew2} {
			rows, err := s.GetSubscribers(ctx, subsNamespace, key)
			require.NoError(t, err)
			require.Empty(t, rows)
		}

		// A draining runtime refuses the batch, registering nothing.
		require.NoError(t, a.subscriptions.Drain(ctx))
		_, err = a.subscriptions.SubscribeAll(ctx, []cluster.SubscriptionTopic{
			{Namespace: subsNamespace, Key: []byte("group-late")},
		})
		require.ErrorIs(t, err, cluster.ErrSubscriptionsDraining)
		rows, err = s.GetSubscribers(ctx, subsNamespace, []byte("group-late"))
		require.NoError(t, err)
		require.Empty(t, rows)
	})
}

// testCloseAll pins the batch release path: one call releases every handle,
// deleting rows only for topics whose last local handle was in the batch, and
// interoperating with single Subscribe/Close in any order — including a
// handle already closed individually, which CloseAll must skip.
func testCloseAll(t *testing.T, s cluster.Store) {
	t.Run("testCloseAll", func(t *testing.T) {
		ctx := context.Background()

		a := startNode(t, s, "instance-a", fastMembershipConfig(), fastOwnershipConfig())
		waitForLiveMembers(t, a, 1)

		keyShared := []byte("group-shared")
		keyNew1 := []byte("group-new-1")
		keyNew2 := []byte("group-new-2")

		hSingle, err := a.subscriptions.Subscribe(ctx, subsNamespace, keyShared)
		require.NoError(t, err)

		handles, err := a.subscriptions.SubscribeAll(ctx, []cluster.SubscriptionTopic{
			{Namespace: subsNamespace, Key: keyShared},
			{Namespace: subsNamespace, Key: keyNew1},
			{Namespace: subsNamespace, Key: keyNew2},
		})
		require.NoError(t, err)

		// A handle closed individually first is skipped by the batch; its
		// topic's row is already gone.
		require.NoError(t, handles[2].Close(ctx))
		rows, err := s.GetSubscribers(ctx, subsNamespace, keyNew2)
		require.NoError(t, err)
		require.Empty(t, rows)

		// The batch releases the rest: keyNew1's row goes (last handle), the
		// shared topic's row survives on the single handle's refcount.
		require.NoError(t, a.subscriptions.CloseAll(ctx, handles))
		rows, err = s.GetSubscribers(ctx, subsNamespace, keyNew1)
		require.NoError(t, err)
		require.Empty(t, rows)
		rows, err = s.GetSubscribers(ctx, subsNamespace, keyShared)
		require.NoError(t, err)
		require.Len(t, rows, 1)

		// Idempotent: a second batch (and a straggling single Close) is a
		// no-op, not a double decrement against the surviving registration.
		require.NoError(t, a.subscriptions.CloseAll(ctx, handles))
		require.NoError(t, handles[0].Close(ctx))
		rows, err = s.GetSubscribers(ctx, subsNamespace, keyShared)
		require.NoError(t, err)
		require.Len(t, rows, 1)

		require.NoError(t, hSingle.Close(ctx))
		rows, err = s.GetSubscribers(ctx, subsNamespace, keyShared)
		require.NoError(t, err)
		require.Empty(t, rows)
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

		// A peer resolves the subscriber (past its cache TTL), with the dial
		// address joined in from the membership view — rows don't carry it.
		require.Eventually(t, func() bool {
			subs, err := b.subscriptions.Subscribers(ctx, subsNamespace, key)
			require.NoError(t, err)
			return len(subs) == 1 && subs[0].InstanceID == "instance-a" &&
				subs[0].Address == "instance-a.local:8085"
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

func (s *subscriptionPutGateStore) PutSubscription(ctx context.Context, namespace string, key []byte, instanceID string) error {
	gated := s.gate.Load()
	if gated {
		s.entered <- struct{}{}
		<-s.release
	}
	err := s.Store.PutSubscription(ctx, namespace, key, instanceID)
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

// memberFilterStore hides one instance's member record from GetMembers scans
// while the gate is up, simulating a transient scan miss (an eventually
// consistent or paginated read dropping a live member for one poll).
type memberFilterStore struct {
	cluster.Store
	hidden string
	hide   atomic.Bool
}

func (s *memberFilterStore) GetMembers(ctx context.Context) ([]*cluster.MemberRecord, error) {
	records, err := s.Store.GetMembers(ctx)
	if err != nil || !s.hide.Load() {
		return records, err
	}
	filtered := make([]*cluster.MemberRecord, 0, len(records))
	for _, r := range records {
		if r.InstanceID != s.hidden {
			filtered = append(filtered, r)
		}
	}
	return filtered, nil
}

func testSubscriptionLiveRowSurvivesScanBlips(t *testing.T, s cluster.Store) {
	t.Run("testSubscriptionLiveRowSurvivesScanBlips", func(t *testing.T) {
		ctx := context.Background()
		log := zap.NewNop()

		// The corpse clock must measure CONTINUOUS absence: two transient
		// scan blips more than RowGCAfter apart — with the member live and
		// healthy the whole time in between — must not add up to sweeping its
		// row. The member's own session is never interrupted, so nothing on
		// its side would ever re-assert the swept row.
		x := startNode(t, s, "instance-x", fastMembershipConfig(), fastOwnershipConfig())
		waitForLiveMembers(t, x, 1)

		key := []byte("group-1")
		_, err := x.subscriptions.Subscribe(ctx, subsNamespace, key)
		require.NoError(t, err)

		// An observer whose membership scans can transiently miss x.
		filtered := &memberFilterStore{Store: s, hidden: "instance-x"}
		obs := cluster.NewMembership(log, filtered, member("instance-o", "10.0.0.9:8085"), fastMembershipConfig())
		require.NoError(t, obs.Start(ctx))
		t.Cleanup(obs.Stop)
		subs := cluster.NewSubscriptions(log, obs, s, cluster.SubscriptionsConfig{
			CacheTTL:   25 * time.Millisecond,
			RowGCAfter: time.Second,
		})

		require.Eventually(t, func() bool {
			resolved, err := subs.Subscribers(ctx, subsNamespace, key)
			require.NoError(t, err)
			return len(resolved) == 1 && resolved[0].InstanceID == "instance-x"
		}, 5*time.Second, 10*time.Millisecond)

		blip := func() {
			filtered.hide.Store(true)
			require.NoError(t, obs.Refresh(ctx))
			// A resolution lands inside the blip: x is excluded from delivery
			// (correct) and its unknown-since anchor is touched.
			require.Eventually(t, func() bool {
				resolved, err := subs.Subscribers(ctx, subsNamespace, key)
				require.NoError(t, err)
				return len(resolved) == 0
			}, 5*time.Second, 10*time.Millisecond)
			filtered.hide.Store(false)
			require.NoError(t, obs.Refresh(ctx))
		}

		blip()

		// x is live and healthy, but no resolution touches its row for longer
		// than RowGCAfter.
		time.Sleep(1200 * time.Millisecond)

		blip()

		// The second blip's resolution must not have swept the live row: give
		// any (buggy) async sweep time to land, then check the registry.
		time.Sleep(100 * time.Millisecond)
		rows, err := s.GetSubscribers(ctx, subsNamespace, key)
		require.NoError(t, err)
		require.Len(t, rows, 1)
		require.Equal(t, "instance-x", rows[0].InstanceID)

		// And delivery resumes on the observer's next fresh resolution.
		require.Eventually(t, func() bool {
			resolved, err := subs.Subscribers(ctx, subsNamespace, key)
			require.NoError(t, err)
			return len(resolved) == 1 && resolved[0].InstanceID == "instance-x"
		}, 5*time.Second, 10*time.Millisecond)
	})
}

func testSubscriptionSlowMemberRowNotSwept(t *testing.T, s cluster.Store) {
	t.Run("testSubscriptionSlowMemberRowNotSwept", func(t *testing.T) {
		ctx := context.Background()
		log := zap.NewNop()

		// A member whose heartbeat counter freezes but whose registry record
		// still stands is slow (or wedged), not provably dead: it drops out of
		// delivery immediately, but its rows must never be swept while the
		// record is observed — membership GC is the authority on corpsehood.
		x := startNode(t, s, "instance-x", fastMembershipConfig(), fastOwnershipConfig())
		waitForLiveMembers(t, x, 1)

		key := []byte("group-1")
		_, err := x.subscriptions.Subscribe(ctx, subsNamespace, key)
		require.NoError(t, err)

		// Observer with member GC out of the way, so the record stays put and
		// only the (potentially buggy) row sweep could remove state.
		mCfg := fastMembershipConfig()
		mCfg.MemberGCAfter = time.Minute
		obs := cluster.NewMembership(log, s, member("instance-o", "10.0.0.9:8085"), mCfg)
		require.NoError(t, obs.Start(ctx))
		t.Cleanup(obs.Stop)
		subs := cluster.NewSubscriptions(log, obs, s, cluster.SubscriptionsConfig{
			CacheTTL:   25 * time.Millisecond,
			RowGCAfter: time.Second,
		})

		require.Eventually(t, func() bool {
			resolved, err := subs.Subscribers(ctx, subsNamespace, key)
			require.NoError(t, err)
			return len(resolved) == 1 && resolved[0].InstanceID == "instance-x"
		}, 5*time.Second, 10*time.Millisecond)

		// Freeze x: loops stop, record remains, counter never moves again.
		x.ownership.Stop()
		x.membership.Stop()

		// x lapses out of delivery once its counter sits still past the
		// liveness window.
		require.Eventually(t, func() bool {
			resolved, err := subs.Subscribers(ctx, subsNamespace, key)
			require.NoError(t, err)
			return len(resolved) == 0
		}, 5*time.Second, 10*time.Millisecond)

		// Keep resolving well past RowGCAfter: the observation timeline spans
		// it, but the record is still there — the row must survive.
		deadline := time.Now().Add(2 * time.Second)
		for time.Now().Before(deadline) {
			_, err := subs.Subscribers(ctx, subsNamespace, key)
			require.NoError(t, err)
			time.Sleep(25 * time.Millisecond)
		}
		rows, err := s.GetSubscribers(ctx, subsNamespace, key)
		require.NoError(t, err)
		require.Len(t, rows, 1)
		require.Equal(t, "instance-x", rows[0].InstanceID)
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
		require.NoError(t, s.PutSubscription(ctx, subsNamespace, key, "instance-corpse"))

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
