package event

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"
	eventpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/event/v1"
)

// fakeStream is a Stream that records nothing but identity and closure.
type fakeStream struct {
	id     string
	mu     sync.Mutex
	closed int
}

func (f *fakeStream) ID() string { return f.id }
func (f *fakeStream) Notify(*eventpb.Event) error {
	return nil
}
func (f *fakeStream) Close() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.closed++
}

func userID(i int) *commonpb.UserId {
	return &commonpb.UserId{Value: fmt.Appendf(nil, "user-%04d", i)}
}

func streamIDs(streams []Stream[*eventpb.Event]) map[string]bool {
	ids := make(map[string]bool, len(streams))
	for _, s := range streams {
		ids[s.ID()] = true
	}
	return ids
}

// manyKeys returns enough distinct keys that a registration is guaranteed to
// span several shards.
func manyKeys(prefix string, n int) []string {
	keys := make([]string, n)
	for i := range keys {
		keys[i] = fmt.Sprintf("%s:%d", prefix, i)
	}
	return keys
}

func TestStreamRegistry_AddRemoveAcrossShards(t *testing.T) {
	r := newStreamRegistry()
	keys := manyKeys("chat", 4*registryShards)
	s1 := &fakeStream{id: "s1"}
	s2 := &fakeStream{id: "s2"}

	require.True(t, r.add("s1", localStream{stream: s1, userID: userID(1)}, keys))
	require.True(t, r.add("s2", localStream{stream: s2, userID: userID(2)}, keys[:1]))

	// Every key resolves s1; the first key resolves both.
	for i, key := range keys {
		got := streamIDs(targets(r, key, nil))
		require.True(t, got["s1"], "key %d must resolve s1", i)
		require.Equal(t, i == 0, got["s2"], "only the first key holds s2")
	}

	// Removing s1 leaves s2 alone on the shared key and empties the rest.
	r.remove("s1", keys)
	require.Equal(t, map[string]bool{"s2": true}, streamIDs(targets(r, keys[0], nil)))
	for _, key := range keys[1:] {
		require.Empty(t, targets(r, key, nil))
	}

	// Emptied keys are dropped from their shards, not left as empty maps.
	for i := range r.shards {
		shard := &r.shards[i]
		shard.mu.RLock()
		for key, byID := range shard.byKey {
			require.NotEmpty(t, byID, "key %s left an empty entry", key)
		}
		shard.mu.RUnlock()
	}

	// Removing under keys the stream never held is a no-op.
	r.remove("s2", keys)
	require.Empty(t, targets(r, keys[0], nil))
}

func TestStreamRegistry_TargetsHonorExclusions(t *testing.T) {
	r := newStreamRegistry()
	const key = "chat:shared"
	require.True(t, r.add("a1", localStream{stream: &fakeStream{id: "a1"}, userID: userID(1)}, []string{key}))
	require.True(t, r.add("a2", localStream{stream: &fakeStream{id: "a2"}, userID: userID(1)}, []string{key}))
	require.True(t, r.add("b1", localStream{stream: &fakeStream{id: "b1"}, userID: userID(2)}, []string{key}))

	require.Len(t, targets(r, key, nil), 3)
	// Excluding a user drops every one of their streams (multi-device) and no
	// one else's.
	require.Equal(t, map[string]bool{"b1": true}, streamIDs(targets(r, key, []*commonpb.UserId{userID(1)})))
	require.Empty(t, targets(r, key, []*commonpb.UserId{userID(1), userID(2)}))
	require.Empty(t, targets(r, "chat:unknown", nil))
}

func TestStreamRegistry_DrainReturnsEachStreamOnceAndRefusesAdds(t *testing.T) {
	r := newStreamRegistry()
	keys := manyKeys("chat", 3*registryShards)
	s1 := &fakeStream{id: "s1"}
	s2 := &fakeStream{id: "s2"}
	require.True(t, r.add("s1", localStream{stream: s1, userID: userID(1)}, keys))
	require.True(t, r.add("s2", localStream{stream: s2, userID: userID(2)}, keys[:10]))

	// s1 sits under every key, yet comes back exactly once.
	drained := r.drain()
	require.Len(t, drained, 2)
	require.Equal(t, map[string]bool{"s1": true, "s2": true}, streamIDs(drained))

	// A registration after drain is refused and leaves nothing behind.
	require.False(t, r.add("s3", localStream{stream: &fakeStream{id: "s3"}, userID: userID(3)}, keys))
	for _, key := range keys {
		require.False(t, streamIDs(targets(r, key, nil))["s3"])
	}

	// Existing streams stay resolvable until their handlers remove them, and a
	// repeat drain sees what remains.
	r.remove("s1", keys)
	require.Equal(t, map[string]bool{"s2": true}, streamIDs(r.drain()))
}

// TestStreamRegistry_AddRolledBackWhenDrainInterleaves pins the partial-
// registration case: draining flips while a multi-shard add is in flight, so
// the keys already landed must be removed again rather than left behind as
// unreachable-but-registered entries.
func TestStreamRegistry_AddRolledBackWhenDrainInterleaves(t *testing.T) {
	r := newStreamRegistry()
	keys := manyKeys("chat", 4*registryShards)

	// Pre-populate so the drain sweep has something to return, and hold one
	// shard's lock so an add must block on it.
	require.True(t, r.add("s0", localStream{stream: &fakeStream{id: "s0"}, userID: userID(0)}, keys[:1]))
	blocked := r.shardFor(keys[0])
	blocked.mu.Lock()

	addDone := make(chan bool)
	go func() {
		addDone <- r.add("s1", localStream{stream: &fakeStream{id: "s1"}, userID: userID(1)}, keys)
	}()

	// Let the add land on whichever shards it reaches before the blocked one,
	// then flip draining under it and release the lock.
	time.Sleep(20 * time.Millisecond)
	r.draining.Store(true)
	blocked.mu.Unlock()

	require.False(t, <-addDone)
	for _, key := range keys {
		require.False(t, streamIDs(targets(r, key, nil))["s1"], "rolled-back add left key %s registered", key)
	}
	require.Equal(t, map[string]bool{"s0": true}, streamIDs(r.drain()))
}

// TestStreamRegistry_Concurrent hammers registration, removal and delivery
// snapshots across shards; it exists for the race detector.
func TestStreamRegistry_Concurrent(t *testing.T) {
	r := newStreamRegistry()
	keys := manyKeys("chat", 2*registryShards)

	var wg sync.WaitGroup
	for i := range 32 {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			id := fmt.Sprintf("s%d", i)
			ls := localStream{stream: &fakeStream{id: id}, userID: userID(i)}
			for range 50 {
				require.True(t, r.add(id, ls, keys))
				for _, key := range keys[:8] {
					r.each(key, []*commonpb.UserId{userID(i)}, func(Stream[*eventpb.Event]) {})
				}
				r.remove(id, keys)
			}
		}(i)
	}
	wg.Wait()

	for _, key := range keys {
		require.Empty(t, targets(r, key, nil))
	}
	require.Empty(t, r.drain())
}

// targets collects what each visits, so the tests can assert on the set of
// streams a key resolves to.
func targets(r *streamRegistry, key string, exclude []*commonpb.UserId) []Stream[*eventpb.Event] {
	var out []Stream[*eventpb.Event]
	r.each(key, exclude, func(s Stream[*eventpb.Event]) { out = append(out, s) })
	return out
}

// TestStreamRegistry_EachDeliversUnderLock pins the delivery pass: it runs
// under the shard's read lock with real streams, a Notify that lag-closes a
// stream mid-pass neither blocks nor deadlocks, and registrations racing the
// pass (which need the write lock) still complete.
func TestStreamRegistry_EachDeliversUnderLock(t *testing.T) {
	r := newStreamRegistry()
	const key = "chat:hot"
	e := &eventpb.Event{Id: MustGenerateEventID()}

	// A one-slot stream lag-closes on its second notify; a roomy one never does.
	laggy := NewEventStream[*eventpb.Event]("laggy", 1)
	healthy := NewEventStream[*eventpb.Event]("healthy", 8)
	require.True(t, r.add("laggy", localStream{stream: laggy, userID: userID(1)}, []string{key}))
	require.True(t, r.add("healthy", localStream{stream: healthy, userID: userID(2)}, []string{key}))

	deliver := func() (errs int) {
		r.each(key, nil, func(s Stream[*eventpb.Event]) {
			if err := s.Notify(e); err != nil {
				errs++
			}
		})
		return errs
	}

	require.Equal(t, 0, deliver())
	require.Equal(t, 1, deliver(), "the laggy stream closes on overflow, inside the locked pass")
	require.Equal(t, 1, deliver(), "a closed stream keeps erroring until its handler removes it")

	// The healthy stream got every event; the laggy one got its one slot then
	// the close.
	require.Len(t, healthy.Channel(), 3)
	<-laggy.Channel()
	_, ok := <-laggy.Channel()
	require.False(t, ok)

	// Deliveries and registrations interleave freely.
	var wg sync.WaitGroup
	for i := range 8 {
		wg.Add(2)
		go func() {
			defer wg.Done()
			for range 100 {
				deliver()
			}
		}()
		go func(i int) {
			defer wg.Done()
			id := fmt.Sprintf("s%d", i)
			for range 100 {
				require.True(t, r.add(id, localStream{stream: NewEventStream[*eventpb.Event](id, 1024), userID: userID(10 + i)}, []string{key}))
				r.remove(id, []string{key})
			}
		}(i)
	}
	wg.Wait()
	require.Equal(t, map[string]bool{"laggy": true, "healthy": true}, streamIDs(targets(r, key, nil)))
}
