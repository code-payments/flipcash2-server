package event

import (
	"fmt"
	"sort"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	eventpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/event/v1"

	"github.com/code-payments/flipcash2-server/cluster"
)

func newTestSession(id string, user int, r *streamRegistry) *streamSession {
	return newStreamSession(id, localStream{stream: &fakeStream{id: id}, userID: userID(user)}, fmt.Sprintf("user:%d", user), r)
}

// TestStreamSession_TopicsTrackRegistry pins that a session and the registry
// never disagree: a key is registered exactly while the session holds it,
// through open, attach, detach and close.
func TestStreamSession_TopicsTrackRegistry(t *testing.T) {
	r := newStreamRegistry()
	s := newTestSession("s1", 1, r)

	require.True(t, s.open())
	require.Equal(t, map[string]bool{"s1": true}, streamIDs(targets(r, s.userKey, nil)))
	attached, released := s.seed([]topicSeed{{key: "chat:a", joined: true}})
	require.Equal(t, []string{"chat:a"}, attached)
	require.Empty(t, released)
	require.Equal(t, map[string]bool{"s1": true}, streamIDs(targets(r, "chat:a", nil)))

	// Attaching registers the key at once, ahead of its cluster handle.
	require.True(t, s.attach("chat:b", 1))
	require.Equal(t, map[string]bool{"s1": true}, streamIDs(targets(r, "chat:b", nil)))

	// Re-attaching a held key is refused, so a racing duplicate does nothing
	// to the registry — though its version is still recorded as the latest
	// known state of that chat.
	require.False(t, s.attach("chat:b", 2))
	require.False(t, s.attach("chat:a", 1))

	// Detaching deregisters; a key never held is a no-op.
	require.Nil(t, s.detach("chat:a", 2))
	require.Empty(t, targets(r, "chat:a", nil))
	require.Nil(t, s.detach("chat:zzz", 1))

	// Close deregisters everything left and latches the session.
	s.close()
	require.Empty(t, targets(r, s.userKey, nil))
	require.Empty(t, targets(r, "chat:b", nil))
	require.False(t, s.attach("chat:c", 1))
	require.Empty(t, targets(r, "chat:c", nil))
}

// TestStreamSession_HandleOwnership pins who releases a cluster handle: the
// session once it has taken it (returned from detach or close), the caller
// when the session refuses it because the stream ended or left the key while
// the registration was in flight.
func TestStreamSession_HandleOwnership(t *testing.T) {
	r := newStreamRegistry()
	s := newTestSession("s1", 1, r)
	require.True(t, s.open())

	userHandle := &cluster.SubscriptionHandle{}
	require.Empty(t, s.setHandles(userHandle, nil, nil))

	// A handle set after the key was detached is refused: the leave won the
	// race, and the registrant releases it.
	require.True(t, s.attach("chat:a", 1))
	require.Nil(t, s.detach("chat:a", 2))
	require.False(t, s.setHandle("chat:a", &cluster.SubscriptionHandle{}))

	// A handle set on a live key is taken, and comes back out of detach.
	handleB := &cluster.SubscriptionHandle{}
	require.True(t, s.attach("chat:b", 1))
	require.True(t, s.setHandle("chat:b", handleB))
	require.Same(t, handleB, s.detach("chat:b", 2))

	// Two registrations in flight for one key — the stream left and rejoined
	// between their attaches — complete in either order: the first to land is
	// kept, the second is refused and its registrant releases it, so the key
	// holds exactly one handle and neither is stranded.
	handleE := &cluster.SubscriptionHandle{}
	require.True(t, s.attach("chat:e", 1))
	require.Nil(t, s.detach("chat:e", 2))
	require.True(t, s.attach("chat:e", 3))
	require.True(t, s.setHandle("chat:e", handleE))
	require.False(t, s.setHandle("chat:e", &cluster.SubscriptionHandle{}))
	require.Same(t, handleE, s.detach("chat:e", 4))

	// The same two registrations, but the second fails after the first has
	// landed its handle: the rollback leaves the attachment standing — the
	// stream is under the key and registered, as the rejoin intends — and
	// the landed handle is still the session's to release, not stranded.
	handleF := &cluster.SubscriptionHandle{}
	require.True(t, s.attach("chat:f", 1))
	require.Nil(t, s.detach("chat:f", 2))
	require.True(t, s.attach("chat:f", 3))
	require.True(t, s.setHandle("chat:f", handleF))
	s.rollback("chat:f", 3)
	require.Equal(t, map[string]bool{"s1": true}, streamIDs(targets(r, "chat:f", nil)))
	require.False(t, s.attach("chat:f", 3))
	require.Same(t, handleF, s.detach("chat:f", 4))

	// Close returns every handle held — the user's and each attached chat's —
	// but not one still in flight, which its registrant will find refused.
	handleC := &cluster.SubscriptionHandle{}
	require.True(t, s.attach("chat:c", 1))
	require.True(t, s.setHandle("chat:c", handleC))
	require.True(t, s.attach("chat:d", 1))
	handles := s.close()
	require.ElementsMatch(t, []*cluster.SubscriptionHandle{userHandle, handleC}, handles)
	require.False(t, s.setHandle("chat:d", &cluster.SubscriptionHandle{}))
}

// TestStreamSession_VersionGate pins that transitions apply as state, by
// roster version: an older one is dropped whatever order it arrives in, a
// version is remembered across a detach, and a rolled-back attach restores
// the gate it displaced so the same join can be retried and nothing older
// can.
func TestStreamSession_VersionGate(t *testing.T) {
	r := newStreamRegistry()
	s := newTestSession("s1", 1, r)
	require.True(t, s.open())
	s.setHandles(&cluster.SubscriptionHandle{}, nil, nil)
	on := func(key string) bool { return len(targets(r, key, nil)) == 1 }

	// Join v3, then a stale leave v2: the leave is dropped, the stream stays on.
	require.True(t, s.attach("chat:a", 3))
	require.True(t, s.setHandle("chat:a", &cluster.SubscriptionHandle{}))
	require.Nil(t, s.detach("chat:a", 2))
	require.True(t, on("chat:a"))

	// A duplicate of the same join is dropped by version before the key check.
	require.False(t, s.attach("chat:a", 3))

	// Leave v4 applies; then a stale rejoin v3 is dropped even though the key
	// is free — the version survives the detach.
	require.NotNil(t, s.detach("chat:a", 4))
	require.False(t, on("chat:a"))
	require.False(t, s.attach("chat:a", 3))
	require.False(t, on("chat:a"))

	// The other order: a leave v6 arrives before the join v5 it followed. The
	// leave is a no-op (not on the key) but records v6, so the late join is
	// dropped and the stream ends where the roster is.
	require.Nil(t, s.detach("chat:a", 6))
	require.False(t, s.attach("chat:a", 5))
	require.False(t, on("chat:a"))

	// A newer join applies; versions are per chat key.
	require.True(t, s.attach("chat:a", 7))
	require.True(t, on("chat:a"))
	require.True(t, s.attach("chat:b", 1))

	// A rollback releases the key and forgets a version nothing preceded, so
	// the same join can land on a retry; a rollback of a key not held is
	// harmless.
	require.True(t, s.attach("chat:c", 2))
	s.rollback("chat:c", 2)
	require.False(t, on("chat:c"))
	require.True(t, s.attach("chat:c", 2))
	require.True(t, on("chat:c"))
	s.rollback("chat:zzz", 1)

	// A rollback restores the gate the attach displaced rather than
	// forgetting it: chat:g was left at v3 (seeded), a join at v7 fails to
	// register, and afterwards a stale copy of the v2 join it once undid is
	// still stale, the v3 leave still applied, and the v7 join retryable.
	_, _ = s.seed([]topicSeed{{key: "chat:g", version: 3, joined: false}})
	require.True(t, s.attach("chat:g", 7))
	s.rollback("chat:g", 7)
	require.False(t, on("chat:g"))
	require.False(t, s.attach("chat:g", 2))
	require.Nil(t, s.detach("chat:g", 3))
	require.False(t, on("chat:g"))
	require.True(t, s.attach("chat:g", 7))
	require.True(t, on("chat:g"))

	// A rollback of an attach the key has since moved past does nothing: the
	// v1 join's registration failed, but a v2 leave and v3 rejoin landed
	// first. The v3 attach stands, its version is kept, and the v1 join
	// cannot be retried over it.
	require.True(t, s.attach("chat:d", 1))
	require.Nil(t, s.detach("chat:d", 2))
	require.True(t, s.attach("chat:d", 3))
	s.rollback("chat:d", 1)
	require.True(t, on("chat:d"))
	require.False(t, s.attach("chat:d", 1))
	require.Nil(t, s.detach("chat:d", 2))
	require.True(t, s.setHandle("chat:d", &cluster.SubscriptionHandle{}))
	require.NotNil(t, s.detach("chat:d", 4))
	require.False(t, on("chat:d"))
}

// TestStreamSession_RollbackAfterSupersededAttach pins that a rollback is
// gated on the attach whose registration failed, not on the version last
// applied: a newer join refused as a duplicate while the registration was in
// flight — delivered as a transition, or read by the open's seed — shares
// that registration, so its failure still takes the stream off the key and
// restores the displaced gate, and the reconcile can then re-attach at the
// newer version. Gating on the version would leave the key held with no
// handle and refuse every repair.
func TestStreamSession_RollbackAfterSupersededAttach(t *testing.T) {
	// Each case gets its own registry, so a key one leaves attached cannot
	// stand in for another's.
	newCase := func(id string, user int) (*streamSession, func(key string) bool) {
		r := newStreamRegistry()
		s := newTestSession(id, user, r)
		require.True(t, s.open())
		return s, func(key string) bool { return len(targets(r, key, nil)) == 1 }
	}

	t.Run("transition", func(t *testing.T) {
		s, on := newCase("s1", 1)

		// A v1 join's registration is in flight when a v3 rejoin arrives (its
		// v2 leave delayed or lost): refused as a duplicate, v3 recorded.
		require.True(t, s.attach("chat:a", 1))
		require.False(t, s.attach("chat:a", 3))
		require.True(t, on("chat:a"))

		// The v1 registration fails: the stream comes off the key, and the
		// gate goes back to nothing, so the reconcile's read (joined at v3)
		// re-attaches and lands its handle.
		s.rollback("chat:a", 1)
		require.False(t, on("chat:a"))
		require.NotContains(t, s.displaced, "chat:a")
		require.True(t, s.attach("chat:a", 3))
		require.True(t, on("chat:a"))
		handle := &cluster.SubscriptionHandle{}
		require.True(t, s.setHandle("chat:a", handle))
		require.Same(t, handle, s.detach("chat:a", 4))

		// The delayed v2 leave arriving before the repair is harmless: it
		// records v2 off the key, and the v3 join still applies over it.
		require.True(t, s.attach("chat:b", 1))
		require.False(t, s.attach("chat:b", 3))
		s.rollback("chat:b", 1)
		require.Nil(t, s.detach("chat:b", 2))
		require.False(t, on("chat:b"))
		require.True(t, s.attach("chat:b", 3))
		require.True(t, on("chat:b"))

		// A stale copy of the failed v1 join can retry, and a later v3 attach
		// refused as its duplicate still ends registered by the v1 handle.
		require.True(t, s.attach("chat:c", 1))
		require.False(t, s.attach("chat:c", 3))
		s.rollback("chat:c", 1)
		require.True(t, s.attach("chat:c", 1))
		require.False(t, s.attach("chat:c", 3))
		require.True(t, s.setHandle("chat:c", &cluster.SubscriptionHandle{}))
		require.True(t, on("chat:c"))
	})

	t.Run("seed", func(t *testing.T) {
		s, on := newCase("s2", 2)

		// The open's read reflects a v3 rejoin the v1 transition's in-flight
		// registration predates: nothing to attach, v3 recorded.
		require.True(t, s.attach("chat:a", 1))
		attached, released := s.seed([]topicSeed{{key: "chat:a", version: 3, joined: true}})
		require.Empty(t, attached)
		require.Empty(t, released)

		s.rollback("chat:a", 1)
		require.False(t, on("chat:a"))
		require.True(t, s.attach("chat:a", 3))
		require.True(t, on("chat:a"))
	})

	t.Run("settled key is left alone", func(t *testing.T) {
		s, on := newCase("s3", 3)

		// Detached meanwhile: the leave's state stands, and its version.
		require.True(t, s.attach("chat:a", 1))
		require.Nil(t, s.detach("chat:a", 2))
		s.rollback("chat:a", 1)
		require.False(t, on("chat:a"))
		require.False(t, s.attach("chat:a", 1))
		require.True(t, s.attach("chat:a", 3))

		// Landed by an earlier registrant meanwhile: the handle stands.
		handle := &cluster.SubscriptionHandle{}
		require.True(t, s.attach("chat:b", 1))
		require.Nil(t, s.detach("chat:b", 2))
		require.True(t, s.attach("chat:b", 3))
		require.True(t, s.setHandle("chat:b", handle))
		s.rollback("chat:b", 3)
		require.True(t, on("chat:b"))
		require.Same(t, handle, s.detach("chat:b", 4))
	})
}

// TestStreamSession_SeededVersions pins that the versions an open seeds from
// the membership records gate what follows: a transition the record already
// reflects is stale from the first event, and a newer one applies. A
// membership from creation seeds at zero, so its first transition applies. A
// departed record seeds its version without registering its key, so a delayed
// copy of the join it superseded cannot reattach the stream.
func TestStreamSession_SeededVersions(t *testing.T) {
	r := newStreamRegistry()
	s := newTestSession("s1", 1, r)
	require.True(t, s.open())
	attached, released := s.seed([]topicSeed{
		{key: "chat:a", version: 3, joined: true},
		{key: "chat:b", version: 0, joined: true},
		{key: "chat:c", version: 4, joined: false},
	})
	require.Equal(t, []string{"chat:a", "chat:b"}, attached)
	require.Empty(t, released)
	require.Empty(t, s.setHandles(&cluster.SubscriptionHandle{}, attached, []*cluster.SubscriptionHandle{{}, {}}))
	on := func(key string) bool { return len(targets(r, key, nil)) == 1 }

	// chat:c was left at v4: the stream is not under it, the v3 join it
	// undid is stale, and only a newer join puts the stream on.
	require.False(t, on("chat:c"))
	require.False(t, s.attach("chat:c", 3))
	require.Nil(t, s.detach("chat:c", 4))
	require.False(t, on("chat:c"))
	require.True(t, s.attach("chat:c", 5))
	require.True(t, on("chat:c"))

	// chat:a was last moved at v3: a delayed leave from v2 — or a replay of
	// the v3 join itself — cannot take the stream off it.
	require.Nil(t, s.detach("chat:a", 2))
	require.Nil(t, s.detach("chat:a", 3))
	require.True(t, on("chat:a"))

	// A leave at v4 is news, and applies.
	require.NotNil(t, s.detach("chat:a", 4))
	require.False(t, on("chat:a"))

	// chat:b is a creation-time membership at v0: the group's first
	// transition applies.
	require.NotNil(t, s.detach("chat:b", 1))
	require.False(t, on("chat:b"))
}

// TestStreamSession_SeedMergesWithTransitions pins that the records read at
// open merge with the transitions delivered while the read was in flight,
// through the version gate: a record a transition already superseded is
// stale, a record newer than what was applied wins — releasing a handle a
// transition's registration had landed — and the open's batch of handles is
// refused for any key a transition moved meanwhile.
func TestStreamSession_SeedMergesWithTransitions(t *testing.T) {
	r := newStreamRegistry()
	s := newTestSession("s1", 1, r)
	require.True(t, s.open())
	on := func(key string) bool { return len(targets(r, key, nil)) == 1 }

	// Before the read lands: a join on chat:a fully registered (v7), a leave
	// on chat:b (v4), and a join on chat:d whose registration is in flight.
	handleA := &cluster.SubscriptionHandle{}
	require.True(t, s.attach("chat:a", 7))
	require.True(t, s.setHandle("chat:a", handleA))
	require.Nil(t, s.detach("chat:b", 4))
	require.True(t, s.attach("chat:d", 2))

	// The read: chat:a departed at v3 (predates the join: stale), chat:b
	// joined at v3 (predates the leave: stale), chat:c joined at v1 (news),
	// chat:d joined at v2 (the same transition: stale), chat:e departed at
	// v9 (news, but nothing to take off).
	attached, released := s.seed([]topicSeed{
		{key: "chat:a", version: 3, joined: false},
		{key: "chat:b", version: 3, joined: true},
		{key: "chat:c", version: 1, joined: true},
		{key: "chat:d", version: 2, joined: true},
		{key: "chat:e", version: 9, joined: false},
	})
	require.Equal(t, []string{"chat:c"}, attached)
	require.Empty(t, released)
	require.True(t, on("chat:a"))
	require.False(t, on("chat:b"))
	require.True(t, on("chat:c"))
	require.True(t, on("chat:d"))
	require.False(t, on("chat:e"))

	// A record newer than the applied transition wins: chat:a departed at v8
	// takes the stream off and hands back the handle the join had landed.
	attached, released = s.seed([]topicSeed{{key: "chat:a", version: 8, joined: false}})
	require.Empty(t, attached)
	require.Equal(t, []*cluster.SubscriptionHandle{handleA}, released)
	require.False(t, on("chat:a"))

	// So does one that detaches a key whose registration is still in flight:
	// chat:d departed at v3 takes the stream off with no handle to return,
	// and settles the key — the attach's displaced watermark goes with it, as
	// a detach would drop it, so the registrant's rollback has nothing to
	// restore and the handle it lands is refused.
	require.Contains(t, s.displaced, "chat:d")
	attached, released = s.seed([]topicSeed{{key: "chat:d", version: 3, joined: false}})
	require.Empty(t, attached)
	require.Empty(t, released)
	require.False(t, on("chat:d"))
	require.NotContains(t, s.displaced, "chat:d")
	require.False(t, s.setHandle("chat:d", &cluster.SubscriptionHandle{}))

	// The batch lands for chat:c — but a leave at v2 detached it meanwhile,
	// so its handle is refused for the caller to release; the user handle is
	// taken regardless.
	require.Nil(t, s.detach("chat:c", 2))
	userHandle, handleC := &cluster.SubscriptionHandle{}, &cluster.SubscriptionHandle{}
	require.Equal(t, []*cluster.SubscriptionHandle{handleC}, s.setHandles(userHandle, []string{"chat:c"}, []*cluster.SubscriptionHandle{handleC}))
	require.Same(t, userHandle, s.close()[0])

	// A closed session applies nothing and refuses every handle.
	attached, released = s.seed([]topicSeed{{key: "chat:f", version: 1, joined: true}})
	require.Empty(t, attached)
	require.Empty(t, released)
	require.False(t, on("chat:f"))
	require.Len(t, s.setHandles(&cluster.SubscriptionHandle{}, []string{"chat:f"}, []*cluster.SubscriptionHandle{{}}), 2)
}

// TestStreamSession_OpenRefusedWhenDraining pins that a draining registry
// refuses an open, a seed and an attach alike, with nothing registered either
// way.
func TestStreamSession_OpenRefusedWhenDraining(t *testing.T) {
	r := newStreamRegistry()
	live := newTestSession("live", 1, r)
	require.True(t, live.open())

	r.drain()

	late := newTestSession("late", 2, r)
	require.False(t, late.open())
	require.Empty(t, targets(r, late.userKey, nil))

	attached, released := live.seed([]topicSeed{{key: "chat:a", joined: true}})
	require.Empty(t, attached)
	require.Empty(t, released)
	require.Empty(t, targets(r, "chat:a", nil))

	require.False(t, live.attach("chat:a", 1))
	require.Empty(t, targets(r, "chat:a", nil))
}

// TestStreamSessions_Index pins the by-user index: every open session of a
// user, none of another's, and nothing once the last is removed.
func TestStreamSessions_Index(t *testing.T) {
	r := newStreamRegistry()
	x := newStreamSessions()

	s1 := newTestSession("s1", 1, r)
	s2 := newTestSession("s2", 1, r)
	other := newTestSession("o1", 2, r)
	for _, s := range []*streamSession{s1, s2, other} {
		x.add(s)
	}

	ids := func(sessions []*streamSession) map[string]bool {
		out := make(map[string]bool, len(sessions))
		for _, s := range sessions {
			out[s.id] = true
		}
		return out
	}
	require.Equal(t, map[string]bool{"s1": true, "s2": true}, ids(x.forUser(s1.userKey)))
	require.Equal(t, map[string]bool{"o1": true}, ids(x.forUser(other.userKey)))
	require.Empty(t, x.forUser("user:nobody"))

	x.remove(s1)
	require.Equal(t, map[string]bool{"s2": true}, ids(x.forUser(s1.userKey)))
	x.remove(s2)
	require.Empty(t, x.forUser(s1.userKey))
	require.NotContains(t, x.byUser, s1.userKey)

	// Removing twice is harmless.
	x.remove(s2)
}

// TestStreamSessions_Due pins the reconcile's schedule: a user is due an
// interval after their first session opened, then an interval after each
// reconcile's read began, rationed to the quota, least recently reconciled
// first. A later session of theirs does not reset the schedule.
func TestStreamSessions_Due(t *testing.T) {
	r := newStreamRegistry()
	x := newStreamSessions()
	clock := time.Date(2026, 9, 15, 12, 0, 0, 0, time.UTC)
	x.now = func() time.Time { return clock }
	const interval = time.Minute
	due := func(quota int) []string {
		keys := x.due(clock, interval, quota)
		sort.Strings(keys)
		return keys
	}

	// Nothing open: nothing due.
	require.Empty(t, due(10))

	// Just opened: the open's own read counts, so the user is due an interval
	// later, not before, and stays due until reconciled.
	s1 := newTestSession("s1", 1, r)
	x.add(s1)
	require.Empty(t, due(10))
	clock = clock.Add(interval - time.Millisecond)
	require.Empty(t, due(10))
	clock = clock.Add(time.Millisecond)
	require.Equal(t, []string{s1.userKey}, due(10))
	clock = clock.Add(time.Hour)
	require.Equal(t, []string{s1.userKey}, due(10))

	// Reconciled: due again an interval after the read began, not before.
	x.markReconciled(s1.userKey, clock)
	clock = clock.Add(interval - time.Millisecond)
	require.Empty(t, due(10))
	clock = clock.Add(time.Millisecond)
	require.Equal(t, []string{s1.userKey}, due(10))
	x.markReconciled(s1.userKey, clock)

	// A second session opening does not move the user's schedule: it reads
	// its own memberships at open.
	clock = clock.Add(interval / 2)
	s2 := newTestSession("s2", 1, r)
	x.add(s2)
	clock = clock.Add(interval / 2)
	require.Equal(t, []string{s1.userKey}, due(10))
	x.markReconciled(s1.userKey, clock)

	// Rationed, least recently reconciled first.
	for i := 2; i <= 5; i++ {
		s := newTestSession(fmt.Sprintf("s%d", i), i, r)
		clock = clock.Add(time.Second)
		x.add(s)
	}
	clock = clock.Add(interval)
	require.Equal(t, []string{"user:1", "user:2"}, due(2))
	require.Equal(t, []string{"user:1", "user:2", "user:3", "user:4", "user:5"}, due(10))
	x.markReconciled("user:1", clock)
	x.markReconciled("user:2", clock)
	require.Equal(t, []string{"user:3", "user:4"}, due(2))

	// A user whose last session closed is gone from the schedule; marking
	// them reconciled afterwards does not bring them back.
	s5 := newTestSession("s5", 5, r)
	x.remove(s5)
	x.markReconciled(s5.userKey, clock)
	require.NotContains(t, x.byUser, s5.userKey)
	require.Equal(t, []string{"user:3", "user:4"}, due(10))
}

var _ Stream[*eventpb.Event] = (*fakeStream)(nil)
