package event

import (
	"fmt"
	"testing"

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

	require.True(t, s.open([]topicSeed{{key: "chat:a", joined: true}}))
	require.Equal(t, map[string]bool{"s1": true}, streamIDs(targets(r, s.userKey, nil)))
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
	require.True(t, s.open(nil))

	userHandle := &cluster.SubscriptionHandle{}
	s.setHandles(userHandle, nil, nil)

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
// version is remembered across a detach, and a rolled-back attach forgets
// its version so the same join can be retried.
func TestStreamSession_VersionGate(t *testing.T) {
	r := newStreamRegistry()
	s := newTestSession("s1", 1, r)
	require.True(t, s.open(nil))
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

	// A rollback releases the key and forgets the version, so the same join
	// can land on a retry; a rollback of a key not held is harmless.
	require.True(t, s.attach("chat:c", 2))
	s.rollback("chat:c", 2)
	require.False(t, on("chat:c"))
	require.True(t, s.attach("chat:c", 2))
	require.True(t, on("chat:c"))
	s.rollback("chat:zzz", 1)

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

// TestStreamSession_SeededVersions pins that the versions an open seeds from
// the membership records gate what follows: a transition the record already
// reflects is stale from the first event, and a newer one applies. A
// membership from creation seeds at zero, so its first transition applies. A
// departed record seeds its version without registering its key, so a delayed
// copy of the join it superseded cannot reattach the stream.
func TestStreamSession_SeededVersions(t *testing.T) {
	r := newStreamRegistry()
	s := newTestSession("s1", 1, r)
	require.True(t, s.open([]topicSeed{
		{key: "chat:a", version: 3, joined: true},
		{key: "chat:b", version: 0, joined: true},
		{key: "chat:c", version: 4, joined: false},
	}))
	s.setHandles(&cluster.SubscriptionHandle{}, []string{"chat:a", "chat:b"}, []*cluster.SubscriptionHandle{{}, {}})
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

// TestStreamSession_OpenRefusedWhenDraining pins that a draining registry
// refuses an open and an attach alike, with nothing registered either way.
func TestStreamSession_OpenRefusedWhenDraining(t *testing.T) {
	r := newStreamRegistry()
	live := newTestSession("live", 1, r)
	require.True(t, live.open(nil))

	r.drain()

	late := newTestSession("late", 2, r)
	require.False(t, late.open([]topicSeed{{key: "chat:a", joined: true}}))
	require.Empty(t, targets(r, late.userKey, nil))
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

var _ Stream[*eventpb.Event] = (*fakeStream)(nil)
