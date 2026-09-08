package cluster

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

// fakeRegistry serves a controllable member list, so liveness observations can
// be scripted without a real store or heartbeat loops.
type fakeRegistry struct {
	mu      sync.Mutex
	records []*MemberRecord
}

func (f *fakeRegistry) setCounter(instanceID string, counter uint64) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, r := range f.records {
		if r.InstanceID == instanceID {
			r.HeartbeatCounter = counter
			return
		}
	}
	f.records = append(f.records, &MemberRecord{
		Member:           Member{InstanceID: instanceID, Address: instanceID + ".local:8085"},
		HeartbeatCounter: counter,
	})
}

func (f *fakeRegistry) PutMember(_ context.Context, member *Member, counter uint64) error {
	f.setCounter(member.InstanceID, counter)
	return nil
}
func (f *fakeRegistry) Heartbeat(context.Context, string) (uint64, error) { return 1, nil }
func (f *fakeRegistry) SetDraining(context.Context, string, bool) error   { return nil }
func (f *fakeRegistry) DeleteMember(context.Context, string) error        { return nil }
func (f *fakeRegistry) GetMembers(context.Context) ([]*MemberRecord, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]*MemberRecord, len(f.records))
	for i, r := range f.records {
		clone := *r
		out[i] = &clone
	}
	return out, nil
}

func TestSuspicionWindow_FlooredAboveHeartbeatJitter(t *testing.T) {
	registry := &fakeRegistry{}
	m := NewMembership(zap.NewNop(), registry, &Member{InstanceID: "a"}, MembershipConfig{
		HeartbeatInterval: 5 * time.Second,
		PollInterval:      2 * time.Second,
	})

	// A window at (or below) the heartbeat cadence leaves zero tolerance for
	// a beat that merely lands late: it must be floored to heartbeat + poll.
	o := NewOwnership(zap.NewNop(), m, nil, nil, OwnershipConfig{SuspicionWindow: 5 * time.Second})
	require.Equal(t, 7*time.Second, o.cfg.SuspicionWindow)

	// An explicitly larger window is respected.
	o = NewOwnership(zap.NewNop(), m, nil, nil, OwnershipConfig{SuspicionWindow: 10 * time.Second})
	require.Equal(t, 10*time.Second, o.cfg.SuspicionWindow)
}

func TestTakeoverEvidence_ReportInvalidatedByObservedBeat(t *testing.T) {
	ctx := context.Background()

	registry := &fakeRegistry{}
	m := NewMembership(zap.NewNop(), registry, &Member{InstanceID: "a"}, MembershipConfig{
		// Tiny cadences keep the suspicion floor below the window under test.
		HeartbeatInterval: 5 * time.Millisecond,
		PollInterval:      5 * time.Millisecond,
		LivenessWindow:    time.Minute, // Suspicion, not expiry, must decide.
	})
	o := NewOwnership(zap.NewNop(), m, nil, nil, OwnershipConfig{SuspicionWindow: 30 * time.Millisecond})
	require.Equal(t, 30*time.Millisecond, o.cfg.SuspicionWindow)

	registry.setCounter("b", 1)
	require.NoError(t, m.Refresh(ctx))

	// A forward to b fails and is reported — but then a beat is observed.
	o.NoteUnreachable("b")
	time.Sleep(10 * time.Millisecond)
	registry.setCounter("b", 2)
	require.NoError(t, m.Refresh(ctx))

	// b's counter now sits still past the suspicion window, and the report is
	// still recent — but the beat observed AFTER the report proved b alive
	// and store-reachable, so the stale report must not corroborate a
	// takeover.
	time.Sleep(50 * time.Millisecond)
	require.NoError(t, m.Refresh(ctx))
	require.Nil(t, o.takeoverEvidence("b"), "beat observed after the report must invalidate it")

	// A fresh report filed with no beat observed since does corroborate.
	o.NoteUnreachable("b")
	time.Sleep(50 * time.Millisecond)
	evidence := o.takeoverEvidence("b")
	require.NotNil(t, evidence)
	require.EqualValues(t, 2, evidence.HeartbeatCounter)
}

func TestOwnedKeyID_Unambiguous(t *testing.T) {
	// Arbitrary key bytes must never collide across (namespace, key) pairs:
	// with a bare separator, ("chat", "x/y") and ("chat/x", "y") would map to
	// one in-process entry, cross-wiring two claims' local state.
	pairs := [][2]struct {
		ns  string
		key string
	}{
		{{"chat", "x/y"}, {"chat/x", "y"}},
		{{"a", "bc"}, {"ab", "c"}},
		{{"chat", "1|x"}, {"chat1", "|x"}},
		{{"", "chat"}, {"chat", ""}},
	}
	for _, p := range pairs {
		idA := ownedKeyID(p[0].ns, []byte(p[0].key))
		idB := ownedKeyID(p[1].ns, []byte(p[1].key))
		if idA == idB {
			t.Fatalf("collision: (%q,%q) and (%q,%q) both map to %q", p[0].ns, p[0].key, p[1].ns, p[1].key, idA)
		}
	}
}

func TestWaitCond_TimerWakeupNeverLost(t *testing.T) {
	// A near-zero timeout races the timer's broadcast against Wait enqueueing
	// the waiter. With no other broadcaster, a lost wakeup blocks forever —
	// the release() shape whenever time.Until(deadline) goes tiny near the
	// drain deadline with wedged work in flight.
	done := make(chan struct{})
	go func() {
		defer close(done)
		var mu sync.Mutex
		cond := sync.NewCond(&mu)
		mu.Lock()
		defer mu.Unlock()
		for range 50_000 {
			waitCond(cond, time.Nanosecond)
		}
	}()

	select {
	case <-done:
	case <-time.After(30 * time.Second):
		t.Fatal("waitCond lost the timer broadcast and blocked forever")
	}
}
