package cluster

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

// TestConfigDefaults pins the resolved production defaults: every runtime test
// overrides every field, so without this nothing would notice a withDefaults
// edit silently shifting production behavior.
func TestConfigDefaults(t *testing.T) {
	m := MembershipConfig{}.withDefaults()
	require.Equal(t, 5*time.Second, m.HeartbeatInterval)
	require.Equal(t, 2*time.Second, m.PollInterval)
	require.Equal(t, 15*time.Second, m.LivenessWindow)
	require.Equal(t, 15*time.Second, m.SelfUnhealthyAfter)
	require.Equal(t, 7*time.Second, m.SessionGapThreshold)
	require.Equal(t, 150*time.Second, m.MemberGCAfter)

	o := OwnershipConfig{}.withDefaults()
	require.Equal(t, 15*time.Minute, o.IdleTTL)
	require.Equal(t, time.Minute, o.ReapInterval)
	require.Equal(t, 5*time.Second, o.DrainDeadline)
	require.Equal(t, 5*time.Second, o.SuspicionWindow) // Pre-floor; see below.
	require.Equal(t, time.Second, o.RedirectCacheTTL)

	// The effective production suspicion window is floored at construction to
	// heartbeat + poll.
	membership := NewMembership(zap.NewNop(), &fakeRegistry{}, &Member{InstanceID: "a"}, MembershipConfig{})
	ownership := NewOwnership(zap.NewNop(), membership, nil, nil, OwnershipConfig{})
	require.Equal(t, 7*time.Second, ownership.cfg.SuspicionWindow)

	// The invariants the suspicion hardening rests on: jitter tolerance of at
	// least one poll interval, and shedding fired no later than displacement
	// becomes possible.
	require.GreaterOrEqual(t, ownership.cfg.SuspicionWindow, m.HeartbeatInterval+m.PollInterval)
	require.LessOrEqual(t, m.SessionGapThreshold, ownership.cfg.SuspicionWindow)
}
