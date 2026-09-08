package cluster_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/code-payments/flipcash2-server/cluster"
	"github.com/code-payments/flipcash2-server/cluster/memory"
)

// staticOverrides pins keys (by string form) to label selectors.
type staticOverrides struct {
	pins map[string]map[string]string
}

func (o *staticOverrides) Get(_ context.Context, _ string, key []byte) (map[string]string, bool, error) {
	selector, ok := o.pins[string(key)]
	return selector, ok, nil
}

// newTestRouter registers the given members and returns a router whose
// membership snapshot sees them all as live.
func newTestRouter(t *testing.T, overrides cluster.Overrides, members ...*cluster.Member) *cluster.Router {
	ctx := context.Background()
	store := memory.NewInMemory()
	for _, m := range members {
		require.NoError(t, store.PutMember(ctx, m, 1))
	}

	membership := cluster.NewMembership(zap.NewNop(), store, members[0], cluster.MembershipConfig{})
	require.NoError(t, membership.Refresh(ctx))
	return cluster.NewRouter(membership, overrides)
}

func poolMember(instanceID, pool string) *cluster.Member {
	return &cluster.Member{
		InstanceID: instanceID,
		Address:    instanceID + ".local:8085",
		Labels:     map[string]string{"pool": pool, "region": "us-east-1"},
	}
}

func TestRouter_OverrideSelectorPool(t *testing.T) {
	ctx := context.Background()

	members := []*cluster.Member{
		poolMember("instance-a", "general"),
		poolMember("instance-b", "general"),
		poolMember("instance-xl-1", "xl"),
		poolMember("instance-xl-2", "xl"),
	}
	overrides := &staticOverrides{pins: map[string]map[string]string{
		"mega-chat": {"pool": "xl"},
	}}
	router := newTestRouter(t, overrides, members...)

	// The pinned key lands in the pool, deterministically.
	owner, err := router.Owner(ctx, "chat", []byte("mega-chat"))
	require.NoError(t, err)
	require.Contains(t, []string{"instance-xl-1", "instance-xl-2"}, owner.InstanceID)
	for range 10 {
		again, err := router.Owner(ctx, "chat", []byte("mega-chat"))
		require.NoError(t, err)
		require.Equal(t, owner.InstanceID, again.InstanceID)
	}

	// Unpinned keys route over the whole fleet: with enough keys, members
	// outside the pool win some.
	sawGeneral := false
	for i := 0; i < 64 && !sawGeneral; i++ {
		owner, err := router.Owner(ctx, "chat", fmt.Appendf(nil, "key-%d", i))
		require.NoError(t, err)
		sawGeneral = owner.InstanceID == "instance-a" || owner.InstanceID == "instance-b"
	}
	require.True(t, sawGeneral)
}

func TestRouter_OverrideInstancePin(t *testing.T) {
	ctx := context.Background()

	members := []*cluster.Member{
		poolMember("instance-a", "general"),
		poolMember("instance-b", "general"),
	}
	overrides := &staticOverrides{pins: map[string]map[string]string{
		"pinned": {cluster.OverrideInstanceLabel: "instance-b"},
	}}
	router := newTestRouter(t, overrides, members...)

	owner, err := router.Owner(ctx, "chat", []byte("pinned"))
	require.NoError(t, err)
	require.Equal(t, "instance-b", owner.InstanceID)
}

func TestRouter_OverrideFailsOpen(t *testing.T) {
	ctx := context.Background()

	members := []*cluster.Member{
		poolMember("instance-a", "general"),
		poolMember("instance-b", "general"),
	}
	overrides := &staticOverrides{pins: map[string]map[string]string{
		// The pool this selector names no longer exists (drained away,
		// relabeled, or the pinned incarnation is gone).
		"mega-chat": {"pool": "xl"},
		"stale-pin": {cluster.OverrideInstanceLabel: "instance-dead"},
	}}
	router := newTestRouter(t, overrides, members...)
	unpinned := newTestRouter(t, nil, members...)

	// An unsatisfiable selector is ignored: the key routes exactly as if
	// there were no pin, rather than erroring or going unroutable.
	for _, key := range []string{"mega-chat", "stale-pin"} {
		owner, err := router.Owner(ctx, "chat", []byte(key))
		require.NoError(t, err)
		expected, err := unpinned.Owner(ctx, "chat", []byte(key))
		require.NoError(t, err)
		require.Equal(t, expected.InstanceID, owner.InstanceID)
	}
}

func TestRouter_OverrideRespectsDraining(t *testing.T) {
	ctx := context.Background()

	xl1 := poolMember("instance-xl-1", "xl")
	xl2 := poolMember("instance-xl-2", "xl")
	xl1.Draining = true

	overrides := &staticOverrides{pins: map[string]map[string]string{
		"mega-chat": {"pool": "xl"},
	}}
	router := newTestRouter(t, overrides, poolMember("instance-a", "general"), xl1, xl2)

	// A deploy inside the pool: the draining member is out of candidacy, so
	// the pin resolves to the surviving pool member — never the void.
	owner, err := router.Owner(ctx, "chat", []byte("mega-chat"))
	require.NoError(t, err)
	require.Equal(t, "instance-xl-2", owner.InstanceID)
}
