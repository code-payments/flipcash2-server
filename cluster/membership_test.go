package cluster_test

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/code-payments/flipcash2-server/cluster"
	"github.com/code-payments/flipcash2-server/cluster/memory"
)

// refreshFailingStore fails GetMembers, so Membership.Start registers
// successfully but cannot complete its initial refresh.
type refreshFailingStore struct {
	cluster.Store
}

func (s *refreshFailingStore) GetMembers(context.Context) ([]*cluster.MemberRecord, error) {
	return nil, errors.New("injected refresh failure")
}

func TestMembership_ObserverSeesMembersWithoutRegistering(t *testing.T) {
	ctx := context.Background()
	backing := memory.NewInMemory()

	member := &cluster.Member{InstanceID: "instance-a", Address: "10.0.0.1:8085"}
	require.NoError(t, backing.PutMember(ctx, member, 1))

	observer := cluster.NewObserver(zap.NewNop(), backing, cluster.MembershipConfig{})
	require.NoError(t, observer.Start(ctx))
	defer observer.Stop()

	// The observer sees the registered member but has no identity of its own.
	live := observer.Live()
	require.Len(t, live, 1)
	require.Equal(t, "instance-a", live[0].InstanceID)
	require.Nil(t, observer.Self())
	require.True(t, observer.SelfHealthy())

	// No record was written for the observer.
	records, err := backing.GetMembers(ctx)
	require.NoError(t, err)
	require.Len(t, records, 1)
	require.Equal(t, "instance-a", records[0].InstanceID)

	// The self-referential lifecycle methods are vacuous no-ops that leave the
	// registry untouched.
	require.NoError(t, observer.SetDraining(ctx, true))
	require.NoError(t, observer.Deregister(ctx))
	records, err = backing.GetMembers(ctx)
	require.NoError(t, err)
	require.Len(t, records, 1)
	require.False(t, records[0].Draining)
}

func TestMembership_ObserverSubscribersFireOnChange(t *testing.T) {
	ctx := context.Background()
	backing := memory.NewInMemory()

	// Deliberately not started: Refresh is driven by hand so the poll loop
	// can't race the change counting.
	observer := cluster.NewObserver(zap.NewNop(), backing, cluster.MembershipConfig{})
	require.NoError(t, observer.Refresh(ctx))

	var changes int
	observer.Subscribe(func() { changes++ })

	require.NoError(t, backing.PutMember(ctx, &cluster.Member{InstanceID: "instance-b", Address: "10.0.0.2:8085"}, 1))
	require.NoError(t, observer.Refresh(ctx))
	require.Equal(t, 1, changes)
	require.Len(t, observer.Live(), 1)

	// An unchanged registry fires nothing.
	require.NoError(t, observer.Refresh(ctx))
	require.Equal(t, 1, changes)
}

func TestMembership_ObserverResolvesSubscriptionsWithoutRegistering(t *testing.T) {
	ctx := context.Background()
	backing := memory.NewInMemory()

	member := &cluster.Member{InstanceID: "instance-a", Address: "10.0.0.1:8085"}
	require.NoError(t, backing.PutMember(ctx, member, 1))
	require.NoError(t, backing.PutSubscription(ctx, "chat", []byte("topic-1"), "instance-a"))

	observer := cluster.NewObserver(zap.NewNop(), backing, cluster.MembershipConfig{})
	require.NoError(t, observer.Refresh(ctx))

	subs := cluster.NewSubscriptions(zap.NewNop(), observer, backing, cluster.SubscriptionsConfig{})
	require.Nil(t, subs.Self())

	// The read side works: the member's row resolves with its address stamped
	// from the observer's membership view.
	resolved, err := subs.Subscribers(ctx, "chat", []byte("topic-1"))
	require.NoError(t, err)
	require.Len(t, resolved, 1)
	require.Equal(t, "instance-a", resolved[0].InstanceID)
	require.Equal(t, "10.0.0.1:8085", resolved[0].Address)

	// The write side is refused: an observer has no liveness to back a row.
	_, err = subs.Subscribe(ctx, "chat", []byte("topic-2"))
	require.ErrorIs(t, err, cluster.ErrObserverMembership)
}

func TestMembership_ObserverStartFailureIsUnhealthy(t *testing.T) {
	ctx := context.Background()
	backing := memory.NewInMemory()

	observer := cluster.NewObserver(zap.NewNop(), &refreshFailingStore{Store: backing}, cluster.MembershipConfig{})
	require.Error(t, observer.Start(ctx))
	require.False(t, observer.SelfHealthy())
}

func TestMembership_StartFailureIsUnhealthyAndUnregistered(t *testing.T) {
	ctx := context.Background()
	backing := memory.NewInMemory()

	self := &cluster.Member{InstanceID: "instance-a", Address: "10.0.0.1:8085"}
	m := cluster.NewMembership(zap.NewNop(), &refreshFailingStore{Store: backing}, self, cluster.MembershipConfig{})

	require.Error(t, m.Start(ctx))

	// A failed Start must not report healthy — no heartbeat loop backs it —
	// and must not leave a corpse record for peers to observe and route to.
	require.False(t, m.SelfHealthy())
	records, err := backing.GetMembers(ctx)
	require.NoError(t, err)
	require.Empty(t, records)
}
