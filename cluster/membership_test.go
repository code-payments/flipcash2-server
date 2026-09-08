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
