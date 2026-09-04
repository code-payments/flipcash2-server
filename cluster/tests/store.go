package tests

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/code-payments/flipcash2-server/cluster"
)

// RunStoreTests runs the store contract suite against a cluster.Store
// implementation, calling teardown between tests.
func RunStoreTests(t *testing.T, s cluster.Store, teardown func()) {
	for _, tf := range []func(t *testing.T, s cluster.Store){
		testRegistry,
		testClaimBasics,
		testClaimTakeover,
		testClaimTakeoverAcrossReRegistration,
		testClaimNamespaceIsolation,
	} {
		tf(t, s)
		teardown()
	}
}

func member(instanceID, address string) *cluster.Member {
	return &cluster.Member{
		InstanceID: instanceID,
		Address:    address,
		Labels:     map[string]string{"role": "all"},
	}
}

func testRegistry(t *testing.T, s cluster.Store) {
	t.Run("testRegistry", func(t *testing.T) {
		ctx := context.Background()

		records, err := s.GetMembers(ctx)
		require.NoError(t, err)
		require.Empty(t, records)

		_, err = s.Heartbeat(ctx, "missing")
		require.ErrorIs(t, err, cluster.ErrMemberNotFound)
		require.ErrorIs(t, s.SetDraining(ctx, "missing", true), cluster.ErrMemberNotFound)

		a := member("instance-a", "10.0.0.1:8085")
		b := member("instance-b", "10.0.0.2:8085")
		require.NoError(t, s.PutMember(ctx, a, 1))
		require.NoError(t, s.PutMember(ctx, b, 1))

		records, err = s.GetMembers(ctx)
		require.NoError(t, err)
		require.Len(t, records, 2)

		byID := make(map[string]*cluster.MemberRecord)
		for _, r := range records {
			byID[r.InstanceID] = r
		}
		require.Equal(t, "10.0.0.1:8085", byID["instance-a"].Address)
		require.Equal(t, map[string]string{"role": "all"}, byID["instance-a"].Labels)
		require.False(t, byID["instance-a"].Draining)
		require.EqualValues(t, 1, byID["instance-a"].HeartbeatCounter)

		counter, err := s.Heartbeat(ctx, "instance-a")
		require.NoError(t, err)
		require.EqualValues(t, 2, counter)
		counter, err = s.Heartbeat(ctx, "instance-a")
		require.NoError(t, err)
		require.EqualValues(t, 3, counter)

		require.NoError(t, s.SetDraining(ctx, "instance-a", true))

		records, err = s.GetMembers(ctx)
		require.NoError(t, err)
		for _, r := range records {
			if r.InstanceID == "instance-a" {
				require.True(t, r.Draining)
				require.EqualValues(t, 3, r.HeartbeatCounter)
			} else {
				require.False(t, r.Draining)
				require.EqualValues(t, 1, r.HeartbeatCounter)
			}
		}

		require.NoError(t, s.DeleteMember(ctx, "instance-a"))
		require.NoError(t, s.DeleteMember(ctx, "instance-a")) // Idempotent.

		records, err = s.GetMembers(ctx)
		require.NoError(t, err)
		require.Len(t, records, 1)
		require.Equal(t, "instance-b", records[0].InstanceID)
	})
}

func testClaimBasics(t *testing.T, s cluster.Store) {
	t.Run("testClaimBasics", func(t *testing.T) {
		ctx := context.Background()

		a := member("instance-a", "10.0.0.1:8085")
		b := member("instance-b", "10.0.0.2:8085")
		require.NoError(t, s.PutMember(ctx, a, 1))
		require.NoError(t, s.PutMember(ctx, b, 1))

		key := []byte("chat-key-1")

		_, err := s.GetClaim(ctx, "chat", key)
		require.ErrorIs(t, err, cluster.ErrClaimNotFound)

		claim, err := s.AcquireClaim(ctx, "chat", key, a, nil)
		require.NoError(t, err)
		require.Equal(t, "chat", claim.Namespace)
		require.Equal(t, key, claim.Key)
		require.Equal(t, "instance-a", claim.OwnerInstanceID)
		require.Equal(t, "10.0.0.1:8085", claim.OwnerAddress)
		require.EqualValues(t, 1, claim.Fence)

		// Owner re-acquire: fence unchanged.
		claim, err = s.AcquireClaim(ctx, "chat", key, a, nil)
		require.NoError(t, err)
		require.EqualValues(t, 1, claim.Fence)

		// A vacant acquire by another member fails and reports the holder.
		holder, err := s.AcquireClaim(ctx, "chat", key, b, nil)
		require.ErrorIs(t, err, cluster.ErrClaimHeld)
		require.NotNil(t, holder)
		require.Equal(t, "instance-a", holder.OwnerInstanceID)
		require.Equal(t, "10.0.0.1:8085", holder.OwnerAddress)

		// A release by a non-holder is a no-op.
		require.NoError(t, s.ReleaseClaim(ctx, "chat", key, "instance-b"))
		claim, err = s.GetClaim(ctx, "chat", key)
		require.NoError(t, err)
		require.Equal(t, "instance-a", claim.OwnerInstanceID)

		// The holder's release deactivates the claim.
		require.NoError(t, s.ReleaseClaim(ctx, "chat", key, "instance-a"))
		_, err = s.GetClaim(ctx, "chat", key)
		require.ErrorIs(t, err, cluster.ErrClaimNotFound)

		// The fence stays monotonic across a release.
		claim, err = s.AcquireClaim(ctx, "chat", key, b, nil)
		require.NoError(t, err)
		require.Equal(t, "instance-b", claim.OwnerInstanceID)
		require.EqualValues(t, 2, claim.Fence)
	})
}

func testClaimTakeover(t *testing.T, s cluster.Store) {
	t.Run("testClaimTakeover", func(t *testing.T) {
		ctx := context.Background()

		a := member("instance-a", "10.0.0.1:8085")
		b := member("instance-b", "10.0.0.2:8085")
		require.NoError(t, s.PutMember(ctx, a, 1))
		require.NoError(t, s.PutMember(ctx, b, 1))

		key := []byte("chat-key-1")

		_, err := s.AcquireClaim(ctx, "chat", key, a, nil)
		require.NoError(t, err)

		// Takeover naming the wrong holder fails.
		holder, err := s.AcquireClaim(ctx, "chat", key, b, &cluster.TakeoverTarget{
			InstanceID:       "instance-c",
			HeartbeatCounter: 1,
		})
		require.ErrorIs(t, err, cluster.ErrClaimHeld)
		require.Equal(t, "instance-a", holder.OwnerInstanceID)

		// The holder heartbeats: evidence observed before the beat goes stale,
		// so the takeover must fail — the holder was alive after all.
		_, err = s.Heartbeat(ctx, "instance-a")
		require.NoError(t, err)
		holder, err = s.AcquireClaim(ctx, "chat", key, b, &cluster.TakeoverTarget{
			InstanceID:       "instance-a",
			HeartbeatCounter: 1,
		})
		require.ErrorIs(t, err, cluster.ErrClaimHeld)
		require.Equal(t, "instance-a", holder.OwnerInstanceID)

		// Takeover with evidence matching the holder's (unmoving) counter
		// succeeds and bumps the fence.
		claim, err := s.AcquireClaim(ctx, "chat", key, b, &cluster.TakeoverTarget{
			InstanceID:       "instance-a",
			HeartbeatCounter: 2,
		})
		require.NoError(t, err)
		require.Equal(t, "instance-b", claim.OwnerInstanceID)
		require.Equal(t, "10.0.0.2:8085", claim.OwnerAddress)
		require.EqualValues(t, 2, claim.Fence)

		// The displaced zombie cannot get its claim back while B holds it.
		holder, err = s.AcquireClaim(ctx, "chat", key, a, nil)
		require.ErrorIs(t, err, cluster.ErrClaimHeld)
		require.Equal(t, "instance-b", holder.OwnerInstanceID)

		// A holder with no registry record at all (deregistered or GC'd) is
		// displaceable with zero-counter evidence.
		key2 := []byte("chat-key-2")
		_, err = s.AcquireClaim(ctx, "chat", key2, a, nil)
		require.NoError(t, err)
		require.NoError(t, s.DeleteMember(ctx, "instance-a"))

		claim, err = s.AcquireClaim(ctx, "chat", key2, b, &cluster.TakeoverTarget{
			InstanceID: "instance-a",
		})
		require.NoError(t, err)
		require.Equal(t, "instance-b", claim.OwnerInstanceID)
		require.EqualValues(t, 2, claim.Fence)
	})
}

func testClaimTakeoverAcrossReRegistration(t *testing.T, s cluster.Store) {
	t.Run("testClaimTakeoverAcrossReRegistration", func(t *testing.T) {
		ctx := context.Background()

		a := member("instance-a", "10.0.0.1:8085")
		b := member("instance-b", "10.0.0.2:8085")
		require.NoError(t, s.PutMember(ctx, a, 1))
		require.NoError(t, s.PutMember(ctx, b, 1))

		// A's first registration epoch reaches counter 3 while owning the key.
		_, err := s.Heartbeat(ctx, "instance-a")
		require.NoError(t, err)
		_, err = s.Heartbeat(ctx, "instance-a")
		require.NoError(t, err)

		key := []byte("chat-key-1")
		_, err = s.AcquireClaim(ctx, "chat", key, a, nil)
		require.NoError(t, err)

		// A's record is GC'd and A re-registers, resuming ABOVE its previous
		// epoch (the membership layer guarantees this). Were the counter to
		// reset instead, a peer's stale prior-epoch evidence would match again
		// and displace a live owner.
		require.NoError(t, s.DeleteMember(ctx, "instance-a"))
		require.NoError(t, s.PutMember(ctx, a, 4))

		// No prior-epoch evidence may displace the re-registered live owner:
		// not the absent-record form, not any counter its old epoch exposed.
		for _, staleCounter := range []uint64{0, 1, 3} {
			holder, err := s.AcquireClaim(ctx, "chat", key, b, &cluster.TakeoverTarget{
				InstanceID:       "instance-a",
				HeartbeatCounter: staleCounter,
			})
			require.ErrorIs(t, err, cluster.ErrClaimHeld, "stale epoch counter %d must not displace", staleCounter)
			require.Equal(t, "instance-a", holder.OwnerInstanceID)
		}

		// Evidence from the current epoch, observed stale, still works.
		claim, err := s.AcquireClaim(ctx, "chat", key, b, &cluster.TakeoverTarget{
			InstanceID:       "instance-a",
			HeartbeatCounter: 4,
		})
		require.NoError(t, err)
		require.Equal(t, "instance-b", claim.OwnerInstanceID)
	})
}

func testClaimNamespaceIsolation(t *testing.T, s cluster.Store) {
	t.Run("testClaimNamespaceIsolation", func(t *testing.T) {
		ctx := context.Background()

		a := member("instance-a", "10.0.0.1:8085")
		b := member("instance-b", "10.0.0.2:8085")
		require.NoError(t, s.PutMember(ctx, a, 1))
		require.NoError(t, s.PutMember(ctx, b, 1))

		key := []byte("shared-key")

		claimA, err := s.AcquireClaim(ctx, "chat", key, a, nil)
		require.NoError(t, err)
		claimB, err := s.AcquireClaim(ctx, "topics", key, b, nil)
		require.NoError(t, err)

		require.Equal(t, "instance-a", claimA.OwnerInstanceID)
		require.Equal(t, "instance-b", claimB.OwnerInstanceID)

		require.NoError(t, s.ReleaseClaim(ctx, "chat", key, "instance-a"))
		_, err = s.GetClaim(ctx, "chat", key)
		require.ErrorIs(t, err, cluster.ErrClaimNotFound)

		claim, err := s.GetClaim(ctx, "topics", key)
		require.NoError(t, err)
		require.Equal(t, "instance-b", claim.OwnerInstanceID)
	})
}
