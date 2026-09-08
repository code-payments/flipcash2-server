package cluster

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func testMembers(n int) []*Member {
	members := make([]*Member, n)
	for i := range members {
		members[i] = &Member{
			InstanceID: fmt.Sprintf("instance-%d", i),
			Address:    fmt.Sprintf("10.0.0.%d:8085", i),
		}
	}
	return members
}

func TestHRW_Deterministic(t *testing.T) {
	members := testMembers(5)
	for i := 0; i < 256; i++ {
		key := fmt.Appendf(nil, "key-%d", i)
		first := hrwOwner(members, "chat", key)
		// Same election regardless of member order or repetition.
		reversed := make([]*Member, len(members))
		for j, m := range members {
			reversed[len(members)-1-j] = m
		}
		require.Equal(t, first.InstanceID, hrwOwner(members, "chat", key).InstanceID)
		require.Equal(t, first.InstanceID, hrwOwner(reversed, "chat", key).InstanceID)
	}
}

func TestHRW_NamespaceIsolation(t *testing.T) {
	members := testMembers(8)
	differs := false
	for i := 0; i < 256; i++ {
		key := fmt.Appendf(nil, "key-%d", i)
		if hrwOwner(members, "chat", key).InstanceID != hrwOwner(members, "topics", key).InstanceID {
			differs = true
			break
		}
	}
	// Namespaces hash independently: the same key must not map identically
	// across namespaces for every key.
	require.True(t, differs)
}

func TestHRW_Balance(t *testing.T) {
	members := testMembers(4)
	counts := make(map[string]int)
	const keys = 8192
	for i := 0; i < keys; i++ {
		key := fmt.Appendf(nil, "key-%d", i)
		counts[hrwOwner(members, "chat", key).InstanceID]++
	}

	// Each member owns roughly 1/N of the keyspace (loose 3x bounds; a real
	// imbalance would blow far past them).
	expected := keys / len(members)
	for _, m := range members {
		require.Greater(t, counts[m.InstanceID], expected/3)
		require.Less(t, counts[m.InstanceID], expected*3)
	}
}

func TestHRW_MinimalDisruption(t *testing.T) {
	members := testMembers(5)
	const keys = 4096

	before := make(map[string]string, keys)
	for i := 0; i < keys; i++ {
		key := fmt.Sprintf("key-%d", i)
		before[key] = hrwOwner(members, "chat", []byte(key)).InstanceID
	}

	// Remove one member: only the keys it owned may move, and each lands on
	// its deterministic runner-up.
	removed := members[2]
	survivors := append(append([]*Member(nil), members[:2]...), members[3:]...)

	moved := 0
	for i := 0; i < keys; i++ {
		key := fmt.Sprintf("key-%d", i)
		after := hrwOwner(survivors, "chat", []byte(key)).InstanceID
		if before[key] == removed.InstanceID {
			moved++
			require.NotEqual(t, removed.InstanceID, after)
		} else {
			// Keys not owned by the removed member keep their owner.
			require.Equal(t, before[key], after)
		}
	}
	// ~1/N of the keyspace moved.
	require.InDelta(t, keys/len(members), moved, float64(keys)/10)
}
