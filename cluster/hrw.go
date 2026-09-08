package cluster

import (
	"github.com/cespare/xxhash/v2"
)

// hrwSep separates hash input fields so distinct (namespace, instance, key)
// triples can never collide by concatenation.
var hrwSep = []byte{0}

// hrwScore is a member's deterministic pseudo-random "bid" for a key. xxhash is
// stable across processes and platforms, which is load-bearing: every server
// must compute identical scores with zero coordination (never use a per-process
// seeded hash here).
func hrwScore(namespace, instanceID string, key []byte) uint64 {
	d := xxhash.New()
	_, _ = d.WriteString(namespace)
	_, _ = d.Write(hrwSep)
	_, _ = d.WriteString(instanceID)
	_, _ = d.Write(hrwSep)
	_, _ = d.Write(key)
	return d.Sum64()
}

// hrwOwner elects the highest-scoring member for the key (rendezvous hashing).
// Each member is equally likely to win any given key, and a membership change
// moves only the keys the changed member wins — every other key keeps its
// owner. Score ties (vanishingly rare) break on instance ID so all servers
// still agree.
func hrwOwner(members []*Member, namespace string, key []byte) *Member {
	var best *Member
	var bestScore uint64
	for _, m := range members {
		score := hrwScore(namespace, m.InstanceID, key)
		if best == nil || score > bestScore || (score == bestScore && m.InstanceID > best.InstanceID) {
			best, bestScore = m, score
		}
	}
	return best
}
