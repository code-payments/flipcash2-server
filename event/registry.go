package event

import (
	"bytes"
	"sync"
	"sync/atomic"

	"github.com/cespare/xxhash/v2"

	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"
	eventpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/event/v1"
)

// registryShards is the number of independently locked partitions in the
// stream registry. Registration and delivery contend only within a shard, so
// a reconnect storm (every stream re-registering all its keys) no longer
// serializes against every publish on the server. A power of two keeps the
// shard pick a mask.
const registryShards = 64

// streamRegistry fans a topic out to every open local stream: stream key →
// stream ID → stream. Multiple streams per key are the point (one per device);
// the cluster subscription layer refcounts them into a single registry row.
//
// Keys are partitioned across registryShards by hash, each shard behind its
// own RWMutex. A stream appears under several keys (its user's, plus one per
// group chat), so one registration touches several shards — each shard is
// locked once per registration, not once per key, and nothing ever holds two
// shard locks at the same time, so there is no lock ordering to get wrong.
//
// draining refuses new registrations once drain has begun. It is checked under
// each shard's lock as keys are added, and set before drain takes any shard
// lock, so a registration either lands entirely before the closing sweep
// reaches its shards (and is swept) or observes the flag and rolls itself
// back — a stream can never slip in behind the sweep.
type streamRegistry struct {
	draining atomic.Bool
	shards   [registryShards]registryShard
}

type registryShard struct {
	mu    sync.RWMutex
	byKey map[string]map[string]localStream
}

func newStreamRegistry() *streamRegistry {
	r := &streamRegistry{}
	for i := range r.shards {
		r.shards[i].byKey = make(map[string]map[string]localStream)
	}
	return r
}

func (r *streamRegistry) shardFor(key string) *registryShard {
	return &r.shards[xxhash.Sum64String(key)&(registryShards-1)]
}

// groupByShard buckets keys by the shard that owns them, so a multi-key
// operation locks each shard exactly once.
func (r *streamRegistry) groupByShard(keys []string) map[*registryShard][]string {
	grouped := make(map[*registryShard][]string)
	for _, key := range keys {
		shard := r.shardFor(key)
		grouped[shard] = append(grouped[shard], key)
	}
	return grouped
}

// add registers the stream under every key. It returns false, having
// registered nothing, if the registry is draining — including when draining
// began part-way through, in which case the keys already added are removed
// again before returning.
func (r *streamRegistry) add(streamID string, ls localStream, keys []string) bool {
	var added []string
	for shard, shardKeys := range r.groupByShard(keys) {
		shard.mu.Lock()
		if r.draining.Load() {
			shard.mu.Unlock()
			r.remove(streamID, added)
			return false
		}
		for _, key := range shardKeys {
			byID, ok := shard.byKey[key]
			if !ok {
				byID = make(map[string]localStream)
				shard.byKey[key] = byID
			}
			byID[streamID] = ls
		}
		shard.mu.Unlock()
		added = append(added, shardKeys...)
	}
	return true
}

// remove deregisters the stream from every key, dropping a key's entry once
// its last stream is gone. Keys the stream was never under are ignored.
func (r *streamRegistry) remove(streamID string, keys []string) {
	for shard, shardKeys := range r.groupByShard(keys) {
		shard.mu.Lock()
		for _, key := range shardKeys {
			if byID, ok := shard.byKey[key]; ok {
				delete(byID, streamID)
				if len(byID) == 0 {
					delete(shard.byKey, key)
				}
			}
		}
		shard.mu.Unlock()
	}
}

// targets returns every stream open under the key whose owner is not in
// exclude. The snapshot is taken under the shard's read lock and released
// before the caller delivers, so a slow stream never blocks registration.
func (r *streamRegistry) targets(key string, exclude []*commonpb.UserId) []Stream[[]*eventpb.Event] {
	shard := r.shardFor(key)
	shard.mu.RLock()
	defer shard.mu.RUnlock()

	byID := shard.byKey[key]
	targets := make([]Stream[[]*eventpb.Event], 0, len(byID))
	for _, ls := range byID {
		if isExcluded(ls.userID, exclude) {
			continue
		}
		targets = append(targets, ls.stream)
	}
	return targets
}

// drain marks the registry as draining, so every subsequent add is refused,
// and returns every open stream exactly once (a stream is registered under
// many keys, but is closed once). Idempotent: a second call returns whatever
// streams are still registered.
func (r *streamRegistry) drain() []Stream[[]*eventpb.Event] {
	r.draining.Store(true)

	seen := make(map[string]Stream[[]*eventpb.Event])
	for i := range r.shards {
		shard := &r.shards[i]
		shard.mu.RLock()
		for _, byID := range shard.byKey {
			for streamID, ls := range byID {
				if _, dup := seen[streamID]; !dup {
					seen[streamID] = ls.stream
				}
			}
		}
		shard.mu.RUnlock()
	}

	streams := make([]Stream[[]*eventpb.Event], 0, len(seen))
	for _, stream := range seen {
		streams = append(streams, stream)
	}
	return streams
}

func isExcluded(userID *commonpb.UserId, exclude []*commonpb.UserId) bool {
	for _, ex := range exclude {
		if bytes.Equal(userID.GetValue(), ex.GetValue()) {
			return true
		}
	}
	return false
}
