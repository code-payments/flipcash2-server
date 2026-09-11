package event

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/cespare/xxhash/v2"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	eventpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/event/v1"
)

const (
	// forwardSenders is how many goroutines drain each peer's outbox, each
	// owning a shard of the peer's queue keyed by the event's topic. One
	// saturates a connection at far more events per second than a server
	// produces; raising it trades per-topic ordering across shards for
	// throughput, and should wait for metrics that show a peer falling behind.
	forwardSenders = 1

	// forwardQueueSize bounds the events waiting on one shard of a peer's
	// outbox. A full shard drops the event — the client's delta sync is the
	// backstop — which is where unbounded goroutine growth used to be. Drops
	// are counted per peer and reported on the sweep tick, never logged per
	// event: a wedged peer at production event rates would otherwise turn
	// every publish into a log line.
	forwardQueueSize = 4096

	// forwardMaxBatchSize and forwardMaxBatchBytes cap one forwarding RPC, on
	// the same reasoning as a stream's batch: the count sits under the proto
	// limit and the byte budget keeps the message under the receiver's
	// default 4MB limit whatever the events carry.
	forwardMaxBatchSize  = 256
	forwardMaxBatchBytes = 1 << 20

	// outboxIdleTimeout is how long a peer's outbox sits empty before its
	// senders exit and it is forgotten. Peer addresses die with their
	// instances, so without this every deploy would leave a set of idle
	// goroutines and queues behind.
	outboxIdleTimeout   = time.Minute
	outboxSweepInterval = outboxIdleTimeout / 2
)

// forwardItem is one queued forward: exactly one of user or chat is set.
type forwardItem struct {
	user *eventpb.UserEvent
	chat *eventpb.ChatEvent
}

// key is the item's topic key, which picks its shard so a topic's events
// stay in order through one sender.
func (i forwardItem) key() []byte {
	if i.user != nil {
		return i.user.GetUserId().GetValue()
	}
	return i.chat.GetChatId().GetValue()
}

func (i forwardItem) size() int {
	if i.user != nil {
		return proto.Size(i.user)
	}
	return proto.Size(i.chat)
}

// batchSendFunc delivers one drained batch to a peer. It owns retries; an
// error means the batch is lost.
type batchSendFunc func(address string, items []forwardItem) error

type outboxConfig struct {
	Senders       int
	QueueSize     int
	MaxBatchSize  int
	MaxBatchBytes int
	IdleTimeout   time.Duration
	// SweepInterval of zero disables the background sweep; tests call sweep
	// directly.
	SweepInterval time.Duration
}

func defaultOutboxConfig() outboxConfig {
	return outboxConfig{
		Senders:       forwardSenders,
		QueueSize:     forwardQueueSize,
		MaxBatchSize:  forwardMaxBatchSize,
		MaxBatchBytes: forwardMaxBatchBytes,
		IdleTimeout:   outboxIdleTimeout,
		SweepInterval: outboxSweepInterval,
	}
}

// outboxes batches forwarded events per peer. Each peer gets a bounded queue
// (sharded across its senders) and long-lived sender goroutines that block
// for a first event, take whatever else has queued up to the batch caps, and
// send that as one RPC — the RPC in flight is exactly the window in which
// the next batch accumulates, so batching scales with load without a linger
// timer. Enqueue never blocks: a full shard drops.
//
// Goroutines per process are therefore fixed at senders × live peers, and
// RPCs per peer at one in flight per sender, however many events publish.
type outboxes struct {
	log  *zap.Logger
	send batchSendFunc
	cfg  outboxConfig

	// mu guards byAddress and, crucially, the shards' open state: enqueue
	// sends under the read lock and sweep closes under the write lock, so a
	// send on a closed shard cannot happen.
	mu        sync.RWMutex
	byAddress map[string]*peerOutbox

	// dropped counts every drop across all peers, for tests and as the
	// process-wide figure; the per-peer counts drive the sweep's report.
	dropped atomic.Uint64
}

type peerOutbox struct {
	shards      []chan forwardItem
	lastEnqueue atomic.Int64 // unix nanos
	senders     sync.WaitGroup

	// dropped counts this peer's drops; reported counts how many the sweep
	// has already logged, so each tick reports only the delta.
	dropped  atomic.Uint64
	reported uint64 // sweep-only, under outboxes.mu
}

func newOutboxes(log *zap.Logger, send batchSendFunc, cfg outboxConfig) *outboxes {
	o := &outboxes{
		log:       log,
		send:      send,
		cfg:       cfg,
		byAddress: make(map[string]*peerOutbox),
	}
	if cfg.SweepInterval > 0 {
		go o.periodicallySweep()
	}
	return o
}

// enqueue queues item for address, opening the peer's outbox on first use.
// It returns false if the item's shard was full and the item was dropped. A
// drop is counted, not logged: the sweep reports each peer's tally.
func (o *outboxes) enqueue(address string, item forwardItem) bool {
	ob := o.acquire(address)
	defer o.mu.RUnlock()

	ob.lastEnqueue.Store(time.Now().UnixNano())

	select {
	case ob.shards[shardIndex(item.key(), len(ob.shards))] <- item:
		return true
	default:
		ob.dropped.Add(1)
		o.dropped.Add(1)
		return false
	}
}

// acquire returns the peer's outbox with o.mu read-held, opening it if
// absent. The loop covers an outbox swept between the write lock releasing
// and the read lock being taken.
func (o *outboxes) acquire(address string) *peerOutbox {
	for {
		o.mu.RLock()
		if ob, ok := o.byAddress[address]; ok {
			return ob
		}
		o.mu.RUnlock()

		o.mu.Lock()
		if _, ok := o.byAddress[address]; !ok {
			o.byAddress[address] = o.open(address)
		}
		o.mu.Unlock()
	}
}

func (o *outboxes) open(address string) *peerOutbox {
	ob := &peerOutbox{shards: make([]chan forwardItem, o.cfg.Senders)}
	ob.lastEnqueue.Store(time.Now().UnixNano())
	for i := range ob.shards {
		shard := make(chan forwardItem, o.cfg.QueueSize)
		ob.shards[i] = shard
		ob.senders.Add(1)
		go func() {
			defer ob.senders.Done()
			o.drain(address, shard)
		}()
	}
	return ob
}

// drain is one sender: it runs until its shard is closed by a sweep.
func (o *outboxes) drain(address string, shard <-chan forwardItem) {
	for {
		first, ok := <-shard
		if !ok {
			return
		}

		batch := drainReady(shard, first, o.cfg.MaxBatchSize, o.cfg.MaxBatchBytes, forwardItem.size)
		if err := o.send(address, batch); err != nil {
			o.log.With(zap.Error(err)).Warn(
				"Failure forwarding event batch over RPC",
				zap.String("receiver_address", address),
				zap.Int("events", len(batch)),
			)
		}
	}
}

func (o *outboxes) periodicallySweep() {
	ticker := time.NewTicker(o.cfg.SweepInterval)
	defer ticker.Stop()
	for range ticker.C {
		o.sweep(time.Now())
	}
}

// sweep reports each peer's drops since the last tick, then closes and
// forgets every outbox that has been idle for IdleTimeout with nothing
// queued, returning how many. Under the write lock no enqueue is in progress,
// so closing the shards is safe; a sender mid-send finishes that batch and
// exits on its next receive.
func (o *outboxes) sweep(now time.Time) (swept int) {
	o.mu.Lock()
	defer o.mu.Unlock()

	for address, ob := range o.byAddress {
		o.reportDrops(address, ob)

		if now.Sub(time.Unix(0, ob.lastEnqueue.Load())) < o.cfg.IdleTimeout {
			continue
		}
		if !ob.empty() {
			continue
		}
		for _, shard := range ob.shards {
			close(shard)
		}
		delete(o.byAddress, address)
		swept++
	}
	return swept
}

// reportDrops logs the peer's drops since the last report, if any: one line
// per peer per tick, whatever the event rate. Runs under o.mu.
func (o *outboxes) reportDrops(address string, ob *peerOutbox) {
	dropped := ob.dropped.Load()
	if delta := dropped - ob.reported; delta > 0 {
		o.log.Warn(
			"Dropped events for a peer whose outbox was full",
			zap.String("receiver_address", address),
			zap.Uint64("dropped", delta),
			zap.Duration("window", o.cfg.SweepInterval),
		)
	}
	ob.reported = dropped
}

func (ob *peerOutbox) empty() bool {
	for _, shard := range ob.shards {
		if len(shard) > 0 {
			return false
		}
	}
	return true
}

func shardIndex(key []byte, shards int) int {
	if shards <= 1 {
		return 0
	}
	return int(xxhash.Sum64(key) % uint64(shards))
}
