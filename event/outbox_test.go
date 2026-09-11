package event

import (
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"

	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"
	eventpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/event/v1"
)

// recordingSender captures batches per address. Each send blocks until
// released when gate is set, so tests can hold a sender mid-RPC and observe
// what queues up behind it.
type recordingSender struct {
	mu      sync.Mutex
	batches map[string][][]forwardItem
	gate    chan struct{}
	err     error
}

func newRecordingSender(gated bool) *recordingSender {
	s := &recordingSender{batches: make(map[string][][]forwardItem)}
	if gated {
		s.gate = make(chan struct{})
	}
	return s
}

func (s *recordingSender) send(address string, items []forwardItem) error {
	s.mu.Lock()
	s.batches[address] = append(s.batches[address], items)
	s.mu.Unlock()
	if s.gate != nil {
		<-s.gate
	}
	return s.err
}

func (s *recordingSender) snapshot(address string) [][]forwardItem {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([][]forwardItem(nil), s.batches[address]...)
}

func (s *recordingSender) waitForBatches(t *testing.T, address string, n int) [][]forwardItem {
	require.Eventually(t, func() bool { return len(s.snapshot(address)) >= n }, time.Second, time.Millisecond)
	return s.snapshot(address)
}

func chatItem(chatID byte, n int) forwardItem {
	return forwardItem{chat: &eventpb.ChatEvent{
		ChatId: &commonpb.ChatId{Value: []byte{chatID}},
		Event:  &eventpb.Event{Id: MustGenerateEventID(), Type: &eventpb.Event_Test{Test: &eventpb.TestEvent{Nonce: uint64(n)}}},
	}}
}

func userItem(userID byte, n int) forwardItem {
	return forwardItem{user: &eventpb.UserEvent{
		UserId: &commonpb.UserId{Value: []byte{userID}},
		Event:  &eventpb.Event{Id: MustGenerateEventID(), Type: &eventpb.Event_Test{Test: &eventpb.TestEvent{Nonce: uint64(n)}}},
	}}
}

func nonces(items []forwardItem) (out []int) {
	for _, it := range items {
		var e *eventpb.Event
		if it.user != nil {
			e = it.user.Event
		} else {
			e = it.chat.Event
		}
		out = append(out, int(e.GetTest().GetNonce()))
	}
	return out
}

func testOutboxConfig() outboxConfig {
	return outboxConfig{
		Senders:       1,
		QueueSize:     4,
		MaxBatchSize:  3,
		MaxBatchBytes: 1 << 20,
		IdleTimeout:   10 * time.Millisecond,
	}
}

// TestOutboxes_BatchesWhatQueuesDuringASend pins the batching mechanism: the
// first event goes alone, and everything that queues while its RPC is in
// flight goes out together on the next send, capped at MaxBatchSize, in
// order.
func TestOutboxes_BatchesWhatQueuesDuringASend(t *testing.T) {
	sender := newRecordingSender(true)
	o := newOutboxes(zaptest.NewLogger(t), sender.send, testOutboxConfig())

	require.True(t, o.enqueue("peer", chatItem(1, 0)))
	sender.waitForBatches(t, "peer", 1) // in flight, gated

	for i := 1; i <= 4; i++ {
		require.True(t, o.enqueue("peer", chatItem(1, i)))
	}

	sender.gate <- struct{}{} // release batch 1
	batches := sender.waitForBatches(t, "peer", 2)
	require.Equal(t, []int{0}, nonces(batches[0]))
	require.Equal(t, []int{1, 2, 3}, nonces(batches[1]), "capped at MaxBatchSize, in order")

	sender.gate <- struct{}{} // release batch 2
	batches = sender.waitForBatches(t, "peer", 3)
	require.Equal(t, []int{4}, nonces(batches[2]))
	sender.gate <- struct{}{}
}

// TestOutboxes_DropsWhenFull pins backpressure: with a send in flight and the
// queue full, enqueue returns false instead of blocking or growing.
func TestOutboxes_DropsWhenFull(t *testing.T) {
	sender := newRecordingSender(true)
	o := newOutboxes(zaptest.NewLogger(t), sender.send, testOutboxConfig())

	require.True(t, o.enqueue("peer", chatItem(1, 0)))
	sender.waitForBatches(t, "peer", 1)

	for i := 1; i <= 4; i++ {
		require.True(t, o.enqueue("peer", chatItem(1, i)), "queue holds QueueSize")
	}
	require.False(t, o.enqueue("peer", chatItem(1, 5)), "the next one drops")
	require.False(t, o.enqueue("peer", chatItem(1, 6)))
	require.Equal(t, uint64(2), o.dropped.Load())

	// Drops are reported per peer on the sweep tick, each tick only what is
	// new since the last, so a tick with nothing new stays quiet.
	o.mu.Lock()
	ob := o.byAddress["peer"]
	o.mu.Unlock()
	require.Equal(t, uint64(2), ob.dropped.Load())
	require.Equal(t, uint64(0), ob.reported)
	o.sweep(time.Now())
	require.Equal(t, uint64(2), ob.reported)
	require.False(t, o.enqueue("peer", chatItem(1, 7)))
	o.sweep(time.Now())
	require.Equal(t, uint64(3), ob.reported)

	close(sender.gate)
}

// TestOutboxes_PeersAreIndependent pins that one peer's backlog never
// touches another's.
func TestOutboxes_PeersAreIndependent(t *testing.T) {
	sender := newRecordingSender(true)
	o := newOutboxes(zaptest.NewLogger(t), sender.send, testOutboxConfig())

	require.True(t, o.enqueue("slow", chatItem(1, 0)))
	sender.waitForBatches(t, "slow", 1)
	for i := 1; i <= 4; i++ {
		require.True(t, o.enqueue("slow", chatItem(1, i)))
	}
	require.False(t, o.enqueue("slow", chatItem(1, 5)))

	// The other peer still accepts and sends.
	require.True(t, o.enqueue("fast", chatItem(1, 9)))
	sender.waitForBatches(t, "fast", 1)

	close(sender.gate)
}

// TestOutboxes_SweepClosesIdleAndReopens pins the lifecycle: an outbox idle
// past IdleTimeout with nothing queued is closed (its senders exit) and a
// later enqueue opens a fresh one; a busy or non-empty one is left alone.
func TestOutboxes_SweepClosesIdleAndReopens(t *testing.T) {
	sender := newRecordingSender(false)
	o := newOutboxes(zaptest.NewLogger(t), sender.send, testOutboxConfig())

	require.True(t, o.enqueue("peer", chatItem(1, 0)))
	sender.waitForBatches(t, "peer", 1)

	// Not idle yet.
	require.Equal(t, 0, o.sweep(time.Now()))

	// Idle, empty: swept, and the sender goroutine exits.
	o.mu.RLock()
	ob := o.byAddress["peer"]
	o.mu.RUnlock()
	require.NotNil(t, ob)
	require.Equal(t, 1, o.sweep(time.Now().Add(time.Second)))
	ob.senders.Wait()
	o.mu.RLock()
	_, ok := o.byAddress["peer"]
	o.mu.RUnlock()
	require.False(t, ok)

	// Reopens on demand.
	require.True(t, o.enqueue("peer", chatItem(1, 1)))
	sender.waitForBatches(t, "peer", 2)
}

func TestOutboxes_SweepSkipsNonEmpty(t *testing.T) {
	sender := newRecordingSender(true)
	o := newOutboxes(zaptest.NewLogger(t), sender.send, testOutboxConfig())

	require.True(t, o.enqueue("peer", chatItem(1, 0)))
	sender.waitForBatches(t, "peer", 1)
	require.True(t, o.enqueue("peer", chatItem(1, 1))) // queued behind the gated send

	require.Equal(t, 0, o.sweep(time.Now().Add(time.Second)), "a queued event keeps the outbox")

	close(sender.gate)
	sender.waitForBatches(t, "peer", 2)
}

// TestOutboxes_SendErrorIsLoggedNotFatal pins that a failed batch is dropped
// and the sender keeps draining.
func TestOutboxes_SendErrorIsLoggedNotFatal(t *testing.T) {
	sender := newRecordingSender(false)
	sender.err = errors.New("peer down")
	o := newOutboxes(zaptest.NewLogger(t), sender.send, testOutboxConfig())

	require.True(t, o.enqueue("peer", chatItem(1, 0)))
	require.True(t, o.enqueue("peer", chatItem(1, 1)))
	require.Eventually(t, func() bool {
		var total int
		for _, b := range sender.snapshot("peer") {
			total += len(b)
		}
		return total == 2
	}, time.Second, time.Millisecond, "both events attempted despite the first batch failing")
}

// TestOutboxes_ShardsKeepATopicOnOneSender pins the multi-sender contract:
// every event for one topic lands on the same shard, so its order survives.
func TestOutboxes_ShardsKeepATopicOnOneSender(t *testing.T) {
	cfg := testOutboxConfig()
	cfg.Senders = 4
	cfg.QueueSize = 64
	o := newOutboxes(zaptest.NewLogger(t), newRecordingSender(true).send, cfg)

	ob := o.acquire("peer")
	o.mu.RUnlock()
	require.Len(t, ob.shards, 4)

	for topic := range byte(16) {
		want := shardIndex([]byte{topic}, 4)
		for n := range 3 {
			require.Equal(t, want, shardIndex(chatItem(topic, n).key(), 4))
			require.Equal(t, want, shardIndex(userItem(topic, n).key(), 4))
		}
	}
	require.Equal(t, 0, shardIndex([]byte("anything"), 1))
}

// TestOutboxes_SendersDrainShardsIndependently pins multi-sender delivery: a
// sender held mid-RPC on one shard stalls only its own topics, another
// topic's shard keeps sending in order around it, the held shard resumes in
// order once released, and a sweep retires every sender of the peer.
func TestOutboxes_SendersDrainShardsIndependently(t *testing.T) {
	cfg := testOutboxConfig()
	cfg.Senders = 2
	cfg.QueueSize = 16
	cfg.MaxBatchSize = 16

	// Two topics that hash to different shards.
	topicA, topicB := byte(0), byte(1)
	for shardIndex([]byte{topicA}, cfg.Senders) == shardIndex([]byte{topicB}, cfg.Senders) {
		topicB++
	}

	// Batches are recorded on arrival; topic A's are then held until released.
	rec := newRecordingSender(false)
	releaseA := make(chan struct{})
	send := func(address string, items []forwardItem) error {
		err := rec.send(address, items)
		if items[0].key()[0] == topicA {
			<-releaseA
		}
		return err
	}
	o := newOutboxes(zaptest.NewLogger(t), send, cfg)

	forTopic := func(topic byte) (out []int) {
		for _, b := range rec.snapshot("peer") {
			if b[0].key()[0] == topic {
				out = append(out, nonces(b)...)
			}
		}
		return out
	}

	// A's sender takes its first event and is held mid-send.
	require.True(t, o.enqueue("peer", chatItem(topicA, 0)))
	require.Eventually(t, func() bool { return len(forTopic(topicA)) == 1 }, time.Second, time.Millisecond)

	// More of A queues behind the held send; B's shard is unaffected.
	for n := 1; n <= 2; n++ {
		require.True(t, o.enqueue("peer", chatItem(topicA, n)))
	}
	for n := 10; n <= 12; n++ {
		require.True(t, o.enqueue("peer", chatItem(topicB, n)))
	}
	require.Eventually(t, func() bool { return len(forTopic(topicB)) == 3 }, time.Second, time.Millisecond)
	require.Equal(t, []int{10, 11, 12}, forTopic(topicB), "B flows, in order, while A is held")
	require.Equal(t, []int{0}, forTopic(topicA), "A's backlog waits on its own sender")

	// Releasing A drains its backlog in order.
	close(releaseA)
	require.Eventually(t, func() bool { return len(forTopic(topicA)) == 3 }, time.Second, time.Millisecond)
	require.Equal(t, []int{0, 1, 2}, forTopic(topicA))

	// A sweep retires both senders.
	ob := o.acquire("peer")
	o.mu.RUnlock()
	require.Equal(t, 1, o.sweep(time.Now().Add(time.Second)))
	ob.senders.Wait()
}

func TestSplitBatch(t *testing.T) {
	items := []forwardItem{chatItem(1, 0), userItem(2, 1), chatItem(1, 2), userItem(3, 3)}
	users, chats := splitBatch(items)
	require.Equal(t, []int{1, 3}, nonces(func() (out []forwardItem) {
		for _, u := range users {
			out = append(out, forwardItem{user: u})
		}
		return out
	}()))
	require.Equal(t, []int{0, 2}, nonces(func() (out []forwardItem) {
		for _, c := range chats {
			out = append(out, forwardItem{chat: c})
		}
		return out
	}()))
}

// TestOutboxes_Concurrent hammers enqueue against sweep across peers; it
// exists for the race detector and to prove enqueue never sends on a closed
// shard.
func TestOutboxes_Concurrent(t *testing.T) {
	cfg := testOutboxConfig()
	cfg.Senders = 4
	cfg.QueueSize = 1024
	cfg.IdleTimeout = 0 // every sweep closes whatever is empty
	sender := newRecordingSender(false)
	o := newOutboxes(zaptest.NewLogger(t), sender.send, cfg)

	var wg sync.WaitGroup
	for i := range 8 {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for n := range 200 {
				o.enqueue(fmt.Sprintf("peer-%d", i%3), chatItem(byte(i), n))
			}
		}(i)
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for range 200 {
			o.sweep(time.Now())
		}
	}()
	wg.Wait()

	// Everything enqueued was sent (nothing dropped at this queue size), even
	// across outboxes being closed and reopened underneath the writers.
	require.Equal(t, uint64(0), o.dropped.Load())
	require.Eventually(t, func() bool {
		var total int
		for i := range 3 {
			for _, b := range sender.snapshot(fmt.Sprintf("peer-%d", i)) {
				total += len(b)
			}
		}
		return total == 8*200
	}, 5*time.Second, time.Millisecond)
}
