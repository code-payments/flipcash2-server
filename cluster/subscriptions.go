package cluster

import (
	"context"
	"errors"
	"sync"
	"time"

	"go.uber.org/zap"
)

// ErrSubscriptionsDraining is returned by Subscribe once Drain has begun: a
// registration accepted mid-drain would write a row nothing will clean up.
var ErrSubscriptionsDraining = errors.New("cluster subscriptions are draining")

// subscriptionOpTimeout bounds the background row writes the runtime performs
// on its own behalf (sweeps and re-assertions), which carry no caller context.
const subscriptionOpTimeout = 5 * time.Second

// Subscriptions tracks which servers host live streams for each (namespace,
// key) topic — the non-exclusive sibling of Ownership. A subscription is
// interest, not ownership: any number of members may subscribe to a topic,
// there is no fence and no takeover, and a dead member's rows simply stop
// counting (validity is the member's liveness, exactly as for claims).
//
// The registry names servers, not streams: the first local stream for a topic
// writes one row, later streams ride it for free, and the last close removes
// it — so a topic's row count (and a publisher's per-event fan-out) is bounded
// by fleet size no matter how many clients subscribe. Steady state is
// write-free; the process heartbeat is what keeps rows valid.
//
// Delivery is the consumer's job: resolve Subscribers, deliver locally when
// subscribed itself, forward to the rest. Delivery is best-effort by contract
// — a cached resolution may briefly miss a just-opened stream or include a
// just-closed one, and the consumer's pull backstop (delta sync against its
// sequenced store) is what makes that safe.
type Subscriptions struct {
	log        *zap.Logger
	membership *Membership
	store      SubscriptionStore
	cfg        SubscriptionsConfig

	mu    sync.Mutex
	local map[string]*localTopic
	cache map[string]subscriberCacheEntry
	// nextGen numbers topic registrations so a handle can tell its own
	// registration from a later one occupying the same topic slot (a stale
	// handle Closed after a Drain+Resume cycle must not decrement — let alone
	// delete — a successor registration).
	nextGen uint64
	// unknownSince tracks, per instance ID, when this observer first saw a
	// subscription row whose member was absent from its live view — the
	// observation timeline behind corpse-row sweeps. Entries clear on any live
	// sighting; bounded by crashed instances with rows still being resolved.
	unknownSince map[string]time.Time
	topicLocks   map[string]*keyLock
	draining     bool
}

// localTopic is one topic's local refcount: how many open handles (streams)
// this process holds against it. The registry row exists while refs > 0. gen
// identifies this registration; handles carry it so a Close outliving the
// registration (drained out, then re-created by a later Subscribe) is a no-op
// instead of a decrement against the successor.
type localTopic struct {
	namespace string
	key       []byte
	refs      int
	gen       uint64
}

type subscriberCacheEntry struct {
	subs    []*Subscription
	expires time.Time
}

// SubscriptionHandle is one local stream's registration against a topic.
// Close it when the stream ends; the topic's registry row is removed when the
// last local handle closes. Close is idempotent.
type SubscriptionHandle struct {
	subs      *Subscriptions
	namespace string
	key       []byte
	gen       uint64
	once      sync.Once
	err       error
}

// Close releases this handle's registration.
func (h *SubscriptionHandle) Close(ctx context.Context) error {
	h.once.Do(func() { h.err = h.subs.unsubscribe(ctx, h.namespace, h.key, h.gen) })
	return h.err
}

// NewSubscriptions creates the subscriptions runtime. It has no background
// loops; cleanup work rides resolution calls and membership callbacks.
func NewSubscriptions(log *zap.Logger, membership *Membership, store SubscriptionStore, cfg SubscriptionsConfig) *Subscriptions {
	cfg = cfg.withDefaults()
	// Floored well above the liveness window: sweeping is judged on this
	// observer's own timeline, and anything close to the window would collect
	// rows of members that were merely slow to be observed.
	if floor := 2 * membership.LivenessWindow(); cfg.RowGCAfter < floor {
		cfg.RowGCAfter = 10 * membership.LivenessWindow()
	}
	s := &Subscriptions{
		log:          log,
		membership:   membership,
		store:        store,
		cfg:          cfg,
		local:        make(map[string]*localTopic),
		cache:        make(map[string]subscriberCacheEntry),
		unknownSince: make(map[string]time.Time),
		topicLocks:   make(map[string]*keyLock),
	}
	// The inverse of Ownership's session-lost shedding: exclusive state might
	// now be someone else's and must be dropped, but interest rows cannot
	// conflict — they can only have been swept as corpse rows while our
	// heartbeats gapped. Re-assert every row still backed by live handles.
	// Async: the notification comes from the heartbeat loop, and store writes
	// must not stall the beats that re-establish the session.
	membership.OnSessionLost(func() {
		go s.reassertLocal(context.Background())
	})
	return s
}

// lockTopic and unlockTopic mirror Ownership's key locks: they serialize a
// topic's registry writes so a first-subscribe racing a last-unsubscribe can
// never interleave as put-then-delete — a live stream with no row behind it,
// which would silently stop delivery to this server for the topic.
func (s *Subscriptions) lockTopic(id string) *keyLock {
	s.mu.Lock()
	kl := s.topicLocks[id]
	if kl == nil {
		kl = &keyLock{}
		s.topicLocks[id] = kl
	}
	kl.refs++
	s.mu.Unlock()

	kl.mu.Lock()
	return kl
}

func (s *Subscriptions) unlockTopic(id string, kl *keyLock) {
	kl.mu.Unlock()

	s.mu.Lock()
	kl.refs--
	if kl.refs == 0 {
		delete(s.topicLocks, id)
	}
	s.mu.Unlock()
}

// Subscribe registers a local stream's interest in the topic. The first local
// handle writes the topic's registry row; later handles share it. Returns
// ErrSubscriptionsDraining once Drain has begun.
func (s *Subscriptions) Subscribe(ctx context.Context, namespace string, key []byte) (*SubscriptionHandle, error) {
	id := ownedKeyID(namespace, key)
	kl := s.lockTopic(id)
	defer s.unlockTopic(id, kl)

	s.mu.Lock()
	if s.draining {
		s.mu.Unlock()
		return nil, ErrSubscriptionsDraining
	}
	if t, ok := s.local[id]; ok {
		t.refs++
		gen := t.gen
		s.mu.Unlock()
		return s.handle(namespace, key, gen), nil
	}
	s.mu.Unlock()

	// First local subscriber: the row must exist before the handle does, or a
	// publish resolved in between would miss a stream the caller believes is
	// registered.
	if err := s.store.PutSubscription(ctx, namespace, key, s.membership.Self()); err != nil {
		return nil, err
	}

	s.mu.Lock()
	if s.draining {
		// Lost the race with Drain's cutoff: the bulk delete may have run
		// before our row landed, so hand it back ourselves.
		s.mu.Unlock()
		deleteCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), subscriptionOpTimeout)
		defer cancel()
		if err := s.store.DeleteSubscription(deleteCtx, namespace, key, s.membership.Self().InstanceID); err != nil {
			s.log.With(zap.Error(err)).Warn("Failed to hand back subscription row acquired during drain",
				zap.String("namespace", namespace),
			)
		}
		return nil, ErrSubscriptionsDraining
	}
	s.nextGen++
	s.local[id] = &localTopic{
		namespace: namespace,
		key:       append([]byte(nil), key...),
		refs:      1,
		gen:       s.nextGen,
	}
	gen := s.nextGen
	// Invalidate so a local publish resolved before the row landed doesn't
	// keep excluding self for the rest of the cache window.
	delete(s.cache, id)
	s.mu.Unlock()

	return s.handle(namespace, key, gen), nil
}

func (s *Subscriptions) handle(namespace string, key []byte, gen uint64) *SubscriptionHandle {
	return &SubscriptionHandle{
		subs:      s,
		namespace: namespace,
		key:       append([]byte(nil), key...),
		gen:       gen,
	}
}

func (s *Subscriptions) unsubscribe(ctx context.Context, namespace string, key []byte, gen uint64) error {
	id := ownedKeyID(namespace, key)
	kl := s.lockTopic(id)
	defer s.unlockTopic(id, kl)

	s.mu.Lock()
	t, ok := s.local[id]
	if !ok {
		// Drained out from under the handle; the bulk path removed the row.
		s.mu.Unlock()
		return nil
	}
	if t.gen != gen {
		// The handle's registration is already gone (drained, then the slot
		// re-created by a post-Resume Subscribe); the current registration's
		// refcount belongs to its own handles.
		s.mu.Unlock()
		return nil
	}
	t.refs--
	if t.refs > 0 {
		s.mu.Unlock()
		return nil
	}
	delete(s.local, id)
	delete(s.cache, id)
	s.mu.Unlock()

	// A failed delete leaves a stale self row: publishers waste a forward
	// here until a later local resolution notices the refcount-less row and
	// sweeps it (or this process drains or dies).
	return s.store.DeleteSubscription(ctx, namespace, key, s.membership.Self().InstanceID)
}

// Subscribers resolves the servers currently interested in the topic: registry
// rows filtered by member liveness, with this process served from its local
// refcounts rather than the store. Results are cached for CacheTTL (empty
// results included), so hot topics cost one registry read per TTL regardless
// of event rate.
func (s *Subscriptions) Subscribers(ctx context.Context, namespace string, key []byte) ([]*Subscription, error) {
	id := ownedKeyID(namespace, key)

	s.mu.Lock()
	if entry, ok := s.cache[id]; ok {
		if time.Now().Before(entry.expires) {
			out := cloneSubscriptions(entry.subs)
			s.mu.Unlock()
			return out, nil
		}
		// Prune on read, like the redirect cache: leaving expired entries
		// around would grow the map by every topic resolved in between.
		delete(s.cache, id)
	}
	s.mu.Unlock()

	rows, err := s.store.GetSubscribers(ctx, namespace, key)
	if err != nil {
		return nil, err
	}

	live := make(map[string]bool)
	for _, m := range s.membership.Live() {
		live[m.InstanceID] = true
	}
	self := s.membership.Self()
	now := time.Now()

	s.mu.Lock()
	_, locallySubscribed := s.local[id]

	subs := make([]*Subscription, 0, len(rows))
	var corpseRows []*Subscription
	selfInRows := false
	staleSelfRow := false
	for _, row := range rows {
		switch {
		case row.InstanceID == self.InstanceID:
			selfInRows = true
			if locallySubscribed {
				subs = append(subs, row)
			} else {
				// Our own row with no handles behind it: a failed unsubscribe
				// delete. Swept below (re-checked under the topic lock).
				staleSelfRow = true
			}
		case live[row.InstanceID]:
			delete(s.unknownSince, row.InstanceID)
			subs = append(subs, row)
		default:
			// Not live: excluded from delivery immediately — correctness
			// never waits on cleanup. Rows whose member stays unknown to this
			// observer past RowGCAfter are crashed instances' leftovers
			// (drains clean up after themselves): sweep them so hot topics
			// don't accumulate garbage.
			first, seen := s.unknownSince[row.InstanceID]
			if !seen {
				s.unknownSince[row.InstanceID] = now
			} else if now.Sub(first) >= s.cfg.RowGCAfter {
				corpseRows = append(corpseRows, row)
			}
		}
	}
	if locallySubscribed && !selfInRows {
		// Locally subscribed but our row is missing — a lost write, or a
		// peer's sweep while our session gapped. Local truth wins for self;
		// re-assert the row below for everyone else's benefit.
		subs = append(subs, &Subscription{
			Namespace:  namespace,
			Key:        append([]byte(nil), key...),
			InstanceID: self.InstanceID,
			Address:    self.Address,
		})
	}

	s.cache[id] = subscriberCacheEntry{
		subs:    cloneSubscriptions(subs),
		expires: now.Add(s.cfg.CacheTTL),
	}
	s.mu.Unlock()

	if locallySubscribed && !selfInRows {
		go s.reassertRow(namespace, key)
	}
	if staleSelfRow {
		go s.sweepSelfRow(namespace, key)
	}
	for _, row := range corpseRows {
		go s.sweepCorpseRow(row)
	}

	return subs, nil
}

// Drain removes every registry row this instance holds and refuses new
// subscriptions; outstanding handles' Closes become no-ops. Call on shutdown
// once streams are closing. Best-effort by design: a row that fails to delete
// stops counting anyway once this member's heartbeats stop, and is swept as a
// corpse row after RowGCAfter.
func (s *Subscriptions) Drain(ctx context.Context) error {
	s.mu.Lock()
	s.draining = true
	topics := make([]*localTopic, 0, len(s.local))
	for _, t := range s.local {
		topics = append(topics, t)
	}
	clear(s.local)
	clear(s.cache)
	s.mu.Unlock()

	self := s.membership.Self()
	sem := make(chan struct{}, releaseConcurrency)
	var wg sync.WaitGroup
	for _, t := range topics {
		sem <- struct{}{}
		wg.Go(func() {
			defer func() { <-sem }()
			if err := s.store.DeleteSubscription(ctx, t.namespace, t.key, self.InstanceID); err != nil {
				s.log.With(zap.Error(err)).Warn("Failed to remove subscription row during drain",
					zap.String("namespace", t.namespace),
				)
			}
		})
	}
	wg.Wait()
	return ctx.Err()
}

// Resume lifts the drain latch for a caller that deliberately aborts a
// shutdown after Drain. Drained registrations are gone — consumers re-register
// as their streams reopen.
func (s *Subscriptions) Resume() {
	s.mu.Lock()
	s.draining = false
	s.mu.Unlock()
}

// reassertLocal re-puts every locally held topic's row after a liveness
// session interruption (see NewSubscriptions).
func (s *Subscriptions) reassertLocal(ctx context.Context) {
	s.mu.Lock()
	draining := s.draining
	topics := make([]*localTopic, 0, len(s.local))
	for _, t := range s.local {
		topics = append(topics, t)
	}
	s.mu.Unlock()
	if draining || len(topics) == 0 {
		return
	}

	s.log.Warn("Re-asserting subscription rows after liveness session interruption",
		zap.Int("topics", len(topics)),
	)
	sem := make(chan struct{}, releaseConcurrency)
	var wg sync.WaitGroup
	for _, t := range topics {
		sem <- struct{}{}
		wg.Go(func() {
			defer func() { <-sem }()
			select {
			case <-ctx.Done():
				return
			default:
			}
			s.reassertRow(t.namespace, t.key)
		})
	}
	wg.Wait()
}

// reassertRow re-puts one topic's row iff it is still backed by live handles,
// serialized against that topic's subscribe/unsubscribe so a re-assertion can
// never resurrect a row a concurrent unsubscribe is removing.
func (s *Subscriptions) reassertRow(namespace string, key []byte) {
	id := ownedKeyID(namespace, key)
	kl := s.lockTopic(id)
	defer s.unlockTopic(id, kl)

	s.mu.Lock()
	_, hasLocal := s.local[id]
	draining := s.draining
	s.mu.Unlock()
	if !hasLocal || draining {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), subscriptionOpTimeout)
	defer cancel()
	if err := s.store.PutSubscription(ctx, namespace, key, s.membership.Self()); err != nil {
		s.log.With(zap.Error(err)).Warn("Failed to re-assert subscription row",
			zap.String("namespace", namespace),
		)
		return
	}

	// Drain takes no topic locks, so its cutoff can land between the check
	// above and the put: the bulk delete then misses our row and it outlives
	// the process. Same shape as Subscribe's post-write re-check — hand the
	// row back ourselves. (Under the topic lock local[id] can only go from
	// present to absent: a re-creating Subscribe is blocked on the lock.)
	s.mu.Lock()
	_, hasLocal = s.local[id]
	draining = s.draining
	s.mu.Unlock()
	if hasLocal && !draining {
		return
	}
	if err := s.store.DeleteSubscription(ctx, namespace, key, s.membership.Self().InstanceID); err != nil {
		s.log.With(zap.Error(err)).Warn("Failed to hand back subscription row re-asserted during drain",
			zap.String("namespace", namespace),
		)
	}
}

// sweepSelfRow deletes this instance's row for a topic it no longer holds
// handles for (a failed unsubscribe delete), re-checking under the topic lock
// so a concurrent re-subscribe is never swept.
func (s *Subscriptions) sweepSelfRow(namespace string, key []byte) {
	id := ownedKeyID(namespace, key)
	kl := s.lockTopic(id)
	defer s.unlockTopic(id, kl)

	s.mu.Lock()
	_, hasLocal := s.local[id]
	s.mu.Unlock()
	if hasLocal {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), subscriptionOpTimeout)
	defer cancel()
	if err := s.store.DeleteSubscription(ctx, namespace, key, s.membership.Self().InstanceID); err != nil {
		s.log.With(zap.Error(err)).Warn("Failed to sweep own stale subscription row",
			zap.String("namespace", namespace),
		)
	}
}

// sweepCorpseRow best-effort deletes a crashed instance's leftover row
// encountered at resolution time.
func (s *Subscriptions) sweepCorpseRow(row *Subscription) {
	ctx, cancel := context.WithTimeout(context.Background(), subscriptionOpTimeout)
	defer cancel()
	if err := s.store.DeleteSubscription(ctx, row.Namespace, row.Key, row.InstanceID); err != nil {
		s.log.With(zap.Error(err)).Warn("Failed to sweep corpse subscription row; will retry at next resolution",
			zap.String("namespace", row.Namespace),
			zap.String("instance_id", row.InstanceID),
		)
		return
	}
	s.log.Info("Swept corpse subscription row",
		zap.String("namespace", row.Namespace),
		zap.String("instance_id", row.InstanceID),
	)
	// Deliberately NOT clearing unknownSince: the instance is still dead, and
	// other topics may hold more of its rows — their sweeps shouldn't restart
	// the observation clock. The entry clears if the instance is ever seen
	// live again, and is bounded meanwhile by distinct crashed instances.
}

func cloneSubscriptions(in []*Subscription) []*Subscription {
	out := make([]*Subscription, len(in))
	for i, sub := range in {
		out[i] = sub.Clone()
	}
	return out
}
