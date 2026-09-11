package cluster

import (
	"context"
	"errors"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cespare/xxhash/v2"
	"go.uber.org/zap"
)

// ErrSubscriptionsDraining is returned by Subscribe once Drain has begun: a
// registration accepted mid-drain would write a row nothing will clean up.
var ErrSubscriptionsDraining = errors.New("cluster subscriptions are draining")

// ErrObserverMembership is returned by Subscribe on a runtime backed by an
// observer membership. An observer has no member record, so a row it wrote
// would never count (a row's validity is its member's liveness) and would
// eventually be swept as a corpse. Observers resolve and forward; only
// registered members host streams.
var ErrObserverMembership = errors.New("observer membership cannot register interest")

// subscriptionOpTimeout bounds the background row writes the runtime performs
// on its own behalf (sweeps and re-assertions), which carry no caller context.
const subscriptionOpTimeout = 5 * time.Second

// subscriptionShards is the number of independently locked partitions of the
// per-topic state. A publish resolves under one shard's lock; a stream open
// or close touches each of its topics' shards once. Contention is therefore
// between operations on the same shard, not between every publish and every
// stream on the server — the same partitioning the event stream registry
// uses, for the same reconnect-storm reason. A power of two keeps the pick a
// mask.
const subscriptionShards = 64

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
//
// Per-topic state (local refcounts, the resolution cache, the topic write
// locks) is partitioned across subscriptionShards by topic ID, each shard
// behind its own mutex. Nothing ever holds two shard locks at once, so there
// is no lock ordering between shards to get wrong. What is not per-topic
// lives outside the shards: the generation counter and drain latch are
// atomics, and the corpse-sweep clock (keyed by instance, not topic) has a
// mutex of its own.
type Subscriptions struct {
	log        *zap.Logger
	membership *Membership
	store      SubscriptionStore
	cfg        SubscriptionsConfig

	shards [subscriptionShards]subscriptionShard

	// nextGen numbers topic registrations so a handle can tell its own
	// registration from a later one occupying the same topic slot (a stale
	// handle Closed after a Drain+Resume cycle must not decrement — let alone
	// delete — a successor registration).
	nextGen atomic.Uint64

	// draining refuses new registrations once Drain has begun. It is set
	// before Drain takes any shard lock and checked under the shard lock as
	// registrations land, so a registration either lands before the drain
	// sweeps its shard (and is swept) or observes the latch — never neither.
	draining atomic.Bool

	// sweepMu guards unknownSince, which tracks, per instance ID, when this
	// observer first saw a subscription row whose member was wholly unobserved
	// (no registry record in the membership view) — the observation timeline
	// behind corpse-row sweeps. Entries re-anchor while the member's record is
	// still observed, clear on any live sighting and on live-set convergence,
	// and are bounded by crashed instances with rows still being resolved.
	sweepMu      sync.Mutex
	unknownSince map[string]time.Time
}

// subscriptionShard is one partition of the per-topic state.
type subscriptionShard struct {
	mu         sync.Mutex
	local      map[string]*localTopic
	cache      map[string]subscriberCacheEntry
	topicLocks map[string]*keyLock
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
//
// An observer membership (NewObserver) is a valid backing for the read side:
// Subscribers resolves (and sweeps corpse rows) exactly as on a member, so an
// event-forwarding-only process can publish toward the fleet. Subscribe is
// refused with ErrObserverMembership — an observer cannot host streams.
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
		unknownSince: make(map[string]time.Time),
	}
	for i := range s.shards {
		s.shards[i] = subscriptionShard{
			local:      make(map[string]*localTopic),
			cache:      make(map[string]subscriberCacheEntry),
			topicLocks: make(map[string]*keyLock),
		}
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
	// The corpse clock must measure continuous absence on the membership
	// timeline, not the gap between the resolutions that happen to touch an
	// instance's rows: a member returning to the live view clears its anchor
	// even when no resolution observed the return, so two transient blips
	// RowGCAfter apart can never add up to sweeping a live subscriber's row.
	membership.Subscribe(func() {
		liveNow := membership.Live()
		s.sweepMu.Lock()
		for _, m := range liveNow {
			delete(s.unknownSince, m.InstanceID)
		}
		s.sweepMu.Unlock()
	})
	return s
}

// Self returns the local member whose interest this runtime registers — the
// identity carried by every subscriber row this process writes. Consumers use
// it to recognize their own rows in Subscribers results exactly (by instance
// ID) instead of comparing separately-configured addresses. Nil on an
// observer, which writes no rows.
func (s *Subscriptions) Self() *Member {
	return s.membership.Self()
}

func (s *Subscriptions) shardFor(id string) *subscriptionShard {
	return &s.shards[xxhash.Sum64String(id)&(subscriptionShards-1)]
}

// groupByShard buckets topic IDs (given as indices into a caller's slice) by
// the shard that owns them, so a multi-topic operation locks each shard
// exactly once.
func (s *Subscriptions) groupByShard(ids []string) map[*subscriptionShard][]int {
	grouped := make(map[*subscriptionShard][]int)
	for i, id := range ids {
		shard := s.shardFor(id)
		grouped[shard] = append(grouped[shard], i)
	}
	return grouped
}

// lockTopic and unlockTopic mirror Ownership's key locks: they serialize a
// topic's registry writes so a first-subscribe racing a last-unsubscribe can
// never interleave as put-then-delete — a live stream with no row behind it,
// which would silently stop delivery to this server for the topic. The lock
// objects live in the topic's shard; the shard lock is held only to find or
// drop them, never while the topic lock is taken.
func (s *Subscriptions) lockTopic(id string) *keyLock {
	shard := s.shardFor(id)
	shard.mu.Lock()
	kl := shard.topicLocks[id]
	if kl == nil {
		kl = &keyLock{}
		shard.topicLocks[id] = kl
	}
	kl.refs++
	shard.mu.Unlock()

	kl.mu.Lock()
	return kl
}

func (s *Subscriptions) unlockTopic(id string, kl *keyLock) {
	kl.mu.Unlock()

	shard := s.shardFor(id)
	shard.mu.Lock()
	kl.refs--
	if kl.refs == 0 {
		delete(shard.topicLocks, id)
	}
	shard.mu.Unlock()
}

// lockTopics takes the distinct topic locks for ids in sorted order:
// concurrent multi-topic operations with overlapping topic sets always
// contend in the same order, and the single-topic operations hold at most one
// of these locks at a time, so no cycle can form. The returned func releases
// them all.
func (s *Subscriptions) lockTopics(ids []string) (unlock func()) {
	seen := make(map[string]struct{}, len(ids))
	distinct := make([]string, 0, len(ids))
	for _, id := range ids {
		if _, dup := seen[id]; dup {
			continue
		}
		seen[id] = struct{}{}
		distinct = append(distinct, id)
	}
	sort.Strings(distinct)
	locks := make([]*keyLock, len(distinct))
	for i, id := range distinct {
		locks[i] = s.lockTopic(id)
	}
	return func() {
		for i, id := range distinct {
			s.unlockTopic(id, locks[i])
		}
	}
}

// Subscribe registers a local stream's interest in the topic. The first local
// handle writes the topic's registry row; later handles share it. Returns
// ErrSubscriptionsDraining once Drain has begun, and ErrObserverMembership on
// a runtime backed by an observer membership.
func (s *Subscriptions) Subscribe(ctx context.Context, namespace string, key []byte) (*SubscriptionHandle, error) {
	if s.membership.observer() {
		return nil, ErrObserverMembership
	}
	id := ownedKeyID(namespace, key)
	kl := s.lockTopic(id)
	defer s.unlockTopic(id, kl)

	shard := s.shardFor(id)
	shard.mu.Lock()
	if s.draining.Load() {
		shard.mu.Unlock()
		return nil, ErrSubscriptionsDraining
	}
	if t, ok := shard.local[id]; ok {
		t.refs++
		gen := t.gen
		shard.mu.Unlock()
		return s.handle(namespace, key, gen), nil
	}
	shard.mu.Unlock()

	// First local subscriber: the row must exist before the handle does, or a
	// publish resolved in between would miss a stream the caller believes is
	// registered.
	if err := s.store.PutSubscription(ctx, namespace, key, s.membership.Self().InstanceID); err != nil {
		return nil, err
	}

	shard.mu.Lock()
	if s.draining.Load() {
		// Lost the race with Drain's cutoff: the bulk delete may have run
		// before our row landed, so hand it back ourselves.
		shard.mu.Unlock()
		deleteCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), subscriptionOpTimeout)
		defer cancel()
		if err := s.store.DeleteSubscription(deleteCtx, namespace, key, s.membership.Self().InstanceID); err != nil {
			s.log.With(zap.Error(err)).Warn("Failed to hand back subscription row acquired during drain",
				zap.String("namespace", namespace),
			)
		}
		return nil, ErrSubscriptionsDraining
	}
	gen := s.nextGen.Add(1)
	shard.local[id] = &localTopic{
		namespace: namespace,
		key:       append([]byte(nil), key...),
		refs:      1,
		gen:       gen,
	}
	// Invalidate so a local publish resolved before the row landed doesn't
	// keep excluding self for the rest of the cache window.
	delete(shard.cache, id)
	shard.mu.Unlock()

	return s.handle(namespace, key, gen), nil
}

// SubscribeAll registers a local stream's interest in every listed topic, as
// Subscribe does for one, writing the missing registry rows in a single store
// batch instead of one round trip per topic. It returns one handle per entry,
// in input order; a duplicated topic gets distinct handles against the same
// registration, exactly as two Subscribe calls would.
//
// All or nothing at the registration level: on error no handles exist and no
// refcounts moved. A failed batch may still have landed some rows — they are
// refcount-less self rows, indistinguishable from a failed unsubscribe delete,
// and the next local resolution of the topic sweeps them (see Subscribers).
func (s *Subscriptions) SubscribeAll(ctx context.Context, topics []SubscriptionTopic) ([]*SubscriptionHandle, error) {
	if len(topics) == 0 {
		return nil, nil
	}
	if s.membership.observer() {
		return nil, ErrObserverMembership
	}

	ids := make([]string, len(topics))
	for i, t := range topics {
		ids[i] = ownedKeyID(t.Namespace, t.Key)
	}
	defer s.lockTopics(ids)()
	byShard := s.groupByShard(ids)

	// Holding every topic lock freezes the registrations: a topic present in
	// local cannot lose its row to a concurrent last-unsubscribe, and an
	// absent one cannot gain a registration. Classify — each shard locked
	// once — then write only what is missing.
	if s.draining.Load() {
		return nil, ErrSubscriptionsDraining
	}
	var missing []SubscriptionTopic
	missingIDs := make(map[string]struct{})
	for shard, indices := range byShard {
		shard.mu.Lock()
		for _, i := range indices {
			if _, ok := shard.local[ids[i]]; ok {
				continue
			}
			if _, dup := missingIDs[ids[i]]; dup {
				continue
			}
			missingIDs[ids[i]] = struct{}{}
			missing = append(missing, topics[i])
		}
		shard.mu.Unlock()
	}

	// The rows must exist before the handles do, for the same reason as in
	// Subscribe: a publish resolved in between would miss streams the caller
	// believes are registered.
	if len(missing) > 0 {
		if err := s.store.PutSubscriptions(ctx, missing, s.membership.Self().InstanceID); err != nil {
			return nil, err
		}
	}

	// Register — each shard locked once. Drain's latch is re-checked under
	// every shard lock, exactly as in Subscribe: a shard registered before the
	// latch is swept by Drain (which visits every shard after setting it), and
	// one that observes the latch stops here. Either way no registration
	// outlives the drain, so on the latch the batch is undone: registrations
	// already landed are released and every row written above is handed back.
	handles := make([]*SubscriptionHandle, len(topics))
	var registered []int
	for shard, indices := range byShard {
		shard.mu.Lock()
		if s.draining.Load() {
			shard.mu.Unlock()
			s.rollBackRegistrations(ids, registered)
			deleteCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), subscriptionOpTimeout)
			defer cancel()
			if err := s.store.DeleteSubscriptions(deleteCtx, missing, s.membership.Self().InstanceID); err != nil {
				s.log.With(zap.Error(err)).Warn("Failed to hand back subscription rows acquired during drain")
			}
			return nil, ErrSubscriptionsDraining
		}
		for _, i := range indices {
			lt, ok := shard.local[ids[i]]
			if !ok {
				lt = &localTopic{
					namespace: topics[i].Namespace,
					key:       append([]byte(nil), topics[i].Key...),
					gen:       s.nextGen.Add(1),
				}
				shard.local[ids[i]] = lt
				// Invalidate so a local publish resolved before the row landed
				// doesn't keep excluding self for the rest of the cache window.
				delete(shard.cache, ids[i])
			}
			lt.refs++
			handles[i] = s.handle(topics[i].Namespace, topics[i].Key, lt.gen)
		}
		registered = append(registered, indices...)
		shard.mu.Unlock()
	}

	return handles, nil
}

// rollBackRegistrations undoes the refcounts SubscribeAll landed before it
// observed the drain latch. The topic locks are still held, so the only other
// mutator is Drain, which clears whole shards; a topic it already cleared is
// simply absent here.
func (s *Subscriptions) rollBackRegistrations(ids []string, indices []int) {
	byShard := make(map[*subscriptionShard][]int)
	for _, i := range indices {
		shard := s.shardFor(ids[i])
		byShard[shard] = append(byShard[shard], i)
	}
	for shard, shardIndices := range byShard {
		shard.mu.Lock()
		for _, i := range shardIndices {
			t, ok := shard.local[ids[i]]
			if !ok {
				continue
			}
			t.refs--
			if t.refs == 0 {
				delete(shard.local, ids[i])
				delete(shard.cache, ids[i])
			}
		}
		shard.mu.Unlock()
	}
}

// CloseAll releases every handle in one pass, batching the registry deletes
// for the topics whose last local handle is among them — the teardown twin of
// SubscribeAll. Each handle is released exactly once across any mix of
// CloseAll and Close calls; handles already closed (or nil) are skipped, and a
// handle from another runtime falls back to its own single-row Close.
//
// The batch delete's error is returned from CloseAll rather than attributed
// to any handle — the registrations are gone locally regardless, and as with
// Drain, a row that fails to delete stops counting once this member stops
// heartbeating and is eventually swept.
func (s *Subscriptions) CloseAll(ctx context.Context, handles []*SubscriptionHandle) error {
	// Claim each unfired handle: the once latches, so a later Close is a
	// no-op, exactly as a Close latches against a later CloseAll.
	pending := make([]*SubscriptionHandle, 0, len(handles))
	var foreignErr error
	for _, h := range handles {
		if h == nil {
			continue
		}
		if h.subs != s {
			if err := h.Close(ctx); err != nil && foreignErr == nil {
				foreignErr = err
			}
			continue
		}
		h.once.Do(func() {
			pending = append(pending, h)
		})
	}
	if len(pending) == 0 {
		return foreignErr
	}

	ids := make([]string, len(pending))
	for i, h := range pending {
		ids[i] = ownedKeyID(h.namespace, h.key)
	}
	defer s.lockTopics(ids)()

	// Decrement every registration — each shard locked once — collecting the
	// topics this batch emptied; the stale-handle guards mirror unsubscribe
	// exactly.
	var emptied []SubscriptionTopic
	for shard, indices := range s.groupByShard(ids) {
		shard.mu.Lock()
		for _, i := range indices {
			h := pending[i]
			t, ok := shard.local[ids[i]]
			if !ok || t.gen != h.gen {
				// Drained out from under the handle, or a successor registration
				// occupies the slot; nothing of ours to release.
				continue
			}
			t.refs--
			if t.refs > 0 {
				continue
			}
			delete(shard.local, ids[i])
			delete(shard.cache, ids[i])
			emptied = append(emptied, SubscriptionTopic{Namespace: h.namespace, Key: h.key})
		}
		shard.mu.Unlock()
	}

	if len(emptied) > 0 {
		if err := s.store.DeleteSubscriptions(ctx, emptied, s.membership.Self().InstanceID); err != nil {
			return err
		}
	}
	return foreignErr
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

	shard := s.shardFor(id)
	shard.mu.Lock()
	t, ok := shard.local[id]
	if !ok {
		// Drained out from under the handle; the bulk path removed the row.
		shard.mu.Unlock()
		return nil
	}
	if t.gen != gen {
		// The handle's registration is already gone (drained, then the slot
		// re-created by a post-Resume Subscribe); the current registration's
		// refcount belongs to its own handles.
		shard.mu.Unlock()
		return nil
	}
	t.refs--
	if t.refs > 0 {
		shard.mu.Unlock()
		return nil
	}
	delete(shard.local, id)
	delete(shard.cache, id)
	shard.mu.Unlock()

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
	shard := s.shardFor(id)

	shard.mu.Lock()
	if entry, ok := shard.cache[id]; ok {
		if time.Now().Before(entry.expires) {
			out := cloneSubscriptions(entry.subs)
			shard.mu.Unlock()
			return out, nil
		}
		// Prune on read, like the redirect cache: leaving expired entries
		// around would grow the map by every topic resolved in between.
		delete(shard.cache, id)
	}
	shard.mu.Unlock()

	rows, err := s.store.GetSubscribers(ctx, namespace, key)
	if err != nil {
		return nil, err
	}

	// Rows carry identity only; the member's dialable address comes from the
	// membership view — available for exactly the rows that count, since a row
	// counts only while its member is in this observer's live view.
	live := make(map[string]string)
	for _, m := range s.membership.Live() {
		live[m.InstanceID] = m.Address
	}
	// A row's member being absent from the live view does not make the row a
	// corpse candidate: only an instance with no membership observation at all
	// (its registry record gone from scans) can be a crashed instance's
	// leftover. Snapshot which not-live row instances are still observed
	// before taking any runtime lock.
	observed := make(map[string]bool, len(rows))
	for _, row := range rows {
		if _, isLive := live[row.InstanceID]; isLive || observed[row.InstanceID] {
			continue
		}
		if _, _, ok := s.membership.LivenessInfo(row.InstanceID); ok {
			observed[row.InstanceID] = true
		}
	}
	self := s.membership.Self()
	now := time.Now()

	// The corpse clock, under its own lock: live members clear their anchor,
	// not-live ones are excluded from delivery immediately — correctness
	// never waits on cleanup — and those whose member stays wholly unobserved
	// past RowGCAfter are crashed instances' leftovers (drains clean up after
	// themselves), swept so hot topics don't accumulate garbage.
	var corpseRows []*Subscription
	s.sweepMu.Lock()
	for _, row := range rows {
		if _, isLive := live[row.InstanceID]; isLive {
			delete(s.unknownSince, row.InstanceID)
			continue
		}
		if self != nil && row.InstanceID == self.InstanceID {
			continue
		}
		if observed[row.InstanceID] {
			// A registry record still backs this member: it is slow or
			// transiently unseen, not dead. Re-anchor so a stale first-sighting
			// can never age into sweeping a live subscriber's row.
			s.unknownSince[row.InstanceID] = now
			continue
		}
		first, seen := s.unknownSince[row.InstanceID]
		if !seen {
			s.unknownSince[row.InstanceID] = now
		} else if now.Sub(first) >= s.cfg.RowGCAfter {
			corpseRows = append(corpseRows, row)
		}
	}
	s.sweepMu.Unlock()

	// Self is judged against the local refcounts and the result cached in the
	// same critical section, so a registration landing in between cannot be
	// papered over by a cache entry that excludes self for a whole TTL.
	shard.mu.Lock()
	_, locallySubscribed := shard.local[id]

	subs := make([]*Subscription, 0, len(rows))
	selfInRows := false
	staleSelfRow := false
	for _, row := range rows {
		addr, isLive := live[row.InstanceID]
		switch {
		// self is nil on an observer, which never has rows of its own.
		case self != nil && row.InstanceID == self.InstanceID:
			selfInRows = true
			if locallySubscribed {
				row.Address = self.Address
				subs = append(subs, row)
			} else {
				// Our own row with no handles behind it: a failed unsubscribe
				// delete. Swept below (re-checked under the topic lock).
				staleSelfRow = true
			}
		case isLive:
			row.Address = addr
			subs = append(subs, row)
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

	shard.cache[id] = subscriberCacheEntry{
		subs:    cloneSubscriptions(subs),
		expires: now.Add(s.cfg.CacheTTL),
	}
	shard.mu.Unlock()

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
	// The latch goes up before any shard is visited (see draining).
	s.draining.Store(true)

	var topics []*localTopic
	for i := range s.shards {
		shard := &s.shards[i]
		shard.mu.Lock()
		for _, t := range shard.local {
			topics = append(topics, t)
		}
		clear(shard.local)
		clear(shard.cache)
		shard.mu.Unlock()
	}

	if len(topics) > 0 {
		rows := make([]SubscriptionTopic, len(topics))
		for i, t := range topics {
			rows[i] = SubscriptionTopic{Namespace: t.namespace, Key: t.key}
		}
		if err := s.store.DeleteSubscriptions(ctx, rows, s.membership.Self().InstanceID); err != nil {
			s.log.With(zap.Error(err)).Warn("Failed to remove subscription rows during drain")
		}
	}
	return ctx.Err()
}

// Resume lifts the drain latch for a caller that deliberately aborts a
// shutdown after Drain. Drained registrations are gone — consumers re-register
// as their streams reopen.
func (s *Subscriptions) Resume() {
	s.draining.Store(false)
}

// localTopics snapshots every registration across the shards.
func (s *Subscriptions) localTopics() []*localTopic {
	var topics []*localTopic
	for i := range s.shards {
		shard := &s.shards[i]
		shard.mu.Lock()
		for _, t := range shard.local {
			topics = append(topics, t)
		}
		shard.mu.Unlock()
	}
	return topics
}

// hasLocal reports whether the topic is registered locally.
func (s *Subscriptions) hasLocal(id string) bool {
	shard := s.shardFor(id)
	shard.mu.Lock()
	_, ok := shard.local[id]
	shard.mu.Unlock()
	return ok
}

// reassertLocal re-puts every locally held topic's row after a liveness
// session interruption (see NewSubscriptions).
func (s *Subscriptions) reassertLocal(ctx context.Context) {
	if s.draining.Load() {
		return
	}
	topics := s.localTopics()
	if len(topics) == 0 {
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

	if !s.hasLocal(id) || s.draining.Load() {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), subscriptionOpTimeout)
	defer cancel()
	if err := s.store.PutSubscription(ctx, namespace, key, s.membership.Self().InstanceID); err != nil {
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
	if s.hasLocal(id) && !s.draining.Load() {
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

	if s.hasLocal(id) {
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
	// Last look before the delete: the sweep decision was made against one
	// resolution's snapshot. A member observed again by now is slow, not dead
	// — its row must stand.
	if _, _, observed := s.membership.LivenessInfo(row.InstanceID); observed {
		return
	}
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
	// live again (at resolution or via membership convergence), and is bounded
	// meanwhile by distinct crashed instances.
}

func cloneSubscriptions(in []*Subscription) []*Subscription {
	out := make([]*Subscription, len(in))
	for i, sub := range in {
		out[i] = sub.Clone()
	}
	return out
}
