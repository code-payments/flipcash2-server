package cluster

import (
	"context"
	"errors"
	"strconv"
	"sync"
	"time"

	"go.uber.org/zap"
)

// NamespaceHooks are a consumer's ownership lifecycle callbacks. OnAcquired
// runs after a claim is won and before any work is served under it (warm
// state); OnReleased runs after the key's in-flight work has quiesced and
// before the claim is released (flush state). Hooks must not assume they run
// exactly once per key: a key can be acquired, idle-released, and re-acquired
// indefinitely.
//
// Hooks should not panic. A panicking OnAcquired retires the key's ownership
// (the half-warmed entry is dropped and the claim handed back for demand to
// re-acquire) and the panic propagates to Do's caller. A panicking OnReleased
// propagates through whichever goroutine is releasing — for the background
// reaper and rescan loops that is fatal to the process.
type NamespaceHooks struct {
	OnAcquired func(ctx context.Context, key []byte)
	OnReleased func(ctx context.Context, key []byte)
}

// Ownership acquires and serves exclusive key ownership on top of routing and
// claims. Keys are acquired lazily on first demand, held stickily until idle
// or rerouted, and drained gracefully on shutdown. It never blocks
// availability: any path that cannot own returns NotOwnerError so the caller
// can forward to the holder or fall back to its store-serialized path.
type Ownership struct {
	log        *zap.Logger
	membership *Membership
	router     *Router
	claims     ClaimStore
	cfg        OwnershipConfig

	mu          sync.Mutex
	namespaces  map[string]NamespaceHooks
	owned       map[string]*ownedKey
	unreachable map[string]time.Time
	redirects   map[string]redirectEntry
	keyLocks    map[string]*keyLock
	draining    bool

	rescanCh chan struct{}
	cancel   context.CancelFunc
	done     chan struct{}
}

// ownedKey tracks one locally owned claim. draining blocks new work from
// entering; inflight counts work currently running under the claim.
type ownedKey struct {
	namespace string
	key       []byte
	claim     *Claim

	mu       sync.Mutex
	cond     *sync.Cond
	inflight int
	warming  bool
	draining bool
	lastUsed time.Time
}

func (k *ownedKey) id() string { return ownedKeyID(k.namespace, k.key) }

// ownedKeyID keys the in-process maps. The namespace length prefix makes the
// (namespace, key) boundary unambiguous for arbitrary key bytes — the stores
// hex-encode for the same reason — without paying for hex on the hot path.
func ownedKeyID(namespace string, key []byte) string {
	return strconv.Itoa(len(namespace)) + "|" + namespace + string(key)
}

// redirectEntry caches a resolved claim holder for a briefly-trusted window so
// the non-owner forward path doesn't pay a store read per request on hot keys.
type redirectEntry struct {
	member  *Member
	expires time.Time
}

// keyLock serializes a key's slow-path {acquire, register} against its
// {flush, release, delete}. Without it, a Do racing a release can win a store
// re-acquire against the not-yet-vacated claim and register the entry after
// the release deletes it — local ownership with no claim behind it, which a
// membership change turns into two servers serving one key until the next
// rescan. Refcounted so the map holds only contended keys.
type keyLock struct {
	mu   sync.Mutex
	refs int
}

func (o *Ownership) lockKey(id string) *keyLock {
	o.mu.Lock()
	kl := o.keyLocks[id]
	if kl == nil {
		kl = &keyLock{}
		o.keyLocks[id] = kl
	}
	kl.refs++
	o.mu.Unlock()

	kl.mu.Lock()
	return kl
}

func (o *Ownership) unlockKey(id string, kl *keyLock) {
	kl.mu.Unlock()

	o.mu.Lock()
	kl.refs--
	if kl.refs == 0 {
		delete(o.keyLocks, id)
	}
	o.mu.Unlock()
}

// NewOwnership creates the ownership runtime. Call Start to begin the idle
// reaper and membership rescans.
func NewOwnership(log *zap.Logger, membership *Membership, router *Router, claims ClaimStore, cfg OwnershipConfig) *Ownership {
	cfg = cfg.withDefaults()
	// A suspicion window below one heartbeat plus one poll leaves zero
	// tolerance for heartbeat jitter: a peer whose beat merely landed late
	// would present matching stale evidence and be displaced while alive.
	if floor := membership.HeartbeatInterval() + membership.PollInterval(); cfg.SuspicionWindow < floor {
		cfg.SuspicionWindow = floor
	}
	return &Ownership{
		log:         log,
		membership:  membership,
		router:      router,
		claims:      claims,
		cfg:         cfg,
		namespaces:  make(map[string]NamespaceHooks),
		owned:       make(map[string]*ownedKey),
		unreachable: make(map[string]time.Time),
		redirects:   make(map[string]redirectEntry),
		keyLocks:    make(map[string]*keyLock),
		rescanCh:    make(chan struct{}, 1),
		done:        make(chan struct{}),
	}
}

// RegisterNamespace installs a namespace's lifecycle hooks. Register before
// serving traffic for the namespace.
func (o *Ownership) RegisterNamespace(namespace string, hooks NamespaceHooks) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.namespaces[namespace] = hooks
}

// Start launches the idle reaper and subscribes to membership changes for
// rebalance drains.
func (o *Ownership) Start(ctx context.Context) {
	loopCtx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	o.cancel = cancel

	o.membership.Subscribe(func() {
		select {
		case o.rescanCh <- struct{}{}:
		default:
		}
	})
	o.membership.OnSessionLost(func() {
		// Async: the notification comes from the heartbeat loop, and a purge
		// quiesces work — blocking there would stall the very heartbeats that
		// re-establish the session.
		go o.purgeOwned(context.WithoutCancel(loopCtx))
	})

	go o.run(loopCtx)
}

// Stop halts the background loops without draining. Use Drain first for a
// graceful shutdown.
func (o *Ownership) Stop() {
	if o.cancel != nil {
		o.cancel()
		<-o.done
	}
}

// Do runs fn under ownership of (namespace, key), acquiring the claim if this
// server is the routed owner and the key is unclaimed (or its holder is
// provably dead). It returns NotOwnerError when the key belongs elsewhere: a
// non-nil Redirect is the member to forward to; a nil Redirect means no
// healthy owner is known and the caller should use its store-serialized
// fallback. Any other error is fn's own.
func (o *Ownership) Do(ctx context.Context, namespace string, key []byte, fn func(ctx context.Context, claim *Claim) error) error {
	// Once our own heartbeats have gone stale the cluster is about to displace
	// our claims: stop trusting them immediately.
	if !o.membership.SelfHealthy() {
		return &NotOwnerError{}
	}

	if ok := o.enterOwned(namespace, key); ok != nil {
		defer ok.exit()
		return fn(ctx, ok.claim)
	}

	self := o.membership.Self()

	owner, err := o.router.Owner(ctx, namespace, key)
	if err != nil {
		if errors.Is(err, ErrNoMembers) {
			return &NotOwnerError{}
		}
		return err
	}

	if owner.InstanceID != self.InstanceID && o.takeoverEvidence(owner.InstanceID) != nil {
		// The routed owner is dead or corroborated-suspect to us but hasn't
		// aged out of the live set yet: route around it now rather than
		// redirecting requests into a black hole for the rest of the window.
		if alt, altErr := o.router.OwnerExcluding(ctx, namespace, key, map[string]bool{owner.InstanceID: true}); altErr == nil {
			owner = alt
		}
	}

	if owner.InstanceID != self.InstanceID {
		// Routed elsewhere. The claim record is the arbiter of where the key
		// actually lives right now (the routed owner may not have acquired
		// yet, or a draining ex-owner may still hold it) — but a hot key must
		// not pay a store read per forwarded request, so resolved redirects
		// are trusted for RedirectCacheTTL. A stale redirect self-corrects:
		// the target answers NotOwner, or a failed forward invalidates it via
		// NoteUnreachable.
		id := ownedKeyID(namespace, key)
		if member, ok := o.cachedRedirect(id); ok {
			return &NotOwnerError{Redirect: member}
		}
		claim, err := o.claims.GetClaim(ctx, namespace, key)
		switch {
		case err == nil:
			if claim.OwnerInstanceID == self.InstanceID {
				// We hold the claim but no longer serve the key (it is
				// mid-release on this server). Momentary: fall back rather
				// than redirect a forward loop back to ourselves.
				return &NotOwnerError{}
			}
			if o.takeoverEvidence(claim.OwnerInstanceID) != nil {
				// The holder is dead or suspect: redirect to the routed owner,
				// whose next acquire will displace the stale claim, instead of
				// bouncing the caller off a corpse. Not cached — this is a
				// transition in progress.
				return &NotOwnerError{Redirect: owner}
			}
			holder := &Member{
				InstanceID: claim.OwnerInstanceID,
				Address:    claim.OwnerAddress,
			}
			o.cacheRedirect(id, holder)
			return &NotOwnerError{Redirect: holder}
		case errors.Is(err, ErrClaimNotFound):
			o.cacheRedirect(id, owner)
			return &NotOwnerError{Redirect: owner}
		default:
			return err
		}
	}

	// Routed to self but draining: keys already owned are served by the fast
	// path above until their release, but no new ownership may be taken —
	// routing just hasn't converged off this member yet.
	if self.Draining {
		return &NotOwnerError{}
	}

	// The key lock makes {acquire, register} atomic with respect to a
	// concurrent release's {flush, vacate, delete}: without it, an acquire
	// evaluated against the not-yet-vacated claim could be registered after
	// the release completes — a local entry with no claim behind it.
	id := ownedKeyID(namespace, key)
	kl := o.lockKey(id)
	unlocked := false
	unlock := func() {
		if !unlocked {
			unlocked = true
			o.unlockKey(id, kl)
		}
	}
	// Deferred as well as called inline: a panic anywhere below (a consumer
	// OnAcquired hook, recovered by an interceptor above) must not strand the
	// key lock, or every future acquire AND release of the key would block
	// forever.
	defer unlock()

	// Re-check the fast path: a concurrent Do (or a release we waited out
	// followed by its re-acquirer) may have installed the entry meanwhile.
	if ok := o.enterOwned(namespace, key); ok != nil {
		unlock()
		defer ok.exit()
		return fn(ctx, ok.claim)
	}

	claim, err := o.acquire(ctx, namespace, key, self)
	if err != nil {
		return err
	}

	ok := o.registerOwned(ctx, namespace, key, claim)
	unlock()
	if ok == nil {
		// Lost a local race with a concurrent release or drain; retriable.
		return &NotOwnerError{}
	}
	defer ok.exit()
	return fn(ctx, ok.claim)
}

// NoteUnreachable records that a forward to the member failed. Combined with a
// heartbeat stale for at least SuspicionWindow, this makes the member's claims
// takeover-eligible ahead of the full liveness window — corroborated
// suspicion, so one network blip alone never displaces anyone.
func (o *Ownership) NoteUnreachable(instanceID string) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.unreachable[instanceID] = time.Now()

	// Cached redirects pointing at the unreachable member are now suspect:
	// drop them so the next request re-resolves instead of repeating the
	// failed forward for the rest of the cache window.
	for id, entry := range o.redirects {
		if entry.member.InstanceID == instanceID {
			delete(o.redirects, id)
		}
	}
}

func (o *Ownership) cachedRedirect(id string) (*Member, bool) {
	o.mu.Lock()
	defer o.mu.Unlock()
	entry, ok := o.redirects[id]
	if !ok {
		return nil, false
	}
	if time.Now().After(entry.expires) {
		// Prune on read: leaving expired entries to the reap tick would grow
		// the map by every distinct forwarded key in between, lengthening
		// every o.mu critical section.
		delete(o.redirects, id)
		return nil, false
	}
	return entry.member, true
}

func (o *Ownership) cacheRedirect(id string, member *Member) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.redirects[id] = redirectEntry{
		member:  member,
		expires: time.Now().Add(o.cfg.RedirectCacheTTL),
	}
}

// OwnedKeys returns the keys currently owned in the namespace.
func (o *Ownership) OwnedKeys(namespace string) [][]byte {
	o.mu.Lock()
	defer o.mu.Unlock()
	var out [][]byte
	for _, ok := range o.owned {
		if ok.namespace == namespace {
			key := make([]byte, len(ok.key))
			copy(key, ok.key)
			out = append(out, key)
		}
	}
	return out
}

// Drain gracefully hands off everything this server owns: mark draining in the
// registry (leaving routing candidacy while heartbeats keep held claims
// valid), then quiesce, flush, and release every owned key. Call on shutdown
// before Membership.Deregister. Respects ctx as an overall deadline; keys not
// drained in time are force-released.
func (o *Ownership) Drain(ctx context.Context) error {
	if err := o.membership.SetDraining(ctx, true); err != nil {
		o.log.With(zap.Error(err)).Warn("Failed to mark cluster member draining; proceeding to release claims")
	}

	// Acquisitions racing the draining flag can still land in o.owned: the
	// flag check and the map insert are guarded by different mutexes, so a Do
	// that read a pre-drain Self() may register after any single snapshot.
	// Setting o.draining under o.mu makes the cutoff atomic with inserts
	// (registerOwned refuses and hands the claim back once it is set), and
	// looping until the owned set is observed empty sweeps up everything
	// inserted before the cutoff — no claim is ever left for liveness expiry.
	o.mu.Lock()
	o.draining = true
	o.mu.Unlock()

	for {
		o.mu.Lock()
		keys := make([]*ownedKey, 0, len(o.owned))
		for _, ok := range o.owned {
			keys = append(keys, ok)
		}
		o.mu.Unlock()

		if len(keys) == 0 {
			return nil
		}

		if err := ctx.Err(); err != nil {
			// Shutdown budget exhausted: leftovers are displaced via liveness
			// expiry after deregistration instead. The draining latch is
			// deliberately left in place — a caller aborting shutdown (rather
			// than proceeding to exit) must opt back into service with Resume.
			o.log.With(zap.Error(err)).Warn("Drain context expired with claims possibly unreleased")
			return err
		}

		o.releaseAll(ctx, keys)

		// A concurrent releaser (rescan, idle reap, purge) may still be
		// mid-release of surviving entries — release() returns immediately on
		// an already-draining key, and the entry leaves o.owned only when its
		// first releaser finishes — so yield instead of spinning goroutine
		// waves until they complete.
		select {
		case <-ctx.Done():
		case <-time.After(20 * time.Millisecond):
		}
	}
}

// Resume returns a drained (or drain-aborted) instance to service: new
// ownership may be acquired again, and the member re-enters routing candidacy
// as peers observe the cleared flag. Only for callers that deliberately abort
// a shutdown after Drain — never on the way to exit, where accepting new keys
// again would strand them.
func (o *Ownership) Resume(ctx context.Context) error {
	if err := o.membership.SetDraining(ctx, false); err != nil {
		return err
	}
	o.mu.Lock()
	o.draining = false
	o.mu.Unlock()
	return nil
}

// releaseConcurrency bounds parallel claim releases during drains and
// rebalance rescans. Releases are independent single-item conditional writes,
// so a serial walk would put a large owned set's drain time (N × RTT) past the
// shutdown grace period; a bounded pool keeps it to a few round-trip waves
// without spiking the store.
const releaseConcurrency = 32

// releaseAll releases the keys with bounded concurrency, returning once every
// release has completed (or hit its own drain deadline).
func (o *Ownership) releaseAll(ctx context.Context, keys []*ownedKey) {
	sem := make(chan struct{}, releaseConcurrency)
	var wg sync.WaitGroup
	for _, ok := range keys {
		sem <- struct{}{}
		wg.Go(func() {
			defer func() { <-sem }()
			o.release(ctx, ok)
		})
	}
	wg.Wait()
}

// enterOwned enters the fast path if the key is locally owned and accepting
// work, incrementing its in-flight count.
func (o *Ownership) enterOwned(namespace string, key []byte) *ownedKey {
	o.mu.Lock()
	ok, exists := o.owned[ownedKeyID(namespace, key)]
	o.mu.Unlock()
	if !exists {
		return nil
	}

	ok.mu.Lock()
	defer ok.mu.Unlock()
	// OnAcquired must complete before any work is served under the claim:
	// block behind an in-progress warm rather than racing it.
	for ok.warming && !ok.draining {
		ok.cond.Wait()
	}
	if ok.draining {
		return nil
	}
	ok.inflight++
	ok.lastUsed = time.Now()
	return ok
}

// exit marks one unit of in-flight work complete.
func (ok *ownedKey) exit() {
	ok.mu.Lock()
	ok.inflight--
	ok.cond.Broadcast()
	ok.mu.Unlock()
}

// acquire wins (or confirms) the claim for a routed-to-self key, displacing a
// provably dead or corroborated-suspect holder. Returns NotOwnerError when the
// holder is alive.
func (o *Ownership) acquire(ctx context.Context, namespace string, key []byte, self *Member) (*Claim, error) {
	claim, err := o.claims.GetClaim(ctx, namespace, key)
	var takeover *TakeoverTarget
	switch {
	case errors.Is(err, ErrClaimNotFound):
		// Unclaimed: plain acquire below.
	case err != nil:
		return nil, err
	case claim.OwnerInstanceID == self.InstanceID:
		// A claim we hold in the store but not locally (e.g. process-internal
		// race after a release began): re-acquire below confirms it.
	default:
		takeover = o.takeoverEvidence(claim.OwnerInstanceID)
		if takeover == nil {
			return nil, &NotOwnerError{Redirect: &Member{
				InstanceID: claim.OwnerInstanceID,
				Address:    claim.OwnerAddress,
			}}
		}
	}

	acquired, err := o.claims.AcquireClaim(ctx, namespace, key, self, takeover)
	if errors.Is(err, ErrClaimHeld) {
		// Lost the race (or the takeover evidence went stale at commit time —
		// the holder was alive after all).
		redirect := &NotOwnerError{}
		if acquired != nil {
			redirect.Redirect = &Member{
				InstanceID: acquired.OwnerInstanceID,
				Address:    acquired.OwnerAddress,
			}
		}
		return nil, redirect
	} else if err != nil {
		return nil, err
	}
	return acquired, nil
}

// takeoverEvidence returns evidence justifying displacement of the holder, or
// nil if the holder must be presumed alive. Displacement is justified when the
// holder's heartbeat has been unmoving for the full liveness window, when the
// holder was reported unreachable and its heartbeat is stale past the
// suspicion window, or when the holder has no registry record at all (the
// store re-verifies that atomically with zero evidence).
func (o *Ownership) takeoverEvidence(instanceID string) *TakeoverTarget {
	counter, staleFor, ok := o.membership.LivenessInfo(instanceID)
	if !ok {
		// Never observed. If the record truly is absent (deregistered or
		// GC'd), the store's attribute-absence check passes; if the member
		// exists and we simply haven't polled it yet, the check fails and the
		// acquire loses safely.
		return &TakeoverTarget{InstanceID: instanceID}
	}

	if staleFor >= o.membership.LivenessWindow() {
		return &TakeoverTarget{InstanceID: instanceID, HeartbeatCounter: counter}
	}

	o.mu.Lock()
	reportedAt, reported := o.unreachable[instanceID]
	o.mu.Unlock()
	if reported && time.Since(reportedAt) < o.membership.LivenessWindow() && staleFor >= o.cfg.SuspicionWindow {
		// The report corroborates only while no heartbeat has been observed
		// since it was filed: a beat after the report proves the member alive
		// and store-reachable, making the earlier failed forward stale
		// evidence of nothing.
		if staleFor >= time.Since(reportedAt) {
			return &TakeoverTarget{InstanceID: instanceID, HeartbeatCounter: counter}
		}
	}

	return nil
}

// registerOwned installs a freshly acquired claim in the owned registry and
// fires OnAcquired, entering the key with one in-flight unit. If another
// goroutine acquired concurrently, its entry wins and is entered instead.
// Returns nil if the key is draining locally.
func (o *Ownership) registerOwned(ctx context.Context, namespace string, key []byte, claim *Claim) *ownedKey {
	id := ownedKeyID(namespace, key)

	o.mu.Lock()
	if o.draining {
		// Lost the race with a drain's atomic cutoff: hand the freshly won
		// claim straight back so the successor finds it vacant, and let the
		// caller fall back. The release is best-effort — on failure the
		// upcoming deregistration makes the claim displaceable anyway.
		o.mu.Unlock()
		releaseCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), o.cfg.DrainDeadline)
		defer cancel()
		if err := o.claims.ReleaseClaim(releaseCtx, namespace, key, claim.OwnerInstanceID); err != nil {
			o.log.With(zap.Error(err)).Warn("Failed to hand back claim acquired during drain",
				zap.String("namespace", namespace),
			)
		}
		return nil
	}
	if existing, exists := o.owned[id]; exists {
		o.mu.Unlock()
		existing.mu.Lock()
		defer existing.mu.Unlock()
		// A concurrent acquirer won the local race: wait out its OnAcquired
		// warm before serving under its entry.
		for existing.warming && !existing.draining {
			existing.cond.Wait()
		}
		if existing.draining {
			return nil
		}
		existing.inflight++
		existing.lastUsed = time.Now()
		return existing
	}

	// The entry is published warming: visible to concurrent Do calls (so they
	// wait here instead of racing a second acquire), but serving no work until
	// OnAcquired completes.
	ok := &ownedKey{
		namespace: namespace,
		key:       key,
		claim:     claim,
		inflight:  1,
		warming:   true,
		lastUsed:  time.Now(),
	}
	ok.cond = sync.NewCond(&ok.mu)
	o.owned[id] = ok
	hooks := o.namespaces[namespace]
	o.mu.Unlock()

	// The warm completion is deferred so a panicking OnAcquired (recovered by
	// an interceptor above Do) can never strand the entry warming with
	// waiters parked forever. On panic the entry is retired outright — its
	// state is half-warmed and must not be served — and the claim handed back
	// under the caller's still-held key lock, so demand re-acquires and
	// re-warms afresh.
	warmed := false
	defer func() {
		ok.mu.Lock()
		ok.warming = false
		if !warmed {
			ok.inflight--
			ok.draining = true
		}
		ok.cond.Broadcast()
		ok.mu.Unlock()

		if warmed {
			return
		}
		o.mu.Lock()
		if o.owned[id] == ok {
			delete(o.owned, id)
		}
		o.mu.Unlock()

		releaseCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), o.cfg.DrainDeadline)
		defer cancel()
		if err := o.claims.ReleaseClaim(releaseCtx, namespace, key, claim.OwnerInstanceID); err != nil {
			o.log.With(zap.Error(err)).Warn("Failed to hand back claim after OnAcquired panic",
				zap.String("namespace", namespace),
			)
		}
		o.log.Warn("OnAcquired hook panicked; retired the key's ownership",
			zap.String("namespace", namespace),
		)
	}()

	if hooks.OnAcquired != nil {
		hooks.OnAcquired(ctx, key)
	}
	warmed = true

	o.log.Debug("Acquired cluster ownership",
		zap.String("namespace", namespace),
		zap.Uint64("fence", claim.Fence),
	)
	return ok
}

// release drains one owned key: block new work, wait (bounded) for in-flight
// work, flush via OnReleased, then release the claim. The claim release is the
// atomic handoff point; the next request for the key anywhere re-routes and
// re-acquires.
func (o *Ownership) release(ctx context.Context, ok *ownedKey) {
	ok.mu.Lock()
	if ok.draining {
		ok.mu.Unlock()
		return
	}
	ok.draining = true

	deadline := time.Now().Add(o.cfg.DrainDeadline)
	for ok.inflight > 0 && time.Now().Before(deadline) {
		waitCond(ok.cond, time.Until(deadline))
	}
	forced := ok.inflight > 0
	ok.mu.Unlock()

	if forced {
		o.log.Warn("Drain deadline hit; force-releasing claim with work in flight",
			zap.String("namespace", ok.namespace),
		)
	}

	// Serialize the vacate-and-delete against any in-flight slow-path
	// acquire for this key (see keyLock): an acquirer that got in before us
	// has registered (and we saw its entry); one that gets in after us finds
	// the claim vacated and re-wins it legitimately.
	id := ok.id()
	kl := o.lockKey(id)
	defer o.unlockKey(id, kl)

	o.mu.Lock()
	hooks := o.namespaces[ok.namespace]
	o.mu.Unlock()

	if hooks.OnReleased != nil {
		hooks.OnReleased(ctx, ok.key)
	}

	releaseCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), o.cfg.DrainDeadline)
	err := o.claims.ReleaseClaim(releaseCtx, ok.namespace, ok.key, ok.claim.OwnerInstanceID)
	cancel()
	if err != nil {
		// The claim will be displaced via liveness expiry instead; correctness
		// is unaffected, handoff is just slower.
		o.log.With(zap.Error(err)).Warn("Failed to release claim; leaving it to liveness expiry",
			zap.String("namespace", ok.namespace),
		)
	}

	o.mu.Lock()
	if o.owned[ok.id()] == ok {
		delete(o.owned, ok.id())
	}
	o.mu.Unlock()

	o.log.Debug("Released cluster ownership", zap.String("namespace", ok.namespace))
}

// waitCond waits on cond with a timeout. Callers must hold cond's lock.
func waitCond(cond *sync.Cond, timeout time.Duration) {
	if timeout <= 0 {
		return
	}
	// The callback must take the lock before broadcasting: the caller holds it
	// until Wait enqueues the waiter, so this ordering guarantees the timer's
	// wakeup can never fire before there is a waiter to receive it — a bare
	// Broadcast could, losing the wakeup and unbounding the wait.
	timer := time.AfterFunc(timeout, func() {
		cond.L.Lock()
		defer cond.L.Unlock()
		cond.Broadcast()
	})
	defer timer.Stop()
	cond.Wait()
}

// rescanOwned re-routes every owned key after a membership change and drains
// the keys that now prefer another member. Handoff is owner-driven and sticky:
// until this drain completes, the routed-to member's acquire fails and
// requests forward here.
func (o *Ownership) rescanOwned(ctx context.Context) {
	self := o.membership.Self()

	o.mu.Lock()
	keys := make([]*ownedKey, 0, len(o.owned))
	for _, ok := range o.owned {
		keys = append(keys, ok)
	}
	o.mu.Unlock()

	var moved []*ownedKey
	for _, ok := range keys {
		owner, err := o.router.Owner(ctx, ok.namespace, ok.key)
		if err != nil {
			continue
		}
		if owner.InstanceID != self.InstanceID {
			moved = append(moved, ok)
		}
	}
	o.releaseAll(ctx, moved)
}

// reapIdle releases claims whose keys have gone unused for IdleTTL, so the
// claim table only ever holds the active working set. It also prunes expired
// redirect-cache entries and unreachable reports past the liveness window
// (already ignored by takeoverEvidence), keeping both maps bounded.
func (o *Ownership) reapIdle(ctx context.Context) {
	now := time.Now()
	livenessWindow := o.membership.LivenessWindow()

	o.mu.Lock()
	var idle []*ownedKey
	for _, ok := range o.owned {
		ok.mu.Lock()
		if ok.inflight == 0 && !ok.draining && time.Since(ok.lastUsed) >= o.cfg.IdleTTL {
			idle = append(idle, ok)
		}
		ok.mu.Unlock()
	}
	for id, entry := range o.redirects {
		if now.After(entry.expires) {
			delete(o.redirects, id)
		}
	}
	for instanceID, reportedAt := range o.unreachable {
		if now.Sub(reportedAt) >= livenessWindow {
			delete(o.unreachable, instanceID)
		}
	}
	o.mu.Unlock()

	for _, ok := range idle {
		o.release(ctx, ok)
	}
}

// purgeOwned sheds every locally owned key after a liveness-session
// interruption: claims may have been displaced (or the whole record deleted)
// while heartbeats gapped, so local ownership state cannot be trusted. Each
// key is quiesced, flushed, and conditionally released — a claim still ours
// releases cleanly (demand re-acquires it), one displaced no-ops. Prevention
// of serving under a displaced claim during the gap itself is the consumer's
// store backstop; this is the remediation that stops it continuing.
func (o *Ownership) purgeOwned(ctx context.Context) {
	o.mu.Lock()
	keys := make([]*ownedKey, 0, len(o.owned))
	for _, ok := range o.owned {
		keys = append(keys, ok)
	}
	clear(o.redirects)
	o.mu.Unlock()

	if len(keys) == 0 {
		return
	}
	o.log.Warn("Shedding local ownership after liveness session interruption",
		zap.Int("keys", len(keys)),
	)
	o.releaseAll(ctx, keys)
}

func (o *Ownership) run(ctx context.Context) {
	defer close(o.done)

	reap := time.NewTicker(o.cfg.ReapInterval)
	defer reap.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-o.rescanCh:
			o.rescanOwned(ctx)
		case <-reap.C:
			o.reapIdle(ctx)
		}
	}
}
