package cluster

import (
	"context"
	"errors"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"go.uber.org/zap"
)

// Membership registers this process in the cluster and maintains an observed
// view of every other member. Liveness is judged KCL-style: a peer is live
// while its heartbeat counter keeps moving across this process's own
// observation timeline — wall clocks are never compared across machines, so
// clock skew cannot produce false takeovers.
//
// A Membership constructed with NewObserver observes without registering: it
// maintains the same live view but writes no member record (see NewObserver
// for the exact semantics of the self-referential methods).
type Membership struct {
	log   *zap.Logger
	store RegistryStore
	cfg   MembershipConfig

	// self is nil in observer mode; the pointer is fixed at construction
	// (fields behind it are guarded by mu, the pointer itself needs no lock).
	self *Member

	mu              sync.RWMutex
	observations    map[string]*observation
	live            []*Member
	liveSignature   string
	subscribers     []func()
	sessionLostSubs []func()
	lastSelfBeat    time.Time
	// sessionLost latches once a heartbeat gap has fired OnSessionLost so a
	// sustained write outage sheds session state exactly once, not on every
	// failed beat. Cleared when a beat lands (or the record is re-registered).
	sessionLost bool
	// lastCounter is the highest heartbeat counter this process has written.
	// A re-registration must resume strictly above it: the takeover guard is
	// an equality check against observed-stale counters, so a value repeating
	// across registration epochs would let stale evidence displace a live
	// owner.
	lastCounter uint64
	// lastRefresh is when the registry was last successfully re-read. It is an
	// observer's health signal: with no heartbeat of its own, a stale view is
	// the only way an observer can be wrong.
	lastRefresh time.Time
	started     bool

	cancel context.CancelFunc
	done   chan struct{}
}

// observation is this process's local view of one member's heartbeat: the last
// counter value seen and when (on our clock) it was last seen to change.
type observation struct {
	record     *MemberRecord
	lastChange time.Time
}

// NewMembership creates the membership runtime for self. Call Start to
// register and begin heartbeating.
func NewMembership(log *zap.Logger, store RegistryStore, self *Member, cfg MembershipConfig) *Membership {
	return &Membership{
		log:          log,
		store:        store,
		cfg:          cfg.withDefaults(),
		self:         self.Clone(),
		observations: make(map[string]*observation),
		done:         make(chan struct{}),
	}
}

// NewObserver creates a read-only membership runtime: it polls the registry
// and maintains the observed live view exactly like a member, but never
// registers a record of its own — it cannot be routed to, holds no claims or
// subscriptions, and leaves nothing behind on exit. For processes that consume
// the cluster without serving it, e.g. an event-streaming-only client that
// resolves owners and forwards but hosts nothing.
//
// On an observer: Self returns nil; SetDraining and Deregister are vacuous
// no-ops (there is no record to flip or delete); OnSessionLost never fires
// (there is no liveness session to lose); and SelfHealthy reports whether
// registry polls are landing — a stale view, not a stale heartbeat, is the
// failure mode an observer must surface.
func NewObserver(log *zap.Logger, store RegistryStore, cfg MembershipConfig) *Membership {
	return &Membership{
		log:          log,
		store:        store,
		cfg:          cfg.withDefaults(),
		observations: make(map[string]*observation),
		done:         make(chan struct{}),
	}
}

// observer reports whether this runtime observes without registering.
func (m *Membership) observer() bool {
	return m.self == nil
}

// Start registers self (observers skip registration), performs an initial
// refresh, and launches the heartbeat and poll loops (observers run only the
// poll loop). The loops run until Stop (or Deregister).
func (m *Membership) Start(ctx context.Context) error {
	if !m.observer() {
		if err := m.store.PutMember(ctx, m.self, 1); err != nil {
			return err
		}
	}

	if err := m.Refresh(ctx); err != nil {
		// Registered but unable to run: don't leave a corpse record behind,
		// and don't report healthy — no heartbeat loop will back it.
		if !m.observer() {
			if delErr := m.store.DeleteMember(ctx, m.self.InstanceID); delErr != nil {
				m.log.With(zap.Error(delErr)).Warn("Failed to remove member record after aborted start")
			}
		}
		return err
	}

	// Only a running heartbeat loop may report healthy: mark started strictly
	// after the last fallible step.
	m.mu.Lock()
	m.lastSelfBeat = time.Now()
	m.lastCounter = 1
	m.started = true
	m.mu.Unlock()

	loopCtx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	m.cancel = cancel
	go m.run(loopCtx)

	return nil
}

// Stop halts the heartbeat and poll loops without deregistering: peers will
// observe the heartbeat go stale and expire this member the hard way. Prefer
// Deregister for a graceful exit.
func (m *Membership) Stop() {
	if m.cancel != nil {
		m.cancel()
		<-m.done
	}
}

// Deregister removes self from the registry and stops the loops. Call only
// after all owned claims are released: a deregistered member's claims are
// instantly displaceable. On an observer it only stops the poll loop — there
// is no record to remove.
func (m *Membership) Deregister(ctx context.Context) error {
	m.Stop()
	if m.observer() {
		return nil
	}
	return m.store.DeleteMember(ctx, m.self.InstanceID)
}

// Self returns this process's member identity (with its current draining
// flag), or nil on an observer — which has no identity to route to.
func (m *Membership) Self() *Member {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.self == nil {
		return nil
	}
	return m.self.Clone()
}

// SetDraining flips this member's draining flag in the registry and locally.
// Draining removes the member from routing candidacy while its heartbeat keeps
// held claims valid — the first step of graceful shutdown. On an observer it
// is a vacuous no-op: an observer was never a routing candidate.
func (m *Membership) SetDraining(ctx context.Context, draining bool) error {
	if m.observer() {
		return nil
	}
	if draining {
		// Fail closed: flip the local flag before the store round-trip so no
		// acquisition can slip in while the write is in flight — a Do that
		// reads Self() from here on refuses new ownership immediately.
		m.mu.Lock()
		m.self.Draining = true
		m.mu.Unlock()
	}
	if err := m.store.SetDraining(ctx, m.self.InstanceID, draining); err != nil {
		return err
	}
	if !draining {
		m.mu.Lock()
		m.self.Draining = false
		m.mu.Unlock()
	}
	return nil
}

// Live returns the members currently considered live, draining members
// included (they hold valid claims); routing filters draining out of candidacy
// separately.
func (m *Membership) Live() []*Member {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make([]*Member, len(m.live))
	for i, member := range m.live {
		out[i] = member.Clone()
	}
	return out
}

// SelfHealthy reports whether this process's own heartbeats are landing. Once
// they have failed for SelfUnhealthyAfter, a suspicious peer may already be
// able to displace this member's claims, so it must stop trusting them.
//
// An observer has no heartbeat; for it this reports whether registry polls
// are landing (last successful refresh within SelfUnhealthyAfter), since a
// stale view is the observer analogue of a stale heartbeat.
func (m *Membership) SelfHealthy() bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if !m.started {
		return false
	}
	if m.self == nil {
		return time.Since(m.lastRefresh) < m.cfg.SelfUnhealthyAfter
	}
	return time.Since(m.lastSelfBeat) < m.cfg.SelfUnhealthyAfter
}

// LivenessInfo returns this process's observation of a member: the last
// heartbeat counter seen and how long ago (on our clock) it last changed. ok
// is false if the member has never been observed. The pair (counter,
// staleness) is the evidence a takeover presents to the claim store.
func (m *Membership) LivenessInfo(instanceID string) (counter uint64, staleFor time.Duration, ok bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	obs, exists := m.observations[instanceID]
	if !exists {
		return 0, 0, false
	}
	return obs.record.HeartbeatCounter, time.Since(obs.lastChange), true
}

// LivenessWindow returns the configured window after which an unmoving
// heartbeat means dead.
func (m *Membership) LivenessWindow() time.Duration {
	return m.cfg.LivenessWindow
}

// HeartbeatInterval returns the member's heartbeat cadence.
func (m *Membership) HeartbeatInterval() time.Duration {
	return m.cfg.HeartbeatInterval
}

// PollInterval returns the roster poll cadence.
func (m *Membership) PollInterval() time.Duration {
	return m.cfg.PollInterval
}

// Subscribe registers fn to be called whenever the live set changes (join,
// leave, death, or a draining flip). fn is invoked from the poll loop and must
// not block; do real work on another goroutine.
func (m *Membership) Subscribe(fn func()) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.subscribers = append(m.subscribers, fn)
}

// OnSessionLost registers fn to be called after this member's liveness session
// was interrupted and re-established: its registry record was deleted (GC'd by
// a peer, so every held claim became displaceable) or its heartbeats gapped
// past SelfUnhealthyAfter (so peers may have displaced claims via takeover).
// Local ownership state can no longer be trusted; subscribers should shed it
// and let demand re-acquire. Called from the heartbeat loop's goroutine.
// Never fires on an observer: with no registration there is no session.
func (m *Membership) OnSessionLost(fn func()) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.sessionLostSubs = append(m.sessionLostSubs, fn)
}

func (m *Membership) notifySessionLost() {
	m.mu.RLock()
	subs := m.sessionLostSubs
	m.mu.RUnlock()
	for _, fn := range subs {
		fn()
	}
}

// Refresh re-reads the registry and rebuilds the live set immediately. The
// poll loop calls this on its interval; tests call it to converge
// deterministically.
func (m *Membership) Refresh(ctx context.Context) error {
	records, err := m.store.GetMembers(ctx)
	if err != nil {
		return err
	}

	now := time.Now()

	m.mu.Lock()

	m.lastRefresh = now

	seen := make(map[string]bool, len(records))
	for _, record := range records {
		seen[record.InstanceID] = true
		obs, exists := m.observations[record.InstanceID]
		if !exists {
			m.observations[record.InstanceID] = &observation{
				record:     record,
				lastChange: now,
			}
			continue
		}
		if record.HeartbeatCounter != obs.record.HeartbeatCounter {
			obs.lastChange = now
		}
		obs.record = record
	}
	for instanceID := range m.observations {
		if !seen[instanceID] {
			delete(m.observations, instanceID)
		}
	}

	// Actively garbage-collect corpse records: store-level TTLs are lazy, and
	// a record whose heartbeat sat unchanged far past the liveness window
	// misleads freshly started observers (a first-sighted record is presumed
	// live). Best-effort and racy by design — deletion of a merely wedged
	// member is healed by its next heartbeat re-registering.
	//
	// The observation is deliberately KEPT until the record actually vanishes
	// from the scan (the !seen cleanup above): dropping it before the delete
	// is confirmed would make the next Refresh first-sight the corpse as
	// presumed-live — routed to for a full liveness window out of every
	// MemberGCAfter, indefinitely, under a persistently failing delete.
	// Keeping it also retries the delete every poll until it lands.
	var gc []string
	for instanceID, obs := range m.observations {
		if m.self != nil && instanceID == m.self.InstanceID {
			continue
		}
		if now.Sub(obs.lastChange) >= m.cfg.MemberGCAfter {
			gc = append(gc, instanceID)
		}
	}
	if len(gc) > 0 {
		go func() {
			for _, instanceID := range gc {
				gcCtx, cancel := context.WithTimeout(context.Background(), m.cfg.PollInterval)
				err := m.store.DeleteMember(gcCtx, instanceID)
				cancel()
				if err != nil {
					m.log.With(zap.Error(err)).Warn("Failed to garbage-collect stale member record; will retry",
						zap.String("instance_id", instanceID),
					)
				} else {
					m.log.Info("Garbage-collected stale member record", zap.String("instance_id", instanceID))
				}
			}
		}()
	}

	live := make([]*Member, 0, len(m.observations))
	for _, obs := range m.observations {
		if now.Sub(obs.lastChange) < m.cfg.LivenessWindow {
			live = append(live, obs.record.Member.Clone())
		}
	}
	sort.Slice(live, func(i, j int) bool { return live[i].InstanceID < live[j].InstanceID })
	m.live = live

	signature := liveSignature(live)
	changed := signature != m.liveSignature
	m.liveSignature = signature

	subscribers := m.subscribers
	m.mu.Unlock()

	if changed {
		for _, fn := range subscribers {
			fn()
		}
	}
	return nil
}

func liveSignature(live []*Member) string {
	var b strings.Builder
	for _, m := range live {
		b.WriteString(m.InstanceID)
		b.WriteByte('|')
		b.WriteString(strconv.FormatBool(m.Draining))
		b.WriteByte(';')
	}
	return b.String()
}

func (m *Membership) run(ctx context.Context) {
	defer close(m.done)

	// Observers have no record to beat: leave the heartbeat channel nil so its
	// select arm blocks forever and only the poll loop runs.
	var heartbeatC <-chan time.Time
	if !m.observer() {
		heartbeat := time.NewTicker(m.cfg.HeartbeatInterval)
		defer heartbeat.Stop()
		heartbeatC = heartbeat.C
	}
	poll := time.NewTicker(m.cfg.PollInterval)
	defer poll.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-heartbeatC:
			opCtx, cancel := context.WithTimeout(ctx, m.cfg.HeartbeatInterval)
			counter, err := m.store.Heartbeat(opCtx, m.self.InstanceID)
			if errors.Is(err, ErrMemberNotFound) {
				// Our record was deleted (a peer GC'd it as a corpse): every
				// claim we held became displaceable the moment it vanished.
				// Re-register a fresh session — with the counter resuming
				// strictly above every value this process could ever have
				// written, so peers' stale prior-epoch takeover evidence can
				// never match again — and tell subscribers to shed local
				// ownership state. UnixNano dominates any increment-per-few-
				// seconds counter (and covers heartbeats whose responses were
				// lost after committing), while staying an opaque, changing
				// number to observers.
				m.mu.Lock()
				reborn := max(m.lastCounter+1, uint64(time.Now().UnixNano()))
				m.mu.Unlock()
				err = m.store.PutMember(opCtx, m.Self(), reborn)
				cancel()
				if err != nil {
					m.log.With(zap.Error(err)).Warn("Failed to re-register garbage-collected member record")
					continue
				}
				m.log.Warn("Member record was garbage-collected; re-registered and shedding session state")
				m.mu.Lock()
				m.lastSelfBeat = time.Now()
				m.lastCounter = reborn
				m.sessionLost = false
				m.mu.Unlock()
				m.notifySessionLost()
				continue
			}
			cancel()
			if err != nil {
				// Failed heartbeats are not fatal here: SelfHealthy going
				// false is what forces owned keys onto the fallback path. But
				// the session-gap check must still run — sustained write
				// failure is exactly the condition under which a suspicious
				// peer displaces our claims, and the success-path check below
				// is unreachable until a beat lands again.
				m.log.With(zap.Error(err)).Warn("Failed to heartbeat cluster member record")
				m.mu.Lock()
				gap := time.Since(m.lastSelfBeat)
				lost := gap >= m.cfg.SessionGapThreshold && !m.sessionLost
				if lost {
					m.sessionLost = true
				}
				m.mu.Unlock()
				if lost {
					m.log.Warn("Heartbeat failures reached session gap threshold; shedding session state",
						zap.Duration("gap", gap),
					)
					m.notifySessionLost()
				}
				continue
			}

			m.mu.Lock()
			gap := time.Since(m.lastSelfBeat)
			m.lastSelfBeat = time.Now()
			m.lastCounter = max(m.lastCounter, counter)
			alreadyShed := m.sessionLost
			m.sessionLost = false
			m.mu.Unlock()
			if gap >= m.cfg.SessionGapThreshold && !alreadyShed {
				// The gap reached the suspicion floor: a peer holding a
				// failed-forward report may have displaced our claims while
				// we still looked healthy to ourselves. Shed local ownership
				// so any such dual serving ends now, not at IdleTTL.
				m.log.Warn("Heartbeat gap reached session threshold; shedding session state",
					zap.Duration("gap", gap),
				)
				m.notifySessionLost()
			}
		case <-poll.C:
			opCtx, cancel := context.WithTimeout(ctx, m.cfg.PollInterval)
			if err := m.Refresh(opCtx); err != nil {
				m.log.With(zap.Error(err)).Warn("Failed to refresh cluster membership")
			}
			cancel()
		}
	}
}
