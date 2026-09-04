// Package cluster provides a generic substrate for consistent routing,
// exclusive ownership, and stream-interest tracking of namespaced keys across
// a fleet of servers.
//
// It is built from four decoupled layers:
//
//  1. Membership: each process registers a member record with a fresh instance
//     ID and keeps it alive with a heartbeat counter. Peers poll the registry
//     and judge liveness by observing counter movement against their own local
//     clock (never by comparing wall clocks across machines).
//
//  2. Routing: a pure function over the live member set. Rendezvous hashing
//     (HRW) deterministically elects the preferred owner for a key; overrides
//     and per-namespace placement hooks filter candidacy first.
//
//  3. Ownership: a claim record per (namespace, key), acquired lazily on first
//     demand and validated against the owner's liveness. Routing decides who
//     should own; the claim record is the sole arbiter of who does. Non-owners
//     receive a redirect and forward.
//
//  4. Subscriptions: a non-exclusive interest registry per (namespace, key)
//     topic — which servers currently host live streams for the topic, one
//     row per interested server no matter how many local streams ride it.
//     There is no arbitration and no fence; a row simply stops counting the
//     moment its member stops being live.
//
// Ownership is an accelerator, not an availability gate: consumers must keep a
// store-serialized fallback path so that losing an owner degrades to
// contention, never to unavailability or corruption. Subscription-driven
// delivery is a hint, not a contract: consumers must keep a pull backstop
// (e.g. sequence-log delta sync) so a missed delivery is healed, never lost.
package cluster

import (
	"fmt"
	"maps"
	"time"
)

// Member is a single server process in the cluster. InstanceID is unique per
// process incarnation — a restarted server registers a new ID, so claims held
// by a previous incarnation are never confused with the current one.
type Member struct {
	InstanceID string
	Address    string
	Labels     map[string]string
	Draining   bool
}

// Clone returns a deep copy.
func (m *Member) Clone() *Member {
	labels := make(map[string]string, len(m.Labels))
	maps.Copy(labels, m.Labels)
	return &Member{
		InstanceID: m.InstanceID,
		Address:    m.Address,
		Labels:     labels,
		Draining:   m.Draining,
	}
}

// MemberRecord is a member's registry row, including the heartbeat counter
// whose movement (not value) is the liveness signal.
type MemberRecord struct {
	Member
	HeartbeatCounter uint64
}

// Claim is exclusive ownership of a key within a namespace. Fence increases
// monotonically per key on every change of owner (never on an owner's
// re-acquire), so a displaced owner's writes can be rejected by any store that
// checks it. Fencing is opt-in per namespace: consumers whose data store
// already serializes writes (e.g. via conditional writes) do not need it.
type Claim struct {
	Namespace       string
	Key             []byte
	OwnerInstanceID string
	OwnerAddress    string
	Fence           uint64
}

// Clone returns a deep copy.
func (c *Claim) Clone() *Claim {
	key := make([]byte, len(c.Key))
	copy(key, c.Key)
	return &Claim{
		Namespace:       c.Namespace,
		Key:             key,
		OwnerInstanceID: c.OwnerInstanceID,
		OwnerAddress:    c.OwnerAddress,
		Fence:           c.Fence,
	}
}

// Subscription is one member's registered interest in a (namespace, key)
// topic: "this server hosts at least one live stream for this topic — deliver
// the topic's events here". Unlike a Claim it is non-exclusive (a topic holds
// one row per interested server) and carries no fence: there is nothing to
// serialize. A subscription counts only while its member is live, judged by
// the same heartbeat observation as claims — so steady state needs no row
// refreshes, and a dead member's rows are ignored immediately regardless of
// when they get swept.
type Subscription struct {
	Namespace  string
	Key        []byte
	InstanceID string
	Address    string
}

// Clone returns a deep copy.
func (s *Subscription) Clone() *Subscription {
	key := make([]byte, len(s.Key))
	copy(key, s.Key)
	return &Subscription{
		Namespace:  s.Namespace,
		Key:        key,
		InstanceID: s.InstanceID,
		Address:    s.Address,
	}
}

// TakeoverTarget is the evidence justifying displacement of a claim holder: the
// holder's identity and the heartbeat counter value the caller observed to be
// stale. The store honors the takeover only if, atomically at commit time, the
// holder's registry record is absent or its counter still equals
// HeartbeatCounter — so a holder that was merely slow (and has heartbeated
// since the observation) cannot be displaced.
//
// The evidence is produced by the membership layer and passed through opaquely;
// alternative store implementations may not need it and are free to ignore it.
type TakeoverTarget struct {
	InstanceID       string
	HeartbeatCounter uint64
}

// NotOwnerError is returned by Ownership.Do when this server does not (and
// should not) own the key. A non-nil Redirect identifies the member believed to
// hold or deserve the claim: forward the request there. A nil Redirect means no
// healthy owner is known (e.g. this server's own heartbeats are failing, or the
// claim is mid-handoff): fall back to the store-serialized path or retry.
type NotOwnerError struct {
	Redirect *Member
}

func (e *NotOwnerError) Error() string {
	if e.Redirect == nil {
		return "not owner: no healthy owner known"
	}
	return fmt.Sprintf("not owner: owned by %s at %s", e.Redirect.InstanceID, e.Redirect.Address)
}

// MembershipConfig tunes the membership runtime. Zero values take defaults.
type MembershipConfig struct {
	// HeartbeatInterval is how often the member's own heartbeat counter is
	// advanced. Default 5s.
	HeartbeatInterval time.Duration

	// PollInterval is how often the registry is re-read to refresh the live
	// set. Must be well under LivenessWindow. Default 2s.
	PollInterval time.Duration

	// LivenessWindow is how long a peer's heartbeat counter may sit unchanged
	// (in this process's own observations) before the peer is considered dead.
	// Default 15s.
	LivenessWindow time.Duration

	// SelfUnhealthyAfter is how long this process's own heartbeat writes may
	// fail before it must assume the rest of the cluster considers it dead and
	// stop serving owned keys. Default: LivenessWindow.
	SelfUnhealthyAfter time.Duration

	// SessionGapThreshold is the gap between successful heartbeat writes at
	// which the member treats its own liveness session as interrupted (firing
	// OnSessionLost, which sheds local ownership). Aligned by default with the
	// suspicion floor (HeartbeatInterval + PollInterval, capped at
	// SelfUnhealthyAfter): a heartbeat delayed past that floor is exactly the
	// window in which a peer holding a failed-forward report may have
	// displaced this member's claims while it still looked healthy to itself
	// — shedding then bounds any such dual ownership at the gap's length
	// instead of IdleTTL. Default: min(SelfUnhealthyAfter, HeartbeatInterval
	// + PollInterval).
	SessionGapThreshold time.Duration

	// MemberGCAfter is how long a member's heartbeat may sit unchanged before
	// any observer actively deletes its registry record. Store-level TTLs are
	// lazy (DynamoDB's can lag days), and a lingering corpse record misleads
	// freshly started observers — a first-sighted record is presumed live for
	// a full LivenessWindow. Must be much larger than LivenessWindow; a
	// wrongly deleted (merely wedged) member re-registers on its next
	// heartbeat. Default: 10 × LivenessWindow.
	MemberGCAfter time.Duration
}

func (c MembershipConfig) withDefaults() MembershipConfig {
	if c.HeartbeatInterval <= 0 {
		c.HeartbeatInterval = 5 * time.Second
	}
	if c.PollInterval <= 0 {
		c.PollInterval = 2 * time.Second
	}
	if c.LivenessWindow <= 0 {
		c.LivenessWindow = 15 * time.Second
	}
	if c.SelfUnhealthyAfter <= 0 {
		c.SelfUnhealthyAfter = c.LivenessWindow
	}
	if c.SessionGapThreshold <= 0 {
		c.SessionGapThreshold = min(c.SelfUnhealthyAfter, c.HeartbeatInterval+c.PollInterval)
	}
	if c.MemberGCAfter < 2*c.LivenessWindow {
		c.MemberGCAfter = 10 * c.LivenessWindow
	}
	return c
}

// OwnershipConfig tunes the ownership runtime. Zero values take defaults.
type OwnershipConfig struct {
	// IdleTTL is how long an owned key may go unused before its claim is
	// released. Deliberately long: claims are sticky, and only liveness is
	// fresh — a short TTL converts the coordination plane's cheapest property
	// (claims touched only at activity boundaries) into churn. Default 15m.
	IdleTTL time.Duration

	// ReapInterval is how often idle claims are scanned for release.
	// Default 1m.
	ReapInterval time.Duration

	// DrainDeadline bounds how long a key's in-flight work may delay its
	// release during handoff or shutdown; past it the release proceeds
	// regardless (the consumer's store-serialized fallback makes a forced
	// release safe). Default 5s.
	DrainDeadline time.Duration

	// SuspicionWindow is the minimum heartbeat staleness at which a holder
	// reported unreachable (via NoteUnreachable) becomes takeover-eligible
	// ahead of the full LivenessWindow. Default 5s; floored at construction
	// to the membership's HeartbeatInterval + PollInterval — anything lower
	// leaves zero tolerance for ordinary heartbeat jitter, displacing owners
	// whose beat merely landed late.
	SuspicionWindow time.Duration

	// RedirectCacheTTL bounds how long a non-owner reuses a resolved redirect
	// for a key without re-reading the claim. Keeps the forward path from
	// paying a store read per request on hot keys; a stale redirect
	// self-corrects (the target answers NotOwner, or a failed forward
	// invalidates via NoteUnreachable). Default 1s.
	RedirectCacheTTL time.Duration
}

func (c OwnershipConfig) withDefaults() OwnershipConfig {
	if c.IdleTTL <= 0 {
		c.IdleTTL = 15 * time.Minute
	}
	if c.ReapInterval <= 0 {
		c.ReapInterval = time.Minute
	}
	if c.DrainDeadline <= 0 {
		c.DrainDeadline = 5 * time.Second
	}
	if c.SuspicionWindow <= 0 {
		c.SuspicionWindow = 5 * time.Second
	}
	if c.RedirectCacheTTL <= 0 {
		c.RedirectCacheTTL = time.Second
	}
	return c
}

// SubscriptionsConfig tunes the subscriptions runtime. Zero values take
// defaults.
type SubscriptionsConfig struct {
	// CacheTTL bounds how long a resolved subscriber set is reused without
	// re-reading the registry, so the publish path pays at most one registry
	// read per topic per TTL no matter the event rate. Empty results are
	// cached too — publishes toward offline users must not read per event.
	// The staleness is covered by the consumer's pull backstop: a just-opened
	// stream misses at most one TTL of events before its delta sync heals it.
	// Default 250ms — hot-topic read load at 4 reads/s per publisher is still
	// orders of magnitude under DynamoDB's per-partition ceiling, so the
	// freshness is nearly free; shrinking it much further buys latency the
	// delta sync already covers.
	CacheTTL time.Duration

	// RowGCAfter is how long a row's member may sit continuously outside this
	// observer's live view before the row is treated as a crashed instance's
	// leftover and deleted at resolution time. Graceful drains remove their
	// own rows; this sweeps what crashes leave behind on topics still being
	// published to. Floored at construction well above the liveness window so
	// a merely-slow member is never swept — and a wrongly swept row is
	// re-asserted by its owner on its next liveness session recovery. Default:
	// 10 × the membership's LivenessWindow, resolved at construction.
	RowGCAfter time.Duration
}

func (c SubscriptionsConfig) withDefaults() SubscriptionsConfig {
	if c.CacheTTL <= 0 {
		c.CacheTTL = 250 * time.Millisecond
	}
	// RowGCAfter needs the membership's liveness window; NewSubscriptions
	// resolves it.
	return c
}
