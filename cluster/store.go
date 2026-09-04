package cluster

import (
	"context"
	"errors"
)

var (
	// ErrMemberNotFound indicates the member record does not exist (e.g. a
	// heartbeat after the record was deleted or garbage-collected).
	ErrMemberNotFound = errors.New("cluster member not found")

	// ErrClaimNotFound indicates no active claim exists for the key.
	ErrClaimNotFound = errors.New("claim not found")

	// ErrClaimHeld indicates the claim is actively held by another member (or
	// a takeover's evidence no longer held at commit time). AcquireClaim
	// returns the current holder alongside this error so the caller can
	// redirect without a second read.
	ErrClaimHeld = errors.New("claim held by another member")

	// ErrNoMembers indicates routing found no eligible members for a key.
	ErrNoMembers = errors.New("no eligible cluster members")
)

// RegistryStore persists cluster membership. Liveness is never derived inside
// the store: records carry a heartbeat counter, and observers judge liveness by
// watching the counter move against their own clocks. Store-level TTLs, where
// an implementation has them, are garbage collection only — never correctness.
type RegistryStore interface {
	// PutMember creates (or replaces) the member's record with the given
	// heartbeat counter. Instance IDs are unique per process incarnation, so a
	// replace only ever overwrites this process's own registration.
	//
	// The counter must never repeat within an instance ID's lifetime: takeover
	// evidence is "counter X was observed stale", checked as equality at
	// commit time, so a re-registration after record deletion must resume
	// strictly above the last value it wrote — resetting would let a peer's
	// stale prior-epoch evidence displace a live owner.
	PutMember(ctx context.Context, member *Member, heartbeatCounter uint64) error

	// Heartbeat advances the member's heartbeat counter and returns the new
	// value. Returns ErrMemberNotFound if the record no longer exists.
	Heartbeat(ctx context.Context, instanceID string) (uint64, error)

	// SetDraining updates the member's draining flag. A draining member is
	// excluded from routing candidacy but remains live, so claims it still
	// holds stay valid while it hands them off. Returns ErrMemberNotFound if
	// the record no longer exists.
	SetDraining(ctx context.Context, instanceID string, draining bool) error

	// DeleteMember removes the member's record. Idempotent.
	DeleteMember(ctx context.Context, instanceID string) error

	// GetMembers returns all registered member records.
	GetMembers(ctx context.Context) ([]*MemberRecord, error)
}

// ClaimStore persists ownership claims. It is the sole arbiter of ownership:
// routing only makes contention rare.
type ClaimStore interface {
	// AcquireClaim atomically acquires (namespace, key) for self. It succeeds
	// if the claim is absent, released, or already held by self (an owner's
	// re-acquire returns the existing claim with the fence unchanged).
	//
	// If takeover is non-nil, the claim may additionally be displaced from
	// takeover.InstanceID, but only if — atomically at commit time — that
	// member's registry record is absent or its heartbeat counter still equals
	// takeover.HeartbeatCounter. The fence increments on every change of
	// owner, including a fresh acquire after a release.
	//
	// When the claim is (still) held by another member, the current holder is
	// returned alongside ErrClaimHeld.
	AcquireClaim(ctx context.Context, namespace string, key []byte, self *Member, takeover *TakeoverTarget) (*Claim, error)

	// GetClaim returns the active claim for (namespace, key), or
	// ErrClaimNotFound if the key is unclaimed or released.
	GetClaim(ctx context.Context, namespace string, key []byte) (*Claim, error)

	// ReleaseClaim releases the claim if it is currently held by instanceID; a
	// release by anyone else is a no-op. The fence survives release, so a
	// later acquire of the same key still increments monotonically.
	ReleaseClaim(ctx context.Context, namespace string, key []byte, instanceID string) error
}

// SubscriptionStore persists stream-interest registrations: one row per
// (namespace, key, instance) meaning "this member hosts live streams for this
// topic". Rows are non-exclusive and unfenced — there is nothing to arbitrate.
//
// A row counts only while its member is live (the same heartbeat-observation
// rule as claims), so implementations must not attach row-level TTLs: nothing
// refreshes a held row (steady state is deliberately write-free), and a row
// expiring under a live subscriber would silently stop delivery to it.
// Cleanup is explicit instead — drains delete their own rows, and observers
// sweep crashed instances' rows at resolution time.
type SubscriptionStore interface {
	// PutSubscription registers (or reasserts) the member's interest row for
	// the topic. Idempotent upsert.
	PutSubscription(ctx context.Context, namespace string, key []byte, member *Member) error

	// DeleteSubscription removes the member's interest row for the topic.
	// Idempotent.
	DeleteSubscription(ctx context.Context, namespace string, key []byte, instanceID string) error

	// GetSubscribers returns every interest row for the topic, dead members'
	// rows included — liveness filtering is the caller's job.
	GetSubscribers(ctx context.Context, namespace string, key []byte) ([]*Subscription, error)
}

// Store combines the registry, claim, and subscription stores. Implementations
// back all three from the same technology so the takeover condition can span
// registry and claims atomically.
type Store interface {
	RegistryStore
	ClaimStore
	SubscriptionStore
}
