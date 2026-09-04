package cluster

import (
	"context"
	"sync"
)

// Placement filters or reorders the candidate members eligible to own a key in
// a namespace, before hashing. It is the seam for role- and region-aware
// routing: e.g. restricting a namespace to members labeled with an owner role,
// or to the key's home region. Returning the input unchanged means "any live
// member". Placement must be deterministic given the same inputs — every
// server runs it independently and must reach the same answer.
type Placement func(ctx context.Context, key []byte, candidates []*Member) ([]*Member, error)

// Overrides pins individual keys to a class of members, consulted before
// hashing. It is the operational escape hatch for moving a hot key onto
// dedicated capacity (and the seam where a load-aware assigner could plug in
// later) — normally empty.
//
// A pin is a label selector, not an instance ID: instance IDs die with each
// process incarnation, so an instance-level pin would silently expire on the
// pinned server's first deploy. A selector names deploy-stable intent (e.g.
// {"pool": "xl"}): candidates matching every selector entry form the pool, and
// hashing elects deterministically within it — so a multi-member pool needs no
// further configuration and a deploy inside the pool hands off to another pool
// member. The reserved key "instance" matches a member's InstanceID, for the
// rare pin that truly means one incarnation. A selector matching no eligible
// candidate is ignored, failing open to hashing over the full candidate set.
type Overrides interface {
	Get(ctx context.Context, namespace string, key []byte) (selector map[string]string, ok bool, err error)
}

// OverrideInstanceLabel is the reserved selector key matching Member.InstanceID.
const OverrideInstanceLabel = "instance"

type noopOverrides struct{}

func (noopOverrides) Get(context.Context, string, []byte) (map[string]string, bool, error) {
	return nil, false, nil
}

// matchesSelector reports whether the member satisfies every selector entry.
func matchesSelector(m *Member, selector map[string]string) bool {
	for k, v := range selector {
		if k == OverrideInstanceLabel {
			if m.InstanceID != v {
				return false
			}
			continue
		}
		if m.Labels[k] != v {
			return false
		}
	}
	return true
}

// Router deterministically elects the member that should own a key: live,
// non-draining members are filtered by the namespace's Placement, overrides
// are consulted, and rendezvous hashing decides among what remains. It is a
// pure function over the membership snapshot — no I/O on the lookup path — and
// only says who *should* own a key; the claim record decides who does.
type Router struct {
	membership *Membership
	overrides  Overrides

	mu         sync.RWMutex
	placements map[string]Placement
}

// NewRouter creates a router over the membership's live set. A nil overrides
// installs the empty implementation.
func NewRouter(membership *Membership, overrides Overrides) *Router {
	if overrides == nil {
		overrides = noopOverrides{}
	}
	return &Router{
		membership: membership,
		overrides:  overrides,
		placements: make(map[string]Placement),
	}
}

// RegisterPlacement installs the namespace's placement hook.
func (r *Router) RegisterPlacement(namespace string, placement Placement) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.placements[namespace] = placement
}

// Owner returns the member that should own the key, or ErrNoMembers if no
// eligible candidate exists.
func (r *Router) Owner(ctx context.Context, namespace string, key []byte) (*Member, error) {
	return r.OwnerExcluding(ctx, namespace, key, nil)
}

// OwnerExcluding is Owner with additional members removed from candidacy —
// used to route around a member the caller has evidence is dead or
// unreachable, without waiting for the live set to converge.
func (r *Router) OwnerExcluding(ctx context.Context, namespace string, key []byte, exclude map[string]bool) (*Member, error) {
	live := r.membership.Live()

	candidates := make([]*Member, 0, len(live))
	for _, m := range live {
		if !m.Draining && !exclude[m.InstanceID] {
			candidates = append(candidates, m)
		}
	}

	r.mu.RLock()
	placement := r.placements[namespace]
	r.mu.RUnlock()

	if placement != nil {
		var err error
		candidates, err = placement(ctx, key, candidates)
		if err != nil {
			return nil, err
		}
	}

	if len(candidates) == 0 {
		return nil, ErrNoMembers
	}

	if selector, ok, err := r.overrides.Get(ctx, namespace, key); err != nil {
		return nil, err
	} else if ok {
		var pool []*Member
		for _, m := range candidates {
			if matchesSelector(m, selector) {
				pool = append(pool, m)
			}
		}
		if len(pool) > 0 {
			return hrwOwner(pool, namespace, key), nil
		}
		// The selector matches no eligible candidate (pool drained away or
		// mislabeled): fall through to hashing over everyone rather than
		// routing into a void.
	}

	return hrwOwner(candidates, namespace, key), nil
}
