package event

import (
	"sort"
	"sync"
	"time"

	"github.com/code-payments/flipcash2-server/cluster"
)

// streamSession is one open stream's registration, as it changes over the
// stream's life. The registry (streamRegistry) fans a topic out to the streams
// under its key and holds nothing per stream; the session is the per-stream
// record of which keys that is — the user's own, plus one per group chat the
// user is a member of — and of the cluster handle registering this server's
// interest in each. Membership moves while a stream is open (see
// Server.followMembership), so the set is mutable: a group joined mid-stream
// is attached, one left is detached, and whatever remains is released as one
// when the stream ends.
//
// Every change goes through mu, and the registry is updated under it, so the
// registry and the session never disagree about which keys the stream is
// under: a key is registered exactly while the session holds it. closed
// latches once teardown has begun, so an attach racing the end of the stream
// is refused rather than landing on a stream that is already gone.
//
// Transitions are applied as state, not as a sequence: each names the roster
// version it produced, and a session applies one to a chat only if it is newer
// than the last it applied there — the same rule a client follows. Arrival
// order is not delivery order (each publish rides its own goroutine, and two
// transitions may cross the forwarding outboxes independently), so without
// the gate a leave and a rejoin delivered the other way round would take the
// stream off a topic its user is on. The versions are kept across detaches,
// so a stale leave cannot undo a newer rejoin either. They are seeded at open
// from the membership records themselves — departed ones included, which is
// what keeps a delayed copy of a join the user has since undone from
// reattaching them — since each carries the version of the user's last
// transition on that group (see chat.GroupMembership). So a transition the
// open snapshot already reflects is stale from the first event, with no clock
// involved.
type streamSession struct {
	id       string
	local    localStream
	userKey  string
	registry *streamRegistry

	mu     sync.Mutex
	closed bool

	// userHandle registers the user's own topic; it lives as long as the
	// stream. topics maps each chat key the stream is under to the handle
	// registering that topic — nil while a registration is still in flight
	// (see attach), which the teardown skips and the registrant releases.
	userHandle *cluster.SubscriptionHandle
	topics     map[string]*cluster.SubscriptionHandle

	// versions is the roster version of the last transition applied per chat
	// key, kept after the key is detached (see above). displaced is, per chat
	// key whose attach has a registration in flight, the watermark that
	// attach overwrote, for rollback to restore should the registration fail,
	// and the version of the attach itself, which is what names the
	// registrant entitled to roll it back; it is dropped once the key's fate
	// is settled (see setHandle, detach).
	versions  map[string]uint64
	displaced map[string]watermark
}

// watermark is a chat key's version gate as it stood before an attach — the
// version last applied there, or none — and the version of the attach that
// displaced it, whose registration is in flight.
type watermark struct {
	version    uint64
	seen       bool
	attachedAt uint64
}

func newStreamSession(id string, local localStream, userKey string, registry *streamRegistry) *streamSession {
	return &streamSession{
		id:        id,
		local:     local,
		userKey:   userKey,
		registry:  registry,
		topics:    make(map[string]*cluster.SubscriptionHandle),
		versions:  make(map[string]uint64),
		displaced: make(map[string]watermark),
	}
}

// topicSeed is one of the user's membership records as read at open: the
// chat's key, the version of the user's last transition there, and whether
// that transition left them joined. Every record seeds a version; only a
// joined one puts the stream under its key (see seed).
type topicSeed struct {
	key     string
	version uint64
	joined  bool
}

// open registers the stream in the registry under its user key, with no
// cluster handle yet (see setHandles) and no chat keys (see seed). It reports
// false, having registered nothing, if the registry is draining.
func (s *streamSession) open() bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.registry.add(s.id, s.local, []string{s.userKey})
}

// seed merges the membership records read at open into the session, each as
// a transition at its record's version through the same gate live
// transitions pass (see admitLocked): a joined record puts the stream under
// its key, a departed one takes it out, and either records its version —
// which is what makes a transition the record already reflects stale, and a
// record that a transition applied meanwhile has already superseded stale in
// turn. It returns the keys it newly attached, for the caller to register
// with the cluster as one batch (see setHandles), and the handles the
// detaches freed, for the caller to release. A draining registry attaches
// nothing; a closed session applies nothing.
func (s *streamSession) seed(seeds []topicSeed) (attached []string, released []*cluster.SubscriptionHandle) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return nil, nil
	}

	var removed []string
	for _, seed := range seeds {
		if !s.admitLocked(seed.key, seed.version) {
			continue
		}
		h, held := s.topics[seed.key]
		switch {
		case seed.joined && !held:
			attached = append(attached, seed.key)
		case !seed.joined && held:
			delete(s.topics, seed.key)
			delete(s.displaced, seed.key)
			removed = append(removed, seed.key)
			if h != nil {
				released = append(released, h)
			}
		}
	}
	if len(removed) > 0 {
		s.registry.remove(s.id, removed)
	}
	if len(attached) > 0 {
		if !s.registry.add(s.id, s.local, attached) {
			return nil, released
		}
		for _, key := range attached {
			s.topics[key] = nil
		}
	}
	return attached, released
}

// setHandles takes ownership of the cluster handles an open landed: the user
// topic's, and one per chat key the seed attached, in the order the caller
// registered them. A chat handle is refused on the same terms as setHandle —
// the key was detached meanwhile, or a transition's own registration landed
// on it first — and, along with every handle if the session has closed, is
// returned for the caller to release.
func (s *streamSession) setHandles(userHandle *cluster.SubscriptionHandle, chatKeys []string, chatHandles []*cluster.SubscriptionHandle) (refused []*cluster.SubscriptionHandle) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return append([]*cluster.SubscriptionHandle{userHandle}, chatHandles...)
	}

	s.userHandle = userHandle
	for i, key := range chatKeys {
		if existing, held := s.topics[key]; !held || existing != nil {
			refused = append(refused, chatHandles[i])
			continue
		}
		s.topics[key] = chatHandles[i]
	}
	return refused
}

// admitLocked applies the version gate to a transition on chatKey, recording
// its version when it passes. Called with mu held.
func (s *streamSession) admitLocked(chatKey string, version uint64) bool {
	if last, seen := s.versions[chatKey]; seen && version <= last {
		return false
	}
	s.versions[chatKey] = version
	return true
}

// attach applies a join at the given roster version: it puts the stream under
// the chat key, registering it locally before the cluster registration that
// follows (see setHandle) — the same order a stream open uses, so a publish
// that resolves the row always finds the stream. It reports false, having
// done nothing, if the transition is stale (see admitLocked), the stream is
// already under the key, is closed, or the registry is draining.
func (s *streamSession) attach(chatKey string, version uint64) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return false
	}
	last, seen := s.versions[chatKey]
	if !s.admitLocked(chatKey, version) {
		return false
	}
	if _, dup := s.topics[chatKey]; dup {
		return false
	}
	if !s.registry.add(s.id, s.local, []string{chatKey}) {
		return false
	}
	s.topics[chatKey] = nil
	s.displaced[chatKey] = watermark{version: last, seen: seen, attachedAt: version}
	return true
}

// setHandle completes an attach with the cluster handle that registered the
// topic. It reports false — leaving the handle with the caller to release —
// if the stream has since closed or left the key, or if the key already holds
// a handle: the stream left and rejoined while this registration was in
// flight, and the rejoin's own registration completed first. A handle is a
// refcount on the topic, not an identity, so whichever lands first is kept and
// the other released; taking both would strand one, and the topic's row with it.
func (s *streamSession) setHandle(chatKey string, h *cluster.SubscriptionHandle) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return false
	}
	if existing, attached := s.topics[chatKey]; !attached || existing != nil {
		return false
	}
	s.topics[chatKey] = h
	delete(s.displaced, chatKey)
	return true
}

// rollback undoes an attach whose cluster registration failed: the stream
// leaves the key, and the version gate goes back to what the attach
// displaced, so a later copy of the same join — or the reconcile — can try
// again while whatever was stale before it stays stale. Both are gated on the
// key's registration in flight still being this attach's (see watermark): if
// the key has since been settled — detached by a leave, or landed by an
// earlier registrant for the same key (see setHandle), so the stream is under
// the key and registered, which is the join's intent — there is nothing left
// to undo, and undoing would strand a handle or unseat a newer transition's
// state. A key already released is a no-op.
//
// The gate is the attach, not the version last applied: a newer join refused
// as a duplicate while this registration was in flight (see attach) records
// its version but shares this attach's registration, so this registration's
// failure is still the key's failure. Restoring the displaced gate then lets
// the reconcile re-attach at the newer version; gating on the version instead
// would leave the key held with no handle and no way to repair it.
func (s *streamSession) rollback(chatKey string, version uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()

	prior, inflight := s.displaced[chatKey]
	if !inflight || prior.attachedAt != version {
		return
	}
	delete(s.displaced, chatKey)
	if h, attached := s.topics[chatKey]; attached {
		if h != nil {
			return
		}
		delete(s.topics, chatKey)
		s.registry.remove(s.id, []string{chatKey})
	}
	if prior.seen {
		s.versions[chatKey] = prior.version
	} else {
		delete(s.versions, chatKey)
	}
}

// detach applies a departure at the given roster version: it takes the
// stream out from under the chat key and returns the handle that registered
// the topic for the caller to release — nil if the transition is stale (see
// admitLocked), the stream was not under the key, or its registration was
// still in flight (its registrant finds the key gone at setHandle and
// releases it).
func (s *streamSession) detach(chatKey string, version uint64) *cluster.SubscriptionHandle {
	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.admitLocked(chatKey, version) {
		return nil
	}
	h, attached := s.topics[chatKey]
	if !attached {
		return nil
	}
	delete(s.topics, chatKey)
	delete(s.displaced, chatKey)
	s.registry.remove(s.id, []string{chatKey})
	return h
}

// close ends the session: the stream leaves every key in the registry, later
// attaches are refused, and every handle it holds is returned for release.
func (s *streamSession) close() []*cluster.SubscriptionHandle {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.closed = true

	keys := make([]string, 0, 1+len(s.topics))
	keys = append(keys, s.userKey)
	handles := make([]*cluster.SubscriptionHandle, 0, 1+len(s.topics))
	if s.userHandle != nil {
		handles = append(handles, s.userHandle)
	}
	for key, h := range s.topics {
		keys = append(keys, key)
		if h != nil {
			handles = append(handles, h)
		}
	}
	s.registry.remove(s.id, keys)
	s.topics = nil
	return handles
}

// streamSessions indexes the open sessions by user key, so a membership
// transition can find every local stream of the user it names, and keeps
// per user the time the membership reconcile schedules on (see
// Server.reconcileMembership): when their memberships were last re-read.
type streamSessions struct {
	mu     sync.RWMutex
	byUser map[string]*userSessions

	// now stamps a user's first open; tests substitute a clock they control.
	now func() time.Time
}

// userSessions is one user's open sessions on this server.
type userSessions struct {
	byID map[string]*streamSession

	// reconciledAt is when the read behind the user's last completed
	// reconcile began — the read's start, since that is when its view of the
	// store was current — or, until one completes, when their first session
	// opened here, whose own read of the memberships stands in for it.
	reconciledAt time.Time
}

func newStreamSessions() *streamSessions {
	return &streamSessions{byUser: make(map[string]*userSessions), now: time.Now}
}

func (x *streamSessions) add(s *streamSession) {
	x.mu.Lock()
	defer x.mu.Unlock()

	u, ok := x.byUser[s.userKey]
	if !ok {
		u = &userSessions{byID: make(map[string]*streamSession), reconciledAt: x.now()}
		x.byUser[s.userKey] = u
	}
	u.byID[s.id] = s
}

func (x *streamSessions) remove(s *streamSession) {
	x.mu.Lock()
	defer x.mu.Unlock()

	if u, ok := x.byUser[s.userKey]; ok {
		delete(u.byID, s.id)
		if len(u.byID) == 0 {
			delete(x.byUser, s.userKey)
		}
	}
}

// forUser returns a snapshot of the user's open sessions.
func (x *streamSessions) forUser(userKey string) []*streamSession {
	x.mu.RLock()
	defer x.mu.RUnlock()

	u := x.byUser[userKey]
	if u == nil {
		return nil
	}
	sessions := make([]*streamSession, 0, len(u.byID))
	for _, s := range u.byID {
		sessions = append(sessions, s)
	}
	return sessions
}

// count is the number of users with a session open.
func (x *streamSessions) count() int {
	x.mu.RLock()
	defer x.mu.RUnlock()

	return len(x.byUser)
}

// markReconciled records that a reconcile of the user's memberships, whose
// read began at startedAt, has completed. A user with no sessions left is
// not resurrected.
func (x *streamSessions) markReconciled(userKey string, startedAt time.Time) {
	x.mu.Lock()
	defer x.mu.Unlock()

	if u, ok := x.byUser[userKey]; ok {
		u.reconciledAt = startedAt
	}
}

// due picks the users whose memberships the sweep should re-read now: up to
// quota of those not reconciled within interval, least recently reconciled
// first. The quota paces the re-reads across the interval instead of
// bunching them at its boundary; a fleet of streams that opened together
// spreads out over its first cycle and stays spread.
func (x *streamSessions) due(now time.Time, interval time.Duration, quota int) []string {
	x.mu.RLock()
	defer x.mu.RUnlock()

	var candidates []sweepCandidate
	for userKey, u := range x.byUser {
		if !now.Before(u.reconciledAt.Add(interval)) {
			candidates = append(candidates, sweepCandidate{userKey: userKey, reconciledAt: u.reconciledAt})
		}
	}

	if quota < len(candidates) {
		sort.Slice(candidates, func(i, j int) bool { return candidates[i].reconciledAt.Before(candidates[j].reconciledAt) })
		candidates = candidates[:quota]
	}
	userKeys := make([]string, 0, len(candidates))
	for _, c := range candidates {
		userKeys = append(userKeys, c.userKey)
	}
	return userKeys
}

type sweepCandidate struct {
	userKey      string
	reconciledAt time.Time
}
