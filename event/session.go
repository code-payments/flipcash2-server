package event

import (
	"sync"

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
	// key, kept after the key is detached (see above).
	versions map[string]uint64
}

func newStreamSession(id string, local localStream, userKey string, registry *streamRegistry) *streamSession {
	return &streamSession{
		id:       id,
		local:    local,
		userKey:  userKey,
		registry: registry,
		topics:   make(map[string]*cluster.SubscriptionHandle),
		versions: make(map[string]uint64),
	}
}

// topicSeed is one of the user's membership records as read at open: the
// chat's key, the version of the user's last transition there, and whether
// that transition left them joined. Every record seeds a version; only a
// joined one puts the stream under its key.
type topicSeed struct {
	key     string
	version uint64
	joined  bool
}

// open registers the stream in the registry under its user key and the key of
// every joined seed, with no cluster handles yet (see setHandles), and seeds
// every seed's version — joined or departed — so that a transition the record
// already reflects is stale. It reports false, having registered nothing, if
// the registry is draining.
func (s *streamSession) open(seeds []topicSeed) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	keys := make([]string, 0, 1+len(seeds))
	keys = append(keys, s.userKey)
	for _, seed := range seeds {
		if seed.joined {
			keys = append(keys, seed.key)
		}
	}
	if !s.registry.add(s.id, s.local, keys) {
		return false
	}
	for _, seed := range seeds {
		s.versions[seed.key] = seed.version
		if seed.joined {
			s.topics[seed.key] = nil
		}
	}
	return true
}

// setHandles takes ownership of the cluster handles an open landed: the user
// topic's, and one per joined chat key, in the order the caller registered
// them.
func (s *streamSession) setHandles(userHandle *cluster.SubscriptionHandle, chatKeys []string, chatHandles []*cluster.SubscriptionHandle) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.userHandle = userHandle
	for i, key := range chatKeys {
		s.topics[key] = chatHandles[i]
	}
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
	return true
}

// rollback undoes an attach whose cluster registration failed: the stream
// leaves the key, and the transition's version is forgotten so a later copy
// of the same join can try again. Both are gated on the version still being
// the one the attach recorded: if a newer transition has since moved the key
// — a leave, or a leave and rejoin whose own registration is in flight — its
// state stands, and the failed attach has nothing left to undo. A key already
// released is a no-op.
func (s *streamSession) rollback(chatKey string, version uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if last, seen := s.versions[chatKey]; !seen || last != version {
		return
	}
	if _, attached := s.topics[chatKey]; attached {
		delete(s.topics, chatKey)
		s.registry.remove(s.id, []string{chatKey})
	}
	delete(s.versions, chatKey)
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
// transition can find every local stream of the user it names.
type streamSessions struct {
	mu     sync.RWMutex
	byUser map[string]map[string]*streamSession
}

func newStreamSessions() *streamSessions {
	return &streamSessions{byUser: make(map[string]map[string]*streamSession)}
}

func (x *streamSessions) add(s *streamSession) {
	x.mu.Lock()
	defer x.mu.Unlock()

	byID, ok := x.byUser[s.userKey]
	if !ok {
		byID = make(map[string]*streamSession)
		x.byUser[s.userKey] = byID
	}
	byID[s.id] = s
}

func (x *streamSessions) remove(s *streamSession) {
	x.mu.Lock()
	defer x.mu.Unlock()

	if byID, ok := x.byUser[s.userKey]; ok {
		delete(byID, s.id)
		if len(byID) == 0 {
			delete(x.byUser, s.userKey)
		}
	}
}

// forUser returns a snapshot of the user's open sessions.
func (x *streamSessions) forUser(userKey string) []*streamSession {
	x.mu.RLock()
	defer x.mu.RUnlock()

	byID := x.byUser[userKey]
	sessions := make([]*streamSession, 0, len(byID))
	for _, s := range byID {
		sessions = append(sessions, s)
	}
	return sessions
}
