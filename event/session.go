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
// stream follows — and of the cluster handle registering this server's
// interest in each. Membership and archives move while a stream is open (see
// Server.followMembership and Server.followArchive), so the set is mutable: a
// group joined mid-stream is attached, one left is detached, and whatever
// remains is released as one when the stream ends.
//
// Every change goes through mu, and the registry is updated under it, so the
// registry and the session never disagree about which keys the stream is
// under: a key is registered exactly while the session holds it. closed
// latches once teardown has begun, so an attach racing the end of the stream
// is refused rather than landing on a stream that is already gone.
//
// The session follows a group (holds its key) exactly while two gates, each
// kept per chat key and each ordered by its own version, say it should:
//
//   - The roster gate says whether the user is a member: the roster version
//     of the last membership transition applied there, and whether it left
//     them joined.
//   - The archive gate says whether the user has archived the group: the
//     version of the last ViewerState of theirs applied there, and whether it
//     held an archive.
//
// A key is held while the roster gate has the user joined and the group is
// either not archived or followed with the archive: while the stream follows
// the user's archive (see Server.followArchiveRequest), or for the linger
// after it stops (lingering), so a client stepping out of its archive and
// back in does not pay for a re-read. Every path that attaches — a join, the
// open's seed, the reconcile, an unarchive, following turning on — applies
// its own gate and then the rule over both, under mu, so a join or a
// reconcile racing an archive cannot attach an archived group while the
// stream is not following, whichever lands first: the archive takes the key
// off if it was attached, and the attach sees the archive if it was not.
//
// The two gates are kept apart because the versions are unrelated (a roster
// version is the group's, a ViewerState version the user's own on it), and
// because a key may move with neither changing: an unarchive puts the stream
// back under a key at the roster version it was taken off at, and following
// turning on puts it under every archived key at theirs. So an attach applies
// the roster gate, and then, newer or not, converges the key on what both
// gates and the following say: a stale copy of a join the gates already
// reflect attaches only a key the rule wants held and the session does not
// hold.
//
// Roster transitions are applied as state, not as a sequence: each names the
// roster version it produced, and a session applies one to a chat only if it
// is newer than the last it applied there — the same rule a client follows.
// Arrival order is not delivery order (each publish rides its own goroutine,
// and two transitions may cross the forwarding outboxes independently), so
// without the gate a leave and a rejoin delivered the other way round would
// take the stream off a topic its user is on. The versions are kept across
// detaches, so a stale leave cannot undo a newer rejoin either. They are
// seeded at open from the membership records themselves — departed ones
// included, which is what keeps a delayed copy of a join the user has since
// undone from reattaching them — since each carries the version of the
// user's last transition on that group (see chat.GroupMembership). So a
// transition the open snapshot already reflects is stale from the first
// event, with no clock involved.
//
// The archive gate is applied the same way, by the ViewerState version a
// MetadataUpdate.ViewerStateChanged carries, so a stale one moves nothing.
// A departure the roster gate admits ends the archive with the membership,
// as the store does in the departure's write: the key is marked not archived
// at the version it had, so a delayed copy of the archive the departure
// ended stays stale, and a return starts unarchived. A strongly consistent
// read of the user's archives (chat.Store.GetArchivedChats, see
// applyArchives) seeds the gate when following turns on and on every
// reconcile; the open does not read it (see Server.streamUserEvents).
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

	// roster is the roster gate: per chat key, the last transition applied,
	// kept after the key is detached (see above). displaced is, per chat key
	// whose attach has a registration in flight, the gate that attach
	// overwrote, for rollback to restore should the registration fail, and
	// the version of the attach itself, which is what names the registrant
	// entitled to roll it back; it is dropped once the key's fate is settled
	// (see setHandle, detach).
	roster    map[string]rosterMark
	displaced map[string]watermark

	// archives is the archive gate, per chat key the user's last applied
	// ViewerState there. following is whether the stream follows the user's
	// archive; lingering, whether it stopped following and is keeping the
	// archived keys it holds until the linger numbered lingerGen ends (see
	// stopFollowing, endLinger).
	archives  map[string]archiveMark
	following bool
	lingering bool
	lingerGen uint64
}

// rosterMark is a chat key's roster gate: the roster version of the last
// membership transition applied there, and whether it left the user joined.
type rosterMark struct {
	version uint64
	joined  bool
}

// archiveMark is a chat key's archive gate: the version of the user's last
// ViewerState applied there, whether it held an archive, and when the
// session recorded it, which is what lets a strongly consistent read that
// began later overrule it (see applyArchives).
type archiveMark struct {
	version    uint64
	archived   bool
	recordedAt time.Time
}

// watermark is a chat key's roster gate as it stood before an attach — the
// transition last applied there, or none — and the version of the attach
// that displaced it, whose registration is in flight.
type watermark struct {
	mark       rosterMark
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
		roster:    make(map[string]rosterMark),
		displaced: make(map[string]watermark),
		archives:  make(map[string]archiveMark),
	}
}

// topicSeed is one of the user's membership records as read from a listing
// of one of their folders: the chat's key, the version of the user's last
// transition there, and whether that transition left them joined. Every
// record seeds a version; only a joined one can put the stream under its key
// (see seed).
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

// seed merges membership records read from the store into the session, each
// as a transition at its record's version through the same roster gate live
// transitions pass (see admitLocked), and then converges each record's key on
// the follow rule (see the type doc): a key the rule wants held is attached,
// one it does not is taken out. Either way the record's version is recorded
// if it is newer — which is what makes a transition the record already
// reflects stale, and a record that a transition applied meanwhile has
// already superseded stale in turn. It returns the keys it newly attached,
// for the caller to register with the cluster as one batch (see setHandles
// and setChatHandles), and the handles the detaches freed, for the caller to
// release. A draining registry attaches nothing; a closed session applies
// nothing.
//
// The open seeds the user's inbox records, and following the archive seeds
// their archived ones (see Server.followArchiveRequest), whose keys converge
// even when their records are not news: a group the open attached and an
// archive then took off is put back at the version it was taken off at.
func (s *streamSession) seed(seeds []topicSeed) (attached []string, released []*cluster.SubscriptionHandle) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return nil, nil
	}

	var removed []string
	for _, seed := range seeds {
		s.admitLocked(seed.key, seed.version, seed.joined)
		h, held := s.topics[seed.key]
		want := s.wantLocked(seed.key)
		switch {
		case want && !held:
			attached = append(attached, seed.key)
		case !want && held:
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
// registered them (see setChatHandles). Every handle is returned for the
// caller to release if the session has closed.
func (s *streamSession) setHandles(userHandle *cluster.SubscriptionHandle, chatKeys []string, chatHandles []*cluster.SubscriptionHandle) (refused []*cluster.SubscriptionHandle) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return append([]*cluster.SubscriptionHandle{userHandle}, chatHandles...)
	}

	s.userHandle = userHandle
	return s.setChatHandlesLocked(chatKeys, chatHandles)
}

// setChatHandles takes ownership of the cluster handles a seed's batch
// landed, one per chat key it attached, in the order the caller registered
// them. A handle is refused on the same terms as setHandle — the key was
// detached meanwhile, or another registration landed on it first — and,
// along with every handle if the session has closed, is returned for the
// caller to release.
func (s *streamSession) setChatHandles(chatKeys []string, chatHandles []*cluster.SubscriptionHandle) (refused []*cluster.SubscriptionHandle) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return chatHandles
	}
	return s.setChatHandlesLocked(chatKeys, chatHandles)
}

func (s *streamSession) setChatHandlesLocked(chatKeys []string, chatHandles []*cluster.SubscriptionHandle) (refused []*cluster.SubscriptionHandle) {
	for i, key := range chatKeys {
		if existing, held := s.topics[key]; !held || existing != nil {
			refused = append(refused, chatHandles[i])
			continue
		}
		s.topics[key] = chatHandles[i]
	}
	return refused
}

// admitLocked applies the roster gate to a transition on chatKey, recording
// it when it is newer than the last applied there. Called with mu held.
func (s *streamSession) admitLocked(chatKey string, version uint64, joined bool) bool {
	if last, seen := s.roster[chatKey]; seen && version <= last.version {
		return false
	}
	s.roster[chatKey] = rosterMark{version: version, joined: joined}
	return true
}

// wantLocked is the follow rule (see the type doc): whether the stream should
// be under chatKey, by both gates and the following. Called with mu held.
func (s *streamSession) wantLocked(chatKey string) bool {
	return s.roster[chatKey].joined && s.followsLocked(chatKey)
}

// followsLocked reports whether the archive gate lets the stream be under
// chatKey: the group is not archived, or the stream follows the archive or
// is lingering on it. Called with mu held.
func (s *streamSession) followsLocked(chatKey string) bool {
	return !s.archives[chatKey].archived || s.following || s.lingering
}

// attach applies a join at the given roster version and converges the key on
// the follow rule: it puts the stream under the chat key if the rule wants it
// there and it is not, registering it locally before the cluster
// registration that follows (see setHandle) — the same order a stream open
// uses, so a publish that resolves the row always finds the stream. The join
// is recorded if it is newer than the gate, and converges the key even when
// it is not, so a read of a record the gate already reflects re-attaches a
// key the session was taken off for an archive since undone or a failed
// registration since rolled back. It reports false, having attached nothing,
// if the rule does not want the key held (the user is not joined by the
// gate, or the group is archived and not followed), the stream is already
// under the key, is closed, or the registry is draining.
func (s *streamSession) attach(chatKey string, version uint64) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return false
	}
	prior, seen := s.roster[chatKey]
	s.admitLocked(chatKey, version, true)
	if !s.wantLocked(chatKey) {
		return false
	}
	if _, dup := s.topics[chatKey]; dup {
		return false
	}
	if !s.registry.add(s.id, s.local, []string{chatKey}) {
		return false
	}
	s.topics[chatKey] = nil
	s.displaced[chatKey] = watermark{mark: prior, seen: seen, attachedAt: version}
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
// leaves the key, and the roster gate goes back to what the attach
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
		s.roster[chatKey] = prior.mark
	} else {
		delete(s.roster, chatKey)
	}
}

// detach applies a departure at the given roster version: it takes the
// stream out from under the chat key and returns the handle that registered
// the topic for the caller to release — nil if the transition is stale (see
// admitLocked), the stream was not under the key, or its registration was
// still in flight (its registrant finds the key gone at setHandle and
// releases it). A departure that applies also ends the user's archive of the
// group in the archive gate (see the type doc).
func (s *streamSession) detach(chatKey string, version uint64) *cluster.SubscriptionHandle {
	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.admitLocked(chatKey, version, false) {
		return nil
	}
	if mark, ok := s.archives[chatKey]; ok && mark.archived {
		mark.archived = false
		s.archives[chatKey] = mark
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

// archiveChange is what applying one of the user's ViewerStates to a session
// asks of the caller (see archiveChanged).
type archiveChange struct {
	// released is the handle of a key an archive took the stream off, for
	// the caller to release; nil if there was none, or its registration was
	// still in flight (its registrant releases it).
	released *cluster.SubscriptionHandle

	// attach is set when an unarchive leaves the stream off a key it should
	// now be under, by the roster gate's account or for want of one: the
	// caller attaches it at version when known, and reads the user's
	// membership for it when not.
	attach  bool
	known   bool
	version uint64
}

// archiveChanged applies the user's ViewerState on chatKey at the given
// version through the archive gate: a state no newer than the last applied
// there moves nothing. An archive takes the stream off the key at once unless
// it follows the archive or is lingering on it. An unarchive leaves the key
// to the caller to attach (see archiveChange) when the stream is not under
// it and the roster gate has the user joined, or has no record of the group
// at all — a group archived before the open is in no record the open read
// (see Server.streamUserEvents) — but never when it has them departed: a
// return is a join, which attaches on its own.
func (s *streamSession) archiveChanged(chatKey string, version uint64, archived bool) archiveChange {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return archiveChange{}
	}
	if last, seen := s.archives[chatKey]; seen && version <= last.version {
		return archiveChange{}
	}
	s.archives[chatKey] = archiveMark{version: version, archived: archived, recordedAt: time.Now()}

	h, held := s.topics[chatKey]
	if archived {
		if !held || s.followsLocked(chatKey) {
			return archiveChange{}
		}
		delete(s.topics, chatKey)
		delete(s.displaced, chatKey)
		s.registry.remove(s.id, []string{chatKey})
		return archiveChange{released: h}
	}

	if held {
		return archiveChange{}
	}
	mark, known := s.roster[chatKey]
	if known && !mark.joined {
		return archiveChange{}
	}
	return archiveChange{attach: true, known: known, version: mark.version}
}

// applyArchives merges a strongly consistent read of the user's archives
// into the archive gate: archived is every group key the read found
// archived, with the ViewerState version it found, and readStartedAt is when
// the read was issued. A key it names is marked archived if the version is
// newer than the gate's. A key it does not name that the gate has archived is
// marked not archived — the archive was undone, by an unarchive or a
// departure, before the read — but only if the session recorded that archive
// before the read was issued: an archive whose ViewerStateChanged the session
// applied meanwhile may have landed after the read, which a strong read
// orders no differently from the read missing it. The gate's version is
// kept, so a delayed copy of the archive stays stale.
//
// It moves no key itself: the caller follows it with the paths that do
// (detachUnfollowed, seed, attach).
func (s *streamSession) applyArchives(archived map[string]uint64, readStartedAt time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return
	}
	now := time.Now()
	for key, version := range archived {
		if last, seen := s.archives[key]; seen && version <= last.version {
			continue
		}
		s.archives[key] = archiveMark{version: version, archived: true, recordedAt: now}
	}
	for key, mark := range s.archives {
		if _, ok := archived[key]; ok || !mark.archived || !mark.recordedAt.Before(readStartedAt) {
			continue
		}
		mark.archived = false
		s.archives[key] = mark
	}
}

// detachUnfollowed takes the stream off every key the archive gate has
// archived while the stream neither follows the archive nor lingers on it,
// returning the handles for the caller to release as one batch. It is how
// the reconcile heals an archive the session never heard of, and how a
// linger ends (see endLinger).
func (s *streamSession) detachUnfollowed() []*cluster.SubscriptionHandle {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.detachUnfollowedLocked()
}

func (s *streamSession) detachUnfollowedLocked() []*cluster.SubscriptionHandle {
	if s.closed || s.following || s.lingering {
		return nil
	}
	var removed []string
	var released []*cluster.SubscriptionHandle
	for key, h := range s.topics {
		if !s.archives[key].archived {
			continue
		}
		removed = append(removed, key)
		if h != nil {
			released = append(released, h)
		}
	}
	for _, key := range removed {
		delete(s.topics, key)
		delete(s.displaced, key)
	}
	if len(removed) > 0 {
		s.registry.remove(s.id, removed)
	}
	return released
}

// startFollowing turns following the archive on. It reports whether the
// stream already held every archived key it knows of — it was following
// already, or was lingering, and so never let one go — in which case there
// is nothing to read; otherwise the caller reads the user's archived
// memberships and seeds them (see Server.followArchiveRequest). A pending
// linger is cancelled either way.
func (s *streamSession) startFollowing() (holding bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	holding = s.following || s.lingering
	s.following = true
	if s.lingering {
		s.lingering = false
		s.lingerGen++
	}
	return holding
}

// stopFollowing turns following the archive off. The stream keeps the
// archived keys it holds, lingering on them until the caller ends the linger
// numbered gen (see endLinger). It reports started false, beginning no
// linger, if the stream was not following: a linger already under way keeps
// its own end.
func (s *streamSession) stopFollowing() (gen uint64, started bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.following {
		return 0, false
	}
	s.following = false
	s.lingering = true
	s.lingerGen++
	return s.lingerGen, true
}

// endLinger ends the linger numbered gen, taking the stream off every key the
// archive gate has archived (see detachUnfollowed) and returning their
// handles for the caller to release — unless following was turned back on,
// or turned off again, meanwhile, which cancelled or superseded it.
func (s *streamSession) endLinger(gen uint64) []*cluster.SubscriptionHandle {
	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.lingering || s.lingerGen != gen {
		return nil
	}
	s.lingering = false
	return s.detachUnfollowedLocked()
}

// unheldMembers is every chat key the roster gate has the user joined to
// that the stream is not under, each as a seed at the gate's own version:
// what a caller about to seed records from the store adds for the groups the
// records may not list yet (see Server.followArchiveRequest). A seed of the
// gate's own version is not news, so it converges the key and records
// nothing.
func (s *streamSession) unheldMembers() []topicSeed {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return nil
	}
	var seeds []topicSeed
	for key, mark := range s.roster {
		if _, held := s.topics[key]; mark.joined && !held {
			seeds = append(seeds, topicSeed{key: key, version: mark.version, joined: true})
		}
	}
	return seeds
}

// followsArchive reports whether the stream follows the user's archive or is
// lingering on it, and so wants the archived groups' keys held.
func (s *streamSession) followsArchive() bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.following || s.lingering
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
