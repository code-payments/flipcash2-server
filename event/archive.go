package event

import (
	"context"
	"errors"
	"sync"
	"time"

	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"
	eventpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/event/v1"

	"github.com/code-payments/flipcash2-server/chat"
	"github.com/code-payments/flipcash2-server/cluster"
	"github.com/code-payments/flipcash2-server/model"
)

// A user stream and the user's archive (see chat/archive.go and the proto's
// StreamEventsRequest.FollowArchive):
//
//   - An archived DM is streamed as every DM is, on the user's own topic;
//     nothing here touches DMs.
//   - An archived group's chat topic is followed by a stream only while that
//     stream follows the user's archive, which it does from a FollowArchive
//     turning it on until one turning it off, and for a linger after (see
//     WithFollowArchiveLinger). A stream opens not following it. An
//     unarchived group is followed whatever the setting.
//   - What arrives on the user's own topic about an archived group — their
//     own ViewerStateChanged, their own MemberLeft, a creator's LobbyUpdate —
//     arrives whatever the setting: nothing filters at delivery, the stream
//     is simply not under the group's chat key.
//
// The setting is the stream's own (streamSession.following), so the user's
// other streams, here or on another server, are unaffected by it; every
// stream follows the user's archives and unarchives as they happen (see
// followArchive), each by its own setting.

// followArchiveResult is the answer to one FollowArchive: the seq to ack, or
// the status the stream ends with.
type followArchiveResult struct {
	seq uint64
	err error
}

// serveFollowArchive answers a user stream's FollowArchive requests, one at a
// time in the order received (see run), until the stream ends or one fails.
// Each answer is handed back to run, which sends the ack, so acks go out in
// request order and in line with the stream's events.
//
// Turning following on is answered once the stream is under every archived
// group's key, its cluster registration landed (see followArchiveRequest).
// Turning it off is answered at once: the stream keeps the archived groups it
// holds for the linger, and lets them go when the linger ends unless
// following was turned back on meanwhile, which cancels it and is answered
// with no read at all. A redundant request changes nothing and is answered
// all the same.
func (s *Server) serveFollowArchive(ctx context.Context, log *zap.Logger, session *streamSession, requests <-chan *eventpb.StreamEventsRequest_FollowArchive, results chan<- followArchiveResult) {
	var linger *time.Timer
	defer func() {
		if linger != nil {
			linger.Stop()
		}
	}()

	for {
		var req *eventpb.StreamEventsRequest_FollowArchive
		select {
		case <-ctx.Done():
			return
		case req = <-requests:
		}

		var err error
		if req.GetFollow() {
			if linger != nil {
				linger.Stop()
				linger = nil
			}
			err = s.followArchiveRequest(ctx, log, session)
		} else if gen, started := session.stopFollowing(); started {
			if linger != nil {
				linger.Stop()
			}
			linger = time.AfterFunc(s.followArchiveLinger, func() { s.endLinger(session, gen) })
		}

		select {
		case results <- followArchiveResult{seq: req.GetSeq(), err: err}:
		case <-ctx.Done():
			return
		}
		if err != nil {
			return
		}
	}
}

// followArchiveRequest turns following the user's archive on for the stream.
// A stream already holding every archived group it knows of — following, or
// lingering after it stopped — has nothing to do. Otherwise it reads the
// user's archived memberships, with the roster version of each, alongside
// the strongly consistent read of their archives, applies the archives to the
// stream's archive gate (see streamSession.applyArchives), and seeds the
// memberships, attaching the stream to each archived group it is not already
// under, with one batched registration for them all, as a stream open does
// for its inbox. It returns only once that registration has landed, so a
// publish on any server from then on finds this server registered, which is
// what the ack promises.
//
// The membership listing trails an archive by a moment (see
// chat.Store.GetGroupMembershipsForUser), so a group archived just before the
// read may be missing from it. If the stream knows the user is in it — it
// was in the inbox the open read, or joined since — it is seeded from the
// stream's own roster gate instead, and an archive landing after following
// turned on, before or during the reads, detaches nothing. One unarchived
// just before the read may still be listed archived; the archives read marks
// it unarchived, and the stream follows it either way.
//
// A failed read or registration ends the stream with Internal, as a failed
// read at open does, and a draining server ends it with Unavailable: an ack
// would promise what the stream does not do, and the client reconnects and
// asks again.
func (s *Server) followArchiveRequest(ctx context.Context, log *zap.Logger, session *streamSession) error {
	if session.startFollowing() {
		return nil
	}

	ctx, cancel := context.WithTimeout(ctx, followArchiveTimeout)
	defer cancel()

	userID := session.local.userID
	startedAt := time.Now()
	var memberships []chat.GroupMembership
	var membershipsErr error
	var reads sync.WaitGroup
	reads.Go(func() {
		memberships, membershipsErr = s.chats.GetGroupMembershipsForUser(ctx, userID, chat.FolderArchived)
	})
	archives, archivesErr := s.chats.GetArchivedChats(ctx, userID)
	reads.Wait()
	if err := errors.Join(membershipsErr, archivesErr); err != nil {
		log.With(zap.Error(err)).Warn("Failure loading archived group memberships for stream")
		return status.Error(codes.Internal, "failure loading archived group memberships")
	}

	session.applyArchives(archivedGroupKeys(archives), startedAt)

	seeds := make([]topicSeed, 0, len(memberships))
	chatIDs := make(map[string]*commonpb.ChatId, len(memberships))
	for _, m := range memberships {
		key := chatStreamKey(m.ChatID)
		seeds = append(seeds, topicSeed{key: key, version: m.Version, joined: m.Joined})
		chatIDs[key] = m.ChatID
	}
	// The groups the stream already knows the user is in but is not under —
	// archived ones an archive took it off — join the seed at the roster
	// version it knows, so one archived a moment ago, and not yet listed, is
	// followed too.
	for _, seed := range session.unheldMembers() {
		if _, listed := chatIDs[seed.key]; listed {
			continue
		}
		chatID, ok := chatIDFromStreamKey(seed.key)
		if !ok {
			continue
		}
		seeds = append(seeds, seed)
		chatIDs[seed.key] = chatID
	}
	chatKeys, released := session.seed(seeds)
	if len(released) > 0 {
		s.releaseHandles(ctx, released)
	}
	if len(chatKeys) == 0 {
		// A draining registry attaches nothing, and the ack would promise
		// groups the stream is not under; the client follows again on the
		// server it reconnects to.
		if s.streams.draining.Load() {
			return status.Error(codes.Unavailable, "server is draining")
		}
		return nil
	}

	topics := make([]cluster.SubscriptionTopic, 0, len(chatKeys))
	for _, key := range chatKeys {
		topics = append(topics, cluster.SubscriptionTopic{Namespace: ChatEventsNamespace, Key: chatIDs[key].Value})
	}
	handles, err := s.subscriptions.SubscribeAll(ctx, topics)
	if err != nil {
		if errors.Is(err, cluster.ErrSubscriptionsDraining) {
			return status.Error(codes.Unavailable, "server is draining")
		}
		log.With(zap.Error(err)).Warn("Failure registering stream subscriptions for archived groups")
		return status.Error(codes.Internal, "failure registering stream subscriptions")
	}
	if refused := session.setChatHandles(chatKeys, handles); len(refused) > 0 {
		s.releaseHandles(ctx, refused)
	}
	return nil
}

// endLinger ends the linger numbered gen on the session (see
// streamSession.endLinger), releasing the archived groups' registrations as
// one batch. It runs on the linger's own timer, off any stream's path.
func (s *Server) endLinger(session *streamSession, gen uint64) {
	handles := session.endLinger(gen)
	if len(handles) == 0 {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), membershipSyncTimeout)
	defer cancel()
	s.releaseHandles(ctx, handles)
}

// followArchive moves a user's open streams on and off a group's chat topic
// as they archive and unarchive it, driven by the user's own
// MetadataUpdate.ViewerStateChanged, which every change to their state on a
// chat publishes on their user topic (see chat.Server's
// publishViewerStateChanged) and which carries the whole state, archive
// included, at its version. Like followMembership, it reaches exactly the
// servers hosting the user's streams, and each adjusts its own.
//
// Every state is applied through each session's archive gate by its version,
// so a stale one moves nothing (see streamSession.archiveChanged). An archive
// takes every stream not following the archive off the group's key at once,
// before the event is notified, and releases the registrations off the
// caller's path. An unarchive attaches every stream not under the key that
// the roster gate has as a member, at the roster version the gate holds, on
// a detached goroutine (see unarchiveStreams); a stream whose gate has no
// record of the group — archived before the stream opened, so absent from
// the inbox the open read — is attached at the version a strongly
// consistent read of the user's membership finds, and not at all if it finds
// none. A state on a DM is ignored: a DM has no chat topic.
func (s *Server) followArchive(streamKey string, e *eventpb.Event) {
	update := e.GetChatUpdate()
	if update == nil || !chat.IsGroupChatID(update.GetChat()) {
		return
	}
	for _, md := range update.GetMetadataUpdates() {
		changed := md.GetViewerStateChanged()
		if changed == nil {
			continue
		}
		sessions := s.sessions.forUser(streamKey)
		if len(sessions) == 0 {
			return
		}
		state := changed.GetViewerState()
		archived := state.GetSettings().GetArchive() != nil
		chatKey := chatStreamKey(update.GetChat())

		var released []*cluster.SubscriptionHandle
		var pending []pendingUnarchive
		for _, session := range sessions {
			change := session.archiveChanged(chatKey, state.GetVersion(), archived)
			if change.released != nil {
				released = append(released, change.released)
			}
			if change.attach {
				pending = append(pending, pendingUnarchive{session: session, known: change.known, version: change.version})
			}
		}
		if len(released) > 0 {
			go func() {
				ctx, cancel := context.WithTimeout(context.Background(), membershipSyncTimeout)
				defer cancel()
				s.releaseHandles(ctx, released)
			}()
		}
		if len(pending) > 0 {
			go s.unarchiveStreams(update.GetChat(), chatKey, pending)
		}
	}
}

// pendingUnarchive is a session an unarchive leaves to be attached (see
// streamSession.archiveChanged): at version when its roster gate knows the
// membership, at what the membership read finds when not.
type pendingUnarchive struct {
	session *streamSession
	known   bool
	version uint64
}

// unarchiveStreams attaches the sessions an unarchive left off the group's
// key, each through attachSession, which applies the follow rule again under
// the session's lock, so an archive landing meanwhile wins. The membership is
// read — once, strongly consistent (chat.Store.GetGroupMemberRecords), a
// point read of the user's record on the group — only if some session's
// roster gate has no record of the group; a failed read, or a registration
// failure, stops the pass, and the reconcile attaches what it left.
func (s *Server) unarchiveStreams(chatID *commonpb.ChatId, chatKey string, pending []pendingUnarchive) {
	ctx, cancel := context.WithTimeout(context.Background(), membershipSyncTimeout)
	defer cancel()

	var read, member bool
	var readVersion uint64
	for _, p := range pending {
		version := p.version
		if !p.known {
			if !read {
				read = true
				userID := p.session.local.userID
				records, err := s.chats.GetGroupMemberRecords(ctx, userID, []*commonpb.ChatId{chatID})
				if err != nil {
					s.log.With(
						zap.Error(err),
						zap.String("user_id", model.UserIDString(userID)),
						zap.String("chat_id", model.ChatIDString(chatID)),
					).Warn("Failure reading group membership for unarchived chat")
					return
				}
				var record chat.GroupMember
				record, member = records[string(chatID.Value)]
				readVersion = record.Version
			}
			if !member {
				continue
			}
			version = readVersion
		}
		if _, err := s.attachSession(ctx, p.session, chatID, chatKey, version); err != nil {
			return
		}
	}
}

// archivedGroupKeys is the chat keys of the groups among archives, a read of
// the user's archives, each with the version of the user's state there. DMs
// are left out: a DM has no chat topic.
func archivedGroupKeys(archives []chat.ArchivedChat) map[string]uint64 {
	keys := make(map[string]uint64, len(archives))
	for _, a := range archives {
		if chat.IsGroupChatID(a.ChatID) {
			keys[chatStreamKey(a.ChatID)] = a.Version
		}
	}
	return keys
}
