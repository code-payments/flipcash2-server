package event

import (
	"context"
	"errors"
	"sync"

	"github.com/google/uuid"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"
	eventpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/event/v1"

	ocp_client "github.com/code-payments/ocp-server/grpc/client"

	"github.com/code-payments/flipcash2-server/chat"
	"github.com/code-payments/flipcash2-server/cluster"
	"github.com/code-payments/flipcash2-server/rpc"
)

// streamUserEvents serves a stream for the signing user: every event
// addressed to them, on their own topic and on the topic of every group they
// are a member of and have not archived — and of their archived groups too
// while the stream follows their archive (see archive.go) — following their
// membership and archives as they move.
func (s *Server) streamUserEvents(ctx context.Context, log *zap.Logger, stream grpc.BidiStreamingServer[eventpb.StreamEventsRequest, eventpb.StreamEventsResponse], userID *commonpb.UserId) error {
	// The badge counts what was pushed while the user was not in the app, so
	// it resets at both ends of a stream. A stream open is the client coming to
	// the foreground (guaranteed on app open), so the count that built up while
	// they were away is cleared, and the pushes they receive while here count
	// from one. A stream's end (see the teardown below) is them leaving it, or
	// the stream dying under them: what arrived while it was open was seen as
	// it arrived, so the first push after they leave counts from one too,
	// rather than from everything delivered during the session. Best-effort
	// both times: a failure here must not block streaming.
	//
	// The reset at open runs alongside the membership read below and is
	// waited for right after it, so it costs the open no latency of its own
	// yet still lands before the stream goes live. It never outlives the
	// handler: the deferred wait covers the exits taken before that point.
	//
	// Only a stream from an iOS client resets, at either end. The count only
	// moves for iOS devices (see push.BadgeResolver), so for anyone else there
	// is nothing to clear, and for a user on several platforms, activity
	// elsewhere must not clear what the iOS device has yet to show. A client
	// whose device type cannot be read is not known to be iOS, so it does not
	// reset either.
	resetsBadge := rpc.GetClientDeviceType(ctx) == ocp_client.DeviceTypeIOS
	var openReset sync.WaitGroup
	if resetsBadge {
		openReset.Go(func() {
			if err := s.badges.Reset(ctx, userID); err != nil {
				log.With(zap.Error(err)).Warn("Failed to reset badge count on stream open")
			}
		})
	}
	defer openReset.Wait()

	streamID := uuid.New().String()

	log = log.With(zap.String("stream_id", streamID))

	// Sanity check whether the stream is still valid before doing expensive
	// operations
	select {
	case <-ctx.Done():
		log.Debug("Stream context cancelled; ending stream")
		return status.Error(codes.Canceled, "")
	default:
	}

	log.Debug("Initializing stream")

	ss := NewEventStream(streamID, streamBufferSize, isTypingOnly)

	// The stream serves the user's own topic plus one topic per group chat
	// they are joined to, so group publishers can resolve the hosting servers
	// by group instead of once per member.
	//
	// The local stream must be resolvable before a topic's registry row is, or
	// a publish racing the open could resolve the row yet find no stream
	// behind it — so the user key is registered up front, before the first
	// Subscribe below, and each chat key before its own row (see seed).
	//
	// And into the index before even that, so that from the first event the
	// user key can deliver, every transition on it moves this stream (see
	// followMembership): indexed after registering, a transition landing in
	// between would notify the stream and move nothing. The membership
	// records are then merged in behind the transitions through the same
	// version gate (see streamSession.seed), so a record and a transition
	// that cross — the read reflecting a transition already applied, or a
	// transition landing on a record that predates it — resolve to the newer
	// of the two, not the later-arriving. Departed records seed a version
	// too, so a delayed copy of a join the user has since undone is stale on
	// arrival rather than news.
	//
	// What remains uncovered is a transition this server never hears of: one
	// published before this server's row for the user's topic lands (below)
	// — which the user's first stream here cannot receive — that the read
	// also predates, by its timing or by the inverted membership index's
	// replication lag. The reconcile sweep re-reads for those (see
	// reconcileMembership).
	//
	// The read is of the user's inbox alone: a stream opens not following
	// their archive, so their archived groups' topics are not its to follow
	// until it is asked to (see followArchiveRequest). Nothing else is read
	// at open, the strongly consistent read of their archives included, which
	// costs a query of their whole state partition on every open, every app
	// foreground, to close a window the reconcile closes anyway: the listing
	// trails an archive or unarchive by the index's replication lag, so a
	// group archived a moment before the open may still be listed and
	// attached — its updates reach a client that holds it archived and has
	// no use for them until the archive's ViewerStateChanged, if it arrives,
	// or the first reconcile takes the stream off it — and one unarchived a
	// moment before may not be listed yet, and is missed until its
	// ViewerStateChanged or the reconcile attaches it, the gap covered by the
	// client's catch-up after an unarchive. A transition or state the stream
	// does hear of meanwhile is merged through the gates as above, so a seed
	// never attaches a group an archive delivered before it has taken off.
	session := newStreamSession(streamID, localStream{stream: ss, userID: userID}, userStreamKey(userID), s.streams)
	s.sessions.add(session)
	if !session.open() {
		// Indexed but never registered: a transition may still have found the
		// session and attached a chat key before the registry began draining,
		// so it is torn down as a closed stream is, not merely unindexed.
		s.sessions.remove(session)
		if handles := session.close(); len(handles) > 0 {
			closeCtx, cancel := context.WithTimeout(context.Background(), subscriptionCloseTimeout)
			s.releaseHandles(closeCtx, handles)
			cancel()
		}
		log.Debug("Rejecting stream on shut-down server")
		return status.Error(codes.Unavailable, "server is draining")
	}

	defer func() {
		log.Debug("Closing streamer")

		// Out of the index first, so no transition attaches to a stream that
		// is ending; then out of the registry, releasing every registration
		// the stream still holds as one batched close.
		s.sessions.remove(session)
		handles := session.close()

		closeCtx, cancel := context.WithTimeout(context.Background(), subscriptionCloseTimeout)
		if err := s.subscriptions.CloseAll(closeCtx, handles); err != nil {
			log.With(zap.Error(err)).Warn("Failed to close stream subscriptions")
		}
		cancel()

		// The badge resets on the way out as on the way in (see the reset
		// at open), for an iOS stream only. Every end of a registered stream
		// passes through here: the client leaving, a missed pong, a lag close,
		// a draining server. A stream that was never registered was turned
		// away at open, which had already reset if it was going to. The
		// stream's context is done by now, so the reset runs on its own
		// budget.
		if resetsBadge {
			resetCtx, cancel := context.WithTimeout(context.Background(), badgeResetTimeout)
			if err := s.badges.Reset(resetCtx, userID); err != nil {
				log.With(zap.Error(err)).Warn("Failed to reset badge count on stream close")
			}
			cancel()
		}
	}()

	memberships, err := s.chats.GetGroupMembershipsForUser(ctx, userID, chat.FolderInbox)
	// Before any return from here, so the open reset always lands before the
	// teardown's reset at close.
	openReset.Wait()
	if err != nil {
		log.With(zap.Error(err)).Warn("Failure loading group memberships for stream")
		return status.Error(codes.Internal, "failure loading group memberships")
	}

	seeds := make([]topicSeed, 0, len(memberships))
	chatIDs := make(map[string]*commonpb.ChatId, len(memberships))
	for _, m := range memberships {
		key := chatStreamKey(m.ChatID)
		seeds = append(seeds, topicSeed{key: key, version: m.Version, joined: m.Joined})
		chatIDs[key] = m.ChatID
	}
	chatKeys, released := session.seed(seeds)
	if len(released) > 0 {
		s.releaseHandles(ctx, released)
	}

	// Register this server's interest in the stream's topics with the cluster,
	// so publishers on other servers forward here — one batched registration
	// covering the user topic and every group topic the seed attached, so the
	// stream-open path pays a single store round trip no matter how many
	// groups. Non-exclusive: the same user (and all the more so the same
	// group) may hold streams on any number of servers simultaneously.
	topics := make([]cluster.SubscriptionTopic, 0, 1+len(chatKeys))
	topics = append(topics, cluster.SubscriptionTopic{Namespace: UserEventsNamespace, Key: userID.Value})
	for _, key := range chatKeys {
		topics = append(topics, cluster.SubscriptionTopic{Namespace: ChatEventsNamespace, Key: chatIDs[key].Value})
	}
	subscriptions, err := s.subscriptions.SubscribeAll(ctx, topics)
	if err != nil {
		if errors.Is(err, cluster.ErrSubscriptionsDraining) {
			log.Debug("Rejecting stream on draining server")
			return status.Error(codes.Unavailable, "server is draining")
		}
		log.With(zap.Error(err)).Warn("Failure registering stream subscriptions")
		return status.Error(codes.Internal, "failure registering stream subscriptions")
	}
	// A chat handle the session refuses belongs to a key a transition moved
	// while the batch was in flight (see setHandles); it is released as a
	// transition's own would be.
	if refused := session.setHandles(subscriptions[0], chatKeys, subscriptions[1:]); len(refused) > 0 {
		s.releaseHandles(ctx, refused)
	}

	return s.run(ctx, log, stream, ss, nil, session)
}
