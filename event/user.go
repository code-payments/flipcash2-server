package event

import (
	"context"
	"errors"

	"github.com/google/uuid"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"
	eventpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/event/v1"

	"github.com/code-payments/flipcash2-server/cluster"
)

// streamUserEvents serves a stream for the signing user: every event
// addressed to them, on their own topic and on the topic of every group they
// are a member of, following their membership as it moves.
func (s *Server) streamUserEvents(ctx context.Context, log *zap.Logger, stream grpc.BidiStreamingServer[eventpb.StreamEventsRequest, eventpb.StreamEventsResponse], userID *commonpb.UserId) error {
	// A stream open is the client coming to the foreground (guaranteed on app
	// open), which is when the badge resets to zero. Best-effort: a failure here
	// must not block streaming.
	if err := s.badges.Reset(ctx, userID); err != nil {
		log.With(zap.Error(err)).Warn("Failed to reset badge count on stream open")
	}

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

	ss := NewEventStream[*eventpb.Event](streamID, streamBufferSize)

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
	}()

	memberships, err := s.chats.GetGroupMembershipsForUser(ctx, userID)
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

	return s.run(ctx, log, stream, ss, nil)
}
