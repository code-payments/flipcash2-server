package event

import (
	"bytes"
	"context"
	"errors"
	"time"

	"github.com/google/uuid"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"
	eventpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/event/v1"
	messagingpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/messaging/v1"

	"github.com/code-payments/flipcash2-server/chat"
	"github.com/code-payments/flipcash2-server/cluster"
	"github.com/code-payments/flipcash2-server/model"
	"github.com/code-payments/flipcash2-server/redact"
)

// defaultChatPreviewLifetime is how long a chat preview stream lives (see
// chatPreview): the server's bound, fixed at open, after which the stream
// ends with STREAM_EXPIRED whatever the client does. It is also how long a
// viewer's standing, evaluated once at open, is trusted: a viewer whose
// balance has since dropped, whose staff flag was since revoked, or who was
// since removed from the roster keeps the preview until the window closes,
// and is refused the next one. The window is the same lag a member has
// indefinitely on their reads, and a non-member has for the admission window
// on theirs, so no standing is re-evaluated while a preview runs.
const defaultChatPreviewLifetime = 5 * time.Minute

// WithChatPreviewLifetime overrides defaultChatPreviewLifetime. Tests use it
// to run a preview's window fast; production keeps the default.
func WithChatPreviewLifetime(lifetime time.Duration) ServerOption {
	return func(s *Server) {
		s.chatPreviewLifetime = lifetime
	}
}

// chatPreview is a stream targeted at a preview of a single group chat (see
// eventpb.StreamEventsRequest.ChatPreviewParams): the viewer, the chat, the
// mode the viewer asked for, the reading the viewer's standing resolved it to
// when the stream opened, and when the stream's window closes. The stream
// carries ChatUpdate for that chat and nothing else, every message on it
// under the one reading, fixed at open, until it expires.
//
// A user stream serves a user's own topic, which addresses them as a member:
// it never hears a group they are not a member of. A preview is what lets a
// non-member follow a group they may read before joining — a qualifying
// viewer in full, anyone else the group admits redacted — so it is a
// non-member's alone: a member is DENIED a preview whatever the mode, their
// user stream already carrying the chat in full, and a member who wants it
// blurred blurs it themselves. It is registered by the chat rather than the
// viewer: under the chat's key alone, where every update of a group is
// published. The viewer's own membership transitions are not among them —
// the chat topic's delivery excludes their subject, whose own copy is
// addressed to their user stream — and a preview has no use for them: a
// viewer who joins while previewing switches to their user stream, which
// carries the chat from the join on, and the preview runs out its window
// unextended. A DM is never previewed: it admits its members and no one
// else, and its members follow it on their user streams, so a DM is refused
// before its record is read (see Server.streamChatPreview).
//
// Every event delivered to the stream passes shape, which admits only a
// ChatUpdate for the chat. The chat topic carries nothing else today, but the
// filter is what makes that a property of the stream rather than of the
// publishers.
//
// The standing is evaluated once, at open, and trusted for the window (see
// defaultChatPreviewLifetime). A preview is not moved by the membership
// transitions that move a user stream's chat topics (see
// Server.followMembership): its key is fixed by the chat it names, so it is
// indexed in the registry and nowhere else.
type chatPreview struct {
	chatID    *commonpb.ChatId
	userID    *commonpb.UserId
	mode      messagingpb.ViewMode
	reading   chat.Reading
	expiresAt time.Time
}

// shape is what the stream delivers of an event it was notified: the event
// as published, if it is a ChatUpdate for the stream's chat and the stream
// reads in full; its redaction (see redact.ChatUpdate) if the stream reads
// redacted; and nothing — nil, no error — for any other event. An update the
// stream cannot redact is an error, and ends the stream: a message that
// cannot be redacted is not delivered to a viewer who may only see it
// redacted, as it fails the read on every other path.
//
// The published event is never modified: it is shared by pointer with every
// other stream it was delivered to. A redacted copy is a new event with the
// same identity and time.
func (p *chatPreview) shape(e *eventpb.Event) (*eventpb.Event, error) {
	update := e.GetChatUpdate()
	if update == nil || !bytes.Equal(update.GetChat().GetValue(), p.chatID.GetValue()) {
		return nil, nil
	}
	if p.reading != chat.ReadingRedacted {
		return e, nil
	}
	redacted, err := redact.ChatUpdate(p.chatID, update)
	if err != nil {
		return nil, err
	}
	return &eventpb.Event{
		Id:   e.GetId(),
		Ts:   e.GetTs(),
		Type: &eventpb.Event_ChatUpdate{ChatUpdate: redacted},
	}, nil
}

// chatStanding is userID's standing in chatID as needed to answer a read
// under mode (see chat.Access.Standing), by the same rule as every other read
// under a ViewMode. It reads the chat's canonical record itself so that a
// chat that does not exist is chat.ErrChatNotFound, which a preview's open
// reports as NOT_FOUND, distinct from the DENIED that a standing alone would
// give it.
func (s *Server) chatStanding(ctx context.Context, chatID *commonpb.ChatId, userID *commonpb.UserId, mode messagingpb.ViewMode) (chat.Standing, error) {
	c, err := s.chats.GetChatByID(ctx, chatID)
	if err != nil {
		return chat.Standing{}, err
	}
	return s.access.StandingWithChat(ctx, c, userID, mode)
}

// streamChatPreview serves a preview of one group chat (see chatPreview): it
// resolves the viewer's reading of the chat under the mode asked, refusing
// the stream with NOT_FOUND or DENIED as a read would be refused, registers
// the stream by the chat, and runs it until its window closes.
func (s *Server) streamChatPreview(ctx context.Context, log *zap.Logger, stream grpc.BidiStreamingServer[eventpb.StreamEventsRequest, eventpb.StreamEventsResponse], userID *commonpb.UserId, params *eventpb.StreamEventsRequest_ChatPreviewParams) error {
	chatID, mode := params.GetChatId(), params.GetViewMode()

	log = log.With(
		zap.String("chat_id", model.ChatIDString(chatID)),
		zap.String("view_mode", mode.String()),
	)

	deny := func() error {
		return stream.Send(&eventpb.StreamEventsResponse{Type: &eventpb.StreamEventsResponse_Error{
			Error: &eventpb.StreamEventsResponse_StreamError{Code: eventpb.StreamEventsResponse_StreamError_DENIED},
		}})
	}

	// Only a group is previewed (see chatPreview): anything else is DENIED
	// whatever the viewer's standing, before its record is read — a DM is
	// its members' alone, and an ID of neither width names nothing.
	if !chat.IsGroupChatID(chatID) {
		return deny()
	}

	standing, err := s.chatStanding(ctx, chatID, userID, mode)
	switch {
	case errors.Is(err, chat.ErrChatNotFound):
		return stream.Send(&eventpb.StreamEventsResponse{Type: &eventpb.StreamEventsResponse_Error{
			Error: &eventpb.StreamEventsResponse_StreamError{Code: eventpb.StreamEventsResponse_StreamError_NOT_FOUND},
		}})
	case err != nil:
		log.With(zap.Error(err)).Warn("Failure determining chat preview standing")
		return status.Error(codes.Internal, "failure determining chat standing")
	case standing.IsMember:
		// A member has nothing to preview (see chatPreview).
		return deny()
	}
	reading := standing.Reading(mode)
	if reading == chat.ReadingDenied {
		return deny()
	}

	// A preview is opened to read a chat, not on coming to the foreground,
	// so the badge is left to the user stream that open already reset.

	streamID := uuid.New().String()

	log = log.With(zap.String("stream_id", streamID))

	select {
	case <-ctx.Done():
		log.Debug("Stream context cancelled; ending stream")
		return status.Error(codes.Canceled, "")
	default:
	}

	log.Debug("Initializing chat preview stream")

	// The window is fixed here, at open, before the registration it does not
	// wait on.
	preview := &chatPreview{
		chatID:    chatID,
		userID:    userID,
		mode:      mode,
		reading:   reading,
		expiresAt: time.Now().Add(s.chatPreviewLifetime),
	}
	ss := NewEventStream[*eventpb.Event](streamID, streamBufferSize)

	// The stream's one key is fixed by the chat it names (see chatPreview). As
	// at a user stream's open, the local registration leads the cluster one,
	// so a publish that resolves this server's row always finds the stream
	// behind it; and on teardown the stream leaves the registry before its
	// row goes.
	chatKey := chatStreamKey(chatID)
	if !s.streams.add(streamID, localStream{stream: ss, userID: userID}, []string{chatKey}) {
		log.Debug("Rejecting chat preview stream on shut-down server")
		return status.Error(codes.Unavailable, "server is draining")
	}
	var handle *cluster.SubscriptionHandle
	defer func() {
		log.Debug("Closing chat preview streamer")

		s.streams.remove(streamID, []string{chatKey})
		if handle == nil {
			return
		}
		closeCtx, cancel := context.WithTimeout(context.Background(), subscriptionCloseTimeout)
		s.releaseHandles(closeCtx, []*cluster.SubscriptionHandle{handle})
		cancel()
	}()

	handle, err = s.subscriptions.Subscribe(ctx, ChatEventsNamespace, chatID.Value)
	if err != nil {
		if errors.Is(err, cluster.ErrSubscriptionsDraining) {
			log.Debug("Rejecting chat preview stream on draining server")
			return status.Error(codes.Unavailable, "server is draining")
		}
		log.With(zap.Error(err)).Warn("Failure registering chat preview stream subscription")
		return status.Error(codes.Internal, "failure registering stream subscription")
	}

	return s.run(ctx, log, stream, ss, preview)
}
