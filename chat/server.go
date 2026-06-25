package chat

import (
	"context"
	"encoding/binary"
	"errors"
	"time"

	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	chatpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/chat/v1"
	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"
	messagingpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/messaging/v1"
	phonepb "github.com/code-payments/flipcash2-protobuf-api/generated/go/phone/v1"
	profilepb "github.com/code-payments/flipcash2-protobuf-api/generated/go/profile/v1"

	"github.com/code-payments/flipcash2-server/auth"
	"github.com/code-payments/flipcash2-server/model"
)

// maxDmChatFeedPageSize bounds a single GetDmChatFeed page. It matches the
// max_items on GetDmChatFeedResponse.chats, so a page never exceeds what the
// response is allowed to carry.
const maxDmChatFeedPageSize = 100

// MessageRef identifies a chat's message to hydrate. The feed builds one ref per
// chat (its last message) to batch the lookup across the page.
type MessageRef struct {
	ChatID    *commonpb.ChatId
	MessageID *messagingpb.MessageId
}

// PointerRef names a chat and the members whose pointers to hydrate. The feed
// builds one ref per chat (with that chat's members) to batch the pointer lookup
// across the page.
type PointerRef struct {
	ChatID  *commonpb.ChatId
	Members []*commonpb.UserId
}

// MessagingReader is the read slice of the messaging domain the Chat service
// needs to hydrate feed metadata. It is declared here (consumer side) so the
// chat package need not import messaging, keeping the messaging→chat dependency
// one-way; the messaging package supplies the concrete adapter.
type MessagingReader interface {
	// LastMessages returns the message for each ref that exists, keyed by
	// string(chatID.Value). Refs without a message are absent from the map.
	LastMessages(ctx context.Context, refs []MessageRef) (map[string]*messagingpb.Message, error)

	// Pointers returns the delivered/read pointers for the members named in each
	// ref, keyed by string(chatID.Value). Chats with no matching pointers are
	// absent from the map.
	Pointers(ctx context.Context, refs []PointerRef) (map[string][]*messagingpb.Pointer, error)

	// LatestEventSequences returns the head event sequence of each given chat,
	// keyed by string(chatID.Value). Chats at head 0 (no messages) are absent from
	// the map, so a missing key means 0.
	LatestEventSequences(ctx context.Context, chatIDs []*commonpb.ChatId) (map[string]uint64, error)
}

// ProfileReader is the read slice of the profile domain the Chat service needs
// to hydrate member profiles. Like MessagingReader it is declared here (consumer
// side) so the chat package need not import profile; the profile package
// supplies the concrete adapter.
type ProfileReader interface {
	// GetPhoneNumbers returns the linked phone number for each of the given
	// users that has one, keyed by string(userID.Value). Users without a linked
	// phone number are absent from the map.
	GetPhoneNumbers(ctx context.Context, userIDs []*commonpb.UserId) (map[string]*phonepb.PhoneNumber, error)
}

type Server struct {
	log *zap.Logger

	authz auth.Authorizer

	chats     Store
	messaging MessagingReader
	profiles  ProfileReader

	chatpb.UnimplementedChatServer
}

func NewServer(log *zap.Logger, authz auth.Authorizer, chats Store, messaging MessagingReader, profiles ProfileReader) *Server {
	return &Server{
		log:       log,
		authz:     authz,
		chats:     chats,
		messaging: messaging,
		profiles:  profiles,
	}
}

func (s *Server) GetChat(ctx context.Context, req *chatpb.GetChatRequest) (*chatpb.GetChatResponse, error) {
	userID, err := s.authz.Authorize(ctx, req, &req.Auth)
	if err != nil {
		return nil, err
	}

	log := s.log.With(zap.String("user_id", model.UserIDString(userID)))

	c, err := s.chats.GetChatByID(ctx, req.ChatId)
	switch {
	case errors.Is(err, ErrChatNotFound):
		return &chatpb.GetChatResponse{Result: chatpb.GetChatResponse_NOT_FOUND}, nil
	case err != nil:
		log.With(zap.Error(err)).Warn("Failure getting chat")
		return nil, status.Error(codes.Internal, "")
	}

	if !c.HasMember(userID) {
		return &chatpb.GetChatResponse{Result: chatpb.GetChatResponse_DENIED}, nil
	}

	metadata, err := s.hydrate(ctx, []*Chat{c})
	if err != nil {
		log.With(zap.Error(err)).Warn("Failure hydrating chat metadata")
		return nil, status.Error(codes.Internal, "")
	}

	return &chatpb.GetChatResponse{
		Result:   chatpb.GetChatResponse_OK,
		Metadata: metadata[0],
	}, nil
}

func (s *Server) GetDmChatFeed(ctx context.Context, req *chatpb.GetDmChatFeedRequest) (*chatpb.GetDmChatFeedResponse, error) {
	userID, err := s.authz.Authorize(ctx, req, &req.Auth)
	if err != nil {
		return nil, err
	}

	log := s.log.With(zap.String("user_id", model.UserIDString(userID)))

	limit := maxDmChatFeedPageSize
	if pageSize := req.GetQueryOptions().GetPageSize(); pageSize > 0 && int(pageSize) < limit {
		limit = int(pageSize)
	}

	// The first request (no token) mints a snapshot watermark at the current
	// time; later requests carry it back in the token so every page is served
	// against the same point-in-time view. The cursor advances within it.
	var snapshot time.Time
	var cursor *DmFeedCursor
	if token := req.GetQueryOptions().GetPagingToken(); token != nil {
		var ok bool
		snapshot, cursor, ok = decodeDmFeedToken(token)
		if !ok {
			return nil, status.Error(codes.InvalidArgument, "invalid paging token")
		}
	} else {
		snapshot = time.Now().UTC()
	}

	// Fetch one extra to detect whether a further page remains.
	chats, err := s.chats.GetDmFeedPage(ctx, userID, snapshot, cursor, limit+1)
	if err != nil {
		log.With(zap.Error(err)).Warn("Failure getting DM chats")
		return nil, status.Error(codes.Internal, "")
	}

	hasMore := len(chats) > limit
	if hasMore {
		chats = chats[:limit]
	}

	metadata, err := s.hydrate(ctx, chats)
	if err != nil {
		log.With(zap.Error(err)).Warn("Failure hydrating DM feed metadata")
		return nil, status.Error(codes.Internal, "")
	}

	resp := &chatpb.GetDmChatFeedResponse{
		Result:  chatpb.GetDmChatFeedResponse_OK,
		Chats:   metadata,
		HasMore: hasMore,
	}
	// Carry the snapshot forward and advance the cursor to the last returned
	// chat. An empty page has nothing to resume from, so the token is omitted.
	if n := len(chats); n > 0 {
		last := chats[n-1]
		resp.PagingToken = encodeDmFeedToken(snapshot, &DmFeedCursor{
			LastActivity: last.LastActivity,
			ChatID:       last.ID,
		})
	}
	return resp, nil
}

// dmFeedTokenLen is the byte length of an encoded GetDmChatFeed paging token:
// the snapshot watermark and the cursor's last_activity, each as big-endian
// int64 unix-nanos, followed by the cursor's chat ID.
const dmFeedTokenLen = 8 + 8 + ChatIDSize

// encodeDmFeedToken serializes the snapshot watermark and resume cursor into an
// opaque paging token for the client to echo on the next request.
func encodeDmFeedToken(snapshot time.Time, cursor *DmFeedCursor) *commonpb.PagingToken {
	buf := make([]byte, dmFeedTokenLen)
	binary.BigEndian.PutUint64(buf[0:8], uint64(snapshot.UnixNano()))
	binary.BigEndian.PutUint64(buf[8:16], uint64(cursor.LastActivity.UnixNano()))
	copy(buf[16:], cursor.ChatID.Value)
	return &commonpb.PagingToken{Value: buf}
}

// decodeDmFeedToken reverses encodeDmFeedToken. ok is false if the token is nil
// or not the expected length (e.g. a client-fabricated value).
func decodeDmFeedToken(token *commonpb.PagingToken) (snapshot time.Time, cursor *DmFeedCursor, ok bool) {
	if token == nil || len(token.Value) != dmFeedTokenLen {
		return time.Time{}, nil, false
	}
	snapshot = time.Unix(0, int64(binary.BigEndian.Uint64(token.Value[0:8]))).UTC()
	cursor = &DmFeedCursor{
		LastActivity: time.Unix(0, int64(binary.BigEndian.Uint64(token.Value[8:16]))).UTC(),
		ChatID:       &commonpb.ChatId{Value: append([]byte(nil), token.Value[16:]...)},
	}
	return snapshot, cursor, true
}

// hydrate builds the proto metadata for a set of chats, batching the reads
// across the whole set: every chat's last message in one call, every chat's
// pointers in one call, every chat's head event sequence in one call, and every
// DM member's phone number in one call. Member profiles otherwise carry an empty
// placeholder (UserProfile is a required field).
//
// Phone numbers are populated only for members of DM chats, so each party can
// resolve the other to a contact. Group chats deliberately do not expose member
// phone numbers.
func (s *Server) hydrate(ctx context.Context, chats []*Chat) ([]*chatpb.Metadata, error) {
	var msgRefs []MessageRef
	var seqChatIDs []*commonpb.ChatId
	pointerRefs := make([]PointerRef, len(chats))
	uniqueDmUserIds := make(map[string]*commonpb.UserId)
	for i, c := range chats {
		pointerRefs[i] = PointerRef{ChatID: c.ID, Members: c.Members}
		if c.LastMessageID != nil {
			msgRefs = append(msgRefs, MessageRef{ChatID: c.ID, MessageID: c.LastMessageID})
			// A chat's head is 0 unless it has at least one message, which is
			// exactly when it has a last message ID. Skip the rest: their head is
			// the proto default 0.
			seqChatIDs = append(seqChatIDs, c.ID)
		}
		if c.Type == chatpb.Metadata_DM {
			for _, m := range c.Members {
				uniqueDmUserIds[string(m.Value)] = m
			}
		}
	}
	dmUserIDs := make([]*commonpb.UserId, 0, len(uniqueDmUserIds))
	for _, u := range uniqueDmUserIds {
		dmUserIDs = append(dmUserIDs, u)
	}

	lastMessages, err := s.messaging.LastMessages(ctx, msgRefs)
	if err != nil {
		return nil, err
	}
	pointers, err := s.messaging.Pointers(ctx, pointerRefs)
	if err != nil {
		return nil, err
	}
	latestEventSeqs, err := s.messaging.LatestEventSequences(ctx, seqChatIDs)
	if err != nil {
		return nil, err
	}
	phoneNumbersByUserId, err := s.profiles.GetPhoneNumbers(ctx, dmUserIDs)
	if err != nil {
		return nil, err
	}

	metadata := make([]*chatpb.Metadata, len(chats))
	for i, c := range chats {
		key := string(c.ID.Value)
		md := c.ToProto()
		md.LastMessage = lastMessages[key]
		md.LatestEventSequence = latestEventSeqs[key]
		assignPointers(md, pointers[key])
		for _, m := range md.Members {
			profile := &profilepb.UserProfile{}
			if md.Type == chatpb.Metadata_DM {
				profile.PhoneNumber = phoneNumbersByUserId[string(m.UserId.Value)]
			}
			m.UserProfile = profile
		}
		metadata[i] = md
	}
	return metadata, nil
}

// assignPointers distributes a chat's pointers onto the matching member entries
// by user ID. SENT pointers are never shared with the chat, so they are dropped
// defensively; each member is left with its DELIVERED and/or READ pointers.
func assignPointers(md *chatpb.Metadata, pointers []*messagingpb.Pointer) {
	if len(pointers) == 0 {
		return
	}
	byUser := make(map[string][]*messagingpb.Pointer, len(md.Members))
	for _, p := range pointers {
		if p.Type == messagingpb.Pointer_SENT {
			continue
		}
		byUser[string(p.UserId.Value)] = append(byUser[string(p.UserId.Value)], p)
	}
	for _, m := range md.Members {
		m.Pointers = byUser[string(m.UserId.Value)]
	}
}
