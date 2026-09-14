package chat

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"sort"
	"time"

	"github.com/google/uuid"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	chatpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/chat/v1"
	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"

	"github.com/code-payments/flipcash2-server/model"
)

// The chat feeds: a user's chats, most recently active first, read in pages
// pinned to a snapshot watermark and reconciled by the client against the
// event stream (see the Chat service proto for the contract).
//
// The DM feed and the group feed share that contract but not an index. A DM
// send fans last_activity out to each member's inbox row, so a DM feed page is
// one index query. A group send touches only the canonical item, so a user's
// groups can only be ordered by reading every one of them and sorting. That is
// paid once per snapshot: the first page computes the order, and the paging
// token carries the rest of it forward as a window of chat IDs, so later pages
// read only the IDs they emit. The token is what bounds the feed: a user in
// more groups than it can carry is refused (see maxGroupFeedChats).
//
// The window is a hint of what to read, never the authority to read it: every
// page re-checks the caller's membership in each ID it emits (see
// Store.GetGroupChatsForUserByIDs), which is what handles both a member removed
// mid-read and a token a client has tampered with. The same read re-applies the
// snapshot, dropping a group that became active after it — that group's
// freshness is the stream's job, as for a DM.

// ---------------------------------------------------------------------------
// DM feed
// ---------------------------------------------------------------------------

// maxDmChatFeedPageSize bounds a single GetDmChatFeed page. It matches the
// max_items on GetDmChatFeedResponse.chats, so a page never exceeds what the
// response is allowed to carry.
const maxDmChatFeedPageSize = 100

func (s *Server) GetDmChatFeed(ctx context.Context, req *chatpb.GetDmChatFeedRequest) (*chatpb.GetDmChatFeedResponse, error) {
	userID, err := s.authz.Authorize(ctx, req, &req.Auth)
	if err != nil {
		return nil, err
	}

	log := s.log.With(zap.String("user_id", model.UserIDString(userID)))

	// Each DM type is its own feed; the request names which one (proto
	// validation restricts the value to a known DM type).
	chatType := req.GetDmChatType()

	// For backwards compatiblity for legacy clients
	if chatType == chatpb.ChatType_UNKNOWN {
		chatType = chatpb.ChatType_CONTACT_DM
	}

	limit := maxDmChatFeedPageSize
	if pageSize := req.GetQueryOptions().GetPageSize(); pageSize > 0 && int(pageSize) < limit {
		limit = int(pageSize)
	}

	// The first request (no token) mints a snapshot watermark at the current
	// time; later requests carry it back in the token so every page is served
	// against the same point-in-time view. The cursor advances within it. The
	// token also binds the feed's chat type, so a cursor from one feed cannot
	// be replayed against another.
	var snapshot time.Time
	var cursor *DmFeedCursor
	if token := req.GetQueryOptions().GetPagingToken(); token != nil {
		tokenSnapshot, tokenChatType, tokenCursor, ok := decodeDmFeedToken(token)
		if !ok || tokenChatType != chatType {
			return nil, status.Error(codes.InvalidArgument, "invalid paging token")
		}
		snapshot, cursor = tokenSnapshot, tokenCursor
	} else {
		snapshot = time.Now().UTC()
	}

	// Fetch one extra to detect whether a further page remains.
	chats, err := s.chats.GetDmFeedPage(ctx, userID, chatType, snapshot, cursor, limit+1)
	if err != nil {
		log.With(zap.Error(err)).Warn("Failure getting DM chats")
		return nil, status.Error(codes.Internal, "")
	}

	hasMore := len(chats) > limit
	if hasMore {
		chats = chats[:limit]
	}

	// HACK: temporarily pin one group chat to the top of one user's feed, until
	// there is a real group chat feed to serve it from.
	pinned := s.pinnedGroupChat(ctx, log, userID, chatType, req.GetQueryOptions().GetPagingToken() == nil)
	if pinned != nil && len(chats) >= maxDmChatFeedPageSize {
		// The pinned chat takes a slot, so give up the page's last DM to stay
		// within max_items. The cursor below is computed from what's retained, so
		// the next page resumes at the DM that was dropped.
		chats = chats[:maxDmChatFeedPageSize-1]
		hasMore = true
	}

	// Hydrate the pinned chat alongside the page so it shares the batched reads.
	// The paging token is still derived from the DM page alone: a group chat ID
	// is not a valid cursor.
	feed := chats
	if pinned != nil {
		feed = append([]*Chat{pinned}, chats...)
	}

	metadata, err := s.hydrate(ctx, userID, feed)
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
		resp.PagingToken = encodeDmFeedToken(snapshot, chatType, &DmFeedCursor{
			LastActivity: last.LastActivity,
			ChatID:       last.ID,
		})
	}
	return resp, nil
}

// HACK: the staff group chat gets pinned to the top of the first page of a
// staff member's tip DM feed.
var hackStaffChatID = uuid.MustParse("eb62c512-3934-40e0-85f5-9160a20104d0")

// pinnedGroupChat returns the group chat to pin to the top of this feed page,
// or nil when there is none — which is every case but the hack above: a
// non-staff user, a staff user who is not a member of the staff chat, another
// feed type, or a page past the first.
//
// A failure anywhere along the way returns nil rather than an error: the hack
// must never be what breaks a user's feed.
func (s *Server) pinnedGroupChat(ctx context.Context, log *zap.Logger, userID *commonpb.UserId, chatFeedType chatpb.ChatType, isFirstPage bool) *Chat {
	if !isFirstPage || chatFeedType != chatpb.ChatType_TIP_DM {
		return nil
	}

	// Staff first: it is the cheaper (cached) check, and it keeps the staff
	// chat's membership from being probed on behalf of everyone else.
	isStaff, err := s.accounts.IsStaff(ctx, userID)
	if err != nil {
		log.With(zap.Error(err)).Warn("Failure getting user staff status")
		return nil
	}
	if !isStaff {
		return nil
	}

	chatID := &commonpb.ChatId{Value: hackStaffChatID[:]}
	isMember, err := s.chats.IsMember(ctx, chatID, userID)
	if err != nil {
		log.With(zap.Error(err)).Warn("Failure checking staff chat membership")
		return nil
	}
	if !isMember {
		return nil
	}

	c, err := s.chats.GetChatByID(ctx, chatID)
	if err != nil {
		log.With(zap.Error(err)).Warn("Failure getting pinned group chat")
		return nil
	}
	return c
}

// dmFeedTokenLen is the byte length of an encoded GetDmChatFeed paging token:
// the snapshot watermark and the cursor's last_activity, each as big-endian
// int64 unix-nanos, followed by the cursor's chat ID and the feed's chat type
// as a single byte.
const dmFeedTokenLen = 8 + 8 + DmChatIDSize + 1

// encodeDmFeedToken serializes the snapshot watermark, feed chat type, and
// resume cursor into an opaque paging token for the client to echo on the next
// request.
func encodeDmFeedToken(snapshot time.Time, chatType chatpb.ChatType, cursor *DmFeedCursor) *commonpb.PagingToken {
	buf := make([]byte, dmFeedTokenLen)
	binary.BigEndian.PutUint64(buf[0:8], uint64(snapshot.UnixNano()))
	binary.BigEndian.PutUint64(buf[8:16], uint64(cursor.LastActivity.UnixNano()))
	copy(buf[16:16+DmChatIDSize], cursor.ChatID.Value)
	buf[16+DmChatIDSize] = byte(chatType)
	return &commonpb.PagingToken{Value: buf}
}

// decodeDmFeedToken reverses encodeDmFeedToken. ok is false if the token is nil
// or not the expected length (e.g. a client-fabricated value). The caller must
// check the returned chat type against the request's, rejecting a token minted
// for a different feed.
func decodeDmFeedToken(token *commonpb.PagingToken) (snapshot time.Time, chatType chatpb.ChatType, cursor *DmFeedCursor, ok bool) {
	if token == nil || len(token.Value) != dmFeedTokenLen {
		return time.Time{}, chatpb.ChatType_UNKNOWN, nil, false
	}
	snapshot = time.Unix(0, int64(binary.BigEndian.Uint64(token.Value[0:8]))).UTC()
	chatType = chatpb.ChatType(token.Value[16+DmChatIDSize])
	cursor = &DmFeedCursor{
		LastActivity: time.Unix(0, int64(binary.BigEndian.Uint64(token.Value[8:16]))).UTC(),
		ChatID:       &commonpb.ChatId{Value: append([]byte(nil), token.Value[16:16+DmChatIDSize]...)},
	}
	return snapshot, chatType, cursor, true
}

// ---------------------------------------------------------------------------
// Group feed
// ---------------------------------------------------------------------------

const (
	// maxGroupChatFeedPageSize bounds a single GetGroupChatFeed page. It matches
	// the max_items on GetGroupChatFeedResponse.chats.
	maxGroupChatFeedPageSize = 100

	// maxGroupFeedChats bounds how many group chats a user's feed may hold
	// within one snapshot. The feed's order is computed on the first page and
	// carried in the paging token from there, so this is what the token can
	// hold: a feed past it is refused with ErrGroupFeedTooLarge rather than
	// served in part. Sized so that a token carrying every ID but the first
	// page's stays within maxPagingTokenLen.
	maxGroupFeedChats = 1000

	// maxPagingTokenLen mirrors the max_len on common.v1.PagingToken.value.
	// A token longer than this fails proto validation on the way back in.
	maxPagingTokenLen = 16384

	groupFeedTokenVersion = 1

	// groupFeedTokenHeaderLen is the fixed prefix of a group feed token: the
	// version byte and the snapshot watermark. The rest is the window.
	groupFeedTokenHeaderLen = 1 + 8
)

// A token holding every chat of the largest allowed feed must fit the proto's
// token size limit; the difference underflowing uint fails to compile.
const _ uint = maxPagingTokenLen - (groupFeedTokenHeaderLen + maxGroupFeedChats*GroupChatIDSize)

// ErrGroupFeedTooLarge indicates that a user is in more group chats than
// maxGroupFeedChats, so their feed cannot be carried in a paging token.
var ErrGroupFeedTooLarge = errors.New("group feed exceeds the token window")

func (s *Server) GetGroupChatFeed(ctx context.Context, req *chatpb.GetGroupChatFeedRequest) (*chatpb.GetGroupChatFeedResponse, error) {
	userID, err := s.authz.Authorize(ctx, req, &req.Auth)
	if err != nil {
		return nil, err
	}

	log := s.log.With(zap.String("user_id", model.UserIDString(userID)))

	limit := maxGroupChatFeedPageSize
	if pageSize := req.GetQueryOptions().GetPageSize(); pageSize > 0 && int(pageSize) < limit {
		limit = int(pageSize)
	}

	// As for the DM feed, the first request (no token) mints the snapshot
	// watermark and later requests carry it back. Here the token also carries
	// the order to resume from; a first request has none yet, so it computes
	// the order from the top.
	var tok *groupFeedToken
	if token := req.GetQueryOptions().GetPagingToken(); token != nil {
		decoded, ok := decodeGroupFeedToken(token)
		if !ok {
			return nil, status.Error(codes.InvalidArgument, "invalid paging token")
		}
		tok = decoded
	} else {
		tok = &groupFeedToken{Snapshot: time.Now().UTC()}
	}

	page, next, err := s.groupFeedPage(ctx, userID, tok, limit)
	switch {
	case errors.Is(err, ErrGroupFeedTooLarge):
		log.Warn("Group feed exceeds the token window")
		return nil, status.Error(codes.ResourceExhausted, "too many group chats")
	case err != nil:
		log.With(zap.Error(err)).Warn("Failure getting group chats")
		return nil, status.Error(codes.Internal, "")
	}

	metadata, err := s.hydrate(ctx, userID, page)
	if err != nil {
		log.With(zap.Error(err)).Warn("Failure hydrating group feed metadata")
		return nil, status.Error(codes.Internal, "")
	}

	resp := &chatpb.GetGroupChatFeedResponse{
		Result:  chatpb.GetGroupChatFeedResponse_OK,
		Chats:   metadata,
		HasMore: len(next.Window) > 0,
	}
	// An empty page has nothing to resume from, so the token is omitted — as for
	// the DM feed.
	if len(page) > 0 {
		resp.PagingToken = encodeGroupFeedToken(next)
	}
	return resp, nil
}

// groupFeedToken is the decoded state a GetGroupChatFeed page resumes from.
type groupFeedToken struct {
	// Snapshot is the watermark every page of this read is served against.
	Snapshot time.Time

	// Window is the rest of the feed: every chat ID after those already
	// emitted, in feed order. Nil before the first page, when the order has
	// not been computed yet; empty once the feed is exhausted.
	Window []*commonpb.ChatId
}

// encodeGroupFeedToken serializes the token for the client to echo back.
func encodeGroupFeedToken(t *groupFeedToken) *commonpb.PagingToken {
	buf := make([]byte, groupFeedTokenHeaderLen, groupFeedTokenHeaderLen+len(t.Window)*GroupChatIDSize)
	buf[0] = groupFeedTokenVersion
	binary.BigEndian.PutUint64(buf[1:9], uint64(t.Snapshot.UnixNano()))
	for _, id := range t.Window {
		buf = append(buf, id.Value...)
	}
	return &commonpb.PagingToken{Value: buf}
}

// decodeGroupFeedToken reverses encodeGroupFeedToken. ok is false for anything
// the server would not have minted: a nil token, a wrong version, a length that
// is not a header plus whole chat IDs, a window over the bound, or a window
// with a repeated ID. Rejecting the malformed here is only a courtesy to the
// client; what a well-formed token may read is decided by the membership check
// on every page. The decoded window is never nil, so a token always resumes
// rather than recomputing.
func decodeGroupFeedToken(token *commonpb.PagingToken) (t *groupFeedToken, ok bool) {
	if token == nil || len(token.Value) < groupFeedTokenHeaderLen || token.Value[0] != groupFeedTokenVersion {
		return nil, false
	}
	buf := token.Value
	if (len(buf)-groupFeedTokenHeaderLen)%GroupChatIDSize != 0 {
		return nil, false
	}
	n := (len(buf) - groupFeedTokenHeaderLen) / GroupChatIDSize
	if n > maxGroupFeedChats {
		return nil, false
	}

	t = &groupFeedToken{
		Snapshot: time.Unix(0, int64(binary.BigEndian.Uint64(buf[1:9]))).UTC(),
		Window:   make([]*commonpb.ChatId, 0, n),
	}
	seen := make(map[string]struct{}, n)
	for off := groupFeedTokenHeaderLen; off < len(buf); off += GroupChatIDSize {
		id := buf[off : off+GroupChatIDSize]
		if _, dup := seen[string(id)]; dup {
			return nil, false
		}
		seen[string(id)] = struct{}{}
		t.Window = append(t.Window, &commonpb.ChatId{Value: append([]byte(nil), id...)})
	}
	return t, true
}

// groupFeedPage serves one page of the caller's group feed from the state in
// tok, returning the page in feed order and the state the next page resumes
// from. limit must be positive.
//
// The first page (a nil window) computes the whole feed within the snapshot,
// refusing with ErrGroupFeedTooLarge one larger than maxGroupFeedChats, emits
// its head and windows the rest. Every later page is served from the window
// alone. A window ID the caller has since left, or whose group went active
// after the snapshot, is dropped rather than emitted, and the page keeps
// filling from what follows; so a page is short only when the feed within the
// snapshot is exhausted.
func (s *Server) groupFeedPage(ctx context.Context, userID *commonpb.UserId, tok *groupFeedToken, limit int) ([]*Chat, *groupFeedToken, error) {
	next := &groupFeedToken{Snapshot: tok.Snapshot, Window: tok.Window}

	var page []*Chat
	if next.Window == nil {
		feed, err := s.sortedGroupFeed(ctx, userID, next.Snapshot)
		if err != nil {
			return nil, nil, err
		}
		if len(feed) > s.maxGroupFeedChats {
			return nil, nil, ErrGroupFeedTooLarge
		}
		// Emit the head directly, so a user whose whole feed fits in a page
		// pays the one read and nothing more.
		take := min(limit, len(feed))
		page = feed[:take]
		next.Window = make([]*commonpb.ChatId, len(feed)-take)
		for i, c := range feed[take:] {
			next.Window[i] = c.ID
		}
	}

	for len(page) < limit && len(next.Window) > 0 {
		take := min(limit-len(page), len(next.Window))
		ids := next.Window[:take]
		next.Window = next.Window[take:]

		// Re-check membership and re-read the records for exactly the IDs about
		// to be emitted. The read is unordered; the token's order is restored.
		chats, err := s.chats.GetGroupChatsForUserByIDs(ctx, userID, ids)
		if err != nil {
			return nil, nil, err
		}
		byID := make(map[string]*Chat, len(chats))
		for _, c := range chats {
			byID[string(c.ID.Value)] = c
		}
		for _, id := range ids {
			c, ok := byID[string(id.Value)]
			if !ok || c.LastActivity.After(next.Snapshot) {
				continue // Left, gone, or active since the snapshot: dropped.
			}
			page = append(page, c)
		}
	}
	return page, next, nil
}

// sortedGroupFeed computes the caller's whole group feed within the snapshot,
// in descending (last_activity, chat_id) order. It reads every group the
// caller is in.
func (s *Server) sortedGroupFeed(ctx context.Context, userID *commonpb.UserId, snapshot time.Time) ([]*Chat, error) {
	all, err := s.chats.GetGroupChatsForUser(ctx, userID)
	if err != nil {
		return nil, err
	}
	chats := all[:0]
	for _, c := range all {
		if !c.LastActivity.After(snapshot) {
			chats = append(chats, c)
		}
	}
	sort.Slice(chats, func(i, j int) bool {
		return lessByActivityDesc(chats[i], chats[j])
	})
	return chats, nil
}

// lessByActivityDesc orders chats most recent first, breaking ties by chat ID
// descending so the order is total.
func lessByActivityDesc(a, b *Chat) bool {
	if !a.LastActivity.Equal(b.LastActivity) {
		return a.LastActivity.After(b.LastActivity)
	}
	return bytes.Compare(a.ID.Value, b.ID.Value) > 0
}
