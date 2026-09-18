package chat

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"time"

	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	chatpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/chat/v1"
	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"
	messagingpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/messaging/v1"
	profilepb "github.com/code-payments/flipcash2-protobuf-api/generated/go/profile/v1"

	"github.com/code-payments/flipcash2-server/model"
)

// The roster read: a chat's members, paged, most recently joined first (see
// RosterPosition), each page carrying the roster summary.
//
// A page has two shapes, chosen by the group's size — the same choice the
// push fan-out makes between reading a chat's mutes whole and by range (see
// messaging/push.go) — and the proto is written for the weaker one, so a
// client never learns which it got:
//
//   - At or below rosterWholeReadCap members, the roster is read whole in one
//     strongly consistent read that also carries the summary and is verified
//     against it (see Store.GetGroupRoster), then sorted and sliced here. The
//     page is exactly the roster at the summary's version: a single-page
//     roster is member_count members, and a later page is a fresh snapshot
//     at its own summary rather than a continuation of the first. Each page
//     re-reads the roster, which at the cap is a few RCU and at most ten
//     reads over a walk; a group of up to a page's worth is one call.
//
//   - Above it, the page is a range on the join-order index (see
//     Store.GetGroupRosterPage), billed by the page and eventually consistent,
//     alongside a summary read on its own. The page may lag the summary by a
//     transition; the client reconciles by Member.version against the
//     RosterUpdates it holds, as the proto describes.
//
// A DM is the whole shape without the store: its participants are inline on
// the record, at version zero with no join time. The two shapes resume from
// the same cursor, a position in roster order, so a group that crosses the
// cap between pages just serves its next page in the other shape.
//
// Pointers are hydrated for a DM's participants alone; a group member's
// never are, since group pointer advances are not broadcast and a page of
// them would be stale on arrival.
//
// The read is a listener's: a member's, or a non-member's whom the group's
// listener rules admit, evaluated through the shared Access with the usual
// admission cache. A viewer who may only preview the chat is denied — a
// roster is not a shape.

const (
	// maxGetRosterPageSize bounds a single GetRoster page and is its
	// default. It matches the max_items on GetRosterResponse.members.
	maxGetRosterPageSize = 100

	// rosterWholeReadCap is the largest roster read whole and strongly
	// consistent (see above); a larger one is paged from the join-order
	// index. It matches the push fan-out's cap on reading a chat's mutes
	// whole, for the same reason: this many small rows is one cheap read.
	rosterWholeReadCap = 1000

	rosterTokenVersion = 1

	// rosterTokenLen is the fixed size of a roster paging token: the version
	// byte, the chat ID the token is bound to, and the position it resumes
	// after — the join time as nanoseconds and the user ID.
	rosterTokenLen = 1 + GroupChatIDSize + 8 + model.UserIDSize
)

func (s *Server) GetRoster(ctx context.Context, req *chatpb.GetRosterRequest) (*chatpb.GetRosterResponse, error) {
	userID, err := s.authz.Authorize(ctx, req, &req.Auth)
	if err != nil {
		return nil, err
	}

	log := s.log.With(zap.String("user_id", model.UserIDString(userID)))

	limit := maxGetRosterPageSize
	if pageSize := req.GetQueryOptions().GetPageSize(); pageSize > 0 && int(pageSize) < limit {
		limit = int(pageSize)
	}

	// A token names the chat it was minted for: one replayed into another
	// chat would be a valid position there, so it is refused rather than
	// resumed. A DM's token is bound the same way, by the DM's ID.
	var after *RosterPosition
	if token := req.GetQueryOptions().GetPagingToken(); token != nil {
		pos, ok := decodeRosterToken(token, req.ChatId)
		if !ok {
			return nil, status.Error(codes.InvalidArgument, "invalid paging token")
		}
		after = &pos
	}

	// The canonical record first, as for GetChat: its absence is NOT_FOUND,
	// and a DM's roster and every chat's rules are on it.
	c, err := s.chats.GetChatByID(ctx, req.ChatId)
	switch {
	case errors.Is(err, ErrChatNotFound):
		return &chatpb.GetRosterResponse{Result: chatpb.GetRosterResponse_NOT_FOUND}, nil
	case err != nil:
		log.With(zap.Error(err)).Warn("Failure getting chat")
		return nil, status.Error(codes.Internal, "")
	}

	standing, err := s.access.StandingWithChat(ctx, c, userID, messagingpb.ViewMode_FULL)
	if err != nil {
		log.With(zap.Error(err)).Warn("Failure determining chat standing")
		return nil, status.Error(codes.Internal, "")
	}
	if !standing.CanListen {
		return &chatpb.GetRosterResponse{Result: chatpb.GetRosterResponse_DENIED}, nil
	}

	summary, members, hasMore, err := s.rosterPage(ctx, c, after, limit)
	if err != nil {
		log.With(zap.Error(err)).Warn("Failure reading roster page")
		return nil, status.Error(codes.Internal, "")
	}

	hydrated, err := s.hydrateRoster(ctx, c, members)
	if err != nil {
		log.With(zap.Error(err)).Warn("Failure hydrating roster page")
		return nil, status.Error(codes.Internal, "")
	}

	resp := &chatpb.GetRosterResponse{
		Result:        chatpb.GetRosterResponse_OK,
		Members:       hydrated,
		RosterSummary: summary.ToProto(),
		HasMore:       hasMore,
	}
	if hasMore {
		resp.PagingToken = encodeRosterToken(c.ID, members[len(members)-1].Position())
	}
	return resp, nil
}

// rosterPage reads one page of c's roster in roster order, strictly after
// the after position, at most limit members, choosing the shape described
// above, and reports whether members follow it. The summary is the one the
// page was read alongside: the whole shape's is the read's own, so the
// members are the roster at that version; the paged shape's is a separate
// read the page may lag.
func (s *Server) rosterPage(ctx context.Context, c *Chat, after *RosterPosition, limit int) (RosterSummary, []GroupMember, bool, error) {
	if !IsGroupChatID(c.ID) {
		members := make([]GroupMember, 0, len(c.Members))
		for _, m := range c.Members {
			members = append(members, GroupMember{UserID: &commonpb.UserId{Value: append([]byte(nil), m.Value...)}})
		}
		page, hasMore := sliceRoster(members, after, limit)
		return c.RosterSummary, page, hasMore, nil
	}

	summary, err := s.chats.GetGroupRosterSummary(ctx, c.ID)
	if err != nil {
		return RosterSummary{}, nil, false, err
	}
	if summary.MemberCount <= uint64(s.rosterWholeReadCap) {
		summary, members, err := s.chats.GetGroupRoster(ctx, c.ID)
		if err != nil {
			return RosterSummary{}, nil, false, err
		}
		page, hasMore := sliceRoster(members, after, limit)
		return summary, page, hasMore, nil
	}

	// One past the page tells whether more follow, so has_more is exact and a
	// walk never ends on an empty page.
	members, err := s.chats.GetGroupRosterPage(ctx, c.ID, after, limit+1)
	if err != nil {
		return RosterSummary{}, nil, false, err
	}
	hasMore := len(members) > limit
	if hasMore {
		members = members[:limit]
	}
	return summary, members, hasMore, nil
}

// sliceRoster sorts a whole roster into roster order and takes the page
// strictly after the after position, at most limit members, reporting
// whether any follow.
func sliceRoster(members []GroupMember, after *RosterPosition, limit int) ([]GroupMember, bool) {
	SortRoster(members)
	start := 0
	if after != nil {
		for start < len(members) && members[start].Position().Compare(*after) <= 0 {
			start++
		}
	}
	end := len(members)
	hasMore := false
	if end-start > limit {
		end = start + limit
		hasMore = true
	}
	return members[start:end], hasMore
}

// hydrateRoster builds the proto members of a roster page: every member's
// public profile, a contact DM's members' phone numbers, and a DM's members'
// pointers — the same rules hydrate applies to a chat's members, less the
// group member the metadata carries. The reads are independent and run
// concurrently. A group member's join time and version come from their
// record; a DM's participant has neither (see Member.joined_at).
func (s *Server) hydrateRoster(ctx context.Context, c *Chat, members []GroupMember) ([]*chatpb.Member, error) {
	out := make([]*chatpb.Member, 0, len(members))
	userIDs := make([]*commonpb.UserId, 0, len(members))
	for _, m := range members {
		member := &chatpb.Member{UserId: &commonpb.UserId{Value: append([]byte(nil), m.UserID.Value...)}}
		if IsGroupChatID(c.ID) {
			member.JoinedAt = timestamppb.New(m.JoinedAt)
			member.Version = m.Version
		}
		out = append(out, member)
		userIDs = append(userIDs, member.UserId)
	}
	if len(out) == 0 {
		return out, nil
	}

	var (
		publicProfiles map[string]*profilepb.UserProfile
		phoneNumbers   map[string]*commonpb.PhoneNumber
		pointers       map[string][]*messagingpb.Pointer
	)
	g, gctx := errgroup.WithContext(ctx)
	g.Go(func() (err error) {
		publicProfiles, err = s.profiles.GetPublicProfiles(gctx, userIDs)
		return err
	})
	if c.Type == chatpb.ChatType_CONTACT_DM {
		g.Go(func() (err error) {
			phoneNumbers, err = s.profiles.GetPhoneNumbers(gctx, userIDs)
			return err
		})
	}
	if !IsGroupChatID(c.ID) {
		g.Go(func() (err error) {
			pointers, err = s.messaging.Pointers(gctx, []PointerRef{{ChatID: c.ID, Members: userIDs}})
			return err
		})
	}
	if err := g.Wait(); err != nil {
		return nil, err
	}

	for _, m := range out {
		if err := assignProfile(m, c.Type, publicProfiles, phoneNumbers); err != nil {
			return nil, err
		}
	}
	assignPointers(out, pointers[string(c.ID.Value)])
	return out, nil
}

// encodeRosterToken serializes a page's resume position, bound to the chat
// it pages: the version byte, the chat ID padded to a group ID's width (a
// DM's 32-byte ID is bound by its leading bytes, which is as much as a
// fixed-width token can carry and more than a replay needs to be refused),
// the join time as big-endian nanoseconds — zero for a DM participant's
// absent join time, which no real join lands on — and the user ID.
func encodeRosterToken(chatID *commonpb.ChatId, pos RosterPosition) *commonpb.PagingToken {
	buf := make([]byte, rosterTokenLen)
	buf[0] = rosterTokenVersion
	copy(buf[1:1+GroupChatIDSize], chatID.Value)
	if !pos.JoinedAt.IsZero() {
		binary.BigEndian.PutUint64(buf[1+GroupChatIDSize:], uint64(pos.JoinedAt.UnixNano()))
	}
	copy(buf[1+GroupChatIDSize+8:], pos.UserID.Value)
	return &commonpb.PagingToken{Value: buf}
}

// decodeRosterToken parses a roster token minted for chatID; a malformed
// token, or one bound to another chat, is refused.
func decodeRosterToken(token *commonpb.PagingToken, chatID *commonpb.ChatId) (RosterPosition, bool) {
	b := token.GetValue()
	if len(b) != rosterTokenLen || b[0] != rosterTokenVersion {
		return RosterPosition{}, false
	}
	bound := make([]byte, GroupChatIDSize)
	copy(bound, chatID.GetValue())
	if !bytes.Equal(b[1:1+GroupChatIDSize], bound) {
		return RosterPosition{}, false
	}
	pos := RosterPosition{UserID: &commonpb.UserId{Value: append([]byte(nil), b[1+GroupChatIDSize+8:]...)}}
	if nanos := binary.BigEndian.Uint64(b[1+GroupChatIDSize:]); nanos != 0 {
		pos.JoinedAt = time.Unix(0, int64(nanos)).UTC()
	}
	return pos, true
}
