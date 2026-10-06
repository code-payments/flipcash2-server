package chat

import (
	"bytes"
	"context"
	"fmt"

	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	chatpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/chat/v1"
	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"

	"github.com/code-payments/flipcash2-server/model"
)

// A user's featured groups are an ordered list of public group chats they
// show on their full profile. The list is the user's alone and says nothing
// about the groups: featuring a group needs no membership, leaving a group
// does not remove it, and nothing is recorded against the group. It is
// written whole (see Store.SetFeaturedGroups), so a reorder, an addition and
// a removal are the same write, and read whole.
//
// Only public groups may be featured, since the list is shown to anyone and
// a private group (see Chat.IsPrivate) has no public view. The store holds
// the IDs alone and reads no record, so the setter checks each group's
// record before the write, as it checks that the group exists. IsPrivate is
// fixed when a group is created, so a group featured as public stays public
// and a read is not expected to find a private one (it checks all the same;
// see featuredGroupsMetadata). Whether each group still exists is decided
// when the list is read for display, not stored.

// MaxFeaturedGroups is the most groups a user may feature. A profile shows
// them all, so it is small; it is also what keeps a replace within one
// store transaction.
const MaxFeaturedGroups = 10

// FeaturedGroups is a user's featured groups as stored: the groups in the
// user's order, and the list's version, which moves by exactly one with each
// write that changes the list and never otherwise (state, not delta, as a
// roster's). A user who has never set a list reads as no groups at version
// zero.
type FeaturedGroups struct {
	ChatIDs []*commonpb.ChatId
	Version uint64
}

// Equal reports whether the two lists name the same groups in the same
// order, whatever their versions.
func (f FeaturedGroups) Equal(chatIDs []*commonpb.ChatId) bool {
	if len(f.ChatIDs) != len(chatIDs) {
		return false
	}
	for i := range chatIDs {
		if !bytes.Equal(f.ChatIDs[i].Value, chatIDs[i].Value) {
			return false
		}
	}
	return true
}

// ValidateFeaturedGroups returns an error unless chatIDs is a list a store
// accepts as a user's featured groups: at most MaxFeaturedGroups group chat
// IDs, none repeated. An empty list is valid and clears the featured
// groups. A repeat is refused rather than collapsed, since which of its
// positions to keep is the caller's choice. It does not check that the groups
// exist or are public: that needs their records, which are the setter's to
// read.
func ValidateFeaturedGroups(chatIDs []*commonpb.ChatId) error {
	if len(chatIDs) > MaxFeaturedGroups {
		return fmt.Errorf("%d featured groups exceeds the limit of %d", len(chatIDs), MaxFeaturedGroups)
	}
	seen := make(map[string]struct{}, len(chatIDs))
	for _, chatID := range chatIDs {
		if !IsGroupChatID(chatID) {
			return fmt.Errorf("featured chat %x is not a group chat id", chatID.GetValue())
		}
		if _, ok := seen[string(chatID.Value)]; ok {
			return fmt.Errorf("featured group %x is repeated", chatID.Value)
		}
		seen[string(chatID.Value)] = struct{}{}
	}
	return nil
}

// SetFeaturedGroups replaces the caller's featured groups. The groups'
// records are read first: a group that does not exist is NOT_FOUND, then a
// private one DENIED, each with nothing written. The read is eventually
// consistent (see Store.GetGroupChatsByID), so a group created a moment ago
// may be NOT_FOUND until the read catches up, which a retry corrects; a
// privacy check cannot be fooled by it, since IsPrivate is written with the
// record and never changes. The
// response is the list as written, projected from the records the checks
// read, so it costs no read beyond them.
func (s *Server) SetFeaturedGroups(ctx context.Context, req *chatpb.SetFeaturedGroupsRequest) (*chatpb.SetFeaturedGroupsResponse, error) {
	userID, err := s.authz.Authorize(ctx, req, &req.Auth)
	if err != nil {
		return nil, err
	}
	log := s.log.With(zap.String("user_id", model.UserIDString(userID)))

	if err := ValidateFeaturedGroups(req.ChatIds); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	var records map[string]*Chat
	if len(req.ChatIds) > 0 {
		records, err = s.chats.GetGroupChatsByID(ctx, req.ChatIds)
		if err != nil {
			log.With(zap.Error(err)).Warn("Failure getting featured group records")
			return nil, status.Error(codes.Internal, "")
		}
	}
	for _, chatID := range req.ChatIds {
		if _, ok := records[string(chatID.Value)]; !ok {
			return &chatpb.SetFeaturedGroupsResponse{Result: chatpb.SetFeaturedGroupsResponse_NOT_FOUND}, nil
		}
	}
	for _, chatID := range req.ChatIds {
		if records[string(chatID.Value)].IsPrivate {
			return &chatpb.SetFeaturedGroupsResponse{Result: chatpb.SetFeaturedGroupsResponse_DENIED}, nil
		}
	}

	if _, _, err := s.chats.SetFeaturedGroups(ctx, userID, req.ChatIds); err != nil {
		log.With(zap.Error(err)).Warn("Failure setting featured groups")
		return nil, status.Error(codes.Internal, "")
	}

	featured, err := s.featuredGroupsMetadata(ctx, log, req.ChatIds, records)
	if err != nil {
		log.With(zap.Error(err)).Warn("Failure hydrating featured groups")
		return nil, status.Error(codes.Internal, "")
	}
	return &chatpb.SetFeaturedGroupsResponse{
		Result:         chatpb.SetFeaturedGroupsResponse_OK,
		FeaturedGroups: featured,
	}, nil
}

// GetFeaturedGroups returns the featured groups of the user holding the
// requested handle. The list is the same for every caller: auth, when set,
// is verified as everywhere else, but changes nothing.
func (s *Server) GetFeaturedGroups(ctx context.Context, req *chatpb.GetFeaturedGroupsRequest) (*chatpb.GetFeaturedGroupsResponse, error) {
	log := s.log
	if req.Auth != nil {
		callerID, err := s.authz.Authorize(ctx, req, &req.Auth)
		if err != nil {
			return nil, err
		}
		log = log.With(zap.String("caller_id", model.UserIDString(callerID)))
	}

	var username string
	switch typed := req.Identifier.(type) {
	case *chatpb.GetFeaturedGroupsRequest_Username:
		username = typed.Username.GetValue()
	default:
		return nil, status.Error(codes.InvalidArgument, "unsupported identifier")
	}
	log = log.With(zap.String("username", username))

	userID, ok, err := s.profiles.GetUserIDByUsername(ctx, username)
	if err != nil {
		log.With(zap.Error(err)).Warn("Failure getting user by username")
		return nil, status.Error(codes.Internal, "")
	}
	if !ok {
		return &chatpb.GetFeaturedGroupsResponse{Result: chatpb.GetFeaturedGroupsResponse_NOT_FOUND}, nil
	}
	log = log.With(zap.String("user_id", model.UserIDString(userID)))

	list, err := s.chats.GetFeaturedGroups(ctx, userID)
	if err != nil {
		log.With(zap.Error(err)).Warn("Failure getting featured groups")
		return nil, status.Error(codes.Internal, "")
	}

	var records map[string]*Chat
	if len(list.ChatIDs) > 0 {
		records, err = s.chats.GetGroupChatsByID(ctx, list.ChatIDs)
		if err != nil {
			log.With(zap.Error(err)).Warn("Failure getting featured group records")
			return nil, status.Error(codes.Internal, "")
		}
	}

	featured, err := s.featuredGroupsMetadata(ctx, log, list.ChatIDs, records)
	if err != nil {
		log.With(zap.Error(err)).Warn("Failure hydrating featured groups")
		return nil, status.Error(codes.Internal, "")
	}
	return &chatpb.GetFeaturedGroupsResponse{
		Result:         chatpb.GetFeaturedGroupsResponse_OK,
		FeaturedGroups: featured,
	}, nil
}

// featuredGroupsMetadata projects a featured list, in its order, from the
// groups' records: each as a list view shows it, with no viewer (so nothing
// about anyone's place in the group) and no reading (so no messaging state),
// which is the same for every caller. A group with no record is left out.
//
// A private group is left out too. SetFeaturedGroups refuses one and
// IsPrivate never changes, so none is expected; the check is what keeps a
// record that broke that from reaching a caller with no standing to see it,
// and is logged when it does.
func (s *Server) featuredGroupsMetadata(ctx context.Context, log *zap.Logger, chatIDs []*commonpb.ChatId, records map[string]*Chat) ([]*chatpb.Metadata, error) {
	chats := make([]*Chat, 0, len(chatIDs))
	for _, chatID := range chatIDs {
		c, ok := records[string(chatID.Value)]
		if !ok {
			continue
		}
		if c.IsPrivate {
			log.Warn("Private group in featured groups", zap.String("chat_id", model.ChatIDString(chatID)))
			continue
		}
		chats = append(chats, c)
	}
	if len(chats) == 0 {
		return []*chatpb.Metadata{}, nil
	}
	return s.hydrate(ctx, nil, ListenerStanding{}, ReadingDenied, listDetail, chats)
}
