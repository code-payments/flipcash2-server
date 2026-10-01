package chat

import (
	"context"
	"errors"

	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	chatpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/chat/v1"
	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"
	profilepb "github.com/code-payments/flipcash2-protobuf-api/generated/go/profile/v1"

	"github.com/code-payments/flipcash2-server/model"
)

// Mention suggestions: a ranked pool of people a group member may want to
// @mention, which the client filters locally as they type. Today the pool is
// the group's most recent senders, most recent first, read from its activity
// records (see RecentSender): a fact about the message log, so people who
// have since left are suggested like anyone else, since a user cannot tell
// them apart from members. The server decides the pool's size, and neither
// the size nor the ranking is part of the contract.
//
// The read is a composer's, so it is gated on CanSpeak: a member who
// satisfies the group's rules, including its speaker rules, so a member of a
// creator-only group who is not its creator gets no suggestions. A DM has
// one candidate, the other member, and is refused before anything is read.
//
// The caller, users the caller has blocked, users without a username, and
// users the profile domain no longer knows are dropped after the read, so the
// read takes a few more records than the pool holds; when more than that are
// dropped the pool is simply smaller. A user who has blocked the caller is
// still suggested: the caller still sees their messages in the group, and a
// block governs what reaches the blocker, not what the caller may write.

const (
	// mentionSuggestionsSize is the most suggestions a response carries. It
	// is at most the max_items on GetMentionSuggestionsResponse.suggestions.
	mentionSuggestionsSize = 50

	// mentionSuggestionsSlack is how many records beyond the pool's size are
	// read, to absorb the ones filtered out.
	mentionSuggestionsSlack = 20
)

func (s *Server) GetMentionSuggestions(ctx context.Context, req *chatpb.GetMentionSuggestionsRequest) (*chatpb.GetMentionSuggestionsResponse, error) {
	userID, err := s.authz.Authorize(ctx, req, &req.Auth)
	if err != nil {
		return nil, err
	}

	log := s.log.With(zap.String("user_id", model.UserIDString(userID)))

	if !IsGroupChatID(req.ChatId) {
		return &chatpb.GetMentionSuggestionsResponse{Result: chatpb.GetMentionSuggestionsResponse_DENIED}, nil
	}

	// The canonical record first, so a group that does not exist is
	// NOT_FOUND rather than the DENIED a failed speaker gate would give it.
	c, err := s.chats.GetChatByID(ctx, req.ChatId)
	switch {
	case errors.Is(err, ErrChatNotFound):
		return &chatpb.GetMentionSuggestionsResponse{Result: chatpb.GetMentionSuggestionsResponse_NOT_FOUND}, nil
	case err != nil:
		log.With(zap.Error(err)).Warn("Failure getting chat")
		return nil, status.Error(codes.Internal, "")
	}

	canSpeak, err := s.access.CanSpeak(ctx, c.ID, userID)
	if err != nil {
		log.With(zap.Error(err)).Warn("Failure determining whether the caller may speak")
		return nil, status.Error(codes.Internal, "")
	}
	if !canSpeak {
		return &chatpb.GetMentionSuggestionsResponse{Result: chatpb.GetMentionSuggestionsResponse_DENIED}, nil
	}

	senders, err := s.chats.GetRecentSenders(ctx, c.ID, mentionSuggestionsSize+mentionSuggestionsSlack)
	if err != nil {
		log.With(zap.Error(err)).Warn("Failure getting recent senders")
		return nil, status.Error(codes.Internal, "")
	}

	suggestions, err := s.mentionSuggestions(ctx, userID, senders)
	if err != nil {
		log.With(zap.Error(err)).Warn("Failure building mention suggestions")
		return nil, status.Error(codes.Internal, "")
	}
	return &chatpb.GetMentionSuggestionsResponse{
		Result:      chatpb.GetMentionSuggestionsResponse_OK,
		Suggestions: suggestions,
	}, nil
}

// mentionSuggestions turns a group's recent senders into the viewer's
// suggestions, in the senders' order: the filters described above, then at
// most mentionSuggestionsSize of what remains, each with its public profile.
// The block read and the profile read are independent and run concurrently.
func (s *Server) mentionSuggestions(ctx context.Context, viewerID *commonpb.UserId, senders []RecentSender) ([]*chatpb.MentionSuggestion, error) {
	candidates := make([]RecentSender, 0, len(senders))
	candidateIDs := make([]*commonpb.UserId, 0, len(senders))
	for _, sender := range senders {
		if string(sender.UserID.Value) == string(viewerID.Value) {
			continue
		}
		candidates = append(candidates, sender)
		candidateIDs = append(candidateIDs, sender.UserID)
	}
	out := make([]*chatpb.MentionSuggestion, 0, min(len(candidates), mentionSuggestionsSize))
	if len(candidates) == 0 {
		return out, nil
	}

	var (
		blocked        map[string]bool
		publicProfiles map[string]*profilepb.UserProfile
	)
	g, gctx := errgroup.WithContext(ctx)
	g.Go(func() (err error) {
		blocked, err = s.blocklist.GetBlocked(gctx, viewerID, candidateIDs)
		return err
	})
	g.Go(func() (err error) {
		publicProfiles, err = s.profiles.GetPublicProfiles(gctx, candidateIDs)
		return err
	})
	if err := g.Wait(); err != nil {
		return nil, err
	}

	for _, candidate := range candidates {
		if len(out) == mentionSuggestionsSize {
			break
		}
		key := string(candidate.UserID.Value)
		if blocked[key] {
			continue
		}
		// A user the profile domain no longer knows, or one without a
		// username, cannot be mentioned.
		publicProfile, ok := publicProfiles[key]
		if !ok || publicProfile.GetUsername().GetValue() == "" {
			continue
		}
		out = append(out, &chatpb.MentionSuggestion{
			// One proto per user is shared across callers (see
			// ProfileReader.GetPublicProfiles), so the response carries a copy.
			UserProfile: proto.Clone(publicProfile).(*profilepb.UserProfile),
			LastSentAt:  timestamppb.New(candidate.LastSentAt),
		})
	}
	return out, nil
}
