package chat

import (
	"bytes"
	"context"
	"errors"
	"sort"
	"time"

	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	chatpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/chat/v1"
	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"
	profilepb "github.com/code-payments/flipcash2-protobuf-api/generated/go/profile/v1"

	"github.com/code-payments/flipcash2-server/model"
)

// A sample of a public group's chatters: a short list of its members to show,
// the creator first while they are a member, then the most active members who
// have sent, by activity score (see NextActivityScore) leaning toward
// recency (see sampleRank): a member who shows up every day ranks above one
// who sent once a little more recently, but of two with close scores the
// more recent sender comes first. It is how a group
// shows who is in it without listing members who only read: everyone in it
// is a member as of the read, but a member who has not sent recently is not
// in it.
//
// The candidates are the group's most active senders, read from its activity
// records (see Store.GetActiveSenders), which name people who have since left
// as well as members. So each candidate's membership is checked (see
// Store.GetGroupMembersByID), in chunks in candidate order, stopping as
// soon as one more member than the sample holds is found: that one proves
// has_more without being returned. Each chunk is as many candidates as
// members still to find, plus sampleChattersCheckBuffer, so a group whose
// most active senders are all still members is answered by one read, and a
// later read checks only about as many as are still missing. The read of
// senders is bounded too; when it fills and too few of the senders are
// still members, has_more is set,
// since the server stopped looking before it could tell. The check is
// eventually consistent: it decides who is shown, not who may do anything,
// so a member who left a moment ago appearing, or one who joined a moment
// ago missing, costs nothing a refetch does not fix, and the read costs half
// as much.
//
// A member who sent within the last minute may trail their latest message
// (see ActivityRecordInterval), and one whose activity record has expired
// (see ActivityRetention) is not a candidate until they send again.
//
// The creator is a candidate whether or not they have sent, and is shown at
// their own last send whenever they have a record, however many others rank
// above them: one outside the senders read is looked up directly (see
// Store.GetLastSentAt), only when they are shown and the read of senders
// filled, since otherwise it holds every record the group has.
//
// Only a public group has a sample. A DM is refused before anything is read,
// and a private group after its record, whoever asks: a private group's
// members are shown to no one through it.
//
// A public group's sample is public: unlike the roster (see GetRoster), which
// is a member's alone, it is part of how the group presents itself, as its
// title and pictures are, so it is returned to anyone who asks, member or
// not, with or without auth. Auth, when set, is verified as everywhere else,
// but changes nothing: the sample is the same for every caller, the caller's
// own place in it included, which is why their membership is checked like
// anyone's.

const (
	// sampleChattersSize is the most chatters a sample carries. It is at most
	// the max_items on SampleChattersResponse.chatters.
	sampleChattersSize = 20

	// sampleChattersSenderWindow is how many of a group's most active
	// senders are candidates for the sample.
	sampleChattersSenderWindow = 100

	// sampleChattersCheckBuffer is how many candidates beyond the members
	// still to find are checked per read, to absorb a few who have left
	// without another read.
	sampleChattersCheckBuffer = 5

	// sampleChattersRecencyWeight is how far a sender's rank leans from
	// their activity score toward their last send (see sampleRank): 0 ranks
	// by score alone, 1 by recency alone.
	sampleChattersRecencyWeight = 0.2
)

// sampleRank is the key a sender is ranked by in a sample, highest first:
// their activity score less sampleChattersRecencyWeight of its lead over
// their last send. Comparing two senders, the more recent one ranks first
// exactly when the gap between their last sends is more than
// (1 − w) / w times the gap between their scores (4 times at w = 0.2): close
// scores go to the more recent sender, distant ones to the more active. It
// only reorders the senders read, which are the most active by score alone,
// so a sender just outside that read cannot be ranked in by recency; the
// read is five times the sample, so one that would is far down it.
func sampleRank(sender RecentSender) time.Time {
	lead := sender.ActivityScore.Sub(sender.LastSentAt)
	return sender.ActivityScore.Add(-time.Duration(sampleChattersRecencyWeight * float64(lead)))
}

func (s *Server) SampleChatters(ctx context.Context, req *chatpb.SampleChattersRequest) (*chatpb.SampleChattersResponse, error) {
	log := s.log.With(zap.String("chat_id", model.ChatIDString(req.ChatId)))
	if req.Auth != nil {
		userID, err := s.authz.Authorize(ctx, req, &req.Auth)
		if err != nil {
			return nil, err
		}
		log = log.With(zap.String("user_id", model.UserIDString(userID)))
	}

	if !IsGroupChatID(req.ChatId) {
		return &chatpb.SampleChattersResponse{Result: chatpb.SampleChattersResponse_DENIED}, nil
	}

	c, err := s.chats.GetChatByID(ctx, req.ChatId)
	switch {
	case errors.Is(err, ErrChatNotFound):
		return &chatpb.SampleChattersResponse{Result: chatpb.SampleChattersResponse_NOT_FOUND}, nil
	case err != nil:
		log.With(zap.Error(err)).Warn("Failure getting chat")
		return nil, status.Error(codes.Internal, "")
	}
	if c.IsPrivate {
		return &chatpb.SampleChattersResponse{Result: chatpb.SampleChattersResponse_DENIED}, nil
	}

	chatters, hasMore, err := s.sampleChatters(ctx, c)
	if err != nil {
		log.With(zap.Error(err)).Warn("Failure sampling chatters")
		return nil, status.Error(codes.Internal, "")
	}
	return &chatpb.SampleChattersResponse{
		Result:   chatpb.SampleChattersResponse_OK,
		Chatters: chatters,
		HasMore:  hasMore,
	}, nil
}

// sampleCandidate is one user who may be in a sample, in sample order.
type sampleCandidate struct {
	userID     *commonpb.UserId
	lastSentAt time.Time // zero for a creator outside the senders read
	isCreator  bool
}

// sampleChatters builds the sample of the public group c, as described above.
func (s *Server) sampleChatters(ctx context.Context, c *Chat) ([]*chatpb.SampledChatter, bool, error) {
	senders, err := s.chats.GetActiveSenders(ctx, c.ID, sampleChattersSenderWindow)
	if err != nil {
		return nil, false, err
	}
	// By rank, and of two with the same rank, the more recent sender first.
	sort.SliceStable(senders, func(i, j int) bool {
		ri, rj := sampleRank(senders[i]), sampleRank(senders[j])
		if !ri.Equal(rj) {
			return ri.After(rj)
		}
		return senders[i].LastSentAt.After(senders[j].LastSentAt)
	})

	// The creator first, at their send time if the senders read holds it
	// (otherwise looked up below), then every other sender in rank order.
	candidates := make([]sampleCandidate, 0, len(senders)+1)
	if c.CreatorID != nil {
		creator := sampleCandidate{userID: c.CreatorID, isCreator: true}
		for _, sender := range senders {
			if isSameUser(sender.UserID, c.CreatorID) {
				creator.lastSentAt = sender.LastSentAt
				break
			}
		}
		candidates = append(candidates, creator)
	}
	for _, sender := range senders {
		if c.CreatorID != nil && isSameUser(sender.UserID, c.CreatorID) {
			continue
		}
		candidates = append(candidates, sampleCandidate{userID: sender.UserID, lastSentAt: sender.LastSentAt})
	}

	// Members in candidate order, until one more than the sample holds.
	members := make([]sampleCandidate, 0, sampleChattersSize+1)
	for start := 0; start < len(candidates) && len(members) <= sampleChattersSize; {
		needed := sampleChattersSize + 1 - len(members)
		end := min(start+needed+sampleChattersCheckBuffer, len(candidates))
		chunk := candidates[start:end]
		start = end

		toCheck := make([]*commonpb.UserId, len(chunk))
		for i, candidate := range chunk {
			toCheck[i] = candidate.userID
		}
		joined, err := s.chats.GetGroupMembersByID(ctx, c.ID, toCheck)
		if err != nil {
			return nil, false, err
		}

		for _, candidate := range chunk {
			if len(members) > sampleChattersSize {
				break
			}
			if _, ok := joined[string(candidate.userID.Value)]; ok {
				members = append(members, candidate)
			}
		}
	}

	sendersFull := len(senders) >= sampleChattersSenderWindow
	hasMore := len(members) > sampleChattersSize || sendersFull
	if len(members) > sampleChattersSize {
		members = members[:sampleChattersSize]
	}
	if len(members) == 0 {
		return []*chatpb.SampledChatter{}, hasMore, nil
	}

	// A shown creator outside the senders read may still have a record, once
	// enough others have sent since; a read that did not fill holds every
	// record, so there is nothing more to find.
	if creator := &members[0]; creator.isCreator && creator.lastSentAt.IsZero() && sendersFull {
		lastSentAt, ok, err := s.chats.GetLastSentAt(ctx, c.ID, creator.userID)
		if err != nil {
			return nil, false, err
		}
		if ok {
			creator.lastSentAt = lastSentAt
		}
	}

	userIDs := make([]*commonpb.UserId, len(members))
	for i, member := range members {
		userIDs[i] = member.userID
	}
	publicProfiles, err := s.profiles.GetLimitedPublicProfilesForRow(ctx, userIDs)
	if err != nil {
		return nil, false, err
	}

	out := make([]*chatpb.SampledChatter, len(members))
	for i, member := range members {
		// Every member is a user the profile domain knows (see ProfileReader).
		publicProfile, ok := publicProfiles[string(member.userID.Value)]
		if !ok {
			return nil, false, errors.New("missing public profile for member " + model.UserIDString(member.userID))
		}
		chatter := &chatpb.SampledChatter{
			// One proto per user is shared across callers (see
			// ProfileReader.GetLimitedPublicProfilesForRow), so the response
			// carries a copy.
			UserProfile: proto.Clone(publicProfile).(*profilepb.UserProfile),
			IsCreator:   member.isCreator,
		}
		if !member.lastSentAt.IsZero() {
			chatter.LastSentAt = timestamppb.New(member.lastSentAt)
		}
		out[i] = chatter
	}
	return out, hasMore, nil
}

// isSameUser reports whether a and b name the same user.
func isSameUser(a, b *commonpb.UserId) bool {
	return bytes.Equal(a.GetValue(), b.GetValue())
}
