package poll

import (
	"context"
	"encoding/hex"
	"errors"
	"strings"
	"time"

	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"
	messagingpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/messaging/v1"
	moderationpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/moderation/v1"
	pollpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/poll/v1"

	ocp_data "github.com/code-payments/ocp-server/ocp/data"

	"github.com/code-payments/flipcash2-server/auth"
	"github.com/code-payments/flipcash2-server/chat"
	"github.com/code-payments/flipcash2-server/model"
	"github.com/code-payments/flipcash2-server/moderation"
)

// getPollsChatConcurrency bounds how many chats GetPolls checks the caller's
// standing in at once. A page of messages is nearly always one chat's, so
// this matters only for a request that mixes many.
const getPollsChatConcurrency = 8

// Server implements pollpb.PollServer.
//
// Who may do what is the chat's to say, through the shared chat.Access:
//
//   - Creating a poll is speaking in the chat (Access.SpeakerStanding), and
//     the standing also says whether the chat takes the poll's text in
//     plaintext (a public group) or encrypted under the chat key (a keyed
//     private group).
//   - Seeing a poll is reading the chat in full (Access.CanListen): its
//     members, and a non-member of a public group who satisfies its listener
//     rules. An unsent poll is its creator's alone (see Poll.Live).
//   - Voting is a member's alone (Access.IsMember), whatever the chat's
//     speaker rules: a member who cannot send in the chat still votes.
type Server struct {
	log *zap.Logger

	authz auth.Authorizer

	polls     Store
	access    *chat.Access
	moderator moderation.Client
	ocpData   ocp_data.Provider

	now func() time.Time

	pollpb.UnimplementedPollServer
}

// ServerOption configures a Server at construction beyond its required
// dependencies.
type ServerOption func(*Server)

// WithClock overrides the clock the server decides a poll's status by, for
// tests.
func WithClock(now func() time.Time) ServerOption {
	return func(s *Server) {
		s.now = now
	}
}

// NewServer returns a Poll service over polls. access is the chat.Access the
// chat and messaging servers share, so a poll is gated by exactly what gates
// the chat it belongs to.
func NewServer(
	log *zap.Logger,

	authz auth.Authorizer,

	polls Store,
	access *chat.Access,
	moderator moderation.Client,
	ocpData ocp_data.Provider,

	opts ...ServerOption,
) *Server {
	s := &Server{
		log: log,

		authz: authz,

		polls:     polls,
		access:    access,
		moderator: moderator,
		ocpData:   ocpData,

		now: time.Now,
	}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

// CreatePoll creates a poll for a group chat.
//
// The request is checked in the order a client can act on: the chat must be a
// group (CHAT_NOT_SUPPORTED); the caller must be able to speak in it
// (DENIED); the text must be in the form the chat takes, plaintext in a
// public group and encrypted under the chat key in a private one
// (ENCRYPTION_NOT_ALLOWED, ENCRYPTION_REQUIRED); plaintext must carry as many
// options as the definition counts (OPTION_COUNT_MISMATCH); the poll must end
// between MinDuration and MaxDuration from now (INVALID_ENDS_AT); a BALANCE
// poll's parameters must name a mint and a currency the tally can value, and
// a COUNT poll must carry none (INVALID_BALANCE_PARAMETERS); and plaintext
// must pass moderation (TEXT_MODERATED, see moderate). Only then is
// anything written. Local checks
// come first, the currency lookup next, and moderation, a call out, last.
//
// A poll in a private group is live from creation, since the server cannot
// see the message that sends it; any other is unsent until its creator sends
// it (see Poll.Live).
//
// The RPC is retry-safe. The poll's ID is derived from the caller and the
// request's idempotency key (see MustDerivePollID), so a retry names the same
// poll, and one that already exists is answered from the store before any
// check runs, whatever parameters the retry carries. A retry that loses a
// race with its twin is caught by the store's uniqueness condition and
// answered the same way.
func (s *Server) CreatePoll(ctx context.Context, req *pollpb.CreatePollRequest) (*pollpb.CreatePollResponse, error) {
	userID, err := s.authz.Authorize(ctx, req, &req.Auth)
	if err != nil {
		return nil, err
	}

	log := s.log.With(
		zap.String("user_id", model.UserIDString(userID)),
		zap.String("chat_id", hex.EncodeToString(req.GetChatId().GetValue())),
	)

	definition := req.Definition

	// A DM is told apart by its ID's length, before anything is read.
	if !chat.IsGroupChatID(req.ChatId) {
		return &pollpb.CreatePollResponse{Result: pollpb.CreatePollResponse_CHAT_NOT_SUPPORTED}, nil
	}

	// Validation requires the key at its fixed width, which MustDerivePollID
	// relies on.
	pollID := MustDerivePollID(userID, req.IdempotencyKey)
	log = log.With(zap.String("poll_id", hex.EncodeToString(pollID.Value)))

	existing, err := s.polls.GetPoll(ctx, pollID)
	switch {
	case err == nil:
		return s.replayCreatePoll(ctx, log, userID, existing)
	case !errors.Is(err, ErrPollNotFound):
		log.With(zap.Error(err)).Warn("Failure getting poll")
		return nil, status.Error(codes.Internal, "")
	}

	speaker, err := s.access.SpeakerStanding(ctx, req.ChatId, userID)
	if err != nil {
		log.With(zap.Error(err)).Warn("Failure determining speaker standing")
		return nil, status.Error(codes.Internal, "")
	}
	if !speaker.CanSpeak {
		return &pollpb.CreatePollResponse{Result: pollpb.CreatePollResponse_DENIED}, nil
	}

	if result, ok := textVerdict(speaker, definition); !ok {
		return &pollpb.CreatePollResponse{Result: result}, nil
	}

	if text := definition.GetPlaintext(); text != nil && uint32(len(text.Options)) != definition.OptionCount {
		return &pollpb.CreatePollResponse{Result: pollpb.CreatePollResponse_OPTION_COUNT_MISMATCH}, nil
	}

	now := s.now()
	if untilEnd := definition.EndsAt.AsTime().Sub(now); untilEnd < MinDuration || untilEnd > MaxDuration {
		return &pollpb.CreatePollResponse{Result: pollpb.CreatePollResponse_INVALID_ENDS_AT}, nil
	}

	validParameters, err := s.validBalanceParameters(ctx, definition)
	if err != nil {
		log.With(zap.Error(err)).Warn("Failure checking balance parameters")
		return nil, status.Error(codes.Internal, "")
	}
	if !validParameters {
		return &pollpb.CreatePollResponse{Result: pollpb.CreatePollResponse_INVALID_BALANCE_PARAMETERS}, nil
	}

	if text := definition.GetPlaintext(); text != nil {
		result, category, err := s.moderate(ctx, log, text)
		if err != nil {
			return nil, status.Error(codes.Internal, "")
		}
		if result != pollpb.CreatePollResponse_OK {
			return &pollpb.CreatePollResponse{
				Result:          result,
				FlaggedCategory: category,
			}, nil
		}
	}

	p := &Poll{
		ID:         pollID,
		ChatID:     proto.Clone(req.ChatId).(*commonpb.ChatId),
		CreatorID:  userID,
		Definition: proto.Clone(definition).(*pollpb.PollDefinition),
		CreatedAt:  now,
		// Only a keyed private group takes encrypted content, and its polls are
		// live from creation (see Poll.Live).
		Live:   speaker.Encryption == chat.EncryptionRequired,
		Status: StatusOpen,
	}
	err = s.polls.CreatePoll(ctx, p)
	switch {
	case errors.Is(err, ErrPollExists):
		// A concurrent retry won the write since the read above. Its poll is
		// this request's poll, so answer with it.
		existing, err := s.polls.GetPoll(ctx, pollID)
		if errors.Is(err, ErrPollNotFound) {
			// The first attempt's poll was never sent and has ended, so it is
			// gone, but its ID is still held until the store deletes it. The key
			// is spent: the client retries with a new one.
			return nil, status.Error(codes.FailedPrecondition, "idempotency key already used")
		}
		if err != nil {
			log.With(zap.Error(err)).Warn("Failure getting poll created by concurrent request")
			return nil, status.Error(codes.Internal, "")
		}
		return s.replayCreatePoll(ctx, log, userID, existing)
	case err != nil:
		log.With(zap.Error(err)).Warn("Failure creating poll")
		return nil, status.Error(codes.Internal, "")
	}

	return &pollpb.CreatePollResponse{
		Result: pollpb.CreatePollResponse_OK,
		Poll:   p.ToProto(now, nil),
	}, nil
}

// replayCreatePoll answers a CreatePoll whose poll p already exists: an earlier
// attempt by the same caller (the ID embeds them) created it, and this request
// is a retry. The response is the poll as GetPoll would show its creator now,
// with no standing checked: the poll's existence was decided when it was
// created.
func (s *Server) replayCreatePoll(ctx context.Context, log *zap.Logger, userID *commonpb.UserId, p *Poll) (*pollpb.CreatePollResponse, error) {
	votes, err := s.polls.GetVotes(ctx, userID, []*pollpb.PollId{p.ID})
	if err != nil {
		log.With(zap.Error(err)).Warn("Failure getting self vote")
		return nil, status.Error(codes.Internal, "")
	}
	return &pollpb.CreatePollResponse{
		Result: pollpb.CreatePollResponse_OK,
		Poll:   p.ToProto(s.now(), votes[string(p.ID.Value)]),
	}, nil
}

// textVerdict judges a definition's text against what the chat takes, as the
// speaker's standing says (see chat.SpeakerStanding): a public group takes
// plaintext alone, and a keyed private group text encrypted under its scheme
// alone. ok is false, with the result to answer, when the chat does not take
// the text.
func textVerdict(speaker chat.SpeakerStanding, definition *pollpb.PollDefinition) (result pollpb.CreatePollResponse_Result, ok bool) {
	switch speaker.Encryption {
	case chat.EncryptionNone:
		if definition.GetPlaintext() == nil {
			return pollpb.CreatePollResponse_ENCRYPTION_NOT_ALLOWED, false
		}
		return pollpb.CreatePollResponse_OK, true
	case chat.EncryptionRequired:
		encrypted := definition.GetEncrypted()
		if encrypted == nil {
			return pollpb.CreatePollResponse_ENCRYPTION_REQUIRED, false
		}
		if scheme, ok := pollTextScheme(speaker.Scheme); !ok || encrypted.Scheme != scheme {
			return pollpb.CreatePollResponse_ENCRYPTION_NOT_ALLOWED, false
		}
		return pollpb.CreatePollResponse_OK, true
	default:
		// Only a DM takes optional encryption, and a DM is refused by its ID
		// before any standing is asked for.
		return pollpb.CreatePollResponse_CHAT_NOT_SUPPORTED, false
	}
}

// pollTextScheme is the EncryptedPollText scheme that corresponds to a chat's
// content scheme: poll text in a private group is encrypted with the same
// chat key as its messages.
func pollTextScheme(scheme messagingpb.EncryptedContent_Scheme) (pollpb.EncryptedPollText_Scheme, bool) {
	switch scheme {
	case messagingpb.EncryptedContent_CHAT_KEY_XCHACHA20POLY1305:
		return pollpb.EncryptedPollText_CHAT_KEY_XCHACHA20POLY1305, true
	default:
		return pollpb.EncryptedPollText_UNKNOWN, false
	}
}

// validBalanceParameters reports whether a definition's balance parameters fit
// its mode: none for a COUNT poll, and for a BALANCE poll a mint and a
// currency the tally can value (see isSupportedMint and isSupportedCurrency).
func (s *Server) validBalanceParameters(ctx context.Context, definition *pollpb.PollDefinition) (bool, error) {
	parameters := definition.GetBalanceParameters()
	switch definition.GetMode() {
	case pollpb.Mode_COUNT:
		return parameters == nil, nil
	case pollpb.Mode_BALANCE:
		if parameters == nil {
			return false, nil
		}
		supported, err := isSupportedMint(ctx, s.ocpData, parameters.Mint)
		if err != nil || !supported {
			return false, err
		}
		return isSupportedCurrency(parameters.Currency), nil
	default:
		return false, nil
	}
}

// moderate runs a poll's plaintext through the general text classifier as one
// text, the question and then each option on a line of its own, and returns
// the result to answer: OK, or TEXT_MODERATED and the best-fit category.
//
// One text costs one call however many options there are, and gives the
// classifier the context an option alone ("Yes", "3", an emoji) lacks.
//
// Any failure to classify is an error, and nothing is created. That includes
// ErrUnsupportedLanguage, as for a group description (see
// chat.Server.moderateDescription): there is no second classifier to cover
// the text, so a language the classifier cannot score is refused rather than
// posted unmoderated.
func (s *Server) moderate(ctx context.Context, log *zap.Logger, text *pollpb.PollText) (pollpb.CreatePollResponse_Result, moderationpb.FlaggedCategory, error) {
	result, err := s.moderator.ClassifyText(ctx, moderationText(text))
	if err != nil {
		log.With(zap.Error(err)).Warn("Failure classifying poll text")
		return pollpb.CreatePollResponse_OK, moderationpb.FlaggedCategory_NONE, err
	}
	if result == nil || !result.Flagged {
		return pollpb.CreatePollResponse_OK, moderationpb.FlaggedCategory_NONE, nil
	}
	log.With(zap.String("text", moderationText(text)), zap.Strings("categories", result.FlaggedCategories)).Info("Poll text is flagged")
	return pollpb.CreatePollResponse_TEXT_MODERATED, moderation.HighestFlaggedCategory(result), nil
}

// moderationText is the one text a poll's plaintext is moderated as: the
// question, then each option, a line each.
func moderationText(text *pollpb.PollText) string {
	lines := make([]string, 0, 1+len(text.Options))
	lines = append(lines, text.Question)
	for _, option := range text.Options {
		lines = append(lines, option.Text)
	}
	return strings.Join(lines, "\n")
}

// GetPoll returns a poll as the caller sees it, with their own vote.
//
// A poll that does not exist, and an unsent one to anyone but its creator, is
// NOT_FOUND, checked before the caller's standing so a non-member learns
// nothing of a poll that is not live. Otherwise the caller must be able to
// read the poll's chat in full (DENIED). Results are carried once the poll is
// closed (see Poll.StateProto).
func (s *Server) GetPoll(ctx context.Context, req *pollpb.GetPollRequest) (*pollpb.GetPollResponse, error) {
	userID, err := s.authz.Authorize(ctx, req, &req.Auth)
	if err != nil {
		return nil, err
	}

	log := s.log.With(
		zap.String("user_id", model.UserIDString(userID)),
		zap.String("poll_id", hex.EncodeToString(req.GetPollId().GetValue())),
	)

	p, err := s.polls.GetPoll(ctx, req.PollId)
	switch {
	case errors.Is(err, ErrPollNotFound):
		return &pollpb.GetPollResponse{Result: pollpb.GetPollResponse_NOT_FOUND}, nil
	case err != nil:
		log.With(zap.Error(err)).Warn("Failure getting poll")
		return nil, status.Error(codes.Internal, "")
	}
	if !visibleTo(p, userID) {
		return &pollpb.GetPollResponse{Result: pollpb.GetPollResponse_NOT_FOUND}, nil
	}

	canListen, err := s.access.CanListen(ctx, p.ChatID, userID)
	if err != nil {
		log.With(zap.Error(err)).Warn("Failure determining listener standing")
		return nil, status.Error(codes.Internal, "")
	}
	if !canListen {
		return &pollpb.GetPollResponse{Result: pollpb.GetPollResponse_DENIED}, nil
	}

	votes, err := s.polls.GetVotes(ctx, userID, []*pollpb.PollId{p.ID})
	if err != nil {
		log.With(zap.Error(err)).Warn("Failure getting self vote")
		return nil, status.Error(codes.Internal, "")
	}

	return &pollpb.GetPollResponse{
		Result: pollpb.GetPollResponse_OK,
		Poll:   p.ToProto(s.now(), votes[string(p.ID.Value)]),
	}, nil
}

// GetPolls is the batch form of GetPoll. Every poll GetPoll would not answer
// OK is left out, and the caller's standing is asked once per chat the polls
// belong to, not once per poll.
func (s *Server) GetPolls(ctx context.Context, req *pollpb.GetPollsRequest) (*pollpb.GetPollsResponse, error) {
	userID, err := s.authz.Authorize(ctx, req, &req.Auth)
	if err != nil {
		return nil, err
	}

	log := s.log.With(zap.String("user_id", model.UserIDString(userID)))

	stored, err := s.polls.GetPolls(ctx, req.PollIds)
	if err != nil {
		log.With(zap.Error(err)).Warn("Failure getting polls")
		return nil, status.Error(codes.Internal, "")
	}

	var (
		visible []*Poll
		chatIDs []*commonpb.ChatId
		inChats = make(map[string]bool)
	)
	for _, p := range stored {
		if !visibleTo(p, userID) {
			continue
		}
		visible = append(visible, p)
		if !inChats[string(p.ChatID.Value)] {
			inChats[string(p.ChatID.Value)] = true
			chatIDs = append(chatIDs, p.ChatID)
		}
	}

	listens := make([]bool, len(chatIDs))
	eg, egCtx := errgroup.WithContext(ctx)
	eg.SetLimit(getPollsChatConcurrency)
	for i, chatID := range chatIDs {
		eg.Go(func() error {
			canListen, err := s.access.CanListen(egCtx, chatID, userID)
			listens[i] = canListen
			return err
		})
	}
	if err := eg.Wait(); err != nil {
		log.With(zap.Error(err)).Warn("Failure determining listener standing")
		return nil, status.Error(codes.Internal, "")
	}
	listenable := make(map[string]bool, len(chatIDs))
	for i, chatID := range chatIDs {
		listenable[string(chatID.Value)] = listens[i]
	}

	var (
		shown   []*Poll
		shownID []*pollpb.PollId
	)
	for _, p := range visible {
		if listenable[string(p.ChatID.Value)] {
			shown = append(shown, p)
			shownID = append(shownID, p.ID)
		}
	}

	votes, err := s.polls.GetVotes(ctx, userID, shownID)
	if err != nil {
		log.With(zap.Error(err)).Warn("Failure getting self votes")
		return nil, status.Error(codes.Internal, "")
	}

	now := s.now()
	resp := &pollpb.GetPollsResponse{Result: pollpb.GetPollsResponse_OK}
	for _, p := range shown {
		resp.Polls = append(resp.Polls, p.ToProto(now, votes[string(p.ID.Value)]))
	}
	return resp, nil
}

// Vote replaces the caller's selection in an open poll.
//
// A poll that does not exist or is not live is NOT_FOUND, and the caller must
// be a member of its chat (DENIED): membership alone, never the chat's rules.
// A poll that has ended is POLL_CLOSED, carrying its state, before the
// selection is looked at, since no selection would be taken. The selection
// must name options the poll has, and only one unless it allows several
// (INVALID_SELECTION); an empty one removes the caller's vote.
//
// The vote is accepted as of the clock read that found the poll open, and
// its write must land within VoteWriteTimeout of it, or it is abandoned and
// the RPC fails: that bound is what lets the tally count every accepted vote
// without the poll's item being part of each (see TallyDelay). The bound is a
// context timeout, so it runs on the real clock from just after the read,
// whatever clock the server decides status by (see WithClock).
func (s *Server) Vote(ctx context.Context, req *pollpb.VoteRequest) (*pollpb.VoteResponse, error) {
	userID, err := s.authz.Authorize(ctx, req, &req.Auth)
	if err != nil {
		return nil, err
	}

	log := s.log.With(
		zap.String("user_id", model.UserIDString(userID)),
		zap.String("poll_id", hex.EncodeToString(req.GetPollId().GetValue())),
	)

	p, err := s.polls.GetPoll(ctx, req.PollId)
	switch {
	case errors.Is(err, ErrPollNotFound):
		return &pollpb.VoteResponse{Result: pollpb.VoteResponse_NOT_FOUND}, nil
	case err != nil:
		log.With(zap.Error(err)).Warn("Failure getting poll")
		return nil, status.Error(codes.Internal, "")
	}
	if !p.Live {
		return &pollpb.VoteResponse{Result: pollpb.VoteResponse_NOT_FOUND}, nil
	}

	isMember, err := s.access.IsMember(ctx, p.ChatID, userID)
	if err != nil {
		log.With(zap.Error(err)).Warn("Failure checking membership")
		return nil, status.Error(codes.Internal, "")
	}
	if !isMember {
		return &pollpb.VoteResponse{Result: pollpb.VoteResponse_DENIED}, nil
	}

	now := s.now()
	if !p.IsOpenAt(now) {
		votes, err := s.polls.GetVotes(ctx, userID, []*pollpb.PollId{p.ID})
		if err != nil {
			log.With(zap.Error(err)).Warn("Failure getting self vote")
			return nil, status.Error(codes.Internal, "")
		}
		return &pollpb.VoteResponse{
			Result: pollpb.VoteResponse_POLL_CLOSED,
			State:  p.StateProto(now, votes[string(p.ID.Value)]),
		}, nil
	}

	if !validSelection(p.Definition, req.OptionIndices) {
		return &pollpb.VoteResponse{Result: pollpb.VoteResponse_INVALID_SELECTION}, nil
	}

	writeCtx, cancel := context.WithTimeout(ctx, VoteWriteTimeout)
	defer cancel()
	if err := s.polls.SetVote(writeCtx, p.ID, userID, req.OptionIndices, now); err != nil {
		log.With(zap.Error(err)).Warn("Failure setting vote")
		return nil, status.Error(codes.Internal, "")
	}

	return &pollpb.VoteResponse{
		Result: pollpb.VoteResponse_OK,
		State:  p.StateProto(now, NormalizeSelection(req.OptionIndices)),
	}, nil
}

// validSelection reports whether options is a selection the poll takes: each
// an option it has, and no more than one unless it allows multiple choices.
// An empty selection, which removes a vote, is valid. That no option repeats
// is the request's validation.
func validSelection(definition *pollpb.PollDefinition, options []uint32) bool {
	if len(options) > 1 && !definition.GetAllowsMultipleChoices() {
		return false
	}
	for _, option := range options {
		if option >= definition.GetOptionCount() {
			return false
		}
	}
	return true
}

// visibleTo reports whether userID may know the poll exists at all: anyone
// may know a live poll, standing permitting, and only its creator an unsent
// one.
func visibleTo(p *Poll, userID *commonpb.UserId) bool {
	return p.Live || proto.Equal(p.CreatorID, userID)
}
