package chat

import (
	"context"
	"errors"
	"time"

	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	chatpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/chat/v1"
	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"
	messagingpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/messaging/v1"
	moderationpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/moderation/v1"

	"github.com/code-payments/flipcash2-server/blob"
	"github.com/code-payments/flipcash2-server/model"
	"github.com/code-payments/flipcash2-server/moderation"
)

// StartChat creates a group chat with the caller as its first and only member.
//
// The request is checked in the order a client can act on: the rules it asks
// for must be ones a group can carry — which today means they must include a
// minimum listener balance (see RulesFromProto) — and ones the caller
// satisfies (RULES_NOT_SATISFIED); the title must pass moderation
// (TITLE_MODERATED); and the picture, if any, must be a READY image the caller
// owns (PICTURE_BLOB_NOT_ACCEPTED). Only then is anything written. Rule checks
// come first because they are local reads, moderation next because it is a
// call out, and the picture last because attaching it grants read access —
// against a chat ID minted for the purpose — and that grant, though harmless
// against a chat that is never created, is best made only once everything
// else has passed.
//
// The picture is attached before the record is written, so the group never
// exists without its picture readable: a client that read the blob id from the
// metadata but could not fetch the blob would render a broken image. The
// creation itself is a single PutChat; it does not by itself make the creator
// anything other than a member (see Chat.CreatorID).
//
// Like JoinChat, a creation is a join, and is announced to the creator's other
// devices as one — a MemberJoined on their user topic carrying the metadata —
// so they insert the new chat without a refetch. There is no one else to
// tell.
//
// The RPC is retry-safe. The group's ID is derived from the caller and the
// request's idempotency key (see MustDeriveGroupChatID), so a retry names the
// same group, and one that already exists is answered from its record before
// any check runs: a title the moderator has since learned to flag, a balance
// that has since fallen below the minimum, or a staff gate since closed are
// facts about a new group, not this one. Which parameters the retry carries
// does not matter either; the key is the request's identity. A retry that
// loses a race with its twin — both pass the read, one write lands — is caught
// by the store's uniqueness condition and answered the same way. Nothing is
// published for a retry: the creation was announced when it happened.
//
// The RPC shares the membership RPCs' staff gate (see
// requireStaffForGroupManagementRPC).
func (s *Server) StartChat(ctx context.Context, req *chatpb.StartChatRequest) (*chatpb.StartChatResponse, error) {
	userID, err := s.authz.Authorize(ctx, req, &req.Auth)
	if err != nil {
		return nil, err
	}

	log := s.log.With(zap.String("user_id", model.UserIDString(userID)))

	// Validation requires the oneof to be set, and GROUP is its only variant, so
	// anything else here is a proto this server predates.
	params := req.GetGroup()
	if params == nil {
		return nil, status.Error(codes.InvalidArgument, "unsupported chat parameters")
	}

	// Validation also requires the key, at its fixed width; this is the check
	// MustDeriveGroupChatID relies on.
	if len(req.GetIdempotencyKey().GetValue()) != IdempotencyKeySize {
		return nil, status.Error(codes.InvalidArgument, "idempotency key is required")
	}
	chatID := MustDeriveGroupChatID(userID, req.IdempotencyKey)

	existing, err := s.chats.GetChatByID(ctx, chatID)
	switch {
	case err == nil:
		return s.replayStartChat(ctx, log, userID, existing)
	case !errors.Is(err, ErrChatNotFound):
		log.With(zap.Error(err)).Warn("Failure getting chat")
		return nil, status.Error(codes.Internal, "")
	}

	allowed, err := s.requireStaffForGroupManagementRPC(ctx, log, userID)
	if err != nil {
		return nil, err
	}
	if !allowed {
		return &chatpb.StartChatResponse{Result: chatpb.StartChatResponse_DENIED}, nil
	}

	isStaffOnly, minimumListenerBalance, err := RulesFromProto(params.Rules)
	if err != nil {
		return &chatpb.StartChatResponse{Result: chatpb.StartChatResponse_INVALID_RULES}, nil
	}

	// The creator must satisfy the group's own rules in full, speaker rules
	// included: a group whose creator cannot read it is a group nobody can
	// reach, and one whose creator cannot post in it is a room they opened and
	// cannot use. The rules are evaluated from the request rather than a
	// stored record, since there is no record yet.
	satisfied, err := s.rules.CanSpeakWithRules(ctx, params.Rules, userID)
	if err != nil {
		log.With(zap.Error(err)).Warn("Failure evaluating chat rules")
		return nil, status.Error(codes.Internal, "")
	}
	if !satisfied {
		return &chatpb.StartChatResponse{Result: chatpb.StartChatResponse_RULES_NOT_SATISFIED}, nil
	}

	flagged, category, err := s.moderateTitle(ctx, log, params.Title)
	if err != nil {
		return nil, status.Error(codes.Internal, "")
	}
	if flagged {
		return &chatpb.StartChatResponse{
			Result:          chatpb.StartChatResponse_TITLE_MODERATED,
			FlaggedCategory: category,
		}, nil
	}

	// Every reason the blob domain gives for refusing the picture reads as one
	// result: a client's recourse — pick or upload another picture — is the
	// same for each of them. Anything else is a failure to attach, and the
	// server's fault. The grant is keyed by the chat ID, so a retry that
	// reaches here again repeats it rather than orphaning one.
	if params.Picture != nil {
		err := s.media.SetAsChatPicture(ctx, userID, chatID, params.Picture)
		switch {
		case errors.Is(err, blob.ErrBlobNotFound),
			errors.Is(err, blob.ErrBlobNotReady),
			errors.Is(err, blob.ErrBlobRejected),
			errors.Is(err, blob.ErrBlobInvalid):
			log.With(zap.Error(err)).Info("Chat picture not accepted")
			return &chatpb.StartChatResponse{Result: chatpb.StartChatResponse_PICTURE_BLOB_NOT_ACCEPTED}, nil
		case err != nil:
			log.With(zap.Error(err)).Warn("Failure setting chat picture")
			return nil, status.Error(codes.Internal, "")
		}
	}

	c := &Chat{
		ID:                     chatID,
		Type:                   chatpb.ChatType_GROUP,
		Members:                []*commonpb.UserId{userID},
		Title:                  params.Title,
		IsStaffOnly:            isStaffOnly,
		MinimumListenerBalance: minimumListenerBalance,
		CreatorID:              userID,
		PictureBlobID:          params.Picture,
		LastActivity:           time.Now().UTC(),
	}
	err = s.chats.PutChat(ctx, c)
	switch {
	case errors.Is(err, ErrChatExists):
		// A concurrent retry won the write since the read above. Its group is
		// this request's group, so answer with it. The read back can still
		// miss on a lagging replica; that is an error here, and the client's
		// next retry finds the record on the way in.
		existing, err := s.chats.GetChatByID(ctx, chatID)
		if err != nil {
			log.With(zap.Error(err)).Warn("Failure getting chat created by concurrent request")
			return nil, status.Error(codes.Internal, "")
		}
		return s.replayStartChat(ctx, log, userID, existing)
	case err != nil:
		log.With(zap.Error(err)).Warn("Failure creating chat")
		return nil, status.Error(codes.Internal, "")
	}

	metadata, err := s.hydrate(ctx, userID, memberStanding, ReadingFull, []*Chat{c})
	if err != nil {
		// The group exists; only the read back failed. It will surface on the
		// creator's next feed read.
		log.With(zap.Error(err)).Warn("Failure hydrating chat metadata")
		return nil, status.Error(codes.Internal, "")
	}
	md := metadata[0]

	// A new group's roster is known without reading it: one member, no
	// transitions yet. hydrate's batch read is eventually consistent and can
	// miss an item written moments ago, which would read as an empty roster.
	md.RosterSummary = RosterSummary{MemberCount: 1, Version: 0}.ToProto()

	// The creator is the only member, so there is no members-side update: just
	// the creator's own, carrying the metadata.
	s.publishRosterUpdate(chatID, userID, nil, &chatpb.RosterUpdate{
		Kind: &chatpb.RosterUpdate_MemberJoined_{MemberJoined: &chatpb.RosterUpdate_MemberJoined{
			Member: &chatpb.Member{
				UserId:      userID,
				UserProfile: md.Members[0].UserProfile,
			},
			Metadata: md,
		}},
		RosterSummary: md.RosterSummary,
	})

	return &chatpb.StartChatResponse{
		Result: chatpb.StartChatResponse_OK,
		Chat:   md,
	}, nil
}

// replayStartChat answers a StartChat whose group c already exists: an earlier
// attempt by the same caller — the ID embeds them, so it can be no one else —
// created it, and this request is a retry (see StartChat). The response is the
// group as GetChat would show the caller now: the roster has moved on since
// creation, and the creator may even have left, in which case they see it as
// the non-member they are. Nothing is published; a retry is not news.
func (s *Server) replayStartChat(ctx context.Context, log *zap.Logger, userID *commonpb.UserId, c *Chat) (*chatpb.StartChatResponse, error) {
	standing, err := s.access.StandingWithRules(ctx, c.ID, c.Rules(), userID, messagingpb.ViewMode_FULL)
	if err != nil {
		log.With(zap.Error(err)).Warn("Failure determining chat standing")
		return nil, status.Error(codes.Internal, "")
	}

	metadata, err := s.hydrate(ctx, userID, standing, standing.Reading(messagingpb.ViewMode_FULL), []*Chat{c})
	if err != nil {
		log.With(zap.Error(err)).Warn("Failure hydrating chat metadata")
		return nil, status.Error(codes.Internal, "")
	}

	return &chatpb.StartChatResponse{
		Result: chatpb.StartChatResponse_OK,
		Chat:   metadata[0],
	}, nil
}

// moderateTitle runs a group title through both classifiers, exactly as a
// display name is (see profile.Server.SetDisplayName): the general text
// classifier catches what it can, and the group-title classifier covers the
// title-specific abuse it is not tuned for. It reports whether the title is
// flagged and, if so, the best-fit category — the title classifier's when both
// flag, since its category is the specific one and the two classifiers' scores
// are on different scales and cannot be ranked together.
//
// ErrUnsupportedLanguage from the text classifier is not fatal: a short title
// often gives it too little to identify a language from, and the title
// classifier still covers it. Any other failure to classify is an error, and
// the caller persists nothing: allowing a title that cannot be classified
// would leave an unmoderated title in place.
func (s *Server) moderateTitle(ctx context.Context, log *zap.Logger, title string) (flagged bool, category moderationpb.FlaggedCategory, err error) {
	textResult, err := s.moderator.ClassifyText(ctx, title)
	if err != nil && !errors.Is(err, moderation.ErrUnsupportedLanguage) {
		log.With(zap.Error(err)).Warn("Failure classifying chat title as text")
		return false, moderationpb.FlaggedCategory_NONE, err
	}

	titleResult, err := s.moderator.ClassifyGroupTitle(ctx, title)
	if err != nil {
		log.With(zap.Error(err)).Warn("Failure classifying chat title")
		return false, moderationpb.FlaggedCategory_NONE, err
	}

	for _, result := range []*moderation.Result{titleResult, textResult} {
		if result == nil || !result.Flagged {
			continue
		}
		log.With(zap.Strings("categories", result.FlaggedCategories)).Info("Chat title is flagged")
		return true, moderation.HighestFlaggedCategory(result), nil
	}
	return false, moderationpb.FlaggedCategory_NONE, nil
}
