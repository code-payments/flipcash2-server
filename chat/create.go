package chat

import (
	"context"
	"errors"
	"time"

	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	blobpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/blob/v1"
	chatpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/chat/v1"
	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"
	messagingpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/messaging/v1"
	moderationpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/moderation/v1"

	"github.com/code-payments/flipcash2-server/balance"
	"github.com/code-payments/flipcash2-server/blob"
	"github.com/code-payments/flipcash2-server/model"
	"github.com/code-payments/flipcash2-server/moderation"
)

// StartChat creates a group chat with the caller as its first and only member.
//
// The request is checked in the order a client can act on: the rules it asks
// for must be ones a group can carry — which today means they must include a
// minimum listener balance, in a currency OCP can value (see RulesFromProto)
// — and ones the caller satisfies (RULES_NOT_SATISFIED); the title must pass
// moderation (TITLE_MODERATED), and then the description, if any
// (DESCRIPTION_MODERATED); and the profile picture and cover
// picture, if any, must each be a READY image the caller owns
// (PROFILE_PICTURE_BLOB_NOT_ACCEPTED, COVER_PICTURE_BLOB_NOT_ACCEPTED). Only
// then is anything written. Rule checks come first because they are local
// reads, moderation next because it is a call out, and the pictures last
// because attaching one grants read access — against a chat ID minted for the
// purpose — and that grant, though harmless against a chat that is never
// created, is best made only once everything else has passed.
//
// A description a group may not carry at all (see ValidateDescription) is
// refused as malformed, with InvalidArgument, before any of that.
//
// The pictures are attached before the record is written, so the group never
// exists without its pictures readable: a client that read a blob id from the
// metadata but could not fetch the blob would render a broken image. The
// creation itself is a single PutChat; it does not by itself make the creator
// anything other than a member (see Chat.CreatorID).
//
// Like JoinChat, a creation is a join, and is announced to the creator's other
// devices as one — a MemberJoined on their user topic carrying the metadata —
// so they insert the new chat without a refetch. There is no one else to
// tell.
//
// A private group (see Chat.IsPrivate) is created the same way, with two
// differences. It carries no rules, so there are none to validate or satisfy.
// And while private groups are being built, only a staff user may create one
// (DENIED otherwise, see canCreatePrivateGroup). What is created is a group
// without a key: the creator's client stores the chat key's envelope as a
// second step (see SetKeyEnvelope), since the envelope is bound to the chat
// ID this RPC returns, and until it has, nothing happens in the group (see
// Chat.IsPrivate).
//
// The RPC is retry-safe. The group's ID is derived from the caller and the
// request's idempotency key (see MustDeriveGroupChatID), so a retry names the
// same group, and one that already exists is answered from its record before
// any check runs: a title the moderator has since learned to flag, a balance
// that has since fallen below the minimum, or a staff flag since revoked, are
// facts about a new group, not this one. Which parameters the retry carries
// does not matter either, the kind of group included; the key is the
// request's identity. A retry that
// loses a race with its twin — both pass the read, one write lands — is caught
// by the store's uniqueness condition and answered the same way. Nothing is
// published for a retry: the creation was announced when it happened.
func (s *Server) StartChat(ctx context.Context, req *chatpb.StartChatRequest) (*chatpb.StartChatResponse, error) {
	userID, err := s.authz.Authorize(ctx, req, &req.Auth)
	if err != nil {
		return nil, err
	}

	log := s.log.With(zap.String("user_id", model.UserIDString(userID)))

	// Validation requires the oneof to be set, so anything but the two kinds
	// of group here is a variant this server predates. The two differ in what
	// a group is created with: a public group has rules, a private one has
	// none.
	var (
		title          string
		description    string
		profilePicture *blobpb.BlobId
		coverPicture   *blobpb.BlobId
		rules          *chatpb.Rules
		isPrivate      bool
	)
	switch params := req.Parameters.(type) {
	case *chatpb.StartChatRequest_PublicGroup:
		p := params.PublicGroup
		title, description, profilePicture, coverPicture, rules = p.GetTitle(), p.GetDescription(), p.GetProfilePicture(), p.GetCoverPicture(), p.GetRules()
	case *chatpb.StartChatRequest_PrivateGroup:
		p := params.PrivateGroup
		title, description, profilePicture, coverPicture, isPrivate = p.GetTitle(), p.GetDescription(), p.GetProfilePicture(), p.GetCoverPicture(), true
	default:
		return nil, status.Error(codes.InvalidArgument, "unsupported chat parameters")
	}

	// Validation also requires the key, at its fixed width; this is the check
	// MustDeriveGroupChatID relies on.
	if len(req.GetIdempotencyKey().GetValue()) != IdempotencyKeySize {
		return nil, status.Error(codes.InvalidArgument, "idempotency key is required")
	}
	chatID := MustDeriveGroupChatID(userID, req.IdempotencyKey)

	// A description the store would never hold is a malformed request, refused
	// before anything is read like the key, and so the RPC is correct on its
	// own rather than by virtue of the request validation in front of it.
	if err := ValidateDescription(description); err != nil {
		return nil, status.Error(codes.InvalidArgument, "invalid description")
	}

	existing, err := s.chats.GetChatByID(ctx, chatID)
	switch {
	case err == nil:
		return s.replayStartChat(ctx, log, userID, existing)
	case !errors.Is(err, ErrChatNotFound):
		log.With(zap.Error(err)).Warn("Failure getting chat")
		return nil, status.Error(codes.Internal, "")
	}

	var (
		isStaffOnly            bool
		minimumListenerBalance *MinimumBalance
	)
	if isPrivate {
		allowed, err := s.canCreatePrivateGroup(ctx, userID)
		if err != nil {
			log.With(zap.Error(err)).Warn("Failure checking private group creation")
			return nil, status.Error(codes.Internal, "")
		}
		if !allowed {
			return &chatpb.StartChatResponse{Result: chatpb.StartChatResponse_DENIED}, nil
		}
	} else {
		isStaffOnly, minimumListenerBalance, err = RulesFromProto(rules)
		if err != nil {
			return &chatpb.StartChatResponse{Result: chatpb.StartChatResponse_INVALID_RULES}, nil
		}

		// The creator must satisfy the group's own rules in full, speaker rules
		// included: a group whose creator cannot read it is a group nobody can
		// reach, and one whose creator cannot post in it is a room they opened and
		// cannot use. The rules are evaluated from the request rather than a
		// stored record, since there is no record yet, with the caller as the
		// creator they will record.
		//
		// This is also where the requirement's currency is first put to OCP. One
		// OCP cannot value is a rule the server cannot enforce, refused as
		// INVALID_RULES like any other (see RulesFromProto) rather than failed:
		// the currency is the client's choice, and no group is written that no one
		// could ever be admitted to.
		satisfied, err := s.rules.CanSpeakWithRules(ctx, chatID, GroupRules{Rules: rules, CreatorID: userID}, userID)
		if errors.Is(err, balance.ErrUnsupportedCurrency) {
			return &chatpb.StartChatResponse{Result: chatpb.StartChatResponse_INVALID_RULES}, nil
		}
		if err != nil {
			log.With(zap.Error(err)).Warn("Failure evaluating chat rules")
			return nil, status.Error(codes.Internal, "")
		}
		if !satisfied {
			return &chatpb.StartChatResponse{Result: chatpb.StartChatResponse_RULES_NOT_SATISFIED}, nil
		}
	}

	flagged, category, err := s.moderateTitle(ctx, log, title)
	if err != nil {
		return nil, status.Error(codes.Internal, "")
	}
	if flagged {
		return &chatpb.StartChatResponse{
			Result:          chatpb.StartChatResponse_TITLE_MODERATED,
			FlaggedCategory: category,
		}, nil
	}

	if description != "" {
		flagged, category, err := s.moderateDescription(ctx, log, description)
		if err != nil {
			return nil, status.Error(codes.Internal, "")
		}
		if flagged {
			return &chatpb.StartChatResponse{
				Result:          chatpb.StartChatResponse_DESCRIPTION_MODERATED,
				FlaggedCategory: category,
			}, nil
		}
	}

	// Every reason the blob domain gives for refusing a picture reads as one
	// result per picture: a client's recourse — pick or upload another
	// picture — is the same for each of them. Anything else is a failure to
	// attach, and the server's fault. The grant is keyed by the chat ID, so a
	// retry that reaches here again repeats it rather than orphaning one.
	if profilePicture != nil {
		accepted, err := s.attachChatPicture(ctx, log, userID, chatID, profilePicture)
		if err != nil {
			return nil, err
		}
		if !accepted {
			return &chatpb.StartChatResponse{Result: chatpb.StartChatResponse_PROFILE_PICTURE_BLOB_NOT_ACCEPTED}, nil
		}
	}
	if coverPicture != nil {
		accepted, err := s.attachChatPicture(ctx, log, userID, chatID, coverPicture)
		if err != nil {
			return nil, err
		}
		if !accepted {
			return &chatpb.StartChatResponse{Result: chatpb.StartChatResponse_COVER_PICTURE_BLOB_NOT_ACCEPTED}, nil
		}
	}

	c := &Chat{
		ID:                     chatID,
		Type:                   chatpb.ChatType_GROUP,
		Members:                []*commonpb.UserId{userID},
		Title:                  title,
		IsStaffOnly:            isStaffOnly,
		MinimumListenerBalance: minimumListenerBalance,
		IsPrivate:              isPrivate,
		CreatorID:              userID,
		Description:            description,
		ProfilePictureBlobID:   profilePicture,
		CoverPictureBlobID:     coverPicture,
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

	metadata, err := s.hydrate(ctx, userID, memberListenerStanding, ReadingFull, fullDetail, []*Chat{c})
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
	// the creator's own, carrying the metadata, and the creation's record —
	// the creation's join time at version zero (see announcedMember).
	member, err := s.announcedMember(ctx, log, chatID, userID, md.Members[0].UserProfile, RosterSummary{MemberCount: 1, Version: 0})
	if err != nil {
		return nil, err
	}
	s.publishRosterUpdate(chatID, userID, nil, &chatpb.RosterUpdate{
		Kind: &chatpb.RosterUpdate_MemberJoined_{MemberJoined: &chatpb.RosterUpdate_MemberJoined{
			Member:   member,
			Metadata: md,
		}},
		RosterSummary: md.RosterSummary,
	})

	return &chatpb.StartChatResponse{
		Result: chatpb.StartChatResponse_OK,
		Chat:   md,
	}, nil
}

// canCreatePrivateGroup reports whether userID may create a private group:
// today, only a staff user. It is a transitional gate, like use_e2ee's (see
// useE2ee): private groups were built in steps behind it, and clients have
// not shipped them. Opening creation to everyone is removing this check.
func (s *Server) canCreatePrivateGroup(ctx context.Context, userID *commonpb.UserId) (bool, error) {
	return s.accounts.IsStaff(ctx, userID)
}

// replayStartChat answers a StartChat whose group c already exists: an earlier
// attempt by the same caller — the ID embeds them, so it can be no one else —
// created it, and this request is a retry (see StartChat). The response is the
// group as GetChat would show the caller now: the roster has moved on since
// creation, and the creator may even have left, in which case they see it as
// the non-member they are. Nothing is published; a retry is not news.
func (s *Server) replayStartChat(ctx context.Context, log *zap.Logger, userID *commonpb.UserId, c *Chat) (*chatpb.StartChatResponse, error) {
	standing, err := s.access.ListenerStandingWithRules(ctx, c.ID, c.GroupRules(), userID, messagingpb.ViewMode_FULL)
	if err != nil {
		log.With(zap.Error(err)).Warn("Failure determining chat standing")
		return nil, status.Error(codes.Internal, "")
	}

	metadata, err := s.hydrate(ctx, userID, standing, standing.Reading(messagingpb.ViewMode_FULL), fullDetail, []*Chat{c})
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
		log.With(zap.String("title", title), zap.Strings("categories", result.FlaggedCategories)).Info("Chat title is flagged")
		return true, moderation.HighestFlaggedCategory(result), nil
	}
	return false, moderationpb.FlaggedCategory_NONE, nil
}

// moderateDescription runs a group description through the general text
// classifier alone, exactly as a user's bio is (see profile.Server.SetBio):
// there is no description-specific classifier, and the group-title one is
// tuned for a short name, not free text. It reports whether the description
// is flagged and, if so, the best-fit category. The caller moderates only a
// description it is setting, never the empty one.
//
// Any failure to classify is an error, and the caller persists nothing. That
// includes ErrUnsupportedLanguage, which a title lets through because a second
// classifier still covers it: a description has no second classifier, so for
// now a language the text classifier cannot score is refused rather than set
// unmoderated.
func (s *Server) moderateDescription(ctx context.Context, log *zap.Logger, description string) (flagged bool, category moderationpb.FlaggedCategory, err error) {
	result, err := s.moderator.ClassifyText(ctx, description)
	if err != nil {
		log.With(zap.Error(err)).Warn("Failure classifying chat description")
		return false, moderationpb.FlaggedCategory_NONE, err
	}
	if result == nil || !result.Flagged {
		return false, moderationpb.FlaggedCategory_NONE, nil
	}
	log.With(zap.String("description", description), zap.Strings("categories", result.FlaggedCategories)).Info("Chat description is flagged")
	return true, moderation.HighestFlaggedCategory(result), nil
}

// attachChatPicture attaches blobID to chatID as one of its pictures (see
// Media.SetAsChatMedia), reporting whether the blob domain accepted it. Every
// reason it gives for refusing a blob reads as not accepted, since a client's
// recourse is the same for each; anything else is a failure to attach, and
// returned as the RPC's error.
func (s *Server) attachChatPicture(ctx context.Context, log *zap.Logger, userID *commonpb.UserId, chatID *commonpb.ChatId, blobID *blobpb.BlobId) (accepted bool, err error) {
	err = s.media.SetAsChatMedia(ctx, userID, chatID, blobID)
	switch {
	case errors.Is(err, blob.ErrBlobNotFound),
		errors.Is(err, blob.ErrBlobNotReady),
		errors.Is(err, blob.ErrBlobRejected),
		errors.Is(err, blob.ErrBlobInvalid):
		log.With(zap.Error(err)).Info("Chat picture not accepted")
		return false, nil
	case err != nil:
		log.With(zap.Error(err)).Warn("Failure setting chat picture")
		return false, status.Error(codes.Internal, "")
	}
	return true, nil
}
