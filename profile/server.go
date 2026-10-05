package profile

import (
	"bytes"
	"context"
	"errors"
	"strings"

	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	blobpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/blob/v1"
	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"
	profilepb "github.com/code-payments/flipcash2-protobuf-api/generated/go/profile/v1"

	"github.com/code-payments/flipcash2-server/account"
	"github.com/code-payments/flipcash2-server/auth"
	"github.com/code-payments/flipcash2-server/balance"
	"github.com/code-payments/flipcash2-server/blob"
	"github.com/code-payments/flipcash2-server/model"
	"github.com/code-payments/flipcash2-server/moderation"
	"github.com/code-payments/flipcash2-server/social/x"
)

type Server struct {
	log *zap.Logger

	authz auth.Authorizer

	accounts account.Store
	profiles Store

	media Media

	moderator moderation.Client

	balances *balance.Client

	xClient *x.Client

	// firstUsername is told about every user's first handle (see
	// WithFirstUsernameHandler), nil when nothing is.
	firstUsername FirstUsernameHandler

	profilepb.UnimplementedProfileServer
}

func NewServer(log *zap.Logger, authz auth.Authorizer, accounts account.Store, profiles Store, media Media, moderator moderation.Client, balances *balance.Client, xClient *x.Client, opts ...ServerOption) *Server {
	s := &Server{
		log: log,

		authz: authz,

		accounts: accounts,
		profiles: profiles,

		media: media,

		moderator: moderator,

		balances: balances,

		xClient: xClient,
	}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

func (s *Server) GetProfile(ctx context.Context, req *profilepb.GetProfileRequest) (*profilepb.GetProfileResponse, error) {
	log := s.log

	var requestingUserID *commonpb.UserId
	var err error
	if req.Auth != nil {
		requestingUserID, err = s.authz.Authorize(ctx, req, &req.Auth)
		if err != nil {
			return nil, err
		}
		log = log.With(zap.String("requesting_user_id", model.UserIDString(requestingUserID)))
	}

	var userID *commonpb.UserId
	switch typed := req.Identifier.(type) {
	case *profilepb.GetProfileRequest_UserId:
		userID = typed.UserId
	case *profilepb.GetProfileRequest_Username:
		log = log.With(zap.String("username", typed.Username.Value))

		userID, err = s.profiles.GetUserIdByUsername(ctx, typed.Username.Value)
		if errors.Is(err, ErrNotFound) {
			return &profilepb.GetProfileResponse{Result: profilepb.GetProfileResponse_NOT_FOUND}, nil
		} else if err != nil {
			log.Warn("Failed to get user by username", zap.Error(err))
			return nil, status.Error(codes.Internal, "failed to get profile")
		}
	default:
		return nil, status.Error(codes.InvalidArgument, "unsupported identifier")
	}

	log = log.With(zap.String("user_id", model.UserIDString(userID)))

	includePrivateFields := requestingUserID != nil && bytes.Equal(userID.Value, requestingUserID.Value)

	profile, err := s.profiles.GetFullProfile(ctx, userID, includePrivateFields)
	if errors.Is(err, ErrNotFound) {
		return &profilepb.GetProfileResponse{Result: profilepb.GetProfileResponse_NOT_FOUND}, nil
	} else if err != nil {
		log.Warn("Failed to get profile", zap.Error(err))
		return nil, status.Error(codes.Internal, "failed to get profile")
	}

	if err := hydratePictures(ctx, s.media, profile.ProfilePicture, profile.CoverPicture); err != nil {
		log.Warn("Failed to hydrate profile pictures", zap.Error(err))
		return nil, status.Error(codes.Internal, "failed to get profile")
	}

	return &profilepb.GetProfileResponse{UserProfile: profile}, nil
}

func (s *Server) SetDisplayName(ctx context.Context, req *profilepb.SetDisplayNameRequest) (*profilepb.SetDisplayNameResponse, error) {
	userID, err := s.authz.Authorize(ctx, req, &req.Auth)
	if err != nil {
		return nil, err
	}

	log := s.log.With(
		zap.String("user_id", model.UserIDString(userID)),
		zap.String("display_name", req.DisplayName),
	)

	isRegistered, err := s.accounts.IsRegistered(ctx, userID)
	if err != nil {
		log.Info("Failed to get registration flag")
		return nil, status.Errorf(codes.Internal, "failed to get registration flag")
	} else if !isRegistered {
		return &profilepb.SetDisplayNameResponse{Result: profilepb.SetDisplayNameResponse_DENIED}, nil
	}

	// A name of nothing but whitespace would show as blank, so it is refused
	// before moderating: request validation only requires a character, and a
	// classifier has nothing to judge in one of these.
	if strings.TrimSpace(req.DisplayName) == "" {
		return &profilepb.SetDisplayNameResponse{Result: profilepb.SetDisplayNameResponse_INVALID_DISPLAY_NAME}, nil
	}

	if s.moderator != nil {
		// Moderate before persisting, so a flagged name is never briefly visible to
		// anyone reading the profile. Both classifiers run: the general text
		// classifier catches what it can, and the display-name classifier covers
		// the name-specific abuse it is not tuned for.
		textResult, err := s.moderator.ClassifyText(ctx, req.DisplayName)
		if err != nil && !errors.Is(err, moderation.ErrUnsupportedLanguage) {
			log.Warn("Failed to classify display name as text", zap.Error(err))
			return nil, status.Error(codes.Internal, "failed to moderate display name")
		}
		// ErrUnsupportedLanguage is not fatal here. A one- or two-word name often
		// gives the text classifier too little to identify a language from, and the
		// display-name classifier below still covers the name.
		//
		// A meaningless name is the user's to choose, so the text classifier's
		// gibberish verdict is set aside; the display-name classifier does not score
		// it at all. Anything else the text classifier flagged still stands.
		textResult = textResult.Ignoring(moderation.CategoryGibberish)

		displayNameResult, err := s.moderator.ClassifyDisplayName(ctx, req.DisplayName)
		if err != nil {
			// A name that cannot be classified is never persisted, since allowing it
			// would leave an unmoderated name in place.
			log.Warn("Failed to classify display name", zap.Error(err))
			return nil, status.Error(codes.Internal, "failed to moderate display name")
		}

		// The display-name result is checked first so that its name-specific
		// category is the one reported when both classifiers flag. Their scores are
		// on different scales, so they cannot be merged and ranked together.
		for _, result := range []*moderation.Result{displayNameResult, textResult} {
			if result == nil || !result.Flagged {
				continue
			}

			log.Info("Display name is flagged", zap.String("display_name", req.DisplayName), zap.Strings("categories", result.FlaggedCategories))
			return &profilepb.SetDisplayNameResponse{
				Result:          profilepb.SetDisplayNameResponse_FAILED_MODERATED,
				FlaggedCategory: moderation.HighestFlaggedCategory(result),
			}, nil
		}
	}

	// The caller's handle is read up front. A handle is never released without
	// another taking its place, so a user seen holding one here still holds one
	// after the write: they skip the default username path, and the handle read is
	// the one the response reports.
	var username *commonpb.Username
	userProfile, err := s.profiles.GetFullProfile(ctx, userID, false)
	switch {
	case err == nil:
		username = userProfile.Username
	case errors.Is(err, ErrNotFound):
	default:
		log.Warn("Failed to get profile", zap.Error(err))
		return nil, status.Error(codes.Internal, "failed to get profile")
	}

	// A display name that can be spelled as a handle also earns the user a default
	// one, if they hold none. It needs no moderation of its
	// own: it is the display name just moderated above, set off by a number, and
	// it skips the balance gate on claiming a handle by design.
	var defaultUsername DefaultUsernameResult
	usernameBase, eligibleForDefaultUsername := DefaultUsernameBase(req.DisplayName)
	eligibleForDefaultUsername = eligibleForDefaultUsername && username == nil
	if eligibleForDefaultUsername {
		defaultUsername, err = s.profiles.SetDisplayNameWithDefaultUsername(ctx, userID, req.DisplayName, usernameBase)
	} else {
		err = s.profiles.SetDisplayName(ctx, userID, req.DisplayName)
	}
	if err != nil {
		if errors.Is(err, ErrInvalidDisplayName) {
			return nil, status.Error(codes.InvalidArgument, "invalid display name")
		}

		s.log.Warn("Failed to set display name", zap.Error(err))
		return nil, status.Error(codes.Internal, "failed to set display name")
	}

	switch {
	case defaultUsername.Username != "":
		username = &commonpb.Username{Value: defaultUsername.Username}

		// The store assigns a default handle only to a user holding none, in the
		// same transaction that checks, so this is exactly their first.
		s.onFirstUsername(ctx, userID, defaultUsername.Username)
	case defaultUsername.NoneAvailable:
		// The user is left without a handle until a later display name finds one.
		// This only happens once a name's low numbers are all held and its cuts for
		// the random fallback are reserved words, or every number tried was lost to
		// concurrent assignments, so it is worth knowing about when it does.
		log.Warn("No default username available", zap.String("username_base", usernameBase))
	case eligibleForDefaultUsername:
		// Neither assigned nor none available: the store found the user already
		// holding a handle, one a concurrent write gave them after the read above.
		// Only then is the handle read again, so the response still reports it. A
		// failed read fails the call even though the name is set; a retry is safe,
		// since the name is the same and the handle is not assigned again.
		userProfile, err := s.profiles.GetFullProfile(ctx, userID, false)
		if err != nil {
			log.Warn("Failed to get profile after setting display name", zap.Error(err))
			return nil, status.Error(codes.Internal, "failed to get profile")
		}
		username = userProfile.Username
	}

	// The response carries the caller's handle as it stands now, whether just
	// assigned, held from before, or none, so a client never has to guess whether
	// this call gave them one.
	return &profilepb.SetDisplayNameResponse{
		Result:   profilepb.SetDisplayNameResponse_OK,
		Username: username,
	}, nil
}

func (s *Server) SetUsername(ctx context.Context, req *profilepb.SetUsernameRequest) (*profilepb.SetUsernameResponse, error) {
	userID, err := s.authz.Authorize(ctx, req, &req.Auth)
	if err != nil {
		return nil, err
	}

	// A handle is only ever held in canonical form, so whatever arrives is put in
	// it before anything is checked against it — the moderator then classifies the
	// handle that would actually be held, and the store is only handed a spelling
	// it accepts. Request validation already enforces the same form on the wire,
	// so this is what makes the RPC correct on its own rather than by virtue of
	// what runs in front of it.
	username := NormalizeUsername(req.GetUsername().GetValue())

	log := s.log.With(
		zap.String("user_id", model.UserIDString(userID)),
		zap.String("username", username),
	)

	isRegistered, err := s.accounts.IsRegistered(ctx, userID)
	if err != nil {
		log.Warn("Failed to get registration flag", zap.Error(err))
		return nil, status.Error(codes.Internal, "failed to get registration flag")
	} else if !isRegistered {
		return &profilepb.SetUsernameResponse{Result: profilepb.SetUsernameResponse_DENIED}, nil
	}

	// Validate before moderating, so a handle the store would reject anyway is
	// never sent to a classifier.
	if err := ValidateUsername(username); err != nil {
		return &profilepb.SetUsernameResponse{Result: profilepb.SetUsernameResponse_INVALID_USERNAME}, nil
	}

	// A reserved word is refused on the handle alone, with no lookup and no
	// classification: nobody may hold it, so who holds it now does not matter.
	if IsUsernameReserved(username) {
		return &profilepb.SetUsernameResponse{Result: profilepb.SetUsernameResponse_RESERVED_WORD}, nil
	}

	// Find who holds the handle before moderating, so neither a claim that cannot
	// succeed nor one that changes nothing pays for a classification. This is not
	// what makes the claim safe: another user can take the handle in between, which
	// the store still catches below.
	holder, err := s.profiles.GetUserIdByUsername(ctx, username)
	switch {
	case err == nil && bytes.Equal(holder.Value, userID.Value):
		// The user already holds this handle, so there is nothing to persist and
		// nothing new to judge — it was moderated when it was first claimed. A client
		// retrying a claim it already made lands here, and a default handle claimed
		// as-is stays auto-assigned (see Store.IsUsernameAutoAssigned).
		return &profilepb.SetUsernameResponse{}, nil
	case err == nil:
		return &profilepb.SetUsernameResponse{Result: profilepb.SetUsernameResponse_ALREADY_TAKEN}, nil
	case errors.Is(err, ErrNotFound):
		// Nobody holds it, so it is the caller's to claim.
	default:
		log.Warn("Failed to get user by username", zap.Error(err))
		return nil, status.Error(codes.Internal, "failed to set username")
	}

	// Whether this is the caller's first handle, known only by reading what they
	// hold before the claim, and read only when something is told of a first one
	// (see FirstUsernameHandler). Two claims by the caller racing can both read
	// none, which the handler tolerates. A failed read costs the handler its
	// call, never the claim.
	var isFirstUsername bool
	if s.firstUsername != nil {
		current, err := s.profiles.GetFullProfile(ctx, userID, false)
		switch {
		case err == nil:
			isFirstUsername = current.Username == nil
		case errors.Is(err, ErrNotFound):
			isFirstUsername = true
		default:
			log.Warn("Failed to get profile to check for a first username", zap.Error(err))
		}
	}

	// Gate on balance only once the handle is known to be claimable, so a user
	// re-claiming the handle they already hold is never turned away for a balance
	// that has since dropped, and a claim that would fail anyway costs no RPC. The
	// gate sits ahead of moderation so an ineligible user never pays for a
	// classification.
	if s.balances != nil {
		totalBalance, err := s.balances.GetTotalUsdfBalance(ctx, userID)
		switch {
		case err == nil:
			if totalBalance < account.MinUsernameTotalBalance {
				return &profilepb.SetUsernameResponse{Result: profilepb.SetUsernameResponse_INSUFFICIENT_BALANCE}, nil
			}
		case errors.Is(err, balance.ErrNotFound):
			// A registered user without an owner account holds nothing, so the gate is
			// what it would be at a zero balance.
			return &profilepb.SetUsernameResponse{Result: profilepb.SetUsernameResponse_INSUFFICIENT_BALANCE}, nil
		default:
			// A balance that cannot be read leaves the gate unenforced, so the claim is
			// refused rather than let through.
			log.Warn("Failed to get total balance", zap.Error(err))
			return nil, status.Error(codes.Internal, "failed to get total balance")
		}
	}

	if s.moderator != nil {
		// Moderate before claiming, so a flagged handle is never briefly held — and
		// never briefly addressable by anyone who guesses it. Both classifiers run:
		// the username classifier covers what a handle uniquely risks (squatting an
		// identity, reading as an official role), and the general text classifier
		// adds what it can on top.
		textResult, err := s.moderator.ClassifyText(ctx, username)
		if err != nil && !errors.Is(err, moderation.ErrUnsupportedLanguage) {
			log.Warn("Failed to classify username as text", zap.Error(err))
			return nil, status.Error(codes.Internal, "failed to moderate username")
		}
		// ErrUnsupportedLanguage is not fatal here. A handle is at most 15 characters
		// with no whitespace, so the text classifier frequently has too little to
		// identify a language from, and the username classifier below still covers it.
		//
		// A meaningless handle is the user's to choose, so the text classifier's
		// gibberish verdict is set aside; the username classifier does not score it
		// at all. Anything else the text classifier flagged still stands.
		textResult = textResult.Ignoring(moderation.CategoryGibberish)

		usernameResult, err := s.moderator.ClassifyUsername(ctx, username)
		if err != nil {
			// A handle that cannot be classified is never claimed, since allowing it
			// would leave an unmoderated handle in place.
			log.Warn("Failed to classify username", zap.Error(err))
			return nil, status.Error(codes.Internal, "failed to moderate username")
		}

		// The username result is checked first so that its handle-specific category
		// is the one reported when both classifiers flag. Their scores are on
		// different scales, so they cannot be merged and ranked together.
		for _, result := range []*moderation.Result{usernameResult, textResult} {
			if result == nil || !result.Flagged {
				continue
			}

			log.Info("Username is flagged", zap.String("username", req.Username.Value), zap.Strings("categories", result.FlaggedCategories))
			return &profilepb.SetUsernameResponse{
				Result:          profilepb.SetUsernameResponse_FAILED_MODERATED,
				FlaggedCategory: moderation.HighestFlaggedCategory(result),
			}, nil
		}
	}

	if err := s.profiles.SetUsername(ctx, userID, username); err != nil {
		switch {
		case errors.Is(err, ErrUsernameTaken):
			return &profilepb.SetUsernameResponse{Result: profilepb.SetUsernameResponse_ALREADY_TAKEN}, nil
		case errors.Is(err, ErrInvalidUsername):
			// Validated above, so this only trips if the store's notion of a valid
			// handle drifted from ValidateUsername.
			log.Warn("Store rejected a validated username")
			return &profilepb.SetUsernameResponse{Result: profilepb.SetUsernameResponse_INVALID_USERNAME}, nil
		default:
			log.Warn("Failed to set username", zap.Error(err))
			return nil, status.Error(codes.Internal, "failed to set username")
		}
	}

	if isFirstUsername {
		s.onFirstUsername(ctx, userID, username)
	}

	return &profilepb.SetUsernameResponse{}, nil
}

func (s *Server) SetProfilePicture(ctx context.Context, req *profilepb.SetProfilePictureRequest) (*profilepb.SetProfilePictureResponse, error) {
	userID, err := s.authz.Authorize(ctx, req, &req.Auth)
	if err != nil {
		return nil, err
	}

	log := s.log.With(
		zap.String("user_id", model.UserIDString(userID)),
		zap.String("blob_id", blob.IDString(req.BlobId)),
	)

	isRegistered, err := s.accounts.IsRegistered(ctx, userID)
	if err != nil {
		log.Warn("Failed to get registration flag")
		return nil, status.Errorf(codes.Internal, "failed to get registration flag")
	} else if !isRegistered {
		return &profilepb.SetProfilePictureResponse{Result: profilepb.SetProfilePictureResponse_DENIED}, nil
	}

	// Grant before persisting, so the picture is readable the instant it is
	// discoverable — a profile the client could read a blob id from, but not the
	// blob, would render as a broken image. This also validates the blob, so
	// nothing is persisted for a blob that cannot back a picture.
	if err := s.media.SetAsProfileMedia(ctx, userID, req.BlobId); err != nil {
		if result, ok := setProfilePictureResultForErr(err); ok {
			return &profilepb.SetProfilePictureResponse{Result: result}, nil
		}

		log.Warn("Failed to set blob as profile picture", zap.Error(err))
		return nil, status.Error(codes.Internal, "failed to set profile picture")
	}

	if err := s.profiles.SetProfilePicture(ctx, userID, req.BlobId); err != nil {
		log.Warn("Failed to set profile picture", zap.Error(err))
		return nil, status.Error(codes.Internal, "failed to set profile picture")
	}

	picture := &blobpb.Media{
		Renditions: []*blobpb.Rendition{{
			Role:   blobpb.Rendition_ORIGINAL,
			BlobId: req.BlobId,
		}},
	}
	if err := hydratePictures(ctx, s.media, picture); err != nil {
		log.Warn("Failed to hydrate profile picture", zap.Error(err))
		return nil, status.Error(codes.Internal, "failed to set profile picture")
	}

	return &profilepb.SetProfilePictureResponse{ProfilePicture: picture}, nil
}

// setProfilePictureResultForErr maps the reasons a blob cannot back a picture onto
// the result the client sees, which tells it what to do next: retry once the blob is
// READY, or upload again because this id is terminally unusable. It reports ok=false
// for any other error, which is a server fault rather than a client one.
func setProfilePictureResultForErr(err error) (profilepb.SetProfilePictureResponse_Result, bool) {
	switch {
	case errors.Is(err, blob.ErrBlobNotFound):
		return profilepb.SetProfilePictureResponse_BLOB_NOT_FOUND, true
	case errors.Is(err, blob.ErrBlobNotReady):
		return profilepb.SetProfilePictureResponse_BLOB_NOT_READY, true
	case errors.Is(err, blob.ErrBlobRejected):
		return profilepb.SetProfilePictureResponse_BLOB_REJECTED, true
	case errors.Is(err, blob.ErrBlobInvalid):
		return profilepb.SetProfilePictureResponse_INVALID_BLOB, true
	default:
		return profilepb.SetProfilePictureResponse_OK, false
	}
}

func (s *Server) SetCoverPicture(ctx context.Context, req *profilepb.SetCoverPictureRequest) (*profilepb.SetCoverPictureResponse, error) {
	userID, err := s.authz.Authorize(ctx, req, &req.Auth)
	if err != nil {
		return nil, err
	}

	log := s.log.With(
		zap.String("user_id", model.UserIDString(userID)),
		zap.String("blob_id", blob.IDString(req.BlobId)),
	)

	isRegistered, err := s.accounts.IsRegistered(ctx, userID)
	if err != nil {
		log.Warn("Failed to get registration flag")
		return nil, status.Errorf(codes.Internal, "failed to get registration flag")
	} else if !isRegistered {
		return &profilepb.SetCoverPictureResponse{Result: profilepb.SetCoverPictureResponse_DENIED}, nil
	}

	// Grant before persisting, exactly as for the profile picture: the cover is
	// shown from the same public profile surface, so it takes the same grant,
	// and the grant doubles as the validation that the blob can back a picture.
	if err := s.media.SetAsProfileMedia(ctx, userID, req.BlobId); err != nil {
		if result, ok := setCoverPictureResultForErr(err); ok {
			return &profilepb.SetCoverPictureResponse{Result: result}, nil
		}

		log.Warn("Failed to set blob as cover picture", zap.Error(err))
		return nil, status.Error(codes.Internal, "failed to set cover picture")
	}

	if err := s.profiles.SetCoverPicture(ctx, userID, req.BlobId); err != nil {
		log.Warn("Failed to set cover picture", zap.Error(err))
		return nil, status.Error(codes.Internal, "failed to set cover picture")
	}

	picture := &blobpb.Media{
		Renditions: []*blobpb.Rendition{{
			Role:   blobpb.Rendition_ORIGINAL,
			BlobId: req.BlobId,
		}},
	}
	if err := hydratePictures(ctx, s.media, picture); err != nil {
		log.Warn("Failed to hydrate cover picture", zap.Error(err))
		return nil, status.Error(codes.Internal, "failed to set cover picture")
	}

	return &profilepb.SetCoverPictureResponse{CoverPicture: picture}, nil
}

// setCoverPictureResultForErr is setProfilePictureResultForErr for the cover
// picture: the blob can fail in exactly the same ways, and the client acts on
// the distinction the same way.
func setCoverPictureResultForErr(err error) (profilepb.SetCoverPictureResponse_Result, bool) {
	switch {
	case errors.Is(err, blob.ErrBlobNotFound):
		return profilepb.SetCoverPictureResponse_BLOB_NOT_FOUND, true
	case errors.Is(err, blob.ErrBlobNotReady):
		return profilepb.SetCoverPictureResponse_BLOB_NOT_READY, true
	case errors.Is(err, blob.ErrBlobRejected):
		return profilepb.SetCoverPictureResponse_BLOB_REJECTED, true
	case errors.Is(err, blob.ErrBlobInvalid):
		return profilepb.SetCoverPictureResponse_INVALID_BLOB, true
	default:
		return profilepb.SetCoverPictureResponse_OK, false
	}
}

func (s *Server) SetBio(ctx context.Context, req *profilepb.SetBioRequest) (*profilepb.SetBioResponse, error) {
	userID, err := s.authz.Authorize(ctx, req, &req.Auth)
	if err != nil {
		return nil, err
	}

	log := s.log.With(zap.String("user_id", model.UserIDString(userID)))

	isRegistered, err := s.accounts.IsRegistered(ctx, userID)
	if err != nil {
		log.Warn("Failed to get registration flag", zap.Error(err))
		return nil, status.Error(codes.Internal, "failed to get registration flag")
	} else if !isRegistered {
		return &profilepb.SetBioResponse{Result: profilepb.SetBioResponse_DENIED}, nil
	}

	// Validate before moderating, so a bio the store would never hold is not
	// sent to a classifier, and so the RPC is correct on its own rather than by
	// virtue of the request validation that runs in front of it.
	if err := ValidateBio(req.Bio); err != nil {
		return &profilepb.SetBioResponse{Result: profilepb.SetBioResponse_INVALID_BIO}, nil
	}

	// Clearing a bio has nothing to judge. Otherwise moderate before persisting,
	// so a flagged bio is never briefly visible to anyone reading the profile.
	// Only the general text classifier runs: there is no bio-specific one, so
	// unlike a display name nothing is set aside — a meaningless bio is still a
	// gibberish verdict, and it stands.
	if req.Bio != "" && s.moderator != nil {
		result, err := s.moderator.ClassifyText(ctx, req.Bio)
		if err != nil {
			// A bio that cannot be classified is never persisted, since allowing it
			// would leave an unmoderated bio in place. That includes
			// ErrUnsupportedLanguage, which a display name or a group title lets
			// through because a second classifier still covers them: a bio has no
			// second classifier, so for now a language the text classifier cannot
			// score is refused rather than set unmoderated.
			log.Warn("Failed to classify bio", zap.Error(err))
			return nil, status.Error(codes.Internal, "failed to moderate bio")
		}

		if result.Flagged {
			log.Info("Bio is flagged", zap.String("bio", req.Bio), zap.Strings("categories", result.FlaggedCategories))
			return &profilepb.SetBioResponse{
				Result:          profilepb.SetBioResponse_FAILED_MODERATED,
				FlaggedCategory: moderation.HighestFlaggedCategory(result),
			}, nil
		}
	}

	if err := s.profiles.SetBio(ctx, userID, req.Bio); err != nil {
		log.Warn("Failed to set bio", zap.Error(err))
		return nil, status.Error(codes.Internal, "failed to set bio")
	}

	return &profilepb.SetBioResponse{}, nil
}

func (s *Server) UpdateFlipcard(ctx context.Context, req *profilepb.UpdateFlipcardRequest) (*profilepb.UpdateFlipcardResponse, error) {
	userID, err := s.authz.Authorize(ctx, req, &req.Auth)
	if err != nil {
		return nil, err
	}

	log := s.log.With(zap.String("user_id", model.UserIDString(userID)))

	isRegistered, err := s.accounts.IsRegistered(ctx, userID)
	if err != nil {
		log.Warn("Failed to get registration flag")
		return nil, status.Errorf(codes.Internal, "failed to get registration flag")
	} else if !isRegistered {
		return &profilepb.UpdateFlipcardResponse{Result: profilepb.UpdateFlipcardResponse_DENIED}, nil
	}

	// Every field of the customization is optional, so whatever is left unset
	// here stays as the user already had it.
	if req.Color != nil {
		colorHex := NormalizeColorHex(req.Color.Hex)

		if err := s.profiles.SetFlipcardColor(ctx, userID, colorHex); err != nil {
			log.Warn("Failed to set flipcard color", zap.Error(err), zap.String("color", colorHex))
			return nil, status.Error(codes.Internal, "failed to set flipcard color")
		}
	}

	return &profilepb.UpdateFlipcardResponse{}, nil
}

func (s *Server) SetMinDmChatInitFee(ctx context.Context, req *profilepb.SetMinDmChatInitFeeRequest) (*profilepb.SetMinDmChatInitFeeResponse, error) {
	userID, err := s.authz.Authorize(ctx, req, &req.Auth)
	if err != nil {
		return nil, err
	}

	log := s.log.With(
		zap.String("user_id", model.UserIDString(userID)),
		zap.String("currency", req.MinDmChatInitFee.GetCurrency()),
		zap.Float64("native_amount", req.MinDmChatInitFee.GetNativeAmount()),
	)

	isRegistered, err := s.accounts.IsRegistered(ctx, userID)
	if err != nil {
		log.Warn("Failed to get registration flag", zap.Error(err))
		return nil, status.Error(codes.Internal, "failed to get registration flag")
	} else if !isRegistered {
		return &profilepb.SetMinDmChatInitFeeResponse{Result: profilepb.SetMinDmChatInitFeeResponse_DENIED}, nil
	}

	// Validate before persisting, so the store only ever holds a fee the server
	// would enforce: one in a currency it knows, at or above that currency's floor.
	if err := ValidateMinDmChatInitFee(req.MinDmChatInitFee); err != nil {
		return &profilepb.SetMinDmChatInitFeeResponse{Result: profilepb.SetMinDmChatInitFeeResponse_INVALID_AMOUNT}, nil
	}

	if err := s.profiles.SetMinDmChatInitFee(ctx, userID, req.MinDmChatInitFee); err != nil {
		log.Warn("Failed to set min dm chat init fee", zap.Error(err))
		return nil, status.Error(codes.Internal, "failed to set min dm chat init fee")
	}

	return &profilepb.SetMinDmChatInitFeeResponse{}, nil
}

func (s *Server) LinkSocialAccount(ctx context.Context, req *profilepb.LinkSocialAccountRequest) (*profilepb.LinkSocialAccountResponse, error) {
	userID, err := s.authz.Authorize(ctx, req, &req.Auth)
	if err != nil {
		return nil, err
	}

	log := s.log.With(zap.String("user_id", model.UserIDString(userID)))

	isRegistered, err := s.accounts.IsRegistered(ctx, userID)
	if err != nil {
		log.Info("Failed to get registration flag")
		return nil, status.Errorf(codes.Internal, "failed to get registration flag")
	} else if !isRegistered {
		return &profilepb.LinkSocialAccountResponse{Result: profilepb.LinkSocialAccountResponse_DENIED}, nil
	}

	switch typed := req.LinkingToken.Type.(type) {
	case *profilepb.LinkSocialAccountRequest_LinkingToken_X:
		log = log.With(zap.String("social_account_type", "x"))

		xUser, err := s.xClient.GetMyUser(ctx, typed.X.AccessToken)
		if err != nil {
			if strings.Contains(strings.ToLower(err.Error()), "http status code: 403") {
				return &profilepb.LinkSocialAccountResponse{Result: profilepb.LinkSocialAccountResponse_INVALID_LINKING_TOKEN}, nil
			}

			log.Warn("Failed to get user from x", zap.Error(err))
			return nil, status.Error(codes.Internal, "failed to get user from x")
		}

		protoXUser := xUser.ToProto()

		if err := protoXUser.Validate(); err != nil {
			log.Warn("Failed to validate proto profile", zap.Error(err))
			return nil, status.Error(codes.Internal, "failed to validate proto profile")
		}

		err = s.profiles.LinkXAccount(ctx, userID, protoXUser, typed.X.AccessToken)
		switch err {
		case nil:
		case ErrExistingSocialLink:
			return &profilepb.LinkSocialAccountResponse{Result: profilepb.LinkSocialAccountResponse_EXISTING_LINK}, nil
		default:
			log.Warn("failed to link account", zap.Error(err))
			return nil, status.Error(codes.Internal, "failed to link account")
		}

		return &profilepb.LinkSocialAccountResponse{SocialProfile: &profilepb.SocialProfile{
			Type: &profilepb.SocialProfile_X{
				X: protoXUser,
			},
		}}, nil
	default:
		return nil, status.Error(codes.Unimplemented, "unsupported linking token type")
	}
}

func (s *Server) UnlinkSocialAccount(ctx context.Context, req *profilepb.UnlinkSocialAccountRequest) (*profilepb.UnlinkSocialAccountResponse, error) {
	userID, err := s.authz.Authorize(ctx, req, &req.Auth)
	if err != nil {
		return nil, err
	}

	log := s.log.With(zap.String("user_id", model.UserIDString(userID)))

	switch typed := req.SocialIdentifier.(type) {
	case *profilepb.UnlinkSocialAccountRequest_XUserId:
		log = log.With(zap.String("x_user_id", typed.XUserId))

		err = s.profiles.UnlinkXAccount(ctx, userID, typed.XUserId)
		if err == ErrNotFound {
			return &profilepb.UnlinkSocialAccountResponse{Result: profilepb.UnlinkSocialAccountResponse_DENIED}, nil
		} else if err != nil {
			log.Warn("Failed to unlink account", zap.Error(err))
			return nil, status.Error(codes.Internal, "failed to unlink account")
		}

		return &profilepb.UnlinkSocialAccountResponse{}, nil
	default:
		return nil, status.Error(codes.Unimplemented, "unsupported social identifier")
	}
}
