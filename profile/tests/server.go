package tests

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	blobpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/blob/v1"
	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"
	moderationpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/moderation/v1"
	profilepb "github.com/code-payments/flipcash2-protobuf-api/generated/go/profile/v1"

	"github.com/code-payments/flipcash2-server/account"
	"github.com/code-payments/flipcash2-server/auth"
	"github.com/code-payments/flipcash2-server/blob"
	blobmemory "github.com/code-payments/flipcash2-server/blob/memory"
	"github.com/code-payments/flipcash2-server/model"
	"github.com/code-payments/flipcash2-server/moderation"
	"github.com/code-payments/flipcash2-server/profile"
	"github.com/code-payments/flipcash2-server/protoutil"
	"github.com/code-payments/flipcash2-server/social/x"
	"github.com/code-payments/flipcash2-server/testutil"
)

func RunServerTests(t *testing.T, accounts account.Store, profiles profile.Store, teardown func()) {
	for _, tf := range []func(t *testing.T, accounts account.Store, profiles profile.Store){
		testServer,
		testProfilePicture,
		testTipCardCustomization,
		testUsernameIsPublic,
		testGetProfileByUsername,
		testSetUsername,
		testSetUsernameStaffGated,
		testDisplayNameModeration,
		testUsernameModeration,
	} {
		tf(t, accounts, profiles)
		teardown()
	}
}

// newMedia returns a real blob.Integration over in-memory blob stores, plus the
// stores themselves so a test can seed blobs in whatever state it needs. Using the
// real integration rather than a fake keeps the grant path — the thing that
// actually authorizes a picture — under test.
func newMedia() (profile.Media, blob.Store, blob.AccessStore) {
	blobs := blobmemory.NewInMemory()
	access := blobmemory.NewInMemoryAccessStore()
	return blob.NewIntegration(blobs, blobmemory.NewInMemoryStorage(), access), blobs, access
}

// seedBlob inserts a blob owned by owner, advanced to the given state. It is the
// shorthand for "a blob that already went through the upload pipeline", so the
// profile tests can exercise every outcome without driving BlobStorage.
func seedBlob(t *testing.T, blobs blob.Store, owner *commonpb.UserId, state blob.State, mimeType string) *blobpb.BlobId {
	ctx := context.Background()
	id := blob.MustGenerateID()

	require.NoError(t, blobs.CreatePending(ctx, &blob.Blob{
		ID:         id,
		Rendition:  blob.RenditionOriginal,
		Owner:      owner,
		State:      blob.StatePending,
		StorageKey: "images/" + blob.IDString(id) + "/original",
		MimeType:   mimeType,
		SizeBytes:  1024,
	}))

	switch state {
	case blob.StatePending:
	case blob.StateRejected:
		_, err := blobs.Reject(ctx, id, &blob.RejectionMetadata{Reason: blob.RejectionReasonModeration})
		require.NoError(t, err)
	default:
		_, err := blobs.Advance(ctx, id, state, nil)
		require.NoError(t, err)
	}
	return id
}

// requireProfileUnset asserts that a user has filled in no part of their profile.
// Whether that reads back as NOT_FOUND or as an empty profile depends on whether
// the store knows the user at all — Postgres shares the user row with account
// binding, the in-memory store only learns of a user when a profile field is set
// — so the assertion is on the fields, which are unset either way. The join
// timestamp is exempt: every user the store knows has one, as is the Tip Card
// customization, which is asserted to be the default rather than absent.
func requireProfileUnset(t *testing.T, resp *profilepb.GetProfileResponse) {
	t.Helper()

	require.Empty(t, resp.UserProfile.GetDisplayName())
	require.Nil(t, resp.UserProfile.GetProfilePicture())
	require.Empty(t, resp.UserProfile.GetSocialProfiles())
	require.Nil(t, resp.UserProfile.GetPhoneNumber())
	require.Nil(t, resp.UserProfile.GetEmailAddress())

	if resp.UserProfile != nil {
		require.NoError(t, protoutil.ProtoEqualError(profile.DefaultTipCardCustomization(), resp.UserProfile.TipCardCustomization))
	}
}

func testServer(t *testing.T, accounts account.Store, profiles profile.Store) {
	ctx := context.Background()
	log := zaptest.NewLogger(t)

	authz := account.NewAuthorizer(log, accounts, auth.NewKeyPairAuthenticator(log))

	media, _, _ := newMedia()
	serv := profile.NewServer(log, authz, accounts, profiles, media, &fakeModerator{}, x.NewClient(), false)
	cc := testutil.RunGRPCServer(t, log, testutil.WithService(func(s *grpc.Server) {
		profilepb.RegisterProfileServer(s, serv)
	}))

	client := profilepb.NewProfileClient(cc)
	userID := model.MustGenerateUserID()
	keyPair := model.MustGenerateKeyPair()

	t.Run("No User", func(t *testing.T) {
		getResp, err := client.GetProfile(ctx, &profilepb.GetProfileRequest{
			Identifier: &profilepb.GetProfileRequest_UserId{UserId: userID},
		})
		require.NoError(t, err)
		require.Equal(t, profilepb.GetProfileResponse_NOT_FOUND, getResp.Result)
		require.Nil(t, getResp.UserProfile)

		setDisplayName := &profilepb.SetDisplayNameRequest{
			DisplayName: "my name",
		}
		require.NoError(t, keyPair.Auth(setDisplayName, &setDisplayName.Auth))
		_, err = client.SetDisplayName(ctx, setDisplayName)
		require.Equal(t, codes.PermissionDenied, status.Code(err))
	})

	t.Run("Registered user", func(t *testing.T) {
		_, err := accounts.Bind(ctx, userID, keyPair.Proto())
		require.NoError(t, err)
		require.NoError(t, accounts.SetRegistrationFlag(ctx, userID, true))

		// Binding of a user does not fill in a profile: there is nothing to read
		// back until one is set.
		getResp, err := client.GetProfile(ctx, &profilepb.GetProfileRequest{
			Identifier: &profilepb.GetProfileRequest_UserId{UserId: userID},
		})
		require.NoError(t, err)
		requireProfileUnset(t, getResp)

		setDisplayName := &profilepb.SetDisplayNameRequest{
			DisplayName: "my name",
		}
		require.NoError(t, keyPair.Auth(setDisplayName, &setDisplayName.Auth))
		setDisplayNameResp, err := client.SetDisplayName(ctx, setDisplayName)
		require.NoError(t, err)
		require.NoError(t, protoutil.ProtoEqualError(&profilepb.SetDisplayNameResponse{Result: profilepb.SetDisplayNameResponse_OK}, setDisplayNameResp))

		expected := &profilepb.UserProfile{
			UserId:               userID,
			DisplayName:          "my name",
			TipCardCustomization: profile.DefaultTipCardCustomization(),
		}

		getResp, err = client.GetProfile(ctx, &profilepb.GetProfileRequest{
			Identifier: &profilepb.GetProfileRequest_UserId{UserId: userID},
		})
		require.NoError(t, err)

		require.NotNil(t, getResp.UserProfile.JoinTs)
		expected.JoinTs = getResp.UserProfile.JoinTs

		require.NoError(t, protoutil.ProtoEqualError(expected, getResp.UserProfile))

		xProfile := &profilepb.XProfile{
			Id:            "123",
			Username:      "registered_user",
			Name:          "registered name",
			Description:   "description",
			ProfilePicUrl: "url",
			VerifiedType:  profilepb.XProfile_BLUE,
			FollowerCount: 888,
		}
		// todo: Need mock X client to use the RPC
		require.NoError(t, profiles.LinkXAccount(ctx, userID, xProfile, "access_token"))

		expected.SocialProfiles = append(expected.SocialProfiles, &profilepb.SocialProfile{
			Type: &profilepb.SocialProfile_X{
				X: xProfile,
			},
		})
		getResp, err = client.GetProfile(ctx, &profilepb.GetProfileRequest{
			Identifier: &profilepb.GetProfileRequest_UserId{UserId: userID},
		})
		require.NoError(t, err)
		require.NoError(t, protoutil.ProtoEqualError(expected, getResp.UserProfile))

		unlink := &profilepb.UnlinkSocialAccountRequest{
			SocialIdentifier: &profilepb.UnlinkSocialAccountRequest_XUserId{
				XUserId: xProfile.Id,
			},
		}
		require.NoError(t, keyPair.Auth(unlink, &unlink.Auth))

		unlinkResp, err := client.UnlinkSocialAccount(ctx, unlink)
		require.NoError(t, err)
		require.NoError(t, protoutil.ProtoEqualError(&profilepb.UnlinkSocialAccountResponse{}, unlinkResp))

		expected.SocialProfiles = nil
		getResp, err = client.GetProfile(ctx, &profilepb.GetProfileRequest{
			Identifier: &profilepb.GetProfileRequest_UserId{UserId: userID},
		})
		require.NoError(t, err)
		require.NoError(t, protoutil.ProtoEqualError(expected, getResp.UserProfile))

		t.Run("Private profile", func(t *testing.T) {
			require.NoError(t, profiles.LinkPhoneNumber(ctx, userID, "+12223334444", &commonpb.Hash{Value: []byte("phone-hash")}))
			require.NoError(t, profiles.LinkEmailAddress(ctx, userID, "someone@gmail.com"))

			get := &profilepb.GetProfileRequest{
				Identifier: &profilepb.GetProfileRequest_UserId{UserId: userID},
			}

			getResp, err = client.GetProfile(ctx, get)
			require.NoError(t, err)
			require.NoError(t, protoutil.ProtoEqualError(expected, getResp.UserProfile))

			otherUserID := model.MustGenerateUserID()
			otherKeypair := model.MustGenerateKeyPair()

			_, err := accounts.Bind(ctx, otherUserID, otherKeypair.Proto())
			require.NoError(t, err)
			require.NoError(t, accounts.SetRegistrationFlag(ctx, otherUserID, true))

			require.NoError(t, otherKeypair.Auth(get, &get.Auth))

			getResp, err = client.GetProfile(ctx, get)
			require.NoError(t, err)
			require.NoError(t, protoutil.ProtoEqualError(expected, getResp.UserProfile))

			expected.PhoneNumber = &commonpb.PhoneNumber{Value: "+12223334444"}
			expected.EmailAddress = &commonpb.EmailAddress{Value: "someone@gmail.com"}
			require.NoError(t, keyPair.Auth(get, &get.Auth))

			getResp, err = client.GetProfile(ctx, get)
			require.NoError(t, err)
			require.NoError(t, protoutil.ProtoEqualError(expected, getResp.UserProfile))
		})
	})

	t.Run("Unregistered user", func(t *testing.T) {
		userID2 := model.MustGenerateUserID()
		keypair2 := model.MustGenerateKeyPair()

		_, err := accounts.Bind(ctx, userID2, keypair2.Proto())
		require.NoError(t, err)
		require.NoError(t, accounts.SetRegistrationFlag(ctx, userID, false))

		setDisplayName := &profilepb.SetDisplayNameRequest{
			DisplayName: "my name",
		}
		require.NoError(t, keypair2.Auth(setDisplayName, &setDisplayName.Auth))
		setDisplayNameResp, err := client.SetDisplayName(ctx, setDisplayName)
		require.NoError(t, err)
		require.NoError(t, protoutil.ProtoEqualError(&profilepb.SetDisplayNameResponse{Result: profilepb.SetDisplayNameResponse_DENIED}, setDisplayNameResp))

		linkXAccount := &profilepb.LinkSocialAccountRequest{
			LinkingToken: &profilepb.LinkSocialAccountRequest_LinkingToken{
				Type: &profilepb.LinkSocialAccountRequest_LinkingToken_X{
					X: &profilepb.LinkSocialAccountRequest_LinkingToken_XLinkingToken{
						AccessToken: "access_token",
					},
				},
			},
		}
		require.NoError(t, keypair2.Auth(linkXAccount, &linkXAccount.Auth))
		linkXAccountResp, err := client.LinkSocialAccount(ctx, linkXAccount)
		require.NoError(t, err)
		require.NoError(t, protoutil.ProtoEqualError(&profilepb.LinkSocialAccountResponse{Result: profilepb.LinkSocialAccountResponse_DENIED}, linkXAccountResp))

		get, err := client.GetProfile(ctx, &profilepb.GetProfileRequest{
			Identifier: &profilepb.GetProfileRequest_UserId{UserId: userID2},
		})
		require.NoError(t, err)
		requireProfileUnset(t, get)
	})
}

func testTipCardCustomization(t *testing.T, accounts account.Store, profiles profile.Store) {
	ctx := context.Background()
	log := zaptest.NewLogger(t)

	authz := account.NewAuthorizer(log, accounts, auth.NewKeyPairAuthenticator(log))

	media, _, _ := newMedia()
	serv := profile.NewServer(log, authz, accounts, profiles, media, &fakeModerator{}, x.NewClient(), false)
	cc := testutil.RunGRPCServer(t, log, testutil.WithService(func(s *grpc.Server) {
		profilepb.RegisterProfileServer(s, serv)
	}))

	client := profilepb.NewProfileClient(cc)

	userID := model.MustGenerateUserID()
	keyPair := model.MustGenerateKeyPair()
	_, err := accounts.Bind(ctx, userID, keyPair.Proto())
	require.NoError(t, err)

	updateTipCard := func(color *commonpb.Color) *profilepb.UpdateTipCardResponse {
		t.Helper()
		req := &profilepb.UpdateTipCardRequest{Color: color}
		require.NoError(t, keyPair.Auth(req, &req.Auth))
		resp, err := client.UpdateTipCard(ctx, req)
		require.NoError(t, err)
		return resp
	}

	// Read without auth, since the customization is public: what this returns is
	// what any other user sees on the Tip Card.
	getColorHex := func() string {
		t.Helper()
		resp, err := client.GetProfile(ctx, &profilepb.GetProfileRequest{Identifier: &profilepb.GetProfileRequest_UserId{UserId: userID}})
		require.NoError(t, err)
		require.Equal(t, profilepb.GetProfileResponse_OK, resp.Result)
		return resp.UserProfile.TipCardCustomization.Color.Hex
	}

	t.Run("Unregistered user is denied", func(t *testing.T) {
		resp := updateTipCard(&commonpb.Color{Hex: "#19191A"})
		require.NoError(t, protoutil.ProtoEqualError(&profilepb.UpdateTipCardResponse{Result: profilepb.UpdateTipCardResponse_DENIED}, resp))

		getResp, err := client.GetProfile(ctx, &profilepb.GetProfileRequest{Identifier: &profilepb.GetProfileRequest_UserId{UserId: userID}})
		require.NoError(t, err)
		requireProfileUnset(t, getResp)
	})

	require.NoError(t, accounts.SetRegistrationFlag(ctx, userID, true))

	t.Run("Color is set and replaced", func(t *testing.T) {
		resp := updateTipCard(&commonpb.Color{Hex: "#19191A"})
		require.NoError(t, protoutil.ProtoEqualError(&profilepb.UpdateTipCardResponse{Result: profilepb.UpdateTipCardResponse_OK}, resp))
		require.Equal(t, "#19191A", getColorHex())

		updateTipCard(&commonpb.Color{Hex: "#FFFFFF"})
		require.Equal(t, "#FFFFFF", getColorHex())
	})

	t.Run("Color is normalized", func(t *testing.T) {
		updateTipCard(&commonpb.Color{Hex: "#abcdef"})
		require.Equal(t, "#ABCDEF", getColorHex())
	})

	t.Run("Unset fields are left alone", func(t *testing.T) {
		updateTipCard(&commonpb.Color{Hex: "#19191A"})

		resp := updateTipCard(nil)
		require.NoError(t, protoutil.ProtoEqualError(&profilepb.UpdateTipCardResponse{Result: profilepb.UpdateTipCardResponse_OK}, resp))
		require.Equal(t, "#19191A", getColorHex())
	})
}

// testUsernameIsPublic covers the read side of usernames: a handle is visible to
// anyone, with no auth. It is seeded through the store rather than claimed over
// the RPC, which testSetUsername covers.
func testUsernameIsPublic(t *testing.T, accounts account.Store, profiles profile.Store) {
	ctx := context.Background()
	log := zaptest.NewLogger(t)

	authz := account.NewAuthorizer(log, accounts, auth.NewKeyPairAuthenticator(log))

	media, _, _ := newMedia()
	serv := profile.NewServer(log, authz, accounts, profiles, media, &fakeModerator{}, x.NewClient(), false)
	cc := testutil.RunGRPCServer(t, log, testutil.WithService(func(s *grpc.Server) {
		profilepb.RegisterProfileServer(s, serv)
	}))

	client := profilepb.NewProfileClient(cc)

	userID := model.MustGenerateUserID()
	keyPair := model.MustGenerateKeyPair()
	_, err := accounts.Bind(ctx, userID, keyPair.Proto())
	require.NoError(t, err)
	require.NoError(t, accounts.SetRegistrationFlag(ctx, userID, true))
	require.NoError(t, profiles.SetDisplayName(ctx, userID, "my name"))

	// Read without auth, since a handle is public: what this returns is what any
	// other user sees.
	getUsername := func() *commonpb.Username {
		t.Helper()
		resp, err := client.GetProfile(ctx, &profilepb.GetProfileRequest{Identifier: &profilepb.GetProfileRequest_UserId{UserId: userID}})
		require.NoError(t, err)
		require.Equal(t, profilepb.GetProfileResponse_OK, resp.Result)
		require.NoError(t, resp.UserProfile.Validate())
		return resp.UserProfile.Username
	}

	// A user who has claimed nothing has no handle.
	require.Nil(t, getUsername())

	require.NoError(t, profiles.SetUsername(ctx, userID, "my_handle"))
	require.Equal(t, "my_handle", getUsername().Value)

	// A change is reflected on the next read.
	require.NoError(t, profiles.SetUsername(ctx, userID, "renamed"))
	require.Equal(t, "renamed", getUsername().Value)
}

// testGetProfileByUsername covers fetching a profile by the handle its holder
// claimed, which is the same profile the holder's user ID returns.
func testGetProfileByUsername(t *testing.T, accounts account.Store, profiles profile.Store) {
	ctx := context.Background()
	log := zaptest.NewLogger(t)

	authz := account.NewAuthorizer(log, accounts, auth.NewKeyPairAuthenticator(log))

	media, _, _ := newMedia()
	serv := profile.NewServer(log, authz, accounts, profiles, media, &fakeModerator{}, x.NewClient(), false)
	cc := testutil.RunGRPCServer(t, log, testutil.WithService(func(s *grpc.Server) {
		profilepb.RegisterProfileServer(s, serv)
	}))

	client := profilepb.NewProfileClient(cc)

	getByUsername := func(username string, keyPair *model.KeyPair) *profilepb.GetProfileResponse {
		t.Helper()
		req := &profilepb.GetProfileRequest{
			Identifier: &profilepb.GetProfileRequest_Username{
				Username: &commonpb.Username{Value: username},
			},
		}
		if keyPair != nil {
			require.NoError(t, keyPair.Auth(req, &req.Auth))
		}
		resp, err := client.GetProfile(ctx, req)
		require.NoError(t, err)
		return resp
	}

	t.Run("Unclaimed handle", func(t *testing.T) {
		resp := getByUsername("nobody_holds_me", nil)
		require.Equal(t, profilepb.GetProfileResponse_NOT_FOUND, resp.Result)
		require.Nil(t, resp.UserProfile)
	})

	userID := model.MustGenerateUserID()
	keyPair := model.MustGenerateKeyPair()
	_, err := accounts.Bind(ctx, userID, keyPair.Proto())
	require.NoError(t, err)
	require.NoError(t, accounts.SetRegistrationFlag(ctx, userID, true))
	require.NoError(t, profiles.SetDisplayName(ctx, userID, "my name"))
	require.NoError(t, profiles.SetUsername(ctx, userID, "by_handle"))
	require.NoError(t, profiles.LinkPhoneNumber(ctx, userID, "+12223334444", &commonpb.Hash{Value: []byte("phone-hash")}))

	t.Run("Claimed handle returns the holder's profile", func(t *testing.T) {
		byUserID, err := client.GetProfile(ctx, &profilepb.GetProfileRequest{
			Identifier: &profilepb.GetProfileRequest_UserId{UserId: userID},
		})
		require.NoError(t, err)
		require.Equal(t, profilepb.GetProfileResponse_OK, byUserID.Result)

		resp := getByUsername("by_handle", nil)
		require.Equal(t, profilepb.GetProfileResponse_OK, resp.Result)
		require.NoError(t, protoutil.ProtoEqualError(byUserID.UserProfile, resp.UserProfile))
		require.Equal(t, "by_handle", resp.UserProfile.Username.Value)
	})

	// Private fields follow who is asking, not which identifier they asked with:
	// the holder authorizing as themselves sees them, and nobody else does.
	t.Run("Private fields are scoped to the holder", func(t *testing.T) {
		otherKeyPair := model.MustGenerateKeyPair()
		_, err := accounts.Bind(ctx, model.MustGenerateUserID(), otherKeyPair.Proto())
		require.NoError(t, err)

		resp := getByUsername("by_handle", &otherKeyPair)
		require.Equal(t, profilepb.GetProfileResponse_OK, resp.Result)
		require.Nil(t, resp.UserProfile.PhoneNumber)

		resp = getByUsername("by_handle", &keyPair)
		require.Equal(t, profilepb.GetProfileResponse_OK, resp.Result)
		require.Equal(t, "+12223334444", resp.UserProfile.PhoneNumber.Value)
	})
}

// testSetUsername covers claiming a handle over the RPC, with the staff gate off
// so registration is the only thing standing between a user and a handle.
func testSetUsername(t *testing.T, accounts account.Store, profiles profile.Store) {
	ctx := context.Background()
	log := zaptest.NewLogger(t)

	authz := account.NewAuthorizer(log, accounts, auth.NewKeyPairAuthenticator(log))

	media, _, _ := newMedia()
	moderator := &fakeModerator{}
	serv := profile.NewServer(log, authz, accounts, profiles, media, moderator, x.NewClient(), false)
	cc := testutil.RunGRPCServer(t, log, testutil.WithService(func(s *grpc.Server) {
		profilepb.RegisterProfileServer(s, serv)
	}))

	client := profilepb.NewProfileClient(cc)

	userID := model.MustGenerateUserID()
	keyPair := model.MustGenerateKeyPair()
	_, err := accounts.Bind(ctx, userID, keyPair.Proto())
	require.NoError(t, err)

	trySetUsername := func(keyPair *model.KeyPair, username string) (*profilepb.SetUsernameResponse, error) {
		t.Helper()
		req := &profilepb.SetUsernameRequest{Username: &commonpb.Username{Value: username}}
		require.NoError(t, keyPair.Auth(req, &req.Auth))
		return client.SetUsername(ctx, req)
	}

	setUsername := func(keyPair *model.KeyPair, username string) *profilepb.SetUsernameResponse {
		t.Helper()
		resp, err := trySetUsername(keyPair, username)
		require.NoError(t, err)
		return resp
	}

	// Read without auth, since a handle is public.
	usernameOf := func(userID *commonpb.UserId) string {
		t.Helper()
		resp, err := client.GetProfile(ctx, &profilepb.GetProfileRequest{Identifier: &profilepb.GetProfileRequest_UserId{UserId: userID}})
		require.NoError(t, err)
		return resp.GetUserProfile().GetUsername().GetValue()
	}

	t.Run("Unregistered user is denied", func(t *testing.T) {
		resp := setUsername(&keyPair, "denied_user")
		require.NoError(t, protoutil.ProtoEqualError(&profilepb.SetUsernameResponse{Result: profilepb.SetUsernameResponse_DENIED}, resp))

		// A denial claims nothing, so the handle is still there for someone else.
		require.Empty(t, usernameOf(userID))
	})

	require.NoError(t, accounts.SetRegistrationFlag(ctx, userID, true))

	t.Run("Handle is claimed", func(t *testing.T) {
		resp := setUsername(&keyPair, "first_handle")
		require.NoError(t, protoutil.ProtoEqualError(&profilepb.SetUsernameResponse{Result: profilepb.SetUsernameResponse_OK}, resp))
		require.Equal(t, "first_handle", usernameOf(userID))

		// The claim is what makes the profile reachable by handle.
		byUsername, err := client.GetProfile(ctx, &profilepb.GetProfileRequest{
			Identifier: &profilepb.GetProfileRequest_Username{Username: &commonpb.Username{Value: "first_handle"}},
		})
		require.NoError(t, err)
		require.Equal(t, profilepb.GetProfileResponse_OK, byUsername.Result)
		require.Equal(t, userID.Value, byUsername.UserProfile.UserId.Value)
	})

	t.Run("Re-claiming the same handle is a no-op", func(t *testing.T) {
		moderator.classifiedUsername = ""

		require.Equal(t, profilepb.SetUsernameResponse_OK, setUsername(&keyPair, "first_handle").Result)
		require.Equal(t, "first_handle", usernameOf(userID))

		// Nothing about the handle changed, so it is not judged a second time — it
		// was moderated when it was first claimed.
		require.Empty(t, moderator.classifiedUsername)
	})

	t.Run("Reserved word is refused", func(t *testing.T) {
		for _, reserved := range []string{"flipcash", "flipcash_admin", "pay_flipcash"} {
			moderator.classifiedUsername = ""

			resp := setUsername(&keyPair, reserved)
			require.NoError(t, protoutil.ProtoEqualError(&profilepb.SetUsernameResponse{Result: profilepb.SetUsernameResponse_RESERVED_WORD}, resp), "username: %q", reserved)

			// Nobody may hold it, so it is refused on the handle alone — without a
			// classification, and without the handle being claimed.
			require.Empty(t, moderator.classifiedUsername)

			getResp, err := client.GetProfile(ctx, &profilepb.GetProfileRequest{
				Identifier: &profilepb.GetProfileRequest_Username{Username: &commonpb.Username{Value: reserved}},
			})
			require.NoError(t, err)
			require.Equal(t, profilepb.GetProfileResponse_NOT_FOUND, getResp.Result)
		}

		// The handle the user already held is untouched.
		require.Equal(t, "first_handle", usernameOf(userID))
	})

	// A handle that is not in canonical form never reaches the RPC: the wire
	// contract on common.v1.Username is the same pattern the store enforces, so
	// request validation turns it away first. INVALID_USERNAME is what the RPC
	// answers with where that validation is not in front of it.
	t.Run("Malformed handle is rejected before the RPC", func(t *testing.T) {
		for _, invalid := range []string{
			"",
			"a",
			"sixteen_chars_ab",
			"has space",
			"has-dash",
			"emoji_🙂",
			"MiXeD_Case",
		} {
			_, err := trySetUsername(&keyPair, invalid)
			require.Equal(t, codes.InvalidArgument, status.Code(err), "username: %q", invalid)
		}

		// The handle the user already held is untouched by any of the rejections.
		require.Equal(t, "first_handle", usernameOf(userID))
	})

	t.Run("A handle has one holder", func(t *testing.T) {
		otherUserID := model.MustGenerateUserID()
		otherKeyPair := model.MustGenerateKeyPair()
		_, err := accounts.Bind(ctx, otherUserID, otherKeyPair.Proto())
		require.NoError(t, err)
		require.NoError(t, accounts.SetRegistrationFlag(ctx, otherUserID, true))

		moderator.classifiedUsername = ""

		resp := setUsername(&otherKeyPair, "first_handle")
		require.NoError(t, protoutil.ProtoEqualError(&profilepb.SetUsernameResponse{Result: profilepb.SetUsernameResponse_ALREADY_TAKEN}, resp))
		require.Empty(t, usernameOf(otherUserID))

		// A handle that is already gone is turned away before it costs a
		// classification.
		require.Empty(t, moderator.classifiedUsername)

		// Once the holder moves off it, the handle is claimable again.
		require.Equal(t, profilepb.SetUsernameResponse_OK, setUsername(&keyPair, "second_handle").Result)
		require.Equal(t, profilepb.SetUsernameResponse_OK, setUsername(&otherKeyPair, "first_handle").Result)
		require.Equal(t, "first_handle", usernameOf(otherUserID))
		require.Equal(t, "second_handle", usernameOf(userID))
	})
}

// testSetUsernameStaffGated covers the rollout gate: while it is on, claiming a
// handle is staff-only, and everyone else is denied however registered they are.
func testSetUsernameStaffGated(t *testing.T, accounts account.Store, profiles profile.Store) {
	ctx := context.Background()
	log := zaptest.NewLogger(t)

	authz := account.NewAuthorizer(log, accounts, auth.NewKeyPairAuthenticator(log))
	media, _, _ := newMedia()

	newClient := func(accounts account.Store) profilepb.ProfileClient {
		t.Helper()
		serv := profile.NewServer(log, authz, accounts, profiles, media, &fakeModerator{}, x.NewClient(), true)
		cc := testutil.RunGRPCServer(t, log, testutil.WithService(func(s *grpc.Server) {
			profilepb.RegisterProfileServer(s, serv)
		}))
		return profilepb.NewProfileClient(cc)
	}

	setUsername := func(client profilepb.ProfileClient, keyPair *model.KeyPair, username string) *profilepb.SetUsernameResponse {
		t.Helper()
		req := &profilepb.SetUsernameRequest{Username: &commonpb.Username{Value: username}}
		require.NoError(t, keyPair.Auth(req, &req.Auth))
		resp, err := client.SetUsername(ctx, req)
		require.NoError(t, err)
		return resp
	}

	userID := model.MustGenerateUserID()
	keyPair := model.MustGenerateKeyPair()
	_, err := accounts.Bind(ctx, userID, keyPair.Proto())
	require.NoError(t, err)
	require.NoError(t, accounts.SetRegistrationFlag(ctx, userID, true))

	t.Run("Non-staff user is denied", func(t *testing.T) {
		resp := setUsername(newClient(accounts), &keyPair, "gated_handle")
		require.NoError(t, protoutil.ProtoEqualError(&profilepb.SetUsernameResponse{Result: profilepb.SetUsernameResponse_DENIED}, resp))

		getResp, err := newClient(accounts).GetProfile(ctx, &profilepb.GetProfileRequest{Identifier: &profilepb.GetProfileRequest_UserId{UserId: userID}})
		require.NoError(t, err)
		require.Nil(t, getResp.GetUserProfile().GetUsername())
	})

	// Neither account store lets a test flag a user as staff, so the staff answer is
	// substituted to cover the other side of the gate.
	t.Run("Staff user is allowed", func(t *testing.T) {
		client := newClient(&staffAccounts{Store: accounts})

		resp := setUsername(client, &keyPair, "gated_handle")
		require.NoError(t, protoutil.ProtoEqualError(&profilepb.SetUsernameResponse{Result: profilepb.SetUsernameResponse_OK}, resp))

		getResp, err := client.GetProfile(ctx, &profilepb.GetProfileRequest{Identifier: &profilepb.GetProfileRequest_UserId{UserId: userID}})
		require.NoError(t, err)
		require.Equal(t, "gated_handle", getResp.GetUserProfile().GetUsername().GetValue())
	})
}

// staffAccounts answers every staff check with yes, leaving the rest of the store
// as it is. It stands in for a staff flag the account stores expose no setter for.
type staffAccounts struct {
	account.Store
}

func (s *staffAccounts) IsStaff(context.Context, *commonpb.UserId) (bool, error) {
	return true, nil
}

func testUsernameModeration(t *testing.T, accounts account.Store, profiles profile.Store) {
	ctx := context.Background()
	log := zaptest.NewLogger(t)

	authz := account.NewAuthorizer(log, accounts, auth.NewKeyPairAuthenticator(log))
	media, _, _ := newMedia()

	moderator := &fakeModerator{}
	serv := profile.NewServer(log, authz, accounts, profiles, media, moderator, x.NewClient(), false)
	cc := testutil.RunGRPCServer(t, log, testutil.WithService(func(s *grpc.Server) {
		profilepb.RegisterProfileServer(s, serv)
	}))
	client := profilepb.NewProfileClient(cc)

	userID := model.MustGenerateUserID()
	keyPair := model.MustGenerateKeyPair()
	_, err := accounts.Bind(ctx, userID, keyPair.Proto())
	require.NoError(t, err)
	require.NoError(t, accounts.SetRegistrationFlag(ctx, userID, true))

	setUsername := func(username string) (*profilepb.SetUsernameResponse, error) {
		t.Helper()
		req := &profilepb.SetUsernameRequest{Username: &commonpb.Username{Value: username}}
		require.NoError(t, keyPair.Auth(req, &req.Auth))
		return client.SetUsername(ctx, req)
	}

	username := func() string {
		t.Helper()
		resp, err := client.GetProfile(ctx, &profilepb.GetProfileRequest{Identifier: &profilepb.GetProfileRequest_UserId{UserId: userID}})
		require.NoError(t, err)
		return resp.GetUserProfile().GetUsername().GetValue()
	}

	// Each subtest configures the moderator from a clean slate, so a verdict left
	// behind by an earlier one cannot be what makes a later one pass.
	reset := func() {
		*moderator = fakeModerator{}
	}

	t.Run("Clean handle is moderated and claimed", func(t *testing.T) {
		reset()

		resp, err := setUsername("clean_handle")
		require.NoError(t, err)
		require.Equal(t, profilepb.SetUsernameResponse_OK, resp.Result)
		require.Equal(t, "clean_handle", username())

		// The handle itself is what was judged, not some other rendering of it.
		require.Equal(t, "clean_handle", moderator.classifiedUsername)
	})

	t.Run("Handle flagged as a username is rejected and not claimed", func(t *testing.T) {
		reset()
		moderator.usernameFlagged = true
		moderator.usernameCategories = []string{"official_role"}

		resp, err := setUsername("support_team")
		require.NoError(t, err)
		require.Equal(t, profilepb.SetUsernameResponse_FAILED_MODERATED, resp.Result)
		require.Equal(t, moderationpb.FlaggedCategory_IMPERSONATION, resp.FlaggedCategory)

		// The handle the user already held is left untouched, and the flagged one is
		// unclaimed — nobody can reach a profile by it.
		require.Equal(t, "clean_handle", username())
		getResp, err := client.GetProfile(ctx, &profilepb.GetProfileRequest{
			Identifier: &profilepb.GetProfileRequest_Username{Username: &commonpb.Username{Value: "support_team"}},
		})
		require.NoError(t, err)
		require.Equal(t, profilepb.GetProfileResponse_NOT_FOUND, getResp.Result)
	})

	t.Run("Handle flagged as text is rejected and not claimed", func(t *testing.T) {
		reset()
		moderator.textFlagged = true
		moderator.textCategories = []string{"general_nsfw"}

		resp, err := setUsername("bad_handle")
		require.NoError(t, err)
		require.Equal(t, profilepb.SetUsernameResponse_FAILED_MODERATED, resp.Result)
		require.Equal(t, moderationpb.FlaggedCategory_NSFW, resp.FlaggedCategory)

		require.Equal(t, "clean_handle", username())
	})

	t.Run("Username category is reported when both classifiers flag", func(t *testing.T) {
		reset()
		moderator.textFlagged = true
		moderator.textCategories = []string{"general_nsfw"}
		moderator.usernameFlagged = true
		moderator.usernameCategories = []string{"official_role"}

		resp, err := setUsername("bad_admin")
		require.NoError(t, err)
		require.Equal(t, profilepb.SetUsernameResponse_FAILED_MODERATED, resp.Result)
		require.Equal(t, moderationpb.FlaggedCategory_IMPERSONATION, resp.FlaggedCategory)

		require.Equal(t, "clean_handle", username())
	})

	t.Run("Handle the text classifier cannot identify a language for is still allowed", func(t *testing.T) {
		reset()
		// A handle is short and has no whitespace, so the text classifier routinely
		// has too little to work with. The username classifier still covers it.
		moderator.textErr = moderation.ErrUnsupportedLanguage

		resp, err := setUsername("unjudged_text")
		require.NoError(t, err)
		require.Equal(t, profilepb.SetUsernameResponse_OK, resp.Result)
		require.Equal(t, "unjudged_text", username())
	})

	t.Run("Handle the text classifier fails on fails closed", func(t *testing.T) {
		reset()
		moderator.textErr = errors.New("classifier unavailable")

		_, err := setUsername("unclassifiable")
		require.Equal(t, codes.Internal, status.Code(err))

		require.Equal(t, "unjudged_text", username())
	})

	t.Run("Handle the username classifier fails on fails closed", func(t *testing.T) {
		reset()
		moderator.usernameErr = errors.New("classifier unavailable")

		_, err := setUsername("unclassifiable")
		require.Equal(t, codes.Internal, status.Code(err))

		require.Equal(t, "unjudged_text", username())
	})
}

func testProfilePicture(t *testing.T, accounts account.Store, profiles profile.Store) {
	ctx := context.Background()
	log := zaptest.NewLogger(t)

	authz := account.NewAuthorizer(log, accounts, auth.NewKeyPairAuthenticator(log))

	media, blobs, access := newMedia()
	serv := profile.NewServer(log, authz, accounts, profiles, media, &fakeModerator{}, x.NewClient(), false)
	cc := testutil.RunGRPCServer(t, log, testutil.WithService(func(s *grpc.Server) {
		profilepb.RegisterProfileServer(s, serv)
	}))

	client := profilepb.NewProfileClient(cc)

	userID := model.MustGenerateUserID()
	keyPair := model.MustGenerateKeyPair()
	_, err := accounts.Bind(ctx, userID, keyPair.Proto())
	require.NoError(t, err)

	setProfilePicture := func(blobID *blobpb.BlobId) *profilepb.SetProfilePictureResponse {
		t.Helper()
		req := &profilepb.SetProfilePictureRequest{BlobId: blobID}
		require.NoError(t, keyPair.Auth(req, &req.Auth))
		resp, err := client.SetProfilePicture(ctx, req)
		require.NoError(t, err)
		return resp
	}

	// The profile principal the pictures are granted to. Asserting on the grant
	// directly is what proves a picture is actually readable — and, once
	// superseded, that it is not.
	principal := blob.PrincipalForProfile(userID)
	isGranted := func(blobID *blobpb.BlobId) bool {
		t.Helper()
		granted, err := access.HasGrant(ctx, blobID, principal, blob.PermissionRead)
		require.NoError(t, err)
		return granted
	}

	t.Run("Unregistered user is denied", func(t *testing.T) {
		blobID := seedBlob(t, blobs, userID, blob.StateReady, "image/jpeg")

		resp := setProfilePicture(blobID)
		require.Equal(t, profilepb.SetProfilePictureResponse_DENIED, resp.Result)
		require.Nil(t, resp.ProfilePicture)

		// Nothing was granted, so a denied picture is not readable through the profile.
		require.False(t, isGranted(blobID))
	})

	require.NoError(t, accounts.SetRegistrationFlag(ctx, userID, true))

	t.Run("Blob must be usable", func(t *testing.T) {
		otherUser := model.MustGenerateUserID()

		for _, tc := range []struct {
			name     string
			blobID   *blobpb.BlobId
			expected profilepb.SetProfilePictureResponse_Result
		}{
			{
				name:     "no such blob",
				blobID:   blob.MustGenerateID(),
				expected: profilepb.SetProfilePictureResponse_BLOB_NOT_FOUND,
			},
			{
				// Owned by someone else: indistinguishable from absent, so the id of
				// another user's blob cannot be probed for existence.
				name:     "owned by another user",
				blobID:   seedBlob(t, blobs, otherUser, blob.StateReady, "image/jpeg"),
				expected: profilepb.SetProfilePictureResponse_BLOB_NOT_FOUND,
			},
			{
				name:     "still processing",
				blobID:   seedBlob(t, blobs, userID, blob.StateUploaded, "image/jpeg"),
				expected: profilepb.SetProfilePictureResponse_BLOB_NOT_READY,
			},
			{
				name:     "failed moderation",
				blobID:   seedBlob(t, blobs, userID, blob.StateRejected, "image/jpeg"),
				expected: profilepb.SetProfilePictureResponse_BLOB_REJECTED,
			},
			{
				name:     "not an image",
				blobID:   seedBlob(t, blobs, userID, blob.StateReady, "application/pdf"),
				expected: profilepb.SetProfilePictureResponse_INVALID_BLOB,
			},
		} {
			t.Run(tc.name, func(t *testing.T) {
				resp := setProfilePicture(tc.blobID)
				require.Equal(t, tc.expected, resp.Result)
				require.Nil(t, resp.ProfilePicture)
				require.False(t, isGranted(tc.blobID))
			})
		}

		// None of the failures left a picture behind.
		getResp, err := client.GetProfile(ctx, &profilepb.GetProfileRequest{Identifier: &profilepb.GetProfileRequest_UserId{UserId: userID}})
		require.NoError(t, err)
		require.Nil(t, getResp.UserProfile.GetProfilePicture())
	})

	first := seedBlob(t, blobs, userID, blob.StateReady, "image/jpeg")

	t.Run("Set a picture", func(t *testing.T) {
		resp := setProfilePicture(first)
		require.Equal(t, profilepb.SetProfilePictureResponse_OK, resp.Result)

		// This blob has no derived renditions (no manifest was attached), so only the
		// ORIGINAL resolves; expansion of a full set is covered separately below.
		require.Len(t, resp.ProfilePicture.Renditions, 1)
		rendition := resp.ProfilePicture.Renditions[0]
		require.Equal(t, blobpb.Rendition_ORIGINAL, rendition.Role)
		require.Equal(t, first.Value, rendition.BlobId.Value)

		// The response carries resolved metadata, so a client needs no second round trip.
		require.NotNil(t, rendition.Blob)
		require.Equal(t, "image/jpeg", rendition.Blob.MimeType)
		require.NotEmpty(t, rendition.Blob.DownloadUrl.GetUrl())

		require.True(t, isGranted(first))
	})

	t.Run("Get hydrates the picture", func(t *testing.T) {
		// Unauthenticated: a profile picture is public.
		getResp, err := client.GetProfile(ctx, &profilepb.GetProfileRequest{Identifier: &profilepb.GetProfileRequest_UserId{UserId: userID}})
		require.NoError(t, err)
		require.Equal(t, profilepb.GetProfileResponse_OK, getResp.Result)

		renditions := getResp.UserProfile.GetProfilePicture().GetRenditions()
		require.Len(t, renditions, 1)
		require.Equal(t, first.Value, renditions[0].BlobId.Value)
		require.NotEmpty(t, renditions[0].Blob.GetDownloadUrl().GetUrl())
	})

	t.Run("Replace a picture", func(t *testing.T) {
		second := seedBlob(t, blobs, userID, blob.StateReady, "image/png")

		resp := setProfilePicture(second)
		require.Equal(t, profilepb.SetProfilePictureResponse_OK, resp.Result)
		require.Equal(t, second.Value, resp.ProfilePicture.Renditions[0].BlobId.Value)

		// The profile now serves the new picture.
		getResp, err := client.GetProfile(ctx, &profilepb.GetProfileRequest{Identifier: &profilepb.GetProfileRequest_UserId{UserId: userID}})
		require.NoError(t, err)
		require.Equal(t, second.Value, getResp.UserProfile.GetProfilePicture().GetRenditions()[0].BlobId.Value)

		// Grants are never revoked, so the superseded picture stays readable through
		// the profile to anyone still holding its blob id.
		require.True(t, isGranted(second))
		require.True(t, isGranted(first))
	})

	t.Run("Setting the same picture again is idempotent", func(t *testing.T) {
		current := seedBlob(t, blobs, userID, blob.StateReady, "image/webp")
		require.Equal(t, profilepb.SetProfilePictureResponse_OK, setProfilePicture(current).Result)
		require.Equal(t, profilepb.SetProfilePictureResponse_OK, setProfilePicture(current).Result)
		require.True(t, isGranted(current))
	})

	t.Run("A picture with derived renditions expands on read", func(t *testing.T) {
		// A READY original that carries a derived DISPLAY in its manifest.
		original := seedBlob(t, blobs, userID, blob.StateReady, "image/jpeg")
		displayID := blob.MustGenerateID()
		require.NoError(t, blobs.AttachRenditions(ctx, original, []blob.RenditionRef{{
			ID:         displayID,
			Rendition:  blob.RenditionDisplay,
			MimeType:   "image/jpeg",
			SizeBytes:  64,
			StorageKey: "images/x/display_800x600.jpg",
			Image:      &blob.ImageMetadata{Width: 800, Height: 600},
		}}))

		requireRenditionSet := func(t *testing.T, renditions []*blobpb.Rendition) {
			t.Helper()
			require.Len(t, renditions, 2)

			require.Equal(t, blobpb.Rendition_ORIGINAL, renditions[0].Role)
			require.Equal(t, original.Value, renditions[0].BlobId.Value)
			require.NotEmpty(t, renditions[0].Blob.GetDownloadUrl().GetUrl())

			require.Equal(t, blobpb.Rendition_DISPLAY, renditions[1].Role)
			require.Equal(t, displayID.Value, renditions[1].BlobId.Value)
			require.Equal(t, "image/jpeg", renditions[1].Blob.MimeType)
			require.EqualValues(t, 800, renditions[1].Blob.GetImage().GetWidth())
			require.NotEmpty(t, renditions[1].Blob.GetDownloadUrl().GetUrl())
		}

		// The Set response carries the full set...
		resp := setProfilePicture(original)
		require.Equal(t, profilepb.SetProfilePictureResponse_OK, resp.Result)
		requireRenditionSet(t, resp.ProfilePicture.GetRenditions())

		// ...and so does the public Get.
		getResp, err := client.GetProfile(ctx, &profilepb.GetProfileRequest{Identifier: &profilepb.GetProfileRequest_UserId{UserId: userID}})
		require.NoError(t, err)
		require.Equal(t, profilepb.GetProfileResponse_OK, getResp.Result)
		requireRenditionSet(t, getResp.UserProfile.GetProfilePicture().GetRenditions())
	})
}

func testDisplayNameModeration(t *testing.T, accounts account.Store, profiles profile.Store) {
	ctx := context.Background()
	log := zaptest.NewLogger(t)

	authz := account.NewAuthorizer(log, accounts, auth.NewKeyPairAuthenticator(log))
	media, _, _ := newMedia()

	moderator := &fakeModerator{}
	serv := profile.NewServer(log, authz, accounts, profiles, media, moderator, x.NewClient(), false)
	cc := testutil.RunGRPCServer(t, log, testutil.WithService(func(s *grpc.Server) {
		profilepb.RegisterProfileServer(s, serv)
	}))
	client := profilepb.NewProfileClient(cc)

	userID := model.MustGenerateUserID()
	keyPair := model.MustGenerateKeyPair()
	_, err := accounts.Bind(ctx, userID, keyPair.Proto())
	require.NoError(t, err)
	require.NoError(t, accounts.SetRegistrationFlag(ctx, userID, true))

	setDisplayName := func(name string) (*profilepb.SetDisplayNameResponse, error) {
		t.Helper()
		req := &profilepb.SetDisplayNameRequest{DisplayName: name}
		require.NoError(t, keyPair.Auth(req, &req.Auth))
		return client.SetDisplayName(ctx, req)
	}

	displayName := func() string {
		t.Helper()
		resp, err := client.GetProfile(ctx, &profilepb.GetProfileRequest{Identifier: &profilepb.GetProfileRequest_UserId{UserId: userID}})
		require.NoError(t, err)
		return resp.GetUserProfile().GetDisplayName()
	}

	// Each subtest configures the moderator from a clean slate, so a verdict left
	// behind by an earlier one cannot be what makes a later one pass.
	reset := func() {
		*moderator = fakeModerator{}
	}

	t.Run("Clean name is moderated and persisted", func(t *testing.T) {
		reset()

		resp, err := setDisplayName("clean name")
		require.NoError(t, err)
		require.Equal(t, profilepb.SetDisplayNameResponse_OK, resp.Result)
		require.Equal(t, "clean name", displayName())
	})

	t.Run("Name flagged as text is rejected and not persisted", func(t *testing.T) {
		reset()
		moderator.textFlagged = true
		moderator.textCategories = []string{"general_nsfw"}

		resp, err := setDisplayName("bad name")
		require.NoError(t, err)
		require.Equal(t, profilepb.SetDisplayNameResponse_FAILED_MODERATED, resp.Result)
		require.Equal(t, moderationpb.FlaggedCategory_NSFW, resp.FlaggedCategory)

		// The prior clean name is left untouched.
		require.Equal(t, "clean name", displayName())
	})

	t.Run("Name flagged as a display name is rejected and not persisted", func(t *testing.T) {
		reset()
		moderator.displayNameFlagged = true
		moderator.displayNameCategories = []string{"solicitation"}

		resp, err := setDisplayName("dm me for signals")
		require.NoError(t, err)
		require.Equal(t, profilepb.SetDisplayNameResponse_FAILED_MODERATED, resp.Result)
		require.Equal(t, moderationpb.FlaggedCategory_SPAM, resp.FlaggedCategory)

		require.Equal(t, "clean name", displayName())
	})

	t.Run("Display name category is reported when both classifiers flag", func(t *testing.T) {
		reset()
		moderator.textFlagged = true
		moderator.textCategories = []string{"general_nsfw"}
		moderator.displayNameFlagged = true
		moderator.displayNameCategories = []string{"solicitation"}

		resp, err := setDisplayName("bad name, dm me")
		require.NoError(t, err)
		require.Equal(t, profilepb.SetDisplayNameResponse_FAILED_MODERATED, resp.Result)
		require.Equal(t, moderationpb.FlaggedCategory_SPAM, resp.FlaggedCategory)

		require.Equal(t, "clean name", displayName())
	})

	t.Run("Name the text classifier cannot identify a language for is still allowed", func(t *testing.T) {
		reset()
		// A short name gives the text classifier too little to work with. The
		// display-name classifier still covers it, so this is not fatal.
		moderator.textErr = moderation.ErrUnsupportedLanguage

		resp, err := setDisplayName("네네네")
		require.NoError(t, err)
		require.Equal(t, profilepb.SetDisplayNameResponse_OK, resp.Result)
		require.Equal(t, "네네네", displayName())
	})

	t.Run("Name the text classifier fails on fails closed", func(t *testing.T) {
		reset()
		moderator.textErr = errors.New("classifier unavailable")

		_, err := setDisplayName("unclassifiable")
		require.Equal(t, codes.Internal, status.Code(err))

		require.Equal(t, "네네네", displayName())
	})

	t.Run("Name the display name classifier fails on fails closed", func(t *testing.T) {
		reset()
		moderator.displayNameErr = errors.New("classifier unavailable")

		_, err := setDisplayName("unclassifiable")
		require.Equal(t, codes.Internal, status.Code(err))

		require.Equal(t, "네네네", displayName())
	})
}

// fakeModerator is a configurable moderation.Client for the display-name and
// username tests. Each of those paths runs the general text classifier alongside
// its own, so every classifier is configured independently; ClassifyImage and
// ClassifyCurrencyName are here only to satisfy the interface.
type fakeModerator struct {
	textFlagged    bool
	textCategories []string
	textErr        error

	displayNameFlagged    bool
	displayNameCategories []string
	displayNameErr        error

	usernameFlagged    bool
	usernameCategories []string
	usernameErr        error

	// classifiedUsername records the handle ClassifyUsername last saw, so a test
	// can assert the moderator judged the canonical form rather than what was typed.
	classifiedUsername string
}

func (m *fakeModerator) ClassifyText(context.Context, string) (*moderation.Result, error) {
	return fakeResult(m.textFlagged, m.textCategories, m.textErr)
}

func (m *fakeModerator) ClassifyDisplayName(context.Context, string) (*moderation.Result, error) {
	return fakeResult(m.displayNameFlagged, m.displayNameCategories, m.displayNameErr)
}

func (m *fakeModerator) ClassifyImage(context.Context, []byte) (*moderation.Result, error) {
	return &moderation.Result{}, nil
}

func (m *fakeModerator) ClassifyCurrencyName(context.Context, string) (*moderation.Result, error) {
	return &moderation.Result{}, nil
}

func (m *fakeModerator) ClassifyUsername(_ context.Context, username string) (*moderation.Result, error) {
	m.classifiedUsername = username
	return fakeResult(m.usernameFlagged, m.usernameCategories, m.usernameErr)
}

func fakeResult(flagged bool, categories []string, err error) (*moderation.Result, error) {
	if err != nil {
		return nil, err
	}
	result := &moderation.Result{Flagged: flagged}
	if len(categories) > 0 {
		result.FlaggedCategories = categories
		result.CategoryScores = make(map[string]float64, len(categories))
		for i, category := range categories {
			result.CategoryScores[category] = float64(i + 1)
		}
	}
	return result, nil
}
