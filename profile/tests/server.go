package tests

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	blobpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/blob/v1"
	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"
	emailpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/email/v1"
	phonepb "github.com/code-payments/flipcash2-protobuf-api/generated/go/phone/v1"
	profilepb "github.com/code-payments/flipcash2-protobuf-api/generated/go/profile/v1"

	"github.com/code-payments/flipcash2-server/account"
	"github.com/code-payments/flipcash2-server/auth"
	"github.com/code-payments/flipcash2-server/blob"
	blobmemory "github.com/code-payments/flipcash2-server/blob/memory"
	"github.com/code-payments/flipcash2-server/model"
	"github.com/code-payments/flipcash2-server/profile"
	"github.com/code-payments/flipcash2-server/protoutil"
	"github.com/code-payments/flipcash2-server/social/x"
	"github.com/code-payments/flipcash2-server/testutil"
)

func RunServerTests(t *testing.T, accounts account.Store, profiles profile.Store, teardown func()) {
	for _, tf := range []func(t *testing.T, accounts account.Store, profiles profile.Store){
		testServer,
		testProfilePicture,
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

func testServer(t *testing.T, accounts account.Store, profiles profile.Store) {
	ctx := context.Background()
	log := zaptest.NewLogger(t)

	authz := account.NewAuthorizer(log, accounts, auth.NewKeyPairAuthenticator(log))

	media, _, _ := newMedia()
	serv := profile.NewServer(log, authz, accounts, profiles, media, x.NewClient())
	cc := testutil.RunGRPCServer(t, log, testutil.WithService(func(s *grpc.Server) {
		profilepb.RegisterProfileServer(s, serv)
	}))

	client := profilepb.NewProfileClient(cc)
	userID := model.MustGenerateUserID()
	keyPair := model.MustGenerateKeyPair()

	t.Run("No User", func(t *testing.T) {
		getResp, err := client.GetProfile(ctx, &profilepb.GetProfileRequest{
			UserId: userID,
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

		// Binding of a user isn't sufficient, a profile must be set!
		getResp, err := client.GetProfile(ctx, &profilepb.GetProfileRequest{
			UserId: userID,
		})
		require.NoError(t, err)
		require.Equal(t, profilepb.GetProfileResponse_NOT_FOUND, getResp.Result)
		require.Nil(t, getResp.UserProfile)

		setDisplayName := &profilepb.SetDisplayNameRequest{
			DisplayName: "my name",
		}
		require.NoError(t, keyPair.Auth(setDisplayName, &setDisplayName.Auth))
		setDisplayNameResp, err := client.SetDisplayName(ctx, setDisplayName)
		require.NoError(t, err)
		require.NoError(t, protoutil.ProtoEqualError(&profilepb.SetDisplayNameResponse{Result: profilepb.SetDisplayNameResponse_OK}, setDisplayNameResp))

		expected := &profilepb.UserProfile{DisplayName: "my name"}

		getResp, err = client.GetProfile(ctx, &profilepb.GetProfileRequest{
			UserId: userID,
		})
		require.NoError(t, err)
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
			UserId: userID,
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
			UserId: userID,
		})
		require.NoError(t, err)
		require.NoError(t, protoutil.ProtoEqualError(expected, getResp.UserProfile))

		t.Run("Private profile", func(t *testing.T) {
			require.NoError(t, profiles.LinkPhoneNumber(ctx, userID, "+12223334444", &commonpb.Hash{Value: []byte("phone-hash")}))
			require.NoError(t, profiles.LinkEmailAddress(ctx, userID, "someone@gmail.com"))

			get := &profilepb.GetProfileRequest{
				UserId: userID,
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

			expected.PhoneNumber = &phonepb.PhoneNumber{Value: "+12223334444"}
			expected.EmailAddress = &emailpb.EmailAddress{Value: "someone@gmail.com"}
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
			UserId: userID2,
		})
		require.NoError(t, err)
		require.NoError(t, protoutil.ProtoEqualError(&profilepb.GetProfileResponse{Result: profilepb.GetProfileResponse_NOT_FOUND}, get))
	})
}

func testProfilePicture(t *testing.T, accounts account.Store, profiles profile.Store) {
	ctx := context.Background()
	log := zaptest.NewLogger(t)

	authz := account.NewAuthorizer(log, accounts, auth.NewKeyPairAuthenticator(log))

	media, blobs, access := newMedia()
	serv := profile.NewServer(log, authz, accounts, profiles, media, x.NewClient())
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
		getResp, err := client.GetProfile(ctx, &profilepb.GetProfileRequest{UserId: userID})
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
		getResp, err := client.GetProfile(ctx, &profilepb.GetProfileRequest{UserId: userID})
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
		getResp, err := client.GetProfile(ctx, &profilepb.GetProfileRequest{UserId: userID})
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
		getResp, err := client.GetProfile(ctx, &profilepb.GetProfileRequest{UserId: userID})
		require.NoError(t, err)
		require.Equal(t, profilepb.GetProfileResponse_OK, getResp.Result)
		requireRenditionSet(t, getResp.UserProfile.GetProfilePicture().GetRenditions())
	})
}
