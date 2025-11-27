package tests

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	profilepb "github.com/code-payments/flipcash2-protobuf-api/generated/go/profile/v1"

	"github.com/code-payments/flipcash2-server/model"
	"github.com/code-payments/flipcash2-server/profile"
	"github.com/code-payments/flipcash2-server/protoutil"
)

func RunStoreTests(t *testing.T, s profile.Store, teardown func()) {
	for _, tf := range []func(t *testing.T, s profile.Store){
		testStore,
		testXProfiles,
	} {
		tf(t, s)
		teardown()
	}
}

func testStore(t *testing.T, s profile.Store) {
	ctx := context.Background()

	userID := model.MustGenerateUserID()

	_, err := s.GetProfile(ctx, userID, false)
	require.ErrorIs(t, err, profile.ErrNotFound)

	require.NoError(t, s.UnlinkPhoneNumber(ctx, userID, "+12223334444"))
	require.NoError(t, s.UnlinkEmailAddress(ctx, userID, "someone@gmail.com"))

	require.NoError(t, s.SetDisplayName(ctx, userID, "my name"))
	require.NoError(t, s.SetPhoneNumber(ctx, userID, "+12223334444"))
	require.NoError(t, s.SetEmailAddress(ctx, userID, "someone@gmail.com"))

	profile, err := s.GetProfile(ctx, userID, false)
	require.NoError(t, err)
	require.Equal(t, "my name", profile.DisplayName)

	require.NoError(t, s.SetDisplayName(ctx, userID, "my other name"))

	profile, err = s.GetProfile(ctx, userID, false)
	require.NoError(t, err)
	require.Equal(t, "my other name", profile.DisplayName)
	require.Nil(t, profile.PhoneNumber)
	require.Nil(t, profile.EmailAddress)

	profile, err = s.GetProfile(ctx, userID, true)
	require.NoError(t, err)
	require.Equal(t, "my other name", profile.DisplayName)
	require.Equal(t, "+12223334444", profile.PhoneNumber.Value)
	require.Equal(t, "someone@gmail.com", profile.EmailAddress.Value)

	require.NoError(t, s.UnlinkPhoneNumber(ctx, userID, "+15556667777"))
	require.NoError(t, s.UnlinkEmailAddress(ctx, userID, "someone.else@gmail.com"))

	profile, err = s.GetProfile(ctx, userID, true)
	require.NoError(t, err)
	require.Equal(t, "my other name", profile.DisplayName)
	require.Equal(t, "+12223334444", profile.PhoneNumber.Value)
	require.Equal(t, "someone@gmail.com", profile.EmailAddress.Value)

	require.NoError(t, s.UnlinkPhoneNumber(ctx, userID, "+12223334444"))

	profile, err = s.GetProfile(ctx, userID, true)
	require.NoError(t, err)
	require.Nil(t, profile.PhoneNumber)
	require.NotNil(t, profile.EmailAddress)

	require.NoError(t, s.UnlinkEmailAddress(ctx, userID, "someone@gmail.com"))

	profile, err = s.GetProfile(ctx, userID, true)
	require.NoError(t, err)
	require.Nil(t, profile.PhoneNumber)
	require.Nil(t, profile.EmailAddress)

}

func testXProfiles(t *testing.T, s profile.Store) {
	ctx := context.Background()

	userID1 := model.MustGenerateUserID()
	userID2 := model.MustGenerateUserID()
	require.NoError(t, s.SetDisplayName(ctx, userID1, "user1"))
	require.NoError(t, s.SetDisplayName(ctx, userID2, "user2"))

	_, err := s.GetXProfile(ctx, userID1)
	require.Equal(t, profile.ErrNotFound, err)

	// Link an initial X account to user 1
	expected1 := &profilepb.XProfile{
		Id:            "1",
		Username:      "username",
		Name:          "name",
		Description:   "description",
		ProfilePicUrl: "url",
		VerifiedType:  profilepb.XProfile_NONE,
		FollowerCount: 42,
	}
	require.NoError(t, s.LinkXAccount(ctx, userID1, expected1, "accessToken1"))

	/// Fail to link a new X account to user 1 (the original one is maintained)
	expected2 := &profilepb.XProfile{
		Id:            "2",
		Username:      "username2",
		Name:          "name2",
		Description:   "description2",
		ProfilePicUrl: "url2",
		VerifiedType:  profilepb.XProfile_BLUE,
		FollowerCount: 1_000_000,
	}
	require.Equal(t, profile.ErrExistingSocialLink, s.LinkXAccount(ctx, userID1, expected2, "accessToken2"))

	actual, err := s.GetXProfile(ctx, userID1)
	require.NoError(t, err)
	require.NoError(t, protoutil.ProtoEqualError(expected1, actual))

	fullProfile, err := s.GetProfile(ctx, userID1, false)
	require.NoError(t, err)
	require.NoError(t, protoutil.ProtoEqualError(expected1, fullProfile.SocialProfiles[0].GetX()))

	// Link the original X account to user 2, which removes the link from user 1
	require.NoError(t, s.LinkXAccount(ctx, userID2, expected1, "accessToken3"))

	_, err = s.GetXProfile(ctx, userID1)
	require.Equal(t, profile.ErrNotFound, err)

	actual, err = s.GetXProfile(ctx, userID2)
	require.NoError(t, err)
	require.NoError(t, protoutil.ProtoEqualError(expected1, actual))

	fullProfile, err = s.GetProfile(ctx, userID2, false)
	require.NoError(t, err)
	require.NoError(t, protoutil.ProtoEqualError(expected1, fullProfile.SocialProfiles[0].GetX()))

	// Relink the X account with updated user metadata, which should cause a refresh
	expected3 := &profilepb.XProfile{
		Id:            expected1.Id,
		Username:      "username3",
		Name:          "name3",
		Description:   "description3",
		ProfilePicUrl: "url3",
		VerifiedType:  profilepb.XProfile_NONE,
		FollowerCount: 123,
	}
	require.NoError(t, s.LinkXAccount(ctx, userID2, expected3, "accessToken4"))

	actual, err = s.GetXProfile(ctx, userID2)
	require.NoError(t, err)
	require.NoError(t, protoutil.ProtoEqualError(expected3, actual))

	fullProfile, err = s.GetProfile(ctx, userID2, false)
	require.NoError(t, err)
	require.NoError(t, protoutil.ProtoEqualError(expected3, fullProfile.SocialProfiles[0].GetX()))

	require.Equal(t, profile.ErrNotFound, s.UnlinkXAccount(ctx, userID2, "not found"))
	require.Equal(t, profile.ErrNotFound, s.UnlinkXAccount(ctx, userID1, expected3.Id))

	actual, err = s.GetXProfile(ctx, userID2)
	require.NoError(t, err)
	require.NoError(t, protoutil.ProtoEqualError(expected3, actual))

	fullProfile, err = s.GetProfile(ctx, userID2, false)
	require.NoError(t, err)
	require.NoError(t, protoutil.ProtoEqualError(expected3, fullProfile.SocialProfiles[0].GetX()))

	require.NoError(t, s.UnlinkXAccount(ctx, userID2, expected3.Id))

	_, err = s.GetXProfile(ctx, userID2)
	require.Equal(t, profile.ErrNotFound, err)

	fullProfile, err = s.GetProfile(ctx, userID2, false)
	require.NoError(t, err)
	require.Empty(t, fullProfile.SocialProfiles)
}
