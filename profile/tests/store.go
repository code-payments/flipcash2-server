package tests

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	blobpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/blob/v1"
	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"
	profilepb "github.com/code-payments/flipcash2-protobuf-api/generated/go/profile/v1"

	"github.com/code-payments/flipcash2-server/blob"
	"github.com/code-payments/flipcash2-server/model"
	"github.com/code-payments/flipcash2-server/profile"
	"github.com/code-payments/flipcash2-server/protoutil"
)

func RunStoreTests(t *testing.T, s profile.Store, teardown func()) {
	for _, tf := range []func(t *testing.T, s profile.Store){
		testStore,
		testXProfiles,
		testPhoneEmailTransfer,
		testGetPhonesByHashes,
		testGetPhoneNumbersForPayment,
		testGetPublicProfiles,
		testGetUserIdByPhoneNumber,
		testLinkPhoneNumberForPayment,
		testProfilePictures,
		testTipCardColor,
		testUsername,
		testJoinTs,
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
	require.NoError(t, s.LinkPhoneNumber(ctx, userID, "+12223334444", &commonpb.Hash{Value: []byte("phone-hash")}))
	require.NoError(t, s.LinkEmailAddress(ctx, userID, "someone@gmail.com"))

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

func testPhoneEmailTransfer(t *testing.T, s profile.Store) {
	ctx := context.Background()

	userID1 := model.MustGenerateUserID()
	userID2 := model.MustGenerateUserID()

	const phone = "+12223334444"
	const email = "someone@gmail.com"
	phoneHash := &commonpb.Hash{Value: []byte("phone-hash")}

	require.NoError(t, s.SetDisplayName(ctx, userID1, "user1"))
	require.NoError(t, s.SetDisplayName(ctx, userID2, "user2"))

	require.NoError(t, s.LinkPhoneNumber(ctx, userID1, phone, phoneHash))
	require.NoError(t, s.LinkEmailAddress(ctx, userID1, email))

	p, err := s.GetProfile(ctx, userID1, true)
	require.NoError(t, err)
	require.Equal(t, phone, p.PhoneNumber.Value)
	require.Equal(t, email, p.EmailAddress.Value)

	// Re-claim both on user2; user1 should lose them.
	require.NoError(t, s.LinkPhoneNumber(ctx, userID2, phone, phoneHash))
	require.NoError(t, s.LinkEmailAddress(ctx, userID2, email))

	p, err = s.GetProfile(ctx, userID1, true)
	require.NoError(t, err)
	require.Nil(t, p.PhoneNumber)
	require.Nil(t, p.EmailAddress)

	p, err = s.GetProfile(ctx, userID2, true)
	require.NoError(t, err)
	require.Equal(t, phone, p.PhoneNumber.Value)
	require.Equal(t, email, p.EmailAddress.Value)

	// Setting the same value on the same user is a no-op (no spurious clear).
	require.NoError(t, s.LinkPhoneNumber(ctx, userID2, phone, phoneHash))
	require.NoError(t, s.LinkEmailAddress(ctx, userID2, email))

	p, err = s.GetProfile(ctx, userID2, true)
	require.NoError(t, err)
	require.Equal(t, phone, p.PhoneNumber.Value)
	require.Equal(t, email, p.EmailAddress.Value)
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

func testGetPhonesByHashes(t *testing.T, s profile.Store) {
	ctx := context.Background()

	user1 := model.MustGenerateUserID()
	user2 := model.MustGenerateUserID()
	user3 := model.MustGenerateUserID()

	hash1 := &commonpb.Hash{Value: []byte("hash1")}
	hash2 := &commonpb.Hash{Value: []byte("hash2")}
	hash3 := &commonpb.Hash{Value: []byte("hash3")}
	missing := &commonpb.Hash{Value: []byte("hash-miss")}

	require.NoError(t, s.SetDisplayName(ctx, user1, "u1"))
	require.NoError(t, s.SetDisplayName(ctx, user2, "u2"))
	require.NoError(t, s.SetDisplayName(ctx, user3, "u3"))

	require.NoError(t, s.LinkPhoneNumber(ctx, user1, "+11111111111", hash1))
	require.NoError(t, s.LinkPhoneNumber(ctx, user2, "+12222222222", hash2))
	require.NoError(t, s.LinkPhoneNumber(ctx, user3, "+13333333333", hash3))

	// Subset hit + one miss.
	got, err := s.GetPhonesByHashes(ctx, []*commonpb.Hash{hash1, hash3, missing})
	require.NoError(t, err)
	require.ElementsMatch(t,
		[]string{"+11111111111", "+13333333333"},
		phoneValues(got),
	)

	// Empty input.
	got, err = s.GetPhonesByHashes(ctx, nil)
	require.NoError(t, err)
	require.Empty(t, got)

	// All misses.
	got, err = s.GetPhonesByHashes(ctx, []*commonpb.Hash{missing})
	require.NoError(t, err)
	require.Empty(t, got)

	// The payment-only variant excludes users that have not enabled their number
	// for payment.
	gotPay, err := s.GetPhonesByHashesForPayment(ctx, []*commonpb.Hash{hash1, hash2, hash3})
	require.NoError(t, err)
	require.Empty(t, gotPay)

	flipped, err := s.LinkPhoneNumberForPayment(ctx, user1, "+11111111111")
	require.NoError(t, err)
	require.True(t, flipped)
	flipped, err = s.LinkPhoneNumberForPayment(ctx, user3, "+13333333333")
	require.NoError(t, err)
	require.True(t, flipped)

	// Only the enabled numbers are returned (user2 is still excluded), each
	// paired with the user that owns it.
	gotPay, err = s.GetPhonesByHashesForPayment(ctx, []*commonpb.Hash{hash1, hash2, hash3, missing})
	require.NoError(t, err)
	require.ElementsMatch(t,
		[]string{"+11111111111", "+13333333333"},
		paymentPhoneValues(gotPay),
	)
	ownerByPhone := make(map[string][]byte, len(gotPay))
	for _, p := range gotPay {
		ownerByPhone[p.PhoneNumber.Value] = p.UserID.Value
		require.False(t, p.JoinedAt.IsZero())
	}
	require.Equal(t, user1.Value, ownerByPhone["+11111111111"])
	require.Equal(t, user3.Value, ownerByPhone["+13333333333"])

	// Empty input is still handled.
	gotPay, err = s.GetPhonesByHashesForPayment(ctx, nil)
	require.NoError(t, err)
	require.Empty(t, gotPay)
}

func testGetPhoneNumbersForPayment(t *testing.T, s profile.Store) {
	ctx := context.Background()

	user1 := model.MustGenerateUserID()
	user2 := model.MustGenerateUserID()
	user3 := model.MustGenerateUserID()
	noPhone := model.MustGenerateUserID()

	require.NoError(t, s.SetDisplayName(ctx, user1, "u1"))
	require.NoError(t, s.SetDisplayName(ctx, user2, "u2"))
	require.NoError(t, s.SetDisplayName(ctx, user3, "u3"))

	require.NoError(t, s.LinkPhoneNumber(ctx, user1, "+11111111111", &commonpb.Hash{Value: []byte("hash1")}))
	require.NoError(t, s.LinkPhoneNumber(ctx, user2, "+12222222222", &commonpb.Hash{Value: []byte("hash2")}))
	require.NoError(t, s.LinkPhoneNumber(ctx, user3, "+13333333333", &commonpb.Hash{Value: []byte("hash3")}))

	// Empty input.
	got, err := s.GetPhoneNumbersForPayment(ctx, nil)
	require.NoError(t, err)
	require.Empty(t, got)

	// Nobody is linked for payment yet.
	got, err = s.GetPhoneNumbersForPayment(ctx, []*commonpb.UserId{user1, user2, user3})
	require.NoError(t, err)
	require.Empty(t, got)

	// Enable two of the three for payment.
	_, err = s.LinkPhoneNumberForPayment(ctx, user1, "+11111111111")
	require.NoError(t, err)
	_, err = s.LinkPhoneNumberForPayment(ctx, user3, "+13333333333")
	require.NoError(t, err)

	// Only the payment-enabled users are returned, keyed by user ID. user2 is
	// excluded (linked but not for payment), and noPhone has no number at all.
	got, err = s.GetPhoneNumbersForPayment(ctx, []*commonpb.UserId{user1, user2, user3, noPhone})
	require.NoError(t, err)
	require.Len(t, got, 2)
	require.Equal(t, "+11111111111", got[string(user1.Value)].Value)
	require.Equal(t, "+13333333333", got[string(user3.Value)].Value)
	_, ok := got[string(user2.Value)]
	require.False(t, ok)
	_, ok = got[string(noPhone.Value)]
	require.False(t, ok)
}

func testTipCardColor(t *testing.T, s profile.Store) {
	ctx := context.Background()

	user1 := model.MustGenerateUserID()
	user2 := model.MustGenerateUserID()

	// A user who has picked no colour still reads back a complete customization,
	// resolved from the default.
	require.NoError(t, s.SetDisplayName(ctx, user1, "user one"))
	p, err := s.GetProfile(ctx, user1, false)
	require.NoError(t, err)
	require.NoError(t, protoutil.ProtoEqualError(profile.DefaultTipCardCustomization(), p.TipCardCustomization))

	// Setting a colour is enough on its own to make the store know a user, the
	// same way setting any other profile field is.
	require.NoError(t, s.SetTipCardColor(ctx, user2, "#19191A"))
	p, err = s.GetProfile(ctx, user2, false)
	require.NoError(t, err)
	require.Equal(t, "#19191A", p.TipCardCustomization.Color.Hex)

	// Colours are per user: user2's choice does not reach user1.
	p, err = s.GetProfile(ctx, user1, false)
	require.NoError(t, err)
	require.Equal(t, profile.DefaultTipCardColorHex, p.TipCardCustomization.Color.Hex)

	// A second set replaces the first rather than accumulating.
	require.NoError(t, s.SetTipCardColor(ctx, user2, "#FFFFFF"))
	p, err = s.GetProfile(ctx, user2, false)
	require.NoError(t, err)
	require.Equal(t, "#FFFFFF", p.TipCardCustomization.Color.Hex)

	// Whatever casing reached the store, reads are canonical.
	require.NoError(t, s.SetTipCardColor(ctx, user2, "#abcdef"))
	p, err = s.GetProfile(ctx, user2, false)
	require.NoError(t, err)
	require.Equal(t, "#ABCDEF", p.TipCardCustomization.Color.Hex)

	// The batch read resolves the same way as the single one — it is the path
	// chat member rows are built from, so a default missed here would leave a
	// member row failing validation.
	got, err := s.GetPublicProfiles(ctx, []*commonpb.UserId{user1, user2})
	require.NoError(t, err)
	require.Len(t, got, 2)
	require.NoError(t, got[string(user1.Value)].Validate())
	require.NoError(t, got[string(user2.Value)].Validate())
	require.Equal(t, profile.DefaultTipCardColorHex, got[string(user1.Value)].TipCardCustomization.Color.Hex)
	require.Equal(t, "#ABCDEF", got[string(user2.Value)].TipCardCustomization.Color.Hex)
}

func testUsername(t *testing.T, s profile.Store) {
	ctx := context.Background()

	user1 := model.MustGenerateUserID()
	user2 := model.MustGenerateUserID()

	// A user who has claimed nothing has no handle, and their profile is still a
	// complete one.
	require.NoError(t, s.SetDisplayName(ctx, user1, "user one"))
	p, err := s.GetProfile(ctx, user1, false)
	require.NoError(t, err)
	require.Nil(t, p.Username)
	require.NoError(t, p.Validate())

	// A handle nobody has claimed resolves to nobody.
	_, err = s.GetUserIdByUsername(ctx, "user_one")
	require.ErrorIs(t, err, profile.ErrNotFound)

	// Only canonical handles are ever held, so nothing else reaches the store.
	for _, invalid := range []string{"", "a", "Uppercase", "has-a-dash", "has space", "sixteencharacter"} {
		require.ErrorIs(t, s.SetUsername(ctx, user1, invalid), profile.ErrInvalidUsername)
	}
	_, err = s.GetUserIdByUsername(ctx, "uppercase")
	require.ErrorIs(t, err, profile.ErrNotFound)

	require.NoError(t, s.SetUsername(ctx, user1, "user_one"))

	p, err = s.GetProfile(ctx, user1, false)
	require.NoError(t, err)
	require.Equal(t, "user_one", p.Username.Value)
	require.NoError(t, p.Validate())

	// Claiming a handle is enough on its own to make the store know a user, the
	// same way setting any other profile field is.
	require.NoError(t, s.SetUsername(ctx, user2, "user_two"))
	p, err = s.GetProfile(ctx, user2, false)
	require.NoError(t, err)
	require.Equal(t, "user_two", p.Username.Value)

	// A handle resolves back to its holder, whatever casing it is looked up in.
	for _, lookup := range []string{"user_one", "USER_ONE", "User_One"} {
		got, err := s.GetUserIdByUsername(ctx, lookup)
		require.NoError(t, err)
		require.Equal(t, user1.Value, got.Value)
	}

	// A handle has one holder: a second user cannot take it, and the failed claim
	// leaves the handle they already held alone.
	require.ErrorIs(t, s.SetUsername(ctx, user2, "user_one"), profile.ErrUsernameTaken)
	p, err = s.GetProfile(ctx, user2, false)
	require.NoError(t, err)
	require.Equal(t, "user_two", p.Username.Value)

	// Re-claiming the handle a user already holds is a no-op, not a conflict.
	require.NoError(t, s.SetUsername(ctx, user1, "user_one"))
	p, err = s.GetProfile(ctx, user1, false)
	require.NoError(t, err)
	require.Equal(t, "user_one", p.Username.Value)

	// Changing a handle replaces the old one rather than accumulating, and frees
	// it for anyone else to take.
	require.NoError(t, s.SetUsername(ctx, user1, "renamed"))
	p, err = s.GetProfile(ctx, user1, false)
	require.NoError(t, err)
	require.Equal(t, "renamed", p.Username.Value)

	_, err = s.GetUserIdByUsername(ctx, "user_one")
	require.ErrorIs(t, err, profile.ErrNotFound)

	require.NoError(t, s.SetUsername(ctx, user2, "user_one"))
	got, err := s.GetUserIdByUsername(ctx, "user_one")
	require.NoError(t, err)
	require.Equal(t, user2.Value, got.Value)

	// The batch read carries handles the same way the single one does — it is the
	// path chat member rows are built from.
	noUsername := model.MustGenerateUserID()
	require.NoError(t, s.SetDisplayName(ctx, noUsername, "no handle"))

	publicProfiles, err := s.GetPublicProfiles(ctx, []*commonpb.UserId{user1, user2, noUsername})
	require.NoError(t, err)
	require.Len(t, publicProfiles, 3)
	require.Equal(t, "renamed", publicProfiles[string(user1.Value)].Username.Value)
	require.Equal(t, "user_one", publicProfiles[string(user2.Value)].Username.Value)
	require.Nil(t, publicProfiles[string(noUsername.Value)].Username)
	for _, publicProfile := range publicProfiles {
		require.NoError(t, publicProfile.Validate())
	}
}

func testGetPublicProfiles(t *testing.T, s profile.Store) {
	ctx := context.Background()

	user1 := model.MustGenerateUserID()
	user2 := model.MustGenerateUserID()
	unknown := model.MustGenerateUserID()

	// Empty input.
	got, err := s.GetPublicProfiles(ctx, nil)
	require.NoError(t, err)
	require.Empty(t, got)

	// Nobody is known to the store yet.
	got, err = s.GetPublicProfiles(ctx, []*commonpb.UserId{user1, user2})
	require.NoError(t, err)
	require.Empty(t, got)

	require.NoError(t, s.SetDisplayName(ctx, user1, "user one"))
	require.NoError(t, s.SetDisplayName(ctx, user2, "user two"))

	// Known users are returned keyed by user ID; unknown ones are absent.
	got, err = s.GetPublicProfiles(ctx, []*commonpb.UserId{user1, user2, unknown})
	require.NoError(t, err)
	require.Len(t, got, 2)
	require.Equal(t, "user one", got[string(user1.Value)].DisplayName)
	require.Equal(t, "user two", got[string(user2.Value)].DisplayName)
	_, ok := got[string(unknown.Value)]
	require.False(t, ok)

	// Every entry is a complete public profile: it validates, and carries a join
	// timestamp whether or not the user set anything else.
	for _, user := range []*commonpb.UserId{user1, user2} {
		p := got[string(user.Value)]
		require.NoError(t, p.Validate())
		require.NotNil(t, p.JoinTs)
		require.False(t, p.JoinTs.AsTime().IsZero())
	}

	// Private fields are never part of a public profile.
	require.NoError(t, s.LinkPhoneNumber(ctx, user1, "+12223334444", &commonpb.Hash{Value: []byte("phone-hash")}))
	require.NoError(t, s.LinkEmailAddress(ctx, user1, "someone@gmail.com"))

	got, err = s.GetPublicProfiles(ctx, []*commonpb.UserId{user1})
	require.NoError(t, err)
	require.Nil(t, got[string(user1.Value)].PhoneNumber)
	require.Nil(t, got[string(user1.Value)].EmailAddress)

	// A user known to the store but with no display name still has a public
	// profile: an empty name, and the join timestamp that makes it valid.
	noName := model.MustGenerateUserID()
	require.NoError(t, s.SetProfilePicture(ctx, noName, blob.MustGenerateID()))

	got, err = s.GetPublicProfiles(ctx, []*commonpb.UserId{noName})
	require.NoError(t, err)
	require.Len(t, got, 1)
	require.Empty(t, got[string(noName.Value)].DisplayName)
	require.NoError(t, got[string(noName.Value)].Validate())

	// A rename is reflected on the next read.
	require.NoError(t, s.SetDisplayName(ctx, user1, "user one renamed"))
	got, err = s.GetPublicProfiles(ctx, []*commonpb.UserId{user1})
	require.NoError(t, err)
	require.Equal(t, "user one renamed", got[string(user1.Value)].DisplayName)
}

func testGetUserIdByPhoneNumber(t *testing.T, s profile.Store) {
	ctx := context.Background()

	_, err := s.GetUserIdByPhoneNumber(ctx, "+19998887777")
	require.ErrorIs(t, err, profile.ErrNotFound)

	user1 := model.MustGenerateUserID()
	user2 := model.MustGenerateUserID()

	require.NoError(t, s.SetDisplayName(ctx, user1, "u1"))
	require.NoError(t, s.SetDisplayName(ctx, user2, "u2"))

	require.NoError(t, s.LinkPhoneNumber(ctx, user1, "+11111111111", &commonpb.Hash{Value: []byte("hash1")}))
	require.NoError(t, s.LinkPhoneNumber(ctx, user2, "+12222222222", &commonpb.Hash{Value: []byte("hash2")}))

	got, err := s.GetUserIdByPhoneNumber(ctx, "+11111111111")
	require.NoError(t, err)
	require.Equal(t, user1.Value, got.Value)

	got, err = s.GetUserIdByPhoneNumber(ctx, "+12222222222")
	require.NoError(t, err)
	require.Equal(t, user2.Value, got.Value)

	_, err = s.GetUserIdByPhoneNumber(ctx, "+19998887777")
	require.ErrorIs(t, err, profile.ErrNotFound)

	// Transfer the number to a different user — old user no longer resolves.
	require.NoError(t, s.LinkPhoneNumber(ctx, user2, "+11111111111", &commonpb.Hash{Value: []byte("hash1")}))

	got, err = s.GetUserIdByPhoneNumber(ctx, "+11111111111")
	require.NoError(t, err)
	require.Equal(t, user2.Value, got.Value)

	// Unlink leaves the number unresolvable.
	require.NoError(t, s.UnlinkPhoneNumber(ctx, user2, "+11111111111"))
	_, err = s.GetUserIdByPhoneNumber(ctx, "+11111111111")
	require.ErrorIs(t, err, profile.ErrNotFound)
}

func testLinkPhoneNumberForPayment(t *testing.T, s profile.Store) {
	ctx := context.Background()

	const phone = "+11111111111"
	phoneHash := &commonpb.Hash{Value: []byte("hash1")}

	user1 := model.MustGenerateUserID()
	user2 := model.MustGenerateUserID()

	require.NoError(t, s.SetDisplayName(ctx, user1, "u1"))
	require.NoError(t, s.SetDisplayName(ctx, user2, "u2"))

	// Enabling for payment without a linked number is not associated.
	flipped, err := s.LinkPhoneNumberForPayment(ctx, user1, phone)
	require.ErrorIs(t, err, profile.ErrNotFound)
	require.False(t, flipped)

	require.NoError(t, s.LinkPhoneNumber(ctx, user1, phone, phoneHash))

	// A linked number that has not been enabled for payment does not resolve.
	_, err = s.GetUserIdByPhoneNumberForPayment(ctx, phone)
	require.ErrorIs(t, err, profile.ErrNotFound)
	linked, err := s.IsPhoneNumberLinkedForPayment(ctx, user1, phone)
	require.NoError(t, err)
	require.False(t, linked)

	// Enabling a number not linked to the user is not associated.
	flipped, err = s.LinkPhoneNumberForPayment(ctx, user1, "+19998887777")
	require.ErrorIs(t, err, profile.ErrNotFound)
	require.False(t, flipped)

	// First enable flips the flag from false to true.
	flipped, err = s.LinkPhoneNumberForPayment(ctx, user1, phone)
	require.NoError(t, err)
	require.True(t, flipped)

	// The number now resolves for payment to its owner.
	got, err := s.GetUserIdByPhoneNumberForPayment(ctx, phone)
	require.NoError(t, err)
	require.Equal(t, user1.Value, got.Value)

	// IsPhoneNumberLinkedForPayment is true only for the exact (user, phone) pair.
	linked, err = s.IsPhoneNumberLinkedForPayment(ctx, user1, phone)
	require.NoError(t, err)
	require.True(t, linked)
	linked, err = s.IsPhoneNumberLinkedForPayment(ctx, user1, "+19998887777")
	require.NoError(t, err)
	require.False(t, linked)
	linked, err = s.IsPhoneNumberLinkedForPayment(ctx, user2, phone)
	require.NoError(t, err)
	require.False(t, linked)

	// Enabling again is idempotent and does not report a flip.
	flipped, err = s.LinkPhoneNumberForPayment(ctx, user1, phone)
	require.NoError(t, err)
	require.False(t, flipped)

	// Unlinking the number clears the payment flag.
	require.NoError(t, s.UnlinkPhoneNumber(ctx, user1, phone))
	_, err = s.GetUserIdByPhoneNumberForPayment(ctx, phone)
	require.ErrorIs(t, err, profile.ErrNotFound)
	linked, err = s.IsPhoneNumberLinkedForPayment(ctx, user1, phone)
	require.NoError(t, err)
	require.False(t, linked)

	// Re-linking starts from a disabled state (the flag was reset on unlink).
	require.NoError(t, s.LinkPhoneNumber(ctx, user1, phone, phoneHash))
	_, err = s.GetUserIdByPhoneNumberForPayment(ctx, phone)
	require.ErrorIs(t, err, profile.ErrNotFound)
	flipped, err = s.LinkPhoneNumberForPayment(ctx, user1, phone)
	require.NoError(t, err)
	require.True(t, flipped)

	// Transferring the number to another user clears the original owner's flag,
	// so it no longer resolves for payment until the new owner enables it.
	require.NoError(t, s.LinkPhoneNumber(ctx, user2, phone, phoneHash))
	_, err = s.GetUserIdByPhoneNumberForPayment(ctx, phone)
	require.ErrorIs(t, err, profile.ErrNotFound)
	linked, err = s.IsPhoneNumberLinkedForPayment(ctx, user1, phone)
	require.NoError(t, err)
	require.False(t, linked)

	flipped, err = s.LinkPhoneNumberForPayment(ctx, user2, phone)
	require.NoError(t, err)
	require.True(t, flipped)

	got, err = s.GetUserIdByPhoneNumberForPayment(ctx, phone)
	require.NoError(t, err)
	require.Equal(t, user2.Value, got.Value)
	linked, err = s.IsPhoneNumberLinkedForPayment(ctx, user2, phone)
	require.NoError(t, err)
	require.True(t, linked)
}

func phoneValues(phones []*commonpb.PhoneNumber) []string {
	out := make([]string, len(phones))
	for i, p := range phones {
		out[i] = p.Value
	}
	return out
}

func paymentPhoneValues(phones []*profile.PhoneForPayment) []string {
	out := make([]string, len(phones))
	for i, p := range phones {
		out[i] = p.PhoneNumber.Value
	}
	return out
}

func testProfilePictures(t *testing.T, s profile.Store) {
	ctx := context.Background()

	userID := model.MustGenerateUserID()
	otherUserID := model.MustGenerateUserID()

	pictureBlob := func(p *profilepb.UserProfile) *blobpb.BlobId {
		t.Helper()
		renditions := p.GetProfilePicture().GetRenditions()
		require.Len(t, renditions, 1)
		require.Equal(t, blobpb.Rendition_ORIGINAL, renditions[0].Role)
		return renditions[0].BlobId
	}

	t.Run("Unset", func(t *testing.T) {
		profiles, err := s.GetPublicProfiles(ctx, []*commonpb.UserId{userID})
		require.NoError(t, err)
		require.Empty(t, profiles)

		profiles, err = s.GetPublicProfiles(ctx, nil)
		require.NoError(t, err)
		require.Empty(t, profiles)
	})

	first := blob.MustGenerateID()

	t.Run("Set", func(t *testing.T) {
		require.NoError(t, s.SetProfilePicture(ctx, userID, first))

		// A picture alone is enough of a profile to exist, even with no display name.
		p, err := s.GetProfile(ctx, userID, false)
		require.NoError(t, err)
		require.Equal(t, first.Value, pictureBlob(p).Value)

		profiles, err := s.GetPublicProfiles(ctx, []*commonpb.UserId{userID, otherUserID})
		require.NoError(t, err)
		require.Len(t, profiles, 1)
		require.Equal(t, first.Value, pictureBlob(profiles[string(userID.Value)]).Value)
	})

	second := blob.MustGenerateID()

	t.Run("Replace", func(t *testing.T) {
		require.NoError(t, s.SetProfilePicture(ctx, userID, second))

		p, err := s.GetProfile(ctx, userID, false)
		require.NoError(t, err)
		require.Equal(t, second.Value, pictureBlob(p).Value)

		profiles, err := s.GetPublicProfiles(ctx, []*commonpb.UserId{userID})
		require.NoError(t, err)
		require.Equal(t, second.Value, pictureBlob(profiles[string(userID.Value)]).Value)
	})

	t.Run("Set the same picture again", func(t *testing.T) {
		require.NoError(t, s.SetProfilePicture(ctx, userID, second))

		p, err := s.GetProfile(ctx, userID, false)
		require.NoError(t, err)
		require.Equal(t, second.Value, pictureBlob(p).Value)
	})

	t.Run("Pictures are per user", func(t *testing.T) {
		third := blob.MustGenerateID()
		require.NoError(t, s.SetProfilePicture(ctx, otherUserID, third))

		profiles, err := s.GetPublicProfiles(ctx, []*commonpb.UserId{userID, otherUserID})
		require.NoError(t, err)
		require.Len(t, profiles, 2)
		require.Equal(t, second.Value, pictureBlob(profiles[string(userID.Value)]).Value)
		require.Equal(t, third.Value, pictureBlob(profiles[string(otherUserID.Value)]).Value)
	})
}

func testJoinTs(t *testing.T, s profile.Store) {
	ctx := context.Background()

	userID := model.MustGenerateUserID()

	before := time.Now().Add(-time.Minute)
	require.NoError(t, s.SetDisplayName(ctx, userID, "my name"))
	after := time.Now().Add(time.Minute)

	// The timestamp is public: it comes back whether or not private fields were
	// asked for, and it reflects when the user first became known to the store.
	for _, includePrivateFields := range []bool{false, true} {
		p, err := s.GetProfile(ctx, userID, includePrivateFields)
		require.NoError(t, err)
		require.NoError(t, p.Validate())
		require.NotNil(t, p.JoinTs)
		joinedAt := p.JoinTs.AsTime()
		require.True(t, joinedAt.After(before), "join ts %s is before %s", joinedAt, before)
		require.True(t, joinedAt.Before(after), "join ts %s is after %s", joinedAt, after)
	}

	p, err := s.GetProfile(ctx, userID, false)
	require.NoError(t, err)
	joinedAt := p.JoinTs.AsTime()

	// Later edits to the profile do not move the join timestamp.
	require.NoError(t, s.SetDisplayName(ctx, userID, "my other name"))
	require.NoError(t, s.LinkPhoneNumber(ctx, userID, "+12223334444", &commonpb.Hash{Value: []byte("phone-hash")}))
	require.NoError(t, s.LinkEmailAddress(ctx, userID, "someone@gmail.com"))

	p, err = s.GetProfile(ctx, userID, true)
	require.NoError(t, err)
	require.Equal(t, joinedAt, p.JoinTs.AsTime())

	// Linking a social account does not move it either.
	require.NoError(t, s.LinkXAccount(ctx, userID, &profilepb.XProfile{
		Id:            "join-ts",
		Username:      "username",
		Name:          "name",
		Description:   "description",
		ProfilePicUrl: "url",
		VerifiedType:  profilepb.XProfile_NONE,
		FollowerCount: 42,
	}, "accessToken"))

	p, err = s.GetProfile(ctx, userID, false)
	require.NoError(t, err)
	require.NoError(t, p.Validate())
	require.Equal(t, joinedAt, p.JoinTs.AsTime())
}
