package tests

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"
	phonepb "github.com/code-payments/flipcash2-protobuf-api/generated/go/phone/v1"
	profilepb "github.com/code-payments/flipcash2-protobuf-api/generated/go/profile/v1"

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
		testGetUserIdByPhoneNumber,
		testLinkPhoneNumberForPayment,
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
	got, err = s.GetPhonesByHashesForPayment(ctx, []*commonpb.Hash{hash1, hash2, hash3})
	require.NoError(t, err)
	require.Empty(t, got)

	flipped, err := s.LinkPhoneNumberForPayment(ctx, user1, "+11111111111")
	require.NoError(t, err)
	require.True(t, flipped)
	flipped, err = s.LinkPhoneNumberForPayment(ctx, user3, "+13333333333")
	require.NoError(t, err)
	require.True(t, flipped)

	// Only the enabled numbers are returned (user2 is still excluded).
	got, err = s.GetPhonesByHashesForPayment(ctx, []*commonpb.Hash{hash1, hash2, hash3, missing})
	require.NoError(t, err)
	require.ElementsMatch(t,
		[]string{"+11111111111", "+13333333333"},
		phoneValues(got),
	)

	// Empty input is still handled.
	got, err = s.GetPhonesByHashesForPayment(ctx, nil)
	require.NoError(t, err)
	require.Empty(t, got)
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

func phoneValues(phones []*phonepb.PhoneNumber) []string {
	out := make([]string, len(phones))
	for i, p := range phones {
		out[i] = p.Value
	}
	return out
}
