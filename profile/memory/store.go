package memory

import (
	"context"
	"encoding/base64"
	"sync"

	"google.golang.org/protobuf/proto"

	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"
	emailpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/email/v1"
	phonepb "github.com/code-payments/flipcash2-protobuf-api/generated/go/phone/v1"
	profilepb "github.com/code-payments/flipcash2-protobuf-api/generated/go/profile/v1"

	"github.com/code-payments/flipcash2-server/profile"
)

type InMemoryStore struct {
	sync.Mutex

	profiles        map[string]*profilepb.UserProfile
	xProfilesByUser map[string]*profilepb.XProfile
}

func NewInMemory() profile.Store {
	return &InMemoryStore{
		profiles:        make(map[string]*profilepb.UserProfile),
		xProfilesByUser: make(map[string]*profilepb.XProfile),
	}
}

func (m *InMemoryStore) GetProfile(_ context.Context, id *commonpb.UserId, includePrivateProfile bool) (*profilepb.UserProfile, error) {
	m.Lock()
	defer m.Unlock()

	baseProfile, ok := m.profiles[userIDCacheKey(id)]
	clonedBaseProfile := &profilepb.UserProfile{}
	if ok {
		clonedBaseProfile = proto.Clone(baseProfile).(*profilepb.UserProfile)
	}

	xProfile, ok := m.xProfilesByUser[userIDCacheKey(id)]
	if ok {
		clonedXProfile := proto.Clone(xProfile).(*profilepb.XProfile)
		clonedBaseProfile.SocialProfiles = append(clonedBaseProfile.SocialProfiles, &profilepb.SocialProfile{
			Type: &profilepb.SocialProfile_X{
				X: clonedXProfile,
			},
		})
	}

	if !includePrivateProfile {
		clonedBaseProfile.PhoneNumber = nil
		clonedBaseProfile.EmailAddress = nil
	}

	if len(clonedBaseProfile.DisplayName) == 0 && len(clonedBaseProfile.SocialProfiles) == 0 && clonedBaseProfile.PhoneNumber == nil && clonedBaseProfile.EmailAddress == nil {
		return nil, profile.ErrNotFound
	}

	return clonedBaseProfile, nil
}

func (m *InMemoryStore) SetDisplayName(_ context.Context, id *commonpb.UserId, displayName string) error {
	m.Lock()
	defer m.Unlock()

	profile, ok := m.profiles[userIDCacheKey(id)]
	if !ok {
		profile = &profilepb.UserProfile{}
	}

	// TODO: Validate eventually
	profile.DisplayName = displayName

	m.profiles[userIDCacheKey(id)] = profile

	return nil
}

func (m *InMemoryStore) SetPhoneNumber(_ context.Context, id *commonpb.UserId, phoneNumber string) error {
	m.Lock()
	defer m.Unlock()

	profile, ok := m.profiles[userIDCacheKey(id)]
	if !ok {
		profile = &profilepb.UserProfile{}
	}

	profile.PhoneNumber = &phonepb.PhoneNumber{Value: phoneNumber}

	m.profiles[userIDCacheKey(id)] = profile

	return nil
}

func (m *InMemoryStore) UnlinkPhoneNumber(ctx context.Context, userID *commonpb.UserId, phoneNumber string) error {
	m.Lock()
	defer m.Unlock()

	profile, ok := m.profiles[userIDCacheKey(userID)]
	if !ok {
		return nil
	}

	if profile.PhoneNumber != nil && profile.PhoneNumber.Value == phoneNumber {
		profile.PhoneNumber = nil
	}

	return nil
}

func (m *InMemoryStore) SetEmailAddress(_ context.Context, id *commonpb.UserId, emailAddress string) error {
	m.Lock()
	defer m.Unlock()

	profile, ok := m.profiles[userIDCacheKey(id)]
	if !ok {
		profile = &profilepb.UserProfile{}
	}

	profile.EmailAddress = &emailpb.EmailAddress{Value: emailAddress}

	m.profiles[userIDCacheKey(id)] = profile

	return nil
}

func (m *InMemoryStore) UnlinkEmailAddress(ctx context.Context, userID *commonpb.UserId, emailAddress string) error {
	m.Lock()
	defer m.Unlock()

	profile, ok := m.profiles[userIDCacheKey(userID)]
	if !ok {
		return nil
	}

	if profile.EmailAddress != nil && profile.EmailAddress.Value == emailAddress {
		profile.EmailAddress = nil
	}

	return nil
}

func (m *InMemoryStore) LinkXAccount(ctx context.Context, userID *commonpb.UserId, xProfile *profilepb.XProfile, accessToken string) error {
	m.Lock()
	defer m.Unlock()

	existingByUser, ok := m.xProfilesByUser[userIDCacheKey(userID)]
	if ok {
		if existingByUser.Id != xProfile.Id {
			return profile.ErrExistingSocialLink
		}

		existingByUser.Username = xProfile.Username
		existingByUser.Name = xProfile.Name
		existingByUser.Description = xProfile.Description
		existingByUser.ProfilePicUrl = xProfile.ProfilePicUrl
		existingByUser.VerifiedType = xProfile.VerifiedType
		existingByUser.FollowerCount = xProfile.FollowerCount
		return nil
	}

	for key, profile := range m.xProfilesByUser {
		if profile.Id == xProfile.Id {
			delete(m.xProfilesByUser, key)
		}
	}

	cloned := proto.Clone(xProfile).(*profilepb.XProfile)
	m.xProfilesByUser[userIDCacheKey(userID)] = cloned

	return nil
}

func (m *InMemoryStore) UnlinkXAccount(ctx context.Context, userID *commonpb.UserId, xUserID string) error {
	m.Lock()
	defer m.Unlock()

	existingByUser, ok := m.xProfilesByUser[userIDCacheKey(userID)]
	if !ok {
		return profile.ErrNotFound
	}

	if existingByUser.Id != xUserID {
		return profile.ErrNotFound
	}

	delete(m.xProfilesByUser, userIDCacheKey(userID))

	return nil

}

func (m *InMemoryStore) GetXProfile(ctx context.Context, userID *commonpb.UserId) (*profilepb.XProfile, error) {
	m.Lock()
	defer m.Unlock()

	val, ok := m.xProfilesByUser[userIDCacheKey(userID)]
	if !ok {
		return nil, profile.ErrNotFound
	}

	return proto.Clone(val).(*profilepb.XProfile), nil
}

func (m *InMemoryStore) reset() {
	m.Lock()
	defer m.Unlock()

	m.profiles = make(map[string]*profilepb.UserProfile)
	m.xProfilesByUser = make(map[string]*profilepb.XProfile)
}

func userIDCacheKey(id *commonpb.UserId) string {
	return base64.StdEncoding.EncodeToString(id.Value)
}
