package memory

import (
	"context"
	"encoding/base64"
	"sync"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	blobpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/blob/v1"
	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"
	profilepb "github.com/code-payments/flipcash2-protobuf-api/generated/go/profile/v1"

	"github.com/code-payments/flipcash2-server/profile"
)

type InMemoryStore struct {
	sync.Mutex

	profiles               map[string]*profilepb.UserProfile
	phoneHashesByUser      map[string][]byte
	linkedForPaymentByUser map[string]bool
	xProfilesByUser        map[string]*profilepb.XProfile
	createdAtByUser        map[string]time.Time
	tipCardColorByUser     map[string]string
	usernameByUser         map[string]string
}

func NewInMemory() profile.Store {
	return &InMemoryStore{
		profiles:               make(map[string]*profilepb.UserProfile),
		phoneHashesByUser:      make(map[string][]byte),
		linkedForPaymentByUser: make(map[string]bool),
		xProfilesByUser:        make(map[string]*profilepb.XProfile),
		createdAtByUser:        make(map[string]time.Time),
		tipCardColorByUser:     make(map[string]string),
		usernameByUser:         make(map[string]string),
	}
}

// tipCardCustomization resolves the Tip Card customization for key, filling in
// defaults for anything the user has not picked. Callers hold the lock.
func (m *InMemoryStore) tipCardCustomization(key string) *profilepb.TipCardCustomization {
	var storedColorHex *string
	if colorHex, ok := m.tipCardColorByUser[key]; ok {
		storedColorHex = &colorHex
	}
	return profile.TipCardCustomizationFromStored(storedColorHex)
}

// username resolves the handle for key, leaving it unset for a user who has not
// claimed one. Callers hold the lock.
func (m *InMemoryStore) username(key string) *commonpb.Username {
	username, ok := m.usernameByUser[key]
	if !ok {
		return nil
	}
	return &commonpb.Username{Value: username}
}

// ensureProfile returns the profile for key, creating an empty one (and
// recording the user's join timestamp) if it does not yet exist.
func (m *InMemoryStore) ensureProfile(key string) *profilepb.UserProfile {
	p, ok := m.profiles[key]
	if !ok {
		p = &profilepb.UserProfile{}
		m.profiles[key] = p
		m.createdAtByUser[key] = time.Now()
	}
	return p
}

func (m *InMemoryStore) GetProfile(_ context.Context, id *commonpb.UserId, includePrivateProfile bool) (*profilepb.UserProfile, error) {
	m.Lock()
	defer m.Unlock()

	key := userIDCacheKey(id)

	baseProfile, ok := m.profiles[key]
	if !ok {
		return nil, profile.ErrNotFound
	}

	clonedBaseProfile := proto.Clone(baseProfile).(*profilepb.UserProfile)
	clonedBaseProfile.UserId = proto.Clone(id).(*commonpb.UserId)
	clonedBaseProfile.JoinTs = timestamppb.New(m.createdAtByUser[key])
	clonedBaseProfile.TipCardCustomization = m.tipCardCustomization(key)
	clonedBaseProfile.Username = m.username(key)

	xProfile, ok := m.xProfilesByUser[key]
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

	return clonedBaseProfile, nil
}

func (m *InMemoryStore) SetProfilePicture(_ context.Context, id *commonpb.UserId, blobID *blobpb.BlobId) error {
	m.Lock()
	defer m.Unlock()

	p := m.ensureProfile(userIDCacheKey(id))

	// Only the ORIGINAL is stored; the server derives no renditions yet.
	p.ProfilePicture = &blobpb.Media{
		Renditions: []*blobpb.Rendition{{
			Role:   blobpb.Rendition_ORIGINAL,
			BlobId: proto.Clone(blobID).(*blobpb.BlobId),
		}},
	}

	return nil
}

func (m *InMemoryStore) SetTipCardColor(_ context.Context, id *commonpb.UserId, colorHex string) error {
	m.Lock()
	defer m.Unlock()

	key := userIDCacheKey(id)
	m.ensureProfile(key)
	m.tipCardColorByUser[key] = colorHex

	return nil
}

// profilePictureBlob returns a copy of the blob holding the media's ORIGINAL
// rendition, or nil when there is no media or it has no original.
func profilePictureBlob(media *blobpb.Media) *blobpb.BlobId {
	for _, r := range media.GetRenditions() {
		if r.Role == blobpb.Rendition_ORIGINAL && r.BlobId != nil {
			return proto.Clone(r.BlobId).(*blobpb.BlobId)
		}
	}
	return nil
}

func (m *InMemoryStore) SetDisplayName(_ context.Context, id *commonpb.UserId, displayName string) error {
	m.Lock()
	defer m.Unlock()

	profile := m.ensureProfile(userIDCacheKey(id))

	// TODO: Validate eventually
	profile.DisplayName = displayName

	return nil
}

func (m *InMemoryStore) SetUsername(_ context.Context, id *commonpb.UserId, username string) error {
	if err := profile.ValidateUsername(username); err != nil {
		return err
	}

	m.Lock()
	defer m.Unlock()

	targetKey := userIDCacheKey(id)
	if held, ok := m.usernameByUser[targetKey]; ok {
		// Re-claiming the handle the user already holds asks for the state they are
		// already in, so it is a no-op rather than a conflict.
		if held == username {
			return nil
		}
		return profile.ErrUsernameAlreadySet
	}
	for _, held := range m.usernameByUser {
		if held == username {
			return profile.ErrUsernameTaken
		}
	}

	m.ensureProfile(targetKey)
	m.usernameByUser[targetKey] = username

	return nil
}

func (m *InMemoryStore) GetUserIdByUsername(_ context.Context, username string) (*commonpb.UserId, error) {
	m.Lock()
	defer m.Unlock()

	normalized := profile.NormalizeUsername(username)
	for key, held := range m.usernameByUser {
		if held != normalized {
			continue
		}
		decoded, err := base64.StdEncoding.DecodeString(key)
		if err != nil {
			return nil, err
		}
		return &commonpb.UserId{Value: decoded}, nil
	}
	return nil, profile.ErrNotFound
}

func (m *InMemoryStore) GetPublicProfiles(_ context.Context, userIDs []*commonpb.UserId) (map[string]*profilepb.UserProfile, error) {
	out := make(map[string]*profilepb.UserProfile)
	if len(userIDs) == 0 {
		return out, nil
	}

	m.Lock()
	defer m.Unlock()

	for _, userID := range userIDs {
		key := userIDCacheKey(userID)
		p, ok := m.profiles[key]
		if !ok {
			continue
		}

		// Only the public fields, and a fresh proto per user so a caller mutating
		// what it gets back cannot reach into the store.
		publicProfile := &profilepb.UserProfile{
			UserId:               proto.Clone(userID).(*commonpb.UserId),
			DisplayName:          p.DisplayName,
			Username:             m.username(key),
			JoinTs:               timestamppb.New(m.createdAtByUser[key]),
			TipCardCustomization: m.tipCardCustomization(key),
		}
		if blobID := profilePictureBlob(p.ProfilePicture); blobID != nil {
			publicProfile.ProfilePicture = &blobpb.Media{
				Renditions: []*blobpb.Rendition{{
					Role:   blobpb.Rendition_ORIGINAL,
					BlobId: blobID,
				}},
			}
		}
		out[string(userID.Value)] = publicProfile
	}
	return out, nil
}

func (m *InMemoryStore) LinkPhoneNumber(_ context.Context, id *commonpb.UserId, phoneNumber string, phoneNumberHash *commonpb.Hash) error {
	m.Lock()
	defer m.Unlock()

	targetKey := userIDCacheKey(id)
	for key, p := range m.profiles {
		if key == targetKey {
			continue
		}
		if p.PhoneNumber != nil && p.PhoneNumber.Value == phoneNumber {
			p.PhoneNumber = nil
			delete(m.phoneHashesByUser, key)
			delete(m.linkedForPaymentByUser, key)
		}
	}

	profile := m.ensureProfile(targetKey)

	profile.PhoneNumber = &commonpb.PhoneNumber{Value: phoneNumber}

	m.phoneHashesByUser[targetKey] = phoneNumberHash.Value

	return nil
}

func (m *InMemoryStore) UnlinkPhoneNumber(ctx context.Context, userID *commonpb.UserId, phoneNumber string) error {
	m.Lock()
	defer m.Unlock()

	key := userIDCacheKey(userID)
	profile, ok := m.profiles[key]
	if !ok {
		return nil
	}

	if profile.PhoneNumber != nil && profile.PhoneNumber.Value == phoneNumber {
		profile.PhoneNumber = nil
		delete(m.phoneHashesByUser, key)
		delete(m.linkedForPaymentByUser, key)
	}

	return nil
}

func (m *InMemoryStore) LinkPhoneNumberForPayment(_ context.Context, id *commonpb.UserId, phoneNumber string) (bool, error) {
	m.Lock()
	defer m.Unlock()

	key := userIDCacheKey(id)
	p, ok := m.profiles[key]
	if !ok || p.PhoneNumber == nil || p.PhoneNumber.Value != phoneNumber {
		return false, profile.ErrNotFound
	}

	wasLinked := m.linkedForPaymentByUser[key]
	m.linkedForPaymentByUser[key] = true

	return !wasLinked, nil
}

func (m *InMemoryStore) IsPhoneNumberLinkedForPayment(_ context.Context, id *commonpb.UserId, phoneNumber string) (bool, error) {
	m.Lock()
	defer m.Unlock()

	key := userIDCacheKey(id)
	p, ok := m.profiles[key]
	if !ok || p.PhoneNumber == nil || p.PhoneNumber.Value != phoneNumber {
		return false, nil
	}
	return m.linkedForPaymentByUser[key], nil
}

func (m *InMemoryStore) GetPhonesByHashes(_ context.Context, hashes []*commonpb.Hash) ([]*commonpb.PhoneNumber, error) {
	matches, err := m.getPhonesByHashes(hashes, false)
	if err != nil {
		return nil, err
	}
	out := make([]*commonpb.PhoneNumber, len(matches))
	for i, match := range matches {
		out[i] = match.PhoneNumber
	}
	return out, nil
}

func (m *InMemoryStore) GetPhonesByHashesForPayment(_ context.Context, hashes []*commonpb.Hash) ([]*profile.PhoneForPayment, error) {
	return m.getPhonesByHashes(hashes, true)
}

func (m *InMemoryStore) getPhonesByHashes(hashes []*commonpb.Hash, forPaymentOnly bool) ([]*profile.PhoneForPayment, error) {
	if len(hashes) == 0 {
		return nil, nil
	}

	m.Lock()
	defer m.Unlock()

	wanted := make(map[string]struct{}, len(hashes))
	for _, h := range hashes {
		wanted[string(h.Value)] = struct{}{}
	}

	var out []*profile.PhoneForPayment
	for key, hash := range m.phoneHashesByUser {
		if _, ok := wanted[string(hash)]; !ok {
			continue
		}
		if forPaymentOnly && !m.linkedForPaymentByUser[key] {
			continue
		}
		p, ok := m.profiles[key]
		if !ok || p.PhoneNumber == nil {
			continue
		}
		userID, err := base64.StdEncoding.DecodeString(key)
		if err != nil {
			return nil, err
		}
		out = append(out, &profile.PhoneForPayment{
			PhoneNumber: &commonpb.PhoneNumber{Value: p.PhoneNumber.Value},
			UserID:      &commonpb.UserId{Value: userID},
			JoinedAt:    m.createdAtByUser[key],
		})
	}
	return out, nil
}

func (m *InMemoryStore) GetPhoneNumbersForPayment(_ context.Context, userIDs []*commonpb.UserId) (map[string]*commonpb.PhoneNumber, error) {
	out := make(map[string]*commonpb.PhoneNumber)
	if len(userIDs) == 0 {
		return out, nil
	}

	m.Lock()
	defer m.Unlock()

	for _, userID := range userIDs {
		key := userIDCacheKey(userID)
		if !m.linkedForPaymentByUser[key] {
			continue
		}
		p, ok := m.profiles[key]
		if !ok || p.PhoneNumber == nil {
			continue
		}
		out[string(userID.Value)] = &commonpb.PhoneNumber{Value: p.PhoneNumber.Value}
	}
	return out, nil
}

func (m *InMemoryStore) GetUserIdByPhoneNumber(_ context.Context, phoneNumber string) (*commonpb.UserId, error) {
	m.Lock()
	defer m.Unlock()

	for key, p := range m.profiles {
		if p.PhoneNumber == nil || p.PhoneNumber.Value != phoneNumber {
			continue
		}
		decoded, err := base64.StdEncoding.DecodeString(key)
		if err != nil {
			return nil, err
		}
		return &commonpb.UserId{Value: decoded}, nil
	}
	return nil, profile.ErrNotFound
}

func (m *InMemoryStore) GetUserIdByPhoneNumberForPayment(_ context.Context, phoneNumber string) (*commonpb.UserId, error) {
	m.Lock()
	defer m.Unlock()

	for key, p := range m.profiles {
		if p.PhoneNumber == nil || p.PhoneNumber.Value != phoneNumber {
			continue
		}
		if !m.linkedForPaymentByUser[key] {
			continue
		}
		decoded, err := base64.StdEncoding.DecodeString(key)
		if err != nil {
			return nil, err
		}
		return &commonpb.UserId{Value: decoded}, nil
	}
	return nil, profile.ErrNotFound
}

func (m *InMemoryStore) LinkEmailAddress(_ context.Context, id *commonpb.UserId, emailAddress string) error {
	m.Lock()
	defer m.Unlock()

	targetKey := userIDCacheKey(id)
	for key, p := range m.profiles {
		if key == targetKey {
			continue
		}
		if p.EmailAddress != nil && p.EmailAddress.Value == emailAddress {
			p.EmailAddress = nil
		}
	}

	profile := m.ensureProfile(targetKey)

	profile.EmailAddress = &commonpb.EmailAddress{Value: emailAddress}

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

	// A user reachable only through an X link is still a user, and Postgres would
	// have a row (and so a join timestamp) for them. Record one here too.
	m.ensureProfile(userIDCacheKey(userID))

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
	m.phoneHashesByUser = make(map[string][]byte)
	m.linkedForPaymentByUser = make(map[string]bool)
	m.xProfilesByUser = make(map[string]*profilepb.XProfile)
	m.createdAtByUser = make(map[string]time.Time)
	m.tipCardColorByUser = make(map[string]string)
	m.usernameByUser = make(map[string]string)
}

func userIDCacheKey(id *commonpb.UserId) string {
	return base64.StdEncoding.EncodeToString(id.Value)
}
