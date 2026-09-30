package memory

import (
	"context"
	"encoding/base64"
	"errors"
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
	flipcardColorByUser    map[string]string
	usernameByUser         map[string]string
	autoAssignedByUser     map[string]struct{} // users whose current handle was assigned by default
	minDmChatInitFeeByUser map[string]*commonpb.FiatPaymentAmount
}

func NewInMemory() profile.Store {
	return &InMemoryStore{
		profiles:               make(map[string]*profilepb.UserProfile),
		phoneHashesByUser:      make(map[string][]byte),
		linkedForPaymentByUser: make(map[string]bool),
		xProfilesByUser:        make(map[string]*profilepb.XProfile),
		createdAtByUser:        make(map[string]time.Time),
		flipcardColorByUser:    make(map[string]string),
		usernameByUser:         make(map[string]string),
		autoAssignedByUser:     make(map[string]struct{}),
		minDmChatInitFeeByUser: make(map[string]*commonpb.FiatPaymentAmount),
	}
}

// minDmChatInitFee returns a copy of the minimum DM chat initialization fee for
// key, or nil for a user who has not set one. Callers hold the lock.
func (m *InMemoryStore) minDmChatInitFee(key string) *commonpb.FiatPaymentAmount {
	fee, ok := m.minDmChatInitFeeByUser[key]
	if !ok {
		return nil
	}
	return proto.Clone(fee).(*commonpb.FiatPaymentAmount)
}

// flipcardCustomization resolves the Flipcard customization for key, filling in
// defaults for anything the user has not picked. Callers hold the lock.
func (m *InMemoryStore) flipcardCustomization(key string) *profilepb.FlipcardCustomization {
	var storedColorHex *string
	if colorHex, ok := m.flipcardColorByUser[key]; ok {
		storedColorHex = &colorHex
	}
	return profile.FlipcardCustomizationFromStored(storedColorHex)
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
	clonedBaseProfile.FlipcardCustomization = m.flipcardCustomization(key)
	clonedBaseProfile.Username = m.username(key)
	clonedBaseProfile.MinDmChatInitFee = m.minDmChatInitFee(key)

	xProfile, ok := m.xProfilesByUser[key]
	if ok {
		clonedXProfile := proto.Clone(xProfile).(*profilepb.XProfile)
		clonedBaseProfile.SocialProfiles = append(clonedBaseProfile.SocialProfiles, &profilepb.SocialProfile{
			Type: &profilepb.SocialProfile_X{
				X: clonedXProfile,
			},
		})
	}

	if includePrivateProfile {
		_, clonedBaseProfile.IsUsernameAutoAssigned = m.autoAssignedByUser[key]
	} else {
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

func (m *InMemoryStore) SetFlipcardColor(_ context.Context, id *commonpb.UserId, colorHex string) error {
	m.Lock()
	defer m.Unlock()

	key := userIDCacheKey(id)
	m.ensureProfile(key)
	m.flipcardColorByUser[key] = colorHex

	return nil
}

func (m *InMemoryStore) SetMinDmChatInitFee(_ context.Context, id *commonpb.UserId, fee *commonpb.FiatPaymentAmount) error {
	m.Lock()
	defer m.Unlock()

	key := userIDCacheKey(id)
	m.ensureProfile(key)
	m.minDmChatInitFeeByUser[key] = proto.Clone(fee).(*commonpb.FiatPaymentAmount)

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

func (m *InMemoryStore) SetDisplayNameWithDefaultUsername(ctx context.Context, id *commonpb.UserId, displayName, usernameBase string) (profile.DefaultUsernameResult, error) {
	m.Lock()
	defer m.Unlock()

	key := userIDCacheKey(id)

	var result profile.DefaultUsernameResult
	if _, hasUsername := m.usernameByUser[key]; !hasUsername {
		// Nothing is written until the whole assignment has succeeded, so a failure
		// leaves the display name unset too. The lock rules out a concurrent claim,
		// so a claim only fails on a handle already held.
		claim := func(ctx context.Context, username string) (bool, error) {
			held, err := m.heldUsernames(ctx, []string{username})
			if err != nil {
				return false, err
			}
			if _, ok := held[username]; ok {
				return false, profile.ErrUsernameTaken
			}
			return true, nil
		}

		username, err := profile.AssignDefaultUsername(ctx, usernameBase, m.heldUsernames, claim)
		switch {
		case err == nil:
			result.Username = username
		case errors.Is(err, profile.ErrNoDefaultUsername):
			result.NoneAvailable = true
		default:
			return profile.DefaultUsernameResult{}, err
		}
	}

	m.ensureProfile(key).DisplayName = displayName
	if result.Username != "" {
		m.usernameByUser[key] = result.Username
		m.autoAssignedByUser[key] = struct{}{}
	}

	return result, nil
}

// heldUsernames is a profile.HeldUsernamesFunc over the store. Callers hold the
// lock.
func (m *InMemoryStore) heldUsernames(_ context.Context, usernames []string) (map[string]struct{}, error) {
	wanted := make(map[string]struct{}, len(usernames))
	for _, username := range usernames {
		wanted[username] = struct{}{}
	}

	held := make(map[string]struct{})
	for _, username := range m.usernameByUser {
		if _, ok := wanted[username]; ok {
			held[username] = struct{}{}
		}
	}
	return held, nil
}

func (m *InMemoryStore) SetUsername(_ context.Context, id *commonpb.UserId, username string) error {
	if err := profile.ValidateUsername(username); err != nil {
		return err
	}

	m.Lock()
	defer m.Unlock()

	targetKey := userIDCacheKey(id)
	for key, held := range m.usernameByUser {
		if key != targetKey && held == username {
			return profile.ErrUsernameTaken
		}
	}

	m.ensureProfile(targetKey)
	if m.usernameByUser[targetKey] != username {
		m.usernameByUser[targetKey] = username
		delete(m.autoAssignedByUser, targetKey)
	}

	return nil
}

func (m *InMemoryStore) IsUsernameAutoAssigned(_ context.Context, id *commonpb.UserId) (bool, error) {
	m.Lock()
	defer m.Unlock()

	key := userIDCacheKey(id)
	if _, ok := m.profiles[key]; !ok {
		return false, profile.ErrNotFound
	}
	_, autoAssigned := m.autoAssignedByUser[key]
	return autoAssigned, nil
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
			UserId:                proto.Clone(userID).(*commonpb.UserId),
			DisplayName:           p.DisplayName,
			Username:              m.username(key),
			JoinTs:                timestamppb.New(m.createdAtByUser[key]),
			FlipcardCustomization: m.flipcardCustomization(key),
			MinDmChatInitFee:      m.minDmChatInitFee(key),
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
	m.flipcardColorByUser = make(map[string]string)
	m.usernameByUser = make(map[string]string)
	m.autoAssignedByUser = make(map[string]struct{})
	m.minDmChatInitFeeByUser = make(map[string]*commonpb.FiatPaymentAmount)
}

func userIDCacheKey(id *commonpb.UserId) string {
	return base64.StdEncoding.EncodeToString(id.Value)
}
