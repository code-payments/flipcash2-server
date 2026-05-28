package postgres

import (
	"context"

	"github.com/jackc/pgx/v5/pgxpool"

	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"
	emailpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/email/v1"
	phonepb "github.com/code-payments/flipcash2-protobuf-api/generated/go/phone/v1"
	profilepb "github.com/code-payments/flipcash2-protobuf-api/generated/go/profile/v1"

	"github.com/code-payments/ocp-server/pointer"
	"github.com/code-payments/flipcash2-server/profile"
)

type store struct {
	pool *pgxpool.Pool
}

func NewInPostgres(pool *pgxpool.Pool) profile.Store {
	return &store{
		pool: pool,
	}
}

func (s *store) GetProfile(ctx context.Context, id *commonpb.UserId, includePrivateProfile bool) (*profilepb.UserProfile, error) {
	displayName, err := dbGetDisplayName(ctx, s.pool, id)
	if err != nil {
		return nil, err
	}

	userProfile := &profilepb.UserProfile{
		DisplayName: *pointer.StringOrDefault(displayName, ""),
	}

	if includePrivateProfile {
		phoneNumber, err := dbGetPhoneNumber(ctx, s.pool, id)
		if err != nil {
			return nil, err
		}
		if phoneNumber != nil {
			userProfile.PhoneNumber = &phonepb.PhoneNumber{Value: *phoneNumber}
		}

		emailAddress, err := dbGetEmailAddress(ctx, s.pool, id)
		if err != nil {
			return nil, err
		}
		if emailAddress != nil {
			userProfile.EmailAddress = &emailpb.EmailAddress{Value: *emailAddress}
		}
	}

	xProfileModel, err := dbGetXProfile(ctx, s.pool, id)
	if err == nil {
		xProfile, err := fromXProfileModel(xProfileModel)
		if err != nil {
			return nil, err
		}

		userProfile.SocialProfiles = append(userProfile.SocialProfiles, &profilepb.SocialProfile{
			Type: &profilepb.SocialProfile_X{
				X: xProfile,
			},
		})
	} else if err != profile.ErrNotFound {
		return nil, err
	}

	if len(userProfile.DisplayName) == 0 && len(userProfile.SocialProfiles) == 0 && userProfile.PhoneNumber == nil && userProfile.EmailAddress == nil {
		return nil, profile.ErrNotFound
	}
	return userProfile, nil
}

func (s *store) SetDisplayName(ctx context.Context, id *commonpb.UserId, displayName string) error {
	return dbSetDisplayName(ctx, s.pool, id, displayName)
}

func (s *store) LinkPhoneNumber(ctx context.Context, id *commonpb.UserId, phoneNumber string, phoneNumberHash *commonpb.Hash) error {
	return dbLinkPhoneNumber(ctx, s.pool, id, phoneNumber, phoneNumberHash)
}

func (s *store) UnlinkPhoneNumber(ctx context.Context, userID *commonpb.UserId, phoneNumber string) error {
	return dbUnlinkPhoneNumber(ctx, s.pool, userID, phoneNumber)
}

func (s *store) LinkPhoneNumberForPayment(ctx context.Context, userID *commonpb.UserId, phoneNumber string) (bool, error) {
	return dbLinkPhoneNumberForPayment(ctx, s.pool, userID, phoneNumber)
}

func (s *store) IsPhoneNumberLinkedForPayment(ctx context.Context, userID *commonpb.UserId, phoneNumber string) (bool, error) {
	return dbIsPhoneNumberLinkedForPayment(ctx, s.pool, userID, phoneNumber)
}

func (s *store) GetPhonesByHashes(ctx context.Context, hashes []*commonpb.Hash) ([]*phonepb.PhoneNumber, error) {
	return dbGetPhonesByHashes(ctx, s.pool, hashes)
}

func (s *store) GetPhonesByHashesForPayment(ctx context.Context, hashes []*commonpb.Hash) ([]*phonepb.PhoneNumber, error) {
	return dbGetPhonesByHashesForPayment(ctx, s.pool, hashes)
}

func (s *store) GetUserIdByPhoneNumber(ctx context.Context, phoneNumber string) (*commonpb.UserId, error) {
	return dbGetUserIdByPhoneNumber(ctx, s.pool, phoneNumber)
}

func (s *store) GetUserIdByPhoneNumberForPayment(ctx context.Context, phoneNumber string) (*commonpb.UserId, error) {
	return dbGetUserIdByPhoneNumberForPayment(ctx, s.pool, phoneNumber)
}

func (s *store) LinkEmailAddress(ctx context.Context, id *commonpb.UserId, emailAddress string) error {
	return dbLinkEmailAddress(ctx, s.pool, id, emailAddress)
}

func (s *store) UnlinkEmailAddress(ctx context.Context, userID *commonpb.UserId, emailAddress string) error {
	return dbUnlinkEmailAddress(ctx, s.pool, userID, emailAddress)
}

func (s *store) LinkXAccount(ctx context.Context, userID *commonpb.UserId, xProfile *profilepb.XProfile, accessToken string) error {
	model, err := toXProfileModel(userID, xProfile, accessToken)
	if err != nil {
		return err
	}

	existing, err := dbGetXProfile(ctx, s.pool, userID)
	if err != nil && err != profile.ErrNotFound {
		return err
	}

	if existing != nil && existing.ID != xProfile.Id {
		return profile.ErrExistingSocialLink
	}

	return model.dbUpsert(ctx, s.pool)
}

func (s *store) UnlinkXAccount(ctx context.Context, userID *commonpb.UserId, xUserID string) error {
	return dbUnlinkXAccount(ctx, s.pool, userID, xUserID)
}

func (s *store) GetXProfile(ctx context.Context, userID *commonpb.UserId) (*profilepb.XProfile, error) {
	model, err := dbGetXProfile(ctx, s.pool, userID)
	if err != nil {
		return nil, err
	}
	return fromXProfileModel(model)
}

func (s *store) reset() {
	_, err := s.pool.Exec(context.Background(), `UPDATE `+usersTableName+` SET "displayName" = NULL, "phoneNumber" = NULL, "phoneNumberHash" = NULL, "emailAddress" = NULL, "isPhoneNumberLinkedForPayment" = FALSE`)
	if err != nil {
		panic(err)
	}

	_, err = s.pool.Exec(context.Background(), "DELETE FROM "+xProfilesTableName)
	if err != nil {
		panic(err)
	}
}
