package profile

import (
	"context"

	blobpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/blob/v1"
	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"
	profilepb "github.com/code-payments/flipcash2-protobuf-api/generated/go/profile/v1"

	"github.com/code-payments/flipcash2-server/chat"
)

// chatProfileReader adapts a profile Store to chat.ProfileReader, the read slice
// the Chat service uses to hydrate member profiles. It lives here (rather than
// in chat) because chat must not import profile; the dependency is one-way.
type chatProfileReader struct {
	store Store
	media Media
}

// NewChatProfileReader returns a chat.ProfileReader backed by the given profile
// store and blob storage, for wiring the Chat service. The media dependency is what
// lets member avatars come back with resolved blob metadata, so chat itself never
// touches blob storage.
func NewChatProfileReader(store Store, media Media) chat.ProfileReader {
	return &chatProfileReader{store: store, media: media}
}

func (r *chatProfileReader) GetPhoneNumbers(ctx context.Context, userIDs []*commonpb.UserId) (map[string]*commonpb.PhoneNumber, error) {
	// Only numbers the user has enabled for payment are shared into the chat.
	return r.store.GetPhoneNumbersForPayment(ctx, userIDs)
}

func (r *chatProfileReader) GetPublicProfiles(ctx context.Context, userIDs []*commonpb.UserId) (map[string]*profilepb.UserProfile, error) {
	publicProfiles, err := r.store.GetPublicProfiles(ctx, userIDs)
	if err != nil {
		return nil, err
	}

	// The store leaves each picture's blob metadata unresolved, so collect them
	// and resolve the page's avatars in one batch rather than one per member.
	toHydrate := make([]*blobpb.Media, 0, len(publicProfiles))
	for _, publicProfile := range publicProfiles {
		if publicProfile.ProfilePicture != nil {
			toHydrate = append(toHydrate, publicProfile.ProfilePicture)
		}
	}
	if err := hydratePictures(ctx, r.media, toHydrate...); err != nil {
		return nil, err
	}
	return publicProfiles, nil
}
